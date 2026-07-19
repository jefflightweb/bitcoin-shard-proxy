// Package worker — tcp.go provides TCPIngress: a TCP listener that accepts
// reliable frame delivery connections and feeds them into the shared Forwarder.
//
// # Protocol
//
// Each TCP connection carries a stream of BRC-12, BRC-124, or BRC-128 frames with no framing
// envelope. The proxy reads the minimum header first (44 bytes for BRC-12,
// extended to 92 for BRC-124/BRC-128), then reads the declared payload:
//
//  1. Read [frame.HeaderSizeLegacy] (44) bytes — enough to see the version byte
//     and, for BRC-12, the PayLen field.
//  2. If FrameVer == BRC-124/BRC-128: read 48 more bytes to complete the 92-byte header
//     (bytes 44–91), which includes the 4-byte PayLen field at bytes 88–91.
//  3. Read PayLen bytes of payload.
//  4. Forward assembled frame to [forwarder.Forwarder.Process].
//
// A [bufio.Reader] (64 KiB) absorbs kernel round-trips under burst load.
// BRC-12, BRC-124, and BRC-128 frames are forwarded verbatim.
package worker

import (
	"bufio"
	"fmt"
	"io"
	"log/slog"
	"net"
	"sync"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/forwarder"
	"github.com/lightwebinc/shard-proxy/metrics"
)

const tcpBufSize = 64 * 1024 // 64 KiB read buffer per TCP connection

// TCPIngress listens for TCP connections carrying a stream of BRC-12, BRC-124, or BRC-128 frames
// and forwards each frame via the shared [forwarder.Forwarder].
type TCPIngress struct {
	fwd      *forwarder.Forwarder
	ifaces   []*net.Interface
	rec      *metrics.Recorder
	log      *slog.Logger
	class    forwarder.IngressClass
	via      forwarder.EgressWriteFunc
	viaFlush func() int
}

// NewTCPIngress constructs a TCPIngress. No sockets are opened until [Run] is
// called.
func NewTCPIngress(fwd *forwarder.Forwarder, ifaces []*net.Interface, rec *metrics.Recorder) *TCPIngress {
	return &TCPIngress{
		fwd:    fwd,
		ifaces: ifaces,
		rec:    rec,
		log:    slog.Default().With("component", "tcp-ingress"),
	}
}

// SetIngressClass sets the frame-class gate for this TCP listener. The zero
// value ([forwarder.IngressPrivileged]) accepts every frame class;
// [forwarder.IngressTransaction] rejects block/coinbase/subtree-data frames.
// Call before [Run].
func (ti *TCPIngress) SetIngressClass(c forwarder.IngressClass) {
	ti.class = c
}

// SetFlushVia replaces the default kernel-multicast egress flush with
// [forwarder.Egress.FlushVia] through fn, followed by flush (which reports the
// frames written). Used by an ingress-mode proxy to ship admitted lane frames
// to its spine over a reliable pipeline instead of emitting multicast locally.
// Call before [Run]. flush may be nil.
func (ti *TCPIngress) SetFlushVia(fn forwarder.EgressWriteFunc, flush func() int) {
	ti.via, ti.viaFlush = fn, flush
}

// flushEgr drains egr by the configured egress path: FlushVia + the sink's
// flush when SetFlushVia was called, the kernel multicast Flush otherwise.
func (ti *TCPIngress) flushEgr(egr *forwarder.Egress) {
	if egr == nil {
		return
	}
	if ti.via != nil {
		egr.FlushVia(ti.via)
		if ti.viaFlush != nil {
			ti.viaFlush()
		}
		return
	}
	egr.Flush()
}

// Run starts the TCP accept loop on listenAddr:listenPort. It blocks until
// done is closed. Each accepted connection is handled in its own goroutine.
func (ti *TCPIngress) Run(listenAddr string, listenPort int, done <-chan struct{}) error {
	addr := fmt.Sprintf("%s:%d", listenAddr, listenPort)
	ln, err := net.Listen("tcp6", addr)
	if err != nil {
		return fmt.Errorf("tcp-ingress: listen %s: %w", addr, err)
	}

	// Close the listener when done is signalled, unblocking Accept.
	go func() {
		<-done
		_ = ln.Close()
	}()

	if ti.rec != nil {
		ti.rec.TCPIngressReady()
	}
	ti.log.Info("TCP ingress ready", "addr", ln.Addr())

	// Open a set of egress targets shared by all connections on this goroutine.
	// Worker 0 ownership is assumed (TCP ingress is a single listener).
	targets, err := ti.fwd.OpenTargets(ti.ifaces, true)
	if err != nil {
		return fmt.Errorf("tcp-ingress: open targets: %w", err)
	}
	defer forwarder.CloseTargets(targets, ti.log)

	var connWG sync.WaitGroup
	defer connWG.Wait()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if isClosedErr(err) {
				return nil
			}
			ti.log.Warn("Accept error", "err", err)
			continue
		}
		if ti.rec != nil {
			ti.rec.TCPConnectionAccepted()
		}
		connWG.Add(1)
		go func() {
			defer connWG.Done()
			go func() {
				<-done
				_ = conn.Close()
			}()
			// Each connection owns its own Egress so the shared multicast
			// egress sockets are not mutated concurrently across goroutines.
			// TCP is reliability-oriented rather than throughput-oriented,
			// so we flush per-frame to keep latency low instead of
			// accumulating a batch.
			egr := forwarder.NewEgress(ti.fwd, targets, 1, ti.rec)
			defer ti.flushEgr(egr)
			ti.handleConn(conn, egr)
		}()
	}
}

// handleConn reads a stream of BRC-12, BRC-124, or BRC-128 frames from conn
// and forwards each via the per-connection Egress. The connection is closed
// on any read error or protocol violation. Each goroutine owns its own
// encode and assembly buffers; egr is flushed once per frame so latency
// for reliable-delivery senders is not held up by batching.
func (ti *TCPIngress) handleConn(conn net.Conn, egr *forwarder.Egress) {
	defer func() { _ = conn.Close() }()
	remote := conn.RemoteAddr()
	ti.log.Debug("TCP connection accepted", "remote", remote)

	br := bufio.NewReaderSize(conn, tcpBufSize)
	hdrBuf := make([]byte, frame.HeaderSize)

	for {
		// Step 1: read the minimum header (44 bytes). This covers both
		// BRC-12 (complete header) and the leading 44 bytes of a BRC-124/BRC-128 header.
		if _, err := io.ReadFull(br, hdrBuf[:frame.HeaderSizeLegacy]); err != nil {
			if err != io.EOF && !isClosedErr(err) {
				ti.log.Debug("TCP read header error", "remote", remote, "err", err)
			}
			return
		}

		// Validate magic before reading further.
		if hdrBuf[0] != 0xE3 || hdrBuf[1] != 0xE1 ||
			hdrBuf[2] != 0xF3 || hdrBuf[3] != 0xE8 {
			ti.log.Warn("TCP bad magic; closing connection", "remote", remote)
			return
		}

		var hdrSize, payLen int
		switch hdrBuf[6] {
		case frame.MsgTypeSubtreeGroupAnnounce:
			// BRC-127 SubtreeGroupAnnounce: 64-byte fixed datagram.
			// 44 bytes already read; read the remaining 20 bytes.
			var ctrlBuf [frame.SubtreeGroupAnnounceSize]byte
			copy(ctrlBuf[:frame.HeaderSizeLegacy], hdrBuf[:frame.HeaderSizeLegacy])
			if _, err := io.ReadFull(br, ctrlBuf[frame.HeaderSizeLegacy:frame.SubtreeGroupAnnounceSize]); err != nil {
				ti.log.Debug("TCP read SubtreeGroupAnnounce extension error", "remote", remote, "err", err)
				return
			}
			if ti.rec != nil {
				ti.rec.TCPBytesReceived(frame.SubtreeGroupAnnounceSize)
			}
			ti.fwd.ForwardControl(egr, ctrlBuf[:], shard.GroupSubtreeGroupAnnounce, ti.fwd.EgressPort())
			ti.flushEgr(egr)
			continue
		case frame.FrameVerV1:
			hdrSize = frame.HeaderSizeLegacy
			payLen = int(uint32(hdrBuf[40])<<24 | uint32(hdrBuf[41])<<16 |
				uint32(hdrBuf[42])<<8 | uint32(hdrBuf[43]))
		case frame.FrameVerV2, frame.FrameVerV4, frame.FrameVerV5, frame.FrameVerV6:
			// Step 2: read the remaining 48 bytes to complete the 92-byte header
			// (BRC-124/BRC-128, BRC-131 block control, or BRC-132 subtree data;
			// includes PayLen at bytes 88–91).
			if _, err := io.ReadFull(br, hdrBuf[frame.HeaderSizeLegacy:frame.HeaderSize]); err != nil {
				ti.log.Debug("TCP read header extension error", "remote", remote, "err", err)
				return
			}
			hdrSize = frame.HeaderSize
			payLen = int(uint32(hdrBuf[88])<<24 | uint32(hdrBuf[89])<<16 |
				uint32(hdrBuf[90])<<8 | uint32(hdrBuf[91]))
		default:
			ti.log.Warn("TCP unsupported frame version; closing connection",
				"remote", remote, "ver", hdrBuf[6])
			return
		}

		// Step 3: allocate frame buffer and read payload.
		frameBuf := make([]byte, hdrSize+payLen)
		copy(frameBuf, hdrBuf[:hdrSize])
		if payLen > 0 {
			if _, err := io.ReadFull(br, frameBuf[hdrSize:]); err != nil {
				ti.log.Debug("TCP read payload error", "remote", remote, "err", err)
				return
			}
		}

		if ti.rec != nil {
			ti.rec.TCPBytesReceived(hdrSize + payLen)
		}
		// The full frame is already read off the stream, so a class
		// rejection (block/coinbase/subtree on a transaction-only socket)
		// drops it without corrupting stream framing. DispatchClass is the
		// single routing authority shared with the UDP path.
		ti.fwd.DispatchClass(egr, frameBuf, remote, -1, ti.class)
		// TCP path: flush per frame to keep reliable-delivery latency low.
		ti.flushEgr(egr)
	}
}
