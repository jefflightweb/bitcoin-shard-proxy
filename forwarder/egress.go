// Package forwarder — egress.go implements the per-worker outbound message
// batcher. Each Process*/ForwardControl call enqueues outbound datagrams
// into an Egress; the worker calls [Egress.Flush] at the end of each receive
// batch, which dispatches one ipv6.PacketConn.WriteBatch (sendmmsg on Linux)
// per target. This amortises the egress syscall cost across the entire
// receive batch instead of paying it per packet per interface.
//
// An Egress is owned by a single worker goroutine and is not safe for
// concurrent use.
package forwarder

import (
	"errors"
	"log/slog"
	"net"
	"sync"
	"syscall"
	"time"

	"golang.org/x/net/ipv6"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/logging"
	"github.com/lightwebinc/shard-proxy/metrics"
)

// Egress queues outbound multicast datagrams for one batch and emits them
// via WriteBatch (sendmmsg on Linux) on Flush. Append references the caller's
// payload bytes directly — the bytes must remain valid until the next Flush
// returns. The verbatim forwarding path feeds bytes from a receive batch
// whose buffers are reused only after Flush; fragment-path payloads come
// from a per-Egress sync.Pool and are released back at Flush time.
// maxGroupCache bounds the per-group destination-address caches. shardBits is
// capped at 12, so group indices occupy [0, 4096). Out-of-range indices fall
// back to per-packet allocation (never hit in practice).
const maxGroupCache = 1 << 12

type Egress struct {
	targets []Target
	pcs     []*ipv6.PacketConn

	// msgs[i] holds the queue of outbound datagrams for targets[i]. All
	// queues grow in lock-step: every Enqueue appends one entry to every
	// target's queue plus one entry to meta. len(msgs[i]) == len(meta) for
	// every i between Enqueue and Flush.
	msgs [][]ipv6.Message
	meta []msgMeta

	// pooledBufs collects fragment buffers from EnqueueDataPooled /
	// EnqueueControlPooled. The slice is drained at Flush, returning each
	// backing buffer to pool exactly once regardless of target count.
	pooledBufs []*[]byte

	// addrCache[i][groupIdx] is the per-target destination *net.UDPAddr
	// (IP+Port+Zone) for a data frame in group groupIdx. The multicast
	// address is a pure function of (mcPrefix, groupID, groupIdx, port) and
	// the Zone is the fixed target iface, so a frame's destination never
	// changes for a given (target, group) — even across resharding/bridging
	// (which alter the group *index* a txid maps to, not the address of an
	// index). Lazily filled on first sight, eliminating a per-packet
	// per-target net.UDPAddr allocation on the egress hot path. Control
	// frames (distinct dst, no group) bypass the cache.
	addrCache [][]*net.UDPAddr

	pool     *sync.Pool
	poolSize int

	log *slog.Logger
	rec *metrics.Recorder

	// throttle bounds category-8 OS/NIC egress-error logs to log-once-then-count
	// per interface, so a sustained ENOBUFS storm cannot itself become an outage.
	throttle *logging.Throttle
}

// msgMeta carries the per-message attributes needed to fire metrics at Flush
// time. Stored once per Enqueue and reused across all targets.
type msgMeta struct {
	groupIdx  uint32
	size      int
	workerID  int
	ctrlLabel string // non-empty → ControlFrameForwarded; empty → PacketForwarded
}

// NewEgress constructs an Egress bound to the given targets. batchHint sets
// the initial capacity reservation for the per-target message queue; growth
// beyond this is dynamic. The Egress draws fragment buffer memory from a
// sync.Pool sized to the forwarder's BRC-130 fragment datagram capacity;
// fragmentation must be enabled on fw for the pool to be initialised.
func NewEgress(fw *Forwarder, targets []Target, batchHint int, rec *metrics.Recorder) *Egress {
	if batchHint < 1 {
		batchHint = 1
	}
	e := &Egress{
		targets:   targets,
		pcs:       make([]*ipv6.PacketConn, len(targets)),
		msgs:      make([][]ipv6.Message, len(targets)),
		meta:      make([]msgMeta, 0, batchHint),
		addrCache: make([][]*net.UDPAddr, len(targets)),
		log:       slog.Default().With("component", "egress"),
		rec:       rec,
		throttle:  logging.NewThrottle(5 * time.Second),
	}
	for i, tgt := range targets {
		if tgt.PC != nil {
			e.pcs[i] = tgt.PC
		} else {
			e.pcs[i] = ipv6.NewPacketConn(tgt.Conn)
		}
		e.msgs[i] = make([]ipv6.Message, 0, batchHint)
		e.addrCache[i] = make([]*net.UDPAddr, maxGroupCache)
	}
	if fw != nil && fw.fragDataSize > 0 {
		e.poolSize = frame.HeaderSizeV3 + fw.fragDataSize
		e.pool = &sync.Pool{
			New: func() any {
				b := make([]byte, e.poolSize)
				return &b
			},
		}
	}
	return e
}

// Targets returns the underlying target slice. Used by the worker for
// shutdown lifecycle; do not mutate.
func (e *Egress) Targets() []Target { return e.targets }

// PoolGet returns a fragment-sized buffer from the pool, or nil if
// fragmentation is disabled on the forwarder. Caller passes the returned
// pointer to EnqueueDataPooled/EnqueueControlPooled so the buffer is
// recycled at Flush.
func (e *Egress) PoolGet() *[]byte {
	if e.pool == nil {
		return nil
	}
	return e.pool.Get().(*[]byte)
}

// EnqueueData queues raw for fan-out to every target with destination dst.
// raw must remain valid until the next Flush call. groupIdx and size are
// captured for the PacketForwarded metric at Flush time.
func (e *Egress) EnqueueData(raw []byte, dst net.UDPAddr, groupIdx uint32, workerID int) {
	e.enqueue(raw, dst, msgMeta{
		groupIdx: groupIdx,
		size:     len(raw),
		workerID: workerID,
	}, nil)
}

// EnqueueDataPooled is EnqueueData where raw was obtained via PoolGet. The
// backing buffer (passed by pointer to the original slice) is returned to
// the pool after Flush completes.
func (e *Egress) EnqueueDataPooled(raw []byte, dst net.UDPAddr, groupIdx uint32, workerID int, pooled *[]byte) {
	e.enqueue(raw, dst, msgMeta{
		groupIdx: groupIdx,
		size:     len(raw),
		workerID: workerID,
	}, pooled)
}

// EnqueueControl queues raw for fan-out to every target as a control-plane
// datagram. label is the metrics.ControlFrameForwarded label fired per
// target at Flush.
func (e *Egress) EnqueueControl(raw []byte, dst net.UDPAddr, label string, workerID int) {
	e.enqueue(raw, dst, msgMeta{
		size:      len(raw),
		workerID:  workerID,
		ctrlLabel: label,
	}, nil)
}

// EnqueueControlPooled is EnqueueControl with a pool-recycled backing buffer.
func (e *Egress) EnqueueControlPooled(raw []byte, dst net.UDPAddr, label string, workerID int, pooled *[]byte) {
	e.enqueue(raw, dst, msgMeta{
		size:      len(raw),
		workerID:  workerID,
		ctrlLabel: label,
	}, pooled)
}

func (e *Egress) enqueue(raw []byte, dst net.UDPAddr, m msgMeta, pooled *[]byte) {
	e.meta = append(e.meta, m)
	if pooled != nil {
		e.pooledBufs = append(e.pooledBufs, pooled)
	}
	// Data frames address a stable per-(target, group) multicast destination,
	// so cache it; control frames carry an arbitrary dst and always build fresh.
	cacheable := m.ctrlLabel == "" && m.groupIdx < maxGroupCache
	for i := range e.targets {
		var addr *net.UDPAddr
		if cacheable {
			if addr = e.addrCache[i][m.groupIdx]; addr == nil {
				addr = &net.UDPAddr{IP: dst.IP, Port: dst.Port, Zone: e.targets[i].Iface.Name}
				e.addrCache[i][m.groupIdx] = addr
			}
		} else {
			addr = &net.UDPAddr{IP: dst.IP, Port: dst.Port, Zone: e.targets[i].Iface.Name}
		}
		e.msgs[i] = append(e.msgs[i], ipv6.Message{
			Buffers: [][]byte{raw},
			Addr:    addr,
		})
	}
}

// Flush writes all queued messages to each target via WriteBatch (sendmmsg
// on Linux; per-packet fallback elsewhere). Per-target write errors are
// recorded as EgressError; messages beyond WriteBatch's sent-count fire
// PacketDropped with reason "write_error". Pool buffers are released exactly
// once each before return.
func (e *Egress) Flush() {
	for i := range e.targets {
		if len(e.msgs[i]) == 0 {
			continue
		}
		sent, err := e.pcs[i].WriteBatch(e.msgs[i], 0)
		if err != nil {
			e.logWriteError(i, len(e.msgs[i]), sent, err)
		}
		e.recordWrite(i, sent, err)
		// Clear slice contents to drop references to pooled buffers /
		// net.UDPAddrs so they can be GC'd or reused safely.
		clear(e.msgs[i])
		e.msgs[i] = e.msgs[i][:0]
	}
	for _, p := range e.pooledBufs {
		e.pool.Put(p)
	}
	e.pooledBufs = e.pooledBufs[:0]
	e.meta = e.meta[:0]
}

// logWriteError emits a category-8 (OS/NIC) log for a WriteBatch failure,
// classifying the kernel errno (ENOBUFS = socket buffer / qdisc backpressure)
// and reporting how many datagrams were dropped. It is throttled per interface
// to log-once-then-count so a sustained error storm cannot flood the log.
func (e *Egress) logWriteError(targetIdx, queued, sent int, err error) {
	iface := e.targets[targetIdx].Iface.Name
	emit, suppressed := e.throttle.Allow(iface)
	if !emit {
		return
	}
	dropped := queued
	if sent > 0 {
		dropped = queued - sent
	}
	attrs := []any{
		"iface", iface,
		"queued", queued,
		"dropped", dropped,
		"err", err,
		"suppressed", suppressed,
	}
	if errno, ok := errnoOf(err); ok {
		attrs = append(attrs, "errno", errno.Error(), "syscall", "sendmmsg")
		if errors.Is(err, syscall.ENOBUFS) {
			e.log.Warn("egress ENOBUFS: kernel send buffer / qdisc backpressure", attrs...)
			return
		}
	}
	e.log.Warn("egress write error", attrs...)
}

// errnoOf extracts a syscall.Errno from a (possibly wrapped) error.
func errnoOf(err error) (syscall.Errno, bool) {
	var errno syscall.Errno
	if errors.As(err, &errno) {
		return errno, true
	}
	return 0, false
}

// recordWrite fires the per-target metrics for one WriteBatch result. sent
// is the count returned by WriteBatch; meta[0:sent] count as forwarded,
// meta[sent:] as write-error drops. WriteBatch returns -1 (not 0) when the
// underlying sendmmsg fails before any message is written, so sent is
// clamped to [0, len(meta)] to keep both loops in bounds.
func (e *Egress) recordWrite(targetIdx, sent int, err error) {
	if e.rec == nil {
		return
	}
	if sent < 0 {
		sent = 0
	} else if sent > len(e.meta) {
		sent = len(e.meta)
	}
	iface := e.targets[targetIdx].Iface.Name
	if err != nil {
		// Match the legacy behaviour: one EgressError per failing target.
		e.rec.EgressError(iface, 0)
	}
	for j := 0; j < sent && j < len(e.meta); j++ {
		m := e.meta[j]
		if m.ctrlLabel != "" {
			e.rec.ControlFrameForwarded(m.ctrlLabel)
		} else {
			e.rec.PacketForwarded(iface, m.workerID, m.groupIdx, m.size)
		}
	}
	for j := sent; j < len(e.meta); j++ {
		m := e.meta[j]
		e.rec.PacketDropped(iface, m.workerID, "write_error")
	}
}
