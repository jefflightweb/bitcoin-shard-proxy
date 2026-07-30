package forwarder

import (
	"fmt"
	"log/slog"
	"net"

	"golang.org/x/net/ipv6"
)

// retryTee mirrors every egressed DATA datagram to a local retry-endpoint cache
// over loopback unicast.
//
// Why this exists: a node must never (S,G)-join its own source — that installs an
// iif==oif mroute and originated frames loop until hop-limit death — so a retry
// endpoint co-located with an origin (the collapsed edge) caches nothing of its
// own node's emissions. It still ADVERTISES itself as a usable cache (BRC-126
// ADVERT is built from static config, with no cache-content gating), so listeners
// discover it, ask it, get MISS, and burn a tier hop before escalating. The tee
// makes that existing advertisement truthful.
//
// It needs NO retry-endpoint change: that ingress socket is an AF_INET6 SOCK_DGRAM
// bound to the WILDCARD [::]:port, which accepts loopback unicast with no
// multicast join, and its cache key is (HashKey, SeqNum) read from the frame bytes
// at fixed offsets — independent of how the datagram was delivered. A locally
// originated unicast copy therefore caches identically to a multicast one.
//
// Cost is one extra syscall per BATCH, not per frame: copies accumulate alongside
// the egress queue and go out as a single WriteBatch (sendmmsg) from Flush, the
// same cadence as the real egress. That is what keeps it viable next to AF_XDP —
// AF_XDP exists to avoid per-packet syscalls, and a per-frame sendto would negate
// it.
type retryTee struct {
	pc   *ipv6.PacketConn
	addr *net.UDPAddr
	msgs []ipv6.Message
	log  *slog.Logger

	// failed counts datagrams the tee could not deliver. A tee failure must never
	// affect real egress — the cache is an optimisation, the forward path is not —
	// so errors are counted and logged once, never propagated.
	failed uint64
	logged bool
}

// newRetryTee dials the local cache-ingest address. addr is host:port; the host
// should be loopback ([::1]) — a tee to a non-local address would put a full
// second copy of the stream on the wire.
func newRetryTee(addr string, batchHint int) (*retryTee, error) {
	ua, err := net.ResolveUDPAddr("udp6", addr)
	if err != nil {
		return nil, fmt.Errorf("retry tee: resolve %q: %w", addr, err)
	}
	if !ua.IP.IsLoopback() {
		// Not fatal — an operator may deliberately tee to a cache on an adjacent
		// host — but it doubles egress volume, so it must never happen silently.
		slog.Warn("retry tee target is not loopback; this duplicates the full egress "+
			"stream onto the network", "addr", addr)
	}
	c, err := net.ListenPacket("udp6", "[::]:0")
	if err != nil {
		return nil, fmt.Errorf("retry tee: listen: %w", err)
	}
	return &retryTee{
		pc:   ipv6.NewPacketConn(c),
		addr: ua,
		msgs: make([]ipv6.Message, 0, batchHint),
		log:  slog.Default().With("component", "retry-tee"),
	}, nil
}

// append queues one copy. raw must stay valid until flush returns — the same
// contract the egress queue already has.
func (t *retryTee) append(raw []byte) {
	t.msgs = append(t.msgs, ipv6.Message{Buffers: [][]byte{raw}, Addr: t.addr})
}

// flush dispatches the queued copies in ONE WriteBatch and resets the queue.
func (t *retryTee) flush() {
	if len(t.msgs) == 0 {
		return
	}
	sent, err := t.pc.WriteBatch(t.msgs, 0)
	if err != nil || sent < len(t.msgs) {
		missed := len(t.msgs) - sent
		if missed < 0 {
			missed = 0
		}
		t.failed += uint64(missed)
		if !t.logged {
			t.logged = true // log once; the counter carries the rest
			t.log.Warn("retry tee write incomplete — the co-located cache will MISS "+
				"for these frames and listeners will escalate to another tier",
				"queued", len(t.msgs), "sent", sent, "err", err)
		}
	}
	clear(t.msgs)
	t.msgs = t.msgs[:0]
}

// Failed reports datagrams the tee could not deliver.
func (t *retryTee) Failed() uint64 { return t.failed }

func (t *retryTee) close() error {
	if t.pc == nil {
		return nil
	}
	return t.pc.Close()
}
