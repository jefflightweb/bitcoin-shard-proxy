package worker

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-proxy/forwarder"
)

// makeLoopbackEgress builds a one-target Egress over a loopback UDP socket so
// FlushVia has queued messages to drain. The socket never receives (FlushVia
// bypasses the kernel write path entirely).
func makeLoopbackEgress(t *testing.T, fwd *forwarder.Forwarder) *forwarder.Egress {
	t.Helper()
	conn, err := net.ListenUDP("udp6", &net.UDPAddr{IP: net.ParseIP("::1")})
	if err != nil {
		t.Fatalf("loopback socket: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	targets := []forwarder.Target{{Iface: &net.Interface{Index: 1, Name: "lo"}, Conn: conn}}
	return forwarder.NewEgress(fwd, targets, 1, nil)
}

// TestTCPIngressFlushVia verifies SetFlushVia routes an admitted framed
// transaction through the sink (the ingress-mode pipeline seam) instead of the
// kernel multicast write, and that the sink flush hook runs.
func TestTCPIngressFlushVia(t *testing.T) {
	fwd := makeTestForwarder()
	ti := NewTCPIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil)

	var sunk [][]byte
	flushed := 0
	ti.SetFlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sunk = append(sunk, append([]byte(nil), raw...))
		return nil
	}, func() int { flushed++; return len(sunk) })

	// One framed V2 transaction, unstamped.
	payload := []byte{0x01, 0, 0, 0, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}
	f := &frame.Frame{Payload: payload}
	f.TxID[0] = 0x42
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}

	egr := makeLoopbackEgress(t, fwd)
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { ti.handleConn(srv, egr); close(done) }()
	go func() { _, _ = cli.Write(buf[:n]); _ = cli.Close() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn hang")
	}

	if len(sunk) != 1 {
		t.Fatalf("sink got %d frames, want 1", len(sunk))
	}
	if flushed == 0 {
		t.Fatal("sink flush hook never ran")
	}
	if got := sunk[0]; got[8] != 0x42 || binary.BigEndian.Uint64(got[48:56]) == 0 {
		t.Fatalf("sunk frame not the stamped tx: txid0=0x%02x seq=%d", got[8], binary.BigEndian.Uint64(got[48:56]))
	}
}

// TestObjectIngressFlushVia verifies a reframed BRC-143 subtree push object
// exits through the FlushVia sink as a stamped BRC-132 frame.
func TestObjectIngressFlushVia(t *testing.T) {
	fwd := makeTestForwarder()
	oi := NewObjectIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil, objfmt.ClassSubtree)

	var sunk [][]byte
	oi.SetFlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sunk = append(sunk, append([]byte(nil), raw...))
		return nil
	}, nil)

	egr := makeLoopbackEgress(t, fwd)
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { oi.handleConn(srv, egr); close(done) }()
	go func() { _, _ = cli.Write(buildSubtreeObj(3)); _ = cli.Close() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn hang")
	}

	if len(sunk) != 1 {
		t.Fatalf("sink got %d frames, want 1", len(sunk))
	}
	if got := sunk[0]; got[6] != frame.FrameVerV5 || binary.BigEndian.Uint64(got[48:56]) == 0 {
		t.Fatalf("sunk frame not a stamped BRC-132: ver=0x%02x seq=%d", got[6], binary.BigEndian.Uint64(got[48:56]))
	}
}
