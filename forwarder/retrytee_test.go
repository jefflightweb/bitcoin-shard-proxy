package forwarder

import (
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/shard"
)

// The tee must deliver a byte-identical copy of every egressed DATA datagram to
// the local cache. Byte-identical matters: the retry endpoint keys its cache on
// (HashKey, SeqNum) read from the frame at fixed offsets, so anything other than
// the exact egressed bytes caches a frame that cannot answer the NACK that asks
// for it — worse than caching nothing.
func TestRetryTeeMirrorsDataFrames(t *testing.T) {
	sink, sinkAddr := openLoopbackUDP(t)
	// The retry endpoint binds the WILDCARD [::]:port and performs no multicast
	// join; loopback unicast reaches it. Reproduce that here.
	egrConn, _ := openLoopbackUDP(t)

	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	egr := NewEgress(fw, makeTargets(t, egrConn), 8, nil)

	teeTarget := net.JoinHostPort("::1", itoaPort(sinkAddr.Port))
	if err := egr.EnableRetryTee(teeTarget, 8); err != nil {
		t.Fatalf("EnableRetryTee: %v", err)
	}
	t.Cleanup(func() { _ = egr.CloseRetryTee() })

	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	want := [][]byte{
		[]byte("frame-one-payload-aaaaaaaaaaaaaaaa"),
		[]byte("frame-two-payload-bbbbbbbbbbbbbbbb"),
		[]byte("frame-three-payload-cccccccccccccc"),
	}
	for i, raw := range want {
		egr.EnqueueData(raw, dst, uint32(i), 0)
	}
	// A control frame must NOT be teed: the cache would count it a decode_error.
	egr.EnqueueControl([]byte("control-frame-should-not-be-teed"), dst, "beacon", 0)

	egr.Flush()

	got := drainSink(t, sink, len(want))
	if len(got) != len(want) {
		t.Fatalf("tee delivered %d datagrams, want %d (control frame must be excluded)",
			len(got), len(want))
	}
	for i := range want {
		if string(got[i]) != string(want[i]) {
			t.Errorf("tee copy %d = %q, want %q — a NACK for this frame would be answered "+
				"with the wrong bytes", i, got[i], want[i])
		}
	}
	if f := egr.RetryTeeFailed(); f != 0 {
		t.Errorf("tee reported %d failures on loopback", f)
	}
}

// A tee that cannot deliver must never disturb real egress: the cache is an
// optimisation, the forward path is not.
func TestRetryTeeFailureDoesNotBreakEgress(t *testing.T) {
	egrConn, _ := openLoopbackUDP(t)
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	egr := NewEgress(fw, makeTargets(t, egrConn), 8, nil)

	sink, sinkAddr := openLoopbackUDP(t)
	if err := egr.EnableRetryTee(net.JoinHostPort("::1", itoaPort(sinkAddr.Port)), 8); err != nil {
		t.Fatalf("EnableRetryTee: %v", err)
	}
	// Close the tee socket underneath it — every subsequent write fails.
	_ = egr.CloseRetryTee()

	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	egr.EnqueueData([]byte("payload-that-must-still-egress"), dst, 1, 0)

	// Must not panic and must complete.
	egr.Flush()
	_ = sink
}

// Disabled by default: no tee socket, no copies, zero behaviour change.
func TestRetryTeeDisabledByDefault(t *testing.T) {
	egrConn, _ := openLoopbackUDP(t)
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	egr := NewEgress(fw, makeTargets(t, egrConn), 8, nil)

	if egr.tee != nil {
		t.Fatal("retry tee must be opt-in")
	}
	dst := net.UDPAddr{IP: net.ParseIP("ff05::1"), Port: 9001}
	egr.EnqueueData([]byte("x"), dst, 1, 0)
	egr.Flush()
	if f := egr.RetryTeeFailed(); f != 0 {
		t.Errorf("disabled tee reported %d failures", f)
	}
}

func drainSink(t *testing.T, c *net.UDPConn, n int) [][]byte {
	t.Helper()
	var out [][]byte
	_ = c.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 4096)
	for len(out) < n {
		k, err := c.Read(buf)
		if err != nil {
			break
		}
		cp := make([]byte, k)
		copy(cp, buf[:k])
		out = append(out, cp)
	}
	// One more short read to catch an unexpected extra datagram (e.g. a control
	// frame that should have been excluded).
	_ = c.SetReadDeadline(time.Now().Add(150 * time.Millisecond))
	if k, err := c.Read(buf); err == nil {
		cp := make([]byte, k)
		copy(cp, buf[:k])
		out = append(out, cp)
	}
	return out
}

func itoaPort(p int) string {
	if p == 0 {
		return "0"
	}
	var b []byte
	for p > 0 {
		b = append([]byte{byte('0' + p%10)}, b...)
		p /= 10
	}
	return string(b)
}
