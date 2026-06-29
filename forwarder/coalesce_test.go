package forwarder

import (
	"bytes"
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/bundle"
	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/shard"
)

// drain captures the datagrams currently queued on egr (one entry per enqueued
// datagram for a single-target Egress) by routing Flush through a capture func.
func drain(egr *Egress) [][]byte {
	var sent [][]byte
	egr.FlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sent = append(sent, append([]byte(nil), raw...))
		return nil
	})
	return sent
}

func TestCoalesce_BuffersAndPacksBundle(t *testing.T) {
	engine := shard.New(0xFF05, shard.DefaultGroupID, 8)
	fw := New(engine, 0xFF05, shard.DefaultGroupID, 9001, false, nil)
	fw.SetCoalesce(true, 9000, 0, true) // carry TxIDs

	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	if egr.coal == nil {
		t.Fatal("coal buffer not created when coalescing enabled")
	}

	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	payloads := [][]byte{[]byte("tx-one"), []byte("tx-two"), []byte("tx-three")}
	for i, p := range payloads {
		raw := buildV2Frame(t, 0xAB, 0, p) // same top byte → same group; zero subtree
		raw[9] = byte(i + 1)               // unique TxID beyond byte 0
		fw.Process(egr, raw, src, 0)
	}

	// Buffered, not yet enqueued.
	if len(egr.coal.buckets) != 1 {
		t.Fatalf("buckets = %d, want 1", len(egr.coal.buckets))
	}
	if len(egr.coal.buckets[0].members) != 3 {
		t.Fatalf("members = %d, want 3", len(egr.coal.buckets[0].members))
	}
	if got := drain(egr); len(got) != 0 {
		t.Fatalf("nothing should be enqueued before FlushCoalesced, got %d", len(got))
	}

	fw.FlushCoalesced(egr, 0)
	sent := drain(egr)
	if len(sent) != 1 {
		t.Fatalf("sent %d datagrams, want 1 bundle", len(sent))
	}
	if !frame.IsBundle(sent[0]) {
		t.Fatal("enqueued datagram is not a bundle")
	}
	b, err := bundle.Decode(sent[0])
	if err != nil {
		t.Fatalf("decode bundle: %v", err)
	}
	if len(b.Members) != 3 {
		t.Errorf("bundle members = %d, want 3", len(b.Members))
	}
	if b.SeqNum != 1 {
		t.Errorf("bundle SeqNum = %d, want 1", b.SeqNum)
	}
	if b.HashKey == 0 {
		t.Error("bundle HashKey = 0, want stamped")
	}
	if !b.TxIDsPresent() {
		t.Error("expected carried TxIDs")
	}
	want := engine.GroupIndex(&[32]byte{0xAB})
	if uint32(b.GroupIdx) != want {
		t.Errorf("bundle GroupIdx = %d, want %d", b.GroupIdx, want)
	}
	for i, m := range b.Members {
		if !bytes.Equal(m.Tx, payloads[i]) {
			t.Errorf("member %d payload = %q, want %q", i, m.Tx, payloads[i])
		}
	}
	if len(egr.coal.buckets) != 0 {
		t.Error("coal buffer not reset after flush")
	}
}

func TestCoalesce_DisabledByDefault(t *testing.T) {
	fw := makeForwarder() // no SetCoalesce
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	if egr.coal != nil {
		t.Fatal("coal buffer should be nil when coalescing disabled")
	}
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	fw.Process(egr, buildV2Frame(t, 0xAB, 0, []byte("p")), src, 0)
	fw.FlushCoalesced(egr, 0) // must be a no-op
	sent := drain(egr)
	if len(sent) != 1 {
		t.Fatalf("sent %d, want 1 (normal individual frame)", len(sent))
	}
	if frame.IsBundle(sent[0]) {
		t.Error("frame should be forwarded individually, not bundled")
	}
}

func TestCoalesce_OversizeMemberFallsThrough(t *testing.T) {
	fw := makeForwarder()
	fw.SetCoalesce(true, 1500, 0, false) // 1500 byte budget, fragmentation disabled
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	// A 1500-byte payload cannot fit a bundle (66 header + 2 + 1500 > 1500),
	// so it must take the normal individual-frame path, not be buffered.
	fw.Process(egr, buildV2Frame(t, 0xCD, 0, make([]byte, 1500)), src, 0)
	if len(egr.coal.buckets) != 0 {
		t.Fatalf("oversize member should not be buffered, buckets = %d", len(egr.coal.buckets))
	}
	sent := drain(egr)
	if len(sent) != 1 || frame.IsBundle(sent[0]) {
		t.Fatalf("oversize tx should be forwarded as one individual frame; sent=%d", len(sent))
	}
}

// batchHint=1 (the TCP ingress and legacy per-packet UDP paths) must NOT get a
// coal buffer, so Process forwards individually and no member can be stranded
// (those paths never call FlushCoalesced). Locks in the TCP exclusion.
func TestCoalesce_ExcludedWhenBatchCapOne(t *testing.T) {
	fw := makeForwarder()
	fw.SetCoalesce(true, 9000, 0, false)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 1, nil)
	if egr.coal != nil {
		t.Fatal("coal must be nil for batchHint=1 (TCP / per-packet path excluded from coalescing)")
	}
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	fw.Process(egr, buildV2Frame(t, 0xAB, 0, []byte("p")), src, 0)
	fw.FlushCoalesced(egr, -1) // no-op: coal is nil
	sent := drain(egr)
	if len(sent) != 1 || frame.IsBundle(sent[0]) {
		t.Fatalf("batchHint=1 frame must be forwarded individually (not stranded/bundled); sent=%d", len(sent))
	}
}

func TestCoalesce_SeqNumContiguousAcrossBatches(t *testing.T) {
	fw := makeForwarder()
	fw.SetCoalesce(true, 9000, 0, false)
	conn, _ := openLoopbackUDP(t)
	egr := NewEgress(fw, makeTargets(t, conn), 8, nil)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	seqOf := func() uint64 {
		fw.Process(egr, buildV2Frame(t, 0xEE, 0, []byte("a")), src, 0)
		fw.Process(egr, buildV2Frame(t, 0xEE, 0, []byte("b")), src, 0)
		fw.FlushCoalesced(egr, 0)
		sent := drain(egr)
		if len(sent) != 1 {
			t.Fatalf("want 1 bundle per batch, got %d", len(sent))
		}
		b, err := bundle.Decode(sent[0])
		if err != nil {
			t.Fatal(err)
		}
		return b.SeqNum
	}
	if s1, s2 := seqOf(), seqOf(); s1 != 1 || s2 != 2 {
		t.Errorf("bundle SeqNums across batches = %d,%d, want 1,2 (contiguous per flow)", s1, s2)
	}
}
