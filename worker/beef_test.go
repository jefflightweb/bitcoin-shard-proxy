package worker

import (
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/shard"
	"github.com/lightwebinc/shard-proxy/forwarder"
)

var beefLaneObj = []byte{0x01, 0x00, 0xBE, 0xEF, 0x11, 0x22, 0x33}

func makeBEEFTestForwarder(t *testing.T) *forwarder.Forwarder {
	t.Helper()
	fwd := makeTestForwarder()
	pe, err := shard.NewPlane(0xFF05, shard.DefaultGroupID, 4, shard.DomainBEEF)
	if err != nil {
		t.Fatalf("NewPlane: %v", err)
	}
	fwd.SetBEEF(pe, 1<<20)
	return fwd
}

func mustRecord(t *testing.T, topics []string) []byte {
	t.Helper()
	rec, err := objfmt.EncodeBEEFRecord(topics, beefLaneObj)
	if err != nil {
		t.Fatalf("EncodeBEEFRecord: %v", err)
	}
	return rec
}

// runTCPConn drives TCPIngress.handleConn (grammar auto-detect) over a
// net.Pipe and returns every admitted frame from the FlushVia sink.
func runTCPConn(t *testing.T, stream []byte) [][]byte {
	t.Helper()
	fwd := makeBEEFTestForwarder(t)
	ti := NewTCPIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil)
	var sunk [][]byte
	ti.SetFlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sunk = append(sunk, append([]byte(nil), raw...))
		return nil
	}, nil)
	egr := makeLoopbackEgress(t, fwd)
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { ti.handleConn(srv, egr); close(done) }()
	go func() { _, _ = cli.Write(stream); _ = cli.Close() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = srv.Close()
		t.Fatal("handleConn did not return")
	}
	return sunk
}

// TestTCPIngressBEEFRecordStream proves the tx port's third grammar: a
// 0xBEEF-tagged record stream splits per record and expands per topic into
// stamped FrameVer 0x09 frames.
func TestTCPIngressBEEFRecordStream(t *testing.T) {
	stream := append(mustRecord(t, []string{"tm_a"}), mustRecord(t, []string{"tm_b", "tm_c"})...)
	sunk := runTCPConn(t, stream)
	if len(sunk) != 3 {
		t.Fatalf("admitted %d frames, want 3 (1 + 2 topics)", len(sunk))
	}
	for i, raw := range sunk {
		if !frame.IsBEEFFrame(raw) {
			t.Fatalf("frame %d is not FrameVer 0x09", i)
		}
		bf, err := frame.DecodeBEEF(raw)
		if err != nil || bf.SeqNum == 0 {
			t.Fatalf("frame %d not stamped: %v", i, err)
		}
	}
}

// TestTCPIngressFramedV9 proves a pre-framed BEEF object rides the framed
// TCP grammar (92-byte header family) unchanged.
func TestTCPIngressFramedV9(t *testing.T) {
	raw, err := objfmt.BEEFMulticastBytes(objfmt.TopicID("tm_framed"), beefLaneObj)
	if err != nil {
		t.Fatalf("BEEFMulticastBytes: %v", err)
	}
	sunk := runTCPConn(t, raw)
	if len(sunk) != 1 || !frame.IsBEEFFrame(sunk[0]) {
		t.Fatalf("framed V9 over TCP: admitted %d, want 1 BEEF frame", len(sunk))
	}
}

// runBEEFLane drives ObjectIngress.handleConn with ClassBEEF (the dedicated
// 8728 lane) and returns admitted frames.
func runBEEFLane(t *testing.T, stream []byte) [][]byte {
	t.Helper()
	fwd := makeBEEFTestForwarder(t)
	oi := NewObjectIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil, objfmt.ClassBEEF)
	var sunk [][]byte
	oi.SetFlushVia(func(_ int, raw []byte, _ *net.UDPAddr) error {
		sunk = append(sunk, append([]byte(nil), raw...))
		return nil
	}, nil)
	egr := makeLoopbackEgress(t, fwd)
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { oi.handleConn(srv, egr); close(done) }()
	go func() { _, _ = cli.Write(stream); _ = cli.Close() }()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		_ = srv.Close()
		t.Fatal("handleConn did not return")
	}
	return sunk
}

// TestObjectIngressBEEFLane proves the dedicated lane splits records and
// expands topics like the shared port.
func TestObjectIngressBEEFLane(t *testing.T) {
	stream := append(mustRecord(t, []string{"tm_x", "tm_y"}), mustRecord(t, []string{"tm_z"})...)
	sunk := runBEEFLane(t, stream)
	if len(sunk) != 3 {
		t.Fatalf("beef lane admitted %d frames, want 3", len(sunk))
	}
}

// TestObjectIngressBEEFLaneRejectsBareTx proves the single-class lane drops
// a non-record stream instead of admitting it as transactions.
func TestObjectIngressBEEFLaneRejectsBareTx(t *testing.T) {
	if sunk := runBEEFLane(t, bareEFTx()); len(sunk) != 0 {
		t.Fatalf("beef lane admitted %d frames from a bare tx stream, want 0", len(sunk))
	}
}
