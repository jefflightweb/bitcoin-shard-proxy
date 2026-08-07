package worker

import (
	"encoding/binary"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/frame"
)

// captureSink collects FlushVia output under a lock so tests can observe
// delivery WHILE the connection is still open (the liveness assertions below
// read mid-stream, unlike the close-then-assert tests).
type captureSink struct {
	mu   sync.Mutex
	sunk [][]byte
}

func (c *captureSink) fn(_ int, raw []byte, _ *net.UDPAddr) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.sunk = append(c.sunk, append([]byte(nil), raw...))
	return nil
}

func (c *captureSink) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.sunk)
}

func (c *captureSink) frames() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.sunk))
	copy(out, c.sunk)
	return out
}

func waitCount(t *testing.T, c *captureSink, want int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if c.count() >= want {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("sink has %d frames, want %d (batch stranded?)", c.count(), want)
}

func encodeTestTx(t *testing.T, txidByte byte) []byte {
	t.Helper()
	payload := []byte{0x01, 0, 0, 0, 0x00, 0x00, 0x00, 0x00, 0x00, 0xEF}
	f := &frame.Frame{Payload: payload}
	f.TxID[0] = txidByte
	buf := make([]byte, frame.HeaderSize+len(payload))
	n, err := frame.Encode(f, buf)
	if err != nil {
		t.Fatalf("Encode: %v", err)
	}
	return buf[:n]
}

func startCapturedConn(t *testing.T) (*captureSink, net.Conn, chan struct{}) {
	t.Helper()
	fwd := makeTestForwarder()
	ti := NewTCPIngress(fwd, []*net.Interface{{Index: 1, Name: "lo"}}, nil)
	sink := &captureSink{}
	ti.SetFlushVia(sink.fn, nil)
	egr := makeLoopbackEgress(t, fwd)
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() { ti.handleConn(srv, egr); ti.flushEgr(egr); close(done) }()
	return sink, cli, done
}

// More frames than one batch, streamed back-to-back: every frame must egress
// and per-flow SeqNums must stay strictly monotonic in egress order — the
// batched cadence changes WHEN flushes happen, never order or completeness.
func TestTCPBatchedCadenceDeliversAllFramesInOrder(t *testing.T) {
	const n = maxTCPBatch*3 + 7 // several full batches + a partial tail
	sink, cli, done := startCapturedConn(t)
	go func() {
		for i := 0; i < n; i++ {
			if _, err := cli.Write(encodeTestTx(t, byte(i))); err != nil {
				return
			}
		}
		_ = cli.Close()
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("handleConn hang")
	}
	frames := sink.frames()
	if len(frames) != n {
		t.Fatalf("egressed %d frames, want %d — batching lost frames", len(frames), n)
	}
	// SeqNum is per-FLOW (HashKey = sender ∥ group ∥ subtree, bytes 40:48):
	// distinct TxIDs shard to distinct groups, so monotonicity holds within
	// each flow, in egress order.
	lastSeq := map[uint64]uint64{}
	for i, fr := range frames {
		hk := binary.BigEndian.Uint64(fr[40:48])
		seq := binary.BigEndian.Uint64(fr[48:56])
		if seq <= lastSeq[hk] {
			t.Fatalf("frame %d flow %x: seq %d not monotonic after %d", i, hk, seq, lastSeq[hk])
		}
		lastSeq[hk] = seq
	}
}

// LIVENESS: one frame then an idle-but-open connection. The frame must egress
// promptly (flushingReader drains before the blocking header read) — not sit
// enqueued until more traffic or close.
func TestTCPSingleFrameThenIdleFlushes(t *testing.T) {
	sink, cli, done := startCapturedConn(t)
	if _, err := cli.Write(encodeTestTx(t, 0x01)); err != nil {
		t.Fatalf("write: %v", err)
	}
	waitCount(t, sink, 1) // must arrive while the connection stays open
	_ = cli.Close()
	<-done
}

// LIVENESS, partial-next-frame: a complete frame followed by a PARTIAL header
// in the same segment. The buffer is non-empty (so a Buffered()==0 gate would
// NOT fire) yet the next read blocks — the completed frame must still egress.
func TestTCPPartialNextFrameStillFlushes(t *testing.T) {
	sink, cli, done := startCapturedConn(t)
	full := encodeTestTx(t, 0x02)
	partial := encodeTestTx(t, 0x03)[:20] // 20 bytes of a 44-byte min header
	go func() { _, _ = cli.Write(append(append([]byte(nil), full...), partial...)) }()
	waitCount(t, sink, 1) // completed frame egresses despite buffered partial
	_ = cli.Close()
	<-done
}

// Two control frames in one batch must not alias: the enqueued reference
// outlives the loop iteration under batched cadence, so each must be a fresh
// heap buffer, not a reused stack slot.
func TestTCPControlFramesInOneBatchDoNotAlias(t *testing.T) {
	sink, cli, done := startCapturedConn(t)
	mk := func(tag byte) []byte {
		buf := make([]byte, frame.SubtreeGroupAnnounceSize)
		binary.BigEndian.PutUint32(buf[0:4], frame.MagicBSV)
		binary.BigEndian.PutUint16(buf[4:6], frame.ProtoVer)
		buf[6] = frame.MsgTypeSubtreeGroupAnnounce
		buf[50] = tag // distinct byte inside the announce body
		return buf
	}
	go func() {
		_, _ = cli.Write(append(mk(0xAA), mk(0xBB)...)) // one segment, one batch
		_ = cli.Close()
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("handleConn hang")
	}
	frames := sink.frames()
	if len(frames) != 2 {
		t.Fatalf("egressed %d control frames, want 2", len(frames))
	}
	if frames[0][50] != 0xAA || frames[1][50] != 0xBB {
		t.Fatalf("control frames aliased: got tags 0x%02X 0x%02X, want 0xAA 0xBB",
			frames[0][50], frames[1][50])
	}
}
