package forwarder

import (
	"fmt"
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/objfmt"
	"github.com/lightwebinc/shard-common/seqhash"
	"github.com/lightwebinc/shard-common/shard"
)

var beefTestObj = []byte{0x01, 0x00, 0xBE, 0xEF, 0xAA, 0xBB, 0xCC, 0xDD}

func makeBEEFForwarder(t *testing.T) (*Forwarder, *shard.PlaneEngine) {
	t.Helper()
	fw := makeForwarder()
	pe, err := shard.NewPlane(0xFF05, shard.DefaultGroupID, 4, shard.DomainBEEF)
	if err != nil {
		t.Fatalf("NewPlane: %v", err)
	}
	fw.SetBEEF(pe, 1<<20)
	return fw, pe
}

func buildBEEFRecordBytes(t *testing.T, topics []string, obj []byte) []byte {
	t.Helper()
	rec, err := objfmt.EncodeBEEFRecord(topics, obj)
	if err != nil {
		t.Fatalf("EncodeBEEFRecord: %v", err)
	}
	return rec
}

func buildBEEFFrameBytes(t *testing.T, topic string, obj []byte) []byte {
	t.Helper()
	raw, err := objfmt.BEEFMulticastBytes(objfmt.TopicID(topic), obj)
	if err != nil {
		t.Fatalf("BEEFMulticastBytes: %v", err)
	}
	return raw
}

// captureEnqueued drains egr, returning each queued datagram (copied) with
// its destination.
func captureEnqueued(egr *Egress) (frames [][]byte, dsts []*net.UDPAddr) {
	egr.FlushVia(func(_ int, b []byte, addr *net.UDPAddr) error {
		frames = append(frames, append([]byte(nil), b...))
		dsts = append(dsts, addr)
		return nil
	})
	return frames, dsts
}

// TestDispatchClass_BEEFOpenClass proves BEEF input (framed V9 and bare
// submission records) is admitted on every ingress class — BEEF is an open
// class, unlike the miner-gated subtree/block lanes.
func TestDispatchClass_BEEFOpenClass(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	framed := func(t *testing.T) []byte { return buildBEEFFrameBytes(t, "tm_open", beefTestObj) }
	record := func(t *testing.T) []byte { return buildBEEFRecordBytes(t, []string{"tm_open"}, beefTestObj) }

	for _, class := range []IngressClass{IngressPrivileged, IngressTransaction, IngressBEEF} {
		for name, build := range map[string]func(*testing.T) []byte{"framed": framed, "record": record} {
			t.Run(fmt.Sprintf("%s/class_%d", name, class), func(t *testing.T) {
				fw, _ := makeBEEFForwarder(t)
				conn, _ := openLoopbackUDP(t)
				egr := makeEgress(t, fw, conn)
				fw.DispatchClass(egr, build(t), src, 0, class)
				if got := countEnqueued(egr); got != 1 {
					t.Errorf("class %d %s: enqueued %d, want 1", class, name, got)
				}
			})
		}
	}
}

// TestDispatchClass_BEEFLaneRejectsOthers proves the dedicated lane is
// single-class: non-BEEF grammars and frame classes drop.
func TestDispatchClass_BEEFLaneRejectsOthers(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	cases := map[string][]byte{
		"brc124_tx":     buildV2Frame(t, 0xAB, 0, []byte("tx")),
		"brc134_anchor": buildAnchorFrame(t, 0xAB, 0, []byte("anchor")),
		"bare_tx_bytes": {0x01, 0x00, 0x00, 0x00, 0x00},
	}
	for name, raw := range cases {
		fw, _ := makeBEEFForwarder(t)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.DispatchClass(egr, raw, src, 0, IngressBEEF)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%s on BEEF lane: enqueued %d, want 0", name, got)
		}
	}
}

// TestSubmitBEEF_Expansion proves one multi-topic record expands into one
// stamped frame per topic, siblings sharing ContentID, each addressed into
// the 0x1000 plane band.
func TestSubmitBEEF_Expansion(t *testing.T) {
	fw, pe := makeBEEFForwarder(t)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	topics := []string{"tm_alpha", "tm_beta"}
	fw.SubmitBEEF(egr, buildBEEFRecordBytes(t, topics, beefTestObj), src, 0)

	frames, _ := captureEnqueued(egr)
	if len(frames) != 2 {
		t.Fatalf("enqueued %d frames, want 2 (one per topic)", len(frames))
	}
	wantContent := objfmt.ContentID(beefTestObj)
	for i, raw := range frames {
		bf, err := frame.DecodeBEEF(raw)
		if err != nil {
			t.Fatalf("frame %d: %v", i, err)
		}
		if bf.ContentID != wantContent {
			t.Errorf("frame %d: sibling ContentID diverged", i)
		}
		if bf.TopicID != objfmt.TopicID(topics[i]) {
			t.Errorf("frame %d: TopicID mismatch", i)
		}
		if bf.SeqNum == 0 || bf.HashKey == 0 {
			t.Errorf("frame %d: not stamped (HashKey %X SeqNum %d)", i, bf.HashKey, bf.SeqNum)
		}
		if idx := pe.GroupIndex(&bf.TopicID); idx < 0x1000 || idx > 0x1FFF {
			t.Errorf("frame %d: group 0x%04X outside the BEEF band", i, idx)
		}
	}
}

// TestSubmitBEEF_Rejects covers the admission bounds: bad grammar, oversize
// object, unknown leading marker.
func TestSubmitBEEF_Rejects(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	good := buildBEEFRecordBytes(t, []string{"tm_x"}, beefTestObj)
	badVer := append([]byte(nil), good...)
	badVer[2] = 0x7F

	badMarker := buildBEEFRecordBytes(t, []string{"tm_x"}, []byte{0xDE, 0xAD, 0xBE, 0xAA, 0x01})

	cases := map[string]struct {
		rec      []byte
		maxBytes int
	}{
		"malformed":  {badVer, 1 << 20},
		"bad_marker": {badMarker, 1 << 20},
		"oversize":   {good, 4},
	}
	for name, c := range cases {
		fw, _ := makeBEEFForwarder(t)
		fw.beefMaxObject = c.maxBytes
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.SubmitBEEF(egr, c.rec, src, 0)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("%s: enqueued %d, want 0", name, got)
		}
	}
}

// TestProcessBEEF_ZeroIngredientFlow is the D4 spec regression: BEEF flows
// are per (sender, group) with a ZERO 32-byte HashKey ingredient — two
// topics that co-reside in one group share one flow (equal HashKeys,
// consecutive SeqNums), and the HashKey matches
// XXH64(sender ∥ banded groupIdx ∥ zeros) exactly.
func TestProcessBEEF_ZeroIngredientFlow(t *testing.T) {
	fw, pe := makeBEEFForwarder(t)
	srcIP := net.ParseIP("fd00::1234")
	src := &net.UDPAddr{IP: srcIP, Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	// Find two distinct topics whose TopicIDs collide into one group at the
	// test width (16 groups ⇒ a handful of tries).
	topicA := "tm_collide_a"
	tidA := objfmt.TopicID(topicA)
	groupA := pe.GroupIndex(&tidA)
	topicB := ""
	for i := 0; i < 1000; i++ {
		cand := fmt.Sprintf("tm_collide_b%d", i)
		tid := objfmt.TopicID(cand)
		if pe.GroupIndex(&tid) == groupA {
			topicB = cand
			break
		}
	}
	if topicB == "" {
		t.Fatal("no colliding topic found in 1000 tries")
	}

	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, topicA, beefTestObj), src, 0)
	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, topicB, append(beefTestObj, 0x01)), src, 0)

	frames, _ := captureEnqueued(egr)
	if len(frames) != 2 {
		t.Fatalf("enqueued %d, want 2", len(frames))
	}
	a, _ := frame.DecodeBEEF(frames[0])
	b, _ := frame.DecodeBEEF(frames[1])

	var ipArr [16]byte
	copy(ipArr[:], srcIP.To16())
	var zeroIngredient [32]byte
	want := seqhash.Hash(ipArr, groupA, zeroIngredient)

	if a.HashKey != want || b.HashKey != want {
		t.Fatalf("HashKeys %X/%X, want %X (zero ingredient, banded group)", a.HashKey, b.HashKey, want)
	}
	if a.SeqNum != 1 || b.SeqNum != 2 {
		t.Fatalf("SeqNums %d/%d, want 1/2 (one flow per (sender, group), topics interleave)", a.SeqNum, b.SeqNum)
	}
}

// pairDedup is an in-memory TxidDedup that remembers every claimed key.
type pairDedup struct{ seen map[[32]byte]bool }

func (f *pairDedup) Claim(_ string, txid [32]byte) (bool, error) {
	if f.seen[txid] {
		return false, nil
	}
	f.seen[txid] = true
	return true, nil
}

// TestProcessBEEF_PairDedup is the D5 regression: dedup claims key the
// (ContentID, TopicID) pair, so a multi-topic submission's siblings both
// pass while an exact re-submission is suppressed.
func TestProcessBEEF_PairDedup(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	fw.SetTxidDedup(&pairDedup{seen: map[[32]byte]bool{}}, "bsp:beef:")
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	rec := buildBEEFRecordBytes(t, []string{"tm_a", "tm_b"}, beefTestObj)
	fw.SubmitBEEF(egr, rec, src, 0)
	if got := countEnqueued(egr); got != 2 {
		t.Fatalf("first submission enqueued %d, want 2 (siblings must not suppress each other)", got)
	}

	egr2 := makeEgress(t, fw, conn)
	fw.SubmitBEEF(egr2, rec, src, 0)
	if got := countEnqueued(egr2); got != 0 {
		t.Fatalf("re-submission enqueued %d, want 0 (pair claim held)", got)
	}

	// Same object to a NEW topic is a fresh pair — never suppressed.
	egr3 := makeEgress(t, fw, conn)
	fw.SubmitBEEF(egr3, buildBEEFRecordBytes(t, []string{"tm_c"}, beefTestObj), src, 0)
	if got := countEnqueued(egr3); got != 1 {
		t.Fatalf("new-topic submission enqueued %d, want 1", got)
	}
}

// TestProcessBEEF_Fragmentation proves an object exceeding the datagram
// capacity leaves as BRC-130 fragments carrying OrigFrameVer 0x09.
func TestProcessBEEF_Fragmentation(t *testing.T) {
	fw, _ := makeBEEFForwarder(t)
	fw.SetFragMTU(1280)
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	conn, _ := openLoopbackUDP(t)
	egr := makeEgress(t, fw, conn)

	big := make([]byte, 4096)
	copy(big, []byte{0x01, 0x00, 0xBE, 0xEF})
	fw.ProcessBEEF(egr, buildBEEFFrameBytes(t, "tm_big", big), src, 0)

	frames, _ := captureEnqueued(egr)
	if len(frames) < 2 {
		t.Fatalf("enqueued %d datagrams, want ≥2 fragments", len(frames))
	}
	for i, raw := range frames {
		if raw[6] != frame.FrameVerV3 {
			t.Fatalf("fragment %d: FrameVer 0x%02X, want 0x03", i, raw[6])
		}
		ff, err := frame.DecodeFragment(raw)
		if err != nil {
			t.Fatalf("fragment %d: %v", i, err)
		}
		if ff.OrigFrameVer != frame.FrameVerV9 {
			t.Fatalf("fragment %d: OrigFrameVer 0x%02X, want 0x09", i, ff.OrigFrameVer)
		}
	}
}
