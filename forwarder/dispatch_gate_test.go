package forwarder

import (
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
)

// countEnqueued drains egr through FlushVia and returns how many datagrams
// were queued for egress, so a test can assert whether a frame was forwarded
// or dropped by the ingress-class gate.
func countEnqueued(egr *Egress) int {
	n := 0
	egr.FlushVia(func(_ int, _ []byte, _ *net.UDPAddr) error {
		n++
		return nil
	})
	return n
}

// dispatchCase is one (frame, expected-forward-on-transaction-socket) pair.
type dispatchCase struct {
	name           string
	raw            []byte
	privilegedOnly bool // dropped on a transaction-only socket
}

func gateCases(t *testing.T) []dispatchCase {
	t.Helper()
	return []dispatchCase{
		{"brc124_tx", buildV2Frame(t, 0xAB, 0, []byte("tx")), false},
		{"brc12_legacy", buildV1Frame(t, 0xAB, []byte("tx")), false},
		{"brc134_anchor", buildAnchorFrame(t, 0xAB, 0, []byte("anchor")), false},
		{"brc131_block", buildBlockBufForwarder(t, 0xBB, []byte("blk")), true},
		{"brc133_coinbase", buildCoinbaseBufForwarder(t, 0xBB, []byte("cb")), true},
		{"brc132_subtree", buildSubtreeDataFrame(t, 0xAA, []byte("sub")), true},
	}
}

// TestDispatchClass_TransactionGate verifies that a transaction-only socket
// forwards tx/anchor frames but drops privileged block/coinbase/subtree-data
// frames, while a privileged socket forwards everything.
func TestDispatchClass_TransactionGate(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}

	for _, tc := range gateCases(t) {
		t.Run(tc.name+"/transaction", func(t *testing.T) {
			fw := makeForwarder()
			conn, _ := openLoopbackUDP(t)
			egr := makeEgress(t, fw, conn)
			fw.DispatchClass(egr, tc.raw, src, 0, IngressTransaction)
			got := countEnqueued(egr)
			if tc.privilegedOnly && got != 0 {
				t.Errorf("transaction socket forwarded privileged frame (%d queued), want 0 (dropped)", got)
			}
			if !tc.privilegedOnly && got == 0 {
				t.Errorf("transaction socket dropped a transaction frame, want forwarded")
			}
		})

		t.Run(tc.name+"/privileged", func(t *testing.T) {
			fw := makeForwarder()
			conn, _ := openLoopbackUDP(t)
			egr := makeEgress(t, fw, conn)
			fw.DispatchClass(egr, tc.raw, src, 0, IngressPrivileged)
			if got := countEnqueued(egr); got == 0 {
				t.Errorf("privileged socket dropped %s, want forwarded", tc.name)
			}
		})
	}
}

// TestDispatch_DefaultAcceptsAll verifies the legacy Dispatch entry point
// behaves as a privileged socket (accepts every frame class).
func TestDispatch_DefaultAcceptsAll(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 12345}
	for _, tc := range gateCases(t) {
		fw := makeForwarder()
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.Dispatch(egr, tc.raw, src, 0)
		if got := countEnqueued(egr); got == 0 {
			t.Errorf("Dispatch dropped %s, want forwarded (legacy accept-all)", tc.name)
		}
	}
}

// TestRejectPrivileged_FrameTypeLabel covers the metric frame-type
// classification branch for both block-announce and coinbase V4 frames plus
// V5 subtree data, exercising rejectPrivileged via a transaction socket.
func TestRejectPrivileged_FrameTypeLabel(t *testing.T) {
	fw := makeForwarder()
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	conn, _ := openLoopbackUDP(t)

	// Each must be dropped (no panic, no enqueue) on a transaction socket.
	for _, raw := range [][]byte{
		buildBlockBufForwarder(t, 0x01, nil),
		buildCoinbaseBufForwarder(t, 0x02, nil),
		buildSubtreeDataFrame(t, 0x03, nil),
	} {
		egr := makeEgress(t, fw, conn)
		fw.DispatchClass(egr, raw, src, 0, IngressTransaction)
		if got := countEnqueued(egr); got != 0 {
			t.Errorf("privileged frame ver=%d enqueued %d, want 0", raw[6], got)
		}
	}

	// Guard the coinbase classification path directly.
	cb := buildCoinbaseBufForwarder(t, 0x02, nil)
	if cb[7] != frame.BlockMsgCoinbase {
		t.Fatalf("coinbase fixture MsgType=%d, want %d", cb[7], frame.BlockMsgCoinbase)
	}
}
