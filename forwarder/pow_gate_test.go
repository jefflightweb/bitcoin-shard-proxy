package forwarder

import (
	"encoding/binary"
	"net"
	"testing"

	"github.com/lightwebinc/shard-common/frame"
	"github.com/lightwebinc/shard-common/pow"
)

// minedAnnouncePayload returns a BRC-131 BlockAnnounce payload whose leading
// 80-byte header carries valid proof of work at an easy (regtest) difficulty,
// found by grinding the nonce. The remaining bytes are the coinbase txid (32)
// + subtree count (4 = zero).
func minedAnnouncePayload(t *testing.T) []byte {
	t.Helper()
	const regtestBits = 0x207fffff
	payload := make([]byte, frame.BlockAnnounceMinPayload) // 80 + 32 + 4
	binary.LittleEndian.PutUint32(payload[72:76], regtestBits)
	for nonce := uint32(0); nonce < 1_000_000; nonce++ {
		binary.LittleEndian.PutUint32(payload[76:80], nonce)
		if pow.CheckHeader(payload[:pow.HeaderSize], nil) {
			return payload
		}
	}
	t.Fatal("could not mine a regtest header")
	return nil
}

func TestProcessBlock_PoWGate(t *testing.T) {
	src := &net.UDPAddr{IP: net.ParseIP("::1"), Port: 1}
	valid := buildBlockBufForwarder(t, 0xA1, minedAnnouncePayload(t))
	// Garbage header: nBits = 0 ⇒ target 0 ⇒ no hash can satisfy it.
	junk := buildBlockBufForwarder(t, 0xA2, make([]byte, frame.BlockAnnounceMinPayload))

	t.Run("gate_off_forwards_anything", func(t *testing.T) {
		fw := makeForwarder()
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.ProcessBlock(egr, junk, src, 0)
		if countEnqueued(egr) == 0 {
			t.Error("gate off: junk block announce should forward")
		}
	})

	t.Run("gate_on_drops_invalid", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetBlockPoW(true, 0)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.ProcessBlock(egr, junk, src, 0)
		if countEnqueued(egr) != 0 {
			t.Error("gate on: invalid-PoW announce must be dropped")
		}
	})

	t.Run("gate_on_forwards_valid", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetBlockPoW(true, 0)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.ProcessBlock(egr, valid, src, 0)
		if countEnqueued(egr) == 0 {
			t.Error("gate on: valid-PoW announce must forward")
		}
	})

	t.Run("floor_rejects_easy_difficulty", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetBlockPoW(true, 0x1d00ffff) // mainnet floor ≫ regtest difficulty
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		fw.ProcessBlock(egr, valid, src, 0)
		if countEnqueued(egr) != 0 {
			t.Error("floor on: a below-floor (regtest) announce must be dropped")
		}
	})

	t.Run("coinbase_unaffected", func(t *testing.T) {
		fw := makeForwarder()
		fw.SetBlockPoW(true, 0x1d00ffff)
		conn, _ := openLoopbackUDP(t)
		egr := makeEgress(t, fw, conn)
		// Coinbase carries no header; the PoW gate must not touch it.
		fw.ProcessBlock(egr, buildCoinbaseBufForwarder(t, 0xB1, nil), src, 0)
		if countEnqueued(egr) == 0 {
			t.Error("coinbase must forward regardless of the block PoW gate")
		}
	})
}
