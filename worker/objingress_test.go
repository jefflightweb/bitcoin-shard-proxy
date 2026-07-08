package worker

import (
	"encoding/binary"
	"net"
	"testing"
	"time"

	"github.com/lightwebinc/shard-common/objfmt"
)

// buildSubtreeObj assembles a BRC-143 subtree push frame: 32B root + uint64
// count + count×32 node hashes (first is the 0xFF×32 coinbase placeholder).
func buildSubtreeObj(count int) []byte {
	b := make([]byte, 0, 40+count*32)
	b = append(b, make([]byte, 32)...) // merkle root (zeros ok for framing)
	var c [8]byte
	binary.BigEndian.PutUint64(c[:], uint64(count))
	b = append(b, c[:]...)
	for i := 0; i < count; i++ {
		node := make([]byte, 32)
		if i == 0 {
			for j := range node {
				node[j] = 0xFF
			}
		} else {
			node[0] = byte(i)
		}
		b = append(b, node...)
	}
	return b
}

// runHandleConn drives handleConn over one side of a net.Pipe, writing stream
// then closing, and reports whether handleConn returned before the timeout.
func runHandleConn(t *testing.T, class objfmt.Class, stream []byte) bool {
	t.Helper()
	oi := NewObjectIngress(makeTestForwarder(), []*net.Interface{{Index: 1, Name: "lo"}}, nil, class)
	srv, cli := net.Pipe()
	done := make(chan struct{})
	go func() {
		oi.handleConn(srv, nil) // egr nil: decode + reframe + dispatch, no egress enqueue
		close(done)
	}()
	go func() {
		_, _ = cli.Write(stream)
		_ = cli.Close()
	}()
	select {
	case <-done:
		return true
	case <-time.After(2 * time.Second):
		_ = srv.Close()
		return false
	}
}

// TestObjectIngressReframesSubtreeStream drives two back-to-back BRC-143 subtree
// objects through the reader → MulticastBytes → DispatchClass path. Each reframe
// produces a real BRC-132 frame that ProcessSubtreeData decodes; the loop must
// consume both and return cleanly at EOF (no hang, no error path).
func TestObjectIngressReframesSubtreeStream(t *testing.T) {
	stream := append(buildSubtreeObj(3), buildSubtreeObj(6)...)
	if !runHandleConn(t, objfmt.ClassSubtree, stream) {
		t.Fatal("handleConn did not return on a valid subtree stream (hang)")
	}
}

// TestObjectIngressClosesOnMalformed asserts a garbage stream (no valid object
// boundary) drops the connection rather than hanging.
func TestObjectIngressClosesOnMalformed(t *testing.T) {
	// A subtree header claiming a count that would be parsed, followed by a
	// truncated body, then EOF → ErrUnexpectedEOF closes the conn.
	obj := buildSubtreeObj(4)
	if !runHandleConn(t, objfmt.ClassSubtree, obj[:len(obj)-10]) {
		t.Fatal("handleConn did not return on a truncated subtree (hang)")
	}
}
