package worker

import (
	"net"
	"testing"
)

// TestAdmitForBindsLocalAddr pins the per-connection hook contract: both hooks
// see every admission, the connection hook sees the LOCAL (dialed) address, and
// with neither set the per-submission cost is a nil check.
func TestAdmitForBindsLocalAddr(t *testing.T) {
	ti := &TCPIngress{}
	if ti.admitFor(&fakeConn{local: &net.TCPAddr{IP: net.ParseIP("2001:db8::1"), Port: 8725}}) != nil {
		t.Fatal("no hooks should bind no observer")
	}
	var plain, perConn int
	var seen net.Addr
	ti.SetAdmitHook(func(frames, bytes int) { plain += frames })
	ti.SetConnAdmitHook(func(local net.Addr, frames, bytes int) { perConn += bytes; seen = local })
	admit := ti.admitFor(&fakeConn{local: &net.TCPAddr{IP: net.ParseIP("2001:db8:80::1"), Port: 8725}})
	admit(1, 100)
	admit(1, 50)
	if plain != 2 || perConn != 150 || seen.String() != "[2001:db8:80::1]:8725" {
		t.Fatalf("plain=%d perConn=%d local=%v", plain, perConn, seen)
	}
}

type fakeConn struct {
	net.Conn
	local net.Addr
}

func (f *fakeConn) LocalAddr() net.Addr { return f.local }
