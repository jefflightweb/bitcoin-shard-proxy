//go:build !freebsd

package worker

import "golang.org/x/sys/unix"

// setReusePortLB enables port sharing with kernel load-balancing across all
// worker sockets bound to the same port. On Linux (and other platforms where
// SO_REUSEPORT already load-balances) this is plain SO_REUSEPORT.
func setReusePortLB(fd int) error {
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT, 1)
}
