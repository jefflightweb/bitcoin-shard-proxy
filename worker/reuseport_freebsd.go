//go:build freebsd

package worker

import "golang.org/x/sys/unix"

// setReusePortLB enables port sharing with kernel load-balancing. FreeBSD's
// plain SO_REUSEPORT only permits the shared bind without distributing load
// (measured: only ~3 of N worker sockets ever receive), so balanced fan-out
// needs SO_REUSEPORT_LB (FreeBSD 12+).
func setReusePortLB(fd int) error {
	return unix.SetsockoptInt(fd, unix.SOL_SOCKET, unix.SO_REUSEPORT_LB, 1)
}
