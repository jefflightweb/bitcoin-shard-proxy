package worker

import "testing"

// retryTeeSetter is the contract every ingress path must satisfy: each builds its
// OWN forwarder.Egress, so the tee has to be enabled on every one of them.
type retryTeeSetter interface{ SetRetryTee(string) }

// A tee wired into only one ingress path silently misses everything submitted
// over the others. That is not hypothetical: BEEF records arrive on the TCP lane,
// so with the tee only on the UDP worker the co-located cache stayed empty under
// real BEEF load while appearing to work perfectly under tx load. Asserting the
// REAL types here means a new ingress path that forgets the tee fails to compile
// this test rather than silently caching nothing.
func TestAllIngressPathsSupportRetryTee(t *testing.T) {
	var (
		_ retryTeeSetter = (*Worker)(nil)
		_ retryTeeSetter = (*TCPIngress)(nil)
		_ retryTeeSetter = (*ObjectIngress)(nil)
	)
	// Setting must not panic on a zero-value instance.
	for _, s := range []retryTeeSetter{&Worker{}, &TCPIngress{}, &ObjectIngress{}} {
		s.SetRetryTee("[::1]:9001")
	}
}
