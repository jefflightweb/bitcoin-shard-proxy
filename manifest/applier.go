package manifest

import (
	"context"
	"log/slog"
	"reflect"
	"sync/atomic"
	"time"

	commanifest "github.com/lightwebinc/shard-common/manifest"

	"github.com/lightwebinc/shard-proxy/metrics"
)

// RestartRequest signals that a manifest adoption requires the proxy
// process to exit (so the orchestrator can roll the pod with the new
// addressing parameters in place). It is goroutine-safe.
type RestartRequest struct {
	requested atomic.Bool
	reason    atomic.Value // string
}

// Request marks a restart as needed with the given reason. Idempotent.
func (r *RestartRequest) Request(reason string) {
	if r.requested.CompareAndSwap(false, true) {
		r.reason.Store(reason)
	}
}

// Requested reports whether a restart has been requested.
func (r *RestartRequest) Requested() bool { return r.requested.Load() }

// Reason returns the recorded reason (empty if not yet requested).
func (r *RestartRequest) Reason() string {
	if v := r.reason.Load(); v != nil {
		return v.(string)
	}
	return ""
}

// Hooks are pluggable change handlers for the proxy applier.
type Hooks struct {
	// OnShardBitsChange fires when adopted ShardBits transitions away
	// from the previous value. With LiveResharding=false this should
	// call RestartRequest.Request; with LiveResharding=true the
	// caller wires it into the bridging-mode entry point.
	OnShardBitsChange func(prev, next uint8)

	// OnSourceModeChange fires on a SourceModeSSM transition.
	OnSourceModeChange func(prevSSM, nextSSM bool)

	// OnSuccessorChange fires when a Successor view appears, changes,
	// or disappears. After is nil when the Successor is no longer
	// adopted.
	OnSuccessorChange func(before, after *commanifest.SuccessorView)

	// OnDomainShardBitsChange fires when a BRC-148 object plane's adopted
	// ShardBits transitions away from the previous value, keyed by
	// DomainID (domain 0x0 is governed by OnShardBitsChange, never here).
	// Like OnShardBitsChange, restart-vs-bridge is the caller's policy;
	// with a per-domain pin the plane never adopts and this fires only if
	// the pin itself is reconfigured.
	OnDomainShardBitsChange func(domain, prev, next uint8)
}

// Applier is the periodic evaluator+notifier. The proxy applier does
// NOT diff PilotGroups (the proxy is not a subscriber) and does NOT
// diff SourceSet (sources affect listeners, not the proxy emitter
// path); those concerns belong to the listener.
type Applier struct {
	Registry  *commanifest.Registry
	Evaluator *commanifest.Evaluator
	Hooks     Hooks
	Rec       *metrics.Recorder
	Log       *slog.Logger
	Interval  time.Duration // default 1s
}

// Run blocks until ctx is cancelled, firing hooks on adoption changes.
func (a *Applier) Run(ctx context.Context) {
	interval := a.Interval
	if interval == 0 {
		interval = 1 * time.Second
	}
	log := a.Log
	if log == nil {
		log = slog.Default().With("component", "manifest-applier")
	}

	t := time.NewTicker(interval)
	defer t.Stop()

	var prev commanifest.Adopted
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
		}
		a.Registry.Evict()
		snap := a.Registry.Snapshot()
		next := a.Evaluator.Evaluate(snap)
		a.publishMetrics(next)

		if a.Hooks.OnShardBitsChange != nil && prev.ShardBits != next.ShardBits {
			log.Info("ShardBits change", "prev", prev.ShardBits, "next", next.ShardBits)
			a.Rec.ManifestAdoption("shard_bits", reasonForChange(prev.ShardBits != 0))
			a.Hooks.OnShardBitsChange(prev.ShardBits, next.ShardBits)
		}
		if a.Hooks.OnSourceModeChange != nil && prev.SourceModeSSM != next.SourceModeSSM {
			log.Info("SourceMode change", "prev_ssm", prev.SourceModeSSM, "next_ssm", next.SourceModeSSM)
			a.Rec.ManifestAdoption("source_mode", reasonForChange(prev.PilotsKnown > 0))
			a.Hooks.OnSourceModeChange(prev.SourceModeSSM, next.SourceModeSSM)
		}
		if a.Hooks.OnSuccessorChange != nil && !reflect.DeepEqual(prev.Successor, next.Successor) {
			log.Info("Successor change", "prev", prev.Successor, "next", next.Successor)
			a.Hooks.OnSuccessorChange(prev.Successor, next.Successor)
		}
		if a.Hooks.OnDomainShardBitsChange != nil {
			for _, d := range domainShardBitsChanges(prev.Domains, next.Domains) {
				log.Info("domain ShardBits change", "domain", d.domain, "prev", d.prev, "next", d.next)
				a.Rec.ManifestAdoption("domain_shard_bits", reasonForChange(d.prev != 0))
				a.Hooks.OnDomainShardBitsChange(d.domain, d.prev, d.next)
			}
		}
		// Per-domain divergence surfaces via the shared DivergenceFields list.
		for _, f := range next.DivergenceFields {
			a.Rec.ManifestDivergence(f, "peer-disagree")
		}
		prev = next
	}
}

func (a *Applier) publishMetrics(v commanifest.Adopted) {
	a.Rec.ManifestSetPilotsKnown(v.PilotsKnown)
	var bits int32
	if v.QuorumMet["shard_bits"] {
		bits |= 1 << 0
	}
	if v.QuorumMet["source_mode"] {
		bits |= 1 << 1
	}
	if v.QuorumMet["successor"] {
		bits |= 1 << 2
	}
	a.Rec.ManifestSetQuorumMetBits(bits)

	state := int32(0)
	window := int64(0)
	if v.Successor != nil {
		now := time.Now().Unix()
		remaining := int64(v.Successor.TransitionEpoch) - now
		window = remaining
		if remaining > 0 {
			state = 1
		} else {
			state = 2
		}
	}
	a.Rec.ManifestSetReshardState(state)
	a.Rec.ManifestSetReshardWindowSeconds(window)
}

func reasonForChange(hadPrev bool) string {
	if hadPrev {
		return "quorum-shift"
	}
	return "bootstrap"
}

// domainChange is one per-domain ShardBits transition.
type domainChange struct {
	domain     uint8
	prev, next uint8
}

// domainShardBitsChanges returns the per-domain ShardBits transitions between
// two adopted views, in ascending DomainID order. A domain present in only
// one map contributes a change from/to its zero value.
func domainShardBitsChanges(prev, next map[uint8]commanifest.DomainAdoption) []domainChange {
	var out []domainChange
	for id := uint8(0); id <= 0x0E; id++ {
		pv := prev[id].ShardBits
		nv := next[id].ShardBits
		if pv != nv {
			out = append(out, domainChange{domain: id, prev: pv, next: nv})
		}
	}
	return out
}
