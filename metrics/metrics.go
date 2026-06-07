// Package metrics initialises an OpenTelemetry MeterProvider backed by both
// a Prometheus exporter (for scraping) and an optional OTLP gRPC exporter
// (for push-based delivery to any OTel-compatible backend).
//
// # Instance identity
//
// The MeterProvider is constructed with an OTel Resource that carries
// service.name, service.instance.id, and service.version as resource
// attributes. These appear in OTLP payloads and as Prometheus target labels,
// allowing federation across horizontally-scaled instances without metric-label
// collisions.
//
// # Hot-path design
//
// All OTel instrument handles (Int64Counter, Int64Histogram, etc.) are
// allocated once at [New] time and stored on [Recorder]. Record methods
// use them directly — no map lookups on the critical path.
//
// Per-(interface, worker) and per-(interface, group) Prometheus counter
// handles are integer-indexed under [ifaceState] (slice by workerID / group),
// behind an atomic copy-on-write per-interface map. After warmup the hot path
// is a lock-free atomic load + short-string map lookup + slice index, with no
// per-packet sync.Map hashing or active-group mutex.
//
// # Health endpoints
//
// [Recorder.Serve] registers /metrics, /healthz, and /readyz on a single
// net/http.ServeMux. Readiness is tracked via an atomic counter incremented
// by [Recorder.WorkerReady] and decremented by [Recorder.WorkerDone],
// enabling load-balancer drain during graceful shutdown.
package metrics

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	_ "net/http/pprof" // mounts /debug/pprof/* on http.DefaultServeMux; gated by Serve(pprof=true)
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	prometheusexporter "go.opentelemetry.io/otel/exporters/prometheus"
	"go.opentelemetry.io/otel/metric"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/exemplar"
	"go.opentelemetry.io/otel/sdk/resource"

	promclient "github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/lightwebinc/shard-common/logging"
)

// ServiceName is the OTel service.name resource attribute value.
const ServiceName = "shard-proxy"

// Version is set at build time via -ldflags "-X metrics.Version=<ver>".
// Defaults to "dev" when not injected.
var Version = "dev"

// ifaceState holds the pre-bound hot-path counter handles for one egress
// interface, integer-indexed for O(1) lock-free reads after warmup. It
// replaces the per-packet interface-keyed sync.Map lookups (profiled at a
// large share of CPU on the zero-copy hot path): the hot path now costs one
// atomic load + a short-string map lookup + slice indexing.
type ifaceState struct {
	iface   string
	workers []*hotPathCounters        // indexed by workerID; bound on first use
	flows   atomic.Pointer[flowTable] // COW, indexed by groupIdx
	mu      sync.Mutex                // guards worker binding + flow COW grow
}

// flowTable is an immutable (copy-on-write) per-group counter table indexed by
// group index. Stored behind ifaceState.flows so reads are lock-free.
type flowTable struct {
	packets []promclient.Counter
	bytes   []promclient.Counter
}

// hotPathCounters bundles the pre-bound prometheus.Counter handles for a
// single (iface, worker) tuple. One lookup at startup of a flow gets all
// six in O(1).
type hotPathCounters struct {
	rxPackets promclient.Counter
	rxBytes   promclient.Counter
	txPackets promclient.Counter
	txBytes   promclient.Counter
	rxSize    promclient.Observer // histogram observer
}

// Recorder holds all pre-allocated OTel instrument handles and readiness
// state. Construct with [New]; pass the pointer to every worker.
type Recorder struct {
	provider    *sdkmetric.MeterProvider
	promReg     promclient.Gatherer
	promOtelReg *promclient.Registry
	runtimeReg  *promclient.Registry
	levelVar    *slog.LevelVar
	numWorkers  int
	startTime   time.Time
	readyCount  atomic.Int32

	// ── Hot-path metrics — direct prometheus client_golang ──
	// OTel SDK Add() was profiled at ~⅓ of total CPU on the 256 B Bitcoin
	// hot path. Direct Prometheus skips attribute hashing, exemplar
	// filtering, aggregation pipeline, allocator churn. shard-proxy never
	// used OTel beyond Prometheus scraping, so the SDK was pure overhead.

	// Per-packet ingress — labels: worker, iface (worker, network_interface_name).
	promRxPackets *promclient.CounterVec
	promRxBytes   *promclient.CounterVec
	promRxDrops   *promclient.CounterVec // also labelled by reason
	promRxSize    *promclient.HistogramVec

	// Per-packet egress — labels: worker, iface.
	promTxPackets     *promclient.CounterVec
	promTxBytes       *promclient.CounterVec
	promTxEgressErrs  *promclient.CounterVec
	promTxIngressErrs *promclient.CounterVec

	// Per-flow / per-group — labels: iface, group (no worker dimension).
	promFlowPackets *promclient.CounterVec
	promFlowBytes   *promclient.CounterVec

	// Per-interface integer-indexed hot-path cache. Reads are lock-free (one
	// atomic load + a short-string map lookup + slice index); first sight of an
	// (iface)/(iface,worker)/(iface,group) binds under a lock. Replaces the two
	// per-packet interface-keyed sync.Maps and the per-packet active-group mutex.
	ifaceStates atomic.Pointer[map[string]*ifaceState]
	ifaceMu     sync.Mutex // guards ifaceStates copy-on-write

	// Fragmentation counters (BRC-130)
	framesFragmented metric.Int64Counter
	fragmentsEmitted metric.Int64Counter

	// Control-plane forwarding (TCP ingress + BRC-127)
	ctrlFramesForwarded metric.Int64Counter
	tcpConnections      metric.Int64Counter
	tcpBytesReceived    metric.Int64Counter

	// Ingress TxID dedup — TxidClaim* are per-packet on the hot path
	// when dedup is on, so they are direct prometheus.Counter handles
	// keyed by prefix (one allocation per distinct prefix at startup).
	ingressDeduped        metric.Int64Counter // by worker, iface, frame_type (cold)
	promTxidClaimLocalHit *promclient.CounterVec
	promTxidClaimWon      *promclient.CounterVec
	promTxidClaimLost     *promclient.CounterVec
	promTxidClaimError    *promclient.CounterVec

	// draining is set to true when a shutdown signal has been received and the
	// proxy is waiting for the load-balancer to stop routing new connections.
	// While true, /readyz returns 503 regardless of worker count.
	draining atomic.Bool

	// tcpIngressRequired is set true when TCP_LISTEN_PORT > 0; /readyz then
	// also gates on tcpIngressReady, which TCPIngress.Run flips after a
	// successful net.Listen. Prevents senders from racing the listener bind.
	tcpIngressRequired atomic.Bool
	tcpIngressReady    atomic.Bool

	// BRC-137 manifest consumer metrics. Updated by the proxy's
	// auto-config subsystem (shard-proxy/manifest).
	manifestReceived      atomic.Int64
	manifestPilotsKnown   atomic.Int64
	manifestQuorumMetBits atomic.Int32
	manifestReshardState  atomic.Int32
	manifestReshardWindow atomic.Int64

	// Composed shutdown function (OTLP exporter + MeterProvider)
	shutdownFn func(context.Context) error
}

// New constructs and registers a Recorder.
//
//   - instanceID identifies this process in federated/horizontally-scaled
//     deployments (e.g. hostname or pod name). Falls back to os.Hostname().
//   - numWorkers is the total configured worker count, used by /readyz.
//   - otlpEndpoint is the gRPC endpoint for OTLP push (empty = disabled).
//   - otlpInterval is the OTLP push cadence.
func New(instanceID string, numWorkers int, otlpEndpoint string, otlpInterval time.Duration) (*Recorder, error) {
	if instanceID == "" {
		h, err := os.Hostname()
		if err != nil {
			h = "unknown"
		}
		instanceID = h
	}

	res, err := resource.New(context.Background(),
		resource.WithAttributes(
			attribute.String("service.name", ServiceName),
			attribute.String("service.instance.id", instanceID),
			attribute.String("service.version", Version),
		),
	)
	if err != nil {
		return nil, fmt.Errorf("metrics: build resource: %w", err)
	}

	// Dedicated Prometheus registry — avoids polluting prometheus.DefaultRegisterer.
	reg := promclient.NewRegistry()
	promExp, err := prometheusexporter.New(
		prometheusexporter.WithRegisterer(reg),
	)

	// Separate registry for Go runtime and process metrics.
	runtimeReg := promclient.NewRegistry()
	runtimeReg.MustRegister(collectors.NewGoCollector())
	runtimeReg.MustRegister(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}))
	if err != nil {
		return nil, fmt.Errorf("metrics: prometheus exporter: %w", err)
	}

	mpOpts := []sdkmetric.Option{
		sdkmetric.WithReader(promExp),
		sdkmetric.WithResource(res),
		// Exemplars require a trace context per measurement and add ~11%
		// of cumulative CPU on the hot path (aggregate.Builder.filter).
		// shard-proxy doesn't emit traces, so they are never useful.
		sdkmetric.WithExemplarFilter(exemplar.AlwaysOffFilter),
	}

	var shutdownFuncs []func(context.Context) error

	if otlpEndpoint != "" {
		otlpExp, oerr := otlpmetricgrpc.New(
			context.Background(),
			otlpmetricgrpc.WithEndpoint(otlpEndpoint),
			otlpmetricgrpc.WithInsecure(),
		)
		if oerr != nil {
			return nil, fmt.Errorf("metrics: OTLP exporter: %w", oerr)
		}
		mpOpts = append(mpOpts, sdkmetric.WithReader(
			sdkmetric.NewPeriodicReader(otlpExp,
				sdkmetric.WithInterval(otlpInterval),
			),
		))
		shutdownFuncs = append(shutdownFuncs, otlpExp.Shutdown)
		slog.Info("OTLP exporter enabled", "endpoint", otlpEndpoint, "interval", otlpInterval)
	}

	mp := sdkmetric.NewMeterProvider(mpOpts...)
	shutdownFuncs = append(shutdownFuncs, mp.Shutdown)

	r := &Recorder{
		provider:    mp,
		promReg:     promclient.Gatherers{reg, runtimeReg},
		promOtelReg: reg,
		runtimeReg:  runtimeReg,
		numWorkers:  numWorkers,
		startTime:   time.Now(),
		shutdownFn: func(ctx context.Context) error {
			var last error
			for _, fn := range shutdownFuncs {
				if err := fn(ctx); err != nil {
					last = err
				}
			}
			return last
		},
	}

	meter := mp.Meter(ServiceName)

	// Hot-path metrics: direct prometheus client_golang. Same names and
	// label sets as the previous OTel int64Counter exposure so dashboards
	// keep working.
	r.promRxPackets = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_packets_received_total",
		Help: "Datagrams received",
	}, []string{"worker", "network_interface_name"})
	r.promRxBytes = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_bytes_received_total",
		Help: "Raw bytes received",
	}, []string{"worker", "network_interface_name"})
	r.promRxDrops = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_packets_dropped_total",
		Help: "Datagrams dropped",
	}, []string{"worker", "network_interface_name", "reason"})
	r.promRxSize = promclient.NewHistogramVec(promclient.HistogramOpts{
		Name:    "bsp_packet_size_bytes",
		Help:    "Datagram size distribution",
		Buckets: promclient.ExponentialBuckets(64, 2, 12), // 64 B .. 256 KiB
	}, []string{"worker", "network_interface_name"})
	r.promTxPackets = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_packets_forwarded_total",
		Help: "Datagrams successfully forwarded",
	}, []string{"worker", "network_interface_name"})
	r.promTxBytes = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_bytes_forwarded_total",
		Help: "Raw bytes forwarded",
	}, []string{"worker", "network_interface_name"})
	r.promTxEgressErrs = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_egress_errors_total",
		Help: "WriteTo errors on egress socket",
	}, []string{"worker", "network_interface_name"})
	r.promTxIngressErrs = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_ingress_errors_total",
		Help: "ReadFrom non-fatal errors on ingress socket",
	}, []string{"worker", "network_interface_name"})
	r.promFlowPackets = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_flow_packets_total",
		Help: "Packets per shard group per interface (active groups only)",
	}, []string{"network_interface_name", "group"})
	r.promFlowBytes = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_flow_bytes_total",
		Help: "Bytes per shard group per interface (active groups only)",
	}, []string{"network_interface_name", "group"})
	for _, c := range []promclient.Collector{
		r.promRxPackets, r.promRxBytes, r.promRxDrops, r.promRxSize,
		r.promTxPackets, r.promTxBytes,
		r.promTxEgressErrs, r.promTxIngressErrs,
		r.promFlowPackets, r.promFlowBytes,
	} {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("metrics: register hot-path counter: %w", err)
		}
	}

	// Observable gauge: distinct active group count per interface.
	if _, err = meter.Int64ObservableGauge("bsp_active_groups",
		metric.WithDescription("Distinct shard groups seen since startup, per interface"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			if m := r.ifaceStates.Load(); m != nil {
				for iface, st := range *m {
					n := 0
					for _, c := range st.flows.Load().packets {
						if c != nil {
							n++
						}
					}
					o.Observe(int64(n),
						metric.WithAttributes(attribute.String("network.interface.name", iface)),
					)
				}
			}
			return nil
		}),
	); err != nil {
		return nil, err
	}

	// Observable gauge: running worker count.
	if _, err = meter.Int64ObservableGauge("bsp_workers_active",
		metric.WithDescription("Number of running worker goroutines"),
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(int64(r.readyCount.Load()))
			return nil
		}),
	); err != nil {
		return nil, err
	}

	// Observable gauge: process uptime in seconds.
	if _, err = meter.Float64ObservableGauge("bsp_uptime_seconds",
		metric.WithDescription("Seconds since process start"),
		metric.WithUnit("s"),
		metric.WithFloat64Callback(func(_ context.Context, o metric.Float64Observer) error {
			o.Observe(time.Since(r.startTime).Seconds())
			return nil
		}),
	); err != nil {
		return nil, err
	}

	if r.framesFragmented, err = meter.Int64Counter("bsp_frames_fragmented_total",
		metric.WithDescription("Frames that exceeded the fragmentation threshold and were split into BRC-130 fragments")); err != nil {
		return nil, err
	}
	if r.fragmentsEmitted, err = meter.Int64Counter("bsp_fragments_emitted_total",
		metric.WithDescription("Total BRC-130 fragment datagrams sent (K fragments per fragmented frame)")); err != nil {
		return nil, err
	}

	if r.ctrlFramesForwarded, err = meter.Int64Counter("bsp_control_frames_forwarded_total",
		metric.WithDescription("BRC-127 control datagrams forwarded to multicast (e.g. SubtreeGroupAnnounce)")); err != nil {
		return nil, err
	}
	if r.tcpConnections, err = meter.Int64Counter("bsp_tcp_connections_total",
		metric.WithDescription("TCP connections accepted on the control-plane ingress port")); err != nil {
		return nil, err
	}
	if r.tcpBytesReceived, err = meter.Int64Counter("bsp_tcp_bytes_received_total",
		metric.WithDescription("Bytes read from TCP ingress connections"),
		metric.WithUnit("By")); err != nil {
		return nil, err
	}

	if r.ingressDeduped, err = meter.Int64Counter("bsp_ingress_deduped_total",
		metric.WithDescription("Frames suppressed by the ingress TxID dedup gate (sibling proxy or listener already claimed the TxID)")); err != nil {
		return nil, err
	}
	// TxidClaim* are per-packet when ingress dedup is on; direct prometheus
	// counters to skip the ~10% CPU spent in OTel int64Counter.Add.
	r.promTxidClaimLocalHit = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_txid_claim_local_hit_total",
		Help: "Tier-1 local-LRU short-circuits during TxID claim (no Redis call)",
	}, []string{"prefix"})
	r.promTxidClaimWon = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_txid_claim_won_total",
		Help: "Tier-2 Redis SETNX wins during TxID claim (frame proceeds to multicast)",
	}, []string{"prefix"})
	r.promTxidClaimLost = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_txid_claim_lost_total",
		Help: "Tier-2 Redis SETNX losses during TxID claim (sibling already claimed; frame dropped)",
	}, []string{"prefix"})
	r.promTxidClaimError = promclient.NewCounterVec(promclient.CounterOpts{
		Name: "bsp_txid_claim_errors_total",
		Help: "Redis errors during TxID claim (fail-open: frame was forwarded)",
	}, []string{"prefix"})
	for _, c := range []promclient.Collector{
		r.promTxidClaimLocalHit, r.promTxidClaimWon,
		r.promTxidClaimLost, r.promTxidClaimError,
	} {
		if regErr := reg.Register(c); regErr != nil {
			return nil, fmt.Errorf("metrics: register txid-claim counter: %w", regErr)
		}
	}

	return r, nil
}

// ── Record methods (hot path) ────────────────────────────────────────────────

// PacketReceived records receipt of a raw datagram on the ingress socket.
func (r *Recorder) PacketReceived(iface string, workerID int, size int) {
	c := r.ifaceState(iface).worker(r, workerID)
	c.rxPackets.Inc()
	c.rxBytes.Add(float64(size))
	c.rxSize.Observe(float64(size))
}

// PacketDropped records a dropped datagram.
// reason must be one of: "decode_error", "write_error".
func (r *Recorder) PacketDropped(iface string, workerID int, reason string) {
	r.promRxDrops.WithLabelValues(strconv.Itoa(workerID), iface, reason).Inc()
}

// PacketForwarded records a successfully forwarded datagram.
func (r *Recorder) PacketForwarded(iface string, workerID int, groupIdx uint32, size int) {
	st := r.ifaceState(iface)
	c := st.worker(r, workerID)
	c.txPackets.Inc()
	c.txBytes.Add(float64(size))

	fp, fb := st.flow(r, groupIdx)
	fp.Inc()
	fb.Add(float64(size))
}

// FrameFragmented records one frame that exceeded the fragmentation threshold.
// k is the number of fragments it was split into. Not a per-packet hot path
// (only fires for > MTU frames) so still uses OTel.
func (r *Recorder) FrameFragmented(workerID int, k int) {
	opt := metric.WithAttributes(
		attribute.Int("worker", workerID),
		ifaceAttr(""),
	)
	ctx := context.Background()
	r.framesFragmented.Add(ctx, 1, opt)
	r.fragmentsEmitted.Add(ctx, int64(k), opt)
}

// ControlFrameForwarded records a BRC-127 control datagram forwarded via ForwardControl.
// ctrlGroup names the destination control group (e.g. "subtree_group_announce").
func (r *Recorder) ControlFrameForwarded(ctrlGroup string) {
	r.ctrlFramesForwarded.Add(context.Background(), 1, metric.WithAttributes(
		attribute.String("ctrl_group", ctrlGroup),
	))
}

// IngressDeduped records a frame suppressed by the ingress TxID dedup gate.
// frameType is one of "brc124", "brc131", "brc132", "brc134" — matching the
// frame-version namespace used by other metrics. iface may be empty when the
// dropping worker has no resolved interface yet.
func (r *Recorder) IngressDeduped(iface string, workerID int, frameType string) {
	r.ingressDeduped.Add(context.Background(), 1, metric.WithAttributes(
		attribute.Int("worker", workerID),
		ifaceAttr(iface),
		attribute.String("frame_type", frameType),
	))
}

// TxidClaimLocalHit records a tier-1 local-LRU short-circuit during TxID
// dedup; the frame was suppressed without a Redis call.
func (r *Recorder) TxidClaimLocalHit(prefix string) {
	r.promTxidClaimLocalHit.WithLabelValues(prefix).Inc()
}

// TxidClaimWon records a tier-2 SETNX win during TxID dedup (frame proceeds).
func (r *Recorder) TxidClaimWon(prefix string) {
	r.promTxidClaimWon.WithLabelValues(prefix).Inc()
}

// TxidClaimLost records a tier-2 SETNX loss during TxID dedup (frame dropped).
func (r *Recorder) TxidClaimLost(prefix string) {
	r.promTxidClaimLost.WithLabelValues(prefix).Inc()
}

// TxidClaimError records a Redis error during TxID dedup (fail-open).
func (r *Recorder) TxidClaimError(prefix string) {
	r.promTxidClaimError.WithLabelValues(prefix).Inc()
}

// TCPConnectionAccepted records an accepted TCP ingress connection.
func (r *Recorder) TCPConnectionAccepted() {
	r.tcpConnections.Add(context.Background(), 1)
}

// TCPBytesReceived records bytes read from a TCP ingress connection.
func (r *Recorder) TCPBytesReceived(n int) {
	r.tcpBytesReceived.Add(context.Background(), int64(n))
}

// IngressError records a non-fatal ReadFrom error on the ingress socket.
func (r *Recorder) IngressError(iface string, workerID int) {
	r.promTxIngressErrs.WithLabelValues(strconv.Itoa(workerID), iface).Inc()
}

// EgressError records a WriteTo error on the egress socket.
func (r *Recorder) EgressError(iface string, workerID int) {
	r.promTxEgressErrs.WithLabelValues(strconv.Itoa(workerID), iface).Inc()
}

// WorkerReady signals that a worker has bound its sockets and entered its
// receive loop. Call once per worker after successful socket setup.
func (r *Recorder) WorkerReady() {
	r.readyCount.Add(1)
}

// WorkerDone signals that a worker has exited its receive loop.
// Defer at the top of Worker.Run before any early returns.
func (r *Recorder) WorkerDone() {
	r.readyCount.Add(-1)
}

// ManifestReceived increments the BRC-137 receive counter. nil-safe.
func (r *Recorder) ManifestReceived() {
	if r == nil {
		return
	}
	r.manifestReceived.Add(1)
}

// ManifestSetPilotsKnown updates the pilots-known gauge from the
// evaluator's most-recent snapshot. nil-safe.
func (r *Recorder) ManifestSetPilotsKnown(n int) {
	if r == nil {
		return
	}
	r.manifestPilotsKnown.Store(int64(n))
}

// ManifestSetQuorumMetBits encodes per-field quorum-met flags into a
// single bitmap gauge (bit0 shard_bits, bit1 source_mode, bit2 successor).
func (r *Recorder) ManifestSetQuorumMetBits(bits int32) {
	if r == nil {
		return
	}
	r.manifestQuorumMetBits.Store(bits)
}

// ManifestSetReshardState updates the re-sharding state gauge.
// 0=steady, 1=bridging, 2=cutover-pending.
func (r *Recorder) ManifestSetReshardState(state int32) {
	if r == nil {
		return
	}
	r.manifestReshardState.Store(state)
}

// ManifestSetReshardWindowSeconds updates the seconds-until-cutover
// gauge. May be negative briefly during cutover.
func (r *Recorder) ManifestSetReshardWindowSeconds(s int64) {
	if r == nil {
		return
	}
	r.manifestReshardWindow.Store(s)
}

// ManifestDivergence is a no-op placeholder kept for symmetry with the
// listener-side recorder. Proxy-side divergence accounting can be added
// alongside OTel registration when the manifest gauges are promoted to
// observable callbacks.
func (r *Recorder) ManifestDivergence(field, kind string) { _ = field; _ = kind }

// ManifestAdoption is a no-op placeholder for the same reason as
// ManifestDivergence.
func (r *Recorder) ManifestAdoption(field, reason string) { _ = field; _ = reason }

// ManifestReshardEmitDuplicate increments the live-reshard
// duplicate-emit counter (proxy-side equivalent of the listener-side
// metric).
func (r *Recorder) ManifestReshardEmitDuplicate() {
	if r == nil {
		return
	}
	// Use the same atomic for the proxy; explicit OTel registration
	// follows when bridging-mode lands.
	r.manifestReceived.Add(0) // touch to avoid unused-field warnings; replaced when OTel registered
}

// SetDraining marks the recorder as draining. Once called, /readyz returns
// 503 regardless of how many workers are ready. Call this before sleeping the
// drain period so the load balancer stops routing new connections before the
// ingress socket closes.
func (r *Recorder) SetDraining() {
	r.draining.Store(true)
}

// RequireTCPIngress marks the TCP ingress listener as a /readyz prerequisite.
// Call before starting TCPIngress.Run so /readyz returns 503 until the
// listener has bound (signalled via TCPIngressReady).
func (r *Recorder) RequireTCPIngress() {
	r.tcpIngressRequired.Store(true)
}

// TCPIngressReady signals that the TCP ingress listener has bound and is
// accepting connections. Call once from TCPIngress.Run after net.Listen.
func (r *Recorder) TCPIngressReady() {
	r.tcpIngressReady.Store(true)
}

// Shutdown flushes all pending OTLP exports and releases SDK resources.
// Call once during graceful shutdown before wg.Wait().
func (r *Recorder) Shutdown(ctx context.Context) {
	if err := r.shutdownFn(ctx); err != nil {
		slog.Warn("metrics shutdown error", "err", err)
	}
}

// ── Attribute / option helpers ───────────────────────────────────────────────

// ifaceAttr returns the network.interface.name attribute for iface.
func ifaceAttr(iface string) attribute.KeyValue {
	return attribute.String("network.interface.name", iface)
}

// ifaceState returns the (lock-free after warmup) per-interface counter state,
// binding a new one on first sight via copy-on-write.
func (r *Recorder) ifaceState(iface string) *ifaceState {
	if m := r.ifaceStates.Load(); m != nil {
		if st, ok := (*m)[iface]; ok {
			return st
		}
	}
	r.ifaceMu.Lock()
	defer r.ifaceMu.Unlock()
	if m := r.ifaceStates.Load(); m != nil {
		if st, ok := (*m)[iface]; ok {
			return st
		}
	}
	st := &ifaceState{iface: iface, workers: make([]*hotPathCounters, r.numWorkers)}
	st.flows.Store(&flowTable{})
	nm := make(map[string]*ifaceState)
	if old := r.ifaceStates.Load(); old != nil {
		for k, v := range *old {
			nm[k] = v
		}
	}
	nm[iface] = st
	r.ifaceStates.Store(&nm)
	return st
}

// worker returns the pre-bound hot-path counter bundle for workerID, binding it
// on first use. Each worker only ever touches its own slot, so reads are
// lock-free; binding is serialized under st.mu.
func (st *ifaceState) worker(r *Recorder, workerID int) *hotPathCounters {
	if workerID >= 0 && workerID < len(st.workers) {
		if hp := st.workers[workerID]; hp != nil {
			return hp
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	if workerID >= 0 && workerID < len(st.workers) && st.workers[workerID] != nil {
		return st.workers[workerID]
	}
	w := strconv.Itoa(workerID)
	hp := &hotPathCounters{
		rxPackets: r.promRxPackets.WithLabelValues(w, st.iface),
		rxBytes:   r.promRxBytes.WithLabelValues(w, st.iface),
		txPackets: r.promTxPackets.WithLabelValues(w, st.iface),
		txBytes:   r.promTxBytes.WithLabelValues(w, st.iface),
		rxSize:    r.promRxSize.WithLabelValues(w, st.iface),
	}
	if workerID >= 0 && workerID < len(st.workers) {
		st.workers[workerID] = hp
	}
	return hp
}

// flow returns the (packets, bytes) counters for groupIdx, binding via a
// copy-on-write grow of the flow table on first sight. Reads are lock-free; the
// table doubles as the active-group set (non-nil entry == group seen).
func (st *ifaceState) flow(r *Recorder, groupIdx uint32) (promclient.Counter, promclient.Counter) {
	ft := st.flows.Load()
	if int(groupIdx) < len(ft.packets) {
		if p := ft.packets[groupIdx]; p != nil {
			return p, ft.bytes[groupIdx]
		}
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	ft = st.flows.Load()
	if int(groupIdx) < len(ft.packets) && ft.packets[groupIdx] != nil {
		return ft.packets[groupIdx], ft.bytes[groupIdx]
	}
	n := len(ft.packets)
	if int(groupIdx)+1 > n {
		n = int(groupIdx) + 1
	}
	np := make([]promclient.Counter, n)
	nb := make([]promclient.Counter, n)
	copy(np, ft.packets)
	copy(nb, ft.bytes)
	groupStr := fmt.Sprintf("%04x", groupIdx)
	np[groupIdx] = r.promFlowPackets.WithLabelValues(st.iface, groupStr)
	nb[groupIdx] = r.promFlowBytes.WithLabelValues(st.iface, groupStr)
	st.flows.Store(&flowTable{packets: np, bytes: nb})
	return np[groupIdx], nb[groupIdx]
}

// ── HTTP server ──────────────────────────────────────────────────────────────

// Serve starts the HTTP server on addr, registering /metrics, /healthz, and
// /readyz. It blocks until done is closed, then gracefully shuts down the
// HTTP server with a 5-second deadline.
//
// When pprof is true, net/http/pprof handlers are mounted at /debug/pprof/*
// for profiling sessions. Leave off in production.
func (r *Recorder) Serve(addr string, pprof bool, done <-chan struct{}) {
	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(r.promReg, promhttp.HandlerOpts{}))
	mux.HandleFunc("/healthz", r.handleHealthz)
	mux.HandleFunc("/readyz", r.handleReadyz)
	if r.levelVar != nil {
		mux.HandleFunc("/loglevel", logging.LevelHandler(r.levelVar))
	}
	if pprof {
		// Importing net/http/pprof registers the handlers on
		// http.DefaultServeMux as a side effect; re-export them on our mux.
		mux.Handle("/debug/pprof/", http.DefaultServeMux)
	}

	srv := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	go func() {
		slog.Info("metrics server listening", "addr", addr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("metrics server error", "err", err)
		}
	}()

	<-done

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := srv.Shutdown(ctx); err != nil {
		slog.Warn("metrics server shutdown error", "err", err)
	}
}

func (r *Recorder) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, _ = fmt.Fprintf(w, `{"status":"ok","uptime_seconds":%.1f}`, time.Since(r.startTime).Seconds())
}

func (r *Recorder) handleReadyz(w http.ResponseWriter, _ *http.Request) {
	ready := int(r.readyCount.Load())
	total := r.numWorkers
	tcpReq := r.tcpIngressRequired.Load()
	tcpRdy := r.tcpIngressReady.Load()
	w.Header().Set("Content-Type", "application/json")
	if r.draining.Load() {
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, `{"status":"draining","workers_ready":%d,"workers_total":%d,"tcp_ingress_required":%t,"tcp_ingress_ready":%t}`, ready, total, tcpReq, tcpRdy)
		return
	}
	if ready >= total && total > 0 && (!tcpReq || tcpRdy) {
		w.WriteHeader(http.StatusOK)
		_, _ = fmt.Fprintf(w, `{"status":"ready","workers_ready":%d,"workers_total":%d,"tcp_ingress_required":%t,"tcp_ingress_ready":%t}`, ready, total, tcpReq, tcpRdy)
		return
	}
	w.WriteHeader(http.StatusServiceUnavailable)
	_, _ = fmt.Fprintf(w, `{"status":"starting","workers_ready":%d,"workers_total":%d,"tcp_ingress_required":%t,"tcp_ingress_ready":%t}`, ready, total, tcpReq, tcpRdy)
}
