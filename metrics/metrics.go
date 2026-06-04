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
// Per-(interface, group) metric.MeasurementOption values are cached in a
// sync.Map keyed by an ifaceGroupKey struct. The first packet to a new
// (iface, group) pair allocates and stores the option; subsequent packets
// retrieve it with a single sync.Map Load — zero allocation after first hit.
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

// ifaceGroupKey is the composite cache key for per-(interface, group)
// OTel MeasurementOption values.
type ifaceGroupKey struct {
	iface string
	group uint32
}

// workerIfaceKey is the composite cache key for per-(interface, worker)
// hot-path Prometheus counter handles.
type workerIfaceKey struct {
	iface  string
	worker int
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

	// Per-(iface, worker) child-counter cache. Each hot-path Recorder method
	// looks up a struct of pre-bound prometheus.Counter handles, avoiding
	// the WithLabelValues lookup on every packet.
	hotPathCache sync.Map // workerIfaceKey -> *hotPathCounters

	// Active group tracking — iface → set of group indices
	activeGroupsMu sync.Mutex
	activeGroups   map[string]map[uint32]struct{}

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

	// Per-(iface, group) prometheus.Counter pair cache — used by
	// flowCounters() for [packets, bytes] handles.
	attrCache sync.Map

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
		provider:     mp,
		promReg:      promclient.Gatherers{reg, runtimeReg},
		promOtelReg:  reg,
		runtimeReg:   runtimeReg,
		numWorkers:   numWorkers,
		startTime:    time.Now(),
		activeGroups: make(map[string]map[uint32]struct{}),
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
			r.activeGroupsMu.Lock()
			defer r.activeGroupsMu.Unlock()
			for iface, groups := range r.activeGroups {
				o.Observe(int64(len(groups)),
					metric.WithAttributes(attribute.String("network.interface.name", iface)),
				)
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
	c := r.hotPath(iface, workerID)
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
	c := r.hotPath(iface, workerID)
	c.txPackets.Inc()
	c.txBytes.Add(float64(size))

	fp, fb := r.flowCounters(iface, groupIdx)
	fp.Inc()
	fb.Add(float64(size))

	r.trackGroup(iface, groupIdx)
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

// hotPath returns the cached bundle of prometheus.Counter handles for an
// (iface, worker) tuple. First call binds + stores; subsequent calls are
// one sync.Map Load. WithLabelValues internally uses a label-keyed map;
// caching the resulting Counter handles avoids that lookup per packet.
func (r *Recorder) hotPath(iface string, workerID int) *hotPathCounters {
	key := workerIfaceKey{iface: iface, worker: workerID}
	if v, ok := r.hotPathCache.Load(key); ok {
		return v.(*hotPathCounters)
	}
	w := strconv.Itoa(workerID)
	c := &hotPathCounters{
		rxPackets: r.promRxPackets.WithLabelValues(w, iface),
		rxBytes:   r.promRxBytes.WithLabelValues(w, iface),
		txPackets: r.promTxPackets.WithLabelValues(w, iface),
		txBytes:   r.promTxBytes.WithLabelValues(w, iface),
		rxSize:    r.promRxSize.WithLabelValues(w, iface),
	}
	if actual, loaded := r.hotPathCache.LoadOrStore(key, c); loaded {
		return actual.(*hotPathCounters)
	}
	return c
}

// flowCounters returns the (packets, bytes) prometheus.Counter handles for a
// per-(iface, group) flow, cached the same way as hotPath.
func (r *Recorder) flowCounters(iface string, groupIdx uint32) (promclient.Counter, promclient.Counter) {
	key := ifaceGroupKey{iface: iface, group: groupIdx}
	if v, ok := r.attrCache.Load(key); ok {
		pair := v.([2]promclient.Counter)
		return pair[0], pair[1]
	}
	groupStr := fmt.Sprintf("%04x", groupIdx)
	pair := [2]promclient.Counter{
		r.promFlowPackets.WithLabelValues(iface, groupStr),
		r.promFlowBytes.WithLabelValues(iface, groupStr),
	}
	if actual, loaded := r.attrCache.LoadOrStore(key, pair); loaded {
		p := actual.([2]promclient.Counter)
		return p[0], p[1]
	}
	return pair[0], pair[1]
}

// trackGroup records that groupIdx was observed on iface.
func (r *Recorder) trackGroup(iface string, groupIdx uint32) {
	r.activeGroupsMu.Lock()
	m, ok := r.activeGroups[iface]
	if !ok {
		m = make(map[uint32]struct{})
		r.activeGroups[iface] = m
	}
	m[groupIdx] = struct{}{}
	r.activeGroupsMu.Unlock()
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
