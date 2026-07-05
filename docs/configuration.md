# Configuration Reference

All parameters are accepted from CLI flags first; environment variables serve
as fallbacks; hard-coded defaults apply when neither is present.

## Flags and Environment Variables

| Flag | Env var | Default | Description |
|------|---------|---------|-------------|
| `-listen` | `LISTEN_ADDR` | `[::]` | Ingress bind address (without port) |
| `-udp-listen-port` | `UDP_LISTEN_PORT` | `8725` | UDP listen port for incoming BSV transaction frames (BRC-12, BRC-124, or BRC-128) |
| `-tcp-listen-port` | `TCP_LISTEN_PORT` | `0` | TCP ingress port for reliable delivery (0 = disabled) |
| `-miner-listen-port` | `MINER_LISTEN_PORT` | `0` | UDP miner ingress for privileged frames (BRC-131 block / BRC-133 coinbase / BRC-132 subtree data); 0 = disabled. See [Miner-tier ingress gate](#miner-tier-ingress-gate) |
| `-miner-tcp-listen-port` | `MINER_TCP_LISTEN_PORT` | `0` | TCP miner ingress for privileged frames; 0 = disabled |
| `-tx-accept-privileged` | `TX_ACCEPT_PRIVILEGED` | `false` | Let the user transaction ingress also accept privileged frames (legacy single-port behaviour). `false` = the user port is transaction-only |
| `-require-block-pow` | `REQUIRE_BLOCK_POW` | `false` | Gate BRC-131 block announces on a cheap stateless proof-of-work check of the in-frame header. Permissionless (validates work, not identity). See [Block-announce proof-of-work](#block-announce-proof-of-work) |
| `-min-pow-bits` | `MIN_POW_BITS` | `0` | PoW difficulty floor in Bitcoin compact `nBits` form (e.g. `0x1d00ffff`); `0` = header self-consistency only (weak) |
| `-iface` | `MULTICAST_IF` | `eth0` | Comma-separated NIC names for multicast egress |
| `-egress-port` | `EGRESS_PORT` | `9001` | Destination UDP port for multicast groups |
| `-egress-hoplimit` | `EGRESS_HOPLIMIT` | `1` | IPv6 multicast hop limit (`IPV6_MULTICAST_HOPS`) written on egress. `1` = single L2 segment; raise above 1 for routed/tunneled mesh fabrics where the datagram must cross ip6gre / PIM hops |
| `-egress-loop` | `EGRESS_MULTICAST_LOOP` | `false` | Enable `IPV6_MULTICAST_LOOP` on egress. Required on collapsed / mesh-router nodes so locally-originated multicast is picked up by the kernel MFC and forwarded to tunnel / consumer OIFs. Off for plain L2 egress |
| `-shard-bits` | `SHARD_BITS` | `2` | Key bit width (1–12 per BRC-129) |
| `-scope` | `MC_SCOPE` | `site` | Multicast scope: `link` \| `site` \| `org` \| `global` |
| `-mc-group-id` | `MC_GROUP_ID` | `0x000B` | IANA group-id (bytes 12–13); default = IANA Bitcoin allocation `FF0X::B` |
| `-source-mode` | `SOURCE_MODE` | `asm` | Multicast addressing model: `asm` (FF0x; default) or `ssm` (FF3x per RFC 4607). SSM derives the prefix via `shard.Prefix(SSM, scope)` → FF35 site / FF3E global; RFC 8815 deprecates ASM at global scope and is rejected. Requires PIM-SSM in the fabric. See [SSM Support Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#source-specific-multicast-ssm). |
| `-bind-source` | `BIND_SOURCE` | `""` | IPv6 literal bound on every multicast egress socket via `syscall.Bind` in `openEgressSocket`. **Required when `-source-mode=ssm`** and MUST be distinct per replica — anycast/ECMP-shared sources break PIM-SSM RPF. For single-identity deployments use VRRP active-standby. |
| `-stamp-source` | `STAMP_SOURCE` | `true` | Authoritatively stamp the BRC HashKey from the **observed** packet source IP, overriding any sender-supplied value, so the per-flow identity is the real ingress source. This is what makes own-traffic exclusion per-consumer at a direct/collapsed edge (the proxy sees each consumer's distinct source). Set `false` **only** behind a source-rewriting load balancer, where every consumer would otherwise appear as the LB address and the upstream-supplied HashKey is trusted instead. |
| `-workers` | `NUM_WORKERS` | `runtime.NumCPU()` | Worker goroutine count (0 = NumCPU) |
| `-debug` | `DEBUG` | `false` | Enable per-packet debug logging and multicast loopback |
| `-drain-timeout` | `DRAIN_TIMEOUT` | `0s` | Pre-drain delay before closing sockets; `/readyz` returns 503 during this window (`0s` = disabled) |
| `-metrics-addr` | `METRICS_ADDR` | `:9100` | HTTP bind address for `/metrics`, `/healthz`, `/readyz` |
| `-instance` | `INSTANCE_ID` | hostname | OTel `service.instance.id` for federation |
| `-otlp-endpoint` | `OTLP_ENDPOINT` | `""` | OTLP gRPC endpoint (empty = disabled) |
| `-otlp-interval` | `OTLP_INTERVAL` | `30s` | OTLP push interval |
| `-log-format` | `LOG_FORMAT` | `text` | Log output format: `text` (stderr, dev default) or `json` (stdout, for fleet aggregation). See [Unified Logging](https://github.com/lightwebinc/shard-common/blob/main/docs/logging.md) |
| `-log-level` | `LOG_LEVEL` | `info` | Log level: `debug` \| `info` \| `warn` \| `error`. Runtime-togglable via `POST /loglevel?level=` and SIGHUP. `-debug` is a deprecated alias for `debug` |
| `-trace-sampling` | `TRACE_SAMPLING` | `0` | Distributed-trace head sampling ratio `0`–`1` (`0` = tracing off, no-op tracer; exports via `-otlp-endpoint`; control-plane only, never the packet hot path) |
| `-frag-mtu` | `FRAG_MTU` | `0` | Path MTU for BRC-130 fragmentation (0 = disabled) |
| `-coalesce` | `COALESCE` | `false` | Opt-in BRC-142 origin-side frame coalescing (pack many small same-`(group, subtree)` tx per datagram to cut egress pps). See [BRC-142 Coalescing](#brc-142-coalescing-origin-side) |
| `-coalesce-max-bytes` | `COALESCE_MAX_BYTES` | `1500` | Max coalesced bundle datagram size in bytes (1500 = Ethernet MTU baseline; 9000 for jumbo on a controlled underlay) |
| `-coalesce-max-members` | `COALESCE_MAX_MEMBERS` | `0` | Max member transactions per bundle (`0` = MTU-bound only) |
| `-coalesce-carry-txid` | `COALESCE_CARRY_TXID` | `false` | Carry each member's 32-byte TxID on the wire (for downstream dedup / operator accounting) instead of recomputing it on receipt |
| `-recv-batch` | `BSP_RECV_BATCH` | `32` | Datagrams per `recvmmsg` syscall (1 = per-packet legacy path) |
| `-recv-buf-bytes` | `BSP_RECV_BUF_BYTES` | `0` | Per-worker `SO_RCVBUF` in bytes (`0` = system default; capped by `net.core.rmem_max`) |
| `-ingress-dedup` | `INGRESS_DEDUP` | `true` | Enable ingress TxID dedup. `false` bypasses the dedup gate entirely — only sound for single-proxy ingest topologies. See [Ingress TxID Deduplication](#ingress-txid-dedup) |
| `-pprof` | `BSP_PPROF` | `false` | Mount `net/http/pprof` at `/debug/pprof/*` on the metrics server (profiling only) |

---

## Ingress Modes

The proxy supports two ingress transports. Both feed the same forwarding
pipeline; you may run both simultaneously.

### UDP ingress (default)

UDP ingress uses `SO_REUSEPORT` to distribute incoming datagrams across all
worker goroutines with no userspace coordination. Each worker pulls up to
`-recv-batch` datagrams per `recvmmsg(2)` syscall and flushes the
corresponding egress queue once per batch via `sendmmsg(2)` (one syscall per
target interface), amortising syscall cost across the batch. On platforms
without `recvmmsg`/`sendmmsg` (macOS, FreeBSD) the `golang.org/x/net/ipv6`
library transparently falls back to per-packet send/recv; the proxy is
functionally identical but does not gain the syscall-amortisation speed-up.
This is the high-throughput path.

```
-udp-listen-port 8725   # (default)
```

### TCP ingress (optional)

TCP ingress provides reliable, ordered delivery for senders that require it
(e.g. over lossy links). Each accepted connection carries a stream of BRC-12, BRC-124, or BRC-128
frames concatenated end-to-end. The proxy reads 44 bytes first, then extends
to 92 bytes if BRC-124 (`PayLen` at bytes 88–91), then reads `PayLen` payload bytes.

TCP ingress is disabled by default. Enable it with:

```
-tcp-listen-port 9100
```

Both transports can run at the same time:

```
shard-proxy \
  -iface eth0 \
  -udp-listen-port 8725 \
  -tcp-listen-port 9100
```

---

## Miner-tier ingress gate

Block announcements (BRC-131), coinbase transactions (BRC-133), and subtree
data (BRC-132) are privileged control-plane frames: each egresses to a
broadcast group every subscriber receives. Only miner-tier peers should emit
them — end-user / service consumers (which submit ordinary transactions) must
not be able to announce blocks, including via a bridged libp2p path.

The proxy enforces this as a **per-socket frame-class gate**, so a single edge
can serve miners and ordinary consumers interleaved (the separation is the
port, not the host):

| Socket | Flag | Accepts |
|--------|------|---------|
| User / transaction ingress | `-udp-listen-port` (8725), `-tcp-listen-port` | BRC-12 / 124 / 128 transactions + BRC-134 anchor. Privileged BRC-131/133/132 frames are **dropped** and counted (`bsp_privileged_frame_rejected_total`). |
| Miner ingress | `-miner-listen-port`, `-miner-tcp-listen-port` | Everything, including BRC-131/133/132. |

Opening a miner port is the proxy's "accept block/coinbase/subtree data?"
switch — leave both at `0` and the proxy ingests transactions only. Expose the
miner port to miner-tier peers alone, over their tunnels and/or with a firewall
source restriction; the user port stays open to all consumers as today. The
operator's control plane / firewall policy decides which peers may reach the
miner port.

```
shard-proxy \
  -iface eth0 \
  -udp-listen-port 8725 \      # consumers: transactions only
  -miner-listen-port 9000      # miners: privileged frames, tunnel-only
```

`-tx-accept-privileged` (default `false`) restores the legacy single-port
behaviour where the user port also accepts privileged frames — use it for
collapsed/dev nodes that ingest everything on one socket.

> **Upgrade note:** the default is now secure (transaction-only user port). A
> single-port deployment that relied on ingesting block/coinbase/subtree data
> on 8725 must either configure a `-miner-listen-port` or set
> `-tx-accept-privileged=true`.

The gate is the application-layer enforcement point; it holds even if a
network ACL is misconfigured. See [DESIGN.md § Ingress
Authorization](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#ingress-authorization-miner-tier-gate).

---

## Block-announce proof-of-work

The miner-tier gate above is **admission control** — a domain-local policy
about whose traffic may use this operator's resources. It is identity/network
based, which is permissioned by nature and does not generalise across domains.

The proof-of-work gate is the **permissionless** complement: it validates the
*artifact*, not the emitter. With `-require-block-pow`, a BRC-131 block
announce is forwarded only if its in-frame 80-byte header hashes under the
target its own `nBits` claims, and that target is at least as hard as
`-min-pow-bits` (the difficulty floor). Anyone may announce a block; forging a
passing header costs work proportional to the floor, while the proxy verifies
it with a single double-SHA256 — that asymmetry is the spam gate, with no
allowlist and nothing to coordinate across domains.

```
-require-block-pow -min-pow-bits 0x1d00ffff
```

Scope and limits:
- Applies to **block announces only**. Coinbase (BRC-133) and subtree data
  (BRC-132) carry no in-frame header, so they stay on the admission gate.
- Set a real `-min-pow-bits` floor in production: `0` only checks the header is
  self-consistent, which a forger satisfies trivially by claiming easy
  difficulty.
- This is **not** full consensus validation (no chain context — it cannot
  verify `nBits` is the correct retarget for the height). It rejects spam at
  ingress; the consuming node (Teranode) does full validation.

Rejections increment `bsp_block_pow_rejected_total`.

---

## Shard Bits

`-shard-bits N` configures the number of txid prefix bits used to derive the
multicast group index. The total number of groups is 2^N.

| Bits | Groups | Typical use |
|------|------------|--------------------|
| 1 | 2 | Minimal / testing |
| 2 | 4 | Default |
| 8 | 256 | Medium deployments |
| 12 | 4 096 | Maximum (BRC-129 bound) |

BRC-129 zoning bounds shard group indices to `0x0000`–`0x0FFF`, so conformant
deployments use at most 12 bits. (The flag validator currently accepts up to
15 for lab use.)

Increasing bits by 1 splits every existing group into two child groups
(consistent hashing). Subscribers need only join additional groups.

---

## Forwarding

For **BRC-124/BRC-128 frames**, if `SeqNum` (bytes 48–55) is already non-zero the
sender has pre-stamped the frame and the proxy forwards it verbatim. If `SeqNum`
is zero the proxy stamps `HashKey` (bytes 40–47) as
`XXH64(senderIPv6 ∥ groupIdx ∥ subtreeID)` and `SeqNum` (bytes 48–55) as a
monotonic per-flow counter, in-place. The `SubtreeID` field is always passed
through unchanged.

For **BRC-12 (legacy) frames**, the proxy always forwards the original bytes verbatim without
any modification.

For **BRC-134 anchor frames** (`FrameVerV6`), the proxy forwards to `GroupBlockBroadcast`
(`FF0X::B:FFFE`). Anchor frames use a virtual `groupIdx` of `0xFFF9` for HashKey derivation
so anchors are accounted as an independent flow (label `brc134`) distinct from BRC-131 block
control. See [bsv-multicast/docs/brc-134-anchor-transactions.md](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-134-anchor-transactions.md).

---

## BRC-142 Coalescing (origin-side)

Coalescing is the inverse of BRC-130 fragmentation: instead of splitting one
large frame across many datagrams, it packs **many small transactions into one
datagram** — a BRC-142 *bundle* frame (`FrameVer 0x08`, 66-byte header). It is
opt-in (`-coalesce`, off by default) and trades a small latency/CPU cost for a
large reduction in egress packets-per-second when the workload is many tiny
transactions.

Within a single receive batch the worker buckets eligible BRC-124/BRC-128
transactions by their `(sender, group, subtree)` flow and, at batch end, packs
each bucket into one or more bundle datagrams (up to the byte/member budget)
before the egress flush. Each bundle draws its `HashKey`/`SeqNum` from the **same
per-flow counter** that stamps individual frames, so a flow's bundles and any
individual frames it also emits share one contiguous sequence space — a listener
gap-tracks the `(group, subtree, HashKey)` flow uniformly regardless of frame
version, and NACK/retry treats a bundle as a single "fat frame."

**Origin-only rule.** Coalescing runs **only at the emitting (origin) proxy** —
the collapsed or ingress edge that first sees the individual transactions. A
regional spine that *relays* an already-coalesced bundle forwards it
**verbatim**: it is not re-coalesced, not re-stamped, and not split. A bundle is
a complete, self-describing multicast frame bound to the single group it was
built for, so a relay re-emits it unchanged (one bundle in → one multicast
datagram out). Never enable origin coalescing on a
node whose role is to relay bundles.

| Flag | Env | Default | Notes |
|------|-----|---------|-------|
| `-coalesce` | `COALESCE` | `false` | Master switch. Off = every transaction egresses as its own frame (legacy) |
| `-coalesce-max-bytes` | `COALESCE_MAX_BYTES` | `1500` | Bundle datagram cap. `1500` = public-internet Ethernet MTU (realistic baseline); `9000` for jumbo on a controlled underlay. `0` also means 1500 |
| `-coalesce-max-members` | `COALESCE_MAX_MEMBERS` | `0` | Hard cap on member transactions per bundle. `0` = bounded only by `-coalesce-max-bytes` (capped by the wire `TxCount` uint16) |
| `-coalesce-carry-txid` | `COALESCE_CARRY_TXID` | `false` | When set, each member carries its 32-byte TxID on the wire (for downstream dedup / operator accounting) — an all-or-none flag across the bundle. When unset the receiver recomputes TxIDs, saving 32 B/member |

```bash
# origin edge: coalesce small tx to cut egress pps, Ethernet-MTU bundles
shard-proxy \
  -iface eth0 \
  -coalesce \
  -coalesce-max-bytes  1500 \
  -coalesce-carry-txid           # carry TxIDs for accounting/dedup downstream
```

Wire format (bundle header + member section), the `Coalescer`/`Decoalesce`/
`Rebucketer` transforms, and the all-or-none TxID flag are specified once in the
canonical BRC-142 spec — see
[`shard-common/docs/protocol.md` § BRC-142](https://github.com/lightwebinc/shard-common/blob/main/docs/protocol.md#3a-brc-142-coalescing-bundle-frame-format)
and the public
[bsv-multicast/docs/brc-142-coalescing-frame.md](https://github.com/lightwebinc/bsv-multicast/blob/main/docs/brc-142-coalescing-frame.md).
Bundles are re-aligned to a receiver's shard generation (re-bucket) and split
back to individual frames (decoalesce) at the **delivery edge** (listener), not
in this proxy.

Bundle metrics: `bsp_coalesce_bundles_total` (bundle datagrams flushed) and
`bsp_coalesce_members_total` (member transactions packed) count origin packing,
with the per-bundle member count distributed in `bsp_coalesce_members_per_bundle`.
`bsp_coalesce_flush_total{reason}` records flushes by reason — `batch` for
origin-packed bundles, `relay` for verbatim spine re-emits, `encode_error` on a
skipped bundle.

---

## Multicast Scope

| Value | Prefix | Reach |
|----------|--------|-----------------------------------------------------|
| `link` | `FF02` | Same L2 segment only |
| `site` | `FF05` | Site-local (default; crosses routers within a site) |
| `org` | `FF08` | Organisation-wide |
| `global` | `FF0E` | Internet-routable |

---

## Metrics Endpoints

The metrics HTTP server (default `:9100`) exposes:

- **`/metrics`** — Prometheus text format
- **`/healthz`** — Always `200 OK` if the process is running
- **`/readyz`** — `200` when all workers are ready; `503` while starting or draining

---

## Graceful Drain

When a shutdown signal is received the proxy performs a two-phase shutdown:

1. **Drain phase** — `/readyz` immediately returns `503` (status `"draining"`), signalling the load balancer to stop routing new connections. The process sleeps for `-drain-timeout`. Workers continue forwarding in-flight packets during this window.
2. **Quiesce phase** — The ingress socket is closed, each worker exits its receive loop, and the process waits for all goroutines to finish before exiting.

Setting `-drain-timeout 0s` (the default) skips the sleep and closes sockets immediately after marking draining — suitable for single-node or development deployments.

For production with a load balancer or BGP, set `-drain-timeout` to at least the LB health-check interval plus one check period:

```bash
# LB health-check every 5 s — allow two missed checks before closing
shard-proxy -iface eth0 -drain-timeout 15s
```

> **`TimeoutStopSec` note:** systemd will send `SIGKILL` after `TimeoutStopSec` if the process has not exited. Ensure `TimeoutStopSec > drain-timeout + 15s` (OTLP flush + worker drain buffer). The default service unit sets `TimeoutStopSec=30`.

---

## Example Invocations

### Minimal (single NIC, defaults)

```bash
shard-proxy -iface eth0
```

### Multi-NIC, custom shard bits, OTLP

```bash
shard-proxy \
  -iface eth0,eth1 \
  -shard-bits 8 \
  -udp-listen-port 8725 \
  -egress-port 9001 \
  -otlp-endpoint collector:4317
```

### With TCP ingress

```bash
shard-proxy \
  -iface eth0 \
  -udp-listen-port 8725 \
  -tcp-listen-port 9100
```

### With graceful drain (behind a load balancer)

```bash
shard-proxy \
  -iface eth0 \
  -udp-listen-port 8725 \
  -drain-timeout 15s
```

## IANA group-id

The proxy follows IANA's IPv6 multicast allocation practice (96-bit
boundary) and the IANA-assigned Bitcoin group `FF0X::B`. The `-mc-group-id`
flag configures the 16-bit group-id occupying bytes 12–13 of every
generated multicast address. The default `0x000B` produces addresses of
the form `FF0X::B:<shard_index>` (IANA Bitcoin).

```bash
./shard-proxy \
  -mc-group-id 0x000B \
  -scope site \
  -shard-bits 8
```

Operators MAY override the group-id for testing or private deployments
(e.g. `-mc-group-id 0xCAFE`). Conformant production deployments use
`0x000B`.

## Fan-out to multiple interfaces

Every forwarded datagram is written to all listed interfaces in order,
with no copying and no extra goroutines on the hot path:

```bash
./shard-proxy \
  -iface       eth0,eth1 \
  -shard-bits  8         \
  -scope       site      \
  -udp-listen-port 8725  \
  -egress-port 9001
```

## Subscriber join

Each subscriber calls `IPV6_JOIN_GROUP` (or `setsockopt MCAST_JOIN_GROUP`)
for the multicast group address(es) covering its desired shard range:

```text
FF05::B:<shard_index>             # Default (IANA Bitcoin group-id 0x000B)
FF05::CAFE:<shard_index>          # With overridden group-id 0xCAFE
```

`SHARD_BITS` is a fixed, deployment-wide setting shared by all subscribers.
Doubling `SHARD_BITS` splits every existing group into two children —
subscribers join additional groups without invalidating existing ones,
so scale-up requires no redesign.

## Ingress TxID dedup

The proxy can optionally suppress duplicate ingress frames before stamping
and multicasting. A two-tier claim store is consulted on every BRC-124/128
(V2), BRC-131 block (V4), BRC-132 subtree data (V5), and BRC-134 anchor (V6)
frame. Legacy BRC-12 (V1) frames bypass the gate.

- **Tier 1** — in-process LRU keyed by TxID, sharded across 64 stripes
  (each with its own mutex) to keep the dedup path from serialising all
  workers. Memory bounded by `-txid-dedup-local-cap` (default 1 048 576
  entries, ~50 MiB). A hit short-circuits and the frame is dropped without
  contacting Redis.
- **Tier 2** — a modular `shard-common/cache` backend `SetNX`
  (`redis|aerospike|memory`). Selected by `-txid-dedup-backend` (or inferred:
  `redis` when `-txid-dedup-redis-addr` is set, else `none`). On a tier-1 miss
  the proxy claims `<prefix><hex-txid>`; on win it forwards, on loss it drops.
  Errors fail open (frame is forwarded; a metric is recorded). See
  [`shard-common/docs/cache-backend.md`](https://github.com/lightwebinc/shard-common/blob/main/docs/cache-backend.md).

The whole gate runs per packet, so it costs CPU. After the localSet
sharding + direct-Prometheus counter work the dedup-on overhead is small
(measured ~6 % fewer pps at 256 B vs dedup-off on a 25 GbE single-host
loopback), but `-ingress-dedup=false` bypasses it entirely for
deployments that provably never ingest duplicates (a single proxy with a
single upstream feed). Leave it on for any multi-proxy or bridged topology.

### Flags

| Flag | Env | Default | Notes |
|------|-----|---------|-------|
| `-ingress-dedup` | `INGRESS_DEDUP` | `true` | `false` bypasses the entire gate. Only safe for single-proxy ingest |
| `-txid-dedup-backend` | `TXID_DEDUP_BACKEND` | inferred | `redis\|aerospike\|memory\|none`. Empty → `redis` if addr set, else `none` |
| `-txid-dedup-redis-addr` | `TXID_DEDUP_REDIS_ADDR` | `""` | Redis/Valkey/Dragonfly address; empty disables tier-2 (local-only) |
| `-txid-dedup-aerospike-hosts` | `TXID_DEDUP_AEROSPIKE_HOSTS` | `""` | Comma-separated `host:port`; required when backend=`aerospike` |
| `-txid-dedup-aerospike-namespace` | `TXID_DEDUP_AEROSPIKE_NAMESPACE` | `cache` | Aerospike namespace (must be provisioned) |
| `-txid-dedup-aerospike-set` | `TXID_DEDUP_AEROSPIKE_SET` | `bsp` | Aerospike set |
| `-txid-dedup-prefix` | `TXID_DEDUP_PREFIX` | `bsp:tx:` | Must match the local listener's `-ingress-set-prefix` for collapsed deployments |
| `-txid-dedup-ttl` | `TXID_DEDUP_TTL` | `10m` | Range 1m – 30m typical |
| `-txid-dedup-local-cap` | `TXID_DEDUP_LOCAL_CAP` | `1048576` | 0 also disables the feature; prefer `-ingress-dedup=false` for clarity |

### Topology guidance

- **Single proxy, no Redis** — leave `-txid-dedup-redis-addr` empty. The
  tier-1 LRU still suppresses local repeats (e.g. multiple upstream peers
  forwarding the same TxID).
- **Multiple proxies at one site** — point all proxies at the same Redis.
  Whichever proxy wins the SETNX multicasts; siblings drop.
- **Listener marks the ingress set** — when the local listener has
  `-mark-ingress-set` enabled, its courtesy SETNX populates the same
  namespace, so a TxID delivered via cross-site bridge prevents the local
  proxy from re-multicasting it.

### Metrics

- `bsp_ingress_deduped_total{frame_type, worker, network.interface.name}` —
  frames suppressed by the gate.
- `bsp_txid_claim_local_hit_total{prefix}` — tier-1 short-circuits.
- `bsp_txid_claim_won_total{prefix}` / `bsp_txid_claim_lost_total{prefix}` —
  tier-2 SETNX outcomes.
- `bsp_txid_claim_errors_total{prefix}` — Redis errors (fail-open).

## Auto-Shard-Config (BRC-139)

Optional consumer of BRC-139 ShardManifest announcements. Default off
(legacy behavior unchanged); opt-in via `-manifest-consumer-enabled`.
Unlike the listener, the proxy does not currently join the beacon
group — enabling auto-config adds a dedicated beacon-receive socket.
See the [Automatic Shard Configuration Plan](https://github.com/lightwebinc/bsv-multicast/blob/main/DESIGN.md#automatic-shard-configuration)
for the system-level design.

### `-manifest-consumer-enabled` / `MANIFEST_CONSUMER_ENABLED` (default: `false`)

Master switch. When true, the proxy opens an IPv6 multicast socket on
the beacon group (FFxx::B:FFFD) on the configured scope, decodes
incoming ShardManifest datagrams (MsgType 0x40; non-manifest MsgTypes
are silently dropped), and runs the manifest evaluator on a 1 s tick.

### `-manifest-bootstrap` / `MANIFEST_BOOTSTRAP` (default: `optional`)

`optional` ⇒ start with CLI/env values. `required` ⇒ refuse to bind
data-plane egress until quorum is reached for `ShardBits` (and
`SourceModeSSM` when SSM).

### `-pilot-quorum` / `PILOT_QUORUM` (default: `2`)

Minimum distinct authoritative announcers required for adoption. `1`
is permitted (logs a warning).

### `-pilot-hysteresis` / `PILOT_HYSTERESIS` (default: `0`)

Duration a candidate value must hold quorum before adoption. `0`
selects `2 × AnnounceInterval` of the candidate manifest.

### `-manifest-beacon-scope` / `MANIFEST_BEACON_SCOPE` (default: empty)

Multicast scope for the beacon-group join. Empty inherits `-scope`.

### `-manifest-beacon-port` / `MANIFEST_BEACON_PORT` (default: `9001`)

UDP port on which the proxy joins the beacon group to receive BRC-139
manifests. Matches the shard-manifest daemon's `-port`.

### `-live-resharding` / `LIVE_RESHARDING` (default: `false`)

Opt-in BRC-139 bridging mode. When false (default), a `ShardBits` or
`SourceModeSSM` adoption triggers a graceful restart (the manifest
applier writes into the internal restart-signal channel, which the
main signal-select handles as an early SIGTERM so the standard drain
path runs). When true, the applier installs a secondary
`forwarder.BridgingEngine` on the forwarder so the per-frame fast
path emits to BOTH the active and successor groups; the
listener-side egress dedup absorbs the duplicate frames.

### `-bridging-window` / `BRIDGING_WINDOW` (default: `0`)

Local floor on the bridging duration. `0` ⇒ honour the pilot's
`TransitionEpoch` verbatim.

## Helm chart

Every flag documented in this file is exposed under `.config` in the corresponding Helm chart's `values.yaml`. See the chart repository for installation snippets and the `values.schema.json` for validation rules.

Chart: [`lightwebinc/shard-proxy-helm`](https://github.com/lightwebinc/shard-proxy-helm)
