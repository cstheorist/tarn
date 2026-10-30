# Gateway topology resource limits

`pool_size` is a per-leader maximum. A gateway opens one socket on demand
and grows a shard's pool only when its existing clients have external
owners. Loading membership and bucket ownership still contacts every live
group at startup; it does not create a full pool for every group. A gateway
supports at most 1,024 stable shard positions, including removed positions.

The optional `topology` object has these defaults:

```json
"topology": {
  "max_connections": 256,
  "completion_connections": 16,
  "idle_secs": 30,
  "fanout": 8,
  "active_branches": 64,
  "queued_branches": 256,
  "control_reply_bytes": 67108864,
  "response_bytes": 268435456,
  "response_frame_bytes": 33554432,
  "stats_shards_per_tick": 8
}
```

Connecting sockets, leader probes, capability discovery, obsolete pools
and sockets whose I/O owners are retiring share `max_connections`. Normal
discovery leaves `completion_connections` slots unused. An existing
completion can use that reserve; a newly discovered completion connection
is not installed in the ordinary pool. Existing RPC count/byte completion
reserves remain separate. These are admission bounds, not a guarantee of
progress through unavailable storage or exhausted response capacity.

At pressure, least-recently-used pool clients are evicted only when the
pool owns their sole handle and they have no outstanding requests. A
transaction between statements still owns its client. A cancelled caller
with a pending reply still pins its socket. Eviction never closes either
kind of owner. An idle sweep runs with the existing maintenance worker,
at most ten seconds after the configured idle age when that worker is
being scheduled. Closing a socket does not release its budget until both
physical I/O halves drop. Saturation can therefore refuse discovery with
1205/HY000 even after a handle was removed from a pool.
Pressure eviction waits up to 50 ms for physical retirement before trying
another idle owner; it never refunds the slot itself. Completion discovery
bypasses the ordinary failed-discovery backoff.

Read fanouts poll at most `fanout` branches per operation and share FIFO
admission for `active_branches` across the gateway. The waiting set has
`queued_branches` entries; overflow is an explicit 1205 before that branch
runs. A branch holds its slot across its storage continuations. Completion
RPCs keep their separate transport reserve. Bucket/DDL broadcasts also
use bounded windows, retaining existing protocol ordering and error rules.
Statement read sets, native handback ownership and validation are unchanged.
Before opening streams, a serializable read checks the distinct union of
its shards and existing transaction streams against half the ordinary
socket capacity: `(max_connections - completion_connections) / 2` (120 by
default). It fails with 1205 before pinning the requested set if that set
would exceed the limit. This leaves ordinary capacity for other work;
several queries can still overload the aggregate budget and receive an
explicit refusal. Earlier streams are not evicted and a prefix is never
acknowledged. RC scans release each branch's client and can visit larger
sets through their bounded window. These are resource refusals, not a
change in the successful read's validation or freshness contract.

`response_frame_bytes` rejects an advertised frame before allocating its
payload. The existing SQL transport envelope from #134 independently limits
the frame payload to 8 MiB; the stricter limit wins. Read-only budget helpers
retain their explicit resource error, while uncertain mutations retain 40003. `response_bytes` is a gateway-wide bound on charged encoded
frames, including partially received replies and completed bodies retained
by zero-copy field slices. Completed bodies are copied into an independently
owned allocation so a tiny field cannot retain an uncharged large reader
buffer. The charge follows the last `Bytes` owner. At exhaustion the affected
physical connection retires: requests have already been sent, so a mutation
retains outcome-unknown handling, never a pre-effect admission error or an
automatic replay. This introduces a copy on gateway responses; component
and end-to-end costs must be measured under the same durability contract.

These limits are not a whole-process memory bound. Reader scratch buffers,
the temporary body copy, TCP/kernel buffers, handshake buffers, decoded
scalar row arrays, SQL plans/joins/sorts, transaction buffers and allocator
overhead are distinct. The separate query and transaction accounting limits from #134 also apply.
The number of physical scratch buffers is socket-bounded; each frame is
size-bounded. `control_reply_bytes` separately limits retained bucket-report
payloads. An over-budget response fails the refresh without publishing an
incomplete map. At most a fanout window of completed replies can wait behind
an earlier reply; these still carry their global encoded-byte charges.

Concurrent catalog reloads now share the reload lock, including the poller.
Concurrent membership callers share successful refreshes even if the
membership version did not change. Failed leader discovery has a 100 ms
per-shard backoff. Lazy and Vinyl diagnostics each poll a rotating maximum
of `stats_shards_per_tick` groups per second, rather than scale their rate
with topology size. Sample timestamps remain actual observations: a cached
zero depth is not a barrier. Bucket/membership epoch and schema checks remain
authoritative; no routing hierarchy or ownership rule changes.

`SHOW GLOBAL STATUS` includes `Tarn_storage_sockets`, `_sockets_peak`,
`_connect_attempts`, `_socket_refusals`, `Tarn_fanout_active`, `_peak`, and
`Tarn_response_bytes`, `_bytes_peak`, `_refusals`. Socket attempts include
failed and cancelled handshakes. `SHOW TARN RPCS` continues to expose
connection/request observations; those observations are not the socket
admission authority and can omit already retired sockets.
`Tarn_storage_rpc_enqueued`, `_bytes_written` and `_bytes_read` retain
lifetime traffic across evicted connections. They exclude the handshake;
written bytes count completed write batches, excluding a failed partial
write. Requests count enqueue, not transaction outcome. The
`Tarn_topology_catalog_refreshes`, `_member_refreshes` and `_bucket_refreshes`
counters expose completed refresh work. Idle fixture intervals measure
background traffic by subtraction; deadlock polling is explicitly disabled
in this fixture and remains a separate full-graph operation with a bounded
RPC window, proportional to the number of live groups.

Run `GROUPS_COUNT=8 tests/topology.sh` (also 32 or 128) with the normal
release binaries and Tarantool 3.9.0. Every group is a real storage process;
two gateways share it. The fixture retains configuration, binaries,
all paired samples when `BASELINE_BIN` is supplied, errors, RSS, socket
observations and high-water counters. Exact point reads cover every group;
successful scatter results must equal the complete expected set. Runtime
scaling on one host is not independent-host performance evidence. The CI
matrix includes RF1 at 8/32/128 groups, RF3 at eight groups and pool maxima
of one and four. It compares a frozen baseline under the same release
profile, durability and paced load. Missed scheduled slots and actual
elapsed time are retained; a slow run is not reported as full admitted load.

`simulated_1024_group_refresh_coalesces_two_gateways_under_physical_caps`
exercises the actual lazy discovery, response accounting, membership decode
and refresh locks of two configured gateways. One fake wire peer represents
1,024 logical groups. Eight simultaneous refresh callers per gateway must
share one successful full round, with complete membership and socket,
response and fanout limits intact. Its printed payload/traffic/peak counts
are control-plane simulation evidence, not 1,024 real storage groups or
data-plane throughput. Configurations above the 1,024-position limit fail
before opening I/O.

## Measured envelope and retained evidence

The primary integrated study is [run 40972749238](https://github.com/cstheorist/tarn/actions/runs/40972749238):
implementation `3537a7e` against frozen main `a885dc4`. Both gateways use the
normal release profile, Tarantool 3.9.0, the same native storage module,
`fsync`, and identical tables/configuration. Empty Vinyl and lazy-index
tables keep both diagnostic workers active. Eight already-connected clients
schedule 200 point reads/second in two-second windows. Five alternating
AB/BA pairs per configuration each admitted all 400 scheduled reads in
both modes, with zero errors or missed slots. A second automatic PR cohort
also completed all six configurations and every scheduled read.

The table reports medians across the five runs; RSS is a per-gateway
observation after load, not peak process memory. Socket brackets show the
observed range. Latencies are each run's p99 over 400 reads, then the median
of those five p99s. Startup is elapsed time until all configured gateways
accept SQL, including initial membership and bucket loading. The last column
is per-gateway idle traffic with active pollers, measured over two seconds;
bytes exclude handshakes and use the lifetime counters described above.

| Groups / RF / pool / gateways | Sockets, baseline → candidate | RSS MiB, baseline → candidate | Point p99 ms, baseline → candidate | Candidate startup ms | Candidate idle RPC/s / read B/s / written B/s |
| --- | --- | --- | --- | --- | --- |
| 8 / 1 / 1 / 2 | 8 → 8 | 31.94 → 34.50 | 0.636 → 0.648 | 46.8 | 11.0 / 1692 / 349 |
| 8 / 1 / 4 / 1 | 32 → 8–9 | 40.39 → 34.76 | 0.554 → 0.589 | 24.9 | 11.0 / 1694 / 351 |
| 8 / 1 / 4 / 2 | 32 → 8–9 | 40.10 → 34.68 | 0.627 → 0.701 | 32.0 | 11.0 / 1694 / 349 |
| 8 / 3 / 4 / 2 | 32 → 8–9 | 39.85 → 34.72 | 2.309 → 3.053 | 64.3 | 11.2 / 1702 / 358 |
| 32 / 1 / 4 / 2 | 128 → 16 | 65.05 → 44.90 | 3.292 → 3.253 | 238.5 | 18.5 / 2035 / 599 |
| 128 / 1 / 4 / 2 | 512 → 16 | 169.08 → 50.49 | 7.446 → 1.520 | 294.5 | 18.9 / 2048 / 612 |

These short, single-host runs establish a resource envelope, not a general
throughput improvement. Small-topology p99 sometimes regressed, and the
pool-one case used more RSS. With more groups than the ordinary socket
budget, diagnostic polling also incurs discovery work; it remains bounded
by the fixed polling budget. The baseline has no lifetime traffic counters,
so its idle bytes are not inferred from the candidate. Deadlock polling is
disabled here; its full-graph cost remains proportional to topology size.
The measured real-storage envelope is 128 RF1 groups or eight RF3 groups,
not 1,024 real groups or an independent-host availability claim.

The 1,024-group fake-wire simulation coalesces eight concurrent refresh
callers into one successful membership round per gateway. Each of its two
gateways opens 1,024 connections over the round, peaks at 16 physical
sockets, sends 3,072 requests, reads 47,907,840 encoded bytes and writes
95,232 bytes. Its response high-water mark is 47,864,832 bytes and returns
to zero. The integrated macOS debug run took 31.13 seconds; this is control
metadata/capacity evidence, not a production refresh latency claim.

[The evidence index](../tests/evidence/gateway-topology/index.json) retains
source and binary identities, every paired sample, configuration, raw logs,
summary ranges, both integrated cohorts, earlier cohorts and CI failures.
The earlier plain-memtx/cold-client study has missed scheduled slots and is
not used in the table. Its RF3 startup failure, the original insufficient
join-fixture capacity, and the old compaction-witness failure remain recorded.
The successful [complete integrated CI run](https://github.com/cstheorist/tarn/actions/runs/40972723729)
passed native and Lua storage, SQL differentials, query budgets, RPC
saturation, oversized post-mutation frames, movement/join/departure,
recovery and RF3 failover on memtx and Vinyl. All 284 formal configurations
produced their expected outcomes; Verus verified 140 bodies with zero
errors. An additional manual CI run hit a matched 1213 in the existing
backup fixture's direct Lua `lazy_drain`; the complete PR run passed that
same fixture unchanged. It is recorded, not counted as another full pass.

The separately retained integrated RF3 Docker test disconnects the former
leader's replication network while keeping SQL reachable. The majority
acknowledges one increment submitted exactly once. The isolated raw tuple
remains old, SQL refuses the read, and after healing both restarted gateways
read the increment exactly. Socket peaks stay within eight, and the fixture
removes all four owned containers. Reproduce with compatible normal Linux
release binaries inside the checkout and an image containing Tarantool 3.9:

```sh
python3 tests/sql/topology_partition.py --image YOUR_TARANTOOL_IMAGE \
  --gateway target/release/tarn-server --native target/release/libtarn_storage.so \
  --out /absolute/path/inside/checkout/target/partition-evidence
```

For the matched real-group fixture, `GATEWAYS=1` or `2`, `POOL_SIZE=1` or `4`,
`REPLICAS=1` or `3`, `GROUPS_COUNT=8`, `32` or `128`, and `BASELINE_BIN` select
the published configurations. It preserves failed attempts as well as
successful samples. The response-copy cost, periodic discovery churn,
uncounted allocation classes, and explicit serializable pinned-set refusal
remain part of the supported contract; no routing hierarchy is justified by
these measurements.
