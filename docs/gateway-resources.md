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

Read fanouts poll at most `fanout` branches per operation and share FIFO
admission for `active_branches` across the gateway. The waiting set has
`queued_branches` entries; overflow is an explicit 1205 before that branch
runs. A branch holds its slot across its storage continuations. Completion
RPCs keep their separate transport reserve. Bucket/DDL broadcasts also
use bounded windows, retaining existing protocol ordering and error rules.
Statement read sets, native handback ownership and validation are unchanged.
If a transaction needs more pinned shard connections than fit, it fails
explicitly; it does not evict its earlier streams or acknowledge a prefix.

`response_frame_bytes` rejects an advertised frame before allocating its
payload. `response_bytes` is a gateway-wide bound on charged encoded
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
overhead are distinct. Query and transaction accounting is tracked in #134.
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

Run `GROUPS_COUNT=8 tests/topology.sh` (also 32 or 128) with the normal
release binaries and Tarantool 3.9.0. Every group is a real storage process;
two gateways share it. The fixture retains configuration, binaries,
all paired samples when `BASELINE_BIN` is supplied, errors, RSS, socket
observations and high-water counters. Exact point reads cover every group;
successful scatter results must equal the complete expected set. Runtime
scaling on one host is not independent-host or RF3 performance evidence.
