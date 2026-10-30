# Supported strong Key/Value reads

KV v1 exposes read-only, full-primary-key access to ordinary SQL tables.
It uses the same freshness, prepared-write, DDL and bucket checks as strong
SQL point reads. User tuples remain native Tarantool tuples; the API does
not introduce another stored copy or a write path.

## Guarantees

`independent_points` is the only supported consistency mode. Each point
read has the strong point-read contract, including reads of missing keys.
A batch preserves every input position, duplicate and miss, or fails as a
whole. Independent observations can see different committed revisions,
including two occurrences of the same key. Output order does not promise
an execution order across shards.

Neither a same-shard snapshot nor a transactional cross-shard snapshot is
advertised by v1. Requests for those modes, ambient stream transactions,
prefixes, secondary/lazy indexes and mutations are refused. Use SQL
transactions for a transactional snapshot.

A distributed SQL COMMIT can be acknowledged while a participant's physical
APPLY is still pending. Strong SQL and strong KV reads respect the pending
write claim and wait, complete against an eligible view, or refuse. Raw
`box.space[name].index.primary:get(key)` and IPROTO SELECT have no such
request-specific freshness/claim contract. A follower or former leader can
lag, and independent raw observations can expose the APPLY interval. See
[the commit and raw-read guarantees](design.md#21-guarantees).

## Authentication

Create a dedicated authenticated reader. Grant ordinary session/usage,
execute on only `tarn_kv_v1.meta`, `tarn_kv_v1.get` and
`tarn_kv_v1.get_many`, and READ on each permitted user space. Inherited READ
roles are supported. No EVAL, broad execute, internal `_tarn_*` access,
native mutation procedure or table WRITE grant is needed.

Registered functions execute their internal freshness/metadata work as the
definer. Before exposing metadata/data and after yielding reads, they run a
zero-row public SELECT as the authenticated caller. This uses Tarantool's
effective READ check, including role changes; it does not approximate ACLs
from a copied grant list. Tarantool keeps the original caller UID separate
from the [definer's effective UID](https://www.tarantool.io/en/doc/latest/reference/reference_lua/box_session/euid/).
The pinned [SELECT implementation](https://github.com/tarantool/tarantool/blob/8b9a0f59a95bb6c4a491e716966d9df9d5a4f6bc/src/box/box.cc#L3902)
checks READ access before the zero-row limit ends iteration.

Existing administrative `tarn`/guest superuser configurations retain their
privileges. Calling a read-only API does not revoke those privileges.
READ grants also allow raw native reads with the weaker contract above;
raw writes require separate privileges and remain outside this API.
Credentials are supplied explicitly, using a protected file or environment
variables in the example. Authentication and schema fingerprints do not
provide transport encryption; production transport/deployment work is
tracked in [#139](https://github.com/cstheorist/tarn/issues/139).

## Table and ownership handles

`tarn_kv_v1.meta` accepts one map, `{version=1, db='test', name='t'}`.
Identifiers are separate, nonempty strings of at most 64 bytes; v1 refuses
embedded NULs and dots. The requested table must have a declared `PRIMARY`
key whose native parts are integer, unsigned, string or varbinary, with
every partition column present in that key. Hidden/implicit keys,
functional/native-collation parts, and FLOAT/DOUBLE/DECIMAL/JSON primary
keys are unsupported. Other row columns use the existing SQL row codec.

Successful metadata includes:

| Field | Meaning |
|---|---|
| `table` | Frozen `{db,name,sid,ver,definition,layout}` token. |
| `definition_json` | Exact existing SQL `TableDef` JSON. |
| `format`, `primary`, `shard_key_pk` | Physical field layout, primary parts and partition-key positions. |
| `ownership` | Local `{shard_id,bucket_count,bucket_epoch,membership_version}` token. |
| `members`, `buckets` | Credential-free replica addresses and the local bucket report. |
| `limits` | Advertised request and response limits. |

`definition` fingerprints the exact stored JSON; `layout` fingerprints a
fixed-order physical-layout vector. The outer catalog version and the
JSON's historical version are bound independently. Version/epoch values
must be exact nonnegative integers at most `2^53-1`; bucket counts are
limited to 1,000,000 and space/shard IDs to their unsigned 32-bit range.

Every read names the frozen table and ownership tokens. The server derives
each bucket from its native primary key and checks actual ownership before
and after the guarded point read. A caller-supplied epoch cannot certify an
arbitrary key's placement. Metadata exports address components only;
configured URI credentials and options are not returned.

An explicit definition/incarnation fence returns contract `schema_changed`
(1412), without rows; native exceptions retain their original domain/code.
The client discards every accumulated slot. The caller explicitly
opens a new table handle and decides how to resubmit keys; the SDK never
reinterprets old positional keys under a changed primary key. Ownership-only
rediscovery can restart the whole vector under the same table token and
shared finite attempt/deadline budget.

## Reads and limits

`tarn_kv_v1.get` accepts exactly one map:

```lua
{
  version = 1,
  consistency = 'independent_points',
  table = metadata.table,
  ownership = metadata.ownership,
  key = {canonical_primary_part_1, canonical_primary_part_2},
}
```

`get_many` replaces `key` with `keys={{...},{...}}`. RPC callers supply
canonical native keys, including binary weight strings for relevant SQL
collations. The Rust client accepts logical key values and reuses Tarn's
SQL lookup/coercion and row/default codecs.

A success echoes the exact table token and returns `slots`. Each slot has
its one-based `position`, echoed canonical `key`, and `status='present'`
with a physical `tuple`, or `status='missing'`. A present row containing SQL
NULL remains distinct from a missing row. Errors contain no successful
prefix, slots or replacement table token.

| Bound | Value |
|---|---:|
| Keys per request | 1–128, including duplicate and missing keys |
| Encoded keys | 65,536 bytes |
| Encoded data reply | 1,048,576 bytes, including key echoes and framing |
| Encoded metadata reply | 1,048,576 bytes |

Flat key/type/length checks run before serializing input. A stored tuple's
size is checked before encoding it into a reply; the final tuple and full
reply are checked too. The SDK applies the data-reply bound across all
shard replies in one attempt, with metadata counted separately. An
oversized result fails; no clipped prefix is acknowledged as a batch.
The Rust SDK also caps advertised IPROTO response frames at 1 MiB plus
4 KiB of protocol overhead, before reserving their bodies. The envelope
and aggregate data limits above still apply.
The SDK's request and per-response limits are independent of SQL gateway
connection and response-memory budgets.
Storage's own allocation/tuple limits remain separate. Deadline checkpoints
do not establish a universal engine, network or quorum latency bound.

Errors retain `kind`, `domain`, original numeric `code` and `message`.
Domains are `contract`, `mysql` and `tarantool`; equal numeric values in
different domains are not interchangeable. Contract errors use code zero
except terminal `schema_changed` 1412. Kinds include `bad_request`,
`unsupported`, `limit_exceeded`, `ambient_transaction`, `schema_changed`,
`moved`, `access_denied`, `unavailable` and `read_error`. An SQLSTATE is
retained only when actually supplied. Ownership retry follows typed evidence;
it does not guess from an error message. There is no KV mutation retry.

## Rust client

`tarn_sql::kv::StrongKvClient` performs read-only discovery without starting
SQL administrative recovery or background cluster jobs. Its table handle
freezes the definition; each result includes the exact schema used to decode
its named fields. It uses existing SQL codecs for weight strings, changed
field positions and instant-column fill values. Definition hashes are opaque
server tokens: the client checks their shape and exact echo, rather than
claiming independent cryptographic verification of server metadata.

```sh
cargo run --release -p tarn-sql --example strong_kv -- \
  --seeds-file seeds.json --credentials-file credentials.json \
  --database test --table t --keys '[[7,1],[7,2],[7,1]]'
```

Seeds are `[{"id":1,"replicas":["127.0.0.1:3311"]}, ...]` and credentials
are `{"user":"reader","password":"..."}` in a protected local file.
The example emits JSON with schema, named rows/misses, typed binary/decimal
values, errors and connection statistics. There is no password argument,
EVAL, raw fallback or write mode.

## Verification boundary

`ExternalStrongKV.tla` checks external ownership/schema binding, complete
ordered vectors and the independent-points label. Its six controls reach
actual false absence, wrong-incarnation data, mixed schemas, clipped replies,
missing prepared-claim protection and a falsely advertised atomic snapshot.
It assumes the existing freshness and commit contracts; there is no composed
refinement proof. It does not implement Tarantool replication, ACLs, codecs
or MVCC.

Storage/network tests, paired SQL/SDK histories and RF3 faults establish the
runtime mapping separately. Measurements of SQL, strong KV and raw native
reads must retain their distinct contracts, RPC/byte/CPU scopes and complete
repeated evidence. The legacy `kvbench` measures raw reads and is not a
strong-KV benchmark.

## Matched read measurements

`bench/strong_kv.py` records SQL, strong KV and raw native reads separately.
It uses six balanced trial orders, batches of one or 32 independent points,
and one or 16 clients. A batch's latency is the latency of that complete
operation; its key throughput is reported separately. SQL batch 32 issues
32 independent autocommit statements, strong KV uses its bounded API, and
raw native reads pipeline full-primary-key SELECTs.

On Linux, the owned fixture can prepare and audit a fresh measurement cohort:

```sh
cargo build --release --workspace --all-targets --locked
KV_STUDY_ENGINE=memtx tests/kv.sh
KV_STUDY_ENGINE=vinyl tests/kv.sh
```

The manual CI input `strong_kv_qualification` runs native and Lua fault
histories, native and Lua terminal clock faults, and the two measurement
engines on six separate runners, with at most three running concurrently.
A failed cohort remains failed while the other isolated cohorts finish and
retain their evidence.
The optional `strong_kv_scope: histories` selects just the two fault-history
cohorts when investigating a history-fixture change.
Manual qualification keeps in-progress cohorts running; the two scopes use
separate concurrency groups so a history rerun cannot cancel measurements.

Each engine uses two RF3 shards, quorum two, synchronous spaces and fsync,
with test hooks disabled. The workload reads an unchanged 10,000-row,
64-byte-payload corpus for one hot tenant on one owning shard. This is a
read-path study, not a distributed OLTP or production-capacity result.
The fixture checks complete replica images before and after the study.
Warmup and engine caches remain part of the recorded environment; a Vinyl
cohort does not by itself establish cold-disk performance.

Raw samples, errors, source/executable identities, discovery costs and
timed RPC/protocol-byte counters are retained. CPU counters bracket each
phase and identify client, gateways, storage processes and TX threads.
TX-thread CPU is already included in storage-process CPU; those scopes
must not be added together. Protocol bytes exclude TCP headers. The SQL
and KV drivers also use different client stacks, so these observations
do not isolate SQL parsing costs. Raw reads have the weaker visibility
contract above and cannot establish an acceleration of strong reads.
