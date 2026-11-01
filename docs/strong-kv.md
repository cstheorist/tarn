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
provide transport encryption. The production SQL/storage profile in
[production deployment](production.md) does not enable the KV v1 client
transport or grant its procedures to the SQL service account. KV v1 currently
uses explicit TCP/Unix endpoints; verified TLS integration and a dedicated
production reader policy remain follow-up work in
[#181](https://github.com/cstheorist/tarn/issues/181).

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

A request describes its table once. The description is reused, across
requests too, while the table's confirmed catalog row, the schema version
and the space are unchanged. After each point's read the server checks the
caller's READ access, the definition fence and DDL holds, and the ownership
token. A changed catalog row, schema version or space takes the full
description, and a table that no longer matches the handle refuses the
request. The control spaces and the space's synchronous mode are checked
when the request begins and ends, and by each point's own barrier.

An explicit definition/incarnation fence returns contract `schema_changed`
(1412), without rows; native exceptions retain their original domain/code.
The client discards every accumulated slot. The caller explicitly
opens a new table handle and decides how to resubmit keys; the SDK never
reinterprets old positional keys under a changed primary key. Ownership-only
rediscovery can restart the whole vector under the same table token and
shared finite attempt/deadline budget.

Movement reports include bounded `installing`, `reclaiming` and `aborting`
phases. The SDK uses the shared bucket-map routing rules for them. A report
is a routing hint: each data RPC still checks authoritative ownership and
refuses a partial install or reclaim rather than returning incomplete rows.

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
python3 bench/strong_kv_summary.py /tmp/tarn-kv181-*/study/results/study.json
```

`bench/strong_kv_summary.py` turns an accepted study into tables. For each
batch/clients cell and path it reports the median and range of key
throughput over the six trials, the median of the trials' operation p99,
requests and bytes to storage per key, client protocol bytes per key and CPU
per key. It then pairs strong KV with SQL in each trial and reports the
median and range of their ratios: the comparison of equal contracts.

The manual CI input `strong_kv_qualification` runs native and Lua fault
histories, native and Lua terminal clock faults, and the two measurement
engines on six separate runners, with at most three running concurrently.
A failed cohort remains failed while the other isolated cohorts finish and
retain their evidence.
The optional `strong_kv_scope: histories` selects just the two fault-history
cohorts when investigating a history-fixture change, and `measurements` just
the two measurement cohorts. A measurement cohort writes its tables to the job
summary, and one notice per batch/clients cell holds that cell's summary JSON.
The Tests job runs the native and Lua fault and clock histories for every pull
request and every commit to main.
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
timed RPC/protocol-byte counters are retained. Strong KV and raw reads count
the client's own requests and bytes to storage. An SQL phase also reads its
gateway's storage counters (`Tarn_storage_rpc_enqueued`,
`Tarn_storage_bytes_written` and `Tarn_storage_bytes_read` in `SHOW STATUS`)
around the timed window; these include the gateway's background requests in
the window. CPU counters bracket each phase and identify client, gateways,
storage processes and TX threads. TX-thread CPU is already included in
storage-process CPU; those scopes must not be added together. CPU per key
includes each process's background work in the window, which weighs more on
a slower path. Protocol bytes exclude TCP headers. The SQL and KV drivers
also use different client stacks, so these observations do not isolate SQL
parsing costs. Raw reads have the weaker visibility contract above and
cannot establish an acceleration of strong reads.

### Results

A [CI run](https://github.com/cstheorist/tarn/actions/runs/41302329820) of
`46c3615` (main `a06d431` with this summary and the gateway counters)
measured both engines on 4-CPU GitHub-hosted runners: six trials, 5-second
phases after 1-second warmups. `bench/results_strong_kv_memtx.json` and
`bench/results_strong_kv_vinyl.json` hold the summaries;
`python3 bench/strong_kv_summary.py bench/results_strong_kv_memtx.json`
renders the tables.

#### memtx

| Batch, clients | Path | Keys/s, median (range) | Op p99 ms | Storage requests/key | Storage bytes/key | Client bytes/key | Server CPU µs/key | Hot TX µs/key | Client CPU µs/key |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 1, 1 | SQL | 1,940 (1,910–1,960) | 0.726 | 1 | 182 | 347 | 640 | 161 | 123 |
| 1, 1 | strong KV | 984 (972–998) | 2.38 | 1 | 760 | 760 | 1,430 | 728 | 52.9 |
| 1, 1 | raw native | 13,400 (13,200–13,500) | 0.113 | 1 | 144 | 144 | 53.4 | 15.5 | 26.1 |
| 1, 16 | SQL | 4,880 (4,800–4,900) | 9.36 | 1 | 182 | 347 | 443 | 121 | 233 |
| 1, 16 | strong KV | 2,010 (1,940–2,090) | 19.7 | 1 | 760 | 760 | 570 | 477 | 25.8 |
| 1, 16 | raw native | 114,000 (114,000–116,000) | 0.212 | 1 | 146 | 146 | 8.49 | 3.19 | 9.17 |
| 32, 1 | SQL | 1,980 (1,960–1,990) | 20.6 | 1 | 182 | 347 | 638 | 161 | 117 |
| 32, 1 | strong KV | 2,130 (2,090–2,150) | 27.2 | 0.0312 | 140 | 140 | 695 | 365 | 3.78 |
| 32, 1 | raw native | 221,000 (218,000–222,000) | 0.195 | 1 | 146 | 146 | 2.87 | 1.2 | 2.62 |
| 32, 16 | SQL | 5,030 (4,960–5,070) | 126 | 1 | 182 | 347 | 435 | 119 | 226 |
| 32, 16 | strong KV | 3,910 (3,660–3,970) | 141 | 0.0312 | 140 | 140 | 287 | 249 | 4.56 |
| 32, 16 | raw native | 700,000 (689,000–708,000) | 1.1 | 1 | 146 | 146 | 1.45 | 0.942 | 2.02 |

Strong KV ÷ SQL, paired by trial: median (range).

| Batch, clients | Keys/s | Op p99 | Server CPU/key | Hot TX CPU/key |
|---|--:|--:|--:|--:|
| 1, 1 | 0.51 (0.50–0.52) | 3.25 (3.11–3.48) | 2.22 (2.20–2.26) | 4.49 (4.42–4.62) |
| 1, 16 | 0.41 (0.40–0.43) | 2.11 (1.79–2.21) | 1.29 (1.24–1.35) | 3.95 (3.79–4.03) |
| 32, 1 | 1.07 (1.06–1.09) | 1.33 (1.27–1.36) | 1.09 (1.08–1.11) | 2.28 (2.25–2.31) |
| 32, 16 | 0.78 (0.73–0.80) | 1.13 (1.06–1.20) | 0.66 (0.65–0.69) | 2.10 (2.03–2.22) |

#### vinyl

| Batch, clients | Path | Keys/s, median (range) | Op p99 ms | Storage requests/key | Storage bytes/key | Client bytes/key | Server CPU µs/key | Hot TX µs/key | Client CPU µs/key |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 1, 1 | SQL | 2,060 (2,020–2,070) | 0.745 | 1 | 168 | 347 | 544 | 179 | 98.6 |
| 1, 1 | strong KV | 1,070 (1,070–1,090) | 2.29 | 1 | 760 | 760 | 1,180 | 667 | 45.7 |
| 1, 1 | raw native | 16,400 (16,300–16,500) | 0.0904 | 1 | 145 | 145 | 37.9 | 13.5 | 16.1 |
| 1, 16 | SQL | 6,260 (6,210–6,310) | 6.83 | 1 | 167 | 347 | 337 | 126 | 167 |
| 1, 16 | strong KV | 2,160 (2,070–2,250) | 20.1 | 1 | 760 | 760 | 519 | 447 | 25.1 |
| 1, 16 | raw native | 128,000 (126,000–130,000) | 0.179 | 1 | 146 | 146 | 7.09 | 3.51 | 6.88 |
| 32, 1 | SQL | 2,100 (2,080–2,110) | 22.3 | 1 | 167 | 347 | 540 | 177 | 92.5 |
| 32, 1 | strong KV | 2,230 (2,200–2,280) | 28.2 | 0.0312 | 140 | 140 | 593 | 349 | 4.03 |
| 32, 1 | raw native | 204,000 (203,000–206,000) | 0.216 | 1 | 146 | 146 | 3.25 | 1.96 | 1.91 |
| 32, 16 | SQL | 6,450 (6,430–6,520) | 98.1 | 1 | 168 | 347 | 330 | 124 | 161 |
| 32, 16 | strong KV | 3,980 (3,830–4,130) | 141 | 0.0312 | 140 | 140 | 271 | 244 | 4.56 |
| 32, 16 | raw native | 508,000 (506,000–511,000) | 1.26 | 1 | 146 | 146 | 2.46 | 1.96 | 2.41 |

Strong KV ÷ SQL, paired by trial: median (range).

| Batch, clients | Keys/s | Op p99 | Server CPU/key | Hot TX CPU/key |
|---|--:|--:|--:|--:|
| 1, 1 | 0.52 (0.52–0.54) | 3.10 (3.03–3.14) | 2.18 (2.15–2.19) | 3.74 (3.67–3.77) |
| 1, 16 | 0.34 (0.33–0.36) | 2.97 (2.75–3.16) | 1.54 (1.49–1.61) | 3.54 (3.45–3.70) |
| 32, 1 | 1.06 (1.05–1.08) | 1.27 (1.24–1.29) | 1.09 (1.08–1.12) | 1.96 (1.92–2.00) |
| 32, 16 | 0.61 (0.59–0.64) | 1.45 (1.39–2.19) | 0.82 (0.80–0.86) | 1.99 (1.91–2.04) |

**Strong KV against SQL**, the equal contracts. Single-key strong KV reads
run at 0.34 to 0.52 of SQL's rate, with 2.1 to 3.3 times its p99. Batches of
32 from one client run 6 to 7% faster than SQL's 32 statements, and from 16
clients 22 to 39% slower.

**Where strong KV's time goes.** The hot leader's TX thread spends 2 to 4.5
times SQL's CPU per key on strong KV reads. With 16 clients and batches of
32 that thread is saturated, at about 250 µs per key and 3,900 keys/s. The
storage procedure describes the table again before and after every key
([#249](https://github.com/cstheorist/tarn/issues/249)). Strong KV's total
server CPU per key is below SQL's in those cells (0.66 and 0.82 of it),
since it skips the gateway, but it all lands on the one thread that owns
the hot tenant.

**SQL at 16 clients is bounded by its client.** The PyMySQL process used
about one core (161 to 233 µs per key at 4,900 to 6,500 keys/s), so those
SQL rates show the client's limit, not the gateway's.

**Requests and bytes.** Strong KV's batches of 32 send one request per 32
keys and 140 bytes per key. SQL's gateway sends one storage request per key
(167 to 182 bytes) and exchanges 347 bytes per key with the client. A
single-key strong KV request moves 760 bytes: the request carries the table
and ownership tokens, and the reply repeats the table token.

**Raw native reads** reach 13,000 to 700,000 keys/s with a p99 of 0.09 to
1.3 ms. They skip the freshness, claim and ownership checks, so they show
the storage engine's floor, not an acceleration of strong reads.
