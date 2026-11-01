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

A [CI run](https://github.com/cstheorist/tarn/actions/runs/41331543881) of
`554e06a` (main `1ef4f34` with #249's fix) measured both engines on 4-CPU
GitHub-hosted runners: six trials, 5-second phases after 1-second warmups.
`bench/results_strong_kv_memtx.json` and `bench/results_strong_kv_vinyl.json`
hold the summaries; `python3 bench/strong_kv_summary.py
bench/results_strong_kv_memtx.json` renders the tables.

#### memtx

| Batch, clients | Path | Keys/s, median (range) | Op p99 ms | Storage requests/key | Storage bytes/key | Client bytes/key | Server CPU µs/key | Hot TX µs/key | Client CPU µs/key |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 1, 1 | SQL | 2,610 (2,580–2,660) | 0.565 | 1 | 183 | 347 | 391 | 106 | 69.4 |
| 1, 1 | strong KV | 1,850 (1,740–2,180) | 1.04 | 1 | 760 | 760 | 665 | 277 | 33.1 |
| 1, 1 | raw native | 12,100 (11,900–12,300) | 0.116 | 1 | 144 | 144 | 38.7 | 12.7 | 15.6 |
| 1, 16 | SQL | 9,230 (9,060–9,440) | 5.12 | 1 | 183 | 347 | 214 | 62 | 116 |
| 1, 16 | strong KV | 6,770 (6,330–6,900) | 7.36 | 1 | 760 | 760 | 181 | 134 | 18.7 |
| 1, 16 | raw native | 131,000 (130,000–182,000) | 0.175 | 1 | 146 | 146 | 5.05 | 2.07 | 5.02 |
| 32, 1 | SQL | 2,640 (2,590–2,940) | 16.6 | 1 | 183 | 347 | 386 | 106 | 66.1 |
| 32, 1 | strong KV | 5,820 (5,160–6,540) | 11.4 | 0.0312 | 140 | 140 | 258 | 100 | 2.41 |
| 32, 1 | raw native | 250,000 (244,000–264,000) | 0.178 | 1 | 146 | 146 | 2.13 | 1.01 | 1.43 |
| 32, 16 | SQL | 9,520 (9,400–10,000) | 68.3 | 1 | 183 | 347 | 209 | 60.7 | 111 |
| 32, 16 | strong KV | 18,300 (17,000–19,100) | 36.3 | 0.0312 | 140 | 140 | 65.6 | 49.6 | 3.11 |
| 32, 16 | raw native | 723,000 (715,000–738,000) | 1.04 | 1 | 146 | 146 | 0.873 | 0.541 | 2.35 |

Strong KV ÷ SQL, paired by trial: median (range).

| Batch, clients | Keys/s | Op p99 | Server CPU/key | Hot TX CPU/key |
|---|--:|--:|--:|--:|
| 1, 1 | 0.71 (0.67–0.83) | 1.95 (1.74–2.33) | 1.73 (1.55–1.80) | 2.66 (2.49–2.85) |
| 1, 16 | 0.74 (0.67–0.75) | 1.42 (1.37–1.51) | 0.85 (0.83–0.90) | 2.21 (2.04–2.33) |
| 32, 1 | 2.20 (1.96–2.32) | 0.68 (0.65–0.77) | 0.67 (0.65–0.72) | 0.96 (0.92–1.01) |
| 32, 16 | 1.91 (1.81–1.97) | 0.53 (0.48–0.58) | 0.31 (0.31–0.34) | 0.82 (0.77–0.89) |

#### vinyl

| Batch, clients | Path | Keys/s, median (range) | Op p99 ms | Storage requests/key | Storage bytes/key | Client bytes/key | Server CPU µs/key | Hot TX µs/key | Client CPU µs/key |
|---|---|--:|--:|--:|--:|--:|--:|--:|--:|
| 1, 1 | SQL | 1,730 (1,700–1,730) | 0.842 | 1 | 167 | 347 | 714 | 214 | 127 |
| 1, 1 | strong KV | 1,260 (1,220–1,290) | 1.51 | 1 | 760 | 760 | 1,180 | 497 | 53.9 |
| 1, 1 | raw native | 12,500 (12,300–12,600) | 0.122 | 1 | 144 | 144 | 58.3 | 19.1 | 27.4 |
| 1, 16 | SQL | 4,500 (4,460–4,530) | 9.37 | 1 | 167 | 347 | 483 | 166 | 242 |
| 1, 16 | strong KV | 3,620 (3,510–3,640) | 13 | 1 | 760 | 760 | 343 | 257 | 24.4 |
| 1, 16 | raw native | 97,500 (96,700–98,800) | 0.246 | 1 | 146 | 146 | 10.4 | 4.76 | 9.41 |
| 32, 1 | SQL | 1,750 (1,730–1,770) | 25.5 | 1 | 166 | 347 | 713 | 213 | 120 |
| 32, 1 | strong KV | 3,250 (3,190–3,290) | 16.9 | 0.0312 | 140 | 140 | 520 | 202 | 3.71 |
| 32, 1 | raw native | 177,000 (173,000–177,000) | 0.26 | 1 | 146 | 146 | 4.04 | 2.26 | 2.7 |
| 32, 16 | SQL | 4,610 (4,590–4,620) | 136 | 1 | 166 | 347 | 475 | 163 | 235 |
| 32, 16 | strong KV | 9,020 (8,890–9,220) | 69.4 | 0.0312 | 140 | 140 | 137 | 104 | 4.23 |
| 32, 16 | raw native | 467,000 (465,000–470,000) | 1.4 | 1 | 146 | 146 | 2.68 | 2.14 | 2.09 |

Strong KV ÷ SQL, paired by trial: median (range).

| Batch, clients | Keys/s | Op p99 | Server CPU/key | Hot TX CPU/key |
|---|--:|--:|--:|--:|
| 1, 1 | 0.73 (0.70–0.75) | 1.80 (1.77–1.83) | 1.66 (1.65–1.68) | 2.32 (2.30–2.36) |
| 1, 16 | 0.80 (0.79–0.81) | 1.39 (1.36–1.47) | 0.71 (0.70–0.72) | 1.55 (1.54–1.58) |
| 32, 1 | 1.85 (1.82–1.89) | 0.67 (0.63–0.73) | 0.73 (0.72–0.73) | 0.95 (0.93–0.96) |
| 32, 16 | 1.95 (1.93–2.01) | 0.51 (0.49–1.23) | 0.29 (0.28–0.29) | 0.63 (0.62–0.65) |

**Strong KV against SQL**, the equal contracts. Single-key strong KV reads
run at 0.71 to 0.80 of SQL's rate, with 1.4 to 2.0 times its p99. Batches of
32 run at 1.85 to 2.20 times SQL's key rate, with 0.51 to 0.68 of its p99
(SQL issues 32 statements).

**Where strong KV's time goes.** A request now describes its table once,
not before and after every key
([#249](https://github.com/cstheorist/tarn/issues/249)). The hot leader's
TX thread spends 0.63 to 0.96 times SQL's CPU per key on batches of 32, and
1.6 to 2.7 times on single keys. With 16 clients and batches of 32, strong
KV reaches 18,300 keys/s on memtx and 9,020 on vinyl, at 50 and 104 µs of
hot TX CPU per key. Before #249, the [same study](https://github.com/cstheorist/tarn/actions/runs/41302329820)
(`46c3615`) reached 3,910 and 3,980 keys/s there: that thread was saturated
at about 250 µs per key. Runs on different runners also move SQL's rates,
so the ratios paired by trial are the comparison to quote.

**SQL at 16 clients is bounded by its client.** The PyMySQL process used
about one core (111 to 242 µs per key at 4,500 to 9,500 keys/s), so those
SQL rates show the client's limit, not the gateway's.

**Requests and bytes.** Strong KV's batches of 32 send one request per 32
keys and 140 bytes per key. SQL's gateway sends one storage request per key
(166 to 183 bytes) and exchanges 347 bytes per key with the client. A
single-key strong KV request moves 760 bytes: the request carries the table
and ownership tokens, and the reply repeats the table token.

**Raw native reads** reach 12,100 to 723,000 keys/s with a p99 of 0.12 to
1.4 ms. They skip the freshness, claim and ownership checks, so they show
the storage engine's floor, not an acceleration of strong reads.
