# SQL resource evidence (#134)

These complete JSONL runs were collected on 2026-10-28 with Rust 1.98.1,
Tarantool 3.9.0 and MariaDB 10.11 in an arm64 Linux Docker environment on a
shared macOS host. The gateway is an **unoptimized development build**;
the native storage module is a release build. Executable SHA-256:

`4b61e51811922ece7a1a2028740754765b89b24cd00a0a241ac46e45411e04ca`

Each file includes effective limits, runtime versions, all assertions and
all 288 overload outcomes/latencies. All three final native repetitions and the final Lua control completed;
earlier development controls remain separately in the task evidence.
The source includes main through `700ce88` and the query resource change.

## Matched ORDER/LIMIT control

Two shards, RF1, WAL write, memtx, 20,000 rows, 382-character payload,
unindexed nullable integer sort key, 33 retained candidates per shard:

```sql
SELECT id,n,s FROM t ORDER BY n,id LIMIT 20 OFFSET 13
```

Fresh gateways use the same executable, table, SQL, 512 MiB query cap, 2 GiB
shared cap, two worker threads, pool size two and OCC checks. Only `topk`
differs. Each path runs three consecutive repetitions per fresh fixture.
Earlier fixture queries warm storage; these are not cold disk measurements.
Raw files preserve every timing and the RSS baseline as well as the peak.

| Run | Heap median ms | Materialize median ms | Heap peak RSS MiB | Materialize peak RSS MiB | Heap admitted peak MiB | Materialize admitted peak MiB |
|---|---:|---:|---:|---:|---:|---:|
| [native-1](native-1.jsonl) | 48.76 | 60.02 | 25.64 | 49.97 | 40.46 | 67.36 |
| [native-2](native-2.jsonl) | 46.67 | 55.65 | 30.96 | 54.73 | 40.46 | 67.36 |
| [native-3](native-3.jsonl) | 53.00 | 49.25 | 32.37 | 54.13 | 40.46 | 67.36 |
| [lua](lua.jsonl) | 42.74 | 48.65 | 31.02 | 50.05 | 40.46 | 67.60 |

The heap reduced retained memory in these controls. Timings overlap and one
native trial reverses the latency ordering; this evidence makes no general
throughput or latency improvement claim. Logical admitted bytes are not RSS.

## Shared-budget overload

Twelve synchronized clients each offer 24 alternating point reads and
oversized materializing scans: 144 useful point reads offered and 144 large
scans offered. The query cap is 64 MiB and the shared cap is 96 MiB. All
large scans are refused; point reads can also be refused under pressure.
Reported tails cover **all** operations, including fast refusals. These
0.2–0.4 second bursts are smoke measurements, not sustained capacity tests.

| Run | Useful completed / offered | Refused | Useful/s | p95 ms | p99 ms | Peak RSS MiB | Admitted peak MiB |
|---|---:|---:|---:|---:|---:|---:|---:|
| [native-1](native-1.jsonl) | 30 / 144 | 258 | 95.1 | 20.17 | 55.70 | 86.37 | 96.00 |
| [native-2](native-2.jsonl) | 38 / 144 | 250 | 115.0 | 22.15 | 40.87 | 80.11 | 93.02 |
| [native-3](native-3.jsonl) | 22 / 144 | 266 | 92.4 | 14.49 | 41.28 | 80.90 | 96.00 |
| [lua](lua.jsonl) | 24 / 144 | 264 | 96.2 | 15.55 | 33.11 | 80.75 | 92.51 |

Every run remained within its 96 MiB accounting limit, returned no
unexpected errors, drained query ownership after delivery/disconnect and
left the DML sentinel unchanged. The fixture also checks exact MariaDB
top-K results, full transaction rollback, row-lock release, bounded reads,
writes and savepoints, work refusal, blocked output ownership and binary
long-data admission/reset. Allocator retention and unmetered runtime/catalog allocations affect RSS;
kernel buffers have separate costs. The SQL cap is not an RSS theorem.

Reproduce with `TARN_RESOURCE_REF=HOST:PORT tests/resources.sh`.
See [the operational and proof boundaries](../../docs/resources.md).
