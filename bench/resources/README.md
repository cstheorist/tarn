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

## Scale study

[`resource_scale.py`](../resource_scale.py) runs the same three statements
at 1× and 10× data under fixed budgets, with one and eight concurrent
sessions, three times per session (#134's first scale criterion):

- a filtered scan, `SELECT id,v FROM a WHERE v % 10 = 3`, whose WHERE the
  SQL server evaluates on every row; it keeps 1 row in 10;
- a high-fanout join, `SELECT a.id,b.id FROM a JOIN b ON a.g=b.g WHERE a.h = 1`,
  through indexes on `a.h` and `b.g`; its result grows with the square of
  the data (4,000 rows at 1×, 400,000 at 10×);
- a full sort, `SELECT id,v FROM a ORDER BY v,id`, on an unindexed column.

Table `a` holds 20,000 rows at 1× and 200,000 at 10×, each with a
180-character column; `b` holds a twentieth as many. The SQL server is
restarted before each cell, so each cell's high-water marks and peak RSS are
its own. Every result is checked against the data's formula (row count, id
sum and, for the sort, order), and a refusal must be 1041 `Tarn resource
limit:`. The runs were collected on 2026-11-01 on a two-shard RF1 cluster
(WAL write) on a shared two-CPU Linux host, with the default budgets of
[docs/resources.md](../../docs/resources.md): source `17b6319`, release SQL
server SHA-256 `39614341857ce2fbac94eb034bb924d4042c5dae0edbb0fd132d7411a2728dc9`.
Raw results: [scale-memtx.json](scale-memtx.json), [scale-vinyl.json](scale-vinyl.json).

| Engine | Rows of a | Statement | Sessions | Completed | Refused | p50 s | Max s | Peak admitted MiB | Peak RSS MiB |
|---|---:|---|---:|---:|---|---:|---:|---:|---:|
| memtx | 20,000 | scan | 1 | 3/3 | 0 | 0.093 | 0.129 | 34 | 69 |
| memtx | 20,000 | scan | 8 | 24/24 | 0 | 0.286 | 0.414 | 265 | 73 |
| memtx | 20,000 | join | 1 | 3/3 | 0 | 0.047 | 0.077 | 33 | 50 |
| memtx | 20,000 | join | 8 | 24/24 | 0 | 0.394 | 0.813 | 142 | 66 |
| memtx | 20,000 | sort | 1 | 3/3 | 0 | 0.197 | 0.283 | 53 | 83 |
| memtx | 20,000 | sort | 8 | 24/24 | 0 | 1.793 | 3.624 | 352 | 135 |
| memtx | 200,000 | scan | 1 | 3/3 | 0 | 0.424 | 0.484 | 49 | 85 |
| memtx | 200,000 | scan | 8 | 24/24 | 0 | 2.938 | 3.496 | 352 | 134 |
| memtx | 200,000 | join | 1 | 0/3 | 3 query memory budget |  |  | 64 | 68 |
| memtx | 200,000 | join | 8 | 0/24 | 24 query memory budget |  |  | 276 | 108 |
| memtx | 200,000 | sort | 1 | 0/3 | 3 query memory budget |  |  | 63 | 76 |
| memtx | 200,000 | sort | 8 | 0/24 | 24 query memory budget |  |  | 488 | 192 |
| vinyl | 20,000 | scan | 1 | 3/3 | 0 | 0.048 | 0.049 | 34 | 72 |
| vinyl | 20,000 | scan | 8 | 24/24 | 0 | 0.362 | 0.495 | 265 | 74 |
| vinyl | 20,000 | join | 1 | 3/3 | 0 | 0.037 | 0.046 | 33 | 44 |
| vinyl | 20,000 | join | 8 | 24/24 | 0 | 0.264 | 0.367 | 178 | 66 |
| vinyl | 20,000 | sort | 1 | 3/3 | 0 | 0.354 | 0.408 | 53 | 90 |
| vinyl | 20,000 | sort | 8 | 24/24 | 0 | 1.779 | 2.550 | 240 | 126 |
| vinyl | 200,000 | scan | 1 | 3/3 | 0 | 0.747 | 0.873 | 49 | 77 |
| vinyl | 200,000 | scan | 8 | 24/24 | 0 | 3.486 | 4.728 | 345 | 146 |
| vinyl | 200,000 | join | 1 | 0/3 | 3 query memory budget |  |  | 64 | 72 |
| vinyl | 200,000 | join | 8 | 0/24 | 24 query memory budget |  |  | 322 | 122 |
| vinyl | 200,000 | sort | 1 | 0/3 | 3 query memory budget |  |  | 63 | 84 |
| vinyl | 200,000 | sort | 8 | 0/24 | 24 query memory budget |  |  | 488 | 184 |

"Peak admitted" is the SQL server's `Tarn_resource_peak_bytes`: every
session's charges, including each in-flight read's 16 MiB reservation.
"Peak RSS" is its VmHWM.

Every cell either completed with the exact result or was refused with 1041
by the query memory budget. None returned a clipped or wrong result.

- At 1×, every statement completes, alone and with eight sessions.
- At 10×, the filtered scan still completes, keeping 20,000 rows.
- At 10×, the join and the sort are refused.

Eight sessions running the 10× sort reached 488 MiB of the 512 MiB SQL server
budget. Each was still refused by its own 64 MiB budget, not the SQL server's.

Before this change, the same filtered scan was refused even at 50,000 rows.
Each page it read stayed charged until the statement ended, although it kept
only a tenth of the rows. The accounting is conservative: a statement
holding whole rows of `a` is charged several times their stored size. The
sort holds whole rows, including the 180-character column it does not
return.

Regenerate the table with
`python3 bench/resource_scale.py --markdown bench/resources/scale-*.json`.
