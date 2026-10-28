# Benchmarks

All numbers were measured on the development machine: **2 vCPUs** (Intel
Xeon @ 2.80 GHz, a Firecracker VM), 8 GB RAM, Linux 6.18. The load generator
(sysbench 1.0.20) runs on the same two vCPUs as the database, so every
system here is CPU-bound and pays for its client too. Treat the absolute
numbers as small-machine numbers; the ratios between systems are the
interesting part, and even they depend on the hardware (see "Another host"
below). Every system in the tables was measured on the same host within the
same two hours, with the final code.

## Setup

| | |
|---|---|
| Data | sysbench `oltp_common`: 4 tables × 100,000 rows (`id` PK, `k` secondary index, `c` CHAR(120), `pad` CHAR(60)) |
| Workloads | `oltp_point_select`, `oltp_read_only`, `oltp_read_write`, `oltp_update_index`, `oltp_update_non_index`, `oltp_insert`, `oltp_write_only`; prepared statements; 15 s per run; 1, 16 and 64 client threads |
| Errors | 1213 (deadlock / serialization failure), 1205, 1020, 1062 are retried by sysbench and counted in the *errors* column |
| MariaDB 10.11.14 | single node, InnoDB, buffer pool 1 GB, `innodb_flush_log_at_trx_commit=2`, binlog off, REPEATABLE READ (and SERIALIZABLE where noted) |
| MySQL 9.7.1 | same settings as MariaDB, REPEATABLE READ |
| Tarn 2 shards | 2 Tarantool 3.9 shards (one instance each, memtx, `wal_mode=write`) + one tarn-server; strictly serializable |
| Tarn 1×3 sync | 1 shard with 3 Tarantool replicas, synchronous replication (quorum 2) + Raft election: the HA configuration |
| Durability | comparable: every system writes its log at commit and does not fsync per commit (InnoDB flushes once a second; Tarantool `wal_mode=write`) |

Isolation is *not* equal: MariaDB and MySQL run REPEATABLE READ (snapshot
reads, no read validation), Tarn runs strict serializability (every read is
validated at commit; conflicting transactions abort with 1213 and retry).
`mariadb_serializable` shows InnoDB's SERIALIZABLE for comparison.

<!-- TABLES -->
### oltp_point_select

| System | 1 thr QPS | 16 thr QPS | 64 thr QPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 11,871 | 46,391 | 43,191 | 0.12 | 0.75 | 3.07 | 0 |
| MySQL 9.7 (RR) | 8,545 | 26,442 | 23,275 | 0.16 | 1.14 | 6.79 | 0 |
| Tarn 2 shards (strict serializable) | 4,719 | 23,570 | 29,711 | 0.29 | 1.27 | 4.03 | 0 |
| Tarn 1 shard x 3 sync replicas | 5,137 | 26,435 | 32,389 | 0.26 | 1.04 | 3.43 | 0 |

### oltp_read_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 586 | 1,981 | 1,737 | 2.18 | 15.27 | 64.47 | 0 |
| MySQL 9.7 (RR) | 365 | 1,000 | 907 | 3.62 | 25.28 | 95.81 | 0 |
| Tarn 2 shards (strict serializable) | 210 | 551 | 619 | 6.55 | 44.17 | 137.35 | 0 |
| MariaDB 10.11 (SERIALIZABLE) | 568 | 1,817 | 1,713 | 2.22 | 15.00 | 68.05 | 0 |

### oltp_read_write

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 467 | 1,389 | 1,209 | 2.86 | 20.74 | 94.10 | 0 |
| MySQL 9.7 (RR) | 295 | 726 | 658 | 4.49 | 36.24 | 155.80 | 0 |
| Tarn 2 shards (strict serializable) | 159 | 350 | 356 | 8.90 | 64.47 | 325.98 | 588 |
| Tarn 2 shards, ack-on-prepare | 167 | 372 | 357 | 8.28 | 61.08 | 337.94 | 611 |
| Tarn 1 shard x 3 sync replicas | 178 | 452 | 495 | 7.17 | 54.83 | 219.36 | 552 |
| MariaDB 10.11 (SERIALIZABLE) | 458 | 1,395 | 1,247 | 2.81 | 17.32 | 75.82 | 5 |

### oltp_update_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 9,418 | 28,197 | 23,550 | 0.15 | 1.27 | 5.77 | 0 |
| MySQL 9.7 (RR) | 5,681 | 12,966 | 13,237 | 0.23 | 3.49 | 14.46 | 0 |
| Tarn 2 shards (strict serializable) | 3,376 | 14,476 | 17,617 | 0.39 | 2.11 | 7.56 | 0 |
| Tarn 1 shard x 3 sync replicas | 2,054 | 8,151 | 10,231 | 0.68 | 3.02 | 9.91 | 0 |

### oltp_update_non_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 12,486 | 31,174 | 30,780 | 0.14 | 1.14 | 4.41 | 0 |
| MySQL 9.7 (RR) | 7,078 | 16,467 | 15,033 | 0.20 | 2.11 | 9.06 | 0 |
| Tarn 2 shards (strict serializable) | 3,407 | 14,334 | 18,702 | 0.39 | 2.14 | 7.04 | 0 |

### oltp_insert

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 10,608 | 27,946 | 30,020 | 0.14 | 1.27 | 4.57 | 0 |
| MySQL 9.7 (RR) | 7,037 | 17,493 | 15,538 | 0.20 | 1.96 | 15.27 | 0 |
| Tarn 2 shards (strict serializable) | 3,339 | 12,971 | 16,646 | 0.39 | 2.30 | 7.70 | 0 |
| Tarn 1 shard x 3 sync replicas | 2,113 | 8,849 | 11,566 | 0.68 | 2.81 | 8.74 | 0 |

### oltp_write_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 2,268 | 6,557 | 5,874 | 0.60 | 4.65 | 18.95 | 0 |
| MySQL 9.7 (RR) | 1,354 | 3,459 | 3,076 | 1.03 | 7.17 | 43.39 | 0 |
| Tarn 2 shards (strict serializable) | 609 | 1,364 | 1,507 | 2.52 | 18.61 | 66.84 | 14 |
| Tarn 2 shards, ack-on-prepare | 838 | 1,736 | 1,923 | 1.76 | 14.73 | 51.94 | 40 |
| MariaDB 10.11 (SERIALIZABLE) | 2,156 | 6,396 | 6,063 | 0.61 | 4.74 | 20.00 | 0 |

### kv_point_read

| System | 1 thr ops/s | 16 thr ops/s | 64 thr ops/s | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn 2 shards (strict serializable) | 9,068 | 95,549 | 259,797 | 0.15 | 0.28 | 0.41 | 0 |

## Reading the numbers

**Direct Key/Value reads are the fast path.** Reading SQL rows straight from
Tarantool by primary key (with the same bucket routing the SQL layer uses,
`crates/tarn-sql/examples/kvbench.rs`) reaches about 260k reads/s at 64
tasks, over five times the best SQL point-select rate of any system here,
on the same two vCPUs.

**Single-row SQL statements.** A point select is one round trip to the
owning shard; an autocommit single-row write is one round trip and one local
Tarantool transaction. At 64 threads Tarn is ahead of MySQL 9.7 on these:
point selects 29.7k (2 shards) and 32.4k QPS (1 shard) against 23.3k,
`update_index` 17.6k against 13.2k TPS, `update_non_index` 18.7k against
15.0k, `insert` 16.6k against 15.5k. MariaDB 10.11 is faster still: Tarn
reaches 55–75% of it at 64 threads. At one thread Tarn is about 2× slower
than MySQL and 2.5–3.7× slower than MariaDB: it pays for two network hops
(client → SQL server → shard) where they pay for one, and each hop crosses
Tarantool's network and transaction threads. Profiling `tarn-server` under
load (perf) shows about half of all CPU in the kernel's loopback TCP path
and wake-ups, not in SQL processing; Unix sockets for the internal hop did
not change that.

**One shard beats two on this box.** The 3-replica single-shard
configuration is faster than two unreplicated shards for reads and
multi-statement transactions: on two vCPUs fewer active processes means less
context switching, range queries do not fan out, and transactions commit
locally instead of through two-phase commit. Synchronous replication costs
30–40% of single-row write throughput (every commit waits for a second
replica's acknowledgment, and three instances share two vCPUs). On real
hardware each shard leader would have its own machine, and more shards add
capacity.

**Multi-statement transactions** (`read_only`, `read_write`, `write_only`)
issue 10–20 storage round trips each; with two shards almost every
`read_write`/`write_only` transaction is a cross-shard two-phase commit
(PREPARE and APPLY, each a durable local transaction on both shards), and the
four range queries go to both shards because tables are hash-partitioned by
primary key. Here Tarn reaches 45–70% of MySQL and a quarter to a third of
MariaDB. Ack-on-prepare takes one round trip off the commit: 27% lower
`write_only` latency at one thread and 28% more `write_only` throughput at
64 threads, but no gain for `read_write`, whose reads dominate. Strict
serializability costs retries (`errors` column, total over a 15 s run):
about 2 aborts per 100 committed `read_write` transactions at 16 threads and
11 per 100 at 64 threads (7 with one shard), versus none for REPEATABLE
READ; InnoDB's SERIALIZABLE (lock-based) performs like its REPEATABLE READ at
this low contention and rarely aborts.

**Another host.** The development VM moved to a different host during the project.
The same matrix on the first host (Intel Xeon @ 2.30 GHz, previous code
without the review fixes) gave MariaDB 84k, MySQL 62k and Tarn 42k/55k
point-select QPS, and Tarn's single-row writes were roughly level with MySQL
(within 10%) and at 53–65% of MariaDB. On this host every system is slower
but MySQL lost the most (−58% point selects, against −45% for MariaDB and
−30% for Tarn, whose direct KV reads lost the same 30%), so the ranking
between Tarn and MySQL on single-row statements depends on the machine.

**The lock index fix.** The first full run (on the first host) exposed a
performance bug: lock checks read the lock spaces inside transactions, so
concurrent PREPAREs that locked *different* rows conflicted through MVCC
([design §2.5](../docs/design.md#25-the-lock-index-correct-but-slow-found-by-benchmarking)). `oltp_read_write` then collapsed under concurrency:

| oltp_read_write, Tarn 2 shards | 1 thread | 16 threads | 64 threads |
|---|---:|---:|---:|
| before (TPS / errors) | 234 / 0 | 168 / 5,300 | 63 / 8,672 |
| after (TPS / errors) | 234 / 0 | 456 / 186 | 549 / 843 |

## Contention: READ COMMITTED against SERIALIZABLE

`bench/locking.py` runs two contended workloads with 32 PyMySQL clients
for 15 s each, on the same 2-vCPU VM (clients included), at both isolation
levels, against Tarn (2 shards, one tarn-server) and MariaDB 10.11:
*reserve* is the reservation transaction of
[issue #12](https://github.com/cstheorist/tarn/issues/12) (per cart item,
`SELECT ... ORDER BY unit LIMIT 1 FOR UPDATE SKIP LOCKED` over 20,000 free
units, then `DELETE` it and `INSERT` a hold, all in one transaction; carts
draw from 4 items); *counter* is `UPDATE ... SET n = n - 1` on one of 4 hot
rows in an explicit transaction. 1213 and 1205 are retried up to 50 times.

| Workload | System, isolation | Transactions/s | p50 ms | p99 ms | Retries | Gave up |
|---|---|---:|---:|---:|---:|---:|
| reserve, 1 item/cart | Tarn, SERIALIZABLE | 255 | 80 | 606 | 19,631 | 0 |
| | Tarn, READ COMMITTED | 1,627 | 17 | 63 | 0 | 0 |
| | MariaDB, SERIALIZABLE | 1,640 | 16 | 67 | 0 | 0 |
| | MariaDB, READ COMMITTED | 2,520 | 11 | 38 | 0 | 0 |
| reserve, 2 items/cart | Tarn, SERIALIZABLE | 78 | 273 | 1,577 | 14,003 | 18 |
| | Tarn, READ COMMITTED | 761 | 40 | 77 | 0 | 0 |
| | MariaDB, SERIALIZABLE | 852 | 31 | 137 | 28 | 0 |
| | MariaDB, READ COMMITTED | 1,466 | 20 | 52 | 0 | 0 |
| counter, 4 hot rows | Tarn, SERIALIZABLE | 1,067 | 21 | 134 | 39,998 | 0 |
| | Tarn, READ COMMITTED | 3,338 | 8 | 30 | 0 | 0 |
| | MariaDB, SERIALIZABLE | 3,757 | 6 | 29 | 0 | 0 |
| | MariaDB, READ COMMITTED | 3,724 | 7 | 28 | 0 | 0 |

Under contention, optimistic strict serializability turns waiting into
aborts and retries: every reservation in a burst picks the same free unit,
and every counter update in a burst reads the same version. READ COMMITTED
with row locks makes the losers wait instead (zero retries) and gives 3 to
10 times the throughput, at 52-90% of MariaDB's READ COMMITTED on the same
machine. The reservation also depends on reading the queue a page at a time:
`SKIP LOCKED` with `LIMIT` stops at the first free rows instead of reading
all 20,000 units of the item.

## Comparison with published TiDB and YugabyteDB results

TiDB and YugabyteDB could not be run on the development VM, so the only comparison
available is with their published sysbench results, normalized per vCPU of
database servers. This is an indication, not a head-to-head benchmark: the
published runs use real networks, 3-way replication, TLS (YugabyteDB),
larger data sets and dedicated client machines, while the Tarn figures below
include the load generator's share of the two vCPUs and the 2-shard
configuration has no replication.

<!-- COMPARISON -->
| Workload (best throughput) | TiDB 7.5 (80 vCPU) | per vCPU | YugabyteDB (24 vCPU) | per vCPU | Tarn 2 shards (2 vCPU) | per vCPU | Tarn 1×3 sync (2 vCPU) | per vCPU | MariaDB 1 node (2 vCPU) | per vCPU |
|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|
| oltp_point_select | 118,651 | 1,483 | – | – | 29,711 | 14,856 | 32,389 | 16,195 | 46,391 | 23,196 |
| oltp_read_only | – | – | 4,616 | 192 | 619 | 310 | – | – | 1,981 | 991 |
| oltp_read_write | 3,169 | 40 | 245 | 10 | 356 | 178 | 495 | 248 | 1,389 | 695 |
| oltp_update_index | 20,377 | 255 | 260 | 11 | 17,617 | 8,809 | 10,231 | 5,116 | 28,197 | 14,098 |
| oltp_update_non_index | 34,011 | 425 | – | – | 18,702 | 9,351 | – | – | 31,174 | 15,587 |
| oltp_insert | 37,247 | 466 | – | – | 16,646 | 8,323 | 11,566 | 5,783 | 30,020 | 15,010 |
| direct KV point read | – | – | – | – | 259,797 | 129,898 | – | – | – | – |

Sources: [TiDB Cloud v7.5.0 sysbench report](https://docs.pingcap.com/tidbcloud/v7.5-performance-benchmarking-with-sysbench/)
(2 TiDB nodes × 16 vCPU + 3 TiKV nodes × 16 vCPU = 80 vCPU, 32 tables ×
10M rows); [YugabyteDB sysbench page](https://docs.yugabyte.com/stable/benchmark/sysbench-ysql/)
(YugabyteDB 2026.1.2, 3 × c5.2xlarge = 24 vCPU, RF 3, TLS).

## Vinyl tables: cached, and with data ten times the cache

Issue #23's last step: sysbench on vinyl tables with a cache larger than the
data and with data ten times the cache, against memtx and against TiDB with
the same data ten times its block cache. Unlike the rest of this page, this
ran on a GitHub-hosted runner, with every system on the same host:

| | |
|---|---|
| Host | 4 vCPUs (AMD EPYC 9V74), 16 GB RAM; sysbench on the same vCPUs; every system's data on tmpfs |
| Data | 4 tables × 1,000,000 rows: about 957 MB uncompressed, 500 MB per Tarn shard; read once after loading; uniform random keys; 30 s per run; p99 latency |
| Tarn | 2 shards × 1 instance, strictly serializable. Vinyl, cached: 1,213 MB of cache per shard, of which 556 MB filled. Vinyl at 10×: 47 MB of cache per shard. Each shard checkpointed after loading, so every row was in vinyl's files |
| TiDB | v8.5.8, 1 TiDB + 1 PD + 1 TiKV, snapshot isolation; block cache 3,072 MB, or 95 MB at 10× |

| Workload, 64 threads | Tarn memtx | Tarn vinyl, cached | Tarn vinyl, data 10× cache | TiDB | TiDB, data 10× cache |
|---|---:|---:|---:|---:|---:|
| `oltp_point_select` (QPS / p99 ms) | 58,010 / 2.9 | 51,657 / 3.4 | 37,180 / 4.6 | 16,402 / 8.9 | 12,704 / 11.2 |
| `oltp_read_only` (TPS / p99 ms) | 1,223 / 77.2 | 1,013 / 90.8 | 631 / 134.9 | 665 / 139.8 | 551 / 164.4 |
| `oltp_read_write` (TPS / p99 ms) | 791 / 147.6 | 578 / 248.8 | 415 / 277.2 | 404 / 240.0 | 352 / 253.3 |
| `oltp_update_index` (TPS / p99 ms) | 27,216 / 7.7 | 21,768 / 10.5 | 20,120 / 9.7 | 3,954 / 29.2 | 3,249 / 34.3 |
| `oltp_update_non_index` (TPS / p99 ms) | 30,416 / 7.0 | 25,214 / 8.6 | 23,917 / 7.7 | 6,195 / 20.0 | 5,487 / 21.1 |
| `oltp_insert` (TPS / p99 ms) | 31,220 / 6.0 | 22,992 / 9.6 | 24,692 / 7.8 | 4,616 / 25.7 | 4,242 / 27.7 |
| Load, 4 × 1,000,000 rows (rows/s) | 31,896 | 30,063 | 31,334 | 57,228 | 59,568 |

* **Cached, vinyl costs 11–27% against memtx**: 89% of memtx's point
  selects, 80–83% of its updates, 73–74% of its inserts and read-write
  transactions.
* **With data ten times the cache, reads slow down and writes do not.**
  - Point selects ran at 72% of the cached rate and range scans
    (`oltp_read_only`) at 62%, since a read that misses the cache reads and
    decompresses a page.
  - Single-row writes ran at 92–107% of it: they go to vinyl's in-memory
    level. 107% means run-to-run variation.
  - p99 rose from 3.4 to 4.6 ms on point selects.
* **Against TiDB at ten times its block cache**, Tarn at ten times its cache
  served 2.9× the point selects and 4.4–6.2× the single-row writes, with a
  lower p99. Multi-statement transactions came to 1.1–1.2× TiDB's, with a
  similar p99. When the cache shrank, TiDB's reads lost less than Tarn's
  (17–23% against 28–38%) and its writes lost more (8–18% against 0–8%).
* **Isolation differs:** Tarn's read-write transactions are strictly
  serializable and retried 140–292 serialization failures per run. TiDB ran
  snapshot isolation, without failures.
* **TiDB loaded rows nearly twice as fast** (57,000–60,000 rows/s, against
  30,000–32,000).

**On tmpfs, a cache miss reads memory, not a disk.** These numbers measure
what a miss costs the engine, not disk latency. With the data on a disk
larger than RAM, reads that miss would each wait for an I/O on both systems.
The run is
[40218092737](https://github.com/cstheorist/tarn/actions/runs/40218092737)
of the `bench/compare` branch's workflow (`bench/compare/run.sh`,
configuration `push.env`); its raw results are on the branch
`results/compare-40218092737`.

## Where a shard's time goes

A shard runs every storage procedure on one thread, Tarantool's
transaction thread, so that thread's CPU per request bounds what one shard
serves. `bench/profile.sh` measures it
([#28](https://github.com/cstheorist/tarn/issues/28)): it starts one shard
and one SQL server, loads the sysbench tables, and runs `bench/profile.py`,
which runs each workload and reports per statement

* the CPU time of the shard's transaction thread (from `/proc`),
* the Lua heap it allocated (LuaJIT's `gc_allocated`) and the storage calls
  it served, with the share the native procedures served
  ([design §1.5](../docs/design.md#15-native-procedures)),
* where LuaJIT's sampling profiler found the thread: running Lua code,
  collecting garbage, compiling, or in C -- Tarantool's own work (requests,
  the storage engine) and C procedures.

With `AB=3` it runs each workload three times on the shard restarted with
the native procedures and three times on the shard restarted without them,
alternately, so that each run starts from a fresh shard. With 16 threads
and 20 s per run, the means of three runs:

| One shard, 16 threads, Lua → native | statements per second | shard CPU per statement | Lua heap per statement | calls served natively | profiler samples in Lua / GC |
|---|---:|---:|---:|---:|---:|
| `oltp_point_select` | 26,796 → 36,943 | 16.4 → 4.7 µs | 2,756 → 0 B | 100% | 33% / 18% → 0% / 0% |
| `oltp_read_only` | 9,603 → 15,349 | 66.4 → 25.1 µs | 5,702 → 410 B | 67% | 38% / 15% → 6% / 3% |
| `oltp_read_write` | 9,773 → 15,063 | 64.7 → 27.4 µs | 3,925 → 562 B | 67% | 35% / 17% → 10% / 6% |
| `oltp_update_index` | 13,345 → 22,878 | 45.7 → 20.0 µs | 3,446 → 0 B | 100% | 25% / 16% → 0% / 0% |
| `oltp_update_non_index` | 13,760 → 24,210 | 42.6 → 17.1 µs | 3,459 → 1 B | 100% | 25% / 17% → 0% / 0% |
| `oltp_insert` | 12,946 → 22,557 | 41.6 → 14.7 µs | 3,161 → 2 B | 100% | 20% / 17% → 0% / 0% |

Through the native procedure a point select costs the shard under a third
of the CPU it costs through `tarn_select`, and no Lua at all: what is left
is Tarantool's own work for the request, the index lookup and the reply.
The transactions of `oltp_read_only` and `oltp_read_write` make six storage
calls on one shard: their first read, which begins the stream's
transaction, four range reads to an end key, which the native procedure
serves, and the commit; their ten point reads are IPROTO SELECTs, which run
no Lua at all. Serving the range reads natively saves the shard about 0.65
ms per transaction, some 165 µs per read of 100 rows. Throughput rises less
than the shard's CPU falls, as the SQL server and sysbench share the two
vCPUs with it: by 38% for point selects and by over half for the
transactions. (The VM's speed varies over the day: an earlier comparison of
point selects alone gave +25% when it ran slower, and +27% when it ran
faster, at 36,698 and 46,568 a second.)

The last three workloads are autocommit writes, measured in a later run:
`UPDATE`s by primary key of an indexed and of an unindexed column, and
`INSERT`s. The native procedure makes all of them, without Lua: a write
costs the shard 35% to 44% of the CPU it costs through `tarn_autocommit`,
and throughput rises by 71% to 76%. (`oltp_delete` is not among them: run
with the others, it empties the tables they update, and most of its own
deletes find no row.)

On a shard of three replicas (`R=3`), whose tables are synchronous, each
commit waits for a second replica, and the leader also sends every write
to the other two. `R=3 AB=3 bench/profile.sh`, which profiles the leader
and restarts every replica for each run, gives:

| One shard of 3 replicas, 16 threads, Lua → native | statements per second | leader CPU per statement | Lua heap per statement | calls served natively | profiler samples in Lua / GC |
|---|---:|---:|---:|---:|---:|
| `oltp_update_index` | 5,508 → 7,281 | 68.8 → 34.1 µs | 3,469 → 1 B | 100% | 23% / 10% → 0% / 0% |
| `oltp_update_non_index` | 5,747 → 7,844 | 63.0 → 29.0 µs | 3,441 → 2 B | 100% | 22% / 11% → 0% / 0% |
| `oltp_insert` | 5,592 → 7,402 | 61.9 → 25.6 µs | 3,257 → 5 B | 100% | 19% / 11% → 0% / 0% |

The leader's CPU per write falls by half or more here too; throughput rises
by about a third, as the three replicas share the two vCPUs with the SQL
server and sysbench, and every commit waits for its quorum.

The transactions are native too: the commit of one that wrote on one shard
(`tarn_storage.ts_commit_local`), the validation of a read-only one
(`ts_validate`), and the first read of each, which begins its stream
(`ts_select`). In a later run of `oltp_read_only`, `oltp_read_write` and
`oltp_write_only`, whose transactions update two rows, delete one and
insert it again, reading each row first:

| One shard, 16 threads, Lua → native | statements per second | shard CPU per statement | Lua heap per statement | calls served natively | profiler samples in Lua / GC |
|---|---:|---:|---:|---:|---:|
| `oltp_read_only` | 7,546 → 13,401 | 88.2 → 26.8 µs | 4,806 → 1 B | 100% | 36% / 18% → 0% / 0% |
| `oltp_read_write` | 8,042 → 13,619 | 75.7 → 26.6 µs | 4,094 → 5 B | 99% | 35% / 14% → 0% / 0% |
| `oltp_write_only` | 17,154 → 25,761 | 27.1 → 11.2 µs | 1,492 → 1 B | 100% | 23% / 14% → 0% / 0% |

The shard runs no Lua for these transactions any more: they take 30%, 35%
and 41% of the Lua procedures' CPU per statement, and 1.5 to 1.8 times
their throughput. A run of the build before, whose first reads and
validations ran in Lua, gave 40%, 38% and 54% (11,910, 12,788 and 22,205
statements a second). The VM ran slower in these runs than in the first
table's: the Lua procedures took 75.7 to 78.3 µs per statement of
`oltp_read_write` in them, against 64.7 µs there. Run again on the build
that also has the lazy indexes
([#116](https://github.com/cstheorist/tarn/pull/116)), the comparison gave
36%, 34% and 41% of the CPU, at 1.6, 1.7 and 1.5 times the throughput.

### Next to locks

The native procedures used to hand a request back whenever a prepared
transaction held any lock on its table. Since part 6 of
[#28](https://github.com/cstheorist/tarn/issues/28)
([#144](https://github.com/cstheorist/tarn/pull/144),
[#145](https://github.com/cstheorist/tarn/pull/145)) they check the keys
and ranges they touch, as the Lua procedures do. Three regimes compare the
two builds ([#131](https://github.com/cstheorist/tarn/issues/131)), each on
two shards:
- **No locks:** `oltp_update_non_index` alone, 16 threads, 4 tables of
  20,000 rows. Its autocommit writes hold no lock.
- **Unrelated locks:** the same with 8 threads, next to `oltp_write_only`
  with 8. The cross-shard transactions of `oltp_write_only` hold W locks
  while they prepare, mostly on other rows.
- **Contention:** the same on 4 tables of 50 rows, so the locks are often
  on the rows the autocommit writes write.

Each build ran each regime 3 times, alternating; the figures are medians.
The CPU is the transaction threads' of both shards, and the Lua heap both
shards', each divided by all the statements of both workloads.

| Regime | Build | `oltp_update_non_index` TPS (p50 / p95 / p99 ms) | `oltp_write_only` TPS (p50 / p95 / p99 ms) | Native procedures' calls they served | Autocommit writes to Lua | CPU per statement | Lua heap per statement |
|---|---|---:|---:|---:|---:|---:|---:|
| no locks | before | 23,472 (0.63 / 1.23 / 1.70) | | 100% | 0% | 18.9 µs | 1 B |
| | after | 23,518 (0.62 / 1.21 / 1.73) | | 100% | 0% | 18.6 µs | 1 B |
| unrelated locks | before | 2,903 (2.31 / 6.43 / 9.22) | 826 (9.22 / 16.11 / 21.50) | 61% | 55% | 119.0 µs | 5,802 B |
| | after | 4,223 (1.64 / 4.03 / 5.99) | 923 (8.28 / 14.21 / 25.28) | 100% | 0% | 85.0 µs | 3,531 B |
| contention | before | 3,158 (2.11 / 6.09 / 8.58) | 627 (10.27 / 29.19 / 44.17) | 65% | 48% | 109.1 µs | 5,074 B |
| | after | 4,364 (1.58 / 4.10 / 6.21) | 670 (9.56 / 26.68 / 42.61) | 97% | 2.8% | 82.6 µs | 3,341 B |

- **No locks:** both builds are the same.
- **Unrelated locks:** the autocommit writes no longer go to Lua.
  - They make 45% more transactions, at a lower p99 (9.2 → 6.0 ms).
  - The cross-shard transactions make 12% more, with a higher p99
    (21.5 → 25.3 ms): they share the shards with more work.
  - The shards spend 29% less CPU and 39% less Lua heap per statement.
- **Contention:** 2.8% of the autocommit writes meet a lock on their row
  and go to Lua; the Lua procedure waits for it. The rest are native:
  - 38% more autocommit transactions;
  - 24% less CPU per statement;
  - the cross-shard transactions' throughput and latency about the same.

`tarn_info().native.fallbacks` of the build after gives the reasons. In one
run under contention:
- 2,473 autocommit writes went to Lua for a lock on their row;
- 374 one-shard commits for a W lock on what they read;
- 1,348 one-shard commits for a duplicate key, which the Lua procedure
  reports. `oltp_write_only` deletes a row and inserts it again,
  and another transaction may have inserted it first.

### What still runs in Lua

Two kinds of requests still run in the Lua procedures
([#131](https://github.com/cstheorist/tarn/issues/131),
[#141](https://github.com/cstheorist/tarn/issues/141)):
- the commit of a transaction that wrote on more than one shard (PREPARE,
  APPLY, ABORT);
- the row locks of READ COMMITTED transactions (`tarn_lock`), which every
  write of such a transaction takes. A transaction with row locks also
  commits in Lua.

`bench/profile.py` against the first shard, one run of each workload (16
threads, 4 tables of 20,000 rows, 20 s; the server's default isolation set
with `scripts/cluster.sh -I`):

| Shards, isolation | Workload | Statements per second | First shard's CPU per statement | Calls served natively | Samples in Lua / GC |
|---|---|---:|---:|---:|---:|
| 1, SERIALIZABLE | `oltp_read_write` | 15,448 | 22.2 µs | 97% | 1% / 0% |
| 1, SERIALIZABLE | `oltp_write_only` | 30,182 | 8.9 µs | 100% | 0% / 0% |
| 1, READ COMMITTED | `oltp_read_write` | 14,323 | 24.2 µs | 74% | 18% / 8% |
| 1, READ COMMITTED | `oltp_write_only` | 14,757 | 37.0 µs | 0% | 29% / 18% |
| 2, SERIALIZABLE | `oltp_read_write` | 7,557 | 57.4 µs | 71% | 25% / 13% |
| 2, SERIALIZABLE | `oltp_write_only` | 7,489 | 75.3 µs | 40% | 29% / 20% |
| 2, READ COMMITTED | `oltp_read_write` | 9,438 | 38.7 µs | 71% | 26% / 16% |
| 2, READ COMMITTED | `oltp_write_only` | 5,911 | 97.3 µs | 0% | 31% / 22% |

On two shards the first shard's CPU is divided by all the statements,
about half of which it serves.
- **One shard, SERIALIZABLE:** transactions run without Lua.
- **One shard, READ COMMITTED:** the same writes take row locks and commit
  in Lua. A statement of `oltp_write_only` costs the shard 4.2 times as
  much CPU, at half the throughput.
- **Two shards:** most transactions of `oltp_read_write` and
  `oltp_write_only` write on both shards and commit through PREPARE and
  APPLY. The transaction thread then spends 38% to 53% of its samples in
  Lua and its garbage collector.

## What would make Tarn faster

* **Native procedures for the other hot paths**
  ([#28](https://github.com/cstheorist/tarn/issues/28)): the commits of
  cross-shard transactions (PREPARE, APPLY) and the row locks of READ
  COMMITTED still run as Lua procedures, whose calls cost a shard more than
  their storage work, as they did for reads, autocommit writes and
  one-shard transactions (see above). And the native
  procedures hand a request to Lua whenever a prepared transaction holds a
  lock on its table, and a write whenever a row lock is held on its table:
  with the lock index in the native module, they would check the rows
  themselves.
* **Fewer round trips per transaction**: batch the 10 point selects of a
  transaction when the client pipelines; push UPDATE/DELETE ... WHERE pk = ?
  inside transactions down as deferred operations instead of read-then-buffer.
* **Push work to the shards**: projections (`SELECT c` needs 120 of ~200 bytes
  per row), aggregates (`SUM(k)` over a range returns one number per shard)
  and `ORDER BY ... LIMIT` merging.
* **Range sharding** for tables that are scanned by primary-key ranges
  (hash sharding sends every range to every shard).
* **More cores per process**: Tarantool's transaction thread is single
  threaded per instance; on real hardware one would run several shards per
  machine and several SQL servers.


## Storage RPC admission component measurements (issue #127)

The complete [initial](results_rpc_admission_initial.json) and
[scope-sharing revision](results_rpc_admission_optimized.json) records retain
all paired samples, raw output, CPU and latency measurements, exact source
revisions, binary hashes, fixture/workload hashes and environment. The
control is `a0faf804372f911e5122317889aaf2cc1dc6ab83`; initial candidate
`75f53e2404c123c2b34f8426495152d6063e27e4`; revised candidate
`b944dcb39061d1ab1d10fc29891ca51f5b50afc5`. Both drivers use the exact same
phase and `init.lua` fixture. `bench/rpc_admission.py` alternates order
across five repetitions of eight seconds per build, with 64 tasks/four
connections reading one of 1000 memtx tuples over local TCP. The revised
candidate uses one per-connection scope-set reference per request instead
of cloning shared shard/global budget references.

This measures the IPROTO component on one RF1 Tarantool 3.9 process with
MVCC, no TLS and no timed writes. It is not a MySQL, RF3, durable-write or
competitor comparison. The fixture uses Tarantool's default WAL mode;
initialization is outside the timed read phase. Rust 1.98.1 release builds
use matched thin LTO/codegen settings. The shared Docker VM grants the
fixture/driver three CPUs and 3 GiB; other task-owned validation/builds were
stopped during measurements. Other host workloads remained uncontrolled.

| Candidate | Control median (range), ops/s | Candidate median (range), ops/s |
|---|---:|---:|
| Initial | 572,629 (109,582–626,623) | 554,509 (527,902–566,400) |
| Shared scope ownership | 360,729 (335,516–569,769) | 426,295 (289,026–552,590) |

The initial run exposed a cost in several pairs and motivated removing
avoidable reference churn and waiter metadata. The broad, shifting control
and candidate ranges prevent a reliable cost estimate or throughput/latency
improvement claim. No samples were discarded or replaced. An isolated
machine is required to quantify the remaining overhead. These observations
do not establish production performance; separate unit, SQL and live-fault
tests establish the resource/outcome contracts, not this timing probe.
