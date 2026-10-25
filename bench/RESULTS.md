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

**Another host.** The sandbox moved to a different host during the project.
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
(DESIGN.md §2.5). `oltp_read_write` then collapsed under concurrency:

| oltp_read_write, Tarn 2 shards | 1 thread | 16 threads | 64 threads |
|---|---:|---:|---:|
| before (TPS / errors) | 234 / 0 | 168 / 5,300 | 63 / 8,672 |
| after (TPS / errors) | 234 / 0 | 456 / 186 | 549 / 843 |

## Comparison with published TiDB and YugabyteDB results

TiDB and YugabyteDB cannot run in this sandbox, so the only comparison
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

## What would make Tarn faster

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
