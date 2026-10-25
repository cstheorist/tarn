# Benchmarks

All numbers were measured on the development machine: **2 vCPUs** (Intel
Xeon @ 2.30 GHz, a Firecracker VM), 8 GB RAM, Linux 6.18. The load generator
(sysbench 1.0.20) runs on the same two vCPUs as the database, so every
system here is CPU-bound and pays for its client too. Treat the absolute
numbers as small-machine numbers; the ratios between systems are the
interesting part.

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
| MariaDB 10.11 (RR) | 20,576 | 84,134 | 71,414 | 0.06 | 0.46 | 2.11 | 0 |
| MySQL 9.7 (RR) | 15,929 | 62,481 | 51,173 | 0.09 | 0.53 | 2.66 | 0 |
| Tarn 2 shards (strict serializable) | 7,470 | 32,571 | 42,490 | 0.17 | 0.92 | 2.76 | 0 |
| Tarn 1 shard x 3 sync replicas | 9,335 | 43,167 | 54,648 | 0.14 | 0.61 | 1.89 | 0 |

### oltp_read_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 1,132 | 3,294 | 2,762 | 1.08 | 9.56 | 44.17 | 0 |
| MySQL 9.7 (RR) | 718 | 2,012 | 1,572 | 1.67 | 14.46 | 74.46 | 0 |
| Tarn 2 shards (strict serializable) | 315 | 817 | 902 | 4.49 | 30.26 | 92.42 | 0 |
| MariaDB 10.11 (SERIALIZABLE) | 1,082 | 3,358 | 2,637 | 1.03 | 9.39 | 44.98 | 0 |

### oltp_read_write

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 768 | 2,250 | 1,962 | 1.73 | 13.95 | 66.84 | 0 |
| MySQL 9.7 (RR) | 538 | 1,112 | 982 | 2.35 | 24.38 | 110.66 | 0 |
| Tarn 2 shards (strict serializable) | 234 | 456 | 549 | 5.99 | 49.21 | 207.82 | 843 |
| Tarn 2 shards, ack-on-prepare | 283 | 524 | 503 | 4.82 | 40.37 | 240.02 | 844 |
| Tarn 1 shard x 3 sync replicas | 280 | 629 | 669 | 4.41 | 36.89 | 164.45 | 730 |
| MariaDB 10.11 (SERIALIZABLE) | 813 | 2,390 | 1,900 | 1.37 | 12.08 | 59.99 | 6 |

### oltp_update_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 19,988 | 43,426 | 40,386 | 0.08 | 0.97 | 3.62 | 0 |
| MySQL 9.7 (RR) | 11,219 | 23,114 | 24,907 | 0.12 | 2.11 | 5.77 | 0 |
| Tarn 2 shards (strict serializable) | 5,303 | 20,205 | 26,056 | 0.27 | 1.55 | 5.00 | 0 |
| Tarn 1 shard x 3 sync replicas | 2,233 | 9,360 | 14,651 | 0.61 | 2.48 | 6.09 | 0 |

### oltp_update_non_index

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 28,443 | 53,659 | 50,115 | 0.07 | 0.75 | 2.71 | 0 |
| MySQL 9.7 (RR) | 14,005 | 31,661 | 29,500 | 0.11 | 1.18 | 4.10 | 0 |
| Tarn 2 shards (strict serializable) | 5,476 | 20,657 | 26,480 | 0.24 | 1.52 | 4.82 | 0 |

### oltp_insert

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 23,012 | 45,461 | 40,499 | 0.07 | 0.83 | 3.49 | 0 |
| MySQL 9.7 (RR) | 13,569 | 29,632 | 26,579 | 0.10 | 1.23 | 6.43 | 0 |
| Tarn 2 shards (strict serializable) | 5,207 | 19,203 | 24,255 | 0.26 | 1.55 | 5.00 | 0 |
| Tarn 1 shard x 3 sync replicas | 2,330 | 10,019 | 14,560 | 0.59 | 2.30 | 6.43 | 0 |

### oltp_write_only

| System | 1 thr TPS | 16 thr TPS | 64 thr TPS | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| MariaDB 10.11 (RR) | 4,022 | 9,766 | 8,257 | 0.32 | 3.62 | 14.73 | 0 |
| MySQL 9.7 (RR) | 2,307 | 5,679 | 4,735 | 0.64 | 5.18 | 28.67 | 0 |
| Tarn 2 shards (strict serializable) | 962 | 2,138 | 2,756 | 1.55 | 11.24 | 34.33 | 44 |
| Tarn 2 shards, ack-on-prepare | 1,067 | 2,404 | 3,349 | 1.42 | 10.27 | 29.72 | 58 |
| MariaDB 10.11 (SERIALIZABLE) | 4,385 | 10,205 | 9,765 | 0.30 | 3.36 | 12.08 | 0 |

### kv_point_read

| System | 1 thr ops/s | 16 thr ops/s | 64 thr ops/s | 1 thr p95 ms | 16 thr p95 ms | 64 thr p95 ms | errors (max) |
|---|---:|---:|---:|---:|---:|---:|---:|
| Tarn 2 shards (strict serializable) | 15,962 | 147,051 | 363,233 | .100 | .176 | .303 | 0 |

## Reading the numbers

**Direct Key/Value reads are the fast path.** Reading SQL rows straight from
Tarantool by primary key (with the same bucket routing the SQL layer uses,
`crates/tarn-sql/examples/kvbench.rs`) reaches several times the best SQL
point-select throughput of any system here, on the same two vCPUs.

**SQL point reads and writes:** one SQL point select is one round trip to
the owning shard, an autocommit single-row write is one round trip and one
local Tarantool transaction. On one small machine Tarn pays for two network
hops (client → SQL server → shard) where MariaDB pays for one, so it
trails single-node MariaDB/MySQL here; the gap narrows at high concurrency.
Profiling `tarn-server` under load (perf) shows the time dominated by the
kernel's loopback TCP path and wake-ups (≈50% of all CPU in the kernel),
not by SQL processing; Unix sockets for the internal hop did not change the
picture.

**Multi-statement transactions** (`read_only`, `read_write`, `write_only`)
issue 10–20 storage round trips each, and with two shards almost every
`read_write`/`write_only` transaction is a cross-shard two-phase commit
(PREPARE + APPLY, each a durable local transaction on both shards). The
range queries of `read_only`/`read_write` go to both shards because tables
are hash-partitioned by primary key.

**The lock index fix.** The first full run exposed a performance bug: lock
checks read the lock spaces inside transactions, so concurrent PREPAREs that
locked *different* rows conflicted through MVCC (DESIGN.md §2.5).
`oltp_read_write` then collapsed under concurrency:

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
| oltp_point_select | 118,651 | 1,483 | – | – | 42,490 | 21,245 | 54,648 | 27,324 | 84,134 | 42,067 |
| oltp_read_only | – | – | 4,616 | 192 | 902 | 451 | – | – | 3,294 | 1,647 |
| oltp_read_write | 3,169 | 40 | 245 | 10 | 549 | 275 | 669 | 334 | 2,250 | 1,125 |
| oltp_update_index | 20,377 | 255 | 260 | 11 | 26,056 | 13,028 | 14,651 | 7,325 | 43,426 | 21,713 |
| oltp_update_non_index | 34,011 | 425 | – | – | 26,480 | 13,240 | – | – | 53,659 | 26,829 |
| oltp_insert | 37,247 | 466 | – | – | 24,255 | 12,127 | 14,560 | 7,280 | 45,461 | 22,731 |
| direct KV point read | – | – | – | – | 366,436 | 183,218 | – | – | – | – |

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
