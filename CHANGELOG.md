# Changelog

All notable changes to Tarn are documented here. The format follows
[Keep a Changelog](https://keepachangelog.com/en/1.1.0/), and the project
uses [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-10-25

First public release: a research prototype.

### Added

- `tarn-server`: MySQL protocol front end (handshake, `mysql_native_password`,
  text and binary protocol, prepared statements) over Tarantool shards.
- SQL: sysbench-level statements plus joins, aggregates, `GROUP_CONCAT`,
  subqueries without outer references, `UNION [ALL]`, `INSERT ... SELECT`,
  `ON DUPLICATE KEY UPDATE`, `REPLACE`, `AUTO_INCREMENT`, `SHOW` and `SET`
  statements; checked against MariaDB 10.11 by differential and randomized
  tests.
- Storage: every table is a plain Tarantool space, hash-sharded with vshard's
  bucket function, readable directly as key/value. Replicated shards use
  synchronous replication and Raft leader election.
- Transactions: strictly serializable across shards. Autocommit reads wait
  for W-locks of committed but not yet applied transactions. Single-shard
  commits are validated by memtx MVCC. Read-only commits validate. Cross-shard
  commits use a parallel-commit two-phase commit with recovery, an
  optional `--ack-on-prepare` mode, and expiry-based garbage collection of
  protocol records.
- `tarn-iproto`: an async, pipelined Tarantool IPROTO client with streams,
  `MP_DECIMAL` and zero-copy decoding.
- TLA+ specification `tla/TarnCommit.tla` with 33 TLC configurations,
  including 10 protection switches that TLC must catch.
- Verus proofs for shard routing, coordinator decisions and recovery steps,
  and the MySQL wire codec (59 verified).
- Tests: storage protocol (74), IPROTO integration, SQL differential and
  fuzzing against MariaDB, transaction anomalies, a ledger-checked bank
  workload, failover (live leader change and leader kills), fault injection.
- sysbench harness and results against MariaDB 10.11 and MySQL 9.7.

### Fixed during development

Found by TLC, by tests, by benchmarking, and by an independent code review
before the release; see [docs/design.md](docs/design.md) §2.3–§2.8.

- Single-shard reads ignored W-locks of committed but unapplied cross-shard
  transactions, breaking strict serializability (found by TLC).
- Cross-shard read-only commits could validate stale reads that memtx had
  moved into a read view (found by a targeted test).
- Lock checks read the lock spaces through MVCC, so unrelated PREPAREs
  conflicted and `oltp_read_write` collapsed under concurrency (found by
  benchmarking).
- Recovery could abort a committed transaction already applied on one shard
  after a participant record was garbage-collected (critical; found by the
  review).
- Read-only commits could validate on a demoted leader whose connection
  survived (found by the review).
- Autocommit DML could be applied twice after a lost connection; such
  outcomes are now reported as unknown (1180 / SQLSTATE 40003) (found by the
  review).
- The in-memory lock index could keep locks after a failed PREPARE, and lost
  the locks of PREPAREs waiting for a quorum when rebuilt (found by the
  review).
- An expired PREPARE could vote NO and contradict a commit whose records were
  already collected (found by TLC while modelling the fix above).

[Unreleased]: https://github.com/cstheorist/tarn/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/cstheorist/tarn/releases/tag/v0.1.0
