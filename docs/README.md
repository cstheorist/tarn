# Tarn documentation

| Document | What it covers |
|---|---|
| [Getting started](getting-started.md) | Build from source, run a local cluster, use SQL and direct key/value reads, try a failover, run the tests. |
| [SQL compatibility](sql-compatibility.md) | Protocol features, supported statements, types and functions, and where Tarn differs from MySQL. |
| [Operations](operations.md) | Deployment layout, storage-node and `tarn-server` configuration, monitoring, failure behaviour, error codes, security. |
| [Design](design.md) | Architecture, request paths, the commit protocol and its recovery, record garbage collection, timing assumptions, model-checking and benchmark results, limitations. |
| [Verification](verification.md) | The TLA+ model and its configurations, the Verus proofs, every test suite, and CI. |
| [Benchmarks](../bench/RESULTS.md) | sysbench against MariaDB and MySQL: setup, full tables, analysis, and a per-vCPU comparison with published TiDB and YugabyteDB results. |

New to the code? Read the README, then [Design §1](design.md#1-architecture)
for the components and [§1.2](design.md#12-request-paths) for what happens
to a statement, then the module docs at the top of
`crates/tarn-sql/src/txn.rs` (the coordinator) and `lua/tarn.lua` (the
storage side).
