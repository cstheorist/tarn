# Working on Tarn

Tarn is a MySQL-protocol SQL server (Rust) over Tarantool shards (Lua), with a
TLA+ model of its commit protocol (`tla/`) and Verus proofs
(`crates/tarn-verified`). Start with `docs/design.md`.

## Commands

```sh
cargo fmt --all -- --check && cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace                     # needs `tarantool` on PATH (or $TARANTOOL)
lua/test/run.sh                            # storage protocol tests
scripts/cluster.sh start -n 2 -r 1 -p 4000 # local cluster for the SQL suites
python3 tests/sql/difftest.py --tarn 127.0.0.1:4000 --ref 127.0.0.1:3407   # MariaDB: bench/baselines.sh start mariadb
python3 tests/sql/txntest.py --tarn 127.0.0.1:4000 --kv 127.0.0.1:3311,127.0.0.1:3321
python3 tests/sql/rctest.py --tarn 127.0.0.1:4000    # READ COMMITTED, row locks (list every SQL server)
python3 tests/sql/ddltest.py --tarn 127.0.0.1:4000,127.0.0.1:4001   # ALTER TABLE online, dumps (cluster.sh -s 2)
TARN_VINYL_CACHE_MB=0 TARN_DUMP_EVERY=2 scripts/cluster.sh start ... -e vinyl   # the suites on vinyl, reading from disk
tests/faults.sh && tests/failover.sh        # ENGINE=vinyl: the same with vinyl tables
tests/ddlfaults.sh                         # CREATE / DROP / TRUNCATE when a shard fails
tla/check.sh                               # needs tla/tla2tools.jar
```

The formal registry has 299 default configurations, including 224 expected
broken/tradeoff outcomes, and 312 total configurations (302 generated and
ten maintained directly). Explicit check arguments override the default.

## Rules of the road

- The commit protocol lives in `lua/tarn.lua` (participants, recovery,
  garbage collection, row locks) and `crates/tarn-sql/src/txn.rs`
  (coordinator). Change `tla/TarnCommit.tla` with it and model-check; every
  protection has a `Bug*` switch with a `Bug_*` configuration that TLC must
  fail.
- Every write carries the definition version of its table (`TableDef::ver`)
  and the shards refuse older ones (1412): code that writes to a table goes
  through the commit paths that send these fences. Stored tuples are not in
  column order once a table was altered: encode and decode rows with
  `TableDef::encode_tuple` and `Rd::row_of`.
- Tables may be vinyl, whose reads and writes yield. A commit step the model
  takes as atomic does its storage reads and writes first, then its checks
  and lock updates inside `atomic()` with no yield, right before
  `box.commit()` (`lua/tarn.lua` "Yields", `docs/design.md` §2.11); the
  storage tests run with `TARN_CHECK_YIELDS`.
- Code in `crates/tarn-verified` must keep verifying with Verus (140 verified,
  0 errors); keep its Verus idioms.
- SQL behaviour is defined by MariaDB 10.11: add cases to
  `tests/sql/difftest.py`.
- Never re-send a non-idempotent storage call after a lost connection
  (`Cluster::call_once`); report "outcome unknown" (1180/40003) instead.
- Regenerate doc tables with `python3 bench/render_results.py` and
  `python3 scripts/render_docs.py`.
- Examples, tests, issues and PRs stay application-neutral: generic tables
  and columns (`t`, `tenant_id`, `created_at`), and workloads described by
  their shape (rows per key prefix, selectivity), not by an application's
  schema or benchmark.
