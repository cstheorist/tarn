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
python3 tests/sql/rctest.py --tarn 127.0.0.1:4000    # READ COMMITTED, row locks
tests/faults.sh && tests/failover.sh
tla/check.sh                               # needs tla/tla2tools.jar
```

## Rules of the road

- The commit protocol lives in `lua/tarn.lua` (participants, recovery,
  garbage collection, row locks) and `crates/tarn-sql/src/txn.rs`
  (coordinator). Change `tla/TarnCommit.tla` with it and model-check; every
  protection has a `Bug*` switch with a `Bug_*` configuration that TLC must
  fail.
- Code in `crates/tarn-verified` must keep verifying with Verus (59 verified,
  0 errors); keep its Verus idioms.
- SQL behaviour is defined by MariaDB 10.11: add cases to
  `tests/sql/difftest.py`.
- Never re-send a non-idempotent storage call after a lost connection
  (`Cluster::call_once`); report "outcome unknown" (1180/40003) instead.
- Regenerate doc tables with `python3 bench/render_results.py` and
  `python3 scripts/render_docs.py`.
