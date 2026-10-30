# Contributing to Tarn

Thanks for your interest! Bug reports, questions, documentation fixes and
code are all welcome. For anything larger than a small fix, please open an
issue first so that we can agree on the approach.

## Development setup

| Tool | Notes |
|---|---|
| Rust | via [rustup](https://rustup.rs); `rust-toolchain.toml` pins the version (1.98.1), add `rustfmt` and `clippy` |
| Tarantool 3.9.0 | on `PATH`, or point `TARANTOOL` at the binary; see `docs/upgrades.md` |
| Python 3 + MySQL clients | SQL/fault suites: `pip install PyMySQL==1.1.2 mysql-connector-python==9.5.0 msgpack==1.1.2`; production checks also need a MariaDB/MySQL CLI and OpenSSL |
| MariaDB 10.11 | reference server for the differential tests (`bench/baselines.sh start mariadb`, or any instance) |
| Java 11+ and `tla2tools.jar` | only if you touch the protocol (see below) |
| Verus | only if you touch `crates/tarn-verified` (see below) |

[docs/getting-started.md](docs/getting-started.md) shows how to build and run
a local cluster.

## Before you open a pull request

Run the checks that CI runs:

```sh
cargo fmt --all -- --check
cargo clippy --workspace --all-targets -- -D warnings
cargo test --workspace
lua/test/run.sh

scripts/cluster.sh start -n 2 -r 1 -p 4000
bench/baselines.sh start mariadb
python3 tests/sql/difftest.py --tarn 127.0.0.1:4000 --ref 127.0.0.1:3407
python3 tests/sql/txntest.py  --tarn 127.0.0.1:4000 --kv 127.0.0.1:3311,127.0.0.1:3321
tests/faults.sh
tests/failover.sh            # slower: 2 shards x 3 replicas under load
```

and, depending on what you changed:

* **The commit protocol** (`lua/tarn.lua`, `crates/tarn-sql/src/txn.rs`):
  change `tla/TarnCommit.tla` first and model-check it (`tla/check.sh`, and
  the larger configurations relevant to your change; see
  [docs/verification.md](docs/verification.md)). If you add a protection, add
  a `Bug*` switch for it and a `Bug_*` configuration that TLC must fail. Add
  storage-protocol tests in `lua/test/storage_test.lua`.
* **Verified code** (`crates/tarn-verified`): run Verus and keep it at
  zero errors. Keep Verus idioms (`i = i + 1`, explicit arithmetic); the
  crate allows the clippy lints that would rewrite them.
* **SQL behaviour**: add cases to `tests/sql/difftest.py`; the reference is
  MariaDB 10.11, compared exactly.
* **Benchmarks or docs with generated sections**: re-run
  `python3 bench/render_results.py` and `python3 scripts/render_docs.py`.

## Code style

* `rustfmt` with the repository's `rustfmt.toml` (120 columns) and no clippy
  warnings.
* Comments explain *why*; module docs at the top of each file explain how
  the file fits the protocol, using the names of the TLA+ actions
  (`TarnCommit!Prepare`, `ApplyAt`, ...) where they correspond.
* Errors returned to clients use MySQL error codes and SQLSTATEs; see
  [docs/operations.md](docs/operations.md#error-codes).

## Commits and pull requests

* One logical change per pull request; describe what changed, why, and how
  you tested it (the PR template has a checklist).
* Commit messages: a short summary line in the imperative mood ("Fix ...",
  "Add ..."), a blank line, then the reasoning.
* Keep [CHANGELOG.md](CHANGELOG.md) up to date under *Unreleased* for
  user-visible changes.
* Keep examples, tests, issues and pull requests application-neutral: use
  generic tables and columns (`t`, `tenant_id`, `created_at`, ...) and
  describe a workload by its shape (rows per key prefix, how selective a
  filter is), not by a particular application's schema or benchmark.

## License

By contributing, you agree that your contributions are dual licensed under
the MIT and Apache-2.0 licenses, as described in the [README](README.md#license),
without any additional terms or conditions.
