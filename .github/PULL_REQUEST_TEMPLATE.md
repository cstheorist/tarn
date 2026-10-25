## What and why

<!-- What does this change, and why? Link the issue it addresses. -->

## How it was tested

<!-- Commands you ran and what they showed. -->

## Checklist

- [ ] `cargo fmt --all -- --check` and `cargo clippy --workspace --all-targets -- -D warnings` pass
- [ ] `cargo test --workspace` and `lua/test/run.sh` pass
- [ ] SQL suites pass if SQL behaviour changed (`tests/sql/difftest.py`, `tests/sql/txntest.py`)
- [ ] Protocol changes: `tla/TarnCommit.tla` updated and `tla/check.sh` passes
- [ ] Verified code changes: Verus reports 0 errors
- [ ] Docs and `CHANGELOG.md` updated for user-visible changes
