# Finite process-loss effect reconciliation

This defensive fixture is separate from the #160 runtime-cause experiment. It
does not change storage, ALTER, GC scheduling, or production retry behavior.
It makes no claim to reproduce or explain the original GC failure.

Build `target/release/tarn-server` and the native module, install the existing
SQL-suite dependencies (PyMySQL, mysql-connector-python, msgpack), and provide
Tarantool on PATH (or `TARANTOOL`). Run each selected configuration separately:

```sh
ENGINE=memtx GC_NATIVE=native bash tests/gc-effects.sh
ENGINE=vinyl GC_NATIVE=native bash tests/gc-effects.sh
ENGINE=memtx GC_NATIVE=off bash tests/gc-effects.sh
ENGINE=vinyl GC_NATIVE=off bash tests/gc-effects.sh
python3 -m unittest discover -s tests -p test_gc_effects.py -v
```

The fixture always creates its own `mktemp` directory and an RF3, two-shard,
`wal_mode=fsync` cluster. It never reuses a cluster or clears evidence. Callers
must reserve its port range exclusively; the startup bind check is subject to
the usual race. Configure `GC_BASE` (default 7200; storage ports base+11..13 and
base+21..23), `GC_PORT` (7300), `GC_RECOVERY_PORT` (7301), `GC_PROXY_PORT` (7240),
and optionally `GC_EVIDENCE_PARENT`, `TARN_BIN`, `TARN_NATIVE`, `PYTHON`.
No process-name matching is used: PID files belong to this new directory.

The complete fixed SQL submission manifest has six independent target keys:

| Operation | Acknowledgment | Required final effect |
|---|---|---|
| Two unguarded increments in one cross-shard transaction | Separate statement responses and successful COMMIT | Both 1 |
| Two unguarded increments in a serializable transaction whose read was invalidated | Statement responses, COMMIT 1213 / 40001 | Both 0 |
| Independent conflicting increment | Successful autocommit response | 1 |
| One unguarded autocommit increment, successful storage reply held by the existing frame-aware proxy | 1180 / 40003 after the original leader is SIGKILLed | Exactly 1 |

Every SQL setup/statement/commit is journaled with durable intent before send
and a separate raw completion afterward. The journal starts before the first
SQL connection, so connection-constructor failure retains a header. Setup
includes database/table creation and each seed insert. IDs are selected by the
existing bucket function; no target operation shares a key with another target.
The refused transaction reads the independent conflict key. No uncertain
mutation is retried. Reads alone may retry while leaders recover.

After replacement leaders are writable, a separate recovery gateway configured
with **all replica addresses** reads the exact whole table through strong SQL.
The proxy gateway deliberately remains pinned to the failed upstream. Raw
committed user-space tuples are recorded separately from survivor leaders;
their full `[id,v,bucket_id]` layout is retained and the trailing bucket is
validated against the integer key's CRC32C route. They have no Tarn freshness
barrier. Then `cluster.sh restart` starts the
killed original node with its original directory and WAL. The fixture waits
for replication follow state and a captured survivor vclock, reads its raw
tuples, and repeats strong SQL plus survivor raw checks. These are finite,
quiescent observations, not a general freshness assertion for arbitrary raw
reads. Reconciliation rejects additional/missing SQL submissions, missing
setup evidence, partial cross-shard results, lost acknowledged effects,
effects of the refused transaction, duplicate unknown increments, and more
than one completed matching proxy mutation. Statement acknowledgment never
substitutes for COMMIT acknowledgment; actual SQLSTATE is preserved.

Evidence prints its path on every exit and retains source/diff/file hashes,
runtime versions, binary/native hashes, environment, generated commands and
configs, logs, PID receipts, WAL directories, `effects.jsonl`, partial or full
`effects.json`, `failure.txt` on runner failure, and `report.json` on success.
Cleanup sends TERM only to PIDs recorded in this fixture; it does not remove
the directory. A startup failure can precede journal creation; its startup
receipt and all logs still remain. A journal-constructor failure retains any
file it managed to create. Reserve enough disk for retained RF3 WAL evidence.
All six storage nodes supply actual PID, version, native state, fsync, replica
count and synchronous quorum observations; the runner requires RF3/Q2 and the
selected native mode before submitting SQL. An unexpected SQL error or
transport failure is journaled and immediately stops submission. The held
success and process-loss intent are saved before SIGKILL, and the completed
kill receipt is saved immediately after it.
Survivor and restarted phases repeat runtime checks on every live node, and
record the user table's actual engine and synchronous space state on both
leaders alongside the full raw tuples.
`native` checks the configured module path (`tarn.cfg.native`) against the
selected source receipt on every observed node. Writable native leaders,
including promoted followers, must expose a loaded `tarn_info().native` object
with `ready=1`. A read-only follower may legitimately report false before its
first writable role, or retain a loaded object after demotion; its readiness
is not required. `off` requires both configured and loaded native state to
be false on every node. These observations do not assert that every operation
was served in C. Runtime samples are checkpointed into `effects.json` before
validation; an observation failure retains the samples already collected, and
an assertion includes the offending observed info. Actual `_tarn_txn`/`_tarn_part` synchronous space flags are
also required on every live node at each phase.

Offline controls exercise the effect oracle and journal structure only. Live
validation is still required for all four engine/native selections: verify
the captured native runtime state, deterministic refusal, held matching success,
actual owned-leader death, survivor recovery, original-node WAL restart and
catchup, all exact effects, and absence of lock/yield diagnostics. This is a
single induced leader-process loss plus a single-node restart. It does not
establish full-cluster crash/power-loss durability, arbitrary failure schedules,
long-running GC safety, or the cause of the original failure.
