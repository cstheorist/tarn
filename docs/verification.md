# Verification

Tarn's correctness argument has three layers, each catching a different
kind of mistake:

| Layer | Checks | Where |
|---|---|---|
| TLA+ model, checked with TLC | the commit protocol as a design: strict serializability, atomicity, recovery and record garbage collection under crashes, lost and re-delivered messages, and leader failover; READ COMMITTED transactions with row locks (no lost updates, real-time visibility); bucket moves between shards while transactions run | `tla/` |
| Verus proofs | that specific Rust functions do what their specifications say, for every input | `crates/tarn-verified/` |
| Tests | that the implementation matches the model and MySQL: storage protocol, SQL semantics against MariaDB, transaction anomalies, failover, faults | `lua/test/`, `tests/`, `crates/*/tests` |

What each layer found, and how the findings were fixed, is described in the
[design document](design.md#23-what-tlc-found) (§2.3, §2.4, §2.5, §2.8 and §2.9).

## TLA+ model

`tla/TarnCommit.tla` specifies the protocol: shards, keys with owners, three
transactions with fixed read and write sets, direct (non-SQL) readers, and
the actions of execution, single-shard commit, read-only validation,
PREPARE, APPLY, ABORT, coordinator decisions, timeout-driven recovery,
record expiry and garbage collection, READ COMMITTED transactions:
plain reads of the latest committed value, pessimistic row locks with a read
under the lock, their commits, and the window in which a local commit is not
yet visible to readers outside it, and bucket moves: rows have a place that
a move changes in steps (fence and drain on the old shard, a hold on the new
one, the rows deleted there and inserted here), requests reach the shard
they were routed to, and scans read every shard (design §2.12). Faults are coordinator crashes, lost and
re-delivered messages, and shard leader failover (durable state survives,
the MVCC read tracking of open streams and the row-lock table do not).

Invariants checked in every configuration:

| Invariant | Meaning |
|---|---|
| `TypeOK` | well-formed state |
| `AppliedOnlyIfCommitted` | no participant applies a transaction that is not committed |
| `NoAbortAfterCommit` | a committed transaction is never aborted anywhere |
| `PlainOnlyCommitted` | the plain Tarantool spaces only ever hold committed values |
| `DirectReadsClean` | direct key/value readers never see uncommitted data |
| `DirectReadYourWrites` | a direct read after an acknowledged commit sees it |
| `StrictSerializable` | the committed transactions have a serial order, consistent with real time, that explains every value they read (for READ COMMITTED transactions: every value they read under a lock; a scan of every shard must have seen each row exactly once) |
| `NoLostUpdate` | every READ COMMITTED write lands on the value its transaction read under the lock |
| `RCReadsSeeAcked` | a READ COMMITTED read sees every transaction acknowledged before it started |
| `UniqueHolds` | no key is inserted by two committed transactions (global unique indexes) |

`StrictSerializable` is checked constructively: the spec records the order in
which writes reached each key and each transaction's real-time
predecessors, and the invariant searches for a witness order.

### Configurations

`tla/MC.tla` defines ten workloads on two shards (`Skew`, `Local`,
`Frac`, `Recov` with serializable transactions; `Counter`, `Mixed`, `Local`
with READ COMMITTED ones; `Uniq`, inserts of one global unique index
entry; `MoveScan` and `MoveRC`, a row moved to the other shard while
transactions run), and `tla/gen_cfgs.sh` generates one `.cfg` per
combination of workload, fault budget and options. The names say what is
checked:

| Name | Meaning |
|---|---|
| `Skew`, `Local`, `Frac`, `Recov` | the protocol with PREPARE-based read-only commits and records kept forever |
| `*Failover` | a shard leader fails over during execution (with a direct reader) |
| `*_AckOnPrepare` | clients are acknowledged after PREPARE instead of after APPLY |
| `*_ROValidate` | the implementation's commit paths: cross-shard read-only transactions validate instead of preparing, and the decision shard's APPLY records COMMITTED |
| `*_GC` | additionally, transactions expire and protocol records are garbage-collected |
| `RC_*` | READ COMMITTED read-modify-writes racing each other and serializable transactions (`RC_Counter`, `RC_Mixed`, `RC_Local`), with faults, failovers (which empty the row-lock table) or ack-on-prepare |
| `Uniq*` | two rows with one value of a unique key without the shard key insert one global index entry, across shards and locally (`Uniq_RC`: the local one at READ COMMITTED), with faults or a failover |
| `Move_*` | a bucket move while a scan of every shard, writers of the moving row and READ COMMITTED read-modify-writes run (`Move_ScanPrepare`: the scan commits through PREPARE), with faults or a failover |
| `Bug_*` | one protection switched off; TLC **must find a violation** |
| `Tradeoff_*` | a documented weaker guarantee; TLC **must find a violation** |

The full results table (states, depth, time, outcome) is in
[design §5.1](design.md#51-model-checking), generated from the TLC logs in
`tla/out/`.

### Running TLC

You need Java 11+ and `tla2tools.jar`:

```sh
curl -fsSL -o tla/tla2tools.jar https://github.com/tlaplus/tlaplus/releases/download/v1.8.0/tla2tools.jar

tla/check.sh                                    # the fast configurations, outcomes checked (as in CI)
WORKERS=4 tla/run_all.sh Recov_GC Bug_NoTombstone   # any configurations; logs in tla/out/
python3 tla/summarize.py                        # table of every result in tla/out/
```

`run_all.sh` honours `TLA2TOOLS` (path to the jar), `WORKERS` (default 1)
and `TLC_HEAP` (default 3g). The large configurations (`Skew*`,
`Move_ScanPrepare`), which `check.sh` leaves out, take from minutes to an
hour (`Move_ScanPrepare`: 48 minutes, 20.6 million states);
`Skew_GC_ROValidate` did not finish within two hours (47.8 million states
explored without a violation).

To add a protection switch: add a `Bug*` constant to `TarnCommit.tla`, guard
the protection with it, add a flag to `gen_cfgs.sh`, and add a `Bug_*`
configuration that TLC must fail.

## Verus proofs

`crates/tarn-verified` is ordinary Rust inside `verus!` blocks: `cargo`
builds it with the specifications and proofs erased, and the server uses the
result. Verus proves:

* **Routing** (`route.rs`): the CRC-32C used for bucket hashing equals a
  bitwise specification for every input; bucket and shard numbers are in
  range; the bucket-to-shard map of a new cluster is monotone and onto; the
  map built from the buckets the shards report owning gives each bucket the
  shard of the report that holds it, or none, and refuses reports that
  claim a bucket twice.
* **Coordinator decisions** (`coord.rs`): `decide` commits exactly on
  unanimous YES and aborts exactly when some vote is NO; `resolve_step`
  (one step of recovery) decides COMMIT only when the states it is given
  mean "committed", decides ABORT only when a participant is tombstoned,
  fences only participants without a record while the record is STAGING,
  and fences a missing record only before expiry or while some participant
  is prepared.
* **MySQL wire codec** (`wire.rs`): length-encoded integers and strings
  encode to the byte-level specification and decode back without reading
  out of bounds; packet framing produces exactly MySQL's multi-packet
  format (including the empty terminator after a payload that is a multiple
  of 16 MiB) and deframing inverts it. No function can panic.

The proofs are about single functions given their inputs. Protocol-level
facts they rely on, such as "a missing participant record under a STAGING
decision record means the participant never prepared", are checked by TLC.

Running Verus needs the release the proofs were written for and the Rust
toolchain it is built with (1.98.1, the version in `rust-toolchain.toml`):

```sh
rustup toolchain install 1.98.1
curl -fsSL -o verus.zip https://github.com/verus-lang/verus/releases/download/release%2F0.2026.10.04.426d8b0/verus-0.2026.10.04.426d8b0-x86-linux.zip
unzip -q verus.zip -d ~/verus
cd crates/tarn-verified && ~/verus/verus-x86-linux/verus --crate-type=lib src/lib.rs
# verification results:: 66 verified, 0 errors
```

`crates/tarn-verified/tests/erased.rs` exercises the erased code with
concrete inputs (codec boundaries, CRC check value, decision cases).

## Test suites

| Suite | Command | Needs | Checks |
|---|---|---|---|
| Unit tests | `cargo test --workspace` | Tarantool (IPROTO tests start their own) | codec, packets, SQL normalization, expressions, collation weights, coordinator decisions, the cross-shard deadlock search, IPROTO client against a real Tarantool |
| Storage protocol | `lua/test/run.sh` | Tarantool | every participant procedure on two real shards, recovery, record lifecycle and garbage collection, refusal on demoted nodes, lock-index rebuild while a PREPARE waits for a quorum, row locks, schema-change steps and definition fences; bucket moves step by step, and the refusals of requests for buckets a shard does not own (`bucket_test.lua`) |
| SQL differential | `python3 tests/sql/difftest.py --tarn HOST:PORT --ref HOST:PORT` | a Tarn cluster and MariaDB 10.11, PyMySQL | 465 statements compared exactly with MariaDB: column names, rows, error codes, affected rows, final table contents and indexes (also after `ALTER TABLE`), including collations and their weight strings |
| SQL fuzzing | `python3 tests/sql/fuzzdiff.py --tarn ... --ref ... -n 2000` | same | randomly generated queries compared with MariaDB |
| Transactions | `python3 tests/sql/txntest.py --tarn HOST:PORT --kv SHARD1,SHARD2` | a Tarn cluster | rollback, visibility, write skew, lost update, phantoms, fractured reads, real-time order, cross-shard atomicity, direct KV reads, concurrent inserts of case variants of a case-insensitive key, and a concurrent bank workload with a ledger that exposes lost or duplicated commits |
| READ COMMITTED | `python3 tests/sql/rctest.py --tarn HOST:PORT[,HOST:PORT...]` (every SQL server) | a Tarn cluster with two or more shards | isolation variables, lock waits, lock wait timeouts, local and cross-shard deadlocks (also across SQL servers), no deadlock-detector calls without lock waits, locks of failed lock requests released with their transaction, concurrent counters without lost updates, semi-consistent `UPDATE`/`DELETE`, `SKIP LOCKED` job queues, `NOWAIT`, `FOR SHARE`, duplicate inserts and concurrent upserts on unique keys, savepoints, statement rollback, locks released on disconnect |
| Schema changes | `python3 tests/sql/ddltest.py --tarn HOST:PORT[,HOST:PORT]` | a Tarn cluster, ideally with two SQL servers (`cluster.sh -s 2`); the `mysql` client | a mysqldump file, multi-statement batches and `COM_SET_OPTION`, refused alterations, `ALTER TABLE` about 300 times while both servers write the table: no autocommit statement fails, no acknowledged write is lost or duplicated; concurrent schema changes; transactions spanning an `ALTER` |
| Bucket moves | `python3 tests/sql/movetest.py --tarn HOST:PORT[,HOST:PORT...]` | a Tarn cluster with two or more shards | transfers with a ledger, READ COMMITTED read-modify-writes, audits and READ COMMITTED scans of every shard while the buckets of their rows keep moving: nothing lost or applied twice, every scan sees each row once |
| Move faults | `tests/movefaults.sh` | Tarantool | the SQL server driving a move killed while it copies, while its fence drains, between its flip and its send, and between its send and its receive, and after its flip with the move given up on the sending shard: another server gives the move up, or finishes it |
| Failover | `tests/failover.sh` (`ENGINE=vinyl`: vinyl tables) | Tarantool | 2 shards × 3 replicas under the bank workload, READ COMMITTED transfers and `movetest.py`: a live leader change, then two leader kills; every check must hold |
| DDL faults | `tests/ddlfaults.sh` | Tarantool | `CREATE TABLE`, `DROP TABLE` and `TRUNCATE` with a shard down, and with a shard killed while it applies: each takes effect on every shard or on none, checked on the shards directly |
| Faults | `tests/faults.sh` (`ENGINE=vinyl`: a vinyl table) | Tarantool | a reply lost after an autocommit write was applied: the client must get "outcome unknown" and the write must be applied exactly once |

`bench/baselines.sh start mariadb` starts a local MariaDB on port 3407 with
a `sb` user for the differential tests (or point `--ref` at any MariaDB
10.11). `scripts/cluster.sh start -n 2 -r 1 -p 4000` starts a cluster.

## Continuous integration

`.github/workflows/ci.yml` runs on every push and pull request:

* **lint**: `cargo fmt --check` and `cargo clippy -D warnings`;
* **test**: unit and integration tests, storage protocol tests, the SQL
  suites against MariaDB 10.11 (on a cluster with two SQL servers), the
  schema-change, bucket-move, fault-injection and failover tests;
* **tla**: `tla/check.sh` (every `Bug_*` and `Tradeoff_*` configuration must
  fail, the fast correct configurations must pass);
* **verus**: the Verus proofs.
