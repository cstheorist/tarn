# Final planned-write guard: RF3/fsync cost study (#186)

The bounded optimization checks replication mode once inside the existing
nonyielding final guard, then checks every planned space. A two-write
transaction needs one mode inspection instead of three. Clock, control-space,
writable and planned-space error ordering is preserved. Independent space
checks obtain their own mode. No mode survives a yield, and no quorum barrier,
lock, commit RPC or fsync is removed.

## Evidence and scope

The [SQL study](https://github.com/cstheorist/tarn/actions/runs/41532311462)
and [direct-storage study](https://github.com/cstheorist/tarn/actions/runs/41532314628)
measure source `b1e7ce88358e15437fdf82d10b2a0e961513ee0d`. Each uses the same
normal release binary/native library for scalar control and batched candidate;
only two copied Lua files differ. The proposed production code has the same
logic as the measured candidate (its experiment-only comment was removed).

Each mode uses two shards, three synchronous replicas per shard, WAL fsync,
Tarantool 3.9.0, Rust 1.98.1, 256 MiB memtx per storage process, and zero Vinyl
cache. Effective lock/yield debug checks are false before and after every
sample. The fixture is small; zero Vinyl cache is not a beyond-RAM result.
One hosted runner contains all six storage processes and the clients/gateway.

Every mode has five alternating control/candidate pairs on fresh processes,
three-second warmups and 15-second measurement windows. Shapes are one
uncontended client, eight clients on separate keys, and eight on one hot key.
Each transaction increments a row and inserts a unique ledger token on the
same shard. There are no retries. SQL sends BEGIN/UPDATE/INSERT/COMMIT; direct
clients send two `tarn_lock` calls and one `tarn_commit_rc`, using real tuple
layouts, generations, definition fences and buckets. Direct journal I/O and
client scheduling differ from SQL, so comparisons are within each driver;
direct-minus-SQL latency is not gateway-cost attribution.

The full matrices contain **2,393,238 measured acknowledgements**, or
**2,871,016 including warmups**, and zero refused/unknown outcomes. Independent
raw audit verifies exact token ledgers and per-key effects, sample schedules,
quantiles, deltas, release/source hashes and source-copy transformations.
All 4,191,978 direct journal events form complete admission → commit-dispatch
→ terminal sequences. Generation values are checked by the hash-bound driver,
but individual lock-reply generations are not journaled.

Native served/handback deltas are zero in every measured window in both enabled
modes: these calls use the Lua owner path; enabled native lock-copy bookkeeping
still runs. Absolute setup counters are excluded. Sampled
PIDs and leader/read-only roles remain stable; archived node logs record only
term 2 after startup. Samples do not contain continuous term telemetry.

## Matched results

Values below are candidate/control paired medians [minimum, maximum] over five
pairs, **not confidence intervals**. CPU sums six storage TX threads; allocation
sums six storage Lua heaps. Their intervals include diagnostic/background work,
and CPU has clock-tick quantization. Neither is total system CPU or an exact
per-function cost. Every measured window is retained, including outliers.

[Per-arm results](rc_guard_results.csv) include successful TPS ranges, p50/p99,
TX CPU microseconds/ack, allocated/freed bytes, GC phases, waits and outcomes.
Latency columns there are medians of window quantiles, not pooled quantiles.


### SQL

| Engine / native | Shape | Goodput ratio | Allocation/ack ratio | TX CPU/ack ratio | p99 ratio |
|---|---|---:|---:|---:|---:|
| memtx / disabled | uncontended | 1.014 [0.994, 1.121] | 0.864 [0.863, 0.872] | 0.991 [0.981, 1.050] | 0.959 [0.719, 0.991] |
| memtx / disabled | unrelated | 1.018 [0.987, 1.184] | 0.871 [0.870, 0.879] | 0.984 [0.964, 1.001] | 0.964 [0.346, 1.164] |
| memtx / disabled | hot | 1.028 [0.968, 1.145] | 0.876 [0.873, 0.882] | 0.996 [0.895, 1.019] | 1.025 [0.586, 1.178] |
| memtx / enabled | uncontended | 1.011 [0.976, 1.073] | 0.871 [0.867, 0.879] | 0.988 [0.986, 1.003] | 1.022 [0.973, 1.085] |
| memtx / enabled | unrelated | 1.015 [1.011, 1.029] | 0.874 [0.872, 0.883] | 0.976 [0.969, 0.988] | 0.937 [0.901, 0.964] |
| memtx / enabled | hot | 1.015 [1.011, 1.030] | 0.880 [0.876, 0.889] | 0.980 [0.975, 0.999] | 0.951 [0.937, 0.974] |
| vinyl / disabled | uncontended | 1.001 [0.958, 1.093] | 0.872 [0.860, 0.890] | 0.987 [0.983, 1.058] | 0.945 [0.372, 1.075] |
| vinyl / disabled | unrelated | 1.029 [0.981, 1.206] | 0.877 [0.871, 0.891] | 0.998 [0.922, 1.052] | 0.953 [0.902, 0.992] |
| vinyl / disabled | hot | 0.991 [0.967, 1.034] | 0.884 [0.875, 0.895] | 1.025 [0.945, 1.039] | 0.983 [0.695, 1.014] |
| vinyl / enabled | uncontended | 1.071 [0.649, 1.403] | 0.879 [0.834, 0.900] | 0.987 [0.954, 1.045] | 0.635 [0.409, 3.793] |
| vinyl / enabled | unrelated | 0.932 [0.609, 1.942] | 0.880 [0.863, 0.889] | 0.993 [0.934, 1.026] | 1.543 [0.140, 3.785] |
| vinyl / enabled | hot | 0.950 [0.695, 1.368] | 0.890 [0.869, 0.898] | 0.987 [0.966, 1.022] | 1.233 [0.609, 3.050] |

### Direct storage

| Engine / native | Shape | Goodput ratio | Allocation/ack ratio | TX CPU/ack ratio | p99 ratio |
|---|---|---:|---:|---:|---:|
| memtx / disabled | uncontended | 1.008 [0.988, 1.049] | 0.871 [0.864, 0.877] | 0.996 [0.991, 1.002] | 1.003 [0.960, 1.029] |
| memtx / disabled | unrelated | 1.031 [1.001, 1.056] | 0.873 [0.869, 0.878] | 0.971 [0.944, 0.983] | 0.951 [0.835, 0.979] |
| memtx / disabled | hot | 1.015 [1.006, 1.096] | 0.880 [0.877, 0.882] | 0.985 [0.921, 0.992] | 0.972 [0.773, 0.990] |
| memtx / enabled | uncontended | 1.008 [0.984, 1.074] | 0.870 [0.863, 0.871] | 0.980 [0.976, 0.987] | 0.988 [0.972, 1.017] |
| memtx / enabled | unrelated | 1.024 [1.015, 1.044] | 0.873 [0.865, 0.875] | 0.964 [0.943, 0.971] | 0.937 [0.919, 0.959] |
| memtx / enabled | hot | 1.003 [1.001, 1.014] | 0.880 [0.872, 0.884] | 0.980 [0.974, 0.989] | 0.983 [0.967, 1.003] |
| vinyl / disabled | uncontended | 1.027 [0.955, 1.082] | 0.874 [0.857, 0.887] | 0.984 [0.961, 0.995] | 0.966 [0.926, 1.045] |
| vinyl / disabled | unrelated | 1.013 [0.980, 1.072] | 0.877 [0.861, 0.896] | 0.977 [0.945, 0.995] | 0.983 [0.918, 0.995] |
| vinyl / disabled | hot | 1.000 [0.979, 1.045] | 0.883 [0.871, 0.902] | 0.999 [0.968, 1.018] | 0.988 [0.955, 1.018] |
| vinyl / enabled | uncontended | 1.020 [0.992, 1.057] | 0.876 [0.872, 0.910] | 0.994 [0.962, 0.999] | 0.978 [0.802, 1.126] |
| vinyl / enabled | unrelated | 1.017 [0.974, 1.064] | 0.872 [0.868, 0.878] | 0.975 [0.934, 0.997] | 0.973 [0.822, 1.067] |
| vinyl / enabled | hot | 1.017 [0.978, 1.054] | 0.881 [0.875, 0.886] | 1.000 [0.962, 1.013] | 0.945 [0.824, 1.218] |

## Interpretation and the noisy cohort

Allocation medians fall 11–14% in SQL and 12–13% in direct storage. The
memtx/native-enabled unrelated-key gain repeats across the pilot and both
qualification drivers: SQL goodput +1.5%, direct +2.4%, with every paired ratio
above one. Their TX CPU/ack medians fall about 2.4% and 3.6%, respectively.
This establishes a small end-to-end/resource improvement for that declared
workload, not a universal throughput gain. Other shapes have ranges crossing
one; some Vinyl SQL medians are negative.

SQL Vinyl/native-enabled is particularly variable. Its runner reports EPYC
9V74; SQL memtx-enabled reports EPYC 7763, and the disabled SQL jobs EPYC 9V45.
These are separate hosts, so engine/native modes are not matched comparisons.
Seven long-WAL-write warnings occur inside measured diagnostic brackets:
pair0 candidate/unrelated (replicas, 0.672–0.676 s) and pair3 control/unrelated
(leader, 0.580 s). This association does not explain all the variability.
No observation is discarded or numerically corrected.

One additional five-pair SQL Vinyl/native-enabled study was
[declared before dispatch](https://github.com/cstheorist/tarn/issues/186#issuecomment-6589970729).
The [fixed repeat](https://github.com/cstheorist/tarn/actions/runs/41536728436)
measures `51075aa861999ea16b9a1dd63ceebd8a3222049d`; only workflow engine selection
changed from the qualification source. It reconciles 358,218 measured
acknowledgements (428,149 including warmups), with no refusal/unknown/retry.
It ran on EPYC 9V45 and recorded no slow-WAL warnings. It does not replace
the original, and the source of the latency variation remains unresolved.

| Shape | Goodput ratio | Allocation/ack ratio | TX CPU/ack ratio | p99 ratio |
|---|---:|---:|---:|---:|
| Uncontended | 1.001 [0.909, 1.262] | 0.866 [0.856, 0.872] | 1.001 [0.913, 1.065] | 0.982 [0.169, 3.519] |
| Unrelated | 1.010 [0.908, 1.109] | 0.871 [0.867, 0.876] | 0.969 [0.915, 1.020] | 0.997 [0.901, 2.907] |
| Hot | 0.976 [0.868, 1.089] | 0.877 [0.873, 0.882] | 0.967 [0.947, 1.041] | 1.025 [0.575, 5.147] |

Both Vinyl/native-enabled SQL runs have negative hot-key goodput medians,
although every range crosses one. Allocation savings are repeatable; a
Vinyl throughput benefit or absence of regression is not established.
The default scope is Jeremiah's decision. A memtx-only initial default is
the conservative recommendation and would need its own matched check before
landing; the currently measured proposal batches all planned-space guards.

## Why the other candidate stays experimental

The [confirmation-cleanup pilot](https://github.com/cstheorist/tarn/actions/runs/41529992173)
removed roughly one hot-key in-flight wait per acknowledgement, and its
confirmed-owner hold plus rollback controls passed. Hot-key goodput ratio
was 0.998 [0.962, 1.021], so the intended throughput gain was not established.
It is not part of the production change.

The [guard pilot](https://github.com/cstheorist/tarn/actions/runs/41529987854)
independently reproduced mode inspections `N+1 → 1`, zero context switches,
and unchanged direct space checks. At width 32, helper allocation medians were
86,574 → 2,792 bytes/call. These helper repeats are diagnostic, not five
alternating independent direct workload trials. Both pilots reconcile 679,255
acknowledgements including warmups, with no refusal/unknown or retry.

## Validation and reproduction

The early production head `bd86fbd` passed Rust unit/IPROTO tests and 79 focused
live guard checks. The old scalar body, skipped-last-space and retained-mode
controls each failed their intended assertion. Independent static review
covered implementation, test/control logic and benchmark matching. The final
combined PR still requires all exact-head CI, native/Lua storage suites,
MariaDB 10.11 contracts, RF3 faults, model checks and Jeremiah's default decision.

`PlanGuardBatch` checks one two-space decision under an atomic-guard assumption.
Its skip-space and across-yield controls fail `NoUnsafeAcceptance`; actual
no-yield behavior, error order and public Tarantool properties remain live
runtime obligations. No Verus/ABI or commit-transition change is claimed.

Run the retained guard-only study with `bench/rc_cost_compare.py --help`, or the
manual `rc-guard-cost.yml` workflow after it is available on main. Select SQL
and direct separately. The transformer reconstructs the exact scalar control
from production batched sources and fails closed on mixed or drifted inputs.

[Retained evidence](rc-evidence/rc-guard-20261102/README.md) includes compressed
raw attempts, before/after/readback records, exact source copies, logs and
hash manifests, with independent audit results. Each qualification subdirectory
states its audit inputs and limitations. Bulk WAL/snapshots and executables
are omitted; original Actions artifacts retain them for their retention period.
The original audit scripts document dependencies on that original layout and
measured Git checkout rather than claiming to run on the compact archives alone.

