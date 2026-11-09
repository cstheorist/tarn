# Lock calls without the queue wait: RF3/fsync cost study (#314)

A READ COMMITTED lock call (`tarn_lock`, `tarn_lock_range`) starts with a
freshness barrier. Tarantool's `linearizable` barrier collects fresh vclocks
from a quorum and waits for the local WAL to catch up to them. Its last step
then waits for the newest transaction in the synchronous queue
(`txn_limbo_wait_last_txn`). With unrelated keys in flight, that wait took
63% of a transaction's storage time ([`RC_PHASES.md`](RC_PHASES.md)).

On a writable queue owner, the last step adds nothing. A new leader's
PROMOTE settles the queue before the leader can write, and a newer term's
PROMOTE makes an instance read-only before that term's rows reach its WAL.
The lock call checks that it is still writable after the barrier.
`tla/StrongReadFreshness.tla` checks this under those two premises (#324).
Tarantool doesn't export the first two steps, so Tarn runs them itself:
- a round of `tarn_peer_vclock` calls goes to the shard's other replicas,
  shared by the calls that began before it;
- their vclocks are joined, and the local WAL catches up to them.

`TARN_LOCK_BARRIER` selects the barrier:
- `queue` is the full barrier;
- `owner` is the round (the default);
- `auto` runs the round while the queue holds transactions, and the full
  barrier while the queue is empty.

## Method

`.github/workflows/rc-guard-cost.yml` runs `bench/rc_cost_compare.py`, the
harness of [`RC_PLAN_GUARD.md`](RC_PLAN_GUARD.md), with hypothesis
`lock-barrier` (candidate `owner`) or `lock-barrier-auto` (candidate
`auto`). The control is `queue`.

- **Matched lanes.** Both variants run the same sources and the same release
  binaries. Only the storage nodes' `TARN_LOCK_BARRIER` differs.
- **Mechanism check.** After each lane's timing, every storage node reports
  its setting. Only the candidate's leaders ran rounds, and no round fell
  back to the full barrier.
- **Pairs.** Each engine/native configuration runs five alternating
  control/candidate pairs on fresh processes, on one hosted runner per
  configuration. Each lane has three-second warmups and 15-second
  measurement windows.
- **Shapes.** One uncontended client; eight clients on separate keys; eight
  on one hot key. Each transaction increments a row and inserts a unique
  ledger token on the same shard, over SQL (BEGIN, UPDATE, INSERT, COMMIT),
  so it makes two lock calls and one commit. There are no retries.
- **Setup.** Two shards of three synchronous replicas, WAL fsync,
  Tarantool 3.9.0, 256 MiB memtx per storage process, zero vinyl cache.

Values are candidate/control paired medians [minimum, maximum] over the five
pairs. They are not confidence intervals. TX CPU sums the six storage TX
threads per acknowledgement.

| Run | Source | Candidate | Measured acknowledgements | Refused |
|---|---|---|---:|---:|
| [42545257483](https://github.com/cstheorist/tarn/actions/runs/42545257483) | `f88ac08` | `owner`, asking every other replica | 1,001,002 | 0 |
| [42547826184](https://github.com/cstheorist/tarn/actions/runs/42547826184) | `5cfe148` | `owner`, asking a quorum's worth | 1,255,333 | 0 |
| [42547835075](https://github.com/cstheorist/tarn/actions/runs/42547835075) | `5cfe148` | `auto` | 1,192,206 | 0 |

The first version's rounds asked every other replica. The second asks as
many as a quorum needs (one in RF3), those that answered the last round
first. It asks the rest once those haven't answered within four round trips
(2–20 ms).

## Results

### `owner`, asking a quorum's worth (the default)

| Engine / native | Shape | Goodput | p99 | TX CPU per ack |
|---|---|---:|---:|---:|
| memtx / disabled | uncontended | 1.019 [0.959, 1.190] | 1.064 | 1.020 |
| memtx / disabled | unrelated | **1.349** [1.244, 1.393] | 0.802 | 1.013 |
| memtx / disabled | hot | 1.007 [0.921, 1.022] | 0.971 | 1.099 |
| memtx / enabled | uncontended | 1.030 [1.010, 1.193] | 0.999 | 1.019 |
| memtx / enabled | unrelated | **1.318** [1.267, 1.354] | 0.818 | 1.051 |
| memtx / enabled | hot | 0.995 [0.952, 1.006] | 1.026 | 1.098 |
| vinyl / disabled | uncontended | 1.011 [1.005, 1.080] | 0.984 | 1.001 |
| vinyl / disabled | unrelated | **1.227** [1.214, 1.272] | 0.862 | 1.020 |
| vinyl / disabled | hot | 0.993 [0.986, 1.002] | 1.040 | 1.045 |
| vinyl / enabled | uncontended | 1.022 [0.996, 1.081] | 1.029 | 0.994 |
| vinyl / enabled | unrelated | **1.196** [1.185, 1.216] | 0.884 | 1.037 |
| vinyl / enabled | hot | 1.005 [0.990, 1.036] | 1.009 | 1.031 |

### `auto`

| Engine / native | Shape | Goodput | p99 | TX CPU per ack |
|---|---|---:|---:|---:|
| memtx / disabled | uncontended | 0.995 [0.983, 0.999] | 1.030 | 1.005 |
| memtx / disabled | unrelated | **1.209** [1.197, 1.268] | 0.877 | 1.041 |
| memtx / disabled | hot | 0.993 [0.983, 1.011] | 1.000 | 1.008 |
| memtx / enabled | uncontended | 1.068 [0.846, 1.426] | 0.843 | 0.989 |
| memtx / enabled | unrelated | 1.344 [0.861, 1.706] | 0.471 | 1.045 |
| memtx / enabled | hot | 1.088 [0.791, 1.504] | 0.781 | 0.989 |
| vinyl / disabled | uncontended | 1.004 [0.973, 1.017] | 1.004 | 1.010 |
| vinyl / disabled | unrelated | **1.213** [1.195, 1.214] | 0.849 | 1.051 |
| vinyl / disabled | hot | 1.011 [0.978, 1.048] | 1.014 | 1.003 |
| vinyl / enabled | uncontended | 0.988 [0.931, 1.051] | 1.083 | 1.034 |
| vinyl / enabled | unrelated | **1.251** [1.183, 1.685] | 0.990 | 1.094 |
| vinyl / enabled | hot | 0.985 [0.951, 1.011] | 0.994 | 1.033 |

### `owner`, asking every other replica (first version)

| Engine / native | Shape | Goodput | p99 | TX CPU per ack |
|---|---|---:|---:|---:|
| memtx / disabled | uncontended | 1.019 [0.997, 1.166] | 1.084 | 1.111 |
| memtx / disabled | unrelated | **1.323** [1.266, 1.535] | 0.832 | 1.113 |
| memtx / disabled | hot | 0.988 [0.968, 1.082] | 1.066 | 1.159 |
| memtx / enabled | uncontended | 0.989 [0.979, 1.049] | 0.997 | 1.142 |
| memtx / enabled | unrelated | **1.195** [1.181, 1.216] | 0.903 | 1.136 |
| memtx / enabled | hot | 0.975 [0.962, 0.997] | 1.075 | 1.154 |
| vinyl / disabled | uncontended | 1.014 [0.989, 1.034] | 0.943 | 1.128 |
| vinyl / disabled | unrelated | **1.207** [1.202, 1.239] | 0.851 | 1.111 |
| vinyl / disabled | hot | 0.988 [0.986, 0.998] | 1.048 | 1.131 |
| vinyl / enabled | uncontended | 1.007 [1.002, 1.132] | 0.965 | 1.117 |
| vinyl / enabled | unrelated | **1.291** [1.276, 1.312] | 0.868 | 1.124 |
| vinyl / enabled | hot | 0.987 [0.977, 0.999] | 1.064 | 1.156 |

## Interpretation

- **Unrelated keys gain, every time.** In every configuration of the three
  runs, the goodput median rose 20–35% and the p99 median fell. Every pair
  gained except one, in `auto`'s noisy memtx/enabled cohort. This is the
  queue wait that `RC_PHASES.md` measured.
- **One client or a hot key: about the same.** Under `owner` as now built:
  - the uncontended medians are 1–3% above one, and two configurations
    have every pair above one;
  - the hot-key medians are within 0.7% of one, and every range crosses one.

  The runners' full barrier with one client is cheaper than on the 2-CPU
  host of `RC_PHASES.md`, where the round gained there too.
- **CPU.** The first version cost 11–16% more TX CPU per acknowledgement.
  Asking a quorum's worth of replicas brought that down to at most 5%,
  except about 10% on a memtx hot key. `auto` costs at most 5%, except
  vinyl/enabled with unrelated keys (9%): it saves the calls while the queue
  is empty, which is when they gain nothing.
- **`owner` or `auto`.** With unrelated keys both gained alike: medians of
  1.20–1.35 and 1.21–1.25, leaving out `auto`'s noisy cohort. With one
  client, `owner`'s medians were 1–3% above one and `auto`'s within 1.2% of
  one. `auto`'s memtx/disabled pairs were all below one there, by up to
  1.7%. `owner` costs more CPU on a hot key. The runs used different
  runners, so these comparisons aren't matched.
- **The noisy cohort.** `auto`'s memtx/enabled lanes moved widely: control
  goodput ranged from 288 to 528 TPS on the hot key. Every observation is
  kept, and that cohort's medians aren't used above.
- **Separate runners.** Each configuration ran on its own runner, and
  runners differ: `owner`'s memtx/disabled runner ran about 1.7 times as
  fast as the others. Comparisons hold within a configuration.

So `owner` is the default, for the small uncontended gain and a single
path. `auto` remains for deployments short of CPU, and `queue` keeps the full
barrier.
