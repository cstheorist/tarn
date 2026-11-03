# READ COMMITTED profile refresh for #186

Jeremiah approved replacing draft #202 with this narrower profiling PR;
#202 is closed with its historical experiments retained. Both allocation
candidates remain experimental. Leave #186 open for a measured optimization. This refresh does
not change production execution or defaults, and is not evidence of a
candidate speedup.

## Source and fixture

[Run 41515600135](https://github.com/cstheorist/tarn/actions/runs/41515600135)
profiles source `088cdb15477e7646d5483561060fc92cc0548bd3`. Its production
Rust, Lua, cluster scripts and dependency lockfile match main
`295305a26fbdd1aaa773feaafc6a3423548e22bd`; only the collector and manual
workflow were added. Each mode runs two shards with three synchronous
replicas each, WAL `fsync`, quorum `N / 2 + 1`, Tarantool
`3.9.0-0-g8b9a0f5`, Rust 1.98.1 release artifacts, and 256 MiB memtx per
storage process. Vinyl cache is zero; the small fixture does not establish
beyond-RAM behavior. A single host runs the six storage processes, gateway
and load generator. Each runner exposes four logical CPUs. Modes run
sequentially on separate hosted runners:

| Mode | Reported CPU model |
|---|---|
| memtx / native disabled | AMD EPYC 7763 |
| memtx / native enabled | AMD EPYC 7763 |
| vinyl / native disabled | Intel Xeon Platinum 8573C |
| vinyl / native enabled | AMD EPYC 9V74 |

These are **not matched comparisons between engines or native modes**.
For each mode, one client runs the uncontended shape, eight clients use
unrelated keys, and eight clients share one hot key. Every transaction
increments one row and inserts a unique token into a ledger on that same
shard. It sends BEGIN, UPDATE, INSERT and COMMIT, with no retry. Each shape
has a separate three-second warmup, five ordinary five-second windows, and
five separate bounded guest-stack sampling windows. Shape order alternates
between repeats; ordinary and profiled phases are not paired interventions.

All 132 samples complete: 12 warmups, 60 ordinary measurements and 60
profiled measurements. **367,708 acknowledgements** reconcile with exact
ledger identities and per-key counter effects; there are **zero refusals,
unknowns or retries**. All six nodes report effective `check_locks=false`
and `check_yields=false` before and after every sample. Process identities
and leadership remain stable within every sample. All guest histograms
satisfy their caps and stop without a watchdog expiry.

## Ordinary-window results

TPS is median [minimum, maximum] across five windows. Latencies are medians
of each window's acknowledged p50/p99. TX CPU sums the six storage transaction
threads, divided by acknowledgements; Lua allocation likewise sums all six
storage processes. Both intervals include diagnostic/setup/background work
and have tick/GC/JIT uncertainty. They are not per-function costs or total
system CPU (gateway/client CPU is excluded).

| Engine / native | Shape | TPS median [range] | p50 / p99 ms | TX CPU us/ack | Lua KiB/ack |
|---|---|---:|---:|---:|---:|
| memtx / disabled | uncontended | 361.0 [359.4, 361.7] | 2.66 / 4.31 | 1195 | 39.6 |
| memtx / disabled | unrelated | 879.8 [754.2, 884.0] | 8.84 / 15.69 | 897 | 41.0 |
| memtx / disabled | hot | 421.2 [410.0, 423.0] | 18.31 / 27.81 | 1159 | 43.1 |
| memtx / enabled | uncontended | 350.1 [341.9, 355.7] | 2.72 / 5.05 | 1222 | 39.8 |
| memtx / enabled | unrelated | 856.1 [755.5, 870.6] | 8.98 / 16.04 | 902 | 40.9 |
| memtx / enabled | hot | 411.1 [396.2, 416.4] | 18.75 / 27.25 | 1188 | 43.2 |
| vinyl / disabled | uncontended | 363.0 [355.7, 368.2] | 2.43 / 21.65 | 1011 | 40.2 |
| vinyl / disabled | unrelated | 1052.1 [955.5, 1056.5] | 6.82 / 27.94 | 645 | 41.0 |
| vinyl / disabled | hot | 409.6 [327.9, 415.7] | 17.47 / 40.84 | 988 | 43.7 |
| vinyl / enabled | uncontended | 396.5 [385.8, 397.0] | 2.44 / 3.67 | 1220 | 40.1 |
| vinyl / enabled | unrelated | 969.3 [851.6, 978.5] | 8.00 / 14.64 | 876 | 41.0 |
| vinyl / enabled | hot | 455.0 [439.7, 470.2] | 16.78 / 24.70 | 1164 | 43.5 |

## What the profile establishes

- No-contention and unrelated-key windows record zero row-lock and in-flight
  commit waits. Hot-key windows record approximately one `plock_waits` and
  one `commit_waits` increment per acknowledgement. These are event counts,
  not measured wait durations; the two counters describe different waits.
- The workload makes about three storage calls per commit, plus diagnostic
  and background traffic. All native served and fallback counter deltas
  are zero even with the module loaded: these statements use the Lua row-lock
  owner and `tarn_commit_rc` path. A loaded native library is not native
  execution evidence.
- Allocation is roughly 40–44 KiB per acknowledged transaction. The retained
  summary also reports selected GC-step counters and process CPU. This does not attribute
  a fraction of those bytes to waiter construction or final guards.
- Guest stacks repeatedly contain `tarn_lock`/`pl_begin`, freshness/quorum
  barrier functions, and the `tarn_commit_rc` confirmation/release area.
  GC-state samples are also retained. Generic C-state samples and Lua
  call-site lines cannot separate engine CPU, WAL fsync, quorum waiting,
  ownership bookkeeping and final freshness validation into exact costs.
  JIT inlining and callback delay limit source-line attribution. Do not
  label a sample at the commit-hook call site as a measured fsync cost.

Thus this is a current RF3/fsync **baseline and coarse profile**, not the
complete direct/end-to-end, alternating candidate/control study required
by #186. The missing cost separation and candidate comparison remain work
before choosing a production optimization. There is no default-enablement
claim, open-loop capacity result, physical-host scale claim, or complete
semantic/fault qualification of either candidate.

## Why #202 needs replacement

The draft's paired harness sets `TARN_CHECK_LOCKS='0'` and
`TARN_CHECK_YIELDS='0'`, but `lua/storage.lua` enables each check for **any
nonempty value**. Both paired arms ran with test checks enabled. Their
exact-effects records remain useful; timing/CPU/allocation ratios describe
that instrumented runtime. Shared debug overhead need not cancel between
candidates, native modes, GC or scheduling. Do not numerically correct those
ratios into production estimates.

The archived 15-second guard study in `codex/rc-plan-guard-long` independently
reconciles 1,082,777 acknowledgements and ten refusals. Six of twelve observed
goodput-ratio ranges cross one. Native memtx medians are 1.0042, 1.0070 and
0.9994 for uncontended, unrelated and hot shapes. Lua memtx has a hot-row
median ratio 0.8604 [0.5724, 1.4571], alongside an uncontended increase. These
are retained observations, not confidence intervals or production gains.
Earlier condition studies likewise show no consistently positive ranges.
Neither an all-workload gain nor the absence of a production benefit follows
from them. A production candidate needs fresh matched checks-off pairs,
direct and SQL measurements, pinned-MySQL histories, native/Lua equivalence
and the required RF3 faults/models.

The refresh uses empty debug environment settings and verifies effective
flags. Its first diagnostic setup, run 41515114826, was explicitly canceled
while still building after review found the flag error; no measurement lane
ran. The canceled record remains visible, and no main CI was canceled.

Review also found that a worker blocked after COMMIT dispatch or an interrupted
controller could omit its token from failed-sample evidence. The collector now
publishes unknown identities before dispatch and seals admission before taking
its snapshot. Applied-but-unreplied COMMIT and KeyboardInterrupt injections
retain unknown tokens/readback and perform no retry; both fail the pre-fix
collector. These further fixes are in `470c954`, **after** measured source
`088cdb1`. All measured samples settled completely; this is not a claim that
the later failure-path fix was used for their timing.

## Reproduce the retained audit

From the repository root, fetch the measured commit first if it is not already
available locally (the profiling branch was subsequently rebased):

```sh
git fetch origin 088cdb15477e7646d5483561060fc92cc0548bd3
python3 bench/rc-evidence/run-41515600135/audit.py \
  bench/rc-evidence/run-41515600135/manifest.json
```

The auditor uses only Python and Git, verifies archive/member hashes, source,
collector/profiler/release receipts, runtime settings, exact outcomes,
latency quantiles, sample schedules and profile bounds, and recomputes the
summary independently of the collector. `summary.json` preserves all twelve
groups, ranges, GC/CPU metrics and the largest guest stacks. The manifest
lists the full retained files. Exact release executables and full fixture
storage are in the original workflow artifacts; repository archives retain
their hashes and fixture logs/configuration, rather than executable binaries
or the complete WAL/snapshot files.

Independent raw review also checked all 236 retained archive members against
the downloaded artifacts, recomputed every outcome/metric, and confirmed the
summary. The profiler retained 120 histograms containing 21,368 weighted
samples, at most 174 distinct stacks each, with no overflow or watchdog
expiry. See [the independent review](rc-evidence/run-41515600135/independent-review.txt).

## Validation and review

Nine collector accounting/runtime/fault tests pass; the two new fault tests
fail the old collector. Rust format and strict workspace/all-target Clippy
pass. Independent review confirmed and rechecked the flag and attempt-loss
fixes. Production Rust/Lua, protocol/Verus sources and the protected copy/ALTER
paths are unchanged. Local `cargo test --workspace --locked` reaches the
IPROTO integration tests and fails because this desktop has no Tarantool
executable; its prior socket permission failure disappears outside the
sandbox. A full PR CI gate, Lua storage suites with/without the native module,
and any additional release gates remain required before merge. The successful
profiling matrix is not a replacement for them.
