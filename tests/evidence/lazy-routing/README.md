# Composite lazy routing evidence (#185)

The [cost report](../../../docs/lazy-routing-cost.md) defines the workloads,
comparison, accounting and uncertainty. `index.json` hashes the retained
artifacts; `provenance.json` records source, local build limits and failed
attempts. No failed or incomplete attempt is counted as qualification.

* `bound-{uniform,skewed}-{1,4,16}.tar.gz`: six local cases, each with five
  alternating baseline/composite pairs, ten seconds per arm, four RF3/fsync
  groups, 32 client threads/four processes and a twelve-CPU quota. Each
  archive contains the report, stdout and hashed raw-latency sidecars.
* `summary-bound.json`: all sixty arm summaries, including paired ratios,
  extrema, calls, bytes, CPU, target shares, lag, durable metadata and
  controlled outage catch-up. All sixty arms completed without workload
  errors or a p99-ceiling violation.
* `qualification-cluster.json` and `qualify-bound.sh`: exact gateway
  configuration and commands. Storage resource settings and the optimized
  local build command are recorded in the report and provenance. Local
  node logs were removed by the helper's default cleanup; this limitation
  does not remove any per-arm reports or raw latency samples.
* `ci-41073441278-skewed-4.tar.gz` and its summary: the first completed
  standard-release CI case, with five pairs each for write-heavy,
  read-heavy/prefix and scheduled low-load work on four x86_64 CPUs.
  It retains the measured regressions. This archive predates the `KEEP=1`
  cleanup correction and therefore lacks the cluster node logs.
* `validation-local.tar.gz`: 60 Lua lifecycle checks; source/target
  movement and JOIN/restart controls; composite RF3/fsync leader-death and
  recovery on MemTX and Vinyl; ordered SQL merge, stale-key rechecks,
  failed-owner refusal and incarnation migration; both full lazy SQL
  suites against MariaDB (11 tests each, zero failures/skips); the final
  1,092-case differential pass and earlier failed attempts.
* `development-attempts.tar.gz`: exploratory workload/resource probes,
  compiler failures and the raw-retention SIGKILL attempt. Probes differ in
  CPU quota, client processes, base/target distribution and reader version;
  they must not be pooled with qualification.
* `isolated-{uniform,skewed}-{1,4,16}.tar.gz` and
  `summary-isolated-final.json`: the complete sixty-arm matrix rebuilt from
  isolated source `841aee2`. The skewed sixteen-lane case uses a fresh fixture
  after two unsuccessful attempts. Every qualifying arm has its own raw
  samples, counters and source/binary hashes. Four-lane improvement repeats;
  sixteen-lane improvement is within uncertainty in this later run.
* `isolated-interrupted-attempt.tar.gz`,
  `integrated-first-cluster-logs.tar.gz`, `integrated-r2-clock-refusal.tar.gz`
  and `isolated-r3-cluster-logs.tar.gz`: the interrupted three-pair attempt,
  original cluster logs, pre-measurement clock refusal, and successful fresh
  fixture logs respectively. "Integrated" here means main's MySQL contracts
  merged with #185 at `841aee2`; none of these measurements use the externally
  combined `caf39e6` branch.
* `validation-isolated.tar.gz`: the `841aee2` build, all 330 workspace tests
  passing, workspace clippy, and commands/configuration for the final study.
  `sql-final-cluster-logs.tar.gz` retains the fresh cluster used for the full
  1,092-case SQL differential pass already recorded in `validation-local`.
* `ci-41077514469-{uniform,skewed}-1.tar.gz` and corresponding summaries:
  completed cases from the cancelled predecessor workflow on source
  `841aee2`, including the unqualified uniform write-heavy latency result.
* `ci-41092212161-{uniform,skewed}-{1,4,16}.tar.gz` and
  `summary-ci-41092212161.json`: the complete standard-release CI matrix,
  180 qualifying arms across write-heavy, read-heavy and low-load work.
  All archives include the node logs. Source `3f6bd03` has the same tree as
  `841aee2`; the retained results include 13–14% write-heavy and 6–8%
  read-heavy regressions in the skewed four- and sixteen-lane fixtures.
* `validation-b107cbb.tar.gz`: after merging reviewed main `b3317b3`, all
  334 workspace tests, workspace clippy, both 11-test lazy SQL suites,
  1,122 differential cases on each engine, and composite RF3/fsync leader
  failures/recovery pass. Commands and full fixture logs are retained.
  `validation-b107cbb-setup-refusal.tar.gz` preserves the initial invalid
  fixture attempt: incorrect cleanup left the previous cluster listening.
  The successful rerun used explicit-directory cleanup and verified every
  process identity and the rebuilt gateway executable before testing.

The new `LazyRouting` model and six expected-failure outputs are retained
under `tla/`. They cover a finite frozen generation, not unbounded SQL,
hash distribution or production-scale performance. Existing version,
frontier and handoff models retain their separate proof boundaries.

Extract each archive into a fresh directory and run:

```sh
python3 bench/summarize_lazy_routing.py extracted/*.json --output summary.json
```

Use only workload reports as inputs (not configuration or prior summaries).
Keep each report beside its `.samples` directory. The summarizer checks
every sidecar hash and rejects missing pairs, errors and incomplete arms
from qualification. It never combines different fixture configurations.
