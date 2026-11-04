# Bounded engine mark experiment

Run each row in a fresh standalone Tarantool process from the repository root:

```sh
TARN_GC_SOURCE_IDENTITIES_JSON='{"tarn":"COMMIT","runtime":"SHA256","luajit":"COMMIT"}' \
  tarantool tests/gc_upvalue.lua ordinary 20000
```

The positional arguments are `PROFILE [ITERATIONS]`; defaults are `ordinary`
and 20,000. The inclusive iteration bound is 1–1,000,000. Available profiles:

| Profile | JIT | GC | Construction |
| --- | --- | --- | --- |
| `ordinary` | Runtime default, required on | Runtime default | One engine |
| `jit-off` | Explicitly off | Runtime default | One engine |
| `incremental-gc` | Runtime default, required on | Explicit `step(1)` after invalidation | One engine |
| `construction-warm` | Runtime default, required on | Runtime default | Batches of up to 64 engines |
| `construction-warm-incremental-gc` | Runtime default, required on | Explicit `step(1)` after invalidation | Batches of up to 64 engines |
| `retained-construction-warm` | Runtime default, required on | Runtime default | One retained batch of up to 64 engines |
| `retained-construction-warm-incremental-gc` | Runtime default, required on | Explicit `step(1)` after invalidation | One retained batch of up to 64 engines |
| `retained-construction-warm-incremental-gc-jit-off` | Explicitly off | Explicit `step(1)` after invalidation | One retained batch of up to 64 engines |

The runner must supply `TARN_GC_SOURCE_IDENTITIES_JSON`, a nonempty JSON object
containing its independently verified source/runtime identities. The fixture
embeds it without certifying it. `TARN_GC_WORK_DIR` optionally selects the
runner's isolated database/log directory; otherwise the fixture creates a
temporary directory. Use an empty directory per invocation. The database has
no application spaces, network listener, native module, or cluster. WAL is
disabled because no transactions or durable writes are exercised. Logs remain
in the work directory for the runner to collect and remove.

The fixture loads unchanged `tarn_engine` and the actual `tarn_lazy_meter`.
Constructor injection supplies `cfg.sync=false`, an atomic callback executor,
and a checked exclusion no-op. Other engine services are absent and must not
be called. The mark factory returns fresh `{lua=sequence, native=nil}` tables.
Only scalar expectations are retained outside the production upvalue. Every
cut checks the sequence, type, exact field content, and lack of metatable.
Every iteration calls `E.invalidate(true)`, refuses a controlled false cut,
then rebuilds through a true cut and verifies an empty accounting snapshot.
It exercises no transaction terminal callbacks or unrelated allocation churn.

Construction profiles attempt to warm the shared constructor/setter prototype
while each constructor's mark upvalue is open, then invalidate the returned
engines. They do not establish that a particular trace was compiled or that
the open-to-closed transition occurred on a trace. Incremental-GC profiles
are explicitly perturbed diagnostics. Neither replaces the ordinary baseline.

The retained variants construct one batch of `min(64, ITERATIONS)` real engines
and round-robin exercise those same instances for all requested iterations.
Their closed upvalues stay live across repeated invalidations and, in the
incremental-GC variant, repeated GC steps. The older construction profiles
instead create a fresh batch after exercising each previous instance once.
All receipts include the boolean `config.retained_engines`. Constructor counts
are `min(64, ITERATIONS)` for retained variants, `ITERATIONS` for the older
construction profiles, and one for single-engine profiles. Mark counts remain
constructors plus iterations, with three cut checks per iteration.

Stdout contains one JSON object. Exit 0 requires `complete=true`, `passed=true`,
exact iteration/callback counts, and all content invariants. Caught failures
exit 1 with both flags false, partial counts, position, failure kind and error. Process
crashes may produce no JSON; the runner must treat missing/invalid output,
nonzero exit, timeout, and crash evidence as unsuccessful independently.
The receipt includes effective configuration, runtime JIT flags, architecture,
GC64 mode, supplied source identities, and work directory. It reports a
bounded functional result, never a GC root cause or fix. In particular,
upstream LuaJIT `07b3cd3c` versus pinned `8af934d9` remains a hypothesis.
## Running the source-bound experiment

The existing `Native GC failover investigation` workflow accepts the manual
`mode=upvalue` input. It builds separate pinned baseline/candidate runtimes and
runs the configured profiles in fresh processes. Defaults are three trials per
profile and 20,000 iterations per trial; the collector rejects unbounded inputs.
The normal failover workflow mode is unchanged. The actual runtime experiment
is manual, while pull requests run the offline evidence controls.

`scripts/gc_upvalue_study.py` checks source/runtime hashes before and after the
run and retains every invocation, exit/signal, stdout/stderr, configuration and
functional receipt. It kills only its own unreaped child on timeout. Core
capture, when configured on the isolated runner, is limited to 1 GiB per child
and disabled after the first unsuccessful observation; retained cores are
associated with the child's PID. A missing/invalid receipt or collector timeout
cannot count as a functional pass.

Baseline functional failures are retained observations, not an automatic failed
collector job; candidate failures fail its job. All-pass runs report
`bounded_negative`. Every study reports `qualification_pass=false`: successful
collection is not a crash-cause or fix claim. Compare actual compiler/configuration
receipts across the separate runners before attributing a functional difference
to the candidate patch. There is no throughput, RF3/fsync or durability claim
from this standalone, WAL-disabled fixture.

## Bounded trace observation with archived runtimes

The separate `scripts/gc_trace_coverage.py` observer accepts only the audited
baseline/candidate binaries and exact runtime receipts from workflow run
`41764862465`. It verifies their hardcoded SHA-256 identities, the existing
runtime receipt contract, and every receipt evidence hash before execution and
again afterward. A self-consistent rebuilt runtime does not qualify for reuse.

```sh
python3 scripts/gc_trace_coverage.py --source "$PWD" \
  --runtime /absolute/extracted/upvalue-runtime --arm baseline \
  --output /absolute/fresh-observation-directory
```

Use a clean source checkout. The source identity includes the collector,
wrapper, offline controls and this README. Each arm runs exactly one fresh
process for each of `ordinary`, `construction-warm`, and
`retained-construction-warm`, with 20,000 iterations, 30 seconds per child,
zero core bytes, and an 8 MiB per-file `RLIMIT_FSIZE`. Timeout handling sends
TERM and, after five seconds if needed, KILL only to the owned unreaped child.
There are no iteration/profile/timeout overrides or automatic retries.

`tests/gc-upvalue/observe.lua` loads the unchanged functional fixture with its
original arguments and emits the shipped `jit.dump` module's `tbiT` text to
`trace.dump`: trace lifecycle, recorded bytecode and IR. Stdout remains the
original JSON receipt. This row adds diagnostic allocation and file I/O; it
does not replace the unobserved functional study. It performs no machine-code
disassembly, register dumps, core parsing or upstream reproducer workload.

`coverage.json` retains each invocation/result and their hashes; per-trial
records retain stdout/stderr/dump sizes and hashes, exit/signal, limits and
independently validated functional receipts. A timeout or SIGXFSZ/file-cap
result is an incomplete observer, never a runtime cause finding. Missing or
empty dumps cannot complete collection. Functional outcome and observer
completion are separate fields, and target coverage always remains
`unassessed` pending source/prototype/IR review.

For that review, bind recorded `invalidate_tickets` bytecode to its precise
engine source/prototype and the mark USETV operand, including inlined call
context. A trace starting elsewhere can contain the setter. Constructor
versus public invalidation context matters: constructor recording can alias
the still-open mark to a trace SSA slot. Identify UREFO/UREFC, USTORE, OBAR
and guards with their operands; a UREFO count alone does not establish the
open, non-SSA condition in upstream #1085. Record unknown phase/state when
the dump cannot distinguish them. A successful trace compilation proves
neither execution of that store after closing the upvalue nor crash causality.

