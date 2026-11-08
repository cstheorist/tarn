# Native GC crash investigation (#160)

Storage processes crashed in Lua's garbage collector. The cause found is
in Tarn's Lua: the engine's constructor called a function that stored a
fresh table into one of the constructor's locals, and a LuaJIT trace of
that function stores without the collector's barrier once the local's
upvalue has closed. The section "Cause" has the evidence and the fix. The
sections after it keep the investigation as it stood before: its retained
crashes, its matrices and its controls.

## Cause

**The runtime defect.** In Tarantool 3.9.0's LuaJIT (`8af934d9`), a trace
recorded while a function's local is an open upvalue (the function still
running) stores into it through `UREFO`, with no `OBAR`: an open upvalue
lives on the stack, which the collector rescans. Nothing in the trace
checks that the upvalue is still open, so once the function has returned,
the trace stores into the closed upvalue the same way, with no barrier.
A table stored after the collector has marked the closed upvalue, and
before its atomic phase, is never marked: the collector frees it while the
upvalue still refers to it. Upstream LuaJIT's
[07b3cd3c](https://github.com/LuaJIT/LuaJIT/commit/07b3cd3cf9b57a3801a1ebc48144767e31671f21)
guards such traces; this runtime doesn't have it.

[`upvalue-barrier.lua`](../tests/evidence/native-gc-crash/upvalue-barrier.lua)
reproduces it with Tarantool `3.9.0-0-g8b9a0f5` on Linux x86-64:
- `make()` declares `mark` and calls `set()`, which stores a new table into
  it, until `set()` is traced;
- each trial calls `set()` once at a given step of a GC cycle, then finishes
  the cycle;
- a weak table tells whether the collector freed the table `mark` holds.

| `set()` traced | The collector freed the live table |
|---|---:|
| while `mark` is open, as the engine's constructor did | 4,783 of 6,125 stores |
| after `make()` returned | 0 of 5,657 |
| while open, with `mark` a field of a table, as the fix holds it | 0 of 5,787 |

**Where Tarn hit it.** `lua/tarn_engine.lua`'s constructor called
`invalidate_tickets()` before returning, and `invalidate_tickets` stored the
tickets' cut, a fresh table (`c.mark()`), into the constructor's local
`mark`.
- If a hotness counter fired during that call, LuaJIT recorded
  `invalidate_tickets` with `mark` open. Functions share these counters,
  so that can happen on a single call.
- The trace then ran on every later invalidation: `M.init`, each
  `box.status` change, each election change, and each engine copy's cut.
- A jit.attach handler, preloaded into every tarantool process
  (`tarantool -l`), reported each compiled trace that stores a collectable
  value through `UREFO`. It ran over 12 starts of an RF3 shard, with the
  JIT's hotness threshold lowered to 1, 2 and 3 so that code run only a
  few times gets traced.
- In 3 of the 36 storage processes, it found
  `invalidate_tickets`'s store into `mark` (`tarn_engine.lua:38`, upvalue
  1). It found nothing else in any process.

**The retained core shows it** (run
[41524455322](https://github.com/cstheorist/tarn/actions/runs/41524455322),
native vinyl RF3; analysis in #160's comments):
- the crashing process held trace 165 of `invalidate_tickets`: the
  function loaded from the frame, `UREFO` of upvalue 1, a fresh table
  stored through it, no `OBAR`;
- `mark` was closed and black, and it held a pointer into the middle of a
  192-byte node array of another table, a current allocation;
- the collector crashed while traversing that pointer as an object
  (`gc_traverse_trace`).

That is the defect's outcome: the cut freed while referenced, its memory
reused, and the next marking of `mark` reading the reuse. The native
module plays no part in it, which fits the Lua-only memtx crash too.

**The fix.** The engine holds the cut in a table's field (`cut_mark.v`).
A store into a table's field always takes the table's barrier, in a trace
as in the interpreter. `lua/test/jit_upvalue_test.lua` traces eagerly
(hotloop 1) and inspects every trace compiled:
- a control function that stores a table into a local of the function
  calling it is found;
- the engine's constructor gets `invalidate_tickets` traced, and stores
  nothing through `UREFO`;
- loading the storage module stores nothing through `UREFO` either.

The test failed 15 of 15 runs with the old engine, and passed 30 of 30
with the fix. Over the same 12 starts, the startup audit found nothing in
the fixed code. Over the main storage tests (`lua/test/run.sh` with
`storage_test.lua` and the others it runs by default, at the default
hotness threshold), it found two traces, each storing a table into `last`
from the callback that `tarn_bucket_rows` hands `M.move.copy_batch`. That
callback runs only while `tarn_bucket_rows` does, so `last` is open
whenever the trace runs: it lives on the stack, which the collector
rescans, and needs no barrier.

**Not established.**
- The earlier crashes' cores are not available: the original
  `40753059987`, `41070288089`, the Lua-only `41524455126` and main
  `b621bb2` (#221). They crashed in the same collector routines, which is
  consistent with traversing a freed or reused object, but this cause is
  shown only for the core above.
- Only this store pattern was audited. Other defects of this runtime's JIT
  are not excluded.

## Retained observation

[Run 40753059987, job 123044577438](https://github.com/cstheorist/tarn/actions/runs/40753059987/job/123044577438)
ran source `420bde9fa9393c847cf022d4230b233c49b01fa7`, Rust 1.98.1 release
artifacts and Tarantool `3.9.0-0-g8b9a0f5` on Ubuntu 24.04 x86-64. During
RF3 Vinyl failover, shard `s1.2` became writable and loaded the native
module at 19:37:35 UTC. It stopped without an intentional kill marker.
The log reports `SEGV_MAPERR`, address `0x2f`, with
`gc_traverse_trace.part.0+330`, `propagatemark`, `lj_gc_step`, `lj_BC_TNEW`
and `execute_lua_call` in the stack. The fixture deliberately killed
`s1.3`, a different replica, afterward. All three workload finalizations
and the direct native audit failed without a reachable shard-1 leader.
They do not establish recovered data loss or exact effects after that crash.

The [original excerpt and provenance](../tests/evidence/native-gc-crash/)
retain the registers, process/event context and log SHA-256. The original
run has no uploaded artifacts; its runtime/gateway/native binaries and
core are unavailable. Rebuilt source-equivalent artifacts get new hashes;
they must not be described as those original binaries.

The crashing source used native ABI 11 and did **not** contain the later
stored freshness guard callbacks (ABI 13) or public Lua API guard bridge
(ABI 14). Current main contains the ABI 14 bridge. A defect found in that
later path would not by itself explain the earlier crash. A collector
stack names the fault site; it does not establish which component damaged
the trace or whether native integration is responsible.

## Reproduction matrix

The `Native GC failover investigation` workflow builds the original source
and the current PR merge source with their unchanged release profiles,
Rust 1.98.1 and locked dependencies. Each artifact is bound to every
compiled Rust/C input, Cargo manifests/lock, compiler version and binary
SHA-256. The runner refuses a mismatched source or altered artifact.

Three fresh 45-second RF3 trials run in each row:

| Source | Tables | Procedures | JIT | Purpose |
|---|---|---|---|---|
| Original `420bde9` | Vinyl | Native enabled | On | Original source/workload reproduction |
| Original `420bde9` | Vinyl | Lua only | On | Native integration control |
| Original `420bde9` | Vinyl | Native enabled | Off | JIT dependency control |
| Current merge | Vinyl | Native enabled | On | Current behavior, separately identified |
| Current merge | Memtx | Native enabled | On | Engine control |

The historical/current fixture and production Lua remain unchanged. They
retain their own behavior and defaults, including WAL `write`; this is
not a matched performance or fsync-durability study. The JIT-off row uses
a test-only executable wrapper and is not a production recommendation.
The runtime receipt records Tarantool's binary hash, architecture, version,
JIT flags and `ffi.abi('gc64')`; these were missing from the original log.

Each trial runs the existing bank, READ COMMITTED bank and moving-bucket
workloads with their ledger, acknowledged/aborted/unknown-effect and
counter bounds, leader changes, exact final native totals and lock-copy
checks. The collector additionally refuses a success if crash evidence is
present, the deadline expires, or the final native-mode reports disagree.
An unsuccessful trial remains unsuccessful even if a later one passes.

Both successful and failed process/event/workload logs and their hashes
are uploaded. Failed trials retain an unchanged storage/WAL archive. The
isolated CI runner enables cores, retains the exact Tarantool binary, and
compresses any resulting cores for inspection. An empty core archive
does not imply that no process failed: the logs and exit status remain
independent evidence. Artifacts expire after 30 days; retain a selected
failure package before that horizon if it is needed for further work.

No production, locking, commit, recovery, schema, native ABI or verified
executable code changes in this investigation. Existing TLA+/Verus
contracts do not prove LuaJIT heap integrity. Full ordinary CI still runs
separately; diagnostic results must be cited with their own source/run.

## First retained matrix

[Run 40873456226](https://github.com/cstheorist/tarn/actions/runs/40873456226)
completed all 15 trials. The four native rows passed all three trials each.
The three Lua-only workloads also exited successfully with their bank,
ledger, acknowledged/unknown-effect counter bounds and direct native tuple
checks intact. No trial logged a crash signature or produced a core.
The current-source rows used merge commit
`41bcc14b4926efdc0e666d9a06b68e67fc5317b9`, not a moving main reference.

The workflow is retained as **failed**: its collector incorrectly required
JSON `null` for a disabled native module, whereas `tarn_info().native`
reports JSON `false`. Both leaders reported `false` in every Lua trial.
The collector now requires exactly two explicit `false` reports; missing,
null, numeric zero, malformed and mixed native/Lua reports fail its regression
controls. The original manifests have not been rewritten.

[The retained result summary](../tests/evidence/native-gc-crash/run-40873456226.json)
records source, binary/runtime and original-manifest hashes, original trial
results and core inventory. Full binaries, logs and failed-trial archives are
in the run artifacts. Ordinary format/clippy, Tests, TLA+ and Verus jobs
[passed separately](https://github.com/cstheorist/tarn/actions/runs/40873456428).
These bounded negative results do not explain the original crash or establish
its recovered effects; #160 remains open.

The later [five-lane matrix 41101928521](https://github.com/cstheorist/tarn/actions/runs/41101928521)
also completed all fifteen 45-second failover trials: the original observed
source with Vinyl/native/JIT on, Vinyl/Lua/JIT on and Vinyl/native/JIT off,
plus source `952623f` with memtx/native/JIT on and Vinyl/native/JIT on.
[Its independent audit](../tests/evidence/native-gc-crash/matrix-41101928521.json)
reconciles actual ELF hashes, all recorded compiled and fixture Git blobs,
every retained trial-log hash, runtime/JIT/native mode, and the original
ledger and acknowledgement checks. Every lane has an empty real-core
inventory and no observed crash signature; READ COMMITTED reports zero
unexpected errors. Unknown outcomes and the serializable bank's original
error counts remain in the receipt. Full logs and binaries remain local.
These short, source-bound negative results establish neither a crash fix
nor a performance comparison or qualification of newer main.

## Second retained observation and ordinary CI capture

[Run 41070288089, job 124112263428](https://github.com/cstheorist/tarn/actions/runs/41070288089/job/124112263428)
failed during native-enabled RF3 Vinyl failover on merge source
`a1eb5dd890e8ac363c097ed60852e639ef330366`. Replica `s2.1` (PID 41966)
stopped without an intentional kill marker. The log reports `SEGV_MAPERR`
at `0x401c64cc`, in `gc_traverse_trace.part.0+199`, followed by
`propagatemark`, `lj_gc_step`, `gc_step_jit_cp`, `lj_vm_cpcall`, and
`lj_gc_step_jit`. The fixture subsequently killed `s2.2`, a different
replica. Its failed workload finalizations do not establish exact recovered
effects or data loss.

[The retained package](../tests/evidence/native-gc-crash/run-41070288089/)
contains the selected crash excerpt, same-job binary hash receipts with
runner paths removed, and disassembly. The full decoded CI log and original
native-freshness artifact are retained locally, excluded from the published
package; their original hashes and artifact identity remain recorded.
The same job recorded native SHA-256
`1ac18f31dc0b2a9ccee9efcf601c99d8d99a520437fcb63a3cd3ec79771cd0ae`
and Tarantool SHA-256
`bb5e7c24d89e9b6ab86a03e78d001afc2d9c19ac163d9867791e392c35d17d6f`.
Both match actual binaries retained from benchmark run `41070288021`.
These are pre-crash hash receipts, not crash-time mapping evidence; this
CI run again uploaded no core or failed storage/WAL archive. They are not
the missing binaries from the original `40753059987` crash.

In that matching Tarantool ELF, offset 199 is address `0x27e407`, instruction
`movl (%rdx,%rax,4), %eax`. The logged registers yield
`0x401af4b0 + 4 * 0x5c07 = 0x401c64cc`, the reported fault address.
This identifies a trace-table lookup as the fault site; it does not establish
why the trace reference or table was invalid. The two collector crashes
have different offsets and call paths. A shared cause remains unproven.

Ordinary CI now snapshots the actual gateway, native and Tarantool binaries,
source identities and runtime/compiler versions immediately before its two
failover stages. Those stages enable core dumps on their isolated runner.
An always-run collector stops only their owned fixtures, retains remaining
logs/storage/WAL and core files, and checks the binaries against the initial
snapshot. An empty core inventory is explicit. A changed binary or incomplete
collection fails the collector while preserving available evidence. Its
`complete` flag describes artifact collection only; `job_status` independently
retains the test result. Original failover assertions and mutation behavior
remain unchanged. Artifacts still expire after 30 days. A run whose job
passed with an empty core inventory keeps the receipts, whose hashes identify
the binaries, but not the copies of the binaries themselves.

The first ordinary-CI capture [passed in run 41093908604](https://github.com/cstheorist/tarn/actions/runs/41093908604).
[Its audited receipt](../tests/evidence/native-gc-crash/ci-capture-41093908604.json)
binds merge source `1829a54df66ec3d20467c3e94f82e9fadd9e3e9f` to all 363
recorded tracked source files and three actual binaries. Their post-run
identities match. The only working-tree status entry is generated, untracked
`integrity-evidence/`. Both successful failover fixtures were removed by their
existing cleanup, and the actual core inventory is empty. This verifies the
collector's successful-run path, not kernel core generation or a crash fix.

An additional isolated control now verifies core generation before failover:
an empty Tarantool child with an empty environment intentionally raises
SIGSEGV, terminates through the pinned crash handler's SIGABRT, and must
produce exactly one ELF core. Its directory and minimal identity/header
receipt are separate from actual failover cores. Only the probe receipt and
expected crash log are uploaded; the synthetic core is excluded. A
[local Linux ARM64 control](../tests/evidence/native-gc-crash/core-probe-arm64-20261030.json)
produces a 25,468,928-byte ELF core. The x86-64 control passed in ordinary
CI [run 41220996772](https://github.com/cstheorist/tarn/actions/runs/41220996772/job/124595326098),
before both failover stages, which passed too. Unexpected READ COMMITTED
errors also retain their statement, timestamp and ledger token for
investigation under #215; their classification and the failover assertions
remain unchanged.
