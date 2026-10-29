# Native GC crash investigation (#160)

The original crash remains unexplained. This investigation preserves its
evidence and runs independent historical/current, native/Lua and JIT
controls. A green repeat is not a fix or proof of absence.

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
