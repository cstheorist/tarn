# Isolated GC runtime diagnostic for Tarn #160

This opt-in Linux x86-64 **non-GC64** A/B build is defensive functional
validation of a public upstream fix. It does not establish that the mechanism
caused Tarn #160, select a production runtime, or change the common installer.
No benchmark or deployment claim follows from a passing fixture.

Run on a disposable Linux x86-64 host with the common installer's dependencies
already installed: build-essential, cmake, autoconf, automake, libtool, git,
zlib1g-dev, libreadline-dev, libncurses-dev, libssl-dev, libicu-dev,
libunwind-dev, plus Python 3. The builder does not install system packages.

```sh
ci/install-gc-runtime.sh baseline /absolute/fresh/baseline
ci/install-gc-runtime.sh candidate /absolute/fresh/candidate
```

Both arms build Tarantool 3.9.0 at
`8b9a0f59a95bb6c4a491e716966d9df9d5a4f6bc` and its exact LuaJIT submodule
`8af934d93cf26fb1e9b08b28a8b51833d55fc8e8`. A moved release tag or submodule
mismatch fails. Recursive submodules use the pinned superproject gitlinks.
LuaJIT alone is then fetched with full history and tags because its
`SetVersion.cmake` requires plain `git describe`. The fetch never changes
the pinned HEAD or worktree; the builder checks the pin again and validates
the describe string before configuration. Both describe output and shallow
status before/after the fetch are retained as inputs.
The baseline must start pristine; the candidate's complete LuaJIT dirty diff
must match the reviewed patch hash. Each arm uses fresh source and build
folders. Existing outputs are never reused. OUTDIR must be nonexistent or an
empty directory owned by the caller. The only installed binary is
`OUTDIR/bin/tarantool`; all builder writes are inside OUTDIR.

Configure options match `ci/install-tarantool.sh`: RelWithDebInfo,
`ENABLE_BUNDLED_MIMALLOC=OFF`, and `MIMALLOC_OVERRIDE=none`. Both diagnostic
arms additionally hold `LUAJIT_ENABLE_GC64=OFF`. This controls Tarantool's
allocator choice; it does not replace LuaJIT's normal non-GC64 allocator.
`GC_RUNTIME_JOBS` may be 1 or 2 (default 2). Use the same host, compiler and
flags for both arms. Each binary must identify Linux/x64 and pass
`not require('ffi').abi('gc64')` before a success receipt is written.

## Candidate provenance and scope

Mike Pall authored the original fix, “Check for upvalue state transition in
IR_UREFO,” on 2023-11-05, thanking Peter Cawley:

- [upstream commit 07b3cd3cf9b57a3801a1ebc48144767e31671f21](https://github.com/LuaJIT/LuaJIT/commit/07b3cd3cf9b57a3801a1ebc48144767e31671f21)
- [upstream issue #1085](https://github.com/LuaJIT/LuaJIT/issues/1085)
- [pinned Tarantool release](https://github.com/tarantool/tarantool/commit/8b9a0f59a95bb6c4a491e716966d9df9d5a4f6bc)
- [pinned LuaJIT source](https://github.com/tarantool/luajit/commit/8af934d93cf26fb1e9b08b28a8b51833d55fc8e8)

`upstream-07b3cd3c.patch` preserves the exact upstream commit patch and
attribution (SHA-256
`014032ee58a77c1d89716aa4e743bb099ee0a5a8c63fde673fdf7e3002a0c4bf`).
`urefo-x86.patch` is the explicit port against the pinned Tarantool LuaJIT:

- `lj_asm_x86.h`: emit state guards for open and closed references, including
  constant function references where a state check is required.
- `lj_record.c`: retain the SSA alias case, guard GC-value stores recorded
  against open upvalues, distinguish unguarded integer references and stable
  closed constant references.
- `lj_opt_mem.c`: preserve alias soundness by promoting needed open checks.
- `lj_opt_fold.c`: propagate checks through CSE and prevent guarded open
  references from merging across GC steps.
- `lj_state.c`: abort recording when freeing a coroutine closes its upvalues.

The port changes context only where the fork differs: it preserves the
immutable-upvalue `noconstify` path, retains `lj_ir_kint` for the scoped
non-GC64 target, and keeps the existing FFI `ctype_ctsG` dangling-pointer
protection before closing upvalues. No ARM, ARM64, MIPS, PPC, GC64 or other
platform qualification is claimed. This is the essential five-file port,
not only a runtime openness guard.

## Output contract and evidence

On success, `OUTDIR/runtime-receipt.json` has `schema_version: 1` and stable
keys `arm`, `tarantool_commit`, `luajit_commit`, `patch_sha256` (null for
baseline), `binary_sha256`, `build_type`, `source_dirty_diff_sha256`,
`architecture`, `os`, `gc64`, `compiler`, `config`, `submodules`,
`luajit_describe`, and `luajit_shallow`.
`source_dirty_diff_sha256` hashes `inputs/source-dirty.diff`, including the
empty baseline diff. `patch_sha256` hashes `inputs/urefo-x86.patch` for the
candidate. The receipt records toolchain/config inputs, recursive submodule
identities and SHA-256 hashes of compact evidence files. It is written only
after build and runtime identity checks succeed.

Archive `bin/`, `runtime-receipt.json`, `inputs/`, `logs/`, and
`CMakeCache.txt`. These contain exact patches, builder/helper copies,
source/submodule manifests, toolchain versions, the pristine status,
configure/build/version/probe logs, cache and binary hash. Do not upload
`work/`, which retains the full fresh sources and build tree locally for
inspection. Failures retain their logs; a missing success receipt means the
runtime was not qualified. Separate functional fixture observations are
required, including an independent actual non-GC64 assertion.

Local verification on macOS is limited to shell/Python syntax and applying
and reversing the patch on fresh pinned sources. Linux compilation and
functional A/B results must come from the diagnostic run itself.
