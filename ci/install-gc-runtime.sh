#!/usr/bin/env bash
# Isolated diagnostic runtime, not the default Tarantool installer.
# ci/install-gc-runtime.sh baseline|candidate OUTDIR
set -euo pipefail
if [[ $# != 2 || ( $1 != baseline && $1 != candidate ) ]]; then
  echo 'usage: ci/install-gc-runtime.sh baseline|candidate OUTDIR' >&2
  exit 2
fi
if [[ $(uname -s) != Linux || $(uname -m) != x86_64 ]]; then
  echo 'GC runtime diagnostic is qualified only for Linux x86_64 non-GC64' >&2
  exit 2
fi
ARM=$1
JOBS=${GC_RUNTIME_JOBS:-2}
if [[ ! $JOBS =~ ^[12]$ ]]; then
  echo 'GC_RUNTIME_JOBS must be 1 or 2 (default 2)' >&2
  exit 2
fi
for tool in git cmake python3 cc c++ install sha256sum; do
  command -v "$tool" >/dev/null || { echo "missing build tool: $tool" >&2; exit 2; }
done
HERE=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
OUT=$(python3 - "$2" <<'PY'
import os, pathlib, sys
p = pathlib.Path(sys.argv[1]).resolve()
if p.exists():
    if not p.is_dir() or p.stat().st_uid != os.getuid() or any(p.iterdir()):
        sys.exit('OUTDIR must be nonexistent or an empty caller-owned directory')
else:
    p.mkdir(parents=True)
if p.stat().st_uid != os.getuid():
    sys.exit('OUTDIR must belong to the caller')
print(p)
PY
)
TT_COMMIT=8b9a0f59a95bb6c4a491e716966d9df9d5a4f6bc
LJ_COMMIT=8af934d93cf26fb1e9b08b28a8b51833d55fc8e8
PATCH_SHA=f6751b7fa768b590ff76cf79ac31ada461d776c763a85337ff2069beab40bce0
UPSTREAM_SHA=014032ee58a77c1d89716aa4e743bb099ee0a5a8c63fde673fdf7e3002a0c4bf
mkdir -p "$OUT/bin" "$OUT/inputs" "$OUT/logs" "$OUT/work"
cp "$HERE/gc-runtime/urefo-x86.patch" "$HERE/gc-runtime/upstream-07b3cd3c.patch" \
  "$HERE/gc-runtime/README.md" "$HERE/gc-runtime/receipt.py" "$OUT/inputs/"
cp "$HERE/install-gc-runtime.sh" "$OUT/inputs/"
printf '%s  %s\n%s  %s\n' "$PATCH_SHA" "$OUT/inputs/urefo-x86.patch" \
  "$UPSTREAM_SHA" "$OUT/inputs/upstream-07b3cd3c.patch" | sha256sum --check
run() {
  local step=$1; shift
  echo "::group::gc-runtime $ARM: $step"
  if ! "$@" >"$OUT/logs/$step.log" 2>&1; then
    tail -n 60 "$OUT/logs/$step.log" >&2
    echo '::endgroup::'
    exit 1
  fi
  tail -n 5 "$OUT/logs/$step.log"
  echo '::endgroup::'
}
SRC=$OUT/work/tarantool
BUILD=$OUT/work/build
# The release name supplies git-describe metadata; full identities are mandatory.
run clone git clone --quiet --depth 1 --branch 3.9.0 --no-recurse-submodules \
  https://github.com/tarantool/tarantool.git "$SRC"
[[ $(git -C "$SRC" rev-parse HEAD) == "$TT_COMMIT" ]] || { echo 'Tarantool pin mismatch' >&2; exit 1; }
run submodules git -C "$SRC" submodule update --init --recursive --depth 1 --jobs "$JOBS"
LJ=$SRC/third_party/luajit
[[ $(git -C "$LJ" rev-parse HEAD) == "$LJ_COMMIT" ]] || { echo 'LuaJIT pin mismatch' >&2; exit 1; }
# LuaJIT's SetVersion.cmake calls plain git describe, which needs annotated
# tags and their ancestry. Fetch metadata/history only; never move pinned HEAD.
git -C "$LJ" rev-parse --is-shallow-repository > "$OUT/inputs/luajit-shallow-before.txt"
if [[ $(cat "$OUT/inputs/luajit-shallow-before.txt") == true ]]; then
  run luajit-history git -C "$LJ" fetch --unshallow --tags origin
else
  run luajit-history git -C "$LJ" fetch --tags origin
fi
[[ $(git -C "$LJ" rev-parse HEAD) == "$LJ_COMMIT" ]] || { echo 'LuaJIT pin moved during history fetch' >&2; exit 1; }
git -C "$LJ" rev-parse --is-shallow-repository > "$OUT/inputs/luajit-shallow-after.txt"
run luajit-describe git -C "$LJ" describe
cp "$OUT/logs/luajit-describe.log" "$OUT/inputs/luajit-describe.txt"
if ! grep -Eq '^v[0-9]+\.[0-9]+\.[0-9]+(-(rc|beta)[0-9]+)?-[0-9]+-g[0-9a-z]+$' "$OUT/inputs/luajit-describe.txt"; then
  echo 'LuaJIT git describe is not compatible with SetVersion.cmake' >&2; exit 1
fi
git -C "$SRC" submodule status --recursive > "$OUT/inputs/submodules.txt"
if grep -Eq '^[-+U]' "$OUT/inputs/submodules.txt"; then
  echo 'uninitialized or mismatched submodule' >&2; exit 1
fi
# Include untracked files, and do not honor the release's ignore=dirty setting.
git -C "$SRC" status --porcelain --untracked-files=all --ignore-submodules=none > "$OUT/inputs/pristine-status.txt"
[[ ! -s $OUT/inputs/pristine-status.txt ]] || { echo 'source is not pristine' >&2; exit 1; }
git -C "$SRC" rev-parse HEAD > "$OUT/inputs/tarantool-commit.txt"
git -C "$LJ" rev-parse HEAD > "$OUT/inputs/luajit-commit.txt"
cp "$SRC/.gitmodules" "$OUT/inputs/tarantool.gitmodules"
git -C "$SRC" submodule foreach --quiet --recursive 'printf "%s %s\n" "$displaypath" "$(git remote get-url origin)"' \
  > "$OUT/inputs/submodule-urls.txt"
if [[ $ARM == candidate ]]; then
  run patch-check git -C "$LJ" apply --check "$OUT/inputs/urefo-x86.patch"
  run patch git -C "$LJ" apply "$OUT/inputs/urefo-x86.patch"
fi
git -C "$LJ" -c diff.algorithm=myers diff --full-index --binary --no-ext-diff --no-textconv > "$OUT/inputs/source-dirty.diff"
if [[ $ARM == baseline ]]; then
  [[ ! -s $OUT/inputs/source-dirty.diff ]] || { echo 'baseline source changed' >&2; exit 1; }
else
  [[ $(sha256sum "$OUT/inputs/source-dirty.diff" | cut -d' ' -f1) == "$PATCH_SHA" ]] || {
    echo 'candidate differs from reviewed patch' >&2; exit 1;
  }
fi
# Top-level tracked files and gitlinks must remain pinned in both arms.
[[ -z $(git -C "$SRC" diff --ignore-submodules=dirty) ]] || { echo 'unexpected Tarantool diff' >&2; exit 1; }
{
  uname -a
  git --version
  cmake --version
  "${CC:-cc}" --version
  "${CXX:-c++}" --version
} > "$OUT/inputs/toolchain.txt" 2>&1
# Match the common installer; explicitly hold the diagnostic at non-GC64.
run configure cmake -S "$SRC" -B "$BUILD" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DENABLE_BUNDLED_MIMALLOC=OFF -DMIMALLOC_OVERRIDE=none -DLUAJIT_ENABLE_GC64=OFF
cp "$BUILD/CMakeCache.txt" "$OUT/CMakeCache.txt"
run build cmake --build "$BUILD" --parallel "$JOBS" --target tarantool
run install install -D -m 0755 "$BUILD/src/tarantool" "$OUT/bin/tarantool"
run version "$OUT/bin/tarantool" --version
run runtime-probe "$OUT/bin/tarantool" -e \
  'local ffi=require("ffi"); assert(jit.os=="Linux"); assert(jit.arch=="x64"); assert(not ffi.abi("gc64")); print("Linux x64 non-GC64"); os.exit(0)'
python3 "$OUT/inputs/receipt.py" "$OUT" "$ARM" "$JOBS"
echo "diagnostic runtime: $OUT/bin/tarantool"
echo "receipt: $OUT/runtime-receipt.json"
