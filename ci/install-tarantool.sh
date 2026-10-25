#!/bin/bash
# Build Tarantool from source and install it under PREFIX, unless PREFIX
# already holds that version (CI caches PREFIX).
#
#   ci/install-tarantool.sh [VERSION] [PREFIX]     defaults: 3.9.0, ~/.local/tarantool
set -euo pipefail
VERSION=${1:-3.9.0}
PREFIX=${2:-$HOME/.local/tarantool}
if [ -x "$PREFIX/bin/tarantool" ] && "$PREFIX/bin/tarantool" --version | head -1 | grep -q "Tarantool $VERSION-"; then
  echo "using cached $("$PREFIX/bin/tarantool" --version | head -1)"
  exit 0
fi

# On GitHub Actions, report the tail of the output of a failed step as an
# annotation (job logs are not always at hand; annotations are).
LOG=$(mktemp)
annotate() {
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    printf '::error title=Tarantool build failed (%s)::%s\n' "$1" \
      "$(tail -n 40 "$LOG" | sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')"
  fi
}
run() {  # run STEP CMD...: log output, show it, annotate on failure
  local step=$1; shift
  echo "::group::$step"
  if ! "$@" > "$LOG" 2>&1; then
    cat "$LOG"; echo "::endgroup::"; annotate "$step"; exit 1
  fi
  tail -n 5 "$LOG"; echo "::endgroup::"
}

run "apt update" sudo apt-get update -qq
run "install build dependencies" sudo apt-get install -y -qq build-essential cmake autoconf automake libtool \
  git zlib1g-dev libreadline-dev libncurses-dev libssl-dev libicu-dev libunwind-dev
SRC=$(mktemp -d)
run "clone" git clone --quiet --depth 1 --branch "$VERSION" --recurse-submodules \
  https://github.com/tarantool/tarantool.git "$SRC/tarantool"
# The bundled mimalloc is downloaded at build time from a third-party
# server; the system allocator is fine for tests.
run "configure" cmake -S "$SRC/tarantool" -B "$SRC/build" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
  -DENABLE_BUNDLED_MIMALLOC=OFF -DMIMALLOC_OVERRIDE=none
run "build" cmake --build "$SRC/build" --parallel "$(nproc)" --target tarantool
# Tarn needs only the server binary: every module it uses is built in. (A
# full `cmake --install` also wants the EmmyLua stubs, which are generated
# only in ENABLE_DIST builds.)
run "install" install -D -m 0755 "$SRC/build/src/tarantool" "$PREFIX/bin/tarantool"
"$PREFIX/bin/tarantool" --version | head -2
