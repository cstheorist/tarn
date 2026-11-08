#!/bin/bash
# Build the bundle a benchmark run deploys: everything the nodes run that is
# not downloaded from its vendor at run time (TiDB and YugabyteDB are, by the
# control node, at pinned versions; see versions.env).
#
#   bench/cloud/build-bundle.sh OUTDIR
#
# Builds, on Ubuntu 24.04 x86_64 (the VMs' image), from pinned sources:
#   Tarantool  (versions.env TARANTOOL_VERSION; as ci/install-tarantool.sh)
#   tarn-server, libtarn_storage.so (this checkout, cargo --release)
#   sysbench   (stock, MySQL + PostgreSQL drivers) and YugabyteDB's fork
# and adds the runtime .debs sysbench needs, Tarn's Lua and scripts, and
# bench/cloud itself. Writes OUTDIR/tarn-bench-bundle-<tarn sha>.tar.gz,
# its .sha256 and a MANIFEST inside with every version and checksum.
#
# Environment: SRC (work directory, default ~/src/tarn-bench-build),
#   TARANTOOL_BIN / TARN_RELEASE_DIR to reuse builds already made,
#   CARGO_FLAGS (extra flags for cargo build).
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
OUT=$(mkdir -p "${1:?usage: build-bundle.sh OUTDIR}" && cd "$1" && pwd)
. "$ROOT/bench/cloud/versions.env"
SRC=${SRC:-$HOME/src/tarn-bench-build}
PREFIX=/data/tb            # where the bundle lives on every node
mkdir -p "$SRC"
log() { echo "[$(date +%H:%M:%S)] $*"; }

SHA=$(git -C "$ROOT" rev-parse --short=12 HEAD)
DIRTY=$(git -C "$ROOT" status --porcelain -- crates lua scripts | head -1)
STAGE=$SRC/stage/tarn-bench-bundle-$SHA
rm -rf "$STAGE"; mkdir -p "$STAGE"/{bin,lib,debs,opt}

# --- Tarantool ---------------------------------------------------------------
if [ -n "${TARANTOOL_BIN:-}" ]; then
  install -m 0755 "$TARANTOOL_BIN" "$STAGE/bin/tarantool"
else
  log "Tarantool $TARANTOOL_VERSION"
  if [ ! -x "$SRC/tarantool-build/src/tarantool" ]; then
    rm -rf "$SRC/tarantool"
    git clone -q --depth 1 --branch "$TARANTOOL_VERSION" --recurse-submodules --shallow-submodules \
      https://github.com/tarantool/tarantool.git "$SRC/tarantool"
    # LuaJIT takes its version from its tags, which a shallow clone lacks.
    git -C "$SRC/tarantool/third_party/luajit" fetch -q --unshallow --tags
    cmake -S "$SRC/tarantool" -B "$SRC/tarantool-build" -DCMAKE_BUILD_TYPE=RelWithDebInfo \
      -DENABLE_BUNDLED_MIMALLOC=OFF -DMIMALLOC_OVERRIDE=none > "$SRC/tarantool-configure.log"
    cmake --build "$SRC/tarantool-build" --parallel "$(nproc)" --target tarantool > "$SRC/tarantool-build.log"
  fi
  install -m 0755 "$SRC/tarantool-build/src/tarantool" "$STAGE/bin/tarantool"
fi
"$STAGE/bin/tarantool" --version | head -1 | grep -q "Tarantool $TARANTOOL_VERSION-" ||
  { echo "wrong Tarantool: $("$STAGE/bin/tarantool" --version | head -1)"; exit 1; }

# --- Tarn --------------------------------------------------------------------
REL=${TARN_RELEASE_DIR:-}
if [ -z "$REL" ]; then
  log "tarn-server $SHA"
  # shellcheck disable=SC2086
  (cd "$ROOT" && cargo build --release -p tarn-server -p tarn-storage ${CARGO_FLAGS:-})
  REL=$ROOT/target/release
fi
install -m 0755 "$REL/tarn-server" "$STAGE/bin/tarn-server"
install -m 0755 "$REL/libtarn_storage.so" "$STAGE/lib/libtarn_storage.so"
RUSTC=$(cd "$ROOT" && ${RUSTUP_TOOLCHAIN:+env RUSTUP_TOOLCHAIN=$RUSTUP_TOOLCHAIN} rustc --version)
# The repository subset the nodes use (the storage entry point loads its
# siblings from lua/; scripts/ has the probes; bench/ the drivers).
mkdir -p "$STAGE/tarn"
(cd "$ROOT" && git ls-files -z lua scripts bench/sysbench.sh bench/cloud | xargs -0 tar -cf - ) | tar -xf - -C "$STAGE/tarn"
# The working copy of bench/cloud may be ahead of HEAD while it is developed.
cp -a "$ROOT/bench/cloud/." "$STAGE/tarn/bench/cloud/"

# --- sysbench ----------------------------------------------------------------
build_sysbench() {  # name repo ref
  local name=$1 repo=$2 ref=$3 dir=$SRC/$1
  log "sysbench: $name ($ref)"
  if [ ! -d "$dir/.git" ]; then git clone -q "$repo" "$dir"; fi
  git -C "$dir" fetch -q --tags origin
  git -C "$dir" checkout -q --force "$ref"
  git -C "$dir" clean -qfdx
  local patch
  for patch in "$ROOT"/bench/cloud/patches/"$name"-*.patch; do
    [ -f "$patch" ] || continue
    git -C "$dir" apply "$patch"
    echo "$name: patched with $(basename "$patch")" >> "$STAGE/MANIFEST.tmp"
  done
  (cd "$dir" && ./autogen.sh > /dev/null 2>&1 &&
    ./configure --prefix="$PREFIX/opt/$name" --with-mysql --with-pgsql > "$SRC/$name-configure.log" 2>&1 &&
    make -j"$(nproc)" > "$SRC/$name-build.log" 2>&1 &&
    rm -rf "$SRC/install-$name" && make install DESTDIR="$SRC/install-$name" > /dev/null 2>&1)
  cp -a "$SRC/install-$name$PREFIX/opt/$name" "$STAGE/opt/"
  echo "$name: $ref $(git -C "$dir" rev-parse --short=12 HEAD)" >> "$STAGE/MANIFEST.tmp"
}
: > "$STAGE/MANIFEST.tmp"
build_sysbench sysbench https://github.com/akopytov/sysbench.git "$SYSBENCH_REF"
build_sysbench sysbench-yb https://github.com/yugabyte/sysbench.git "$SYSBENCH_YB_REF"

# Runtime libraries sysbench links against, as .debs from the Ubuntu archive
# (worker VMs have no internet access; dpkg -i installs them).
log "runtime .debs"
(cd "$STAGE/debs" && apt-get download -q $RUNTIME_DEBS > /dev/null)
for b in "$STAGE"/opt/*/bin/sysbench; do
  ldd "$b" | grep -q 'not found' && { echo "unresolved libraries in $b"; ldd "$b"; exit 1; }
done

# --- manifest and archive ------------------------------------------------------
{
  echo "tarn-bench bundle $SHA${DIRTY:+ (working copy with uncommitted changes)} built $(date -u +%FT%TZ)"
  echo "built on: $(. /etc/os-release && echo "$PRETTY_NAME") $(uname -m)"
  echo "tarantool: $("$STAGE/bin/tarantool" --version | head -1)"
  echo "tarn: $(git -C "$ROOT" log -1 --format='%H %cs %s')"
  echo "rustc: $RUSTC"
  cat "$STAGE/MANIFEST.tmp"
  echo "debs: $(cd "$STAGE/debs" && ls | tr '\n' ' ')"
  echo "pinned downloads (control node, run time):"
  sed -n 's/^\([A-Z_]*\)=\(.*\)$/  \1=\2/p' "$ROOT/bench/cloud/versions.env"
  echo "sha256:"
  (cd "$STAGE" && find bin lib debs -type f -print0 | sort -z | xargs -0 sha256sum | sed 's/^/  /')
} > "$STAGE/MANIFEST"
rm "$STAGE/MANIFEST.tmp"
NAME=tarn-bench-bundle-$SHA
tar -C "$SRC/stage" -czf "$OUT/$NAME.tar.gz" "$NAME"
(cd "$OUT" && sha256sum "$NAME.tar.gz" > "$NAME.tar.gz.sha256")
log "$OUT/$NAME.tar.gz ($(du -h "$OUT/$NAME.tar.gz" | cut -f1))"
cat "$STAGE/MANIFEST" | head -12
