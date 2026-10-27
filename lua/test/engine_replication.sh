#!/bin/bash
# Fresh RF3 processes: leader loss during copy and after PREPARE, per engine.
# Linux /proc identity checks deliberately refuse to signal other processes.
set -eu
umask 077
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
SOURCE=${ENGINE_SOURCE_DIR:-$DIR}
WORK=${1:?new private work directory required}
BASE=${ENGINE_REPLICATION_BASE:-3750}
# Match storage.lua's default selection, then pin the expected mode. An
# explicitly requested native module may never silently pass as Lua.
NATIVE=${TARN_NATIVE:-}
if [ -z "$NATIVE" ]; then
  NATIVE="$SOURCE/../target/release/libtarn_storage.so"
  [ -f "$NATIVE" ] || NATIVE=0
fi
export TARN_NATIVE="$NATIVE"
MODE=native
[ "$NATIVE" != 0 ] || MODE=lua
[ -d /proc/self ] || { echo 'FAIL engine RF3 requires Linux process identity checks'; exit 1; }
mkdir "$WORK"
printf 'engine-replication-rf3\n' > "$WORK/owner"
printf 'source=%s\nnative=%s\nexpected_mode=%s\n' "$SOURCE" "$NATIVE" "$MODE" > "$WORK/manifest.txt"
for f in "$SOURCE/storage.lua" "$SOURCE/tarn.lua" "$SOURCE/tarn_engine.lua" "$SOURCE/tarn_lazy_meter.lua" \
         "$DIR/test/engine_replication_test.lua" "$DIR/test/engine_replication.sh"; do
  sha256sum "$f" >> "$WORK/manifest.txt"
done
if [ "$NATIVE" != 0 ]; then sha256sum "$NATIVE" >> "$WORK/manifest.txt"; fi
declare -a PIDS STARTS
RUNTIME=''
owned() {
  local i=$1 p=${PIDS[$1]:-}
  [ -n "$p" ] && kill -0 "$p" 2>/dev/null || return 1
  [ -r "/proc/$p/stat" ] || return 1
  # Tarantool's process-title handling relocates environ. Kernel parent and
  # start-time identity survives it, and also works before box.cfg chdir.
  local identity
  identity=$(awk '{print $4 ":" $22}' "/proc/$p/stat")
  [ "$identity" = "$$:${STARTS[$i]:-missing}" ]
}
cleanup() {
  local i p
  for i in 1 2 3; do
    if owned "$i"; then kill -TERM "${PIDS[$i]}" 2>/dev/null || true; fi
  done
  for _ in $(seq 30); do
    local live=0
    for i in 1 2 3; do owned "$i" && live=1 || true; done
    [ "$live" = 0 ] && break
    sleep .1
  done
  for i in 1 2 3; do
    p=${PIDS[$i]:-}
    if owned "$i"; then kill -KILL "$p" 2>/dev/null || true; fi
    if [ -n "$p" ]; then
      if ! kill -0 "$p" 2>/dev/null || owned "$i"; then wait "$p" 2>/dev/null || true
      else echo "FAIL cleanup refused changed process identity for node$i pid$p" >&2; fi
    fi
    unset 'PIDS[i]'
    unset 'STARTS[i]'
  done
}
trap cleanup EXIT
start() {
  local i=$1 port=$((BASE + $1))
  TARN_SHARD_ID=1 TARN_LISTEN="127.0.0.1:$port" TARN_WORKDIR="$RUNTIME/s$i" \
  TARN_REPLICAS="$REPLICAS" TARN_PEERS="$PEERS" TARN_WAL_MODE=fsync \
  TARN_MEMORY_MB=96 TARN_VINYL_MEMORY_MB=16 TARN_VINYL_CACHE_MB=0 \
  TARN_VINYL_READ_THREADS=1 TARN_HOST_INSTANCES=3 TARN_LOG_LEVEL=4 \
  TARN_CHECK_YIELDS=1 TARN_TEST_HOOKS=1 TARN_CHECK_LOCKS=1 \
    "$T" "$SOURCE/storage.lua" >> "$RUNTIME/s$i.log" 2>&1 &
  PIDS[$i]=$!
  STARTS[$i]=$(awk '{print $22}' "/proc/${PIDS[$i]}/stat")
  printf '%s\n' "${PIDS[$i]}" > "$RUNTIME/s$i.pid"
  printf '%s:%s\n' "$$" "${STARTS[$i]}" > "$RUNTIME/s$i.process-identity"
}
run() {
  ENGINE_RF3_WORK="$RUNTIME" ENGINE_RF3_BASE="$BASE" ENGINE_RF3_ENGINE="$engine" \
  ENGINE_RF3_EXPECT_MODE="$MODE" \
    timeout 90 "$T" "$DIR/test/engine_replication_test.lua" "$1"
}
for engine in memtx vinyl; do
  RUNTIME="$WORK/$engine"
  mkdir "$RUNTIME"
  for i in 1 2 3; do
    port=$((BASE + i))
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
      echo "FAIL engine RF3 port $port already in use; no services changed"; exit 1
    fi
  done
  REPLICAS="[\"tarn:tarn@127.0.0.1:$((BASE+1))\",\"tarn:tarn@127.0.0.1:$((BASE+2))\",\"tarn:tarn@127.0.0.1:$((BASE+3))\"]"
  PEERS="{\"1\":$REPLICAS}"
  for i in 1 2 3; do start "$i"; done
  run setup
  run kill-first
  first=$(cat "$RUNTIME/killed-first")
  set +e; wait "${PIDS[$first]}"; rc=$?; set -e
  [ "$rc" = 137 ] || { echo "FAIL first owned leader exit $rc, expected137"; exit 1; }
  unset 'PIDS[first]'
  run after-first
  start "$first"
  run caught-up
  run kill-second
  second=$(cat "$RUNTIME/killed-second")
  set +e; wait "${PIDS[$second]}"; rc=$?; set -e
  [ "$rc" = 137 ] || { echo "FAIL second owned leader exit $rc, expected137"; exit 1; }
  unset 'PIDS[second]'
  run after-second
  cleanup
  printf 'cleaned all owned %s processes\n' "$engine" | tee -a "$WORK/manifest.txt"
done
echo 'engine RF3 both engines complete; only owned processes cleaned'
