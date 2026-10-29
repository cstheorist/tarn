#!/bin/bash
# Real logical-shard withdrawal dependencies; only the processes started here
# are signalled. The caller keeps the work directory and its WAL on failure.
set -euo pipefail
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
mkdir -p "${1:?work directory required}"
WORK=$(cd "$1" && pwd)
BASE=${MEMBER_DEPENDENCY_BASE:-3870}
case "$BASE" in ''|*[!0-9]*) exit 2;; esac
URIS=("tarn:tarn@127.0.0.1:$((BASE + 1))" "tarn:tarn@127.0.0.1:$((BASE + 2))" "tarn:tarn@127.0.0.1:$((BASE + 3))")
PEERS=$(printf '{"1":["%s"],"2":["%s"],"3":["%s"]}' "${URIS[@]}")
PIDS=()
STOPPED=()
CONTROLLER_PID=$$
identity() {
  local proc_line proc_tail
  local -a fields
  [ -r "/proc/$1/stat" ] || return 1
  read -r proc_line < "/proc/$1/stat"
  proc_tail=${proc_line##*) }
  read -ra fields <<< "$proc_tail"
  [ "${fields[1]}" = "$CONTROLLER_PID" ] || return 1
  printf '%s:%s\n' "${fields[1]}" "${fields[19]}"
}
stop_owned() {
  local node=$1 pid expected actual
  [ "${STOPPED[$((node - 1))]:-0}" -eq 0 ] || return 0
  pid=${PIDS[$((node - 1))]}
  kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; STOPPED[$((node - 1))]=1; return 0; }
  read -r expected < "$WORK/s$node.process-identity"
  actual=$(identity "$pid") || return 1
  [ "$actual" = "$expected" ] || { echo 'owned process identity changed' >&2; return 1; }
  kill -TERM "$pid"
  for _ in $(seq 1 100); do
    kill -0 "$pid" 2>/dev/null || { wait "$pid" 2>/dev/null || true; STOPPED[$((node - 1))]=1; return 0; }
    sleep .05
  done
  [ "$(identity "$pid")" = "$expected" ] || return 1
  kill -KILL "$pid"
  wait "$pid" 2>/dev/null || true
  STOPPED[$((node - 1))]=1
}
cleanup() {
  local result=$? node
  trap - EXIT
  set +e
  for node in $(seq 1 "${#PIDS[@]}"); do stop_owned "$node" || result=1; done
  if [ "$result" -ne 0 ]; then
    for node in $(seq 1 "${#PIDS[@]}"); do tail -80 "$WORK/s$node.log"; done
  fi
  exit "$result"
}
trap cleanup EXIT
for node in 1 2 3; do
  TARN_SHARD_ID=$node TARN_LISTEN="127.0.0.1:$((BASE + node))" \
    TARN_WORKDIR="$WORK/s$node" TARN_PEERS="$PEERS" TARN_WAL_MODE=fsync \
    TARN_TEST_HOOKS=1 TARN_CHECK_YIELDS=1 TARN_CHECK_LOCKS=1 TARN_HOST_INSTANCES=3 \
    TARN_MEMORY_MB=128 TARN_VINYL_MEMORY_MB=16 TARN_VINYL_CACHE_MB=0 \
    "$T" "$DIR/storage.lua" > "$WORK/s$node.log" 2>&1 &
  PIDS+=("$!")
  identity "${PIDS[$((node - 1))]}" > "$WORK/s$node.process-identity"
done
"$T" "$DIR/test/member_dependency_test.lua" setup "${URIS[@]}" | tee "$WORK/setup.log"
"$T" "$DIR/test/member_dependency_test.lua" finish "${URIS[@]}" | tee "$WORK/finish.log"
rg -q '^SAFE_TO_STOP_SHARD_2 ' "$WORK/finish.log" 2>/dev/null || \
  grep -q '^SAFE_TO_STOP_SHARD_2 ' "$WORK/finish.log"
stop_owned 2
"$T" "$DIR/test/member_dependency_test.lua" survivors "${URIS[@]}" | tee "$WORK/survivors.log"
