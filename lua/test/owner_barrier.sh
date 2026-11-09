#!/bin/bash
# A private RF3 fixture for the lock calls' barrier without the synchronous
# queue's wait (issue #314, F.owner_barrier in lua/tarn_fresh.lua), with
# TARN_LOCK_BARRIER=owner. No host ports beyond the fixture's own.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:?new private work directory required}
BASE=${OWNER_BARRIER_BASE:-3950}
[ -d /proc/self ] || { echo 'Linux process identity checks required'; exit 1; }
mkdir "$WORK"
declare -a PIDS STARTS
owned() {
    local i=$1 p=${PIDS[$1]:-}
    [ -n "$p" ] && kill -0 "$p" 2>/dev/null && [ -r "/proc/$p/stat" ] || return 1
    [ "$(awk '{print $4 ":" $22}' "/proc/$p/stat")" = "$$:${STARTS[$i]:-missing}" ]
}
cleanup() {
    local result=$?
    local i p
    for i in 1 2 3; do
        if owned "$i"; then kill -CONT "${PIDS[$i]}" 2>/dev/null || true; kill -TERM "${PIDS[$i]}" 2>/dev/null || true; fi
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
        [ -n "$p" ] && { wait "$p" 2>/dev/null || true; }
    done
    if [ "$result" -ne 0 ]; then
        for f in "$WORK"/s*.log; do [ -f "$f" ] && { echo "owner barrier process log: $f"; tail -n 80 "$f"; }; done
    fi
}
trap cleanup EXIT
REPLICAS="[\"tarn:tarn@127.0.0.1:$((BASE+1))\",\"tarn:tarn@127.0.0.1:$((BASE+2))\",\"tarn:tarn@127.0.0.1:$((BASE+3))\"]"
PEERS="{\"1\":$REPLICAS}"
for i in 1 2 3; do
    port=$((BASE+i))
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then echo "FAIL occupied private fixture port $port"; exit 1; fi
    TARN_SHARD_ID=1 TARN_LISTEN="127.0.0.1:$port" TARN_WORKDIR="$WORK/s$i" \
    TARN_REPLICAS="$REPLICAS" TARN_PEERS="$PEERS" \
    TARN_MEMORY_MB=96 TARN_VINYL_MEMORY_MB=16 TARN_VINYL_CACHE_MB=0 \
    TARN_VINYL_READ_THREADS=1 TARN_HOST_INSTANCES=3 TARN_LOG_LEVEL=4 \
    TARN_CHECK_YIELDS=1 TARN_TEST_HOOKS=1 TARN_NATIVE=0 TARN_LOCK_BARRIER=owner \
        "$T" "$DIR/storage.lua" > "$WORK/s$i.log" 2>&1 &
    PIDS[$i]=$!
    STARTS[$i]=$(awk '{print $22}' "/proc/${PIDS[$i]}/stat")
    printf '%s\n' "${PIDS[$i]}" > "$WORK/s$i.pid"
    printf '%s:%s\n' "$$" "${STARTS[$i]}" > "$WORK/s$i.process-identity"
done
OWNER_BARRIER_WORK="$WORK" OWNER_BARRIER_BASE="$BASE" \
    timeout 150 "$T" "$DIR/test/owner_barrier_test.lua"
