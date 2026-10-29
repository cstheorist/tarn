#!/bin/bash
# Actual RF3 clock/refusal/GC controls; all data is private to this fixture. No host ports.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:?new private work directory required}
BASE=${CLOCK_PROTOCOL_BASE:-3960}
export TARN_NATIVE=0
[ -d /proc/self ] || { echo 'Linux process identity checks required'; exit 1; }
mkdir "$WORK"
printf 'clock-protocol-rf3\n' > "$WORK/owner"
find "$DIR" -type f -print0 | sort -z | xargs -0 sha256sum > "$WORK/source.sha256"

declare -a PIDS STARTS
owned() {
    local i=$1 p=${PIDS[$1]:-}
    [ -n "$p" ] && kill -0 "$p" 2>/dev/null && [ -r "/proc/$p/stat" ] || return 1
    [ "$(awk '{print $4 ":" $22}' "/proc/$p/stat")" = "$$:${STARTS[$i]:-missing}" ]
}
cleanup() {
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
        if [ -n "$p" ]; then
            if ! kill -0 "$p" 2>/dev/null || owned "$i"; then wait "$p" 2>/dev/null || true
            else echo "FAIL changed process identity for node$i pid$p" >&2; fi
        fi
    done
}
trap cleanup EXIT
REPLICAS="[\"tarn:tarn@127.0.0.1:$((BASE+1))\",\"tarn:tarn@127.0.0.1:$((BASE+2))\",\"tarn:tarn@127.0.0.1:$((BASE+3))\"]"
PEERS="{\"1\":$REPLICAS}"
for i in 1 2 3; do
    port=$((BASE+i))
    if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then echo "FAIL occupied private fixture port $port"; exit 1; fi
    TARN_SHARD_ID=1 TARN_LISTEN="127.0.0.1:$port" TARN_WORKDIR="$WORK/s$i" \
    TARN_REPLICAS="$REPLICAS" TARN_PEERS="$PEERS" TARN_WAL_MODE=fsync \
    TARN_MEMORY_MB=96 TARN_VINYL_MEMORY_MB=16 TARN_VINYL_CACHE_MB=0 \
    TARN_VINYL_READ_THREADS=1 TARN_HOST_INSTANCES=3 TARN_LOG_LEVEL=4 \
    TARN_CHECK_YIELDS=1 TARN_TEST_HOOKS=1 TARN_CHECK_LOCKS=1 \
        "$T" "$DIR/storage.lua" > "$WORK/s$i.log" 2>&1 &
    PIDS[$i]=$!
    STARTS[$i]=$(awk '{print $22}' "/proc/${PIDS[$i]}/stat")
    printf '%s\n' "${PIDS[$i]}" > "$WORK/s$i.pid"
    printf '%s:%s\n' "$$" "${STARTS[$i]}" > "$WORK/s$i.process-identity"
done
CLOCK_PROTOCOL_WORK="$WORK" CLOCK_PROTOCOL_BASE="$BASE" \
    timeout 100 "$T" "$DIR/test/clock_protocol_test.lua"
echo 'clock protocol RF3 checks complete; exact owned processes cleaned by trap'
