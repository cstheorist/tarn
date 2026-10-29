#!/bin/bash
# A private RF3 fixture for yielding native CALL input ownership. No host ports.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:?new private work directory required}
BASE=${NATIVE_FRESHNESS_BASE:-3850}
NATIVE=${TARN_NATIVE:?native module required}
export NATIVE_FRESHNESS_CONTROL=${NATIVE_FRESHNESS_CONTROL:-native}
case "$NATIVE_FRESHNESS_CONTROL" in native|handback) ;; *) echo 'FAIL invalid freshness control'; exit 1;; esac
export NATIVE_FRESHNESS_PRELOAD="$DIR/test/native_freshness_control.lua"
[ "$NATIVE" != 0 ] && [ -f "$NATIVE" ]
[ -d /proc/self ] || { echo 'Linux process identity checks required'; exit 1; }
mkdir "$WORK"
printf 'native-freshness-rf3\n' > "$WORK/owner"
printf '%s\n' "$NATIVE_FRESHNESS_CONTROL" > "$WORK/control"
find "$DIR" "$DIR/../crates/tarn-storage" "$DIR/../crates/tarn-verified" -type f -print0 \
    | sort -z | xargs -0 sha256sum > "$WORK/source.sha256"
sha256sum "$DIR/../Cargo.lock" "$DIR/../rust-toolchain.toml" >> "$WORK/source.sha256"
sha256sum "$NATIVE" > "$WORK/native.sha256"
sha256sum "$(command -v "$T")" > "$WORK/tarantool.sha256"
"$T" --version > "$WORK/tarantool-version.txt"
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
        if [ -n "$p" ]; then
            if ! kill -0 "$p" 2>/dev/null || owned "$i"; then wait "$p" 2>/dev/null || true
            else echo "FAIL changed process identity for node$i pid$p" >&2; fi
            if kill -0 "$p" 2>/dev/null; then printf 'node%s pid=%s still-present\n' "$i" "$p"
            else printf 'node%s pid=%s reaped\n' "$i" "$p"; fi >> "$WORK/cleanup.txt"
        fi
    done
    # The old fixture left its per-round evidence only on the ephemeral
    # runner. Retain complete records, including on timeout/assertion failure.
    for f in "$WORK"/events.jsonl "$WORK"/source.sha256 "$WORK"/native.sha256 \
             "$WORK"/control "$WORK"/cleanup.txt "$WORK"/tarantool.sha256 "$WORK"/tarantool-version.txt \
             "$WORK"/s*.process-identity "$WORK"/s*/status-events.jsonl; do
        [ -f "$f" ] && { echo "freshness evidence: $f"; cat "$f"; }
    done
    if [ "$result" -ne 0 ]; then
        for f in "$WORK"/s*.log; do [ -f "$f" ] && { echo "freshness process log: $f"; cat "$f"; }; done
    fi
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
        "$T" -e 'dofile(os.getenv("NATIVE_FRESHNESS_PRELOAD"))' "$DIR/storage.lua" > "$WORK/s$i.log" 2>&1 &
    PIDS[$i]=$!
    STARTS[$i]=$(awk '{print $22}' "/proc/${PIDS[$i]}/stat")
    printf '%s\n' "${PIDS[$i]}" > "$WORK/s$i.pid"
    printf '%s:%s\n' "$$" "${STARTS[$i]}" > "$WORK/s$i.process-identity"
done
NATIVE_FRESHNESS_WORK="$WORK" NATIVE_FRESHNESS_BASE="$BASE" \
    timeout 120 "$T" "$DIR/test/native_freshness_test.lua"
echo 'native RF3 yielding-input checks complete; owned-process cleanup observations follow'
