#!/bin/bash
# Dedicated processes: keep topology tests separate from the general suites.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:?private test work directory required}
mkdir -p "$WORK"
for engine in memtx vinyl; do
  set +e
  timeout 30 "$T" "$DIR/test/lazy_create_restart_test.lua" "$WORK/create-$engine" "$engine" crash
  rc=$?
  set -e
  if [ "$rc" -ne 137 ]; then echo "FAIL lazy create $engine expected SIGKILL137, got $rc"; exit 1; fi
  timeout 30 "$T" "$DIR/test/lazy_create_restart_test.lua" "$WORK/create-$engine" "$engine" finish
done
for engine in memtx vinyl; do
  for stage in pending floor; do
    set +e
    timeout 30 "$T" "$DIR/test/lazy_restart_test.lua" "$WORK/restart-$engine" "$engine" "$stage"
    rc=$?
    set -e
    if [ "$rc" -ne 137 ]; then echo "FAIL lazy $engine $stage expected SIGKILL137, got $rc"; exit 1; fi
  done
  timeout 30 "$T" "$DIR/test/lazy_restart_test.lua" "$WORK/restart-$engine" "$engine" finish
done
for engine in memtx vinyl; do
  for op in drop truncate; do
    for stage in native cleanup; do
      set +e
      timeout 30 "$T" "$DIR/test/lazy_clear_restart_test.lua" "$WORK/clear-$engine-$op" "$engine" "$op" "$stage"
      rc=$?
      set -e
      if [ "$rc" -ne 137 ]; then echo "FAIL clear $engine $op $stage expected SIGKILL137, got $rc"; exit 1; fi
    done
    timeout 30 "$T" "$DIR/test/lazy_clear_restart_test.lua" "$WORK/clear-$engine-$op" "$engine" "$op" finish
  done
done
P1=${LAZY_P1:-3551}; P2=${LAZY_P2:-3552}; P3=${LAZY_P3:-3553}
PEERS="{\"1\":[\"tarn:tarn@127.0.0.1:$P1\"],\"2\":[\"tarn:tarn@127.0.0.1:$P2\"],\"3\":[\"tarn:tarn@127.0.0.1:$P3\"]}"
cleanup() { for p in "$WORK"/s*.pid; do [ ! -f "$p" ] || kill "$(cat "$p")" 2>/dev/null || true; done; }
trap cleanup EXIT
start_node() {
  local i=$1 port
  case $i in 1) port=$P1;; 2) port=$P2;; 3) port=$P3;; esac
  TARN_SHARD_ID=$i TARN_LISTEN=127.0.0.1:$port TARN_WORKDIR="$WORK/s$i" TARN_PEERS="$PEERS" \
  TARN_MEMORY_MB=128 TARN_VINYL_MEMORY_MB=16 TARN_VINYL_CACHE_MB=0 TARN_WAL_MODE=fsync \
  TARN_LOG_LEVEL=4 TARN_CHECK_YIELDS=1 TARN_TEST_HOOKS=1 \
  "$T" "$DIR/storage.lua" >> "$WORK/s$i.log" 2>&1 &
  echo $! > "$WORK/s$i.pid"
}
for i in 1 2 3; do start_node "$i"; done
for port in "$P1" "$P2" "$P3"; do
  for _ in $(seq 100); do
    if "$T" -e "local c=require('net.box').connect('tarn:tarn@127.0.0.1:$port',{wait_connected=.1})
       local ok,r=pcall(c.eval,c,'return rawget(_G,\"tarn\")~=nil') os.exit(ok and r and 0 or 1)" \
       </dev/null >/dev/null 2>&1; then break; fi
    sleep .1
  done
done
S1=127.0.0.1:$P1 S2=127.0.0.1:$P2 S3=127.0.0.1:$P3 timeout 90 "$T" "$DIR/test/lazy_topology_test.lua"
# The candidate keeps its heterogeneous auxiliary IDs after real WAL recovery.
kill -9 "$(cat "$WORK/s3.pid")"
wait "$(cat "$WORK/s3.pid")" 2>/dev/null || true
start_node 3
S3=127.0.0.1:$P3 timeout 30 "$T" "$DIR/test/lazy_join_restart_test.lua"
S1=127.0.0.1:$P1 S2=127.0.0.1:$P2 S3=127.0.0.1:$P3 timeout 45 "$T" "$DIR/test/lazy_shipper_test.lua"
