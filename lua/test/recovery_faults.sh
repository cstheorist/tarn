#!/bin/bash
# Recovery and garbage collection move on past what they cannot resolve
# (issue #129): three single-node shards, of which recovery_faults_test.lua
# pauses the third (SIGSTOP) while the other two hold more undecided
# transactions that need it than a recovery pass takes, and newer ones that
# do not; then resumes it.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:?private test work directory required}
mkdir -p "$WORK"
P1=${RECOVERY_P1:-3571}; P2=${RECOVERY_P2:-3572}; P3=${RECOVERY_P3:-3573}
PEERS="{\"1\":[\"tarn:tarn@127.0.0.1:$P1\"],\"2\":[\"tarn:tarn@127.0.0.1:$P2\"],\"3\":[\"tarn:tarn@127.0.0.1:$P3\"]}"
cleanup() {
  for p in "$WORK"/s*.pid; do
    [ -f "$p" ] || continue
    kill -CONT "$(cat "$p")" 2>/dev/null || true
    kill "$(cat "$p")" 2>/dev/null || true
  done
}
trap cleanup EXIT
for i in 1 2 3; do
  case $i in 1) port=$P1;; 2) port=$P2;; 3) port=$P3;; esac
  TARN_SHARD_ID=$i TARN_LISTEN=127.0.0.1:$port TARN_WORKDIR="$WORK/s$i" TARN_PEERS="$PEERS" \
  TARN_MEMORY_MB=128 TARN_LOG_LEVEL=4 TARN_HOST_INSTANCES=3 TARN_CHECK_YIELDS=1 TARN_CHECK_LOCKS=1 \
  "$T" "$DIR/storage.lua" > "$WORK/s$i.log" 2>&1 &
  echo $! > "$WORK/s$i.pid"
done
for port in "$P1" "$P2" "$P3"; do
  for _ in $(seq 100); do
    if "$T" -e "local c=require('net.box').connect('tarn:tarn@127.0.0.1:$port',{wait_connected=.1})
       local ok,r=pcall(c.eval,c,'return rawget(_G,\"tarn\")~=nil') os.exit(ok and r and 0 or 1)" \
       </dev/null >/dev/null 2>&1; then break; fi
    sleep .1
  done
done
rc=0
S1=127.0.0.1:$P1 S2=127.0.0.1:$P2 S3=127.0.0.1:$P3 S3_PID=$(cat "$WORK/s3.pid") \
  timeout 240 "$T" "$DIR/test/recovery_faults_test.lua" < /dev/null || rc=$?
if grep -h "lock copy differs" "$WORK"/s*.log | head -3 | grep .; then
  echo "FAIL the native module's copy of the lock index differed from it"
  rc=1
fi
[ "$rc" -eq 0 ] || echo "logs in $WORK"
exit $rc
