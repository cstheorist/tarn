#!/bin/bash
# Start three single-node shards and run the storage protocol tests against
# them (the third is the shard that joins in member_test.lua).
set -u
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d /tmp/tarn-lua-test.XXXX)
rc=0
if [ -z "${TEST:-}" ]; then
  TARANTOOL="$T" bash "$DIR/test/alter_backfill_faults.sh" "$WORK/backfill" 2>&1 | tee -a "$WORK/test.out"
  r=${PIPESTATUS[0]}
  [ "$r" -ne 0 ] && rc=$r
fi
P1=${P1:-3511}; P2=${P2:-3512}; P3=${P3:-3513}
PEERS="{\"1\":[\"tarn:tarn@127.0.0.1:$P1\"],\"2\":[\"tarn:tarn@127.0.0.1:$P2\"]}"
for i in 1 2 3; do
  case $i in 1) port=$P1;; 2) port=$P2;; 3) port=$P3;; esac
  # Shard 1 sets vinyl_read_threads; shards 2 and 3 take the default for
  # two instances on this machine.
  TARN_VINYL_READ_THREADS=$([ $i = 1 ] && echo 3) TARN_HOST_INSTANCES=2 \
  TARN_SHARD_ID=$i TARN_LISTEN=127.0.0.1:$port TARN_WORKDIR=$WORK/s$i TARN_PEERS="$PEERS" \
  TARN_MEMORY_MB=256 TARN_LOG_LEVEL=4 TARN_CHECK_YIELDS=1 TARN_TEST_HOOKS=1 \
  $T "$DIR/storage.lua" > "$WORK/s$i.log" 2>&1 &
  echo $! > "$WORK/s$i.pid"
done
for p in $P1 $P2 $P3; do for _ in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.1; done; done
# The port opens before the module has made its schema (tarn.init): a table
# a test makes in between gets another space id on that shard. Wait for it.
for p in $P1 $P2 $P3; do
  for _ in $(seq 100); do
    $T -e "local c = require('net.box').connect('tarn:tarn@127.0.0.1:$p', {wait_connected = 1})
           local ok, r = pcall(c.eval, c, 'return rawget(_G, \"tarn\") ~= nil')
           os.exit((ok and r) and 0 or 1)" < /dev/null > /dev/null 2>&1 && break
    sleep 0.1
  done
done
# The files run in turn on the same shards (each uses its own tables).
for f in ${TEST:-$DIR/test/storage_test.lua $DIR/test/alter_backfill_test.lua $DIR/test/join_test.lua $DIR/test/bucket_test.lua $DIR/test/member_test.lua $DIR/test/rebalance_test.lua $DIR/test/native_test.lua}; do
  S1=127.0.0.1:$P1 S2=127.0.0.1:$P2 S3=127.0.0.1:$P3 timeout 120 $T "$f" < /dev/null 2>&1 | tee -a "$WORK/test.out"
  r=${PIPESTATUS[0]}
  [ "$r" -ne 0 ] && rc=$r
done
kill $(cat "$WORK"/s*.pid) 2>/dev/null
if [ "$rc" -ne 0 ]; then
  echo "--- shard logs ($WORK)"; grep -iE "error|warn" "$WORK"/s*.log | tail -20
  # On GitHub Actions, put the failures in an annotation (job logs are not
  # always at hand; annotations are).
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    msg=$({ echo "exit code $rc"; grep -E "FAIL|rror" "$WORK/test.out" | head -30; tail -5 "$WORK/test.out"
            echo "--- shard logs"; grep -iE "error|warn" "$WORK"/s*.log | tail -15; } |
          sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')
    echo "::error title=Storage protocol tests failed::$msg"
  fi
fi
exit "$rc"
