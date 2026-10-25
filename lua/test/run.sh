#!/bin/bash
# Start two single-node shards and run the storage protocol tests against them.
set -u
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=$(mktemp -d /tmp/tarn-lua-test.XXXX)
P1=${P1:-3511}; P2=${P2:-3512}
PEERS="{\"1\":[\"tarn:tarn@127.0.0.1:$P1\"],\"2\":[\"tarn:tarn@127.0.0.1:$P2\"]}"
for i in 1 2; do
  port=$([ $i = 1 ] && echo $P1 || echo $P2)
  TARN_SHARD_ID=$i TARN_LISTEN=127.0.0.1:$port TARN_WORKDIR=$WORK/s$i TARN_PEERS="$PEERS" \
  TARN_MEMORY_MB=256 TARN_LOG_LEVEL=4 $T "$DIR/storage.lua" > "$WORK/s$i.log" 2>&1 &
  echo $! > "$WORK/s$i.pid"
done
for p in $P1 $P2; do for _ in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep 0.1; done; done
S1=127.0.0.1:$P1 S2=127.0.0.1:$P2 timeout 120 $T "${TEST:-$DIR/test/storage_test.lua}" < /dev/null
rc=$?
kill $(cat "$WORK"/s*.pid) 2>/dev/null
[ $rc -ne 0 ] && { echo "--- shard logs ($WORK)"; grep -iE "error|warn" "$WORK"/s*.log | tail -20; }
exit $rc
