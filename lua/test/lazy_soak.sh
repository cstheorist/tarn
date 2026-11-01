#!/bin/bash
# A lazy index under churn (issue #177): two shards, each a source and a
# target, keeping protocol records for about 4 s (TARN_MAX_TXN_AGE=2,
# TARN_CLOCK_SKEW=1). lazy_soak_test.lua writes over many record lifetimes
# and checkpoints, stops the second shard (SIGSTOP) with a small outbox cap,
# kills and restarts it, then checks exact convergence, bounded outboxes and
# the reclaim of tombstones and records. ENGINE=vinyl uses a vinyl table;
# SOAK_SCALE=n makes each phase n times longer.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:?private test work directory required}
mkdir -p "$WORK"
P1=${SOAK_P1:-3591}; P2=${SOAK_P2:-3592}
PEERS="{\"1\":[\"tarn:tarn@127.0.0.1:$P1\"],\"2\":[\"tarn:tarn@127.0.0.1:$P2\"]}"
# Stops the nodes, and waits until they have exited (the next run reuses
# the ports).
cleanup() {
  local pids=()
  for p in "$WORK"/s*.pid; do
    [ -f "$p" ] || continue
    pids+=("$(cat "$p")")
  done
  for pid in "${pids[@]}"; do kill -CONT "$pid" 2>/dev/null || true; kill "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do
    for _ in $(seq 100); do kill -0 "$pid" 2>/dev/null || break; sleep .1; done
    kill -9 "$pid" 2>/dev/null || true
  done
}
trap cleanup EXIT
start_node() {
  local i=$1 port
  case $i in 1) port=$P1;; 2) port=$P2;; esac
  TARN_SHARD_ID=$i TARN_LISTEN=127.0.0.1:$port TARN_WORKDIR="$WORK/s$i" TARN_PEERS="$PEERS" \
  TARN_MEMORY_MB=256 TARN_VINYL_MEMORY_MB=32 TARN_VINYL_CACHE_MB=8 TARN_LOG_LEVEL=4 \
  TARN_MAX_TXN_AGE=2 TARN_CLOCK_SKEW=1 TARN_CHECK_YIELDS=1 TARN_TEST_HOOKS=1 TARN_CHECK_LOCKS=1 \
  "$T" "$DIR/storage.lua" >> "$WORK/s$i.log" 2>&1 &
  echo $! > "$WORK/s$i.pid"
  disown
}
wait_up() {
  local port=$1
  for _ in $(seq 150); do
    if "$T" -e "local c=require('net.box').connect('tarn:tarn@127.0.0.1:$port',{wait_connected=.1})
       local ok,r=pcall(c.eval,c,'return rawget(_G,\"tarn\")~=nil and box.space._tarn_part~=nil')
       os.exit(ok and r and 0 or 1)" </dev/null >/dev/null 2>&1; then return 0; fi
    sleep .1
  done
  return 1
}
for i in 1 2; do start_node "$i"; done
wait_up "$P1"; wait_up "$P2"
# The test asks for shard 2's restart by creating $WORK/restart: this helper
# kills it (SIGKILL), starts it again and creates $WORK/restarted.
rc=0
(
  while [ ! -f "$WORK/restart" ] && [ ! -f "$WORK/done" ]; do sleep .1; done
  if [ -f "$WORK/restart" ]; then
    kill -9 "$(cat "$WORK/s2.pid")" 2>/dev/null || true
    sleep .5
    start_node 2
    wait_up "$P2" && touch "$WORK/restarted"
  fi
) &
helper=$!
S1=127.0.0.1:$P1 S2=127.0.0.1:$P2 S2_PID=$(cat "$WORK/s2.pid") WORK="$WORK" ENGINE="${ENGINE:-memtx}" \
  timeout 300 "$T" "$DIR/test/lazy_soak_test.lua" < /dev/null || rc=$?
touch "$WORK/done"
wait "$helper" || true
if grep -hE "lock copy differs|yielded inside" "$WORK"/s*.log | head -3 | grep .; then
  echo "FAIL a protocol step yielded inside its atomic section, or the lock copy differed"
  rc=1
fi
[ "$rc" -eq 0 ] || echo "logs in $WORK"
exit $rc
