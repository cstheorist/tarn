#!/bin/bash
# Where a shard's transaction thread spends its time (issue #28): starts a
# cluster of one shard and one SQL server, loads the sysbench tables, and
# runs bench/profile.py against it. With AB=n it runs n rounds instead: in
# each, every workload runs on the shard restarted with the native
# procedures, then on the shard restarted without them (each run starts
# from a fresh shard: what one workload leaves in the Lua heap and the JIT
# compiler does not weigh on the next). It prints the means of each: the
# comparison in bench/RESULTS.md ("Where a shard's time goes").
#
#   bench/profile.sh [WORKLOADS]   (default oltp_point_select,oltp_read_only)
#
# Environment: AB=ROUNDS; R (1: the shard's replicas; with more, its
# tables are synchronous, and the leader is profiled, every replica
# restarted for each run); THREADS (16); TIME (20: seconds per workload);
# TABLES, SIZE (4 x 100000 rows); DIR (/tmp/tarn-prof); BASE (4500: replica
# j listens on BASE + 10 + j); PORT (4700: the SQL server); OUT (DIR-results:
# a JSON file per run); TARN_NATIVE (the module, as for scripts/cluster.sh;
# AB's native rounds default to target/release's). Needs sysbench and the
# mysql client.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
T=${TARANTOOL:-tarantool}
TESTS=${1:-oltp_point_select,oltp_read_only}
AB=${AB:-0}; R=${R:-1}; THREADS=${THREADS:-16}; TIME=${TIME:-20}; TABLES=${TABLES:-4}; SIZE=${SIZE:-100000}
DIR=${DIR:-/tmp/tarn-prof}; BASE=${BASE:-4500}; PORT=${PORT:-4700}; OUT=${OUT:-$DIR-results}
mkdir -p "$OUT"

# Wait until a replica of the shard serves requests (its module is loaded,
# and it is the leader); it is the one profiled: SHARD, LEADER.
ready() {
  for _ in $(seq 300); do
    for j in $(seq "$R"); do
      SHARD=127.0.0.1:$((BASE + 10 + j)); LEADER=s1.$j
      "$T" -e "local c = require('net.box').connect('tarn:tarn@$SHARD', {wait_connected = 1})
        local ok, r = pcall(c.eval, c, 'return box.info.status == \"running\" and not box.info.ro and rawget(_G, \"tarn\") ~= nil')
        os.exit((ok and r) and 0 or 1)" < /dev/null > /dev/null 2>&1 && return 0
    done
    sleep 0.5
  done
  echo "the shard did not start" >&2
  return 1
}

profile() {   # profile LABEL [profile.py options]
  local label=$1; shift
  python3 "$ROOT/bench/profile.py" --sql "127.0.0.1:$PORT" --shard "$SHARD" --pid "$(cat "$DIR/$LEADER.pid")" \
    --tables "$TABLES" --size "$SIZE" --tests "$TESTS" --threads "$THREADS" --time "$TIME" \
    --json "$OUT/$label.json" "$@"
}

"$ROOT/scripts/cluster.sh" stop -d "$DIR" 2> /dev/null
"$ROOT/scripts/cluster.sh" start -n 1 -r "$R" -d "$DIR" -b "$BASE" -p "$PORT" || exit 1
trap '"$ROOT/scripts/cluster.sh" stop -d "$DIR"' EXIT
ready || exit 1
if [ "$AB" = 0 ]; then
  profile run --prepare
  exit $?
fi
python3 "$ROOT/bench/profile.py" --sql "127.0.0.1:$PORT" --shard "$SHARD" --pid "$(cat "$DIR/$LEADER.pid")" \
  --tables "$TABLES" --size "$SIZE" --tests "" --prepare || exit 1
NATIVE=${TARN_NATIVE:-$ROOT/target/release/libtarn_storage.so}
rm -f "$OUT"/native-*.json "$OUT"/lua-*.json
for round in $(seq "$AB"); do
  for test in ${TESTS//,/ }; do
    for mode in native lua; do
      # Restart the shard with the module or without it (its data stays):
      # every replica, as any may be the next leader.
      if [ $mode = native ]; then v=$NATIVE; else v=0; fi
      for j in $(seq "$R"); do
        pid=$(cat "$DIR/s1.$j.pid"); kill "$pid"
        while kill -0 "$pid" 2> /dev/null; do sleep 0.2; done
        sed -i "s|TARN_NATIVE=[^ ]*|TARN_NATIVE=$v|" "$DIR/s1.$j.sh"
      done
      for j in $(seq "$R"); do
        "$ROOT/scripts/cluster.sh" restart -d "$DIR" "s1.$j" > /dev/null || exit 1
      done
      ready || exit 1
      # (the SQL server connects to it again on its next read)
      mysql -h 127.0.0.1 -P "$PORT" -u root sbtest -e 'SELECT 1 FROM sbtest1 WHERE id = 1' > /dev/null 2>&1
      echo "== round $round, $test, $mode"
      TESTS=$test profile "$mode-$round-$test" || exit 1
    done
  done
done
python3 "$ROOT/bench/profile.py" --summarize "$OUT"
