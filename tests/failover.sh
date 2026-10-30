#!/bin/bash
# Failover test: 2 shards x 3 synchronous replicas behind one tarn-server.
# Runs the concurrent bank-transfer workload (serializable audits, a ledger
# that exposes lost or duplicated transfers, autocommit counters that expose
# double-applied single-row writes) next to READ COMMITTED transfers under
# row locks (with their own ledger: tests/sql/rctest.py, test_zz_bank) and
# a third bank whose buckets keep moving between the shards
# (tests/sql/movetest.py) and, while they run:
#   1. moves the leadership of shard 1 to another replica (box.ctl.promote):
#      the old leader is demoted while the SQL server's connections to it
#      stay open;
#   2. kill -9's the leader of shard 1;
#   3. kill -9's the leader of shard 2.
# Passes if every check of the workload holds, the final total is exact via
# SQL and via direct Key/Value reads on the new leaders, and transfers kept
# committing after each event (and, with TARN_CHECK_LOCKS, the native
# module's copy of the lock index held what the index held).
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
T=${TARANTOOL:-tarantool}
DIR=${DIR:-/tmp/tarn-failover}
BASE=${BASE:-3500}
PORT=${PORT:-4100}
SECS=${SECS:-60}
ENGINE=${ENGINE:-}   # vinyl: the workloads' tables are vinyl tables
"$ROOT/scripts/cluster.sh" stop -d "$DIR" 2>/dev/null
"$ROOT/scripts/cluster.sh" start -n 2 -r 3 -d "$DIR" -b "$BASE" -p "$PORT" -m "${MEMORY_MB:-256}" -w "${WAL_MODE:-write}" ${ENGINE:+-e "$ENGINE"} || exit 1
exec > >(tee -a "$DIR/events.out") 2>&1   # keep the event log for the failure report

leader_of() {  # shard -> port of its current writable node
  local s=$1 j
  for j in 1 2 3; do
    local p=$((BASE + 10 * s + j))
    if "$T" "$ROOT/scripts/probe.lua" "tarn:tarn@127.0.0.1:$p" < /dev/null > /dev/null 2>&1; then echo "$p"; return 0; fi
  done
  return 1
}

check_alive() {
  local f dead=""
  for f in "$DIR"/s*.pid; do kill -0 "$(cat "$f")" 2>/dev/null || dead="$dead $(basename "$f" .pid)"; done
  [ -n "$dead" ] && echo "$(date +%T) NOT RUNNING:$dead"
}

# Instances that stopped although the test did not kill them, with the end
# of their logs: why they stopped.
stopped_on_their_own() {
  local f n
  for f in "$DIR"/s*.pid; do
    n=$(basename "$f" .pid)
    kill -0 "$(cat "$f")" 2>/dev/null && continue
    [ -e "$DIR/$n.killed" ] && continue
    echo "--- $n stopped on its own; the end of $n.log:"
    tail -12 "$DIR/$n.log"
  done
}

kill_leader() {
  local s=$1 p
  check_alive
  p=$(leader_of "$s") || { echo "shard $s: no leader found"; return 1; }
  local j=$(( (p - BASE - 10 * s) ))
  echo "$(date +%T) killing shard $s leader on port $p (s$s.$j)"
  touch "$DIR/s$s.$j.killed"
  kill -9 "$(cat "$DIR/s$s.$j.pid")"
  local t0; t0=$(date +%s%N)
  for _ in $(seq 200); do
    local np; np=$(leader_of "$s") && {
      local ms=$(( ($(date +%s%N) - t0) / 1000000 ))
      echo "$(date +%T) shard $s new leader on $np after $((ms / 1000)).$(printf '%03d' $((ms % 1000))) s"; return 0; }
    sleep 0.1
  done
  echo "shard $s: no new leader after 20 s"; return 1
}

move_leader() {  # start an election on a follower of shard $1; the old leader stays up
  local s=$1 p j attempt
  check_alive
  p=$(leader_of "$s") || { echo "shard $s: no leader found"; return 1; }
  # Under a heavy write load a follower is often a little behind the leader
  # and loses the election, so retry, alternating followers.
  for attempt in 1 2 3 4 5 6; do
    for j in 1 2 3; do
      local q=$((BASE + 10 * s + j))
      [ "$q" = "$p" ] && continue
      echo "$(date +%T) box.ctl.promote() on s$s.$j (port $q), attempt $attempt; the leader on $p will be demoted with its connections open"
      # The election may also be won by the third replica: any new leader will do.
      "$T" "$ROOT/scripts/promote.lua" "tarn:tarn@127.0.0.1:$q" < /dev/null 2>&1 | head -1
      local until=$(( $(date +%s) + 5 ))
      while [ "$(date +%s)" -lt "$until" ]; do
        local np; np=$(leader_of "$s")
        if [ -n "$np" ] && [ "$np" != "$p" ]; then
          echo "$(date +%T) shard $s leader is now $np; $p still running: $(kill -0 "$(cat "$DIR/s$s.$((p - BASE - 10 * s)).pid")" && echo yes)"
          return 0
        fi
        sleep 0.1
      done
    done
  done
  echo "shard $s: leadership did not move"; return 1
}

python3 "$ROOT/tests/sql/txntest.py" --tarn 127.0.0.1:$PORT --only bank --bank-seconds "$SECS" --threads 8 \
  > "$DIR/bank.out" 2>&1 &
BANK=$!
python3 "$ROOT/tests/sql/rctest.py" --tarn 127.0.0.1:$PORT --only bank --bank-seconds "$SECS" --threads 4 \
  > "$DIR/rcbank.out" 2>&1 &
RCBANK=$!
# Bucket moves (issue #24) under their own transfers and READ COMMITTED
# reads, through the leader changes.
python3 "$ROOT/tests/sql/movetest.py" --tarn 127.0.0.1:$PORT --seconds "$SECS" --threads 2 --move-pause 0.2 \
  > "$DIR/moves.out" 2>&1 &
MOVES=$!
# The events happen at fixed times into the workloads, so that a leadership
# move that needs several attempts does not push the kills past their end.
START=$(date +%s)
at() { local t=$((START + $1)); while [ "$(date +%s)" -lt "$t" ]; do sleep 0.2; done; }
at $((SECS / 5))
move_leader 1 || MOVE_FAILED=1
at $((2 * SECS / 5))
kill_leader 1
at $((3 * SECS / 5))
kill_leader 2
wait $BANK
rc=$?
wait $RCBANK
rcrc=$?
wait $MOVES
mvrc=$?
cat "$DIR/bank.out" "$DIR/rcbank.out" "$DIR/moves.out"
KV="127.0.0.1:$(leader_of 1),127.0.0.1:$(leader_of 2)"
python3 - "$KV" <<EOF
import json, subprocess, sys
nodes = sys.argv[1].split(",")
total = 0
for i in range(64):
    out = subprocess.run(["$T", "$ROOT/tests/sql/kvget.lua", json.dumps(nodes), "txntest.bank", json.dumps([i])],
                         capture_output=True, text=True, stdin=subprocess.DEVNULL).stdout
    rows = json.loads(out.strip().splitlines()[-1])
    assert len(rows) == 1, (i, rows)
    total += rows[0][1]
print("direct KV sum on new leaders:", total)
sys.exit(0 if total == 64000 else 1)
EOF
kv=$?
# (what the new leaders' native procedures served: tarn_info().native)
for s in 1 2; do
  "$T" -e "local c = require('net.box').connect('tarn:tarn@127.0.0.1:$(leader_of $s)', {wait_connected = 1})
    print('shard $s leader, native procedures:', require('json').encode(c:call('tarn_info').native))
    os.exit(0)" < /dev/null 2> /dev/null
done
grep -h -E "tarn recovery|resolve" "$DIR"/s*.log | grep -v "^$" | tail -5
# (TARN_CHECK_LOCKS: the native module's copy of the lock index held what the
# index held, through the leader changes)
copy=0
if grep -h "lock copy differs" "$DIR"/s*.log | head -3 | grep .; then copy=1; fi
if [ $rc -eq 0 ] && [ $rcrc -eq 0 ] && [ $mvrc -eq 0 ] && [ $kv -eq 0 ] && [ $copy -eq 0 ] && [ -z "${MOVE_FAILED:-}" ]; then
  "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "FAILOVER TEST PASSED"
else
  crashed=$(stopped_on_their_own)
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "FAILOVER TEST FAILED (bank rc=$rc rc-bank rc=$rcrc moves rc=$mvrc kv=$kv lock copy=$copy move_failed=${MOVE_FAILED:-0}); logs kept in $DIR"
  [ -n "$crashed" ] && echo "$crashed"
  # On GitHub Actions, put the evidence in an annotation (job logs are not
  # always at hand; annotations are).
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    msg=$({ echo "bank rc=$rc rc-bank rc=$rcrc moves rc=$mvrc kv=$kv lock copy=$copy move_failed=${MOVE_FAILED:-0}"
            [ -n "$crashed" ] && echo "$crashed"
            grep -E "^[0-9]{2}:" "$DIR/events.out" | tail -12
            echo "--- workload"; tail -25 "$DIR/bank.out"; tail -8 "$DIR/rcbank.out"; tail -14 "$DIR/moves.out"
            echo "--- storage logs"; grep -h -iE "error|warn|election|promote" "$DIR"/s*.log | tail -15
            echo "--- tarn-server"; grep -h -iE "error|warn" "$DIR"/tarn*.log | tail -8; } |
          sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')
    echo "::error title=Failover test failed::$msg"
  fi
  exit 1
fi
