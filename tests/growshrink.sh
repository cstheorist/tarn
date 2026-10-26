#!/bin/bash
# Grow a cluster from two shards to four and shrink it to three under load
# (issue #25). A cluster of two shards and three SQL servers (480 buckets, so
# that rebalancing takes seconds; ddl_job_stale = 2 s; the load of a bucket
# counted over windows of 5 s); the storage of shards 3 and 4 starts. The
# bank workloads of tests/sql/txntest.py and tests/sql/rctest.py run through
# the first two SQL servers while tests/sql/growshrink.py --phase grow adds
# shards 3 and 4, rebalances (pausing and resuming), removes shard 2, stops
# its storage and rebalances again. Then the final sums are checked through
# SQL (the workloads' own checks) and through direct Key/Value reads on the
# shards left, and growshrink.py --phase control rebalances by load, cancels
# a rebalance, and kills the SQL server running one.
#   SECS=SECONDS ENGINE=vinyl tests/growshrink.sh
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
T=${TARANTOOL:-tarantool}
DIR=${DIR:-/tmp/tarn-growshrink}
BASE=${BASE:-4400}
PORT=${PORT:-4920}
SECS=${SECS:-60}
ENGINE=${ENGINE:-}
C="$ROOT/scripts/cluster.sh"
"$C" stop -d "$DIR" 2>/dev/null
export TARN_DDL_JOB_STALE=2 TARN_BUCKET_COUNT=480 TARN_LOAD_WINDOW=5
"$C" start -n 2 -r 1 -s 3 -d "$DIR" -b "$BASE" -p "$PORT" -m 256 ${ENGINE:+-e "$ENGINE"} || exit 1
SERVERS="127.0.0.1:$PORT,127.0.0.1:$((PORT + 1)),127.0.0.1:$((PORT + 2))"
rc=0
if r3=$("$C" add-shard -d "$DIR" 3) && r4=$("$C" add-shard -d "$DIR" 4); then
  python3 "$ROOT/tests/sql/txntest.py" --tarn "127.0.0.1:$PORT" --only bank --bank-seconds "$SECS" --threads 8 \
    > "$DIR/bank.out" 2>&1 &
  BANK=$!
  python3 "$ROOT/tests/sql/rctest.py" --tarn "127.0.0.1:$((PORT + 1))" --only bank --bank-seconds "$SECS" \
    --threads 4 > "$DIR/rcbank.out" 2>&1 &
  RCBANK=$!
  python3 "$ROOT/tests/sql/growshrink.py" --phase grow --tarn "$SERVERS" --add "3=$r3" --add "4=$r4" \
    --remove 2 --dir "$DIR" | tee "$DIR/test.out"
  [ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
  wait "$BANK" || rc=1
  wait "$RCBANK" || rc=1
  cat "$DIR/bank.out" "$DIR/rcbank.out" | tee -a "$DIR/test.out"
  # The sums, read straight from the shards left (shards 1, 3 and 4).
  KV="127.0.0.1:$((BASE + 11)),127.0.0.1:$((BASE + 31)),127.0.0.1:$((BASE + 41))"
  python3 - "$KV" <<EOF | tee -a "$DIR/test.out"
import json, subprocess, sys
nodes = sys.argv[1].split(",")
ok = True
for space, n, total in (("txntest.bank", 64, 64000), ("rctest.acct", 32, 32000)):
    s = 0
    for i in range(n):
        out = subprocess.run(["$T", "$ROOT/tests/sql/kvget.lua", json.dumps(nodes), space, json.dumps([i])],
                             capture_output=True, text=True, stdin=subprocess.DEVNULL).stdout
        rows = json.loads(out.strip().splitlines()[-1])
        assert len(rows) == 1, (space, i, rows)
        s += rows[0][1]
    print(f"  {'ok  ' if s == total else 'FAIL'} {space}: the sum of direct Key/Value reads on shards 1, 3 and 4 "
          f"is {s} (want {total})")
    ok = ok and s == total
sys.exit(0 if ok else 1)
EOF
  [ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
  python3 "$ROOT/tests/sql/growshrink.py" --phase control --tarn "$SERVERS" --dir "$DIR" \
    --load-window "$TARN_LOAD_WINDOW" | tee -a "$DIR/test.out"
  [ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
else
  echo "the storage of shards 3 and 4 did not start" | tee "$DIR/test.out"; rc=1
fi
if [ "$rc" -eq 0 ]; then
  "$C" stop -d "$DIR"
  echo "GROW AND SHRINK TEST PASSED"
else
  KEEP=1 "$C" stop -d "$DIR"
  echo "GROW AND SHRINK TEST FAILED; logs kept in $DIR"
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    msg=$({ grep -E "FAIL|Error|Traceback|EXCEPTION" "$DIR/test.out" | head -20
            echo "--- workloads"; tail -6 "$DIR/bank.out" "$DIR/rcbank.out"
            echo "--- tarn-server"; grep -h -iE "error|warn" "$DIR"/tarn*.log | tail -10
            echo "--- storage"; grep -h -iE "error|warn" "$DIR"/s*.log | tail -10; } |
          sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')
    echo "::error title=Grow and shrink test failed::$msg"
  fi
fi
exit "$rc"
