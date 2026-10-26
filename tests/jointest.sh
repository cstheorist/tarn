#!/bin/bash
# A shard joins a running cluster under load, and another leaves (issue #25):
# a cluster of two shards and three SQL servers (480 buckets, so that a shard
# drains in seconds); the storage of shard 3 starts, and tests/sql/jointest.py
# adds it and moves buckets to it while the workload of tests/sql/movetest.py
# runs through the first two SQL servers (the third stays idle until the
# end, with the membership from before), then removes shard 2 and stops its
# storage. Then the third SQL server starts again, with its configuration of
# shards 1 and 2, and must see and write shard 3's rows.
#   R=REPLICAS SECS=SECONDS ENGINE=vinyl tests/jointest.sh
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=${DIR:-/tmp/tarn-join}
BASE=${BASE:-3700}
PORT=${PORT:-4960}
SECS=${SECS:-45}
R=${R:-1}
ENGINE=${ENGINE:-}
C="$ROOT/scripts/cluster.sh"
"$C" stop -d "$DIR" 2>/dev/null
TARN_BUCKET_COUNT=480 "$C" start -n 2 -r "$R" -s 3 -d "$DIR" -b "$BASE" -p "$PORT" -m 256 ${ENGINE:+-e "$ENGINE"} || exit 1
rc=0
if reps=$("$C" add-shard -d "$DIR" 3); then
  python3 "$ROOT/tests/sql/jointest.py" --tarn "127.0.0.1:$PORT,127.0.0.1:$((PORT + 1))" \
    --idle "127.0.0.1:$((PORT + 2))" --add "3=$reps" --remove 2 --dir "$DIR" --seconds "$SECS" | tee "$DIR/test.out"
  [ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
  pid=$(cat "$DIR/tarn3.pid"); kill "$pid"
  for _ in $(seq 100); do kill -0 "$pid" 2>/dev/null || break; sleep 0.1; done
  kill -9 "$pid" 2>/dev/null
  echo "the idle SQL server, started again with its configuration of shards 1 and 2:"
  if "$C" restart -d "$DIR" tarn3; then
    python3 "$ROOT/tests/sql/jointest.py" --verify "127.0.0.1:$((PORT + 2))" --add "3=$reps" --remove 2 | tee -a "$DIR/test.out"
    [ "${PIPESTATUS[0]}" -eq 0 ] || rc=1
  else
    rc=1
  fi
else
  echo "the storage of shard 3 did not start" | tee "$DIR/test.out"; rc=1
fi
if [ "$rc" -eq 0 ]; then
  "$C" stop -d "$DIR"
  echo "JOIN TEST PASSED"
else
  KEEP=1 "$C" stop -d "$DIR"
  echo "JOIN TEST FAILED; logs kept in $DIR"
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    msg=$({ grep -E "FAIL|Error|Traceback" "$DIR/test.out" | head -20
            echo "--- tarn-server"; grep -h -iE "error|warn" "$DIR"/tarn*.log | tail -10; } |
          sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')
    echo "::error title=Join test failed::$msg"
  fi
fi
exit "$rc"
