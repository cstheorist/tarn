#!/bin/bash
# TARN ADD SHARD and TARN REMOVE SHARD whose SQL server dies (issue #25):
# another server gives the join up, or finishes it; the removal goes on when
# run again. Starts a 2-shard cluster with three SQL servers, test hooks,
# ddl_job_stale = 2 s (abandoned jobs are taken over within seconds) and 480
# buckets (a shard drains in seconds), starts the storage of shard 3, and
# runs tests/sql/joinfaults.py.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=${DIR:-/tmp/tarn-joinfaults}
BASE=${BASE:-3600}
PORT=${PORT:-4980}
C="$ROOT/scripts/cluster.sh"
"$C" stop -d "$DIR" 2>/dev/null
export TARN_TEST_HOOKS=1 TARN_DDL_JOB_STALE=2 TARN_BUCKET_COUNT=480
"$C" start -n 2 -r 1 -s 3 -d "$DIR" -b "$BASE" -p "$PORT" -m 256 || exit 1
reps=$("$C" add-shard -d "$DIR" 3) || { "$C" stop -d "$DIR"; echo "the storage of shard 3 did not start"; exit 1; }
python3 "$ROOT/tests/sql/joinfaults.py" --tarn "127.0.0.1:$PORT,127.0.0.1:$((PORT + 1)),127.0.0.1:$((PORT + 2))" \
  --shards "127.0.0.1:$((BASE + 11)),127.0.0.1:$((BASE + 21))" --add "3=$reps" --dir "$DIR" | tee "$DIR/test.out"
rc=${PIPESTATUS[0]}
if [ "$rc" -eq 0 ]; then
  "$C" stop -d "$DIR"
  echo "JOIN FAULT TEST PASSED"
else
  KEEP=1 "$C" stop -d "$DIR"
  echo "JOIN FAULT TEST FAILED; logs kept in $DIR"
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    msg=$({ grep -E "FAIL|Error|Traceback" "$DIR/test.out" | head -20
            echo "--- tarn-server"; grep -h -iE "error|warn" "$DIR"/tarn*.log | tail -10
            echo "--- storage"; grep -h -iE "error|warn" "$DIR"/s*.log | tail -10; } |
          sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')
    echo "::error title=Join fault test failed::$msg"
  fi
  exit 1
fi
