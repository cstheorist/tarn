#!/bin/bash
# Bucket moves whose SQL server dies (issue #24): another server gives the
# move up, or finishes it. Starts a 2-shard cluster with three SQL servers,
# test hooks and ddl_job_stale = 2 s, so that abandoned moves are taken
# over within seconds, and runs tests/sql/movefaults.py.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=${DIR:-/tmp/tarn-movefaults}
BASE=${BASE:-3900}
PORT=${PORT:-4900}
"$ROOT/scripts/cluster.sh" stop -d "$DIR" 2>/dev/null
TARN_TEST_HOOKS=1 TARN_DDL_JOB_STALE=2 \
  "$ROOT/scripts/cluster.sh" start -n 2 -r 1 -s 3 -d "$DIR" -b "$BASE" -p "$PORT" -m 256 || exit 1
python3 "$ROOT/tests/sql/movefaults.py" --tarn "127.0.0.1:$PORT,127.0.0.1:$((PORT + 1)),127.0.0.1:$((PORT + 2))" \
  --shards "127.0.0.1:$((BASE + 11)),127.0.0.1:$((BASE + 21))" --dir "$DIR" | tee "$DIR/test.out"
rc=${PIPESTATUS[0]}
if [ "$rc" -eq 0 ]; then
  "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "MOVE FAULT TEST PASSED"
else
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "MOVE FAULT TEST FAILED; logs kept in $DIR"
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    msg=$({ grep -E "FAIL|Error|Traceback" "$DIR/test.out" | head -20
            echo "--- tarn-server"; grep -h -iE "error|warn" "$DIR"/tarn*.log | tail -10
            echo "--- storage"; grep -h -iE "error|warn" "$DIR"/s*.log | tail -10; } |
          sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')
    echo "::error title=Move fault test failed::$msg"
  fi
  exit 1
fi
