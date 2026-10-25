#!/bin/bash
# Schema changes interrupted by a shard failure (issue #30): CREATE TABLE,
# DROP TABLE and TRUNCATE take effect on every shard or on none. Starts a
# 2-shard cluster with test hooks and ddl_job_stale = 2 s, so that abandoned
# jobs are finished within seconds, and runs tests/sql/ddlfaults.py.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=${DIR:-/tmp/tarn-ddlfaults}
BASE=${BASE:-3800}
PORT=${PORT:-4800}
"$ROOT/scripts/cluster.sh" stop -d "$DIR" 2>/dev/null
TARN_TEST_HOOKS=1 TARN_DDL_JOB_STALE=2 \
  "$ROOT/scripts/cluster.sh" start -n 2 -r 1 -d "$DIR" -b "$BASE" -p "$PORT" -m 256 || exit 1
python3 "$ROOT/tests/sql/ddlfaults.py" --tarn "127.0.0.1:$PORT" \
  --shards "127.0.0.1:$((BASE + 11)),127.0.0.1:$((BASE + 21))" --dir "$DIR" | tee "$DIR/test.out"
rc=${PIPESTATUS[0]}
if [ "$rc" -eq 0 ]; then
  "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "DDL FAULT TEST PASSED"
else
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "DDL FAULT TEST FAILED; logs kept in $DIR"
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    msg=$({ grep -E "FAIL|EXCEPTION" "$DIR/test.out" | head -20
            echo "--- tarn-server"; grep -h -iE "error|warn" "$DIR"/tarn*.log | tail -10
            echo "--- storage"; grep -h -iE "error|warn" "$DIR"/s*.log | tail -10; } |
          sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')
    echo "::error title=DDL fault test failed::$msg"
  fi
  exit 1
fi
