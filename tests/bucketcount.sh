#!/bin/bash
# The number of buckets is chosen when a cluster is made (issue #24): the
# configured count, 3,000 for a cluster made by a version from before the
# bucket map, 30,000 for a new one. Starts a 2-shard cluster whose
# configuration sets bucket_count = 3000 and runs tests/sql/bucketcount.py,
# which removes the map and restarts the nodes to make the other cases.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=${DIR:-/tmp/tarn-bucketcount}
BASE=${BASE:-4300}
PORT=${PORT:-4950}
"$ROOT/scripts/cluster.sh" stop -d "$DIR" 2>/dev/null
TARN_BUCKET_COUNT=3000 "$ROOT/scripts/cluster.sh" start -n 2 -r 1 -d "$DIR" -b "$BASE" -p "$PORT" -m 256 || exit 1
python3 "$ROOT/tests/sql/bucketcount.py" --tarn "127.0.0.1:$PORT" \
  --shards "127.0.0.1:$((BASE + 11)),127.0.0.1:$((BASE + 21))" --dir "$DIR" | tee "$DIR/test.out"
rc=${PIPESTATUS[0]}
if [ "$rc" -eq 0 ]; then
  "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "BUCKET COUNT TEST PASSED"
else
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "BUCKET COUNT TEST FAILED; logs kept in $DIR"
  if [ "${GITHUB_ACTIONS:-}" = true ]; then
    msg=$({ grep -E "FAIL|Error|Traceback" "$DIR/test.out" | head -20
            echo "--- tarn-server"; grep -h -iE "error|warn" "$DIR"/tarn*.log | tail -10; } |
          sed 's/%/%25/g; s/\r//g' | sed ':a;N;$!ba;s/\n/%0A/g')
    echo "::error title=Bucket count test failed::$msg"
  fi
  exit 1
fi
