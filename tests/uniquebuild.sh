#!/bin/bash
# ALTER TABLE adds a unique key without the shard key to a populated table,
# whose global index it builds online while writes go on (#292), on 2 shards
# of one replica each (tests/sql/uniquebuildtest.py). Owns every process it
# starts.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=$(mktemp -d /tmp/tarn-uniquebuild.XXXX)
BASE=${BASE:-6600}; PORT=${PORT:-6700}
cleanup() {
  rc=$?
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR" >/dev/null 2>&1 || true
  if [ "$rc" = 0 ]; then rm -rf "$DIR"; else
    echo "unique build test logs: $DIR"
    for f in "$DIR"/*.log; do [ ! -f "$f" ] || { echo "--- $f"; tail -n 30 "$f"; }; done
  fi
}
trap cleanup EXIT
# (Two SQL servers: the second takes the build over when the first stops.)
TARN_DDL_JOB_STALE=${TARN_DDL_JOB_STALE:-3} \
  "$ROOT/scripts/cluster.sh" start -d "$DIR" -n 2 -r 1 -s 2 -b "$BASE" -p "$PORT" -m 256 ${ENGINE:+-e "$ENGINE"}
python3 "$ROOT/tests/sql/uniquebuildtest.py" --tarn "127.0.0.1:$PORT,127.0.0.1:$((PORT + 1))" --cluster-dir "$DIR" "$@"
