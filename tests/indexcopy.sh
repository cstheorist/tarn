#!/bin/bash
# ALTER TABLE builds new indexes on replicated shards by copying the table
# into a space whose indexes exist before its rows (#267), on 2 shards x 3
# replicas with fsync (tests/sql/indexcopytest.py). Owns every process it
# starts.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=$(mktemp -d /tmp/tarn-indexcopy.XXXX)
BASE=${BASE:-6400}; PORT=${PORT:-6500}
cleanup() {
  rc=$?
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR" >/dev/null 2>&1 || true
  if [ "$rc" = 0 ]; then rm -rf "$DIR"; else
    echo "index copy test logs: $DIR"
    for f in "$DIR"/*.log; do [ ! -f "$f" ] || { echo "--- $f"; tail -n 30 "$f"; }; done
  fi
}
trap cleanup EXIT
# (A copy's BUILD and PREPARE in short slices: each takes several calls.
# Test hooks hold a shard's BUILD.)
TARN_DDL_STEP_SLICE=${TARN_DDL_STEP_SLICE:-0.05} TARN_TEST_HOOKS=1 \
  "$ROOT/scripts/cluster.sh" start -d "$DIR" -n 2 -r 3 -s 1 -b "$BASE" -p "$PORT" -m 256 -w fsync
python3 "$ROOT/tests/sql/indexcopytest.py" --tarn "127.0.0.1:$PORT" --cluster-dir "$DIR" "$@"
