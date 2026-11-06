#!/bin/bash
# An online build of a global unique index on 2 shards x 3 replicas with
# fsync, through a leader crash during its copy and a lost reply during its
# backfill, and one that a duplicate written during its backfill fails
# (#292, tests/sql/uniquefaults.py), on memtx and on vinyl. Owns every
# process it starts.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=$(mktemp -d /tmp/tarn-uniquefaults.XXXX)
BASE=${BASE:-6800}; PORT=${PORT:-6900}
export TARN_TEST_HOOKS=1 TARN_CHECK_YIELDS=1 TARN_CHECK_LOCKS=1
cleanup() {
  rc=$?
  for engine in memtx vinyl; do
    KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR/$engine" >/dev/null 2>&1 || true
  done
  if [ "$rc" = 0 ]; then rm -rf "$DIR"; else
    echo "unique build fault logs: $DIR"
    for f in "$DIR"/*/*.log; do [ ! -f "$f" ] || { echo "--- $f"; tail -n 30 "$f"; }; done
  fi
}
trap cleanup EXIT
for engine in ${ENGINES:-memtx vinyl}; do
  "$ROOT/scripts/cluster.sh" start -n 2 -r 3 -w fsync -m 128 -d "$DIR/$engine" -b "$BASE" -p "$PORT" -e "$engine"
  python3 "$ROOT/tests/sql/uniquefaults.py" --tarn "127.0.0.1:$PORT" --cluster-dir "$DIR/$engine" \
    --engine "$engine" "$@"
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR/$engine"
done
