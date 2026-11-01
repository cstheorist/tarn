#!/bin/bash
# ALTER TABLE adds indexes to a vinyl table on one shard of three replicas
# while writes go on (#254, #263): every replica applies the builds and
# keeps up, and each index holds every row (tests/vinylbuild_test.lua).
# Owns every process it starts.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=$(mktemp -d /tmp/tarn-vinylbuild.XXXX)
BASE=${BASE:-5800}; PORT=${PORT:-5900}
cleanup() {
  rc=$?
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR" >/dev/null 2>&1 || true
  if [ "$rc" = 0 ]; then rm -rf "$DIR"; else echo "vinyl build test logs: $DIR"; fi
}
trap cleanup EXIT
"$ROOT/scripts/cluster.sh" start -d "$DIR" -n 1 -r 3 -s 0 -b "$BASE" -p "$PORT" -m 256 -w fsync
"${TARANTOOL:-tarantool}" "$ROOT/tests/vinylbuild_test.lua" "$DIR" \
  "127.0.0.1:$((BASE + 11))" "127.0.0.1:$((BASE + 12))" "127.0.0.1:$((BASE + 13))"
