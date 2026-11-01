#!/bin/bash
# The election watchdog (#254) on one shard of three replicas: no replica
# stands while the shard has a leader, nor without its quorum, and the
# shard has a leader again once its replicas are back
# (tests/election_test.lua). Owns every process it starts.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
DIR=$(mktemp -d /tmp/tarn-election.XXXX)
BASE=${BASE:-5600}; PORT=${PORT:-5700}
cleanup() {
  rc=$?
  for f in "$DIR"/s1.*.pid; do [ -f "$f" ] && kill -CONT "$(cat "$f")" 2>/dev/null || true; done
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR" >/dev/null 2>&1 || true
  if [ "$rc" = 0 ]; then rm -rf "$DIR"; else echo "election test logs: $DIR"; fi
}
trap cleanup EXIT
"$ROOT/scripts/cluster.sh" start -d "$DIR" -n 1 -r 3 -s 0 -b "$BASE" -p "$PORT" -m 256
"${TARANTOOL:-tarantool}" "$ROOT/tests/election_test.lua" "$DIR" \
  "127.0.0.1:$((BASE + 11))" "127.0.0.1:$((BASE + 12))" "127.0.0.1:$((BASE + 13))"
