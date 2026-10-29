#!/bin/bash
set -eu
T=${TARANTOOL:-tarantool}
HERE=$(cd "$(dirname "$0")" && pwd)
WORK=${1:-$(mktemp -d /tmp/tarn-clock-integration.XXXX)}
mkdir -p "$WORK"
pids=()
cleanup() {
  for pid in "${pids[@]}"; do kill "$pid" 2>/dev/null || true; done
  for pid in "${pids[@]}"; do wait "$pid" 2>/dev/null || true; done
}
trap cleanup EXIT INT TERM
for id in 1 2; do
  "$T" "$HERE/clock_integration_node.lua" "$id" "$WORK/s$id" >"$WORK/s$id.log" 2>&1 &
  pids+=("$!")
done
rc=0
timeout 30 "$T" "$HERE/clock_integration_test.lua" >"$WORK/test.log" 2>&1 || rc=$?
cat "$WORK/test.log"
if [ "$rc" -ne 0 ]; then cat "$WORK/s1.log" "$WORK/s2.log"; fi
exit "$rc"
