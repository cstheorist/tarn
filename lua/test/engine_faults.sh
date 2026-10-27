#!/bin/bash
# Every SIGKILL is sent by the exact private Tarantool process to itself.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")" && pwd)
WORK=${1:?private engine fault work directory required}
for engine in memtx vinyl; do
  for scenario in copy prepared rename published; do
    data="$WORK/$engine-$scenario"
    set +e
    timeout 30 "$T" "$DIR/engine_restart_test.lua" "$data" "$engine" "$scenario" crash
    rc=$?
    set -e
    if [ "$rc" -ne 137 ]; then
      echo "FAIL $engine/$scenario: expected SIGKILL exit 137, got $rc"
      exit 1
    fi
    timeout 30 "$T" "$DIR/engine_restart_test.lua" "$data" "$engine" "$scenario" finish
    timeout 30 "$T" "$DIR/engine_restart_test.lua" "$data" "$engine" "$scenario" reopen
  done
done
