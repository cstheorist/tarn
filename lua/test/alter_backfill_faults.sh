#!/bin/bash
# Private no-listener processes: exercise upgrade and real WAL recovery.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")" && pwd)
WORK=${1:?private test work directory required}
for stage in seed upgrade reopen; do
  timeout 30 $T "$DIR/alter_upgrade_test.lua" "$WORK/upgrade" "$stage"
done
for engine in memtx vinyl; do
  for stage in crash_pad crash_undo; do
    set +e
    timeout 30 $T "$DIR/alter_backfill_restart_test.lua" "$WORK/restart-$engine" "$engine" "$stage"
    rc=$?
    set -e
    if [ "$rc" -ne 137 ]; then
      echo "FAIL $engine $stage: expected SIGKILL exit 137, got $rc"
      exit 1
    fi
  done
  timeout 30 $T "$DIR/alter_backfill_restart_test.lua" "$WORK/restart-$engine" "$engine" finish
  set +e
  timeout 30 $T "$DIR/alter_rename_restart_test.lua" "$WORK/rename-$engine" "$engine" crash
  rc=$?
  set -e
  if [ "$rc" -ne 137 ]; then
    echo "FAIL $engine native rename: expected SIGKILL exit 137, got $rc"
    exit 1
  fi
  timeout 30 $T "$DIR/alter_rename_restart_test.lua" "$WORK/rename-$engine" "$engine" finish
done
