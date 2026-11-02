#!/usr/bin/env bash
# Check the real guard, then require the original scalar body and two unsafe
# substitutions to fail the focused suite at their intended assertions.
set -euo pipefail
root=$(cd "$(dirname "$0")/../.." && pwd)
work=${1:?owned empty work directory required}
mkdir -p "$work"
test_script="$root/lua/test/rc_plan_guard_test.lua"
timeout 60 "${TARANTOOL:-tarantool}" "$test_script" "$work/normal" >"$work/normal.log" 2>&1
for mutant in scalar skip-last stale-mode; do
  case "$mutant" in
    scalar) expected='one current mode and controls scan per final check' ;;
    skip-last) expected='last asynchronous plan rejected' ;;
    stale-mode) expected='first asynchronous plan rejected' ;;
  esac
  status=0
  RC_PLAN_GUARD_MUTANT="$mutant" timeout 60 "${TARANTOOL:-tarantool}" \
    "$test_script" "$work/$mutant" >"$work/$mutant.log" 2>&1 || status=$?
  if [ "$status" != 1 ] || ! grep -Fq ": $expected" "$work/$mutant.log"; then
    cat "$work/$mutant.log"
    echo "unexpected $mutant control result: $status" >&2
    exit 1
  fi
  echo "$mutant: rejected at intended assertion"
done
tail -n 1 "$work/normal.log"
