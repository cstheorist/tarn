#!/bin/bash
# Positive fsync-WAL restart: a volatile clock fault must not shorten
# existing completion history. All data belongs to this new directory.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:?new private work directory required}
mkdir "$WORK"
for stage in write restart; do
  timeout 40 "$T" "$DIR/test/clock_completion_test.lua" "$WORK" "$stage"
done
