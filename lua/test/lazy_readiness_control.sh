#!/bin/sh
# Test-only TARANTOOL wrapper: clients execute the ordinary binary. Only
# storage.lua launched by this owned RF3 controller receives the preload.
set -eu
HERE=$(CDPATH= cd -- "$(dirname -- "$0")" && pwd)
SOURCE=$(CDPATH= cd -- "$HERE/.." && pwd)
REAL=${LAZY_RF3_REAL_TARANTOOL:-tarantool}
case "$REAL" in /*) ;; *) REAL=$(command -v "$REAL");; esac
[ "$REAL" != "$0" ] || { echo 'FAIL recursive status-control wrapper' >&2; exit 1; }
if [ "${1:-}" = "$SOURCE/storage.lua" ]; then
  [ "${TARN_SHARD_ID:-}" = 1 ] && [ "${TARN_TEST_HOOKS:-}" = 1 ] \
    && [ "${TARN_WAL_MODE:-}" = fsync ] \
    || { echo 'FAIL status-control wrapper refused non-fixture storage' >&2; exit 1; }
  RUNTIME=$(dirname -- "${TARN_WORKDIR:?fixture workdir required}")
  [ "$(cat "$RUNTIME/../owner")" = lazy-replication-rf3 ] \
    && tr '\000' '\n' < "/proc/$PPID/cmdline" | grep -Fxq "$HERE/lazy_replication.sh" \
    || { echo 'FAIL status-control wrapper refused owner/controller mismatch' >&2; exit 1; }
  export LAZY_RF3_STATUS_PRELOAD="$HERE/lazy_readiness_control.lua"
  exec "$REAL" -e 'dofile(os.getenv("LAZY_RF3_STATUS_PRELOAD"))' "$@"
fi
if [ "${1:-}" = "$HERE/lazy_replication_test.lua" ] && [ "${LAZY_RF3_BASELINE_CONTROL:-0}" = 1 ]; then
  # A private original-fixture copy with ONLY notification-release
  # instrumentation, retained separately from the fixed test.
  shift
  exec "$REAL" "$HERE/lazy_readiness_baseline_test.lua" "$@"
fi
exec "$REAL" "$@"
