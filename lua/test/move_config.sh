#!/bin/bash
# Invalid movement settings must fail before Tarn creates any schema.
set -eu
T=${TARANTOOL:-tarantool}
DIR=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:?new private work directory required}
mkdir "$WORK"
for control in text negative slots aggregate; do
  case "$control" in
    text) setting=TARN_MOVE_BATCH_BYTES=invalid ;;
    negative) setting=TARN_MOVE_BATCH_ROWS=-1 ;;
    slots) setting=TARN_MOVE_SLOTS=65 ;;
    aggregate) setting=TARN_MOVE_TOTAL_DELTA_BYTES=1 ;;
  esac
  if timeout 10 env "$setting" TARN_NATIVE=0 TARN_LISTEN=127.0.0.1:0 TARN_WORKDIR="$WORK/$control" \
       TARN_REPLICAS='[]' TARN_PEERS='{}' "$T" "$DIR/storage.lua" > "$WORK/$control.log" 2>&1; then
    echo "FAIL invalid movement configuration accepted: $control"; exit 1
  fi
  grep -Eq 'Invalid movement limit|Movement limits exceed supported bounds' "$WORK/$control.log"
  MOVE_CONFIG_WORK="$WORK/$control" timeout 10 "$T" -e "box.cfg{work_dir=os.getenv('MOVE_CONFIG_WORK'),read_only=true,log_level=3}; assert(box.space._tarn_catalog==nil and box.space._tarn_move==nil); os.exit(0)" > "$WORK/$control-reopen.log" 2>&1
  echo "PASS invalid movement configuration refused: $control"
done
