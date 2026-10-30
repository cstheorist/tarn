#!/bin/bash
# Own one fresh RF3/fsync fixture; preserve every result and failed database.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
WORK=${1:?new evidence directory required}
ENGINE=${ENGINE:-memtx}
case "$ENGINE" in memtx|vinyl) ;; *) exit 1;; esac
mkdir "$WORK"
BASE=${TARN_BASE_PORT:-43000}
PORT=${LOADGEN_SQL_PORT:-4300}
for port in "$PORT" "$((BASE+11))" "$((BASE+12))" "$((BASE+13))" "$((BASE+21))" "$((BASE+22))" "$((BASE+23))"; do
  if (exec 3<>"/dev/tcp/127.0.0.1/$port") 2>/dev/null; then
    echo "Port $port is occupied; no existing service changed" >&2; exit 1
  fi
done
export TARN_BASE_PORT="$BASE" TARN_BIN=${TARN_BIN:-$ROOT/target/release/tarn-server}
export TARN_NATIVE=${TARN_NATIVE:-$ROOT/target/release/libtarn_storage.so}
MODE=enabled; [ "$TARN_NATIVE" != 0 ] || MODE=disabled
test -f "$TARN_BIN"
if [ "$TARN_NATIVE" != 0 ]; then test -f "$TARN_NATIVE"; fi
sha256sum "$TARN_BIN" "$ROOT/bench/loadgen.py" > "$WORK/inputs.sha256"
if [ "$TARN_NATIVE" != 0 ]; then sha256sum "$TARN_NATIVE" >> "$WORK/inputs.sha256"; fi
trap 'KEEP=1 bash "$ROOT/scripts/cluster.sh" stop -d "$WORK/cluster" > "$WORK/cleanup.log" 2>&1' EXIT
TARN_VINYL_CACHE_MB=0 TARN_CHECK_LOCKS=1 TARN_CHECK_YIELDS=1 \
  bash "$ROOT/scripts/cluster.sh" start -d "$WORK/cluster" -n 2 -r 3 -w fsync -m 96 -p "$PORT" -e "$ENGINE" > "$WORK/startup.log" 2>&1
ARGS=(--database tarn_load_smoke --endpoint "127.0.0.1:$PORT" --engine "$ENGINE" --rows 32 --row-bytes 256)
python3 "$ROOT/bench/loadgen.py" prepare "${ARGS[@]}" --out "$WORK/prepare"
tarantool "$ROOT/bench/loadgen_runtime.lua" "$BASE" tarn_load_smoke "$ENGINE" "$MODE" > "$WORK/runtime-before.json"
python3 "$ROOT/bench/loadgen.py" run "${ARGS[@]}" --out "$WORK/point" --workload point \
  --warmup .2 --seconds 1 --rate 20 --workers 2 --p99-ms 10000 --max-error-rate 1
python3 "$ROOT/bench/loadgen.py" run "${ARGS[@]}" --out "$WORK/transaction" --keys-per-transaction 2 \
  --warmup .2 --seconds 1 --rate 20 --workers 4 --p99-ms 10000 --max-error-rate 1
python3 "$ROOT/bench/loadgen.py" run "${ARGS[@]}" --out "$WORK/hot" --keys-per-transaction 4 --distribution hot \
  --isolation READ-COMMITTED --warmup .2 --seconds 1 --rate 20 --workers 4 --p99-ms 10000 --max-error-rate 1
set +e
python3 "$ROOT/bench/loadgen.py" run "${ARGS[@]}" --out "$WORK/overload" --keys-per-transaction 2 \
  --warmup .2 --seconds 1 --rate 5000 --workers 1 --queue 1 --p99-ms 50 --max-error-rate 0
status=$?
set -e
test "$status" = 2
python3 - "$WORK/overload/receipt.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1])); assert r['complete'] and r['audit']['exact_effects']
assert r['audit']['counts']['queue_full']>0 and not r['audit']['objectives_pass']
assert r['audit']['all_counts'].get('unknown',0)==0
PY
for phase in point transaction hot; do
  python3 "$ROOT/bench/loadgen.py" audit --database tarn_load_smoke --out "$WORK/$phase" > "$WORK/$phase-audit.json"
done
set +e
python3 "$ROOT/bench/loadgen.py" audit --database tarn_load_smoke --out "$WORK/overload" > "$WORK/overload-audit.json"
status=$?
set -e
test "$status" = 2
tarantool "$ROOT/bench/loadgen_runtime.lua" "$BASE" tarn_load_smoke "$ENGINE" "$MODE" > "$WORK/runtime-after.json"
echo "Scheduled-arrival and exact-effect controls passed: $ENGINE/$MODE (functional only)"
