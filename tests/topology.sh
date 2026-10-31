#!/bin/bash
# Real group-count fixture; each group is a separate Tarantool process.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
GROUPS_COUNT=${GROUPS_COUNT:-8}
W=${TOPOLOGY_WORK:-$(mktemp -d /tmp/tarn-topology.XXXX)}
OUT=${TOPOLOGY_OUT:-$W/evidence}
mkdir -p "$W" "$OUT"
cleanup() {
  rc=$?
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$W/storage" > "$OUT/stop.log" 2>&1 || true
  echo "topology evidence: $OUT (exit $rc)"
}
trap cleanup EXIT
export TARN_BUCKET_COUNT=2048 TARN_VINYL_MEMORY_MB=16 TARN_VINYL_CACHE_MB=0
export TARN_CHECK_YIELDS=1 TARN_CHECK_LOCKS=1
"$ROOT/scripts/cluster.sh" start -n "$GROUPS_COUNT" -r "${REPLICAS:-1}" -s 0 \
  -b "${BASE:-25000}" -d "$W/storage" -w fsync -m 32 > "$OUT/start.log" 2>&1
args=()
[ -z "${BASELINE_BIN:-}" ] || args+=(--baseline "$BASELINE_BIN")
[ -z "${INCOMPATIBLE_BASELINE_BIN:-}" ] || args+=(--incompatible-baseline "$INCOMPATIBLE_BASELINE_BIN")
python3 "$ROOT/tests/sql/topologytest.py" --config "$W/storage/cluster.json" \
  --gateway "${TARN_BIN:-$ROOT/target/release/tarn-server}" --out "$OUT" \
  --port "${SQL_PORT:-26400}" --pairs "${PAIRS:-5}" --seconds "${SECONDS_PER_RUN:-2}" \
  --pool-size "${POOL_SIZE:-4}" --gateways "${GATEWAYS:-2}" --rate "${POINT_RATE:-200}" "${args[@]}"
