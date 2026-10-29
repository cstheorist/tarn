#!/bin/bash
# Dedicated two-shard resource-policy checks; existing clusters are untouched.
# TARN_RESOURCE_REF must name MariaDB 10.11. Output is JSONL measurements.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARN=${TARN_BIN:-$ROOT/target/release/tarn-server}
export TARN_BIN=$TARN
W=$(mktemp -d /tmp/tarn-resources.XXXX)
PORT=${TARN_RESOURCE_PORT:-4700}
cleanup() {
  for name in bounded transaction work materialized matched; do
    [ ! -f "$W/$name.pid" ] || kill "$(cat "$W/$name.pid")" 2>/dev/null || true
  done
  bash "$ROOT/scripts/cluster.sh" stop -d "$W/cluster" >/dev/null 2>&1 || true
}
trap cleanup EXIT
TARN_BIN=$TARN TARN_BASE_PORT=${TARN_RESOURCE_BASE:-3700} TARN_TEST_HOOKS=1 TARN_CHECK_LOCKS=1 \
  bash "$ROOT/scripts/cluster.sh" start -n 2 -r 1 -s 1 -p "$PORT" -d "$W/cluster" -m 256 -t 2 -P 2 > "$W/start.log" 2>&1
python3 - "$W" <<'PY'
import json, pathlib, sys, hashlib, os
root=pathlib.Path(sys.argv[1]); base=json.loads((root/'cluster/cluster.json').read_text())
common=dict(query_bytes=512<<20,transaction_bytes=64<<20,server_bytes=2<<30,query_work=1000000000,server_work=4000000000)
profiles={
 'control':common,
 'bounded':dict(query_bytes=64<<20,server_bytes=96<<20),
 'transaction':dict(transaction_bytes=128<<10),
 'work':dict(query_work=20000),
 'materialized':dict(common,topk=False),
 'matched':common,
}
print(json.dumps(dict(kind='manifest', profiles=profiles, shards=2, replicas=1, wal='write',
 build=os.environ.get('TARN_RESOURCE_BUILD','unspecified'),
 executable_sha256=hashlib.sha256(pathlib.Path(os.environ['TARN_BIN']).read_bytes()).hexdigest())), flush=True)
for name, limits in profiles.items():
 config=dict(base,resources=limits)
 (root/(name+'.json')).write_text(json.dumps(config))
PY
# Restart only the owned control gateway with caps matched to materialized.
kill "$(cat "$W/cluster/tarn1.pid")"
TARN_LOG=warn "$TARN" --config "$W/control.json" --listen "127.0.0.1:$PORT" --node-id 1 --threads 2 --pool 2 > "$W/control.log" 2>&1 &
echo $! > "$W/cluster/tarn1.pid"
i=1
for name in bounded transaction work materialized matched; do
  TARN_LOG=warn "$TARN" --config "$W/$name.json" --listen "127.0.0.1:$((PORT+i))" --node-id "$((i+1))" --threads 2 --pool 2 > "$W/$name.log" 2>&1 &
  echo $! > "$W/$name.pid"
  i=$((i+1))
done
for p in $(seq "$PORT" "$((PORT+5))"); do
  for _ in $(seq 100); do (exec 3<>/dev/tcp/127.0.0.1/$p) 2>/dev/null && break; sleep .1; done
done
set +e
python3 "$ROOT/tests/sql/resourcetest.py" --tarn "127.0.0.1:$PORT" --bounded "127.0.0.1:$((PORT+1))" \
  --transaction "127.0.0.1:$((PORT+2))" --work "127.0.0.1:$((PORT+3))" --materialized "127.0.0.1:$((PORT+4))" \
  --matched "127.0.0.1:$((PORT+5))" --matched-pid "$(cat "$W/matched.pid")" \
  --ref "${TARN_RESOURCE_REF:?set TARN_RESOURCE_REF to MariaDB 10.11}" \
  --pid "$(cat "$W/cluster/tarn1.pid")" --bounded-pid "$(cat "$W/bounded.pid")" --materialized-pid "$(cat "$W/materialized.pid")"
rc=$?
set -e
if grep -q 'lock copy differs' "$W/cluster"/*.log; then rc=1; fi
if [ -n "${TARN_RESOURCE_EVIDENCE:-}" ]; then cp -R "$W" "$TARN_RESOURCE_EVIDENCE"; fi
[ "$rc" = 0 ] || echo "resource test logs: $W" >&2
exit "$rc"
