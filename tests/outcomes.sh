#!/bin/bash
# Isolated RF3/fsync outcome contract. Retain exact source/config/logs even
# on success; every process and fault plan belongs to this fixture.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARN=${TARN_BIN:-$ROOT/target/release/tarn-server}
T=${TARANTOOL:-tarantool}
W=${OUTCOME_DIR:-$(mktemp -d /tmp/tarn-outcomes.XXXX)}
BASE=${BASE:-6200}; PORT=${PORT:-6300}; PP=${PP:-6240}
ENGINE=${ENGINE:-memtx}; R=${R:-3}
mkdir -p "$W"
cleanup() {
  rc=$?
  rm -f "$W/plan.json"
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$W" > "$W/stop.log" 2>&1 || true
  echo "outcome evidence: $W (exit $rc)"
}
trap cleanup EXIT
export TARN_TEST_HOOKS=1 TARN_CHECK_LOCKS=1 TARN_CHECK_YIELDS=1 TARN_BUCKET_COUNT=32
export TARN_BIN="$TARN"
"$ROOT/scripts/cluster.sh" start -d "$W" -n 2 -r "$R" -s 0 -b "$BASE" -p "$PORT" -w fsync -m 256 > "$W/start.log" 2>&1
nodes=()
for shard in 1 2; do
  found=
  for replica in $(seq "$R"); do
    port=$((BASE + shard * 10 + replica))
    if "$T" "$ROOT/scripts/probe.lua" "tarn:tarn@127.0.0.1:$port" </dev/null >/dev/null 2>&1; then
      found=$port; break
    fi
  done
  [ -n "$found" ] || { echo "no leader for shard $shard"; exit 1; }
  nodes+=("127.0.0.1:$found")
done
python3 "$ROOT/tests/outcome_proxy.py" --listen "$PP" --upstream "${nodes[0]##*:}" \
  --plan "$W/plan.json" > "$W/proxy.jsonl" 2> "$W/proxy.err" &
echo $! > "$W/proxy.pid"
ready=0
for _ in $(seq 100); do
  kill -0 "$(cat "$W/proxy.pid")" 2>/dev/null || { cat "$W/proxy.err"; exit 1; }
  if (exec 3<>/dev/tcp/127.0.0.1/"$PP") 2>/dev/null; then ready=1; break; fi
  sleep .05
done
[ "$ready" = 1 ] || { cat "$W/proxy.err"; echo 'proxy did not listen'; exit 1; }
python3 - "$W/cluster.json" "$PP" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1]); c = json.loads(p.read_text())
c['shards'][0]['replicas'] = ['127.0.0.1:' + sys.argv[2]]
c['rpc'] = {'timeout_secs': 5.0}
p.write_text(json.dumps(c, indent=2)+'\n')
PY
"$TARN" --config "$W/cluster.json" --listen "127.0.0.1:$PORT" --allow-all --threads 2 \
  --default-engine "$ENGINE" > "$W/gateway.log" 2>&1 &
echo $! > "$W/gateway.pid"
ready=0
for _ in $(seq 100); do
  if (exec 3<>/dev/tcp/127.0.0.1/"$PORT") 2>/dev/null; then ready=1; break; fi
  sleep .1
done
[ "$ready" = 1 ] || { cat "$W/gateway.log"; exit 1; }
python3 - "$ROOT" "$W" "$TARN" "$ENGINE" "$R" <<'PY'
import hashlib, json, os, pathlib, subprocess, sys
root, dest, binary = map(pathlib.Path, sys.argv[1:4])
native = pathlib.Path(os.environ.get('TARN_NATIVE', str(root/'target/release/libtarn_storage.so')))
hashfile = lambda p: hashlib.sha256(p.read_bytes()).hexdigest()
files = ['contracts/outcomes.json','tests/outcome_proxy.py','tests/sql/outcomes.py','tests/sql/outcome_client.py',
         'lua/tarn.lua','crates/tarn-sql/src/mpw.rs','crates/tarn-sql/src/txn.rs','crates/tarn-sql/src/session.rs']
receipt = {'source': subprocess.check_output(['git','-C',str(root),'rev-parse','HEAD'],text=True).strip(),
           'diff_sha256': hashlib.sha256(subprocess.check_output(['git','-C',str(root),'diff','HEAD'])).hexdigest(),
           'files': {p:hashfile(root/p) for p in files}, 'binary_sha256':hashfile(binary),
           'native': str(native), 'native_sha256':hashfile(native) if native.is_file() else None,
           'rustc':subprocess.check_output(['rustc','--version'],text=True).strip(),
           'engine':sys.argv[4], 'replicas':int(sys.argv[5]), 'wal_mode':'fsync',
           'configuration':json.loads((dest/'cluster.json').read_text())}
(dest/'source.json').write_text(json.dumps(receipt,indent=2)+'\n')
PY
extra=(); [ -z "${OUTCOME_MODE:-}" ] || extra=(--mode "$OUTCOME_MODE")
python3 "$ROOT/tests/sql/outcomes.py" --tarn "127.0.0.1:$PORT" --shards "${nodes[0]},${nodes[1]}" \
  --engine "$ENGINE" --plan "$W/plan.json" --proxy-log "$W/proxy.jsonl" --evidence "$W/results" "${extra[@]}"
if grep -h 'lock copy differs\|yielded inside' "$W"/*.log; then exit 1; fi
echo 'OUTCOME CONTRACT PASSED'
