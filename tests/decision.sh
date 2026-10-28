#!/bin/bash
# Three-shard decision placement, a real lost PREPARE reply, and a killed
# coordinator with a durable YES on a non-first decision writer. Owns every
# process it starts; ENGINE=vinyl and TARN_NATIVE=0 cover the fallback paths.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARN=${TARN_BIN:-$ROOT/target/release/tarn-server}
DIR=$(mktemp -d /tmp/tarn-decision.XXXX)
BASE=${BASE:-5400}; PORT=${PORT:-5500}; PP=${PP:-5440}
export TARN_TEST_HOOKS=1 TARN_CHECK_LOCKS=1
cleanup() {
  rc=$?
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR" >/dev/null 2>&1 || true
  if [ "$rc" = 0 ]; then rm -rf "$DIR"; else echo "decision test logs: $DIR"; fi
}
trap cleanup EXIT
"$ROOT/scripts/cluster.sh" start -d "$DIR" -n 3 -r 1 -s 0 -b "$BASE" -p "$PORT" -m 256
TARN_PROXY_ARM_FILE="$DIR/armed" python3 "$ROOT/tests/proxy.py" "$PP" "$((BASE + 31))" tarn_prepare \
  > "$DIR/proxy.log" 2>&1 &
echo $! > "$DIR/proxy.pid"
python3 - "$DIR/cluster.json" "$PP" <<'PY'
import json, pathlib, sys
p = pathlib.Path(sys.argv[1])
c = json.loads(p.read_text())
c['shards'][2]['replicas'] = ['127.0.0.1:' + sys.argv[2]]
p.write_text(json.dumps(c))
PY
for k in 1 2; do
  {
    printf 'exec %q --config %q --listen 127.0.0.1:%s --node-id %s --allow-all' \
      "$TARN" "$DIR/cluster.json" "$((PORT + k - 1))" "$k"
    [ -z "${ENGINE:-}" ] || printf ' --default-engine %q' "$ENGINE"
    printf '\n'
  } > "$DIR/tarn$k.sh"
  bash "$DIR/tarn$k.sh" > "$DIR/tarn$k.log" 2>&1 &
  echo $! > "$DIR/tarn$k.pid"
done
for port in "$PORT" "$((PORT + 1))"; do
  ready=0
  for _ in $(seq 100); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$port") 2>/dev/null; then ready=1; break; fi
    sleep .1
  done
  [ "$ready" = 1 ] || { cat "$DIR"/tarn*.log; exit 1; }
done
python3 "$ROOT/tests/sql/decisiontest.py" --tarn "127.0.0.1:$PORT,127.0.0.1:$((PORT + 1))" \
  --shards "127.0.0.1:$((BASE + 11)),127.0.0.1:$((BASE + 21)),127.0.0.1:$((BASE + 31))" \
  --cluster-dir "$DIR" --proxy-arm-file "$DIR/armed"
grep -q 'swallowed the reply' "$DIR/proxy.log" || { cat "$DIR/proxy.log"; exit 1; }
if grep -h 'lock copy differs' "$DIR"/s*.log | head -3 | grep .; then exit 1; fi
