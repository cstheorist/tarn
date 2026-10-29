#!/bin/bash
# Real storage commits updates while a TCP proxy withholds their replies.
# Exercise each admission scope with 24 concurrent MySQL sessions. Own all
# started processes; ENGINE=vinyl and TARN_NATIVE=0 cover fallback storage.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARN=${TARN_BIN:-$ROOT/target/release/tarn-server}
W=${RPC_DIR:-$(mktemp -d /tmp/tarn-rpcs.XXXX)}
mkdir -p "$W"
BASE=${BASE:-5800}; PP=${PP:-5840}; QP=${QP:-5900}
cleanup() {
  rc=$?
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$W" >/dev/null 2>&1 || true
  [ -z "${proxy_pid:-}" ] || { kill "$proxy_pid" 2>/dev/null || true; wait "$proxy_pid" 2>/dev/null || true; }
  if [ "$rc" = 0 ] && [ -z "${RPC_DIR:-}" ]; then rm -rf "$W"; else echo "RPC test logs: $W"; fi
}
trap cleanup EXIT
export TARN_CHECK_LOCKS=1 TARN_CHECK_YIELDS=1
"$ROOT/scripts/cluster.sh" start -d "$W" -n 1 -r "${R:-1}" -w "${WAL:-write}" -s 0 -b "$BASE" -p "$QP" -m 256 > "$W/start.log" 2>&1
leader=
for replica in $(seq "${R:-1}"); do
  port=$((BASE + 10 + replica))
  if "${TARANTOOL:-tarantool}" "$ROOT/scripts/probe.lua" "tarn:tarn@127.0.0.1:$port" </dev/null >/dev/null 2>&1; then
    leader=$port; break
  fi
done
[ -n "$leader" ] || { echo 'no writable RPC fixture leader'; exit 1; }
TARN_FAULT_REPLY=stall TARN_FAULT_COUNT=1000000 TARN_PROXY_ARM_FILE="$W/armed" \
  python3 "$ROOT/tests/proxy.py" "$PP" "$leader" autocommit update > "$W/proxy.log" 2>&1 &
proxy_pid=$!
ready=0
for _ in $(seq 100); do
  kill -0 "$proxy_pid" 2>/dev/null || { cat "$W/proxy.log"; exit 1; }
  if (exec 3<>/dev/tcp/127.0.0.1/"$PP") 2>/dev/null; then ready=1; break; fi
  sleep .05
done
[ "$ready" = 1 ] || { cat "$W/proxy.log"; echo 'proxy did not listen'; exit 1; }
for scope in connection shard global; do
  python3 - "$W/cluster.json" "$PP" "$scope" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]); c=json.loads(p.read_text()); scope=sys.argv[3]
c['shards'][0]['replicas']=['127.0.0.1:'+sys.argv[2]]
c['pool_size']=1 if scope=='connection' else 2
c['rpc']={'timeout_secs':1.0,
    'connection':{'requests':3 if scope=='connection' else 16,'reserved_requests':1,'bytes':262144,'reserved_bytes':65536},
    'shard':{'requests':6,'reserved_requests':2,'bytes':524288,'reserved_bytes':131072},
    'global':{'requests':4 if scope=='global' else 8,'reserved_requests':1 if scope=='global' else 2,'bytes':1048576,'reserved_bytes':262144}}
p.write_text(json.dumps(c))
PY
  cp "$W/cluster.json" "$W/config-$scope.json"
  extra=(); [ -z "${ENGINE:-}" ] || extra=(--default-engine "$ENGINE")
  "$TARN" --config "$W/cluster.json" --listen "127.0.0.1:$QP" --node-id 1 --threads 2 --allow-all "${extra[@]}" > "$W/tarn-$scope.log" 2>&1 &
  echo $! > "$W/tarn1.pid"
  ready=0
  for _ in $(seq 100); do
    if (exec 3<>/dev/tcp/127.0.0.1/"$QP") 2>/dev/null; then ready=1; break; fi
    sleep .1
  done
  [ "$ready" = 1 ] || { cat "$W/tarn-$scope.log"; exit 1; }
  python3 "$ROOT/tests/sql/rpctest.py" --tarn "127.0.0.1:$QP" --config "$W/cluster.json" --arm "$W/armed" --scope "$scope" --client "${RPC_CLIENT:-pymysql}" --evidence "$W/results-$scope.json"
  kill "$(cat "$W/tarn1.pid")"
  wait "$(cat "$W/tarn1.pid")" 2>/dev/null || true
  rm "$W/tarn1.pid"
done
RESERVE_PROBE=${TARN_RPC_PROBE:-$ROOT/target/release/examples/rpc_reserve}
[ -x "$RESERVE_PROBE" ] || { echo 'build the probe: cargo build --release -p tarn-sql --example rpc_reserve'; exit 1; }
"$RESERVE_PROBE" "$W/cluster.json"
grep -q 'withholding replies on an open connection' "$W/proxy.log"
if grep -h 'lock copy differs\|yielded inside' "$W"/*.log | head -3 | grep .; then exit 1; fi
echo 'ALL RPC SCOPE TESTS PASSED'
