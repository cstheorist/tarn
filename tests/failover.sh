#!/bin/bash
# Failover test: 2 shards x 3 synchronous replicas behind one tarn-server.
# Runs the concurrent bank-transfer workload (with serializable audits) and
# kill -9's the leader of shard 1, then of shard 2, while it runs. Passes if
# every audit saw the exact total, the final total is exact (via SQL and via
# direct Key/Value reads on the new leaders), and transfers kept committing
# after each failover.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
T=${TARANTOOL:-/opt/src/tarantool/build/src/tarantool}
DIR=${DIR:-/tmp/tarn-failover}
BASE=${BASE:-3500}
PORT=${PORT:-4100}
SECS=${SECS:-45}
"$ROOT/scripts/cluster.sh" stop -d "$DIR" 2>/dev/null
"$ROOT/scripts/cluster.sh" start -n 2 -r 3 -d "$DIR" -b "$BASE" -p "$PORT" -m 256 || exit 1

leader_of() {  # shard -> port of its current writable node
  local s=$1 j
  for j in 1 2 3; do
    local p=$((BASE + 10 * s + j))
    if "$T" "$ROOT/scripts/probe.lua" "tarn:tarn@127.0.0.1:$p" < /dev/null > /dev/null 2>&1; then echo "$p"; return 0; fi
  done
  return 1
}

check_alive() {
  local f dead=""
  for f in "$DIR"/s*.pid; do kill -0 "$(cat "$f")" 2>/dev/null || dead="$dead $(basename "$f" .pid)"; done
  [ -n "$dead" ] && echo "$(date +%T) NOT RUNNING:$dead"
}

kill_leader() {
  local s=$1 p
  check_alive
  p=$(leader_of "$s") || { echo "shard $s: no leader found"; return 1; }
  local j=$(( (p - BASE - 10 * s) ))
  echo "$(date +%T) killing shard $s leader on port $p (s$s.$j)"
  kill -9 "$(cat "$DIR/s$s.$j.pid")"
  local t0; t0=$(date +%s.%N)
  for _ in $(seq 200); do
    local np; np=$(leader_of "$s") && { echo "$(date +%T) shard $s new leader on $np after $(echo "$(date +%s.%N) - $t0" | bc | cut -c1-4) s"; return 0; }
    sleep 0.1
  done
  echo "shard $s: no new leader after 20 s"; return 1
}

python3 "$ROOT/tests/sql/txntest.py" --tarn 127.0.0.1:$PORT --only bank --bank-seconds "$SECS" --threads 8 \
  > "$DIR/bank.out" 2>&1 &
BANK=$!
sleep $((SECS / 4))
kill_leader 1
sleep $((SECS / 3))
kill_leader 2
wait $BANK
rc=$?
cat "$DIR/bank.out"
KV="127.0.0.1:$(leader_of 1),127.0.0.1:$(leader_of 2)"
python3 - "$KV" <<EOF
import json, subprocess, sys
nodes = sys.argv[1].split(",")
total = 0
for i in range(64):
    out = subprocess.run(["$T", "$ROOT/tests/sql/kvget.lua", json.dumps(nodes), "txntest.bank", json.dumps([i])],
                         capture_output=True, text=True, stdin=subprocess.DEVNULL).stdout
    rows = json.loads(out.strip().splitlines()[-1])
    assert len(rows) == 1, (i, rows)
    total += rows[0][1]
print("direct KV sum on new leaders:", total)
sys.exit(0 if total == 64000 else 1)
EOF
kv=$?
grep -h -E "tarn recovery|resolve" "$DIR"/s*.log | grep -v "^$" | tail -5
if [ $rc -eq 0 ] && [ $kv -eq 0 ]; then
  "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "FAILOVER TEST PASSED"
else
  KEEP=1 "$ROOT/scripts/cluster.sh" stop -d "$DIR"
  echo "FAILOVER TEST FAILED (bank rc=$rc kv=$kv); logs kept in $DIR"
  exit 1
fi
