#!/bin/bash
# Fault-injection test: a connection between tarn-server and its shard breaks
# after an autocommit UPDATE was executed but before its reply arrived. The
# statement must be reported as having an unknown outcome (1180 / SQLSTATE
# 40003) and must have been applied exactly once -- never silently re-sent.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
T=${TARANTOOL:-/opt/src/tarantool/build/src/tarantool}
TARN=${TARN_BIN:-$ROOT/target/release/tarn-server}
W=$(mktemp -d /tmp/tarn-faults.XXXX)
SP=${SP:-3641}; PP=${PP:-3642}; QP=${QP:-4641}
cleanup() { kill $(cat "$W"/*.pid 2>/dev/null) 2>/dev/null; }
trap cleanup EXIT
TARN_SHARD_ID=1 TARN_LISTEN=127.0.0.1:$SP TARN_WORKDIR=$W/s1 TARN_MEMORY_MB=256 TARN_LOG_LEVEL=4 \
  TARN_PEERS="{\"1\":[\"tarn:tarn@127.0.0.1:$SP\"]}" "$T" "$ROOT/lua/storage.lua" > "$W/s1.log" 2>&1 &
echo $! > "$W/s1.pid"
for _ in $(seq 50); do "$T" "$ROOT/scripts/probe.lua" "tarn:tarn@127.0.0.1:$SP" < /dev/null && break; sleep 0.1; done
python3 "$ROOT/tests/proxy.py" $PP $SP > "$W/proxy.log" 2>&1 &
echo $! > "$W/proxy.pid"
sleep 0.3
TARN_LOG=warn "$TARN" --shard 127.0.0.1:$PP --listen 127.0.0.1:$QP --allow-all > "$W/tarn.log" 2>&1 &
echo $! > "$W/tarn.pid"
for _ in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/$QP) 2>/dev/null && break; sleep 0.1; done
python3 - $QP <<'PY'
import sys
import pymysql
c = pymysql.connect(host="127.0.0.1", port=int(sys.argv[1]), user="t", password="", autocommit=True)
cur = c.cursor()
for s in ["CREATE DATABASE f", "CREATE TABLE f.t (id INT PRIMARY KEY, v INT)", "INSERT INTO f.t VALUES (1, 0)"]:
    cur.execute(s)
try:
    cur.execute("UPDATE f.t SET v = v + 1 WHERE id = 1")
    print(f"UPDATE returned OK ({cur.rowcount} row)")
    code = 0
except pymysql.MySQLError as e:
    code = e.args[0]
    print(f"UPDATE failed: {e}")
cur.execute("SELECT v FROM f.t WHERE id = 1")
v = cur.fetchone()[0]
print(f"v = {v}")
ok = code == 1180 and v == 1
print("FAULTS TEST PASSED" if ok else "FAULTS TEST FAILED (want error 1180 and v = 1)")
sys.exit(0 if ok else 1)
PY
rc=$?
cat "$W/proxy.log"
cleanup
trap - EXIT
[ $rc -eq 0 ] && rm -rf "$W" || echo "logs in $W"
exit $rc
