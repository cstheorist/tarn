#!/bin/bash
# Fault-injection test: a connection between tarn-server and its shard breaks
# after an autocommit UPDATE was executed but before its reply arrived. The
# statement must be reported as having an unknown outcome (1180 / SQLSTATE
# 40003) and must have been applied exactly once -- never silently re-sent.
# The connection breaks on the call that makes the UPDATE: the native
# procedure's when the shard loaded it (the table is memtx), else
# tarn_autocommit's. ENGINE=vinyl runs it on a vinyl table, TARN_NATIVE=0
# without the native procedures.
# TARN_FAULT_REPLY=invalid-type substitutes a forged retryable procedure error
# under an invalid IPROTO response type after execution; it must not cause replay.
set -u
ROOT=$(cd "$(dirname "$0")/.." && pwd)
T=${TARANTOOL:-tarantool}
TARN=${TARN_BIN:-$ROOT/target/release/tarn-server}
W=$(mktemp -d /tmp/tarn-faults.XXXX)
SP=${SP:-3641}; PP=${PP:-3642}; QP=${QP:-4641}
cleanup() { kill $(cat "$W"/*.pid 2>/dev/null) 2>/dev/null; }
trap cleanup EXIT
TARN_SHARD_ID=1 TARN_LISTEN=127.0.0.1:$SP TARN_WORKDIR=$W/s1 TARN_MEMORY_MB=256 TARN_LOG_LEVEL=4 \
  TARN_PEERS="{\"1\":[\"tarn:tarn@127.0.0.1:$SP\"]}" "$T" "$ROOT/lua/storage.lua" > "$W/s1.log" 2>&1 &
echo $! > "$W/s1.pid"
for _ in $(seq 50); do "$T" "$ROOT/scripts/probe.lua" "tarn:tarn@127.0.0.1:$SP" < /dev/null && break; sleep 0.1; done
# The procedure that makes the UPDATE (lua/tarn.lua "Native procedures"):
# asked until the shard has tried to load the native module.
PROC=
for _ in $(seq 50); do
  PROC=$("$T" -e "local c = require('net.box').connect('tarn:tarn@127.0.0.1:$SP', {wait_connected = 1})
    local ok, r = pcall(c.eval, c, [[
      if tarn_info().native then return 'ts_autocommit' end
      if not require('tarn').cfg.native then return 'tarn_autocommit' end]])
    if ok and r then print(r) end
    os.exit(0)" < /dev/null)
  [ -n "$PROC" ] && break
  sleep 0.1
done
[ -n "$PROC" ] || { echo "FAULTS TEST FAILED: the shard did not load or refuse the native module"; exit 1; }
[ "${ENGINE:-memtx}" = vinyl ] && PROC=tarn_autocommit
echo "breaking the connection on the call of $PROC"
python3 "$ROOT/tests/proxy.py" $PP $SP $PROC update > "$W/proxy.log" 2>&1 &
echo $! > "$W/proxy.pid"
sleep 0.3
TARN_LOG=warn "$TARN" --shard 127.0.0.1:$PP --listen 127.0.0.1:$QP --allow-all \
  ${ENGINE:+--default-engine "$ENGINE"} > "$W/tarn.log" 2>&1 &
echo $! > "$W/tarn.pid"
for _ in $(seq 50); do (exec 3<>/dev/tcp/127.0.0.1/$QP) 2>/dev/null && break; sleep 0.1; done
python3 - $QP <<'PY'
import sys
import pymysql
c = pymysql.connect(host="127.0.0.1", port=int(sys.argv[1]), user="t", password="", autocommit=True)
cur = c.cursor()
for s in ["CREATE DATABASE f", "CREATE TABLE f.t (id INT PRIMARY KEY, v INT)", "INSERT INTO f.t VALUES (1, 0)"]:
    cur.execute(s)
sqlstate = None
raise_error = pymysql.err.raise_mysql_exception
def capture_error(data):
    global sqlstate
    if len(data) >= 9 and data[3:4] == b'#':
        sqlstate = data[4:9].decode('ascii')
    raise_error(data)
pymysql.err.raise_mysql_exception = capture_error
try:
    cur.execute("UPDATE f.t SET v = v + 1 WHERE id = 1")
    print(f"UPDATE returned OK ({cur.rowcount} row)")
    code = 0
except pymysql.MySQLError as e:
    code = e.args[0]
    print(f"UPDATE failed: {e}")
finally:
    pymysql.err.raise_mysql_exception = raise_error
cur.execute("SELECT v FROM f.t WHERE id = 1")
v = cur.fetchone()[0]
print(f"v = {v}")
ok = code == 1180 and sqlstate == '40003' and v == 1
print(f"SQLSTATE = {sqlstate}")
print("FAULTS TEST PASSED" if ok else "FAULTS TEST FAILED (want 1180/40003 and v = 1)")
sys.exit(0 if ok else 1)
PY
rc=$?
cat "$W/proxy.log"
if [ "${TARN_FAULT_REPLY:-drop}" = invalid-type ]; then
  grep -q 'injected invalid response type after execution' "$W/proxy.log" || rc=1
fi
if [ "${TARN_FAULT_REPLY:-drop}" = oversized-frame ]; then
  grep -q 'injected oversized frame after execution' "$W/proxy.log" || rc=1
fi
# (TARN_CHECK_LOCKS: the native module's copy of the lock index held what
# the index held)
if grep -h "lock copy differs" "$W"/s1.log | head -3 | grep .; then
  echo "FAULTS TEST FAILED: the native module's copy of the lock index differed from it"
  rc=1
fi
cleanup
trap - EXIT
[ $rc -eq 0 ] && rm -rf "$W" || echo "logs in $W"
exit $rc
