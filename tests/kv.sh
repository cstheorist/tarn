#!/usr/bin/env bash
# Isolated Linux RF3 strong-KV histories. No existing service/directory adoption.
# TARN_NATIVE=0 selects Lua; otherwise use the supplied native .so (ABI checked).
set -euo pipefail
[ "$(uname -s)" = Linux ] || { echo 'tests/kv.sh requires Linux /proc' >&2; exit 2; }
ROOT=$(cd "$(dirname "$0")/.." && pwd)
PYTHON=${PYTHON:-python3}
TARANTOOL=${TARANTOOL:-tarantool}
TARN_SERVER=${TARN_SERVER:-$ROOT/target/release/tarn-server}
KV_SDK=${KV_SDK:-$ROOT/target/release/examples/strong_kv}
TARN_NATIVE=${TARN_NATIVE:-$ROOT/target/release/libtarn_storage.so}
KV_SCENARIOS=${KV_SCENARIOS:-leader,quorum,stall}
export TARANTOOL
TARANTOOL=$(command -v "$TARANTOOL")
TARANTOOL=$(readlink -f "$TARANTOOL")
TARN_SERVER=$(readlink -f "$TARN_SERVER")
KV_SDK=$(readlink -f "$KV_SDK")
[ -x "$TARANTOOL" ] && [ -x "$TARN_SERVER" ] && [ -x "$KV_SDK" ]
NATIVE_ENABLED=true
if [ "$TARN_NATIVE" = 0 ]; then NATIVE_ENABLED=false
else TARN_NATIVE=$(readlink -f "$TARN_NATIVE"); [ -f "$TARN_NATIVE" ]; fi
# Fail unknown scenarios before starting any process. Read the actual driver's
# declaration without importing it (its dependencies are checked below).
"$PYTHON" - "$ROOT/tests/sql/kvfaults.py" "$KV_SCENARIOS" <<'PY'
import ast,sys
tree=ast.parse(open(sys.argv[1]).read())
allowed=next(ast.literal_eval(n.value) for n in tree.body if isinstance(n,ast.Assign)
             and any(isinstance(t,ast.Name) and t.id=='IMPLEMENTED' for t in n.targets))
requested=sys.argv[2].split(',')
assert requested and len(set(requested))==len(requested) and set(requested)<=allowed, 'unsupported KV_SCENARIOS'
import pymysql
PY
WORK=$(mktemp -d /tmp/tarn-kv181-XXXXXXXX)
chmod 700 "$WORK"
NONCE=$("$PYTHON" -c 'import os; print(os.urandom(16).hex())')
echo "Strong KV evidence: $WORK"
declare -a PIDS=() NAMES=()
declare -A BIRTHS=() WORKDIRS=() EXES=() IDS=()
owned() {
    local pid=$1 line rest
    [ -r "/proc/$pid/stat" ] || return 1
    line=$(cat "/proc/$pid/stat") || return 1
    rest=${line##*) }; set -- $rest
    [ "$2" = "$$" ] && [ "${20}" = "${BIRTHS[$pid]:-missing}" ] || return 1
    [ "$(readlink "/proc/$pid/cwd")" = "${WORKDIRS[$pid]:-missing}" ] || return 1
    [ "$(readlink "/proc/$pid/exe")" = "${EXES[$pid]:-missing}" ] || return 1
    # Tarantool rewrites its process title and relocates the environment.
    # An inherited read-only descriptor keeps the private owner receipt
    # bound to the original child independently of argv/environ storage.
    [ "/proc/$pid/fd/9" -ef "$WORK/owner.json" ]
}
cleanup() {
    local status=$? pid round live=0 failure=0
    trap - EXIT INT TERM
    for pid in "${PIDS[@]}"; do if owned "$pid"; then kill -CONT "$pid" 2>/dev/null || true; fi; done
    for pid in "${PIDS[@]}"; do if owned "$pid"; then kill -TERM "$pid" 2>/dev/null || true; fi; done
    for round in {1..100}; do
        live=0
        for pid in "${PIDS[@]}"; do if owned "$pid"; then live=1; fi; done
        [ "$live" = 0 ] && break
        sleep .05
    done
    for pid in "${PIDS[@]}"; do if owned "$pid"; then kill -KILL "$pid" 2>/dev/null || true; fi; done
    for pid in "${PIDS[@]}"; do
        if [ -r "/proc/$pid/stat" ]; then
            # Do not signal an identity that changed. Preserve the discrepancy.
            local line rest; line=$(cat "/proc/$pid/stat" 2>/dev/null) || continue
            rest=${line##*) }; set -- $rest
            if [ "$2" = "$$" ] && [ "${20}" = "${BIRTHS[$pid]}" ] && [ "$1" != Z ]; then
                echo "Unreaped original child $pid; identity guard refused or shutdown is incomplete" >> "$WORK/cleanup-errors.log"
                failure=1
            fi
        fi
        if [ "$failure" = 0 ]; then wait "$pid" 2>/dev/null || true; fi
    done
    if [ "$failure" != 0 ] && [ "$status" = 0 ]; then status=2; fi
    "$PYTHON" - "$WORK" "$status" "$failure" <<'PY'
import json,pathlib,sys,time
p=pathlib.Path(sys.argv[1]);v={'exit_code':int(sys.argv[2]),'owned_processes_reaped':sys.argv[3]=='0',
 'finished_unix_ns':time.time_ns(),'scope':'owned PID/PPID/birth/cwd/nonce cleanup; no other services touched'}
q=p/'lifecycle.json';q.write_text(json.dumps(v,indent=2)+'\n');q.chmod(0o600)
PY
    echo "Strong KV exit $status; logs, identities, WAL and evidence retained at $WORK"
    if [ "$status" != 0 ]; then
        for name in preflight.stderr preflight.json functional.log faults.log; do
            if [ -f "$WORK/$name" ]; then echo "Retained $name (last 100 lines):"; tail -100 "$WORK/$name"; fi
        done
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"$PYTHON" - "$WORK" "$NONCE" "$$" "$ROOT" "$TARN_SERVER" "$KV_SDK" "$TARN_NATIVE" <<'PY'
import hashlib,json,os,pathlib,subprocess,sys,time
p=pathlib.Path(sys.argv[1]);pid=int(sys.argv[3]);fields=pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(') ',1)[1].split()
owner={'version':1,'backend':'host','owner':'tarn-kv181-host-v1','ownerNonce':sys.argv[2],
 'workdir':str(p),'launcher_pid':pid,'launcher_starttime':int(fields[19])}
q=p/'owner.json';q.write_text(json.dumps(owner,indent=2)+'\n');q.chmod(0o600)
repo=pathlib.Path(sys.argv[4]);paths=list((repo/'lua').glob('*.lua'))
paths += [repo/'tests'/n for n in ['kv.sh','kv_control.py']]
paths += [repo/'tests/sql'/n for n in ['kvready.lua','kvtest.py','kvfaults.py','kvrpc.lua','lazyrpc.lua','lazytest.py','movetest.py']]
paths += [pathlib.Path(x) for x in sys.argv[5:] if x!='0']
files={str(f):{'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()} for f in paths}
head=subprocess.run(['git','-C',str(repo),'rev-parse','HEAD'],capture_output=True,text=True,check=True).stdout.strip()
q=p/'source.json';q.write_text(json.dumps({'head':head,'files':files,'created_unix_ns':time.time_ns(),
 'scope':'actual fixture Lua/test sources and supplied executable bytes; build provenance is a separate gate'},indent=2)+'\n');q.chmod(0o600)
PY
# Hold all eight binds together while choosing the ports. Actual listeners must
# still bind successfully after these reservation sockets close; no adoption.
mapfile -t PORTS < <("$PYTHON" - <<'PY'
import socket
held=[]
for _ in range(8):
 s=socket.socket();s.bind(('127.0.0.1',0));held.append(s)
for s in held: print(s.getsockname()[1])
for s in held:s.close()
PY
)
[ "${#PORTS[@]}" = 8 ]
REPLICAS=$("$PYTHON" - "${PORTS[@]:0:6}" <<'PY'
import json,sys
e=['127.0.0.1:'+p for p in sys.argv[1:]];print(json.dumps([e[:3],e[3:]]))
PY
)
PEERS=$("$PYTHON" - "$REPLICAS" <<'PY'
import json,sys
print(json.dumps({str(i+1):['tarn:tarn@'+e for e in g] for i,g in enumerate(json.loads(sys.argv[1]))}))
PY
)
start() {
    local name=$1 port=$2 dir=$3 expected_exe=$4; shift 4
    mkdir -m 700 "$dir"
    (cd "$dir"; exec 9<"$WORK/owner.json"; exec env TARN_KV_OWNER_NONCE="$NONCE" TARN_KV_NODE_ID="$name" TARN_KV_WORKDIR="$dir" "$@") > "$WORK/$name.log" 2>&1 &
    local pid=$! line rest birth count
    PIDS+=("$pid");NAMES+=("$name");WORKDIRS[$pid]=$dir;EXES[$pid]=$expected_exe;IDS[$pid]=$name
    line=$(cat "/proc/$pid/stat");rest=${line##*) };set -- $rest;birth=${20};BIRTHS[$pid]=$birth
    # Wait for exec/cwd/env, while preserving this child's original birth.
    for count in {1..100}; do owned "$pid" && break; sleep .01; done
    owned "$pid" || { echo "Owned child $name failed its startup identity" >&2; return 1; }
    "$PYTHON" - "$WORK" "$name" "$port" "$pid" "$$" "$birth" "$dir" "$NONCE" <<'PY'
import json,os,pathlib,sys
root=pathlib.Path(sys.argv[1]);name=sys.argv[2];pid=int(sys.argv[4])
n={'node_id':name,'endpoint':'127.0.0.1:'+sys.argv[3],'pid':pid,'ppid':int(sys.argv[5]),
 'starttime':int(sys.argv[6]),'workdir':sys.argv[7],'ownerNonce':sys.argv[8],
 'owner_fd':9,'exe':os.readlink(f'/proc/{pid}/exe')}
if name.startswith('s'):n.update(shard_id=int(name[1]),replica=int(name[3]))
fd=os.open(str(root/'processes.jsonl'),os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600)
try:os.write(fd,(json.dumps(n)+'\n').encode())
finally:os.close(fd)
PY
}
for shard in 1 2; do
    GROUP=$("$PYTHON" - "$REPLICAS" "$shard" <<'PY'
import json,sys
print(json.dumps(['tarn:tarn@'+x for x in json.loads(sys.argv[1])[int(sys.argv[2])-1]]))
PY
)
    for replica in 1 2 3; do
        index=$(( (shard - 1) * 3 + replica - 1 )); name=s${shard}r${replica}
        start "$name" "${PORTS[$index]}" "$WORK/$name" "$TARANTOOL" env \
            TARN_SHARD_ID="$shard" TARN_LISTEN="127.0.0.1:${PORTS[$index]}" TARN_WORKDIR="$WORK/$name" \
            TARN_REPLICAS="$GROUP" TARN_PEERS="$PEERS" TARN_NATIVE="$TARN_NATIVE" TARN_WAL_MODE=fsync \
            TARN_MEMORY_MB=96 TARN_VINYL_MEMORY_MB=16 TARN_VINYL_CACHE_MB=0 TARN_VINYL_TIMEOUT=.25 \
            TARN_VINYL_READ_THREADS=1 TARN_HOST_INSTANCES=6 TARN_TEST_HOOKS=1 TARN_CHECK_YIELDS=1 \
            TARN_CHECK_LOCKS=1 TARN_LOG_LEVEL=4 "$TARANTOOL" "$ROOT/lua/storage.lua"
    done
done
"$PYTHON" - "$WORK" "$REPLICAS" "$NATIVE_ENABLED" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]);m=json.loads((p/'owner.json').read_text());m['nodes']=[json.loads(x) for x in (p/'processes.jsonl').read_text().splitlines()]
q=p/'manifest.json';q.write_text(json.dumps(m,indent=2)+'\n');q.chmod(0o600)
q=p/'ready-config.json';q.write_text(json.dumps({'version':1,'replicas':json.loads(sys.argv[2]),'native':sys.argv[3]=='true'})+'\n');q.chmod(0o600)
PY
"$TARANTOOL" "$ROOT/tests/sql/kvready.lua" "$WORK/ready-config.json" > "$WORK/preflight.json" 2> "$WORK/preflight.stderr"
LEADERS=$("$PYTHON" - "$WORK/preflight.json" <<'PY'
import json,sys
r=json.load(open(sys.argv[1]));assert r['version']==1 and r['status']=='ok' and len(r['leaders'])==2
print(','.join(r['leaders']))
PY
)
GROUP1=$("$PYTHON" -c 'import json,sys;print(",".join(json.loads(sys.argv[1])[0]))' "$REPLICAS")
GROUP2=$("$PYTHON" -c 'import json,sys;print(",".join(json.loads(sys.argv[1])[1]))' "$REPLICAS")
for gateway in 1 2; do
    start "gw$gateway" "${PORTS[$((5+gateway))]}" "$WORK/gw$gateway" "$TARN_SERVER" "$TARN_SERVER" \
        --shard "$GROUP1" --shard "$GROUP2" --listen "127.0.0.1:${PORTS[$((5+gateway))]}" \
        --node-id "$gateway" --threads 2 --allow-all --user sb:
done
"$PYTHON" - "${PORTS[@]:6:2}" <<'PY'
import pymysql,sys,time
deadline=time.monotonic()+90;last=None
while time.monotonic()<deadline:
 try:
  for port in sys.argv[1:]:
   c=pymysql.connect(host='127.0.0.1',port=int(port),user='sb',password='',connect_timeout=1,read_timeout=3,autocommit=True)
   try:
    with c.cursor() as cur:cur.execute('SELECT 1');assert cur.fetchall()==((1,),)
   finally:c.close()
  break
 except Exception as error:last=repr(error);time.sleep(.1)
else:raise RuntimeError('owned gateway readiness deadline: '+str(last))
PY
CONTROL=$("$PYTHON" - "$PYTHON" "$ROOT/tests/kv_control.py" "$WORK/manifest.json" <<'PY'
import json,sys
print(json.dumps([sys.argv[1],sys.argv[2],'--manifest',sys.argv[3]]))
PY
)
printf '%s\n' '{"action":"inspect"}' | "$PYTHON" "$ROOT/tests/kv_control.py" --manifest "$WORK/manifest.json" > "$WORK/broker-preflight.json"
TARN="127.0.0.1:${PORTS[6]},127.0.0.1:${PORTS[7]}"
"$PYTHON" "$ROOT/tests/sql/kvtest.py" --tarn "$TARN" --shards "$LEADERS" --replicas "$REPLICAS" \
    --sdk "$KV_SDK" --evidence "$WORK/functional" > "$WORK/functional.log" 2>&1
cat "$WORK/functional.log"
"$PYTHON" "$ROOT/tests/sql/kvfaults.py" --tarn "$TARN" --shards "$LEADERS" --replicas "$REPLICAS" \
    --sdk "$KV_SDK" --evidence "$WORK/faults" --fault-control "$CONTROL" --scenarios "$KV_SCENARIOS" > "$WORK/faults.log" 2>&1
cat "$WORK/faults.log"
