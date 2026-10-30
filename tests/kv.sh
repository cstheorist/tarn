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
KV_BENCH=${KV_BENCH:-$ROOT/target/release/examples/strong_kv_bench}
KV_STUDY_ENGINE=${KV_STUDY_ENGINE:-}
KV_CLOCK_FAULT=${KV_CLOCK_FAULT:-0}
# Bound route warm-up by the same physical pool configured on both gateways.
KV_SQL_POOL_SIZE=4
TARN_NATIVE=${TARN_NATIVE:-$ROOT/target/release/libtarn_storage.so}
KV_SCENARIOS=${KV_SCENARIOS:-leader,quorum,stall,move,ddl,partition}
HOOKS=1
RELAY=1
case "$KV_CLOCK_FAULT" in 0|1) ;; *) echo 'KV_CLOCK_FAULT must be 0 or 1' >&2; exit 2;; esac
if [ "$KV_CLOCK_FAULT" = 1 ]; then
    [ -z "$KV_STUDY_ENGINE" ] || { echo 'Clock fault and measurement cohorts must be separate' >&2; exit 2; }
    RELAY=0
fi
if [ -n "$KV_STUDY_ENGINE" ]; then
    case "$KV_STUDY_ENGINE" in memtx|vinyl) ;; *) echo 'KV_STUDY_ENGINE must be memtx or vinyl' >&2; exit 2;; esac
    HOOKS=''
    RELAY=0
    KV_BENCH=$(readlink -f "$KV_BENCH")
    [ -x "$KV_BENCH" ] && [ "$TARN_NATIVE" != 0 ]
else KV_BENCH=0
fi
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
printf '%s\n' "$NONCE" > "$WORK/owner-nonce"
chmod 600 "$WORK/owner-nonce"
MARKER_DEV=$(stat -Lc %d "$WORK/owner-nonce")
MARKER_INO=$(stat -Lc %i "$WORK/owner-nonce")
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
    [ "$(readlink "/proc/$pid/fd/9")" = "$WORK/owner-nonce" ] || return 1
    [ "$(stat -Lc %d "/proc/$pid/fd/9")" = "$MARKER_DEV" ] || return 1
    [ "$(stat -Lc %i "/proc/$pid/fd/9")" = "$MARKER_INO" ]
}
cleanup() {
    local status=$? pid round live=0 failure=0
    trap - EXIT INT TERM
    "$PYTHON" - "$WORK" <<'PY'
import json,os,pathlib
import sys
p=pathlib.Path(sys.argv[1]);out=[]
for line in (p/'processes.jsonl').read_text().splitlines() if (p/'processes.jsonl').exists() else []:
 n=json.loads(line);q=pathlib.Path('/proc',str(n['pid']));actual={}
 for key in ['cwd','exe','fd/9']:
  try:actual[key]=os.readlink(q/key)
  except OSError as e:actual[key]={'error':e.errno}
 try:
  actual['stat']=(q/'stat').read_text();env=(q/'environ').read_bytes().split(b'\0')
  actual['original_environment_marker_presence']={k:any(x.startswith((k+'=').encode()) for x in env)
   for k in ['TARN_KV_OWNER_NONCE','TARN_KV_NODE_ID','TARN_KV_WORKDIR']}
  s=(q/'fd/9').stat();actual['marker_stat']={'dev':s.st_dev,'ino':s.st_ino,'uid':s.st_uid}
 except OSError as e:actual['inspection_errno']=e.errno
 out.append({'recorded':n,'actual_before_shutdown':actual})
f=p/'cleanup-identities.json';f.write_text(json.dumps(out,indent=2)+'\n');f.chmod(0o600)
PY
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
        for name in preflight.stderr functional.log faults.log; do
            if [ -f "$WORK/$name" ]; then echo "Retained $name (last 100 lines):"; tail -100 "$WORK/$name"; fi
        done
    fi
    exit "$status"
}
trap cleanup EXIT
trap 'exit 130' INT
trap 'exit 143' TERM
"$PYTHON" - "$WORK" "$NONCE" "$$" "$ROOT" "$TARN_SERVER" "$KV_SDK" "$TARN_NATIVE" "$KV_BENCH" <<'PY'
import hashlib,json,os,pathlib,subprocess,sys,time
p=pathlib.Path(sys.argv[1]);pid=int(sys.argv[3]);fields=pathlib.Path(f'/proc/{pid}/stat').read_text().rsplit(') ',1)[1].split()
owner={'version':1,'backend':'host','owner':'tarn-kv181-host-v1','ownerNonce':sys.argv[2],
 'workdir':str(p),'launcher_pid':pid,'launcher_starttime':int(fields[19])}
q=p/'owner.json';q.write_text(json.dumps(owner,indent=2)+'\n');q.chmod(0o600)
repo=pathlib.Path(sys.argv[4]);paths=list((repo/'lua').glob('*.lua'))
paths += [repo/'tests'/n for n in ['kv.sh','kv_control.py','kv_relay.py'] if (repo/'tests'/n).is_file()]
paths += [repo/'tests/sql'/n for n in ['kvready.lua','kvtest.py','kvfaults.py','kvrpc.lua','lazyrpc.lua','lazytest.py','movetest.py']]
if (repo/'tests/sql/kvclockfaults.py').is_file():paths += [repo/'tests/sql/kvclockfaults.py']
paths += [repo/'bench'/n for n in ['strong_kv.py','strong_kv_fixture.py','strong_kv_cpu.py'] if (repo/'bench'/n).is_file()]
paths += list((repo/'crates').rglob('*.rs')) + list((repo/'crates').rglob('Cargo.toml'))
paths += [repo/n for n in ['Cargo.toml','Cargo.lock','rust-toolchain.toml']]
paths += [pathlib.Path(x) for x in sys.argv[5:] if x!='0']
files={str(f):{'bytes':f.stat().st_size,'sha256':hashlib.sha256(f.read_bytes()).hexdigest()} for f in paths}
head=subprocess.run(['git','-C',str(repo),'rev-parse','HEAD'],capture_output=True,text=True,check=True).stdout.strip()
q=p/'source.json';q.write_text(json.dumps({'head':head,'files':files,'created_unix_ns':time.time_ns(),
 'scope':'actual fixture Lua/test sources and supplied executable bytes; build provenance is a separate gate'},indent=2)+'\n');q.chmod(0o600)
PY
# Hold all eight binds together while choosing the ports. Actual listeners must
# still bind successfully after these reservation sockets close; no adoption.
mapfile -t PORTS < <("$PYTHON" - "$RELAY" <<'PY'
import socket,sys
held=[]
for _ in range(20 if sys.argv[1]=='1' else 8):
 s=socket.socket();s.bind(('127.0.0.1',0));held.append(s)
for s in held: print(s.getsockname()[1])
for s in held:s.close()
PY
)
[ "${#PORTS[@]}" = "$((8 + 12 * RELAY))" ]
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
    (cd "$dir"; exec 9< "$WORK/owner-nonce"; exec env TARN_KV_OWNER_NONCE="$NONCE" TARN_KV_NODE_ID="$name" TARN_KV_WORKDIR="$dir" "$@") > "$WORK/$name.log" 2>&1 &
    local pid=$! line rest birth count
    PIDS+=("$pid");NAMES+=("$name");WORKDIRS[$pid]=$dir;EXES[$pid]=$expected_exe;IDS[$pid]=$name
    line=$(cat "/proc/$pid/stat");rest=${line##*) };set -- $rest;birth=${20};BIRTHS[$pid]=$birth
    # Record the expected original child even if its startup guard fails.
    "$PYTHON" - "$WORK" "$name" "$port" "$pid" "$$" "$birth" "$dir" "$NONCE" "$MARKER_DEV" "$MARKER_INO" "$expected_exe" <<'PY'
import json,os,pathlib,sys
root=pathlib.Path(sys.argv[1]);name=sys.argv[2];pid=int(sys.argv[4])
n={'node_id':name,'endpoint':'127.0.0.1:'+sys.argv[3],'pid':pid,'ppid':int(sys.argv[5]),
 'starttime':int(sys.argv[6]),'workdir':sys.argv[7],'ownerNonce':sys.argv[8],'exe':sys.argv[11],
 'owner_marker':{'fd':9,'path':str(root/'owner-nonce'),'dev':int(sys.argv[9]),'ino':int(sys.argv[10])}}
if name.startswith('s'):n.update(shard_id=int(name[1]),replica=int(name[3]))
fd=os.open(str(root/'processes.jsonl'),os.O_WRONLY|os.O_APPEND|os.O_CREAT,0o600)
try:os.write(fd,(json.dumps(n)+'\n').encode())
finally:os.close(fd)
PY
    # Require exec/cwd and the inherited owner descriptor, preserving birth.
    for count in {1..100}; do owned "$pid" && break; sleep .01; done
    owned "$pid" || { echo "Owned child $name failed its startup identity" >&2; return 1; }
}
if [ "$RELAY" = 1 ]; then
    "$PYTHON" - "$WORK" "$NONCE" "$REPLICAS" "${PORTS[@]:8:12}" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]);groups=json.loads(sys.argv[3]);ports=iter(sys.argv[4:]);flows=[];replication={}
for shard,group in enumerate(groups,1):
 for client,endpoint in enumerate(group,1):
  node=f's{shard}r{client}';uris=[]
  for server,target in enumerate(group,1):
   if client==server:uris.append('tarn:tarn@'+target);continue
   listen='127.0.0.1:'+next(ports);peer=f's{shard}r{server}'
   flows.append({'id':node+'<-'+peer,'client_node':node,'server_node':peer,'listen':listen,'target':target})
   uris.append('tarn:tarn@'+listen)
  replication[node]=uris
assert len(flows)==12
for name,value in [('relay-config.json',{'version':1,'ownerNonce':sys.argv[2],'workdir':str(p),
 'control_socket':str(p/'replication-control.sock'),'flows':flows}),('replication-config.json',replication)]:
 q=p/name;q.write_text(json.dumps(value,indent=2)+'\n');q.chmod(0o600)
PY
    PYTHON_EXE=$(readlink -f "$(command -v "$PYTHON")")
    start relay 0 "$WORK/relay" "$PYTHON_EXE" "$PYTHON" "$ROOT/tests/kv_relay.py" --config "$WORK/relay-config.json"
    for count in {1..100}; do [ -S "$WORK/replication-control.sock" ] && break; sleep .05; done
    [ -S "$WORK/replication-control.sock" ] || { echo 'Owned replication relay did not start' >&2; exit 1; }
fi
for shard in 1 2; do
    GROUP=$("$PYTHON" - "$REPLICAS" "$shard" <<'PY'
import json,sys
print(json.dumps(['tarn:tarn@'+x for x in json.loads(sys.argv[1])[int(sys.argv[2])-1]]))
PY
)
    for replica in 1 2 3; do
        index=$(( (shard - 1) * 3 + replica - 1 )); name=s${shard}r${replica}
        if [ "$RELAY" = 1 ]; then
            GROUP=$("$PYTHON" - "$WORK/replication-config.json" "$name" <<'PY'
import json,sys
print(json.dumps(json.load(open(sys.argv[1]))[sys.argv[2]]))
PY
)
        fi
        start "$name" "${PORTS[$index]}" "$WORK/$name" "$TARANTOOL" env \
            TARN_SHARD_ID="$shard" TARN_LISTEN="127.0.0.1:${PORTS[$index]}" TARN_WORKDIR="$WORK/$name" \
            TARN_REPLICAS="$GROUP" TARN_PEERS="$PEERS" TARN_NATIVE="$TARN_NATIVE" TARN_WAL_MODE=fsync \
            TARN_MEMORY_MB=96 TARN_VINYL_MEMORY_MB=16 TARN_VINYL_CACHE_MB=0 TARN_VINYL_TIMEOUT=.25 \
            TARN_VINYL_READ_THREADS=1 TARN_HOST_INSTANCES=6 TARN_TEST_HOOKS="$HOOKS" TARN_CHECK_YIELDS="$HOOKS" \
            TARN_CHECK_LOCKS="$HOOKS" TARN_LOG_LEVEL=4 "$TARANTOOL" "$ROOT/lua/storage.lua"
    done
done
"$PYTHON" - "$WORK" "$REPLICAS" "$NATIVE_ENABLED" <<'PY'
import json,pathlib,sys
p=pathlib.Path(sys.argv[1]);m=json.loads((p/'owner.json').read_text());records=[json.loads(x) for x in (p/'processes.jsonl').read_text().splitlines()]
m['nodes']=[x for x in records if x['node_id'].startswith('s')]
relay=next((x for x in records if x['node_id']=='relay'),None)
if relay:
 config=json.loads((p/'relay-config.json').read_text());m['relay']={**relay,
  'control_socket':config['control_socket'],'flows':config['flows']}
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
        --node-id "$gateway" --threads 2 --pool "$KV_SQL_POOL_SIZE" --allow-all --user sb:
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
if [ "$KV_CLOCK_FAULT" = 1 ]; then
    "$PYTHON" "$ROOT/tests/sql/kvclockfaults.py" --tarn "$TARN" --shards "$LEADERS" --replicas "$REPLICAS" \
        --sdk "$KV_SDK" --evidence "$WORK/clock-fault" > "$WORK/clock-fault.log" 2>&1
    cat "$WORK/clock-fault.log"
    exit 0
fi
if [ -n "$KV_STUDY_ENGINE" ]; then
    "$PYTHON" "$ROOT/bench/strong_kv_fixture.py" --work "$WORK" --tarn "$TARN" --shards "$LEADERS" \
        --replicas "$REPLICAS" --engine "$KV_STUDY_ENGINE" --driver "$KV_BENCH" --sdk "$KV_SDK" \
        --output "$WORK/study" --trials 6 --seconds "${KV_STUDY_SECONDS:-5}" --warmup "${KV_STUDY_WARMUP:-1}" \
        > "$WORK/study.log" 2>&1
    cat "$WORK/study.log"
    exit 0
fi
"$PYTHON" "$ROOT/tests/sql/kvtest.py" --tarn "$TARN" --shards "$LEADERS" --replicas "$REPLICAS" \
    --sdk "$KV_SDK" --evidence "$WORK/functional" > "$WORK/functional.log" 2>&1
cat "$WORK/functional.log"
"$PYTHON" "$ROOT/tests/sql/kvfaults.py" --tarn "$TARN" --shards "$LEADERS" --replicas "$REPLICAS" \
    --sdk "$KV_SDK" --evidence "$WORK/faults" --fault-control "$CONTROL" --scenarios "$KV_SCENARIOS" \
    --sql-pool-size "$KV_SQL_POOL_SIZE" > "$WORK/faults.log" 2>&1
cat "$WORK/faults.log"
