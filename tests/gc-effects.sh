#!/bin/bash
# Finite process-loss/effect fixture; separate from #160's runtime experiment.
set -euo pipefail
ROOT=$(cd "$(dirname "$0")/.." && pwd)
TARN=${TARN_BIN:-$ROOT/target/release/tarn-server}
T=${TARANTOOL:-tarantool}
PYTHON=${PYTHON:-python3}
BASE=${GC_BASE:-7200}; PORT=${GC_PORT:-7300}; RECOVERY_PORT=${GC_RECOVERY_PORT:-7301}; PP=${GC_PROXY_PORT:-7240}
ENGINE=${ENGINE:-memtx}; NATIVE=${GC_NATIVE:-native}
case "$ENGINE" in memtx|vinyl) ;; *) echo 'ENGINE must be memtx or vinyl'; exit 2;; esac
case "$NATIVE" in native) export TARN_NATIVE=${TARN_NATIVE:-$ROOT/target/release/libtarn_storage.so};; off) export TARN_NATIVE=0;; *) echo 'GC_NATIVE must be native or off'; exit 2;; esac
# Always create a new private directory; never accept a pre-existing cluster.
W=$(mktemp -d "${GC_EVIDENCE_PARENT:-/tmp}/tarn-gc-effects.XXXXXX")
cleanup() {
  rc=$?
  rm -f "$W/plan.json"
  # cluster.sh and this script are the only PID-file writers in this new dir.
  # Preserve files, WAL and logs. No global process-name matching or pkill.
  for f in "$W"/*.pid; do
    [ -f "$f" ] || continue
    pid=$(cat "$f")
    case "$pid" in ''|*[!0-9]*) continue;; esac
    kill "$pid" 2>/dev/null || true
  done
  echo "gc-effects evidence: $W (exit $rc)"
}
trap cleanup EXIT
export TARN_BIN="$TARN" TARN_TEST_HOOKS=1 TARN_CHECK_LOCKS=1 TARN_CHECK_YIELDS=1 TARN_BUCKET_COUNT=32
# Receipt precedes cluster construction, including failures during startup.
"$PYTHON" - "$ROOT" "$W" "$TARN" "$T" "$ENGINE" "$NATIVE" "$BASE" "$PORT" "$RECOVERY_PORT" "$PP" <<'PY'
import hashlib, json, os, pathlib, shutil, socket, subprocess, sys
root, work, binary = map(pathlib.Path, sys.argv[1:4])
def sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest() if path.is_file() else None
def command(argv):
    try:
        p = subprocess.run(argv, capture_output=True, text=True, timeout=10)
        return {'argv':argv, 'status':p.returncode, 'stdout':p.stdout, 'stderr':p.stderr}
    except Exception as e:
        return {'argv':argv, 'error':repr(e)}
paths = ['CLAUDE.md','docs/design.md','scripts/cluster.sh','scripts/probe.lua','scripts/joined.lua',
         'lua/storage.lua','lua/tarn.lua','tests/gc-effects.sh','tests/sql/gc_effects.py',
         'tests/sql/effect_journal.py','tests/sql/outcome_client.py','tests/outcome_proxy.py',
         'tests/sql/lazytest.py','tests/sql/lazyrpc.lua','tests/sql/movetest.py',
         'tests/test_gc_effects.py','tests/gc-effects/README.md']
base, port, recovery, proxy = map(int, sys.argv[7:11])
ports = [base+s*10+r for s in (1,2) for r in (1,2,3)] + [port,recovery,proxy]
native = pathlib.Path(os.environ['TARN_NATIVE'])
tarantool_binary = pathlib.Path(shutil.which(sys.argv[4]) or sys.argv[4]).resolve()
receipt = {'source':command(['git','-C',str(root),'rev-parse','HEAD']),
           'diff':command(['git','-C',str(root),'diff','HEAD']),
           'files':{p:sha(root/p) for p in paths}, 'binary':str(binary),'binary_sha256':sha(binary),
           'native':str(native),'native_sha256':sha(native), 'native_mode':sys.argv[6],
           'tarantool':command([sys.argv[4],'--version']),
           'tarantool_binary':str(tarantool_binary), 'tarantool_sha256':sha(tarantool_binary),
           'rustc':command(['rustc','--version']),
           'engine':sys.argv[5], 'wal':'fsync','shards':2,'replicas':3,'ports':ports,
           'environment':{k:v for k,v in os.environ.items() if k.startswith('TARN_')}}
(work/'source.json').write_text(json.dumps(receipt,indent=2)+'\n')
assert len(set(ports)) == len(ports), 'fixture ports overlap'
assert all(1024 <= p < 65536 for p in ports), 'invalid fixture ports'
# Refuse occupied ports before starting anything. The fixture still requires
# exclusive reservation by its caller; bind checking cannot eliminate races.
sockets=[]
try:
    for p in ports:
        s=socket.socket(); sockets.append(s); s.bind(('127.0.0.1',p))
finally:
    for s in sockets:s.close()
assert binary.is_file(), 'build tarn-server first'
assert sys.argv[6] == 'off' or native.is_file(), 'native mode requires the selected native module'
PY
"$ROOT/scripts/cluster.sh" start -d "$W" -n 2 -r 3 -s 0 -b "$BASE" -p "$PORT" -w fsync -m 256 > "$W/start.log" 2>&1
cp "$W/cluster.json" "$W/recovery.json"
"$PYTHON" - "$ROOT" "$W" "$ENGINE" "$NATIVE" "$BASE" "$PP" <<'PY'
import json, os, pathlib, subprocess, sys
root, work = map(pathlib.Path,sys.argv[1:3]); base, proxy = map(int,sys.argv[5:7])
nodes={}; replicas=[]; leaders=[]
for shard in (1,2):
    replicas.append([])
    for replica in (1,2,3):
        name=f's{shard}.{replica}'; endpoint=f'127.0.0.1:{base+shard*10+replica}'
        nodes[name]={'endpoint':endpoint,'pid':int((work/(name+'.pid')).read_text())}
        replicas[-1].append(endpoint)
        if subprocess.run([os.environ.get('TARANTOOL','tarantool'),str(root/'scripts/probe.lua'),
                           'tarn:tarn@'+endpoint],stdin=subprocess.DEVNULL,stdout=subprocess.DEVNULL,
                          stderr=subprocess.DEVNULL).returncode == 0:
            leaders.append(name)
assert len(leaders) == 2 and leaders[0].startswith('s1.') and leaders[1].startswith('s2.'), leaders
fixture={'engine':sys.argv[3],'native':sys.argv[4],'owned_nodes':nodes,'replicas':replicas,'proxy_node':leaders[0]}
(work/'fixture.json').write_text(json.dumps(fixture,indent=2)+'\n')
config=json.loads((work/'cluster.json').read_text())
config['shards'][0]['replicas']=['127.0.0.1:'+str(proxy)]
config['rpc']={'timeout_secs':5.0}
(work/'proxy-gateway.json').write_text(json.dumps(config,indent=2)+'\n')
(work/'proxy-upstream').write_text(nodes[leaders[0]]['endpoint'].rsplit(':',1)[1])
PY
"$PYTHON" "$ROOT/tests/outcome_proxy.py" --listen "$PP" --upstream "$(cat "$W/proxy-upstream")" --plan "$W/plan.json" > "$W/proxy.jsonl" 2> "$W/proxy.err" &
PROXY_PID=$!
echo "$PROXY_PID" > "$W/proxy.pid"
# Gateway construction immediately connects to storage. Its proxied shard
# must already be listening, rather than racing Python's interpreter startup.
ready=0
for _ in $(seq 150); do
  kill -0 "$PROXY_PID" 2>/dev/null || { cat "$W/proxy.err"; echo 'owned proxy exited before readiness'; exit 1; }
  if (exec 3<>/dev/tcp/127.0.0.1/"$PP") 2>/dev/null; then ready=1; break; fi
  sleep .1
done
[ "$ready" = 1 ] || { cat "$W/proxy.err"; echo 'owned proxy did not listen'; exit 1; }
kill -0 "$PROXY_PID" 2>/dev/null || { cat "$W/proxy.err"; echo 'owned proxy exited at readiness'; exit 1; }
"$TARN" --config "$W/proxy-gateway.json" --listen "127.0.0.1:$PORT" --allow-all --threads 2 --default-engine "$ENGINE" > "$W/gateway.log" 2>&1 &
echo $! > "$W/gateway.pid"
"$TARN" --config "$W/recovery.json" --listen "127.0.0.1:$RECOVERY_PORT" --allow-all --threads 2 --default-engine "$ENGINE" > "$W/recovery-gateway.log" 2>&1 &
echo $! > "$W/recovery-gateway.pid"
"$PYTHON" - "$PORT" "$RECOVERY_PORT" "$PP" <<'PY'
import socket, sys, time
for port in map(int,sys.argv[1:]):
    deadline=time.monotonic()+15
    while True:
        try:
            with socket.create_connection(('127.0.0.1',port),timeout=.5):break
        except OSError:
            assert time.monotonic()<deadline, 'gateway/proxy did not listen'
            time.sleep(.1)
PY
"$PYTHON" "$ROOT/tests/sql/gc_effects.py" --root "$ROOT" --work "$W" --tarn "127.0.0.1:$PORT" --recovery "127.0.0.1:$RECOVERY_PORT" > "$W/runner.log" 2>&1
if grep -h 'lock copy differs\|yielded inside' "$W"/*.log; then exit 1; fi
echo 'GC EFFECT RECONCILIATION PASSED (finite induced loss, single-node WAL restart)'
