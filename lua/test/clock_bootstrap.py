#!/usr/bin/env python3
"""Owned RF3/fsync delayed-join and existing-policy restart regression."""
import argparse
import hashlib
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
STATE = """
local m=box.space._tarn_meta
return {running=box.info.status=='running',ro=box.info.ro,
 marker=m~=nil and m:get{'shard_id'}~=nil,
 policy=m~=nil and m:get{'clock:policy:1'}~=nil,
 loaded=tarn~=nil and tarn.clock.loaded or false,
 admitted=tarn~=nil and tarn.clock.check({1})==nil or false,
 wal=box.cfg.wal_mode,quorum=box.cfg.replication_synchro_quorum}
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('work', type=Path)
    args = ap.parse_args()
    work = args.work.resolve(); work.mkdir()
    tool = os.getenv('TARANTOOL', 'tarantool')
    native = os.getenv('TARN_NATIVE') or str(ROOT/'target/release/libtarn_storage.so')
    native_file = Path(native)
    if os.getenv('TARN_NATIVE') not in (None, '', '0'):
        assert native_file.is_file(), 'explicit native module is missing'
    native_expected = native != '0' and native_file.is_file()
    sockets = [socket.socket() for _ in range(3)]
    for s in sockets: s.bind(('127.0.0.1', 0))
    ports = [s.getsockname()[1] for s in sockets]
    for s in sockets: s.close()
    uris = ['tarn:tarn@127.0.0.1:' + str(p) for p in ports]
    procs, logs = {}, []
    record = dict(complete=False, checks=[], observations=[],
        source_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest()
                       for p in [*sorted((ROOT/'lua').rglob('*.lua')), ROOT/'tests/backup_rpc.lua', Path(__file__)]},
        tarantool=subprocess.check_output([tool, '--version'], text=True).splitlines()[0],
        native=native, native_expected=native_expected,
        native_sha256=hashlib.sha256(native_file.read_bytes()).hexdigest() if native_expected else None,
        boundary='Owned same-host RF3/fsync ordering and restart controls; not a general Raft/checkpoint proof.')

    def save():
        (work/'receipt.json').write_text(json.dumps(record, indent=2)+'\n')

    def start(i):
        assert i not in procs
        env = dict(os.environ, TARN_SHARD_ID='1', TARN_LISTEN='127.0.0.1:'+str(ports[i]),
            TARN_WORKDIR=str(work/('node'+str(i))), TARN_REPLICAS=json.dumps(uris),
            TARN_PEERS=json.dumps({'1': uris}), TARN_ELECTION_MODE='candidate' if i==0 else 'voter',
            TARN_WAL_MODE='fsync', TARN_MEMORY_MB='96', TARN_VINYL_MEMORY_MB='16',
            TARN_VINYL_CACHE_MB='0', TARN_HOST_INSTANCES='3', TARN_VINYL_READ_THREADS='1',
            TARN_CHECK_YIELDS='1', TARN_CHECK_LOCKS='1', TARN_TEST_HOOKS='1', TARN_LOG_LEVEL='4')
        log = (work/('node'+str(i)+'.log')).open('a'); logs.append(log)
        procs[i] = subprocess.Popen([tool, str(ROOT/'lua/storage.lua')], env=env, stdout=log, stderr=log)

    def stop(i):
        p = procs.pop(i, None)
        if p is None or p.poll() is not None: return
        p.terminate()
        try: p.wait(timeout=10)
        except subprocess.TimeoutExpired:
            p.kill(); p.wait(timeout=5)

    def ev(i, code):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.lua', dir=work) as script:
            script.write(code); script.flush()
            r = subprocess.run([tool, str(ROOT/'tests/backup_rpc.lua'), uris[i], script.name, '3'],
                               capture_output=True, text=True, timeout=6)
        if r.returncode: raise RuntimeError(r.stderr[-2000:])
        return json.loads(r.stdout)

    def wait(i, predicate, timeout=35):
        deadline = time.monotonic()+timeout; last = None
        while time.monotonic()<deadline:
            for number,p in procs.items():
                if p.poll() is not None: raise RuntimeError('owned node exited: '+str(number))
            try:
                last=ev(i, STATE)
                if predicate(last): return last
            except (RuntimeError, subprocess.TimeoutExpired) as error:
                last=str(error)
            time.sleep(.05)
        raise RuntimeError('read-only readiness timeout: '+str(last))

    def check(label):
        record['checks'].append(label); save(); print('PASS '+label, flush=True)

    def observe(i, label):
        state=ev(i, STATE); record['observations'].append(dict(node=i,label=label,state=state)); save()
        assert state['wal']=='fsync' and state['quorum']=='N / 2 + 1',state
        return state

    try:
        save(); start(0)
        wait(0,lambda s:s['running'])
        # Allow several real monitor ticks while the configured voters are
        # deliberately absent. No runtime hook skips schema or clock work.
        time.sleep(.5)
        state=observe(0,'configured voters have not started')
        assert not state['marker'] and not state['policy'] and not state['loaded'] and not state['admitted'],state
        check('clock policy remains absent before initial configured replicas join')
        start(1); start(2)
        for i in range(3):
            wait(i,lambda s:s['running'] and s['marker'] and s['policy'])
            observe(i,'initial join complete')
        wait(0,lambda s:s['loaded'] and s['admitted'])
        record['native_runtime']=ev(0,'return tarn_info().native')
        assert (record['native_runtime'] and record['native_runtime']['ready']==1) if native_expected else record['native_runtime'] is False
        check('full initial join installs confirmed marker and timing policy')
        for sid,engine in ((88901,'memtx'),(88902,'vinyl')):
            r=ev(0,"""
local r=tarn_create_table{id=SID,name='test.clock_ENGINE',ver=0,engine='ENGINE',json='{}',lazy={},
 format={{name='id',type='unsigned'},{name='v',type='unsigned'},{name='bucket_id',type='unsigned'}},
 indexes={{name='primary',unique=true,parts={{1,'unsigned'}}}}}
return r
""".replace('SID',str(sid)).replace('ENGINE',engine))
            assert r[0]=='ok',r
            r=ev(0,"return tarn_autocommit({{'insert',SID,{1,7,1}}},{fence={SID,0}})".replace('SID',str(sid)))
            assert r[0]=='ok',r
        check('memtx and Vinyl mutations each acknowledge once after initialization')
        stop(2); stop(0); start(0)
        wait(0,lambda s:not s['ro'] and s['loaded'] and s['policy'] and s['admitted'])
        observe(0,'restart with one configured voter still absent')
        check('existing policy and quorum admission recover without every configured voter present')
        start(2)
        for i in range(3): wait(i,lambda s:s['running'] and s['marker'] and s['policy'])
        wait(0,lambda s:s['admitted'])
        r=ev(0,"""
tarn.fresh.barrier(nil,true)
box.begin{txn_isolation='read-confirmed'}
local a,b=box.space[88901]:get{1},box.space[88902]:get{1}
box.rollback()
return {a:totable(),b:totable()}
""")
        assert r==[[1,7,1],[1,7,1]],r
        record['readback']=r
        check('rejoined RF3 cluster preserves both exact acknowledged tuples after leader restart')
        record['complete']=True
    except BaseException as error:
        record['failure']=repr(error)
        raise
    finally:
        for i in list(procs): stop(i)
        for log in logs: log.close()
        save()


if __name__=='__main__': main()
