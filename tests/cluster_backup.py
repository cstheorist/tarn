#!/usr/bin/env python3
"""Owned RF3/fsync two-shard cluster-cut, fault and exact restore drill."""
import argparse
import hashlib
import json
import os
import pathlib
import socket
import subprocess
import sys
import time
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / 'scripts'))
import cluster_backup as cb


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('--work', required=True)
    ap.add_argument('--engine', choices=['memtx', 'vinyl'], default='memtx')
    ap.add_argument('--rows', type=int, default=256)
    ap.add_argument('--output', required=True)
    args = ap.parse_args()
    work = pathlib.Path(args.work).absolute(); work.mkdir(mode=0o700)
    T = os.getenv('TARANTOOL', 'tarantool')
    ports, sockets = [], []
    for _ in range(6):
        s = socket.socket(); s.bind(('127.0.0.1', 0)); ports.append(s.getsockname()[1]); sockets.append(s)
    for s in sockets:
        s.close()
    uris = ['tarn:tarn@127.0.0.1:' + str(p) for p in ports]
    peers = {str(i + 1): uris[i * 3:i * 3 + 3] for i in range(2)}
    nodes = [dict(name='n' + str(i), shard=i // 3 + 1, uri_env='CB_NODE_' + str(i), source=str(work / ('source' + str(i)))) for i in range(6)]
    for i, n in enumerate(nodes):
        os.environ[n['uri_env']] = uris[i]
    os.environ['CB_ENGINE'] = args.engine
    plan = dict(format=cb.FORMAT, nodes=nodes)
    procs, logs, checks, controllers = {}, [], [], []
    record = dict(format='tarn-cluster-backup-drill-v1', settings=vars(args), checks=checks,
        source_revision=os.getenv('TARN_SOURCE_REVISION') or subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=ROOT, text=True).strip(),
        source_sha256={str(p.relative_to(ROOT)): hashlib.sha256(p.read_bytes()).hexdigest() for p in
            [ROOT/'scripts/cluster_backup.py', ROOT/'scripts/backup.py', ROOT/'scripts/backup_rpc.lua',
             ROOT/'tests/cluster_backup.py', ROOT/'tests/cluster_backup_setup.lua', ROOT/'tests/cluster_backup_retry.lua',
             *sorted((ROOT/'lua').glob('*.lua'))]},
        native=os.getenv('TARN_NATIVE', '0'), boundary='same-host RF3 correctness and outage timings; not independent failure domains or production capacity')
    native_path = pathlib.Path(record['native'])
    record['native_sha256'] = hashlib.sha256(native_path.read_bytes()).hexdigest() if native_path.is_file() else None
    record['environment'] = {'uname': list(os.uname()), 'python': sys.version,
        'tarantool': subprocess.check_output([T, '--version'], text=True).splitlines()[0],
        'cgroup': {name: (pathlib.Path('/sys/fs/cgroup')/name).read_text() for name in
                   ('cpu.max','memory.max','cpu.stat','memory.events') if (pathlib.Path('/sys/fs/cgroup')/name).is_file()}}

    def save():
        pathlib.Path(args.output).write_text(json.dumps(record, indent=2) + '\n')

    def start(i, directory):
        env = dict(os.environ, TARN_SHARD_ID=str(i//3+1), TARN_LISTEN='127.0.0.1:' + str(ports[i]),
            TARN_WORKDIR=str(directory), TARN_REPLICAS=json.dumps(peers[str(i//3+1)]), TARN_PEERS=json.dumps(peers),
            TARN_ELECTION_MODE='candidate' if i % 3 == 0 else 'voter', TARN_MEMORY_MB='64',
            TARN_VINYL_MEMORY_MB='16', TARN_VINYL_CACHE_MB='0', TARN_WAL_MODE='fsync',
            TARN_HOST_INSTANCES='6', TARN_TEST_HOOKS='1', TARN_CHECK_YIELDS='1', TARN_LOG_LEVEL='3')
        log = (work / ('node%d-%s.log' % (i, uuid.uuid4().hex))).open('w'); logs.append(log)
        p = subprocess.Popen([T, str(ROOT/'lua/storage.lua')], env=env, stdout=log, stderr=log)
        procs[i] = p

    def stop(i, kill=False):
        p = procs.pop(i, None)
        if p and p.poll() is None:
            p.kill() if kill else p.terminate()
            p.wait(timeout=15)

    def rpc(i, code, timeout=30):
        p = work / ('rpc-' + uuid.uuid4().hex + '.lua'); p.write_text(code)
        try:
            r = subprocess.run([T, str(ROOT/'tests/backup_rpc.lua'), uris[i], str(p), str(timeout)],
                               capture_output=True, text=True, timeout=timeout+10)
            if r.returncode:
                raise RuntimeError(r.stderr[-3000:])
            return json.loads(r.stdout)
        finally:
            p.unlink()

    def wait(i, code, timeout=60):
        until = time.monotonic() + timeout; last = None
        while time.monotonic() < until:
            if i in procs and procs[i].poll() is not None:
                raise RuntimeError('owned node exited: ' + str(i))
            try:
                last = rpc(i, code, 3)
                if last:
                    return last
            except (RuntimeError, subprocess.TimeoutExpired) as e:
                last = str(e)
            time.sleep(.1)
        raise RuntimeError('readiness timeout: ' + str(last))

    def checked(label):
        checks.append(label); print('PASS ' + label, flush=True); save()

    def stage(action):
        code = 'local action=' + json.dumps(action) + '\n'
        if action == 'lazy_drain':
            code += 'local fixture_retry=(function()\n' + (ROOT/'tests/cluster_backup_retry.lua').read_text() + '\nend)()\n'
        reply = rpc(0, code + (ROOT/'tests/cluster_backup_setup.lua').read_text())
        if action == 'lazy_drain':
            record['lazy_drain'] = reply
            save()  # Preserve exact returned attempts before raising a stage failure.
            if not isinstance(reply, dict) or reply.get('status') != 'ok':
                raise RuntimeError('lazy_drain failed: ' + json.dumps(reply))
        return reply

    try:
        for i, n in enumerate(nodes):
            start(i, n['source'])
        for i in range(6):
            wait(i, "return rawget(_G,'tarn')~=nil and box.space._tarn_meta~=nil")
        for i in (0, 3):
            wait(i, "return not box.info.ro and tarn.clock.check({1,2})==nil")
            rpc(i, """
local function good(r) assert(r[1]=='ok',require('json').encode(r)) end
good(tarn_bucket_init(2,{{SHARD,SHARD}}))
good(tarn_create_table{id=88001,name='test.t',ver=0,engine=ENGINE,json='{}',lazy={},
 format={{name='id',type='unsigned'},{name='v',type='string'},{name='bucket_id',type='unsigned'}},
 indexes={{name='primary',unique=true,parts={{1,'unsigned'}}},{name='bucket_id',unique=false,parts={{3,'unsigned'}}}}})
for id=1,ROWS do
 good(tarn_autocommit({{'insert',88001,{id+(SHARD-1)*ROWS,string.rep(tostring(id)..'/',64),SHARD}}},{fence={88001,0},wb={SHARD}}))
end
return true
""".replace('SHARD', str(i//3+1)).replace('ENGINE', json.dumps(args.engine)).replace('ROWS', str(args.rows)))
        checked('two RF3/fsync shards loaded through Tarn mutation procedures')
        members = {'ver': 1, 'shards': [{'id': i+1, 'state': 'active', 'replicas': peers[str(i+1)]} for i in range(2)]}
        for i in (0,3):
            # Publish once. A known post-publication readiness error is inspected,
            # never treated as permission to resend the administrative mutation.
            wait(i,"return not box.info.ro and tarn.clock.check({1,2})==nil")
            r = rpc(i, 'local m=require("json").decode(' + json.dumps(json.dumps(members)) + ');return tarn_members_set(m)')
            assert r[0]=='ok' or (r[0]=='error' and r[1]==1180),r
            wait(i,"return box.space._tarn_meta:get{'members'}[2].ver==1 and tarn.clock.check({1,2})==nil")
        checked('published membership version is present and ready on both shards')
        record['native_probe'] = stage('native_probe')
        checked('configured native dispatch or explicit Lua-only path exercised')
        c = cb.Cluster(nodes, T)
        try:
            cb.wait_ready(c, nodes, cb.expected_groups(nodes), 30)
            rpc(0,"local s=box.schema.space.create('test.local_only',{is_local=true});s:create_index('pk');s:insert{1};return true")
            try:
                cb.wait_ready(c,nodes,cb.expected_groups(nodes),0)
                raise AssertionError('persistent local space accepted')
            except ValueError:
                checked('unreplicated persistent local space refuses capture')
            rpc(0,"box.space['test.local_only']:drop();return true")
            incomplete_nodes = nodes[:-1]
            try:
                cb.validate_inventory(incomplete_nodes, {k:v for k,v in c.inspect().items() if k!='n5'})
                raise AssertionError('omitted voter accepted')
            except ValueError:
                checked('missing registered replica refused')
            # Real prepared transaction: capture must not call it a clean cut.
            for i in range(6):
                rpc(i, 'tarn.cfg.recovery_after=3600;return true')
            for i in (0, 3):
                r = rpc(i, """
tarn.fresh.begin_stream()
return tarn_prepare('backup-pending',1,{1,2},{{'replace',88001,{KEY,'committed-cut',SHARD}}},{},{},require('fiber').time())
""".replace('SHARD', str(i//3+1)).replace('KEY',str(1+(i//3)*args.rows)))
                assert r[0] == 'yes', r
            try:
                cb.wait_ready(c, nodes, cb.expected_groups(nodes), 0)
                raise AssertionError('pending transaction accepted')
            except ValueError:
                checked('real prepared/staging distributed transaction refuses capture')
            for i in (0, 3):
                r = rpc(i, "return tarn_apply('backup-pending')")
                assert r[0] == 'ok', r
            cb.wait_ready(c, nodes, cb.expected_groups(nodes), 30)
            checked('same distributed decision applied and drained before cut')
            assert rpc(0,"return tarn_job_begin({owner='backup-ddl',kind='alter',space='test.t',names={'test.t'},ver=1,a={}},0)")[0]=='ok'
            try:
                cb.wait_ready(c,nodes,cb.expected_groups(nodes),0)
                raise AssertionError('unfinished DDL job accepted')
            except ValueError:
                checked('unfinished DDL job refuses capture')
            assert rpc(0,"return tarn_job_end('backup-ddl')")[0]=='ok'
            stage('lazy_create'); stage('lazy_pending')
            try:
                cb.wait_ready(c,nodes,cb.expected_groups(nodes),0)
                raise AssertionError('unshipped lazy assignment accepted')
            except ValueError:
                checked('paused real lazy delivery refuses capture without discarding obligations')
            stage('lazy_drain')
            cb.wait_ready(c,nodes,cb.expected_groups(nodes),30)
            checked('lazy delivery drained with persistent versions, frontiers and GC floors')
            stage('move_begin')
            try:
                cb.wait_ready(c,nodes,cb.expected_groups(nodes),0)
                raise AssertionError('unfinished move accepted')
            except ValueError:
                checked('real sending/receiving bucket move refuses capture')
            stage('move_finish')
            cb.wait_ready(c,nodes,cb.expected_groups(nodes),30)
            checked('completed move preserves exact rows and lazy metadata under the new owner')
        finally:
            c.close()
        rpc(3,"""
__backup_inflight={}
require('fiber').create(function()
 box.begin();box.space[88001]:replace{998,'uncommitted',1}
 __backup_inflight.ready=true
 while not __backup_inflight.release do require('fiber').sleep(.01) end
 __backup_inflight.ok=pcall(box.commit);__backup_inflight.done=true
end)
return true
""")
        wait(3,'return __backup_inflight.ready')
        start_time = time.monotonic()
        cut = cb.fence(plan, work/'backup', T, 30)
        checked('all replicas durably fenced and exact coherent cut audited')
        rpc(3,'__backup_inflight.release=true;return true')
        wait(3,'return __backup_inflight.done')
        assert rpc(3,'return not __backup_inflight.ok and box.space[88001]:get{998}==nil')
        checked('pre-fence uncommitted mutation is aborted rather than omitted as committed data')
        try:
            cb.restore(work/'backup', work/'premature-restore', 0)
            raise AssertionError('incomplete capture restored')
        except ValueError:
            checked('interrupted capture has no restorable cluster manifest')
        # Reject actual post-fence native/raw writes; no uncertain write retry.
        assert rpc(0, "local ok=pcall(function() box.space[88001]:replace{999,'forbidden',1} end);return not ok")
        assert rpc(0, "local ok,r=pcall(tarn_autocommit,{{'replace',88001,{999,'forbidden',1}}},{fence={88001,0},wb={1}});return not ok or r[1]~='ok'")
        checked('storage and raw persistent mutations refused under the fence')
        stage('native_fenced')
        checked('native dispatch and Lua handback cannot bypass the cluster fence')
        # Crash a fenced node and prove startup cannot re-enable writes.
        stop(0, kill=True); start(0, nodes[0]['source'])
        wait(0, "return box.info.ro and box.cfg.read_only and rawget(_G,'tarn')==nil")
        checked('crashed fenced leader restarts without writable application initialization')
        # Kill the owned copy controller during a paced physical transfer.
        interrupted = work/'interrupted-copy'
        cmd = [sys.executable,str(ROOT/'scripts/cluster_backup.py'),'--tarantool',T,'copy-node',
               '--cut',str(work/'backup'/'cut.json'),'--node','n0','--output',str(interrupted),'--rate-mib','.001']
        p = subprocess.Popen(cmd,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); controllers.append(p)
        token = wait(0,"local b=rawget(_G,'__tarn_file_backup');return b and b.token or false")
        p.kill(); p.wait(timeout=10)
        assert (interrupted/'INCOMPLETE').exists() and not (interrupted/'manifest.json').exists()
        bridge = cb.backup.Bridge(T,nodes[0]['uri_env'])
        try:
            assert bridge.call('check',token)
            bridge.call('stop',token)  # exact owned GC lease; no mutation replay
        finally:
            bridge.close()
        checked('killed copy controller leaves incomplete files and durable source fences')
        for n in nodes:
            cb.copy_node(cut, n['name'], work/'backup'/n['name'], T, 0)
        manifest = cb.seal(cut, work/'backup', T)
        record['cut'] = {'token': cut['token'], 'fence_s': cut['fence_elapsed_s'],
                         'fence_to_seal_s': time.monotonic()-start_time,
                         'manifest_sha256': cb.sha(manifest)}
        checked('complete cluster manifest binds all physical replicas to the same cut')
        cb.release(cut, T)
        for i in range(6):
            stop(i)
        for i,n in enumerate(nodes):
            start(i,n['source'])
        for i in (0,3):
            wait(i,"return rawget(_G,'tarn')~=nil and not box.info.ro and tarn.clock.check({1,2})==nil")
        assert rpc(3,"return tarn_autocommit({{'replace',88001,{1000,'after-cut',1}}},{fence={88001,0},wb={1}})")[0]=='ok'
        record['cut']['source_resume_to_write_s'] = time.monotonic()-start_time
        checked('explicit source release and restart restore service after the frozen cut')
        for i in range(6):
            stop(i)
        start_time = time.monotonic()
        interrupted_restore = work/'interrupted-restore'
        cmd = [sys.executable,str(ROOT/'scripts/cluster_backup.py'),'restore','--backup',str(work/'backup'),
               '--output',str(interrupted_restore),'--rate-mib','.001']
        p = subprocess.Popen(cmd,stdout=subprocess.DEVNULL,stderr=subprocess.DEVNULL); controllers.append(p)
        deadline = time.monotonic()+10
        while not (interrupted_restore/'n0'/cb.MARKER).exists() and time.monotonic()<deadline:
            if p.poll() is not None:
                raise AssertionError('paced restore exited before interruption')
            time.sleep(.01)
        assert (interrupted_restore/'n0'/cb.MARKER).exists()
        p.kill();p.wait(timeout=10)
        try:
            cb.activate(interrupted_restore,T)
            raise AssertionError('interrupted restore activated')
        except ValueError:
            checked('killed restore controller leaves whole-cluster and per-node startup fences')
        # Corrupt a physical byte without changing its trusted manifest.
        first = cb.read(work/'backup'/'n0'/'manifest.json')['files'][0]['path']
        path = work/'backup'/'n0'/'files'/first
        with path.open('r+b') as f:
            f.seek(-1,2); old=f.read(1); f.seek(-1,2); f.write(bytes([old[0]^1]))
        try:
            try:
                cb.restore(work/'backup',work/'corrupt-restore',0)
                raise AssertionError('corrupted physical copy restored')
            except ValueError:
                checked('corrupted physical copy cannot produce a verified cluster restore')
        finally:
            with path.open('r+b') as f:
                f.seek(-1,2); f.write(old)
        start_time = time.monotonic()
        cb.restore(work/'backup', work/'restore', 0)
        assert (work/'restore'/'INCOMPLETE').exists()
        checked('physical restore stays quarantined until whole-cluster validation')
        start(0, work/'restore'/'n0')
        wait(0,"return box.info.ro and next(box.cfg.replication or {})==nil and rawget(_G,'tarn')==nil")
        stop(0)
        checked('quarantined startup has no replication, writable service or application initialization')
        verified = cb.verify_restore(work/'restore', T)
        record['restore_copy_verify_s'] = time.monotonic()-start_time
        record['verified_spaces'] = {name: audit['spaces'] for name,audit in verified['verified'].items()}
        checked('every user row, schema and persisted protocol space matches the cut')
        marker = work/'restore'/'n0'/cb.MARKER
        contents = marker.read_bytes(); marker.unlink()
        start(0, work/'restore'/'n0')
        assert procs[0].wait(timeout=10) != 0
        stop(0); marker.write_bytes(contents)
        checked('interrupted activation refuses startup when a child marker is already removed')
        cb.activate(work/'restore', T)
        for i in range(6):
            start(i, work/'restore'/nodes[i]['name'])
        for i in (0, 3):
            wait(i, "return rawget(_G,'tarn')~=nil and not box.info.ro and tarn.clock.check({1,2})==nil")
        assert rpc(0, 'return box.space[88001]:len()') == 0
        actual = rpc(3, "return {box.space[88001]:len(),box.space[88001]:get{1}[2],box.space[88001]:get{999}==nil}")
        assert actual == [args.rows*2, 'committed-cut', True], actual
        assert rpc(3,'return box.space[88001]:get{1000}==nil')
        checked('post-cut source acknowledgement is outside the documented RPO')
        assert rpc(3, "return tarn_autocommit({{'replace',88001,{999,'after-restore',1}}},{fence={88001,0},wb={1}})")[0]=='ok'
        checked('isolated restored RF3 cluster serves exact acknowledged data and new writes')
        stage('post_restore')
        checked('late retained lazy frame cannot resurrect membership after GC, move and restore')
        record['restore_to_verified_writes_s'] = time.monotonic()-start_time
        record['status']='passed'; save()
    except BaseException as e:
        record['status']='failed'; record['error']=str(e)
        record['failure_nodes']={}
        for i in procs:
            try:
                record['failure_nodes'][str(i)] = rpc(i,'return {ro=box.info.ro,election=box.info.election,synchro=box.info.synchro,replication=box.info.replication}',3)
            except (RuntimeError,subprocess.TimeoutExpired) as diagnostic:
                record['failure_nodes'][str(i)] = str(diagnostic)
        save(); raise
    finally:
        for p in controllers:
            if p.poll() is None:
                p.kill();p.wait(timeout=10)
        for i in list(procs):
            stop(i)
        for log in logs:
            log.close()


if __name__ == '__main__':
    main()
