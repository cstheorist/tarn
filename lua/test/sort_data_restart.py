#!/usr/bin/env python3
"""A storage node's restart leaves no memtx MVCC story per recovered row (#218).

With MVCC on, Tarantool builds a memtx space's secondary keys during
snapshot recovery when the next space's first row arrives, inside that
row's transaction, and so tracks a read of every row: one story each,
held until later transactions release them. Built from a checkpoint's sort
data instead, the keys need no such scan, so storage nodes run with
memtx_use_sort_data. The control restarts the same node without it.
"""
import argparse
import json
import os
from pathlib import Path
import socket
import subprocess
import tempfile
import time

ROOT = Path(__file__).resolve().parents[2]
ROWS = 20000
LOAD = """
local function space(name, id, secondary)
    local s = box.schema.space.create(name, {id = id, if_not_exists = true})
    s:create_index('pk', {parts = {1, 'unsigned'}, if_not_exists = true})
    if secondary then s:create_index('k', {parts = {2, 'string'}, unique = false, if_not_exists = true}) end
    return s
end
-- Snapshot order is space ID order: the indexed space is followed by
-- another one with rows, the case that left a story per row.
local a, b = space('sort_data_indexed', 20001, true), space('sort_data_next', 20002, false)
for i = 1, %d, 1000 do
    box.begin()
    for j = i, i + 999 do a:replace{j, 'k' .. j %% 97}; b:replace{j, 'v'} end
    box.commit()
end
box.snapshot()
return {rows = a:len()}
""" % ROWS
STATE = """
return {ready = rawget(_G, 'tarn') ~= nil and box.info.status == 'running',
        sort_data = box.cfg.memtx_use_sort_data, rows = box.space.sort_data_indexed and box.space.sort_data_indexed:len(),
        stories = box.stat.memtx.tx().mvcc.tuples.used.stories.count}
"""


def main():
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument('work', type=Path)
    args = ap.parse_args()
    work = args.work.resolve()
    work.mkdir()
    tool = os.getenv('TARANTOOL', 'tarantool')
    s = socket.socket()
    s.bind(('127.0.0.1', 0))
    port = s.getsockname()[1]
    s.close()
    uri = 'tarn:tarn@127.0.0.1:%d' % port
    log = (work / 'node.log').open('a')
    proc = None

    def start(sort_data):
        nonlocal proc
        env = dict(os.environ, TARN_SHARD_ID='1', TARN_LISTEN='127.0.0.1:%d' % port,
                   TARN_WORKDIR=str(work / 'node'), TARN_PEERS=json.dumps({'1': [uri]}),
                   TARN_MEMORY_MB='128', TARN_HOST_INSTANCES='1', TARN_LOG_LEVEL='5',
                   TARN_MEMTX_SORT_DATA='1' if sort_data else '0')
        proc = subprocess.Popen([tool, str(ROOT / 'lua/storage.lua')], env=env, stdout=log, stderr=log)

    def stop():
        proc.terminate()
        proc.wait(timeout=30)

    def ev(code, timeout=60):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.lua', dir=work) as script:
            script.write(code)
            script.flush()
            r = subprocess.run([tool, str(ROOT / 'tests/backup_rpc.lua'), uri, script.name, str(timeout)],
                               capture_output=True, text=True, timeout=timeout + 10)
        if r.returncode:
            raise RuntimeError(r.stderr[-2000:])
        return json.loads(r.stdout)

    def ready():
        deadline, last = time.monotonic() + 60, None
        while time.monotonic() < deadline:
            if proc.poll() is not None:
                raise RuntimeError('storage node exited; see ' + str(work / 'node.log'))
            try:
                last = ev(STATE, 5)
                if last['ready']:
                    return last
            except (RuntimeError, subprocess.TimeoutExpired, ValueError) as e:
                last = str(e)
            time.sleep(.1)
        raise RuntimeError('storage node not ready: ' + str(last))

    try:
        start(True)
        ready()
        assert ev(LOAD)['rows'] == ROWS
        stop()
        assert any((work / 'node').glob('*.sortdata')), 'the checkpoint has no sort data'
        start(True)
        after = ready()
        print('restart with sort data:', json.dumps(after), flush=True)
        assert after['sort_data'] is True and after['rows'] == ROWS, after
        assert after['stories'] < ROWS // 10, ('a story per recovered row', after)
        print('PASS a restart builds secondary keys from the sort data: no story per recovered row', flush=True)
        stop()
        # Control: the same checkpoint recovered without sort data.
        start(False)
        control = ready()
        print('restart without sort data:', json.dumps(control), flush=True)
        assert control['sort_data'] is False and control['stories'] >= ROWS // 2, control
        print('PASS control: without sort data the restart leaves a story per recovered row', flush=True)
    finally:
        if proc is not None and proc.poll() is None:
            proc.kill()
            proc.wait(timeout=10)
        log.close()


if __name__ == '__main__':
    main()
