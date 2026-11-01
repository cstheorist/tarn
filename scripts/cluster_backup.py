#!/usr/bin/env python3
"""Quiescent whole-cluster backup and quarantined restore (see docs/cluster-backup.md).

All replicas must be inventoried and reachable. Stop application/admin admissions
before starting. Refusals never discard obligations or retry a sent mutation.
Source fences persist on failure and need explicit release and process restart.
"""
import argparse
import hashlib
import json
import math
import os
import pathlib
import re
import subprocess
import sys
import time
import urllib.parse
import uuid

import backup

FORMAT = 'tarn-cluster-backup-v1'
MARKER = 'tarn-maintenance.json'


def encoded(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':')).encode()


def sha(value):
    return hashlib.sha256(encoded(value)).hexdigest()


def read(path):
    p = pathlib.Path(path)
    if p.is_symlink() or p.stat().st_size > 64 * backup.CHUNK:
        raise ValueError('invalid or oversized manifest')
    return json.loads(p.read_text())


def write(path, value):
    p = pathlib.Path(path)
    with p.open('x') as f:
        os.chmod(p, 0o600)
        json.dump(value, f, indent=2, sort_keys=True)
        f.write('\n'); f.flush(); os.fsync(f.fileno())
    backup.sync_dir(p.parent)


def plan_nodes(plan):
    nodes = plan['nodes']
    if plan.get('format') != FORMAT or not isinstance(nodes, list) or not 1 <= len(nodes) <= 1024:
        raise ValueError('invalid plan format or replica count')
    names, sources, envs = set(), set(), set()
    for n in nodes:
        if (not re.fullmatch(r'[a-zA-Z0-9_-]{1,80}', n['name']) or n['name'] in names
                or type(n['shard']) is not int or n['shard'] < 1
                or not re.fullmatch(r'[A-Z_][A-Z_0-9]*', n['uri_env']) or n['uri_env'] in envs
                or not pathlib.Path(n['source']).is_absolute()
                or (n.get('host', ''), n['source']) in sources):
            raise ValueError('invalid or duplicate node identity, secret variable or source directory')
        names.add(n['name']); envs.add(n['uri_env']); sources.add((n.get('host', ''), n['source']))
    return nodes


def endpoint(uri):
    p = urllib.parse.urlsplit('//' + uri)
    if p.scheme or not p.hostname or not p.port or p.path or p.query or p.fragment:
        raise ValueError('initial cluster backup requires explicit TCP host:port endpoints')
    return {'host': p.hostname, 'service': str(p.port)}


def normalized_endpoints(items):
    return sorted((x['host'], x['service']) for x in items)


def expected_groups(nodes):
    groups = {}
    for n in nodes:
        groups.setdefault(str(n['shard']), []).append(endpoint(os.environ[n['uri_env']]))
    if len({encoded(e) for values in groups.values() for e in values}) != len(nodes):
        raise ValueError('duplicate endpoint')
    return {s: normalized_endpoints(values) for s, values in groups.items()}


class Cluster:
    def __init__(self, nodes, tarantool='tarantool', timeout=310):
        self.bridges = {}
        try:
            for n in nodes:
                self.bridges[n['name']] = backup.Bridge(tarantool, n['uri_env'], timeout)
        except BaseException:
            self.close()
            raise

    def call(self, name, op, *args):
        return self.bridges[name].call('cluster', op, args)

    def inspect(self):
        return {name: self.call(name, 'inspect') for name in self.bridges}

    def close(self):
        for bridge in self.bridges.values():
            bridge.close()


def validate_inventory(nodes, infos, expected=None, frozen=False, token=None):
    """Reject omitted voters/shards, mixed catalogs and incomplete routing cuts."""
    if set(infos) != {n['name'] for n in nodes}:
        raise ValueError('missing or extra replica receipt')
    ids, groups, catalogs = set(), {}, set()
    for n in nodes:
        x = infos[n['name']]
        if (x['shard'] != n['shard'] or x['uuid'] in ids or x['wal_mode'] != 'fsync'
                or not x['version'].startswith('3.9.0')):
            raise ValueError('replica identity, fsync or pinned Tarantool version differs: ' + n['name'])
        ids.add(x['uuid']); groups.setdefault(str(n['shard']), []).append(x)
        catalogs.add(x['catalog_sha256'])
        if expected is not None:
            if x['members']:
                active = {}
                for s in x['members']['shards']:
                    if s['state'] == 'removed':
                        continue
                    if s['state'] != 'active':
                        raise ValueError('membership is not settled')
                    active[str(s['id'])] = normalized_endpoints(s['replicas'])
            else:
                active = {s: normalized_endpoints(v) for s, v in x['peers'].items()}
            if active != expected:
                raise ValueError('plan does not cover the complete configured membership')
            if normalized_endpoints(x['replicas']) not in (expected[str(n['shard'])], []):
                raise ValueError('local replication configuration differs')
        if frozen:
            m = x.get('marker')
            if not (x['ro'] and x['read_only'] and x['election_mode'] == 'off' and m and m['token'] == token):
                raise ValueError('replica fence is absent or changed')
        if x['errors']:
            raise ValueError('cluster is not drained: ' + n['name'] + ': ' + ', '.join(x['errors']))
    if len(catalogs) != 1:
        raise ValueError('catalogs differ between shards')
    owners, count, sets = {}, None, set()
    membership = {sha(x['members']) for x in infos.values()}
    if len(membership) != 1:
        raise ValueError('membership versions or definitions differ')
    for shard, replicas in groups.items():
        x = replicas[0]
        registered = sorted(r['uuid'] for r in replicas)
        if any(r['registered'] != registered for r in replicas):
            raise ValueError('plan must include every registered replica, including offline/seed voters')
        rs = {r['replicaset_uuid'] for r in replicas}
        if len(rs) != 1 or sets & rs:
            raise ValueError('replicaset identity differs or is duplicated')
        sets |= rs
        if any(r['buckets'] != x['buckets'] or r['bucket_count'] != x['bucket_count'] for r in replicas):
            raise ValueError('replica routing differs')
        if frozen and len({sha(r['vclock']) for r in replicas}) != 1:
            raise ValueError('replicas have not reached the same frozen WAL cut')
        if count is None:
            count = x['bucket_count']
        if type(count) is not int or count < 1 or count > 1000000 or x['bucket_count'] != count:
            raise ValueError('invalid or inconsistent bucket count')
        for b in x['buckets']:
            if type(b) is not int or b < 1 or b > count or b in owners:
                raise ValueError('duplicate or invalid serving bucket owner')
            owners[b] = shard
    if len(owners) != count:
        raise ValueError('missing serving bucket owner')


def wait_ready(cluster, nodes, expected, seconds, frozen=False, token=None):
    deadline = time.monotonic() + seconds
    while True:
        infos = cluster.inspect()  # Read-only observations may be repeated.
        try:
            validate_inventory(nodes, infos, expected, frozen, token)
            return infos
        except ValueError:
            if time.monotonic() >= deadline:
                raise
            time.sleep(min(.2, max(0, deadline - time.monotonic())))


def unchanged(nodes, before, after, token):
    validate_inventory(nodes, after, frozen=True, token=token)
    for n in nodes:
        a, b = before[n['name']], after[n['name']]
        for field in ('uuid', 'replicaset_uuid', 'vclock', 'catalog_sha256', 'members', 'buckets'):
            if a[field] != b[field]:
                raise ValueError('frozen cut changed: ' + n['name'] + ': ' + field)


def fence(plan, output, tarantool, timeout):
    nodes = plan_nodes(plan)
    output = backup.begin(pathlib.Path(output))
    token = str(uuid.uuid4())
    journal = dict(format=FORMAT, token=token, plan=plan, plan_sha256=sha(plan))
    write(output / 'journal.json', journal)  # Available even after a lost FENCE reply.
    c = Cluster(nodes, tarantool)
    start = time.monotonic()
    try:
        expected = expected_groups(nodes)
        infos = wait_ready(c, nodes, expected, timeout)
        # Freeze followers before leaders. A new leader can arise until every
        # registered replica has installed its fence; final validation is mandatory.
        for n in sorted(nodes, key=lambda n: not infos[n['name']]['ro']):
            c.call(n['name'], 'fence', token, sha(plan))
        frozen = wait_ready(c, nodes, None, timeout, True, token)
        audits = {n['name']: c.call(n['name'], 'audit', token) for n in nodes}
        unchanged(nodes, frozen, {k: a['info'] for k, a in audits.items()}, token)
        for shard in {n['shard'] for n in nodes}:
            if len({sha(audits[n['name']]['spaces']) for n in nodes if n['shard'] == shard}) != 1:
                raise ValueError('replica data or protocol metadata differs at the frozen cut')
        cut = dict(journal, nodes=frozen, audits=audits, fence_elapsed_s=time.monotonic() - start,
                   cut_utc=time.strftime('%Y-%m-%dT%H:%M:%SZ', time.gmtime()))
        write(output / 'cut.json', cut)
        return cut
    finally:
        c.close()


def copy_node(cut, name, output, tarantool, rate):
    nodes = plan_nodes(cut['plan'])
    n = next(n for n in nodes if n['name'] == name)
    c = Cluster([n], tarantool)
    try:
        before = c.call(name, 'checkpoint', cut['token'])
        # Whole-cluster checks already sealed the cut; bind this one host's copy.
        for field in ('uuid', 'replicaset_uuid', 'vclock'):
            if before[field] != cut['nodes'][name][field]:
                raise ValueError('node changed before checkpoint')
        result = backup.create(argparse.Namespace(output=output, source=n['source'], uri_env=n['uri_env'],
                       tarantool=tarantool, rate_mib=rate, ttl=86400, seed_uuid=None))
        after = c.call(name, 'inspect')
        if (after['vclock'] != before['vclock'] or not after['marker']
                or after['marker']['token'] != cut['token'] or not after['read_only']):
            raise ValueError('node changed during physical copy')
        receipt = dict(format=FORMAT, token=cut['token'], cut_sha256=sha(cut), name=name,
                       file_manifest_sha256=sha(result))
        write(pathlib.Path(output) / 'cluster-node.json', receipt)
        return receipt
    finally:
        c.close()


def seal(cut, output, tarantool):
    out = pathlib.Path(output)
    nodes = plan_nodes(cut['plan'])
    c = Cluster(nodes, tarantool)
    try:
        unchanged(nodes, cut['nodes'], c.inspect(), cut['token'])
        copies = {}
        for n in nodes:
            root = out / n['name']
            receipt, manifest = read(root / 'cluster-node.json'), read(root / 'manifest.json')
            if ((root / 'INCOMPLETE').exists() or receipt['token'] != cut['token']
                    or receipt['cut_sha256'] != sha(cut) or receipt['name'] != n['name']
                    or receipt['file_manifest_sha256'] != sha(manifest)
                    or manifest['source_uuid'] != cut['nodes'][n['name']]['uuid']):
                raise ValueError('incomplete or mismatched physical-copy receipt')
            copies[n['name']] = receipt
        complete = dict(cut, copies=copies)
        backup.publish(out, 'cluster.json', complete)
        return complete
    finally:
        c.close()


def release(journal, tarantool):
    nodes = plan_nodes(journal['plan'])
    c = Cluster(nodes, tarantool)
    results = {}
    try:
        # Inspect first; do not resend an uncertain release. An absent marker is
        # reported, not changed. Nodes remain RO until the operator restarts them.
        for name, info in c.inspect().items():
            m = info['marker']
            if m and m['token'] != journal['token']:
                raise ValueError('another maintenance operation owns a node')
        for name in c.bridges:
            info = c.call(name, 'inspect')
            results[name] = c.call(name, 'release', journal['token']) if info['marker'] else {'released': False}
        return results
    finally:
        c.close()


def complete_backup(root):
    root = pathlib.Path(root)
    if (root / 'INCOMPLETE').exists():
        raise ValueError('incomplete cluster backup')
    m = read(root / 'cluster.json')
    nodes = plan_nodes(m['plan'])
    if m['format'] != FORMAT or set(m['copies']) != {n['name'] for n in nodes}:
        raise ValueError('incomplete cluster manifest')
    validate_inventory(nodes, m['nodes'], frozen=True, token=m['token'])
    for n in nodes:
        receipt = read(root / n['name'] / 'cluster-node.json')
        files = read(root / n['name'] / 'manifest.json')
        if receipt != m['copies'][n['name']] or sha(files) != receipt['file_manifest_sha256']:
            raise ValueError('physical manifest differs from cluster manifest')
    return m


def restore(root, output, rate):
    m = complete_backup(root)
    out = backup.begin(pathlib.Path(output))
    write(out / 'cluster-source.json', m)
    results = {}
    for n in plan_nodes(m['plan']):
        target = out / n['name']
        result = backup.restore(argparse.Namespace(backup=str(pathlib.Path(root) / n['name']),
                                 output=str(target), rate_mib=rate, seed_uuid=None,
                                 quarantine=dict(format=FORMAT, kind='restore', token=m['token'],
                                     uuid=m['nodes'][n['name']]['uuid'], cluster_sha256=sha(m))))
        # Keep each completed local copy quarantined, and also the parent
        # INCOMPLETE marker, until all copies and logical audits are validated.
        results[n['name']] = result
    write(out / 'copies.json', results)
    return {'copied': list(results), 'verified': False, 'quarantined': True}


def verify_restore(root, tarantool, publish=True):
    """Offline single-node probes: no listener, replication, elections or Tarn fibers."""
    root = pathlib.Path(root).absolute()
    m = read(root / 'cluster-source.json')
    results = {}
    for n in plan_nodes(m['plan']):
        target = root / n['name']
        marker = read(target / MARKER)
        if marker.get('kind') != 'restore' or marker.get('cluster_sha256') != sha(m):
            raise ValueError('restore quarantine identity differs')
        memory = m['nodes'][n['name']].get('memtx_memory', 134217728)
        if type(memory) is not int or memory < 16777216:
            raise ValueError('invalid source memory configuration')
        code = ("package.path=" + json.dumps(str(pathlib.Path(__file__).resolve().parents[1] / 'lua/?.lua;'))
                + "..package.path;box.cfg{work_dir=" + json.dumps(str(target))
                + ",read_only=true,election_mode='off',replication={},memtx_memory=" + str(memory) + ","
                  "vinyl_memory=67108864,vinyl_cache=0,wal_mode='fsync',log_level=2};"
                  "print(require('json').encode(require('tarn_backup').audit("
                + json.dumps(m['token']) + ")));os.exit()")
        # A probe never executes source-provided code; only path/token strings
        # enter this fixed program. Tarantool locks its data directory.
        r = subprocess.run([tarantool, '-e', code], capture_output=True, text=True, timeout=3600)
        if r.returncode:
            raise RuntimeError('isolated restore probe failed for ' + n['name'] + ': ' + r.stderr[-2000:])
        actual = json.loads(r.stdout)
        expected = m['audits'][n['name']]
        if actual['spaces'] != expected['spaces']:
            raise ValueError('restored data/schema/protocol digest mismatch: ' + n['name'])
        for field in ('uuid', 'replicaset_uuid', 'vclock', 'catalog_sha256', 'members', 'buckets', 'registered'):
            if actual['info'][field] != expected['info'][field]:
                raise ValueError('restored ' + field + ' differs: ' + n['name'])
        results[n['name']] = actual
    validate_inventory(plan_nodes(m['plan']), {k: v['info'] for k, v in results.items()},
                       frozen=True, token=m['token'])
    receipt = dict(format=FORMAT, cluster_sha256=sha(m), verified=results,
                   activation='Keep quarantine until isolated network and original configuration are installed.')
    if publish:
        backup.publish(root, 'verified.json', receipt)
    return receipt


def activate(root, tarantool):
    """Prepare a verified, stopped restore for startup in an isolated network."""
    root = pathlib.Path(root)
    if (root / 'INCOMPLETE').exists():
        raise ValueError('restore has not passed whole-cluster verification')
    m, receipt = read(root / 'cluster-source.json'), read(root / 'verified.json')
    if receipt['cluster_sha256'] != sha(m):
        raise ValueError('verification receipt differs')
    verify_restore(root, tarantool, publish=False)
    # Keep a per-node startup guard until the complete set is verified. This
    # operation is explicit because physical identities/endpoint names are reused.
    for n in plan_nodes(m['plan']):
        target = root / n['name']
        marker = read(target / MARKER)
        if marker.get('cluster_sha256') != sha(m):
            raise ValueError('quarantine marker differs')
    for n in plan_nodes(m['plan']):
        target = root / n['name']
        (target / MARKER).unlink(); backup.sync_dir(target)
    # Mere file existence is not a completion receipt. Publish only after the
    # entire receipt and every marker removal have reached durable storage.
    write(root / 'activated.json.partial', {'format': FORMAT, 'cluster_sha256': sha(m),
        'instances': {n['name']: m['nodes'][n['name']]['uuid']
                      for n in plan_nodes(m['plan'])}})
    (root / 'activated.json.partial').rename(root / 'activated.json')
    backup.sync_dir(root)
    return {'activated': True, 'restart_required': True}


def main():
    p = argparse.ArgumentParser(description=__doc__)
    p.add_argument('--tarantool', default=os.getenv('TARANTOOL', 'tarantool'))
    sub = p.add_subparsers(dest='command', required=True)
    for name in ('create', 'fence'):
        q = sub.add_parser(name); q.add_argument('--plan', required=True); q.add_argument('--output', required=True)
        q.add_argument('--timeout', type=float, default=60); q.add_argument('--rate-mib', type=float, default=64)
    q = sub.add_parser('copy-node'); q.add_argument('--cut', required=True); q.add_argument('--node', required=True)
    q.add_argument('--output', required=True); q.add_argument('--rate-mib', type=float, default=64)
    q = sub.add_parser('seal'); q.add_argument('--cut', required=True); q.add_argument('--output', required=True)
    q = sub.add_parser('release'); q.add_argument('--journal', required=True)
    q = sub.add_parser('restore'); q.add_argument('--backup', required=True); q.add_argument('--output', required=True)
    q.add_argument('--rate-mib', type=float, default=64)
    for name in ('verify-restore', 'activate'):
        q = sub.add_parser(name); q.add_argument('--restore', required=True)
    q.add_argument('--isolated-network', required=True, action='store_true',
                   help='confirm original nodes cannot be reached and no clients are admitted')
    args = p.parse_args()
    for name in ('rate_mib', 'timeout'):
        value = getattr(args, name, 1)
        if not math.isfinite(value) or value < 0 or (name == 'timeout' and value > 3600):
            p.error('invalid rate or deadline')
    if args.command in ('create', 'fence'):
        result = fence(read(args.plan), args.output, args.tarantool, args.timeout)
        if args.command == 'create':
            for n in plan_nodes(result['plan']):
                copy_node(result, n['name'], str(pathlib.Path(args.output) / n['name']), args.tarantool, args.rate_mib)
            result = seal(result, args.output, args.tarantool)
    elif args.command == 'copy-node':
        result = copy_node(read(args.cut), args.node, args.output, args.tarantool, args.rate_mib)
    elif args.command == 'seal':
        result = seal(read(args.cut), args.output, args.tarantool)
    elif args.command == 'release':
        result = release(read(args.journal), args.tarantool)
    elif args.command == 'restore':
        result = restore(args.backup, args.output, args.rate_mib)
    elif args.command == 'verify-restore':
        result = verify_restore(args.restore, args.tarantool)
    else:
        result = activate(args.restore, args.tarantool)
    print(json.dumps(result, sort_keys=True))


if __name__ == '__main__':
    try:
        main()
    except (OSError, ValueError, RuntimeError, TimeoutError, KeyError, StopIteration, subprocess.TimeoutExpired) as e:
        print('cluster backup: ' + str(e), file=sys.stderr)
        sys.exit(1)
