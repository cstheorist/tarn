#!/usr/bin/env python3
"""Offline rejection controls for whole-cluster completeness and publication."""
import copy
import pathlib
import sys
import tempfile
import unittest

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parents[1] / 'scripts'))
import cluster_backup as cb


def fixture():
    nodes, infos = [], {}
    for shard in (1, 2):
        ids = ['uuid-%d-%d' % (shard, r) for r in range(3)]
        for r in range(3):
            name = 's%dr%d' % (shard, r)
            nodes.append(dict(name=name, shard=shard, uri_env=name.upper(), source='/data/' + name))
            infos[name] = dict(uuid=ids[r], replicaset_uuid='rs' + str(shard), shard=shard,
                version='3.9.0', wal_mode='fsync', registered=ids, catalog_sha256='catalog',
                members=None, errors=[], buckets=[shard], bucket_count=2, vclock={'1': '9'},
                ro=True, read_only=True, election_mode='off', marker={'token': 'token'})
    return nodes, infos


class ClusterCompleteness(unittest.TestCase):
    def setUp(self):
        self.nodes, self.infos = fixture()

    def validate(self):
        cb.validate_inventory(self.nodes, self.infos, frozen=True, token='token')

    def test_complete_cut(self):
        self.validate()

    def test_omitted_replica(self):
        self.nodes.pop(); self.infos.pop('s2r2')
        with self.assertRaisesRegex(ValueError, 'every registered'):
            self.validate()

    def test_lagging_replica(self):
        self.infos['s2r2']['vclock'] = {'1': '8'}
        with self.assertRaisesRegex(ValueError, 'same frozen WAL'):
            self.validate()

    def test_each_unfinished_obligation_refuses(self):
        for message in ('prepared participant', 'staging decision', '_tarn_lazy_outbox is not empty',
                        'unfinished metadata: job:t', 'unfinished bucket 1', 'unsettled synchronous queue'):
            with self.subTest(message=message):
                self.infos['s1r0']['errors'] = [message]
                with self.assertRaisesRegex(ValueError, 'not drained'):
                    self.validate()

    def test_catalog_divergence(self):
        self.infos['s1r0']['catalog_sha256'] = 'different'
        with self.assertRaisesRegex(ValueError, 'catalogs differ'):
            self.validate()

    def test_missing_bucket(self):
        for r in range(3):
            self.infos['s1r%d' % r]['buckets'] = []
        with self.assertRaisesRegex(ValueError, 'missing serving'):
            self.validate()

    def test_duplicate_owner(self):
        for r in range(3):
            self.infos['s1r%d' % r]['buckets'] = [1, 2]
        with self.assertRaisesRegex(ValueError, 'duplicate or invalid'):
            self.validate()

    def test_old_fence_token(self):
        self.infos['s2r0']['marker']['token'] = 'old'
        with self.assertRaisesRegex(ValueError, 'fence is absent or changed'):
            self.validate()

    def test_return_to_writable_refuses(self):
        self.infos['s1r0']['read_only'] = False
        with self.assertRaisesRegex(ValueError, 'fence is absent or changed'):
            self.validate()

    def test_writes_between_copies_refuse(self):
        after = copy.deepcopy(self.infos)
        for r in range(3):
            after['s1r%d' % r]['vclock']['1'] = '10'
        with self.assertRaisesRegex(ValueError, 'frozen cut changed'):
            cb.unchanged(self.nodes, self.infos, after, 'token')

    def test_interrupted_cluster_never_restores(self):
        with tempfile.TemporaryDirectory() as d:
            cb.backup.begin(pathlib.Path(d) / 'backup')
            with self.assertRaisesRegex(ValueError, 'incomplete cluster'):
                cb.restore(pathlib.Path(d) / 'backup', pathlib.Path(d) / 'restore', 0)
            self.assertFalse((pathlib.Path(d) / 'restore').exists())

    def test_duplicate_node_and_path_traversal(self):
        for name in ('../outside', self.nodes[0]['name']):
            nodes = copy.deepcopy(self.nodes); nodes[-1]['name'] = name
            with self.assertRaisesRegex(ValueError, 'invalid or duplicate'):
                cb.plan_nodes({'format': cb.FORMAT, 'nodes': nodes})


if __name__ == '__main__':
    unittest.main()
