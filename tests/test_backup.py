#!/usr/bin/env python3
"""Offline recovery safety controls for the physical file agent."""
import argparse
import hashlib
import importlib.util
import json
import pathlib
import tempfile
import unittest
import uuid

ROOT = pathlib.Path(__file__).resolve().parents[1]
spec = importlib.util.spec_from_file_location('backup_agent', ROOT / 'scripts/backup.py')
agent = importlib.util.module_from_spec(spec)
spec.loader.exec_module(agent)


class BackupRecovery(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.root = pathlib.Path(self.tmp.name)
        self.backup = self.root / 'backup'
        (self.backup / 'files').mkdir(parents=True)
        self.source = str(uuid.uuid4())
        self.seed = str(uuid.uuid4())
        self.data = (b'SNAP\n0.13\nInstance: ' + self.source.encode() + b'\n\n'
                     + b'\x00\xffpayload containing ' + self.source.encode() + b'\x00' + b'a' * (agent.CHUNK + 20))
        (self.backup / 'files' / '00000000000000000001.snap').write_bytes(self.data)
        self.manifest = dict(format=agent.FORMAT, source_uuid=self.source, seed_uuid=self.seed,
                             replicaset_uuid=str(uuid.uuid4()), files=[dict(path='00000000000000000001.snap',
                             size=len(self.data), sha256=hashlib.sha256(self.data).hexdigest())])
        self.write_manifest()

    def tearDown(self):
        self.tmp.cleanup()

    def write_manifest(self):
        (self.backup / 'manifest.json').write_text(json.dumps(self.manifest))

    def restore(self, seed=None):
        return agent.restore(argparse.Namespace(backup=str(self.backup), output=str(self.root/'target'),
                                                seed_uuid=seed, rate_mib=0))

    def test_seed_changes_only_the_plaintext_header(self):
        result = self.restore(self.seed)
        expected = self.data.replace(self.source.encode(), self.seed.encode(), 1)
        data = (self.root/'target'/'00000000000000000001.snap').read_bytes()
        self.assertEqual(data, expected)
        self.assertEqual(result['files'][0]['sha256'], hashlib.sha256(expected).hexdigest())
        self.assertTrue((self.root/'target'/'restore.json').is_file())
        self.assertFalse((self.root/'target'/'INCOMPLETE').exists())

    def test_original_identity_restore_preserves_every_byte(self):
        self.restore()
        self.assertEqual((self.root/'target'/'00000000000000000001.snap').read_bytes(), self.data)

    def test_unregistered_seed_is_refused_before_output(self):
        with self.assertRaisesRegex(ValueError, 'registered'):
            self.restore(str(uuid.uuid4()))
        self.assertFalse((self.root/'target').exists())

    def test_traversal_is_refused_before_output(self):
        self.manifest['files'][0]['path'] = '../outside.snap'
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, 'invalid backup file path'):
            self.restore()
        self.assertFalse((self.root/'target').exists())

    def test_checksum_failure_never_publishes(self):
        self.manifest['files'][0]['sha256'] = '0'*64
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, 'checksum'):
            self.restore()
        self.assertTrue((self.root/'target'/'INCOMPLETE').exists())
        self.assertFalse((self.root/'target'/'restore.json').exists())

    def test_symlinked_file_is_refused(self):
        path = self.backup/'files'/'00000000000000000001.snap'
        path.unlink()
        outside = self.root/'outside.snap';outside.write_bytes(self.data);path.symlink_to(outside)
        with self.assertRaisesRegex(ValueError, 'symlink'):
            self.restore()
        self.assertFalse((self.root/'target').exists())

    def test_duplicate_paths_are_refused(self):
        self.manifest['files'].append(dict(self.manifest['files'][0]))
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, 'duplicate'):
            self.restore()

    def test_existing_target_is_never_modified(self):
        (self.root/'target').mkdir();sentinel=self.root/'target'/'sentinel';sentinel.write_text('keep')
        with self.assertRaises(FileExistsError):
            self.restore()
        self.assertEqual(sentinel.read_text(),'keep')
        self.assertEqual(list((self.root/'target').iterdir()),[sentinel])


if __name__=='__main__':unittest.main()
