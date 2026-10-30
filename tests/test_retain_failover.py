#!/usr/bin/env python3
"""Artifact round trip and negative controls for ordinary CI crash retention."""
import importlib.util
import json
from pathlib import Path
import tarfile
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('capture', Path(__file__).resolve().parents[1] / 'ci/retain_failover.py')
capture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(capture)


class Retention(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.output = self.root / 'evidence'
        self.binary = self.root / 'tarantool'
        self.binary.write_bytes(b'original executable')
        self.fixture = self.root / 'fixture'
        (self.fixture / 's1').mkdir(parents=True)
        (self.fixture / 's1' / 'data.xlog').write_bytes(b'failed WAL\x00\xff')
        (self.fixture / 'events.out').write_text('FAILOVER TEST FAILED\n')
        self.cores = self.root / 'cores'
        self.cores.mkdir()
        (self.cores / 'core.tarantool.42').write_bytes(b'core bytes\x00\xff')
        with patch.object(capture.subprocess, 'check_output', side_effect=['source-id\n', '', b'']):
            capture.begin(self.root, self.output, {'tarantool': self.binary}, 'core.%p')

    def finish(self):
        return capture.finish(self.output, [self.fixture], self.cores, 'failure')

    def test_failed_fixture_and_core_round_trip(self):
        external = self.root / 'external'
        external.write_bytes(b'do not copy this through the fixture')
        (self.fixture / 'external-link').symlink_to(external)
        self.assertTrue(self.finish())
        receipt = json.loads((self.output / 'after.json').read_text())
        self.assertEqual(receipt['job_status'], 'failure')
        for name, source in [('fixture', self.fixture), ('cores', self.cores)]:
            with tarfile.open(self.output / (name + '.tar.gz')) as archive:
                for member in archive.getmembers():
                    if member.isfile():
                        actual = archive.extractfile(member).read()
                        relative = Path(member.name).relative_to(source.name)
                        self.assertEqual(actual, (source / relative).read_bytes())
                    if member.name.endswith('external-link'):
                        self.assertTrue(member.issym())
        self.assertEqual((self.output / 'binaries/tarantool').read_bytes(), b'original executable')

    def test_changed_binary_fails_but_preserves_original_and_crash(self):
        self.binary.write_bytes(b'replaced executable')
        self.assertFalse(self.finish())
        receipt = json.loads((self.output / 'after.json').read_text())
        self.assertFalse(receipt['binaries']['tarantool']['unchanged'])
        self.assertTrue((self.output / 'cores.tar.gz').is_file())
        self.assertEqual((self.output / 'binaries/tarantool').read_bytes(), b'original executable')

    def test_missing_cores_does_not_discard_failed_fixture(self):
        (self.cores / 'core.tarantool.42').unlink()
        self.cores.rmdir()
        self.assertFalse(self.finish())
        self.assertTrue((self.output / 'fixture.tar.gz').is_file())
        self.assertFalse(json.loads((self.output / 'after.json').read_text())['complete'])

    def test_empty_cores_is_explicit(self):
        (self.cores / 'core.tarantool.42').unlink()
        self.assertTrue(self.finish())
        self.assertEqual(json.loads((self.output / 'after.json').read_text())['cores']['files'], {})


if __name__ == '__main__':
    unittest.main()
