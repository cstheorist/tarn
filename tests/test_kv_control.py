#!/usr/bin/env python3
"""Owned-signal controls: environment rewriting cannot erase identity."""
import copy
import json
import os
from pathlib import Path
import signal
import tempfile
import unittest

import kv_control as broker


class Ownership(unittest.TestCase):
    def setUp(self):
        self.private = tempfile.TemporaryDirectory(prefix='tarn-kv181-control-')
        self.addCleanup(self.private.cleanup)
        self.root = Path(self.private.name)
        self.owner = self.root / 'owner.json'
        self.owner.write_text(json.dumps({'ownerNonce': 'a' * 32}))
        st = self.owner.stat()
        self.node = {'node_id': 's1r1', 'pid': 42, 'ppid': 41, 'starttime': 123,
                     'workdir': str(self.root / 's1r1'), 'exe': '/owned/tarantool',
                     'ownerNonce': 'a' * 32, 'owner_fd': 9}
        self.manifest = {'launcher_pid': 41, 'launcher_starttime': 122,
                         'workdir': str(self.root), 'nodes': [self.node]}
        self.processes = {
            41: {'pid': 41, 'starttime': 122, 'uid': os.getuid()},
            42: {**self.node, 'uid': os.getuid(), 'cwd': self.node['workdir'],
                 'state': 'S', 'owner_file': (st.st_dev, st.st_ino)},
        }

    def request(self, processes):
        sent = []
        result = broker.request(self.manifest, {'action': 'signal', 'node': 's1r1', 'signal': 'STOP'},
                                snapshot=lambda pid: processes[pid],
                                send=lambda pid, sig: sent.append((pid, sig)))
        return result, sent

    def test_owned_child_without_original_environment(self):
        result, sent = self.request(self.processes)
        self.assertEqual(result['status'], 'ok')
        self.assertEqual(sent, [(42, signal.SIGSTOP)])

    def test_no_signal_after_identity_changes(self):
        changes = [('pid', 43), ('ppid', 9), ('starttime', 124), ('uid', os.getuid() + 1),
                   ('cwd', '/elsewhere'), ('exe', '/other/executable'), ('state', 'Z'),
                   ('owner_file', None), ('owner_file', (9, 9))]
        for key, value in changes:
            with self.subTest(field=key, value=value):
                processes = copy.deepcopy(self.processes)
                processes[42][key] = value
                sent = []
                with self.assertRaises(ValueError):
                    broker.request(self.manifest, {'action': 'signal', 'node': 's1r1', 'signal': 'STOP'},
                                   snapshot=lambda pid: processes[pid],
                                   send=lambda pid, sig: sent.append((pid, sig)))
                self.assertEqual(sent, [])
        processes = copy.deepcopy(self.processes)
        processes[41]['starttime'] += 1
        with self.assertRaisesRegex(ValueError, 'launcher identity'):
            self.request(processes)

    def test_replaced_receipt_rejects_old_descriptor(self):
        replacement = self.root / 'new.json'
        replacement.write_bytes(self.owner.read_bytes())
        replacement.replace(self.owner)
        with self.assertRaisesRegex(ValueError, 'owner descriptor'):
            self.request(self.processes)


if __name__ == '__main__':
    unittest.main()
