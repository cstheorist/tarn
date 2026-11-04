#!/usr/bin/env python3
"""Offline controls for fixture evidence; no database durability assertion."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

spec = importlib.util.spec_from_file_location('effect_journal', Path(__file__).parent / 'sql/effect_journal.py')
journal = importlib.util.module_from_spec(spec)
spec.loader.exec_module(journal)


class JournalTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.path = Path(self.temp.name) / 'events.jsonl'

    def create(self, **kwargs):
        result = journal.Journal(self.path, source={'revision': 'example'}, config={'rf': 3}, **kwargs)
        self.addCleanup(result.close)
        return result

    def intent(self, writer, op='seed', **kwargs):
        writer.intent(op, txn_id='setup-1', role='setup', sql='INSERT INTO t VALUES (%s)', params=[7], **kwargs)

    def test_fsync_precedes_submission_and_completion_return(self):
        writer = self.create()
        cuts = []
        with patch.object(journal.os, 'fsync', side_effect=lambda fd: cuts.append(self.path.read_bytes())):
            self.intent(writer)
            cuts.append(b'caller submits once')
            writer.complete('seed', kind='response', raw={'rowcount': 1})
        self.assertEqual(json.loads(cuts[0].splitlines()[-1])['type'], 'intent')
        self.assertEqual(cuts[1], b'caller submits once')
        self.assertEqual(json.loads(cuts[2].splitlines()[-1])['type'], 'completion')
        self.assertFalse(journal.read(self.path)['pending'])

    def test_fixture_constructor_failure_retains_setup_key(self):
        writer = self.create()
        def construct():
            self.intent(writer)
            raise RuntimeError('fixture construction failed')
        with self.assertRaises(RuntimeError):
            construct()
        self.assertEqual(journal.read(self.path)['pending'][0]['params'], [7])

    def test_exact_errors_and_statement_success_are_unclassified(self):
        writer = self.create()
        outcomes = [('error', {'errno': 1213, 'sqlstate': '40001'}),
                    ('error', {'errno': 1180, 'sqlstate': '40003'}),
                    ('transport', {'errno': 2013}), ('response', {'rowcount': 1})]
        for index, (kind, raw) in enumerate(outcomes):
            writer.intent(str(index), txn_id='tx', role='statement', metadata={'index': index})
            writer.complete(str(index), kind=kind, raw=raw)
        records = journal.read(self.path)['records']
        self.assertEqual([r['raw'] for r in records if r['type'] == 'completion'], [v for _, v in outcomes])
        self.assertTrue(all('ack' not in r and 'committed' not in r for r in records))

    def test_invalid_calls_do_not_append(self):
        writer = self.create()
        self.intent(writer)
        before = self.path.read_bytes()
        for call in (lambda: self.intent(writer),
                     lambda: writer.complete('missing', kind='response', raw={}),
                     lambda: writer.complete('seed', kind='error', raw={'bad': object()}),
                     lambda: writer.intent('bad', txn_id='tx', role='setup', metadata=float('nan'))):
            with self.assertRaises((ValueError, TypeError)):
                call()
            self.assertEqual(self.path.read_bytes(), before)
        writer.complete('seed', kind='response', raw=None)
        before = self.path.read_bytes()
        with self.assertRaises(ValueError):
            writer.complete('seed', kind='response', raw=None)
        self.assertEqual(self.path.read_bytes(), before)
        with self.assertRaises(FileExistsError):
            self.create()

    def test_lost_final_reply_never_fabricates_completion(self):
        writer = self.create()
        self.intent(writer)
        with self.path.open('ab') as stream:
            stream.write(b'{"type":"completion","op_id":"seed"}')
        result = journal.read(self.path)
        self.assertTrue(result['incomplete'])
        self.assertEqual([r['op_id'] for r in result['pending']], ['seed'])
        self.assertEqual(len(result['records']), 2)

    def test_corruption_duplicate_and_missing_intent_fail(self):
        writer = self.create()
        header = self.path.read_bytes()
        self.intent(writer)
        intent = self.path.read_bytes()[len(header):]
        bad_lines = [b'{broken}\n', b'{"type":"intent","type":"intent"}\n',
                     b'[]\n', json.dumps({'type': 'completion', 'op_id': 'missing',
                                         'kind': 'response', 'raw': {}}).encode() + b'\n']
        for suffix in bad_lines + [intent + intent]:
            self.path.write_bytes(header + suffix)
            with self.assertRaises(ValueError):
                journal.read(self.path)
        self.path.write_bytes(header + b'{broken}\n' + intent)
        with self.assertRaises(ValueError):
            journal.read(self.path)

    def test_limits_and_failed_sync_stop_caller(self):
        writer = self.create(max_bytes=512, max_entries=1)
        self.intent(writer)
        with self.assertRaises(ValueError):
            writer.complete('seed', kind='response', raw=None)
        with self.assertRaises(ValueError):
            journal.read(self.path, max_bytes=32)
        with self.assertRaises(ValueError):
            journal.read(self.path, max_entries=-1)
        self.path.unlink()
        writer = self.create(max_bytes=512)
        before = self.path.read_bytes()
        with self.assertRaises(ValueError):
            writer.intent('large', txn_id='t', role='setup', metadata='x' * 1024)
        self.assertEqual(self.path.read_bytes(), before)
        with patch.object(journal.os, 'fsync', side_effect=OSError('sync failed')):
            with self.assertRaises(OSError):
                self.intent(writer)
        with self.assertRaises(ValueError):
            writer.intent('later', txn_id='t', role='setup', metadata={})

    def test_header_validation_and_reader_entry_limit(self):
        with self.assertRaises(TypeError):
            journal.Journal(self.path, source=object(), config={})
        self.assertFalse(self.path.exists())
        writer = self.create()
        self.intent(writer)
        writer.complete('seed', kind='response', raw={})
        with self.assertRaises(ValueError):
            journal.read(self.path, max_entries=1)
        valid = self.path.read_bytes()
        for contents in (b'', b'{"type":"header"}',
                         valid.replace(b'"schema": 1', b'"schema": true', 1)):
            self.path.write_bytes(contents)
            with self.assertRaises(ValueError):
                journal.read(self.path)

    def test_numeric_keys_rejected_before_file_or_state_changes(self):
        bad = {'nested': [{1: 'numeric', '1': 'text'}]}
        for field in ('source', 'config'):
            header = {'source': {}, 'config': {}}
            header[field] = bad
            with self.assertRaises(TypeError):
                journal.Journal(self.path, **header)
            self.assertFalse(self.path.exists())
        writer = self.create()
        for field in ('params', 'metadata'):
            before = self.path.read_bytes()
            state = (writer.pending.copy(), writer.seen.copy(), writer.count)
            args = {'sql': 'INSERT INTO t VALUES (%s)', field: bad}
            with self.assertRaises(TypeError):
                writer.intent('seed', txn_id='setup', role='setup', **args)
            self.assertEqual(self.path.read_bytes(), before)
            self.assertEqual((writer.pending, writer.seen, writer.count), state)
            self.assertEqual(len(journal.read(self.path)['records']), 1)
        self.intent(writer)
        before = self.path.read_bytes()
        state = (writer.pending.copy(), writer.seen.copy(), writer.count)
        with self.assertRaises(TypeError):
            writer.complete('seed', kind='response', raw=bad)
        self.assertEqual(self.path.read_bytes(), before)
        self.assertEqual((writer.pending, writer.seen, writer.count), state)
        self.assertEqual(len(journal.read(self.path)['pending']), 1)
        writer.complete('seed', kind='response', raw={'rowcount': 1})
        self.assertFalse(journal.read(self.path)['pending'])

    def test_short_write_poisoning(self):
        writer = self.create()
        original = writer.file
        class ShortWriter:
            def write(self, data):
                return original.write(data[:12])
            def close(self):
                original.close()
        writer.file = ShortWriter()
        with self.assertRaisesRegex(OSError, 'short journal write'):
            self.intent(writer)
        self.assertTrue(writer.failed)
        before = self.path.read_bytes()
        self.assertEqual(writer.count, 0)
        self.assertFalse(writer.pending)
        with self.assertRaisesRegex(ValueError, 'previously failed'):
            self.intent(writer, 'later')
        self.assertEqual(self.path.read_bytes(), before)
        self.assertTrue(journal.read(self.path)['incomplete'])

    def test_complete_valid_reply_without_newline_stays_pending(self):
        writer = self.create()
        self.intent(writer)
        writer.complete('seed', kind='response', raw={'rowcount': 1})
        complete = self.path.read_bytes()
        self.assertFalse(journal.read(self.path)['pending'])
        self.path.write_bytes(complete[:-1])
        result = journal.read(self.path)
        self.assertTrue(result['incomplete'])
        self.assertEqual([r['op_id'] for r in result['pending']], ['seed'])
        self.assertFalse(any(r['type'] == 'completion' for r in result['records']))


if __name__ == '__main__':
    unittest.main()
