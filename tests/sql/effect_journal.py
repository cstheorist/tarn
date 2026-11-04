"""Single-writer fixture journal; no retries, SQL classification, or effect oracle.

Call intent() before submitting any mutation, including setup; complete() after
its raw result. A response to a statement is never interpreted as a commit.
Only newline-terminated records count: an unterminated final line is discarded
and reported incomplete, even if it contains valid JSON. Pending intents remain
pending, never acknowledged. fsync covers journal evidence, not database data.
"""
import json
import os
from pathlib import Path


def _encode(record):
    def check(value):
        if isinstance(value, dict):
            if any(not isinstance(key, str) for key in value):
                raise TypeError('JSON object keys must be strings')
            for child in value.values():
                check(child)
        elif isinstance(value, (list, tuple)):
            for child in value:
                check(child)
    check(record)
    return (json.dumps(record, allow_nan=False, ensure_ascii=True) + '\n').encode()


def _pairs(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError('duplicate JSON field')
        result[key] = value
    return result


def _accept(record, pending, seen):
    if not isinstance(record, dict):
        raise ValueError('record must be an object')
    op = record.get('op_id')
    if not isinstance(op, str) or not op:
        raise ValueError('nonempty operation ID required')
    if record.get('type') == 'intent':
        if op in seen or record.get('role') not in ('setup', 'statement', 'commit'):
            raise ValueError('duplicate intent or invalid role')
        if not isinstance(record.get('txn_id'), str) or not record['txn_id']:
            raise ValueError('explicit transaction ID required')
        if record.get('sql') is None and record.get('metadata') is None:
            raise ValueError('SQL or caller metadata required')
        if record.get('sql') is not None and not isinstance(record['sql'], str):
            raise ValueError('SQL must be text')
        seen.add(op)
        pending[op] = record
    elif record.get('type') == 'completion':
        if op not in pending or record.get('kind') not in ('response', 'error', 'transport') or 'raw' not in record:
            raise ValueError('unknown/duplicate completion or invalid outcome')
        del pending[op]
    else:
        raise ValueError('invalid record type')


class Journal:
    def __init__(self, path, *, source, config, max_bytes=65536, max_entries=10000):
        if type(max_bytes) is not int or type(max_entries) is not int or max_bytes < 1 or max_entries < 1:
            raise ValueError('positive integer limits required')
        header = {'type': 'header', 'schema': 1, 'source': source, 'config': config,
                  'max_bytes': max_bytes, 'max_entries': max_entries}
        data = _encode(header)
        if len(data) > max_bytes:
            raise ValueError('header exceeds record limit')
        self.max_bytes, self.max_entries = max_bytes, max_entries
        self.pending, self.seen, self.count, self.failed = {}, set(), 0, False
        self.file = open(path, 'xb', buffering=0)
        try:
            self._append(data)
            directory = os.open(Path(path).resolve().parent, os.O_RDONLY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except BaseException:
            self.close()
            raise

    def _append(self, data):
        if self.failed:
            raise ValueError('journal write previously failed')
        try:
            if self.file.write(data) != len(data):
                raise OSError('short journal write')
            os.fsync(self.file.fileno())
        except BaseException:
            self.failed = True
            raise

    def _record(self, record):
        data = _encode(record)  # Validate serialization before changing state/file.
        if len(data) > self.max_bytes or self.count >= self.max_entries:
            raise ValueError('journal fixture limit exceeded')
        pending, seen = self.pending.copy(), self.seen.copy()
        _accept(record, pending, seen)
        self._append(data)
        self.pending, self.seen, self.count = pending, seen, self.count + 1

    def intent(self, op_id, *, txn_id, role, sql=None, params=None, metadata=None):
        self._record({'type': 'intent', 'op_id': op_id, 'txn_id': txn_id,
                      'role': role, 'sql': sql, 'params': params, 'metadata': metadata})

    def complete(self, op_id, *, kind, raw):
        self._record({'type': 'completion', 'op_id': op_id, 'kind': kind, 'raw': raw})

    def close(self):
        self.file.close()


def read(path, *, max_bytes=65536, max_entries=10000):
    """Return ordered records, pending invocation records, and incomplete flag.

    Malformed terminated records (including the last) are corruption and raise.
    Caller limits bound reads independently of the header's declared limits.
    """
    if type(max_bytes) is not int or type(max_entries) is not int or max_bytes < 1 or max_entries < 1:
        raise ValueError('positive integer limits required')
    records, pending, seen, incomplete = [], {}, set(), False
    with open(path, 'rb') as stream:
        while True:
            line = stream.readline(max_bytes + 1)
            if not line:
                break
            if len(line) > max_bytes or len(records) > max_entries:
                raise ValueError('journal fixture limit exceeded')
            if not line.endswith(b'\n'):
                incomplete = True
                break
            record = json.loads(line, object_pairs_hook=_pairs,
                                parse_constant=lambda value: (_ for _ in ()).throw(ValueError(value)))
            if not records:
                if not isinstance(record, dict) or record.get('type') != 'header' or type(record.get('schema')) is not int or record['schema'] != 1 or not {'source', 'config'} <= record.keys():
                    raise ValueError('invalid journal header')
                limits = [record.get('max_bytes'), record.get('max_entries')]
                if any(type(v) is not int or v < 1 for v in limits) or len(line) > limits[0]:
                    raise ValueError('invalid journal limits')
                max_bytes, max_entries = min(max_bytes, limits[0]), min(max_entries, limits[1])
            else:
                _accept(record, pending, seen)
            records.append(record)
    if not records:
        raise ValueError('missing complete journal header')
    return {'records': records, 'pending': list(pending.values()), 'incomplete': incomplete}
