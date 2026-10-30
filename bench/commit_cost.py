#!/usr/bin/env python3
"""Bound and reconcile opt-in tarn.commit_cost transport observations (#187).

This diagnostic reports encoded transport work, never SQL acknowledgements,
logical WAL, physical disk bytes or an end-to-end performance improvement.
"""
import argparse
import hashlib
import json
from pathlib import Path
import re
import sqlite3
import sys
import tempfile

MAX_BYTES = 128 * 1024 * 1024
MAX_LINE = 4096
MAX_DB_PAGES = 32768  # 128 MiB with the required 4096-byte page size.
PHASES = {'other', 'prepare', 'apply', 'abort', 'decision_status', 'participant_status',
          'decision', 'local', 'autocommit', 'validate', 'read'}
COMMON = {'cost_event', 'process', 'connection', 'at_ns'}
FIELDS = {
    'request': COMMON | {'owner', 'sync', 'stream', 'request_type', 'phase', 'transaction', 'encoded_bytes'},
    'frame': COMMON | {'sync', 'response_type', 'encoded_bytes'},
    'terminal': COMMON | {'sync', 'delivered', 'elapsed_ns'},
    'retired': COMMON | {'pending', 'deadline'},
}
STRINGS = {'cost_event', 'phase', 'transaction'}
FLAGS = {'delivered', 'deadline'}
ANSI = re.compile(r'\x1b\[[0-9;]*m')
FIELD = re.compile(r'([a-z_]+)=("[a-z_]+"|[a-z0-9_]+)(?:\s+|$)')


def save(path, value):
    path.write_text(json.dumps(value, indent=2, sort_keys=True) + '\n')


def collect(source, output, max_bytes=MAX_BYTES, max_line=MAX_LINE):
    if not 1 <= max_bytes <= MAX_BYTES or not 1 <= max_line <= MAX_LINE:
        raise ValueError('capture limits exceed the declared bounds')
    output.mkdir(parents=True, exist_ok=False)
    digest, total, lines, failure = hashlib.sha256(), 0, 0, None
    with (output / 'trace.log').open('xb') as target:
        while True:
            raw = source.readline(min(max_line, max_bytes - total) + 1)
            if not raw:
                break
            allowed = min(len(raw), max_line, max_bytes - total)
            target.write(raw[:allowed])
            digest.update(raw[:allowed])
            total += allowed
            if len(raw) > allowed:
                failure = 'capture byte or line limit exceeded; preserved prefix is incomplete'
                break
            lines += 1
            if not raw.endswith(b'\n'):
                failure = 'unterminated last line; preserved trace is incomplete'
                break
    receipt = dict(schema=1, complete=failure is None, bytes=total, lines=lines,
                   sha256=digest.hexdigest(), max_bytes=max_bytes, max_line=max_line,
                   error=failure, scope='Bounded log capture only; not transport or SQL completion.')
    save(output / 'capture.json', receipt)
    return receipt


def parse(raw):
    text = ANSI.sub('', raw.decode('utf-8'))
    if 'tarn.commit_cost:' not in text:
        return None
    tail = text.split('tarn.commit_cost:', 1)[1].strip()
    result, offset = {}, 0
    for field in FIELD.finditer(tail):
        if field.start() != offset or field[1] in result:
            raise ValueError('malformed or duplicate trace field')
        result[field[1]] = field[2].strip('"')
        offset = field.end()
    if offset != len(tail) or result.get('cost_event') not in FIELDS:
        raise ValueError('unknown or malformed trace event')
    if set(result) != FIELDS[result['cost_event']]:
        raise ValueError('missing or unknown trace fields')
    for key, value in list(result.items()):
        if key in FLAGS:
            if value not in ('true', 'false'):
                raise ValueError('invalid boolean trace field')
            result[key] = value == 'true'
        elif key not in STRINGS:
            if not value.isascii() or not value.isdecimal() or len(value) > 20 or int(value) > 2**63 - 1:
                raise ValueError('integer trace field exceeds the audit range')
            result[key] = int(value)
    if result.get('phase', 'other') not in PHASES:
        raise ValueError('unknown phase')
    transaction = result.get('transaction', 'unattributed')
    if transaction != 'unattributed' and re.fullmatch('[0-9a-f]{40}', transaction) is None:
        raise ValueError('invalid correlation hash')
    if result['process'] == 0 or result['connection'] == 0:
        raise ValueError('invalid process or connection identity')
    if 'encoded_bytes' in result and result['encoded_bytes'] == 0:
        raise ValueError('empty encoded packet')
    return result


def audit(directory, counters=None):
    receipt_path = directory / 'capture.json'
    if receipt_path.stat().st_size > 4096:
        raise ValueError('capture receipt exceeds 4 KiB')
    capture = json.loads(receipt_path.read_text())
    if capture.get('schema') != 1 or not 0 <= capture.get('bytes', -1) <= MAX_BYTES:
        raise ValueError('invalid capture receipt')
    trace = directory / 'trace.log'
    if trace.stat().st_size != capture['bytes']:
        raise ValueError('capture length changed')
    errors = []
    def error(reason):
        if len(errors) < 64:
            errors.append(reason)
    if not capture.get('complete'):
        error('capture_incomplete')
    # Disk-backed joins keep request cardinality out of Python memory. Both
    # input and SQLite are bounded; no journal grows with the trace size.
    with tempfile.TemporaryDirectory(prefix='tarn-commit-cost-') as work:
        db = sqlite3.connect(str(Path(work) / 'events.sqlite'))
        try:
            db.execute('PRAGMA page_size=4096')
            db.execute(f'PRAGMA max_page_count={MAX_DB_PAGES}')
            db.execute('PRAGMA cache_size=-2048')
            db.execute('PRAGMA journal_mode=OFF')
            db.execute('PRAGMA temp_store=FILE')
            db.execute('CREATE TABLE event(kind TEXT,p INTEGER,c INTEGER,s INTEGER,t INTEGER,n INTEGER,r INTEGER,owner INTEGER,phase TEXT,tx TEXT,delivered INTEGER,elapsed INTEGER)')
            digest, count, seen_bytes = hashlib.sha256(), 0, 0
            with trace.open('rb') as source:
                while True:
                    raw = source.readline(MAX_LINE + 1)
                    if not raw:
                        break
                    seen_bytes += len(raw)
                    if seen_bytes > capture['bytes']:
                        raise ValueError('capture grew during audit')
                    digest.update(raw)
                    if len(raw) > MAX_LINE or not raw.endswith(b'\n'):
                        raise ValueError('invalid bounded trace line')
                    value = parse(raw)
                    if value is None:
                        continue
                    count += 1
                    db.execute('INSERT INTO event VALUES(?,?,?,?,?,?,?,?,?,?,?,?)',
                               (value['cost_event'], value['process'], value['connection'], value.get('sync'),
                                value['at_ns'], value.get('encoded_bytes', 0), value.get('response_type'),
                                value.get('owner'), value.get('phase'), value.get('transaction'),
                                value.get('delivered'), value.get('elapsed_ns')))
            if digest.hexdigest() != capture['sha256']:
                raise ValueError('capture hash changed')
            db.execute('CREATE INDEX identity ON event(p,c,s,kind)')
            if not count:
                error('no_observations')
            relationships_safe = True
            for kind in ('request', 'terminal'):
                if db.execute('SELECT 1 FROM event WHERE kind=? GROUP BY p,c,s HAVING count(*)!=1 LIMIT 1', (kind,)).fetchone():
                    error('duplicate_' + kind)
                    relationships_safe = False
            # A corrupt identity with many duplicates must not turn the
            # following joins into a quadratic Cartesian product.
            if relationships_safe and db.execute("SELECT 1 FROM event f LEFT JOIN event q ON q.p=f.p AND q.c=f.c AND q.s=f.s AND q.kind='request' WHERE f.kind IN ('frame','terminal') AND q.kind IS NULL LIMIT 1").fetchone():
                error('response_without_request')
                relationships_safe = False
            if relationships_safe and db.execute("SELECT 1 FROM event q LEFT JOIN event t ON q.p=t.p AND q.c=t.c AND q.s=t.s AND t.kind='terminal' WHERE q.kind='request' AND t.kind IS NULL LIMIT 1").fetchone():
                error('request_without_terminal')
            if db.execute("SELECT 1 FROM event WHERE kind='frame' AND r NOT IN (0,128) AND r NOT BETWEEN 32768 AND 65535 LIMIT 1").fetchone():
                error('invalid_response_type')
            if relationships_safe and db.execute("SELECT 1 FROM event q LEFT JOIN event f ON q.p=f.p AND q.c=f.c AND q.s=f.s AND f.kind='frame' AND f.r!=128 WHERE q.kind='request' GROUP BY q.rowid HAVING count(f.rowid)!=1 LIMIT 1").fetchone():
                error('terminal_frame_count')
                relationships_safe = False
            if relationships_safe and db.execute("SELECT 1 FROM event f JOIN event q ON q.p=f.p AND q.c=f.c AND q.s=f.s AND q.kind='request' JOIN event t ON q.p=t.p AND q.c=t.c AND q.s=t.s AND t.kind='terminal' WHERE f.kind='frame' AND (f.t<q.t OR f.t>t.t) LIMIT 1").fetchone():
                error('frame_outside_request_lifetime')
            if relationships_safe and db.execute("SELECT 1 FROM event t JOIN event q ON q.p=t.p AND q.c=t.c AND q.s=t.s AND q.kind='request' WHERE t.kind='terminal' AND (t.t<q.t OR abs((t.t-q.t)-t.elapsed)>1) LIMIT 1").fetchone():
                error('elapsed_time_disagreement')
            if relationships_safe and db.execute("SELECT 1 FROM event f JOIN event last ON f.p=last.p AND f.c=last.c AND f.s=last.s WHERE f.kind='frame' AND f.r=128 AND last.kind='frame' AND last.r!=128 AND f.t>last.t LIMIT 1").fetchone():
                error('chunk_after_final')
            phases = [dict(phase=p, owner=o, requests=n, encoded_bytes=b)
                      for p,o,n,b in db.execute("SELECT phase,owner,count(*),sum(n) FROM event WHERE kind='request' GROUP BY phase,owner ORDER BY phase,owner LIMIT 4097")]
            if len(phases) > 4096:
                raise ValueError('phase/owner summary exceeds 4096 groups')
            if relationships_safe:
                elapsed = {(p,o):(total,maximum) for p,o,total,maximum in db.execute("SELECT q.phase,q.owner,sum(t.elapsed),max(t.elapsed) FROM event q JOIN event t ON q.p=t.p AND q.c=t.c AND q.s=t.s AND t.kind='terminal' WHERE q.kind='request' GROUP BY q.phase,q.owner LIMIT 4097")}
                for phase in phases:
                    duration = elapsed.get((phase['phase'], phase['owner']))
                    phase['enqueue_to_terminal_sum_ns'] = duration[0] if duration else None
                    phase['enqueue_to_terminal_max_ns'] = duration[1] if duration else None
            totals = dict(db.execute('SELECT kind,count(*) FROM event GROUP BY kind'))
            # Correlation hashes do not enumerate SQL transactions. Keep this
            # summary separate from all requests and unattributed read traffic.
            participants = [dict(owners=n, correlations=k) for n,k in db.execute("SELECT owners,count(*) FROM (SELECT tx,count(DISTINCT owner) AS owners FROM event WHERE kind='request' AND phase='prepare' AND tx!='unattributed' AND owner!=0 GROUP BY tx) GROUP BY owners ORDER BY owners LIMIT 4097")]
            if len(participants) > 4096:
                raise ValueError('participant summary exceeds its bound')
            socket_reconciled = False
            if counters is not None:
                if len(counters) > 4096:
                    raise ValueError('counter inventory exceeds 4096 connections')
                seen = set()
                for sample in counters:
                    if set(sample) != {'process','connection','bytes_written','bytes_read'}:
                        raise ValueError('invalid counter schema')
                    if any(type(v) is not int or not 0 <= v <= 2**63-1 for v in sample.values()):
                        raise ValueError('invalid socket counter')
                    key = sample['process'], sample['connection']
                    if key in seen:
                        raise ValueError('duplicate socket counter identity')
                    seen.add(key)
                    observed = dict(db.execute("SELECT kind,sum(n) FROM event WHERE p=? AND c=? AND kind IN ('request','frame') GROUP BY kind", key))
                    if observed.get('request', 0) != sample['bytes_written'] or observed.get('frame', 0) != sample['bytes_read']:
                        error('socket_byte_disagreement')
                connections = set(db.execute("SELECT DISTINCT p,c FROM event WHERE kind IN ('request','frame') LIMIT 4097"))
                if len(connections) > 4096 or seen != connections:
                    error('socket_inventory_disagreement')
                socket_reconciled = not errors
            else:
                error('socket_counters_missing')
            return dict(schema=1, qualified=not errors, errors=errors, capture_sha256=capture['sha256'],
                        event_counts=totals, phases=phases, prepare_owner_distribution=participants,
                        unattributed_requests=db.execute("SELECT count(*) FROM event WHERE kind='request' AND tx='unattributed'").fetchone()[0],
                        prepare_requests_without_owner_or_correlation=db.execute("SELECT count(*) FROM event WHERE kind='request' AND phase='prepare' AND (owner=0 OR tx='unattributed')").fetchone()[0],
                        receiver_cancelled=db.execute("SELECT count(*) FROM event WHERE kind='terminal' AND delivered=0").fetchone()[0],
                        socket_reconciled=socket_reconciled,
                        scope='Complete encoded transport observations only. Receiver delivery is not SQL ACK; no WAL/disk/CPU or performance claim.')
        finally:
            db.close()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest='command', required=True)
    capture = commands.add_parser('capture')
    capture.add_argument('--output', type=Path, required=True)
    check = commands.add_parser('audit')
    check.add_argument('--input', type=Path, required=True)
    check.add_argument('--counters', type=Path)
    check.add_argument('--output', type=Path, required=True)
    args = parser.parse_args()
    if args.command == 'capture':
        return 0 if collect(sys.stdin.buffer, args.output)['complete'] else 2
    try:
        if args.counters and args.counters.stat().st_size > 1024 * 1024:
            raise ValueError('counter input exceeds 1 MiB')
        counters = json.loads(args.counters.read_text()) if args.counters else None
        result = audit(args.input, counters)
    except (ValueError, OSError, sqlite3.Error, KeyError, TypeError) as failure:
        result = dict(schema=1, qualified=False, errors=['audit_failed'],
                      failure_type=type(failure).__name__,
                      scope='Raw capture retained. Audit could not establish transport completeness.')
    save(args.output, result)
    return 0 if result['qualified'] else 2


if __name__ == '__main__':
    sys.exit(main())
