"""Independently audit downloaded CI artifacts against their exact Git source."""
import argparse
import collections
import hashlib
import json
import re
import subprocess
import zipfile
from pathlib import Path

parser = argparse.ArgumentParser()
parser.add_argument('--repo', type=Path, required=True)
parser.add_argument('--input-dir', type=Path, required=True)
parser.add_argument('--log-dir', type=Path, required=True)
parser.add_argument('--output', type=Path, required=True)
args = parser.parse_args()
revision = '11aeba6fefee84441175bc225f9686c6f9ed0249'
head = 'c654eae90df29db2c5f8e8ac93fa8016c4117317'
baseline = '8b12c4525bd677499a91b973afbc86cbcbb0cafd'

def git(*values):
    return subprocess.check_output(['git', *values], cwd=args.repo)

def digest(data):
    return hashlib.sha256(data).hexdigest()

assert git('rev-parse', revision + '^{tree}') == git('rev-parse', head + '^{tree}')
source_hashes = {}
def source_hash(rev, name):
    key = (rev, name)
    if key not in source_hashes:
        source_hashes[key] = digest(git('show', rev + ':' + name))
    return source_hashes[key]

registry = git('show', head + ':tla/check.sh').decode().split('CONFIGS=')[0]
expanded = ''
for value in re.findall(r'^DEFAULT="([^"]*)"$', registry, re.M):
    expanded = value.replace('$DEFAULT', expanded)
configs = expanded.split()
formal = (args.log_dir / 'final-ci-c654-formal.log').read_text()
lines = [re.sub(r'^\S+Z ', '', line) for line in formal.splitlines()]
rows = [line for line in lines if re.match(r'^\w+\s+(?:no errors|Invariant \w+ is violated)', line)]
assert [line.split()[0] for line in rows] == configs
assert len(rows) == len(set(configs)) == 319
assert 'all outcomes as expected' in lines
for cfg, row in zip(configs, rows):
    assert (' is violated ' in row) == cfg.startswith(('Bug_', 'Tradeoff_')), cfg
assert 'UNEXPECTED:' not in formal and '##[error]' not in formal
positive = sum(' no errors ' in row for row in rows)
assert positive == 79

movement = args.input_dir / 'tarn176-ci-movement-c654.zip'
totals = collections.Counter()
modes = []
with zipfile.ZipFile(movement) as z:
    results = sorted(name for name in z.namelist() if name.endswith('/result.json'))
    assert len(results) == 18
    for name in results:
        result = json.loads(z.read(name))
        assert result['passed'] and not result['errors'], name
        assert result['source_revision'] == revision, name
        for path, sha in result['source_sha256'].items():
            assert source_hash(revision, path) == sha, (name, path)
        for record in result['traffic']:
            assert not record['errors']
            ack, aborted, unknown, committed = [set(record[k]) for k in ['acked', 'aborted', 'unknown', 'committed']]
            assert ack <= committed and not aborted & committed
            assert committed - ack <= unknown
            assert not ack & aborted and not ack & unknown and not aborted & unknown
            assert len(committed) == len(record['committed'])
            totals.update(acknowledged=len(ack), aborted=len(aborted), unknown=len(unknown),
                          committed=len(committed), unknown_committed=len(unknown & committed))
        modes.append([name.split('/')[0], result['settings']['engine'], result['settings']['fault']])

upgrades = []
for engine in ['memtx', 'vinyl']:
    archive = args.input_dir / ('tarn176-ci-upgrade-' + engine + '-c654.zip')
    with zipfile.ZipFile(archive) as z:
        manifest = json.loads(z.read(next(n for n in z.namelist() if n.endswith('/manifest.json'))))
        assert manifest['result'] == 'passed' and manifest['engine'] == engine
        assert manifest['replicas'] == 3 and manifest['wal_mode'] == 'fsync'
        assert manifest['baseline_revision'] == baseline
        for kind, rev in [('new', revision), ('old', baseline)]:
            for path, sha in manifest[kind + '_source']['files'].items():
                assert source_hash(rev, path) == sha, (engine, kind, path)
            receipt = manifest['build_receipts'][kind]
            assert receipt['revision'] == rev
            for path, sha in receipt['inputs'].items():
                assert source_hash(rev, path) == sha, (engine, kind, path)
            assert receipt['gateway_sha256'] == manifest['artifacts'][kind + '_bin']['sha256']
            assert receipt['native_sha256'] == manifest['artifacts'][kind + '_native']['sha256']
        for artifact in manifest['artifacts'].values():
            assert digest(z.read(artifact['path'].removeprefix('/tmp/'))) == artifact['sha256']
        events = [json.loads(line) for line in z.read(next(n for n in z.namelist() if n.endswith('/events.jsonl'))).splitlines()]
        def selected(event):
            return [e for e in events if e.get('event') == event]
        yes = {(e['cluster'], e['txid']): e['time'] for e in selected('durable_YES_before_restart')}
        applied = selected('prepared_applied_once_after_restart')
        assert len(yes) == len(applied) == len(selected('durable_YES_before_restart'))
        assert collections.Counter((e['cluster'], e['txid']) for e in applied) == collections.Counter({k: 1 for k in yes})
        for e in applied:
            assert e['time'] > yes[(e['cluster'], e['txid'])]
        cleanups = selected('prepare_refusal_cleanup_verified')
        for event in cleanups:
            assert len(event['evidence']) == 2
            for row in event['evidence']:
                assert row['state']['participant'] == 'aborted'
                assert row['state']['value'] == row['state']['locks'] == row['state']['range_locks'] == 0
        required = {'SQL_DDL_move_lazy_canaries_passed': 10, 'native_contract_passed': 5,
                    'storage_admission_refused': 8,
                    'format_2_refused_by_old_binary_without_data_effect': 1,
                    'format_migration_snapshot_reopen_and_downgrade_refusal_passed': 1,
                    'unfinished_work_refused_before_marker_then_drained_migration_passed': 1}
        for event, count in required.items():
            assert len(selected(event)) == count, (engine, event)
        upgrades.append({'engine': engine, 'passed': True, 'archive_sha256': digest(archive.read_bytes()),
                         'source_inputs_checked': {kind: len(manifest[kind + '_source']['files']) for kind in ['old', 'new']},
                         'binaries_checked': len(manifest['artifacts']), 'canaries': 10, 'native_modes': 5,
                         'durable_yes_apply_once_pairs': len(yes), 'admission_refusal_cleanups': len(cleanups)})

for name in ['tests', 'upgrade-memtx', 'upgrade-vinyl', 'verus']:
    log = (args.log_dir / ('final-ci-c654-' + name + '.log')).read_text()
    assert '##[error]' not in log, name
    if name == 'verus':
        assert 'verification results:: 140 verified, 0 errors' in log

summary = {'implementation_head': head, 'tested_revision': revision,
           'tested_tree': git('rev-parse', revision + '^{tree}').decode().strip(),
           'tested_tree_equals_implementation': True, 'ci_run': 41102681469,
           'formal': {'total': 319, 'positive': positive, 'expected_violations': 319-positive},
           'verus': {'verified': 140, 'errors': 0},
           'movement': {'artifact_id': 13002269804, 'artifact_sha256': digest(movement.read_bytes()),
                        'results': len(results), 'exact_ledger_audit': dict(totals), 'modes': modes},
           'upgrades': upgrades,
           'boundary': 'Source-bound CI correctness and admission evidence; no new performance or whole-process memory claim. The separate local bootstrap failure remains unexplained.'}
args.output.write_text(json.dumps(summary, indent=2) + '\n')
print(json.dumps({k: summary[k] for k in ['formal', 'verus', 'upgrades']}))
print(json.dumps(summary['movement']['exact_ledger_audit']))
