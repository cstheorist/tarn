import hashlib
import io
import json
import tarfile
from pathlib import Path

source = Path('/tmp/tarn176-c654-source')
entries = {}
runs = []
names = ['tarn176-bootstrap-prior', 'tarn176-c654-native-source'] + [
    f'tarn176-c654-v2-{mode}-{fault}'
    for mode in ['native', 'lua'] for fault in ['source', 'receiver']
]
for name in names:
    path = Path('/tmp', name)
    result = json.loads((path / 'result.json').read_text())
    failed = name == 'tarn176-c654-native-source'
    assert result['passed'] is not failed, name
    if failed:
        assert result['checks'] == [] and result['errors'], name
    else:
        assert not result['errors'] and len(result['traffic']) == 2, name
        for record in result['traffic']:
            acked, unknown, aborted, committed = (
                set(record[k]) for k in ['acked', 'unknown', 'aborted', 'committed']
            )
            assert not record['errors'] and acked <= committed
            assert not aborted & committed and committed - acked <= unknown
            assert len(committed) == len(record['committed'])
    for file in sorted(path.rglob('*')):
        if not file.is_file():
            continue
        if file.suffix in ('.json', '.log') or (failed and file.suffix in ('.xlog', '.snap')):
            entries[str(file.relative_to('/tmp'))] = file.read_bytes()
    runs.append({
        'directory': name, 'passed': result['passed'],
        'source_revision': result['source_revision'], 'checks': result['checks'],
        'errors': result['errors'],
        'effects': [{k: len(t[k]) if k in t else None for k in ['acked', 'unknown', 'aborted', 'committed']}
                    for t in result.get('traffic', [])],
    })
for name in ['lua/tarn.lua', 'lua/tarn_clock.lua', 'lua/tarn_move.lua',
             'tests/bounded_moves.py', 'tests/bounded_move_rpc.lua']:
    entries['source/' + name] = (source / name).read_bytes()
entries['harness/subreaper.py'] = Path('/tmp/tarn176-subreaper.py').read_bytes()
manifest = {name: {'bytes': len(data), 'sha256': hashlib.sha256(data).hexdigest()}
            for name, data in sorted(entries.items())}
entries['manifest.json'] = (json.dumps(manifest, indent=2) + '\n').encode()
archive = Path('/tmp/tarn176-c654-followup.tar.gz')
with tarfile.open(archive, 'w:gz') as dest:
    for name, data in sorted(entries.items()):
        info = tarfile.TarInfo(name)
        info.size = len(data)
        info.mode = 0o644
        dest.addfile(info, io.BytesIO(data))
index = {
    'source_revision': 'c654eae90df29db2c5f8e8ac93fa8016c4117317',
    'compiled_rust_revision': 'b90abdf8f5bcc8cd0f14298acca248b787266081',
    'build_boundary': 'Existing v7 dev binaries; Cargo, Rust toolchain and crates inputs are unchanged between these revisions. Current Lua source is loaded at runtime. No performance claim.',
    'container_memory_bytes': 4 * 1024**3,
    'binary_sha256': {p.name: hashlib.sha256(p.read_bytes()).hexdigest()
                      for p in sorted(Path('/tmp/tarn176-artifacts-v7').iterdir()) if p.is_file()},
    'archive': {'path': 'current-restart-runs.tar.gz', 'sha256': hashlib.sha256(archive.read_bytes()).hexdigest(), 'files': len(manifest)},
    'boundary': 'Initial current-source startup failed before movement with leader changes and conflicting async schema replication. Both prior/current fresh source-restart controls and the other three current-source controls passed. The original failure is unexplained, not fixed or relabeled as passing.',
    'runs': runs,
}
Path('/tmp/tarn176-c654-followup-index.json').write_text(json.dumps(index, indent=2) + '\n')
print(json.dumps({'files': len(manifest), 'archive_bytes': archive.stat().st_size,
                  'outcomes': [(r['directory'], r['passed']) for r in runs]}))
