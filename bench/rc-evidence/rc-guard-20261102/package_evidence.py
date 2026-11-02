"""Compact retained benchmark evidence; no execution of benchmark artifacts."""
from pathlib import Path
import tarfile,hashlib,json,io,shutil,collections
BASE=Path('/private/tmp/tarn-186-pilots')
DEST=Path('/private/tmp/tarn-186-20261102/bench/rc-evidence/rc-guard-20261102')
SOURCE='d0c0ded421e6a0958722ade199d2a799e0f41418'
def sha(raw):return hashlib.sha256(raw).hexdigest()
def pack_run(run,kind,destination):
    """Retain JSON/JSONL/log/receipt/source evidence, excluding storage and binaries."""
    out=destination/f'{kind}-run-{run.name.removeprefix("run-")}.tar.gz'
    entries=[];omitted=[];seen=set()
    def add(archive,name,raw,origin):
        assert name not in seen;seen.add(name)
        info=tarfile.TarInfo(name);info.size=len(raw);info.mode=0o644;info.mtime=0
        archive.addfile(info,io.BytesIO(raw))
        entries.append(dict(path=name,bytes=len(raw),sha256=sha(raw),origin=origin))
    with tarfile.open(out,'w:gz',compresslevel=9) as target:
        for p in sorted(run.rglob('*')):
            if not p.is_file():continue
            rel=p.relative_to(run).as_posix()
            if p.name=='fixtures.tar.gz':
                with tarfile.open(p,'r|gz') as fixture:
                    for m in fixture:
                        if not m.isfile():continue
                        name=m.name.removeprefix('./')
                        assert not Path(name).is_absolute() and '..' not in Path(name).parts
                        keep=name.startswith(('control/','candidate/')) or name.endswith(('.log','.json','.sh')) or Path(name).name=='params'
                        if keep:add(target,str(Path(rel).with_suffix('').with_suffix(''))+'/'+name,fixture.extractfile(m).read(),rel+'!'+m.name)
                        else:omitted.append(dict(path=rel+'!'+m.name,bytes=m.size,reason='bulk database WAL/snapshot/data'))
            elif p.name in ('tarn-server','libtarn_storage.so'):
                omitted.append(dict(path=rel,bytes=p.stat().st_size,reason='executable binary; SHA256 receipt retained; independently hashed during original audit'))
            elif 'rc-cost-models' in p.parts:
                omitted.append(dict(path=rel,bytes=p.stat().st_size,reason='formal model artifacts outside this pilot evidence package; rc-cost-tla.sha256 receipt retained'))
            else:add(target,rel,p.read_bytes(),rel)
    return dict(kind=kind,run_id=run.name.removeprefix('run-'),source=SOURCE,path=out.name,bytes=out.stat().st_size,sha256=sha(out.read_bytes()),members=entries,omitted=omitted)
DEST.mkdir(parents=True,exist_ok=False)
archives=[pack_run(BASE/f'run-{rid}',kind,DEST) for rid,kind in [('41529987854','guards-pilot'),('41529992173','confirmation-pilot')]]
audit=[]
for name in ['audit.py','audit.log','results.json','package_evidence.py']:
    raw=(Path('/private/tmp/tarn-186-pilot-audit')/name).read_bytes();(DEST/name).write_bytes(raw)
    audit.append(dict(path=name,bytes=len(raw),sha256=sha(raw)))
manifest=dict(schema=1,source=SOURCE,purpose='Retained single-host memtx/native-enabled RC cost pilots; no qualification claim',archives=archives,audit_files=audit,
 original_audit=dict(source_checkout='/private/tmp/tarn-186-cost-pilot',artifact_root=str(BASE),run_ids=['41529987854','41529992173'],script='audit.py',results='results.json',log='audit.log',attempts=679255,samples=120))
(DEST/'manifest.json').write_text(json.dumps(manifest,indent=2)+'\n')
# Reopen every retained archive and independently verify every member against the manifest.
for a in archives:
    expected={m['path']:m for m in a['members']}
    with tarfile.open(DEST/a['path'],'r|gz') as t:
        actual=set()
        for m in t:
            assert m.isfile() and m.name not in actual;actual.add(m.name)
            raw=t.extractfile(m).read();e=expected[m.name];assert len(raw)==e['bytes'] and sha(raw)==e['sha256']
        assert actual==expected.keys()
print(json.dumps([dict(kind=a['kind'],run_id=a['run_id'],bytes=a['bytes'],members=len(a['members']),omitted=len(a['omitted'])) for a in archives]))
