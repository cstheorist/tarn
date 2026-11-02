from pathlib import Path
import json,tarfile,re,datetime,collections
root=Path('/private/tmp/tarn-186-qualification/run-41532311462');results=[]
for cp in root.rglob('comparison.json'):
 d=json.loads(cp.read_text());sessions={(s['pair'],s['variant']):s for s in d['sessions']};counts=collections.Counter();wal=[]
 expected=set(__import__('subprocess').check_output(['git','-C','/private/tmp/tarn-186-20261102','ls-tree','-r','--name-only',d['source'],'lua','scripts'],text=True).splitlines());source_sets={'control':set(),'candidate':set()}
 with tarfile.open(cp.parent/'fixtures.tar.gz','r|gz') as tar:
  for m in tar:
   if not m.isfile():continue
   name=m.name.removeprefix('./')
   for arm in source_sets:
    if name.startswith(arm+'/'):source_sets[arm].add(name[len(arm)+1:])
   match=re.fullmatch(r'cluster-(\d+)-(control|candidate)/s(\d)\.(\d).log',name)
   if not match:continue
   pair,arm,shard,node=match.groups();session=sessions[int(pair),arm];addr=f'127.0.0.1:173{shard}{node}'
   windows=[]
   for s in session['samples']:
    b=next(r['result']['info']['clock']['wall'] for r in s['before'] if r['address']==addr);a=next(r['result']['info']['clock']['wall'] for r in s['after'] if r['address']==addr)
    windows.append((b,a,s['shape'],'warmup' if s['warmup'] else 'measured'))
   for line in tar.extractfile(m).read().decode().splitlines():
    if not re.search(' W>| E>| F>',line):continue
    try:timestamp=datetime.datetime.strptime(line[:23],'%Y-%m-%d %H:%M:%S.%f').replace(tzinfo=datetime.timezone.utc).timestamp()
    except ValueError:continue
    inside=next(((shape,phase) for b,a,shape,phase in windows if b<=timestamp<=a),None)
    category='too long WAL write' if 'too long WAL write' in line else 'other warning/error'
    counts[('diagnostic bracket '+inside[1] if inside else 'outside brackets',category)]+=1
    if category=='too long WAL write':wal.append(dict(pair=int(pair),variant=arm,node=addr,within=inside,text=line))
 assert all(v==expected for v in source_sets.values())
 results.append(dict(engine=d['engine'],native=d['native'],source_files_per_arm=len(expected),warning_counts=[dict(window=k[0],category=k[1],count=v) for k,v in counts.items()],wal_warnings=wal))
Path('/private/tmp/tarn-186-sql-audit/log-context.json').write_text(json.dumps(results,indent=2)+'\n')
for r in results:print(r['engine'],r['native'],r['warning_counts']);print('WAL',[(x['pair'],x['variant'],x['node'],x['within'],x['text'].split('> ',1)[-1]) for x in r['wal_warnings']])
