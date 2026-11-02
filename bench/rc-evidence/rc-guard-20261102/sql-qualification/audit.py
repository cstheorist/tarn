import pathlib,json,hashlib,collections,statistics,tarfile,subprocess,re,ast,io
from contextlib import contextmanager
@contextmanager
def archive(path):
 data={};members=[]
 with tarfile.open(path, mode="r|gz") as t:
  for m in t:
   if m.isfile() and (m.name.startswith(("./control/", "./candidate/")) or m.name.endswith(".log")):
    members.append(m);data[m.name]=t.extractfile(m).read()
 class Cached:
  def getmembers(self):return members
  def extractfile(self,m):return io.BytesIO(data[m.name])
 yield Cached()
ROOT=pathlib.Path('/private/tmp/tarn-186-20261102'); OUT=pathlib.Path('/private/tmp/tarn-186-sql-audit'); SOURCE='b1e7ce88358e15437fdf82d10b2a0e961513ee0d'
def sha(b):return hashlib.sha256(b).hexdigest()
def git(p):return subprocess.check_output(['git','-C',str(ROOT),'show',SOURCE+':'+p])
def stats(v):return dict(median=statistics.median(v),minimum=min(v),maximum=max(v))
results=[]
for cp in pathlib.Path('/private/tmp/tarn-186-qualification/run-41532311462').rglob('comparison.json'):
 d=json.loads(cp.read_text()); hyp=d['hypothesis']; base=cp.parent
 assert d['complete'] and d['source']==SOURCE and not d['source_dirty']
 assert d['engine'] in ('memtx','vinyl') and d['native'] in ('enabled','disabled')
 assert d['harness_sha256']==sha(git('bench/rc_cost_compare.py')) and d['worker_sha256']==sha(git('bench/rc_profile.py'))
 run=cp.parents[3]; builds=[p for p in run.rglob('tarn-server')];assert len(builds)==1
 build=builds[0].parent
 assert d['binary_sha256']==sha((build/'tarn-server').read_bytes())
 assert d['native_sha256']==sha((build/'libtarn_storage.so').read_bytes())
 assert (build/'source.txt').read_text().strip()==SOURCE
 bins={x.split()[1].split('/')[-1]:x.split()[0] for x in (build/'binaries.sha256').read_text().splitlines()}
 assert bins['tarn-server']==d['binary_sha256'] and bins['libtarn_storage.so']==d['native_sha256']
 totals=collections.Counter();measured=collections.Counter();native_totals={'all':collections.Counter(),'measured':collections.Counter()};groups={};log_summary=[];warnings=collections.Counter()
 with archive(base/'fixtures.tar.gz') as tar:
  files={m.name[2:]:m for m in tar.getmembers() if m.isfile()}
  maps={arm:{n[len(arm)+1:]:m for n,m in files.items() if n.startswith(arm+'/')} for arm in ('control','candidate')}
  assert maps['control'].keys()==maps['candidate'].keys()
  changes=[]
  for name,m in maps['control'].items():
   control=tar.extractfile(m).read();candidate=tar.extractfile(maps['candidate'][name]).read()
   assert control==git(name),name
   if candidate!=control:changes.append(name)
   if name.startswith('lua/') and name.endswith('.lua'):
    for arm,raw in [('control',control),('candidate',candidate)]:assert sha(raw)==d['lua_file_sha256'][arm][name]
  assert set(changes)==({'lua/tarn.lua','lua/tarn_fresh.lua'} if hyp=='guards' else {'lua/tarn.lua'})
  # Parse literal source anchors without executing the measured patcher.
  module=ast.parse(git('bench/rc_guard_candidate.py' if hyp=='guards' else 'bench/rc_confirm_candidate.py'))
  literals={n.targets[0].id:ast.literal_eval(n.value) for n in module.body if isinstance(n,ast.Assign) and isinstance(n.targets[0],ast.Name) and isinstance(n.value,ast.Constant)}
  replacements=[('lua/tarn_fresh.lua','_SPACE','_BATCH_SPACE'),('lua/tarn.lua','_PLANS','_BATCH_PLANS')] if hyp=='guards' else [('lua/tarn.lua','_BEFORE','_AFTER')]
  for name,b,a in replacements:
   raw=tar.extractfile(maps['control'][name]).read();target=tar.extractfile(maps['candidate'][name]).read();old,new=literals[b],literals[a]
   if isinstance(old,str):old,new=old.encode(),new.encode()
   assert raw.count(old)==1 and raw.replace(old,new,1)==target
  order=[(pair,arm) for pair in range(5) for arm in (('control','candidate') if pair%2==0 else ('candidate','control'))]
  assert [(s['pair'],s['variant']) for s in d['sessions']]==order
  for session in d['sessions']:
   pair,arm=session['pair'],session['variant'];sfile=json.loads((base/f'pair-{pair}-{arm}.json').read_text());assert sfile==session
   assert session['complete'] and len(session['configuration']['shards'])==2 and all(len(s['replicas'])==3 for s in session['configuration']['shards'])
   assert 'R=3 ' in session['parameters'] and 'WAL=fsync ' in session['parameters']
   assert session['lua_sha256']==d['lua_sha256'][arm]
   assert [(s['shape'],s['warmup'],s['scheduled_seconds'],s['profiled']) for s in session['samples']]==[(shape,warm,3 if warm else 15.0,False) for warm in (True,False) for shape in (('hot','unrelated','uncontended') if not warm and pair%2 else ('uncontended','unrelated','hot'))]
   stable={}
   for sample in session['samples']:
    assert sample['complete'] and not sample['errors'] and not sample['workers_live'] and not sample.get('diagnostic_errors')
    raw=(base/sample['attempts_file']).read_bytes();assert sha(raw)==sample['attempts_sha256']
    attempts=[json.loads(l) for l in raw.splitlines()];counts=collections.Counter(a['outcome'] for a in attempts);totals.update(counts);totals['attempts']+=len(attempts)
    if not sample['warmup']:measured.update(counts);measured['attempts']+=len(attempts)
    assert len({a['token'] for a in attempts})==len(attempts) and set(counts)<={'ok','refused'}
    ack={a['token']:a['key'] for a in attempts if a['outcome']=='ok'};n=len(ack)
    assert sample['acknowledged']==n and sample['refused']==counts['refused']
    assert all(a['error'][0] in (1205,1213) for a in attempts if a['outcome']=='refused')
    assert len(sample['readback']['ledger'])==n and dict(sample['readback']['ledger'])==ack
    effects=collections.Counter(ack.values());assert dict(sample['readback']['counters'])=={k:effects[k] for k in range(8)}
    assert sample['goodput_per_second']==n/sample['elapsed_seconds']
    q={}
    for outcome in ('ok','refused','unknown'):
     lat=sorted(a['latency_seconds'] for a in attempts if a['outcome']==outcome)
     q[outcome]={name:(lat[min(len(lat)-1,int(frac*len(lat)))] if lat else None) for name,frac in [('p50',.5),('p99',.99)]}
    assert sample['latency']==q
    before={r['address']:r['result'] for r in sample['before']};after={r['address']:r['result'] for r in sample['after']}
    assert len(before)==len(after)==6 and before.keys()==after.keys()
    for phase in (before,after):
     assert sum(not r['info']['ro'] for r in phase.values())==2
     for addr,r in phase.items():
      assert r['wal']=='fsync' and r['quorum']=='N / 2 + 1' and r['engine']==d['engine']
      assert all(r['test_flags'].get(k) is False for k in ('check_locks','check_yields'))
      if not r['info']['ro']:
       assert r['info']['native']['ready']==1 if d['native']=='enabled' else r['info']['native'] is False
      identity=(r['pid'],r['info']['ro']);assert stable.setdefault(addr,identity)==identity
    delta={r['address']:r for r in sample['deltas']};assert len(delta)==6
    alloc=waits=cwaits=0
    for addr,b in before.items():
     a=after[addr];dd=delta[addr]
     def flatten(v,prefix=''):
      out={}
      if isinstance(v,dict):
       for k,x in v.items():out.update(flatten(x,prefix+'.'+k if prefix else k))
      elif isinstance(v,(int,float)):out[prefix]=v
      return out
     bn,an=flatten(b['info']['native']),flatten(a['info']['native'])
     for k in bn.keys()|an.keys():
      nd=an.get(k,0)-bn.get(k,0);native_totals['all'][k]+=nd
      if not sample['warmup']:native_totals['measured'][k]+=nd
     assert dd['calls']==a['calls']-b['calls']
     assert dd['metrics']=={k:a['metrics'][k]-b['metrics'][k] for k in dd['metrics']}
     assert dd['stats']=={k:a['info']['stats'][k]-b['info']['stats'][k] for k in dd['stats']}
     alloc+=a['metrics']['gc_allocated']-b['metrics']['gc_allocated'];waits+=a['info']['stats']['plock_waits']-b['info']['stats']['plock_waits'];cwaits+=a['info']['stats']['commit_waits']-b['info']['stats']['commit_waits']
    assert set(sample['cpu_before'])==set(sample['cpu_after'])=={str(r['pid']) for r in before.values()}
    cpu={k:sum(sample['cpu_after'][pid][k]-v[k] for pid,v in sample['cpu_before'].items())/session['cpu_tick_hz']/n*1e6 for k in ('tx_ticks','process_ticks')};assert min(cpu.values())>=0
    if not sample['warmup']:groups[pair,arm,sample['shape']]=dict(goodput=n/sample['elapsed_seconds'],p50_ms=q['ok']['p50']*1e3,p99_ms=q['ok']['p99']*1e3,lua_bytes=alloc/n,tx_cpu_us=cpu['tx_ticks'],process_cpu_us=cpu['process_ticks'],row_waits=waits/n,commit_waits=cwaits/n)
   for addr,(pid,ro) in stable.items():
    shard,node=addr.rsplit(':',1)[1][-2:];log=tar.extractfile(files[f'cluster-{pair}-{arm}/s{shard}.{node}.log']).read().decode()
    pids=set(map(int,re.findall(r'\[(\d+)\]',log)));assert pids=={pid},(hyp,pair,arm,addr,pids,pid)
    bumps=re.findall(r'RAFT: bump term to (\d+)',log);terms=set(map(int,re.findall(r'term[: ]+(\d+)',log)))
    assert terms=={2} and bumps in ([],['2']),(hyp,pair,arm,addr,bumps,terms)
    assert log.count('ready to accept requests')==1
    for line in log.splitlines():
     if re.search(r' W>| E>| F>',line):warnings[re.sub(r'^.*? [WEF]> ', '',line)]+=1
    log_summary.append(dict(pair=pair,variant=arm,address=addr,pid=pid,terms=sorted(terms)))
 summary={shape:{'arms':{arm:{metric:stats([groups[p,arm,shape][metric] for p in range(5)]) for metric in groups[0,arm,shape]} for arm in ('control','candidate')},'ratios':{metric:stats([groups[p,'candidate',shape][metric]/groups[p,'control',shape][metric] for p in range(5)]) for metric in ('goodput','lua_bytes','tx_cpu_us','process_cpu_us','p50_ms','p99_ms')},'wait_delta':{metric:stats([groups[p,'candidate',shape][metric]-groups[p,'control',shape][metric] for p in range(5)]) for metric in ('row_waits','commit_waits')}} for shape in ('uncontended','unrelated','hot')}
 mechanisms={}
 for arm in ('control','candidate'):
  m=json.loads((base/f'pair-0-{arm}.mechanism.log').read_text())
  if hyp=='guards':
   assert m['complete'] and m['census_mode_restored'] and m['direct_space_mode_calls']==2
   assert [(v['width'],v['mode_calls'],v['context_switches']) for v in m['census']]==[(w,1 if arm=='candidate' else 1+w,0) for w in (0,1,2,8,32)]
   assert len(m['samples'])==25 and m['runtime']['term']==2 and m['runtime']['replicas']==3 and m['runtime']['quorum']==2 and m['runtime']['wal']=='fsync'
   mechanisms[arm]={str(w):{metric:stats([s[metric]/m['iterations'] for s in m['samples'] if s['width']==w]) for metric in ('lua_allocated','elapsed_seconds')} for w in (0,1,2,8,32)}
  else:
   assert m['ok'] and m['wrapper_restored'] and m['space_removed'] and not m['watchdog_fired'] and not m['error'] and not m['cleanup_errors']
   assert len(m['cases'])==4
   for c in m['cases']:
    rollback=c['injected_rollback'];assert c['owner_code']==(1213 if rollback else 0) and c['row']==(0 if rollback else 1)
    assert c['waiter_finished_during_confirmed_hold']==(arm=='candidate' and not rollback)
    assert c['commit_waits']==int(arm=='control' and not rollback)
   mechanisms[arm]=m
 cpu_text=(base.parent/'cpu.txt').read_text()
 results.append(dict(hypothesis=hyp,engine=d['engine'],native=d['native'],source=SOURCE,run_id='41532311462',totals=dict(totals),measured_totals=dict(measured),native_deltas={k:dict(v) for k,v in native_totals.items()},changed_files=changes,node_logs_checked=len(log_summary),log_warnings=dict(warnings),cpu=cpu_text,summary=summary,mechanisms=mechanisms))
(OUT/'results.json').write_text(json.dumps(results,indent=2)+'\n')
print(json.dumps([{k:v for k,v in r.items() if k not in ('summary','mechanisms')} for r in results],indent=2))
for r in results:
 print(r['hypothesis'])
 for shape,s in r['summary'].items():
  print(shape,'ratios',json.dumps(s['ratios']),'wait delta',json.dumps(s['wait_delta']))
