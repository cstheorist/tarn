import argparse,json,pathlib,sys,traceback
sys.path.insert(0,'/tmp/routing-final-canonical/tests/sql')
from lazytest import ev
from ddltest import T
nodes=['127.0.0.1:35311','127.0.0.1:35321'];installed=[];result={'source':'harness-final-source-see-input-manifest','control':'Actual PREPARE before BUILD; must reject held/unfenced witness','qualified':False}
try:
 for node in nodes:
  ev(node,"""assert(tarn.cfg.test_hooks and not __index_build_red)
local h={old=tarn_alter_build}; __index_build_red=h
function tarn_alter_build(name,a,gen)
 if name=='ddltest.bigt' then
  local r=tarn_alter_prepare(name,a,gen); h.prepare_reply=r; h.prepared=r[1]=='ok'
 end
 return h.old(name,a,gen)
end
return true""");installed.append(node)
 t=T(argparse.Namespace(tarn='127.0.0.1:4500,127.0.0.1:4501',kv=','.join(nodes)))
 try:t.test_index_build_ahead()
 except AssertionError as e:result['rejected']=True;result['failure']=repr(e)
 else:result['rejected']=False
 result['replicas']=[ev(n,'local h=assert(__index_build_red);return {prepared=h.prepared,reply=h.prepare_reply}') for n in nodes]
 result['qualified']=result['rejected'] and all(r.get('prepared') for r in result['replicas']) and 'write completed after the held BUILD phase ended' in result['failure']
 assert result['qualified'],result
finally:
 for n in installed:ev(n,'local h=__index_build_red; if h then tarn_alter_build=h.old; __index_build_red=nil end;return true')
 pathlib.Path('/tmp/routing-index-build-negative-final.json').write_text(json.dumps(result,indent=2)+'\n')
print(json.dumps(result,indent=2))
