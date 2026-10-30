-- Exact production role reconciliation, including incomplete migrations.
local fio = require('fio')
local here = debug.getinfo(1, 'S').source:sub(2):match('(.*/)') or './'
package.path = here .. '../?.lua;' .. package.path
local work = assert(arg[1]); fio.mktree(work)
box.cfg{work_dir=work, wal_mode='fsync', memtx_memory=64*1024*1024,
    vinyl_memory=16*1024*1024, log_level=3}
local P = require('tarn_production')
local c = {password=string.rep('x',32), replication_password=string.rep('y',32)}
function production_acl_probe() return box.session.user() end
local functions = {'production_acl_probe'}
local s = box.schema.space.create('t', {id=10001}); s:create_index('pk'); s:insert{1}
box.schema.user.create('tarn', {password='legacy'})
box.schema.user.grant('tarn','super')
box.schema.user.grant('guest','super')
P.install(c,functions,false)
assert(P.valid(functions,false), 'initial exact ACL')
local cases = {
    {'inherited super', function() box.schema.user.grant('tarn_service','super') end},
    {'system-space read', function() box.schema.user.grant('tarn_service','read','space','_user') end},
    {'raw data write', function() box.schema.user.grant('tarn_service','write','space','t') end},
    {'guest read', function() box.schema.user.grant('guest','read','space','t') end},
    {'legacy super', function() box.schema.user.grant('tarn','super') end},
    {'replication execute', function() box.schema.user.grant('tarn_repl','execute','function','production_acl_probe') end},
    {'missing RPC grant', function() box.schema.user.revoke('tarn_service','execute','function','production_acl_probe') end},
}
for _, test in ipairs(cases) do
    test[2](); assert(not P.valid(functions,false), test[1] .. ' escaped validation')
    P.install(c,functions,false); assert(P.valid(functions,false), test[1] .. ' did not reconcile')
end
for _, options in ipairs({{setuid=false}, {setuid=true,body='function() return true end'}}) do
    box.schema.func.drop('production_acl_probe')
    box.schema.func.create('production_acl_probe',options)
    assert(not P.valid(functions,false), 'incomplete/unexpected function attested')
    assert(not pcall(P.install,c,functions,false), 'unsafe function migration accepted')
    box.schema.func.drop('production_acl_probe'); P.install(c,functions,false)
    assert(P.valid(functions,false), 'offline function migration did not repair ACL')
end
local before=box.info.lsn
P.install(c,functions,false)
assert(box.info.lsn==before, 'unchanged ACL installation wrote WAL')
print('production ACL: exact grants, seven negative controls, function migrations and idempotent setup passed')
os.exit()
