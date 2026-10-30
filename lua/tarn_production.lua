-- Explicit Community Edition production boundary. The operator controls
-- this manifest and the CA; credentials never enter argv or public JSON.
local P = {}
local fio, json, bit = require('fio'), require('json'), require('bit')
local ffi = require('ffi')
pcall(ffi.cdef, 'unsigned int geteuid(void); long sysconf(int);')
local function read(path, limit)
    local f = assert(fio.open(path, {'O_RDONLY', 'O_NOFOLLOW'}), 'cannot open production input')
    local st = f:stat()
    assert(bit.band(st.mode, 0xf000) == 0x8000 and st.size <= limit, 'invalid production input file')
    assert((st.uid == 0 or st.uid == tonumber(ffi.C.geteuid())) and bit.band(st.mode,18) == 0, 'production input must not be writable by other UIDs')
    local s = f:read(limit + 1); f:close()
    assert(s and #s <= limit, 'oversized production input'); return s, st
end
function P.secret(path)
    local s, st = read(path, 65536)
    assert(st.uid == tonumber(ffi.C.geteuid()) and bit.band(st.mode, 63) == 0, 'secret must be private and owned by service UID')
    s = s:gsub('\r?\n$', '')
    assert(#s >= 32 and #s <= 128 and s:match('^[%w_-]+$'), 'storage secret must be a 32..128 byte base64url token')
    return s
end
local function file(path)
    local f = io.open(path); if not f then return nil end
    local s = f:read(65536); f:close(); return s
end
function P.resources(c, workdir)
    assert(jit.os == 'Linux', 'production host validation currently requires Linux')
    local h = c.host
    assert(type(h) == 'table' and h.min_cpus >= 1 and h.min_memory_bytes > 0 and h.min_free_disk_bytes > 0, 'positive resource minima required')
    local cpus = tonumber(ffi.C.sysconf(84))
    local quota = file('/sys/fs/cgroup/cpu.max')
    if quota then local q,p = quota:match('^(%d+)%s+(%d+)'); if q then cpus = math.min(cpus, math.floor(tonumber(q)/tonumber(p))) end end
    local mem = assert(file('/proc/meminfo')):match('MemTotal:%s*(%d+)')
    mem = assert(tonumber(mem)) * 1024
    for _, path in ipairs({'/sys/fs/cgroup/memory.max', '/sys/fs/cgroup/memory/memory.limit_in_bytes'}) do
        local n = tonumber(file(path)); if n then mem = math.min(mem, n) end
    end
    -- Linux 64-bit statvfs ABI, the supported production targets. This
    -- structure is private to this module to avoid an FFI type collision.
    pcall(ffi.cdef, [[struct tarn_statvfs { unsigned long bsize, frsize, blocks, bfree, bavail,
        files, ffree, favail, fsid, flag, namemax; int spare[6]; };
        int statvfs(const char *, struct tarn_statvfs *);]])
    assert(ffi.sizeof('long') == 8, 'production requires 64-bit Linux')
    local st = ffi.new('struct tarn_statvfs[1]')
    assert(ffi.C.statvfs(workdir, st) == 0, 'cannot measure storage filesystem')
    local free = tonumber(st[0].bavail) * tonumber(st[0].frsize)
    assert(cpus >= h.min_cpus and mem >= h.min_memory_bytes and free >= h.min_free_disk_bytes,
           'host below production CPU/memory/free disk minima')
    local budget = (c.memtx_mb + c.vinyl_memory_mb + c.vinyl_cache_mb) * 1024 * 1024
    assert(budget <= mem * .75 and budget <= h.min_memory_bytes * .75, 'storage quotas need at least 25% process/OS headroom')
    -- These pools are not whole-process bounds; pressure guards remain live.
    return {cpus = cpus, memory_bytes = mem, free_disk_bytes = free}
end
function P.load(path, workdir, replicas, peers, shard_id)
    local text = read(path, 1048576)
    local c = json.decode(text)
    local allowed = {version=true,public_uri=true,failure_domain=true,failure_domains=true,routes=true,
        listen_unix=true,service_user=true,replication_user=true,service_password_file=true,
        replication_password_file=true,memtx_mb=true,vinyl_memory_mb=true,vinyl_cache_mb=true,
        host=true,shard_id=true,workdir=true,replicas=true,peers=true}
    for k in pairs(c) do assert(allowed[k], 'unknown production setting: ' .. tostring(k)) end
    assert(c.shard_id == shard_id and c.workdir == workdir and json.encode(c.replicas) == json.encode(replicas), 'production node identity/configuration mismatch')
    assert(c.version == 1 and type(c.public_uri) == 'string' and c.public_uri:match('^tls://') and
           type(c.failure_domain) == 'string' and #c.failure_domain > 0 and type(c.routes) == 'table', 'invalid production manifest')
    assert(#replicas >= 3 and #replicas % 2 == 1, 'production requires odd RF >= 3')
    assert(c.service_user == 'tarn_service' and c.replication_user == 'tarn_repl', 'production uses separate fixed service and replication accounts')
    c.password = P.secret(c.service_password_file); c.replication_password = P.secret(c.replication_password_file)
    assert(c.password ~= c.replication_password, 'service and replication secrets must differ')
    assert(c.memtx_mb >= 64 and c.vinyl_memory_mb >= 16 and c.vinyl_cache_mb >= 0, 'invalid storage memory budget')
    local parent = fio.dirname(c.listen_unix)
    local st = assert(fio.lstat(parent), 'private socket directory missing')
    assert(c.listen_unix:sub(1,1) == '/' and st.uid == tonumber(ffi.C.geteuid()) and
           bit.band(st.mode, 0xf000) == 0x4000 and bit.band(st.mode,63) == 0, 'socket directory must be private and service owned')
    local found, domains, seen = false, {}, {}
    for _, uri in ipairs(replicas) do
        assert(uri:match('^tls://') and not uri:find('@',1,true), 'production requires TLS replica endpoints without credentials')
        local d = assert(c.failure_domains[uri], 'replica missing a failure domain')
        assert(type(d) == 'string' and #d > 0 and not domains[d] and not seen[uri], 'replicas require distinct failure domains/endpoints')
        domains[d], seen[uri] = true, true
        if uri == c.public_uri then found = true; assert(d == c.failure_domain, 'local failure domain mismatch') end
    end
    assert(found, 'local TLS endpoint absent from replica set')
    c.replicas = replicas
    c.resolve = function(uri)
        local path = assert(c.routes[uri], 'no TLS route for configured endpoint')
        assert(type(path) == 'string' and path:sub(1,1) == '/' and fio.dirname(path) == parent, 'TLS routes must use the private socket directory')
        return 'unix/:' .. path
    end
    assert(c.routes[c.public_uri] == c.listen_unix, 'self route must point to own private storage socket')
    for _, uri in ipairs(replicas) do c.resolve(uri) end
    for _, uris in pairs(peers) do for _, uri in ipairs(uris) do c.resolve(uri) end end
    P.enabled = true
    c.resources = P.resources(c, workdir)
    return c
end
local function reset_user(user, password)
    box.schema.user.create(user, {password = password, if_not_exists = true})
    local digest = require('digest')
    local want = digest.base64_encode(digest.sha1(digest.sha1(password)))
    local t = box.space._user.index.name:get{user}
    if not t[5] or t[5]['chap-sha1'] ~= want then box.schema.user.passwd(user, password) end
end
local function reconcile(user, wanted)
    local t = box.space._user.index.name:get{user}; if not t then return end
    local uid = t[1]
    local stale = {}
    for _, p in box.space._priv.index.primary:pairs({uid}, {iterator='EQ'}) do
        if p[2] == uid then stale[#stale+1] = p end
    end
    for _, p in ipairs(stale) do
        local mask = wanted[p[3] .. ':' .. p[4]] or 0
        if mask == 0 then box.space._priv:delete{uid,p[3],p[4]}
        elseif mask ~= p[5] then box.space._priv:update({uid,p[3],p[4]}, {{'=',5,mask}}) end
    end
end
function P.readable(space)
    if P.enabled and space.id >= 10000 then
        box.schema.user.grant('tarn_service', 'read', 'space', space.id, {if_not_exists=true})
    end
end
local function expected_function(name, language)
    local f = box.func[name]
    if not f or f.setuid ~= true or f.language ~= language then return false end
    local tuple = box.space._func:get{f.id}
    return tuple[2] == 1 and tuple[6] == ''
end
function P.install(c, functions, native)
    reset_user('tarn_service', c.password); reset_user('tarn_repl', c.replication_password)
    -- Public's default Lua console functions must not remain inherited.
    local wanted = {['universe:0'] = 24} -- SESSION | USAGE
    for _, name in ipairs(functions) do
        local f = box.func[name]; if f then wanted['function:' .. f.id] = 4 end
    end
    for _, t in box.space._space:pairs() do if t[1] >= 10000 then wanted['space:' .. t[1]] = 1 end end
    if native then for _, name in ipairs({'ts_select','ts_autocommit','ts_commit_local','ts_validate'}) do
        wanted['function:' .. assert(box.func['tarn_storage.' .. name]).id] = 4
    end end
    reconcile('tarn_service', wanted)
    reconcile('tarn_repl', {['universe:0']=24, ['role:' .. box.space._user.index.name:get{'replication'}[1]]=4})
    reconcile('guest', {})
    if box.schema.user.exists('tarn') then reconcile('tarn', {}) end
    box.schema.user.grant('tarn_service', 'session,usage', 'universe', nil, {if_not_exists=true})
    -- SQL's streaming IPROTO SELECT reads native committed tuples. It never
    -- gets raw write rights; SQL RPCs enforce the transaction protocol.
    for _, t in box.space._space:pairs() do if t[1] >= 10000 then
        box.schema.user.grant('tarn_service', 'read', 'space', t[1], {if_not_exists=true})
    end end
    box.schema.user.grant('tarn_repl', 'session,usage', 'universe', nil, {if_not_exists=true})
    box.schema.user.grant('tarn_repl', 'replication', nil, nil, {if_not_exists=true})
    for _, name in ipairs(functions) do
        local f = box.func[name]
        if f then
            assert(expected_function(name, 'LUA'), 'production needs an offline function migration: ' .. name)
        else box.schema.func.create(name, {setuid=true}) end
        box.schema.user.grant('tarn_service', 'execute', 'function', name, {if_not_exists=true})
    end
    if native then for _, name in ipairs({'ts_state','ts_select','ts_autocommit','ts_commit_local','ts_validate'}) do
        local full = 'tarn_storage.' .. name
        assert(expected_function(full, 'C'), 'production needs an offline native function migration: ' .. full)
        if name ~= 'ts_state' then
            box.schema.user.grant('tarn_service', 'execute', 'function', full, {if_not_exists=true})
        end
    end end
end
-- Read-only validation works on followers too. A half-finished migration or
-- interrupted schema setup must never attest to the production profile.
function P.valid(functions, native)
    local wanted = {['universe:0'] = 24}
    for _, name in ipairs(functions) do
        local f = box.func[name]
        if not expected_function(name, 'LUA') then return false end
        wanted['function:' .. f.id] = 4
    end
    for _, t in box.space._space:pairs() do if t[1] >= 10000 then wanted['space:' .. t[1]] = 1 end end
    if native then for _, name in ipairs({'ts_state','ts_select','ts_autocommit','ts_commit_local','ts_validate'}) do
        local full = 'tarn_storage.' .. name
        if not expected_function(full, 'C') then return false end
        if name ~= 'ts_state' then wanted['function:' .. box.func[full].id] = 4 end
    end end
    local function exact(user, want)
        local t = box.space._user.index.name:get{user}; if not t then return false end
        for _, p in box.space._priv.index.primary:pairs({t[1]}, {iterator='EQ'}) do
            if p[2] == t[1] then
                local key = p[3] .. ':' .. p[4]
                if want[key] ~= p[5] then return false end
                want[key] = nil
            end
        end
        return next(want) == nil
    end
    return exact('tarn_service', wanted) and exact('guest', {}) and
        (not box.schema.user.exists('tarn') or exact('tarn', {})) and
        exact('tarn_repl', {['universe:0']=24, ['role:' .. box.space._user.index.name:get{'replication'}[1]]=4})
end

return P
