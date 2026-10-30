-- Exact integers, NULL/fills, scan coverage, prepared locks and Vinyl history.
local net = require('net.box')
local fiber = require('fiber')
local json = require('json')
local c = net.connect(os.getenv('S1'), {user='tarn', password='tarn', wait_connected=5})
local function check(name, cond, detail)
    assert(cond, name .. ': ' .. json.encode(detail))
    print('  ok   ' .. name)
end
for _, engine in ipairs({'memtx', 'vinyl'}) do
    local name = 'test.push135_' .. engine
    local r = c:call('tarn_create_table', {{name=name, engine=engine,
        format={{name='id',type='unsigned'}, {name='n',type='integer',is_nullable=true},
                {name='u',type='unsigned',is_nullable=true}, {name='b',type='boolean',is_nullable=true},
                {name='bucket_id',type='unsigned'}},
        indexes={{name='primary',parts={{field=1,type='unsigned'}}}}}})
    check(engine .. ' create', r[1] == 'ok', r)
    c:reload_schema()
    local sid = c.space[name].id
    local writes = {
        {'insert',sid,{1,-9223372036854775808LL,18446744073709551615ULL,false,1}},
        {'insert',sid,{2,9223372036854775807LL,9007199254740993ULL,true,1}},
        {'insert',sid,{3,box.NULL,box.NULL,box.NULL,1}},
        {'insert',sid,{4,5,7,false,1}},
    }
    check('seed', c:call('tarn_autocommit',{writes})[1]=='ok', engine)
    local aggs = {{'sum',2,box.NULL},{'min',2,box.NULL},{'max',2,box.NULL},
                  {'sum',3,box.NULL},{'count',2,box.NULL},{'countstar',0,box.NULL},
                  {'count',4,box.NULL},{'sum',9,7}}
    local function args(preds, omit, lock, begin)
        return {sid,0,{},'ALL',box.NULL,true,preds or {},omit or {},lock~=false,begin==true,box.NULL,aggs}
    end
    r = c:call('tarn_reduce',args())
    check(engine .. ' exact signed/unsigned partials and boolean COUNT', r[1]=='ok' and r[3]==true
        and r[2][1]=='4' and r[2][2]==-9223372036854775808LL and r[2][3]==9223372036854775807LL
        and r[2][4]=='18455751272964292615' and r[2][5]==3 and r[2][6]==4 and r[2][7]==3 and r[2][8]=='28', r)
    r = c:call('tarn_reduce',args({},{{1},{3},{99}}))
    check(engine .. ' excluded extrema and NULLs', r[1]=='ok' and r[2][1]=='9223372036854775812'
        and r[2][2]==5 and r[2][3]==9223372036854775807LL and r[2][6]==2, r)
    r = c:call('tarn_reduce',args({{2,'null',box.NULL,box.NULL,true}}))
    check(engine .. ' all-NULL aggregate input', r[1]=='ok' and r[2][1]==nil and r[2][2]==nil
        and r[2][3]==nil and r[2][5]==0 and r[2][6]==1, r)
    c:eval("require('tarn').cfg.scan_page_rows = 2")
    local pargs = {sid,0,{},'ALL',box.NULL,box.NULL,true,true,box.NULL,false,box.NULL,box.NULL,
        {{1,box.NULL},{9,7}},{{2,'eq',5,box.NULL,true}},{{1,box.NULL}}}
    r = c:call('tarn_select_projected',pargs)
    check(engine .. ' rejected page preserves position and last covered row', r[1]=='ok' and #r[2]==0
        and r[3]~=nil and r[4]==2 and r[5][1]==2 and #r[5]==1, r)
    pargs[11] = r[3]
    r = c:call('tarn_select_projected',pargs)
    check(engine .. ' later page returns projection and fill', r[1]=='ok' and #r[2]==1 and r[2][1][1]==4
        and r[2][1][2]==7 and r[4]==2 and r[5][1]==4, r)
    c:eval("require('tarn').cfg.scan_page_rows = 4096")
    local st = c:new_stream()
    r = st:call('tarn_reduce',args({},nil,false,true))
    check(engine .. ' begins MVCC stream', r[1]=='ok' and st:eval('return box.is_in_txn()'), r)
    c:call('tarn_autocommit',{{{'replace',sid,{4,6,7,false,1}}}})
    r = st:call('tarn_validate',{'P135RO_'..engine,{},{{sid,0,box.NULL,box.NULL,true,true}},true})
    check(engine .. ' overwritten aggregate read fails validation', r[1]=='conflict' and r[2]==1213, r)
    local tx = 'P135W_'..engine
    r = c:call('tarn_prepare',{tx,1,{1},{{'replace',sid,{4,8,7,false,1}}},{},{},fiber.time()})
    check(engine .. ' prepare', r[1]=='yes', r)
    local pending = c:call('tarn_reduce',args({{2,'eq',99,box.NULL,true}}),{is_async=true})
    fiber.sleep(.05)
    check(engine .. ' even a rejected W-locked row blocks reduction', not pending:is_ready(), {})
    c:call('tarn_apply',{tx})
    r = pending:wait_result(5)[1]
    check(engine .. ' completes after APPLY', r[1]=='ok' and r[2][6]==0, r)
    if engine == 'vinyl' then
        c:call('tarn_test_hook',{'reduce_scan','hold'})
        pending = c:call('tarn_reduce',args(),{is_async=true})
        c:call('tarn_test_hook',{'reduce_scan','reached'})
        r = c:call('tarn_prepare',{'P135H',1,{1},{{'replace',sid,{4,9,7,false,1}}},{},{},fiber.time()})
        check('prepare during captured Vinyl reduction', r[1]=='yes', r)
        c:call('tarn_apply',{'P135H'})
        c:call('tarn_test_hook',{'reduce_scan','release'})
        r = pending:wait_result(5)[1]
        check('Vinyl reduction retries across taken/released W lock', r[1]=='ok' and r[2][1]=='8', r)
    end
    r = c:call('tarn_autocommit',{{
        {'replace',sid,{1,4503599627370495,1,false,1}},
        {'replace',sid,{2,4503599627370495,1,false,1}},
        {'replace',sid,{3,1,1,false,1}},
        {'replace',sid,{4,box.NULL,1,false,1}},
    }})
    check('seed sum promotion boundary', r[1]=='ok', r)
    r = c:call('tarn_reduce',args())
    check(engine .. ' promotes the exact small-number sum before precision loss',
        r[1]=='ok' and r[2][1]=='9007199254740991', r)
end
c:close()
print('typed storage read controls passed')
os.exit(0)
