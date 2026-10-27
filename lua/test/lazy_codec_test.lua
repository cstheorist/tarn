-- No database: native MessagePack key identity used by the lazy protocol.
local mp, ffi = require('msgpack'), require('ffi')
local function hex(s) return (s:gsub('.', function(c) return string.format('%02x', c:byte()) end)) end
local vectors = {
    {'0', '9100'}, {'5', '9105'}, {'127', '917f'}, {'128', '91cc80'},
    {'255', '91ccff'}, {'256', '91cd0100'}, {'32767', '91cd7fff'}, {'32768', '91cd8000'},
    {'9007199254740991', '91cf001fffffffffffff'}, {'9007199254740992', '91cf0020000000000000'},
    {'9007199254740993', '91cf0020000000000001'}, {'9223372036854775807', '91cf7fffffffffffffff'},
    {'18446744073709551615', '91cfffffffffffffffff'},
    {'-1', '91ff'}, {'-5', '91fb'}, {'-32', '91e0'}, {'-33', '91d0df'},
    {'-127', '91d081'}, {'-128', '91d080'}, {'-129', '91d1ff7f'}, {'-255', '91d1ff01'},
    {'-256', '91d1ff00'}, {'-32768', '91d18000'}, {'-32769', '91d2ffff7fff'},
    {'-9007199254740991', '91d3ffe0000000000001'}, {'-9007199254740992', '91d3ffe0000000000000'},
    {'-9007199254740993', '91d3ffdfffffffffffff'}, {'-9223372036854775808', '91d38000000000000000'},
}
local function at_most(s, bound) return #s < #bound or (#s == #bound and s <= bound) end
local n = 0
for _, v in ipairs(vectors) do
    local x = assert(tonumber64(v[1]))
    local cases = {{'native', x}}
    local negative = v[1]:sub(1, 1) == '-'
    local magnitude = negative and v[1]:sub(2) or v[1]
    if not negative then cases[#cases + 1] = {'uint64', ffi.cast('uint64_t', x)} end
    if negative or at_most(magnitude, '9223372036854775807') then
        cases[#cases + 1] = {'int64', ffi.cast('int64_t', x)}
    end
    if at_most(magnitude, '9007199254740992') then cases[#cases + 1] = {'number', tonumber(x)} end
    for _, c in ipairs(cases) do
        local actual = hex(mp.encode({c[2]}))
        assert(actual == v[2], v[1] .. ' ' .. c[1] .. ': ' .. actual .. ' ~= ' .. v[2])
        n = n + 1
        print('  ok   ' .. v[1] .. ' ' .. c[1] .. ' ' .. actual)
    end
end
print(n .. ' canonical integer vectors passed')
os.exit(0)
