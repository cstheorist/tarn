-- Observation wrapper only: preserve the original fixture and its JSON stdout.
local fixture, profile, iterations = assert(arg[1]), assert(arg[2]), assert(arg[3])
assert(arg[4] == nil and fixture:sub(1, 1) == '/', 'absolute fixture path required')
assert(iterations == '20000', 'trace diagnostic requires 20000 iterations')
assert(profile == 'ordinary' or profile == 'construction-warm'
       or profile == 'retained-construction-warm', 'unsupported trace profile')
local output = assert(os.getenv('TARN_GC_TRACE_DUMP'), 'runner must supply dump path')
assert(output:sub(1, 1) == '/', 'absolute dump path required')
local chunk = assert(loadfile(fixture))
_G.arg = {[0] = fixture, profile, iterations}
-- Shipped, source-pinned debug module: lifecycle, bytecode, IR, plain text.
-- No machine code, register dumps, or automatic target-coverage inference.
require('jit.dump').on('tbiT', output)
chunk()
