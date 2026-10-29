/*
 * Enter Lua through its public C API, not a stored LuaJIT FFI callback.
 * A callback uses LuaJIT's cached CTState.L, which need not belong to the
 * native procedure's current fiber. luaT_state() is explicitly the GLOBAL
 * state; use it only as an entry stack for these trusted, nonyielding hooks.
 * Transaction identity and terminal hooks remain owned by the current fiber.
 *
 * Every potentially allocating operation is below lua_cpcall's C-only
 * protection boundary. In particular an OOM must not longjmp across Rust.
 * Do not use luaT_cpcall here: its error-to-diag conversion occurs after
 * lua_cpcall returns and may itself allocate. The outer function only gets
 * and restores the stack top. No Rust State borrow may span this call.
 *
 * These declarations are the public Lua 5.1 / Tarantool module API, exported
 * by the hosting process (Tarantool extra/exports, src/lua/utils.h). Avoid a
 * build-time dependency on the host's installed Lua or Tarantool headers.
 */
#include <stddef.h>
#include <stdint.h>

typedef struct lua_State lua_State;
typedef int (*lua_CFunction)(lua_State *);

extern lua_State *luaT_state(void);
extern int lua_gettop(lua_State *L);
extern void lua_settop(lua_State *L, int index);
extern int lua_type(lua_State *L, int index);
extern void *lua_touserdata(lua_State *L, int index);
extern double lua_tonumber(lua_State *L, int index);
extern void lua_pushlstring(lua_State *L, const char *value, size_t size);
extern void lua_rawget(lua_State *L, int index);
extern void luaL_pushuint64(lua_State *L, uint64_t value);
extern void lua_call(lua_State *L, int arguments, int results);
extern int lua_cpcall(lua_State *L, lua_CFunction function, void *context);

enum {
    LUA_TNUMBER = 3,
    LUA_TTABLE = 5,
    LUA_TFUNCTION = 6,
    LUA_REGISTRYINDEX = -10000,
};

struct guard_call {
    uint8_t kind;
    uint64_t argument;
    uint8_t result;
};

static void
raw_field(lua_State *L, int table, const char *name, size_t size)
{
    lua_pushlstring(L, name, size);
    /* The table's relative index changes when the key is pushed. */
    lua_rawget(L, table < 0 && table > LUA_REGISTRYINDEX ? table - 1 : table);
}

static int
guard_dispatch(lua_State *L)
{
    struct guard_call *call = lua_touserdata(L, 1);
    const char *name;
    size_t size;
    switch (call->kind) {
    case 1: name = "native_controls"; size = sizeof("native_controls") - 1; break;
    case 2: name = "native_space"; size = sizeof("native_space") - 1; break;
    case 3: name = "native_mark"; size = sizeof("native_mark") - 1; break;
    case 4: name = "native_ok"; size = sizeof("native_ok") - 1; break;
    default: return 0;
    }
    raw_field(L, LUA_REGISTRYINDEX, "_LOADED", sizeof("_LOADED") - 1);
    if (lua_type(L, -1) != LUA_TTABLE)
        return 0;
    raw_field(L, -1, "tarn", sizeof("tarn") - 1);
    if (lua_type(L, -1) != LUA_TTABLE)
        return 0;
    raw_field(L, -1, "fresh", sizeof("fresh") - 1);
    if (lua_type(L, -1) != LUA_TTABLE)
        return 0;
    raw_field(L, -1, name, size);
    if (lua_type(L, -1) != LUA_TFUNCTION)
        return 0;
    if (call->kind != 1)
        luaL_pushuint64(L, call->argument);
    lua_call(L, call->kind == 1 ? 0 : 1, 1);
    if (lua_type(L, -1) == LUA_TNUMBER) {
        double result = lua_tonumber(L, -1);
        if (result == 1 || (call->kind == 1 && result == 2))
            call->result = (uint8_t)result;
    }
    return 0;
}

uint8_t
tarn_lua_guard(uint8_t kind, uint64_t argument)
{
    if (kind < 1 || kind > 4 || (kind == 2 && argument > UINT32_MAX))
        return 0;
    lua_State *L = luaT_state();
    if (L == NULL)
        return 0;
    int top = lua_gettop(L);
    struct guard_call call = {kind, argument, 0};
    int status = lua_cpcall(L, guard_dispatch, &call);
    lua_settop(L, top);
    return status == 0 ? call.result : 0;
}
