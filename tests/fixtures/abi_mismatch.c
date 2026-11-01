/* An actual loadable C module with an incompatible state header. No data
 * procedure may run after Lua rejects the layout. */
#include <stdint.h>
#include <stddef.h>

extern int box_return_mp(void *, const char *, const char *);
extern int box_error_set(const char *, unsigned, uint32_t, const char *, ...);
static uint32_t layout = 10;

int ts_state(void *ctx, const char *args, const char *end) {
    (void)args; (void)end;
    uintptr_t address = (uintptr_t)&layout;
    unsigned char reply[9] = {0xcf};
    for (size_t i = 0; i < 8; ++i) reply[8 - i] = (address >> (8 * i)) & 255;
    return box_return_mp(ctx, (const char *)reply, (const char *)reply + 9);
}

#define REFUSE(name) \
int name(void *ctx, const char *args, const char *end) { \
    (void)ctx; (void)args; (void)end; \
    return box_error_set(__FILE__, __LINE__, 99, "upgrade fixture: incompatible native ABI"); \
}
REFUSE(ts_select)
REFUSE(ts_autocommit)
REFUSE(ts_commit_local)
REFUSE(ts_validate)
