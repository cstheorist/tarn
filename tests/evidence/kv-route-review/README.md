# Shared KV route readiness review

The original complete hosted history and job log preserve a known-inventory
read-only route refusal before any BEGIN/mutation, despite direct observations
of a healthy replacement. Only that exact 2003 message and the existing
leader-change 1213 can repeat the independent autocommit warmup SELECT, for
at most eight attempts. Other connection errors and unknowns stop. No runtime
query or mutation retry is added.

The [index](index.json) binds the failed source and two fresh focused dev-build
controls to source/build inputs. Each passes 68 functional and 38 leader checks,
on memtx and Vinyl with native enabled or explicit Lua. Complete histories,
raw outcomes, node logs and process cleanup are retained. These successful
controls observed no 2003 retries (one Lua 1213 retry); they do not manufacture
a deterministic live reproduction of the new branch or replace full release CI.
