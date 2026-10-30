# Production security evidence

`native-matched-debug.json` and `lua-integrated-debug.json` passed at
`37e3494d48b99c10a7965b709053f942e0d302ed`, including current main
`8b12c4525bd677499a91b973afbc86cbcbb0cafd`. Every recorded source-file hash
was compared with that Git tree and matched. The files retain binary hashes,
versions, exact commands, driver results, configuration paths, raw samples,
clock/failover checks, and the witnessed 1180/40003 result. No fixture
credentials or private keys are included. Release CI independently retains
its own results/configurations/logs and exact checkout revision.

These are Linux aarch64 debug builds with Rust 1.98.1, debug information off,
Tarantool 3.9.0, PyMySQL 1.0.2, Connector/Python 26.10.0, and MariaDB CLI
10.11.19, running on a shared Docker VM. The native module was loaded and
served requests; the separate Lua-only run checks the fallback deployment.
Three logical RF3 processes on one host do not establish physical failure
domains or common-mode clock accuracy.

## Local matched costs

All bridge TCP sockets use TCP_NODELAY in both cases. Storage control and TLS
use the same TCP/Unix hops, copy buffers, admission, RF3 identities, synchronous
counter RPC, connection count and clock policy. Each condition retains three
sequential 100-operation blocks, after warmup; these are not independent
machine trials. MySQL mode order alternates across three separate connections
per mode. Connection times are separate observations, not a confidence interval.

| Storage transport | WAL | Block p50 latency, ms | Connection, ms |
|---|---|---|---:|
| mTLS | fsync | 0.731, 0.716, 0.707 | 2.064 |
| plaintext loopback control | fsync | 0.699, 0.711, 0.702 | 0.469 |
| plaintext loopback control | write | 0.703, 0.723, 0.724 | 0.379 |
| mTLS | write | 0.762, 0.752, 0.728 | 2.299 |

With storage mTLS and fsync fixed, MySQL plaintext point-read block p50s were
0.573, 0.669, 0.646 ms; MySQL TLS p50s were 0.745, 0.730, 0.674 ms. The
small overlapping fsync/write ranges do not identify a reliable durability
penalty on this shared VM. Do not extrapolate these observations into a
production latency, throughput, or universal encryption/durability percentage.
The scripts permit matched release measurements on deployment hardware.

## Earlier diagnostics

The two `preintegration-*-debug.json` files retain successful functional
runs before current-main integration and the TCP_NODELAY correction. The
native file's source hashes all match `f7e5fc6`. Their **cost results are
not qualified encryption comparisons**: the plaintext upstream client and
TLS upstream client had different Nagle settings, and TLS acceptance also
introduced a roughly 40 ms delayed-ACK handshake cost. They are retained to
show why the matrix was corrected and repeated, not to support current cost
claims. The final source-bound runs above supersede them.
