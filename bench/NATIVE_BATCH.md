# Bounded native primary-key batches (#141)

The bounded Rust replacement for the Lua `tarn_select_keys` loop reduced
transaction-thread CPU per requested key in all 60 direct paired trials.
The B128/C64 hit case reached a median paired rate ratio of **1.812×**,
with both paths near transaction-thread saturation. End-to-end SQL results
were mixed: RF1 B128 hits had a median paired rate ratio of **0.895×** and
p50 ratio of **1.058×**, despite lower storage CPU. The feature therefore
remains **opt-in**, with `native_batch_reads: true`.

This experiment measures one warm memtx read kernel. It does not establish
a whole-OLTP, distributed-write, replication, failover or cross-database
performance advantage. The [storage inventory](../docs/native-storage.md)
explains the remaining Lua responsibilities and recommends a hybrid boundary.

## Contract and method

Both paths use full-primary-key point lookups: Lua already uses `idx:get`,
and Rust uses `box_index_get`. Each batch remains one RPC. The storage
contract preserves input order, duplicates and misses; SQL `IN` instead
deduplicates inputs and the SQL oracle checks the complete ordered result.
The native subset is 1–128 full primary keys on memtx, autocommit, with
write-claim checks even on missing rows. Streams, continuations, Vinyl,
unsupported shapes and resource caps retain guarded Lua. The existing
twelve-argument native read and ABI 11 / 312-byte state are unchanged.

Within each comparison both modes load the **same immutable instrumented
module**, gateway, client, corpus and configuration. Other native paths
remain enabled. Capability discovery occurs before timing. Five paired
samples per case alternate which mode starts, independently of shuffled
case order. Direct samples use 2 s warmup and 5 s measurement per side;
SQL samples use 1 s and 5 s. Identical pair seeds drive the workloads.
Every completed trial, error/outcome, raw latency and native-serving census
is retained. Ratios below are medians **of paired ratios**, with observed
minimum and maximum; these are not confidence intervals or pooled p99s.

The corpus has 100,000 rows and a 64-byte ASCII payload, composite primary
key `(tenant_id,id)` and `PARTITION BY KEY(tenant_id)`. Direct tests use
tenant 42, actual bucket 9, and the SQL-style per-key bucket vector; an
epoch/membership Map control is separate. SQL uses tenant 7. Catalog,
physical tuples, payloads, ordering, route bytes and corpus hashes are
audited. Duplicate/miss labels describe inputs, not equal returned-row counts.

LuaJIT JIT and normal GC remain enabled. Test hooks are asserted off.
Direct performance tests use `check_locks=false`; SQL uses `true` on both
modes. Do not subtract direct from SQL costs: their clients and optional
audit configurations differ. Local builds and task-owned benchmark fixtures
did not overlap measurement windows. Global processes were not paused,
and this is not evidence of an isolated host. Direct host load rose from
2.67 to 4.25; slow late trials on both paths remain included, without
attributing their cause to that load reading.

CPU is gated total transaction-thread time (`clock.thread64`), including
observer and background work, cross-checked against `/proc` ticks. The
maximum direct cross-check difference was 16.7 ms per approximately 5 s
phase. Idle controls are retained without subtraction. Latency includes
client decoding and exact row validation. A near-saturated transaction
thread with client headroom supports a capacity interpretation for that
specific direct case; unsaturated achieved rates do not establish capacity.

## Direct guarded RPC results

All 120 phases / 60 pairs passed exact row and outcome checks:
10,043,859 batches and 335,181,648 returned rows. Of the native requests,
6,041,548 were actually native-served and 107 explicitly handed back,
all for normal `load_window` rotation. Their timing and allocation cost
remains included; client/native/per-reason counters reconcile exactly.

Ratios are **native / Lua**, `median [min, max]`. Lower CPU and p99 are
better; higher achieved batch rate is better. B is input batch length;
C is concurrent in-flight requests. CPU is TX ns per requested key.

| Input / route | B | C | TX CPU | Batch rate | p99 |
|---|---:|---:|---:|---:|---:|
| hits / buckets | 8 | 1 | 0.687 [0.646, 0.718] | 1.047 [1.004, 1.068] | 0.846 [0.745, 1.201] |
| hits / buckets | 8 | 16 | 0.520 [0.501, 0.541] | 1.165 [1.115, 1.214] | 1.014 [0.996, 1.172] |
| hits / buckets | 8 | 64 | 0.471 [0.454, 0.489] | 1.486 [1.471, 1.541] | 0.774 [0.720, 0.829] |
| hits / buckets | 32 | 1 | 0.627 [0.519, 0.718] | 1.079 [0.973, 1.129] | 1.065 [0.690, 1.096] |
| hits / buckets | 32 | 16 | 0.488 [0.252, 0.514] | 1.344 [1.296, 3.351] | 0.828 [0.175, 0.923] |
| hits / buckets | 32 | 64 | 0.517 [0.498, 0.589] | 1.766 [1.586, 1.812] | 0.548 [0.507, 0.574] |
| hits / buckets | 128 | 1 | 0.567 [0.355, 0.591] | 1.220 [1.195, 1.787] | 0.634 [0.411, 0.748] |
| hits / buckets | 128 | 16 | 0.548 [0.526, 0.562] | 1.688 [1.645, 1.780] | 0.557 [0.433, 0.616] |
| hits / buckets | 128 | 64 | 0.527 [0.497, 0.546] | 1.812 [1.730, 1.974] | 0.558 [0.464, 0.579] |
| hits / Map | 128 | 16 | 0.571 [0.535, 0.592] | 1.642 [1.374, 1.788] | 0.527 [0.458, 0.927] |
| duplicates / buckets | 128 | 16 | 0.523 [0.323, 0.528] | 1.695 [1.685, 2.748] | 0.534 [0.125, 0.558] |
| miss / buckets | 128 | 16 | 0.633 [0.163, 0.643] | 1.326 [1.285, 4.754] | 0.814 [0.339, 0.873] |

All 60 CPU ratios were below 1. Eight individual p99 ratios exceeded 1
and one rate ratio was below 1. B8/C16 and B32/C1 also regressed in median
p99. No trials were excluded. Large outlier rate ratios in other cases
are retained in the ranges and are not selected as the headline.

For B128/C64 bucket-vector hits, median TX utilization was 0.968 core
(Lua) and 0.933 core (native); client CPU was 0.240 / 0.440 core with two
Tokio workers. Median key rate was **864,419 → 1,561,348 keys/s**. Its
64-byte logical payload corresponds to **52.8 → 95.3 MiB/s**; this is
logical returned payload, not physical disk or replicated bandwidth.
Measured socket bytes received were 63,142,207 → 114,072,793 bytes/s.
The median paired p99 ratio was 0.558 [0.464, 0.579].

## End-to-end SQL results

All 60 balanced SQL phases / 30 pairs passed exact `IN` set semantics,
ordering and payload checks. Each case used a single PyMySQL worker,
including its GIL, network, gateway and result validation. TX utilization
was unsaturated, so these rates are not SQL capacity estimates. There were
164,052 actually native-served queries and four `load_window` handbacks.

| Replication / input | B | TX CPU/query | Query rate | p50 | p99 |
|---|---:|---:|---:|---:|---:|
| RF1 / hits | 8 | 0.649 [0.540, 0.772] | 1.106 [0.977, 1.427] | 0.964 [0.805, 0.999] | 0.814 [0.628, 1.165] |
| RF1 / hits | 32 | 0.505 [0.499, 0.683] | 1.185 [0.818, 1.269] | 0.782 [0.675, 1.252] | 0.933 [0.910, 1.096] |
| RF1 / hits | 128 | 0.537 [0.382, 0.590] | 0.895 [0.829, 1.244] | 1.058 [0.857, 1.178] | 0.914 [0.786, 1.134] |
| RF1 / duplicates | 128 | 0.369 [0.284, 0.574] | 1.056 [0.805, 2.116] | 0.931 [0.420, 1.445] | 0.896 [0.611, 0.951] |
| RF1 / miss | 128 | 0.770 [0.615, 0.903] | 0.994 [0.749, 1.311] | 0.994 [0.626, 1.729] | 1.075 [0.829, 1.147] |
| RF3 fsync / hits | 128 | 0.445 [0.305, 0.568] | 1.152 [0.957, 1.499] | 0.895 [0.702, 1.217] | 0.764 [0.703, 0.858] |

All 30 paired storage CPU ratios were below 1, but the RF1 B128 hit rate
regressed by 10.5% at the paired median and p50 rose 5.8%. RF1 B128 misses
had a median p99 regression. The full ranges and per-trial values remain
published. Reduced storage CPU is not a uniform end-to-end latency win.

The RF3 control used three joined replicas before loading data,
`wal_mode=fsync`, quorum 2 and an explicitly synchronous user space. All
replicas confirmed the same corpus and vclock before and after measurement.
The leader reported native ABI 11 / size 312 / ready 1; read-only followers
may legitimately report native disabled. This measures reads over that
confirmed corpus, not RF3 write throughput, failover or total replica CPU.

## Allocation and GC scope

The optional `alloc-metrics` feature defaults off in production. Both
measured modes use the same feature-on binary, so instrumentation overhead
is included. Counters record successful Rust allocator requested capacities;
reallocation charges new capacity as allocated and old capacity as freed,
even in place. They are cumulative, independently relaxed, module-wide
`u64` counters. The fixed-stack diagnostic makes no Rust allocation itself.
They exclude Lua, Tarantool C/tuple allocations and RSS.

For direct B128/C64 hits, native requested **35,584 Rust bytes/batch**.
Lua-managed allocation was approximately **29,454 bytes/batch** on Lua,
versus **1.3 bytes/batch** on native. These separate scopes do **not**
prove a total allocation or memory saving; the Rust requested category is
larger than the Lua-managed category it replaces. Rust requested-minus-freed
deltas were zero in every primary direct phase. Across the five B128/C64
phases, `gc_steps_atomic` totaled 531 for 142,799 Lua batches and zero for
261,010 native batches. Other GC categories remain in raw evidence; zero
atomic steps is not a guarantee of zero GC, allocation or retained memory.

## Guard and functional controls

- **Disjoint prepared-W / busy lock index:** ten B32/C1 hit phases, five
  3 s paired trials after 0.5 s warmup, 91,043 batches. Two genuine prepared
  W claims outside the query domain were checked before/after every phase,
  with the intended base insert absent. Native served 47,893 batches with
  no handback; all five CPU ratios were below 1, median
  0.570 [0.536, 0.599]. This is not overlapping-contention capacity.
- **Overlapping existing and missing keys:** four separate loopback CALL
  diagnostics returned exact typed 1213, after approximately the configured
  1 s lock wait. Both native calls first returned `['lua']` with `w_lock`
  reasons, then guarded Lua refused. No hidden retry occurred. These single
  in-server diagnostics are not external-client tail measurements. All
  three owned prepared promises were explicitly aborted, claims returned
  to zero, committed rows stayed unchanged and configuration was restored.
- **Vinyl:** six dumped-run B32/C1 functional phases across hits,
  duplicates and misses. All 2,327 native attempts explicitly handed back
  for `vinyl`, with zero native-served batches and exact results. The
  direct control deliberately probes native; the production gateway already
  filters Vinyl. This does not measure deployment overhead from an extra
  RPC. The 100,000-row persistent-run witness and disabled tuple cache do
  not establish cold OS-cache or disk bandwidth.
- **Pressure test observation:** the full private Vinyl admission suite
  passed 8/8 methods, with 5,642 acknowledged writes stored and 323 exact
  1205 refusals absent. A test-only correction captures raw native regulator
  state immediately after changing the I/O control, on the pressured node,
  while retaining independent pressure/configuration/identity/latency checks.
  The old periodic witness also passed naturally in this validation run;
  it did not reproduce the original CI missed-sampling schedule.

All owned fixtures were stopped, independently checked absent and checked
for OOM events. Frozen hashes were rechecked, and offline WAL was indexed
and hash-verified before removing runtime data. WAL receipts are retained;
the public data bundle excludes WAL and compiled binaries.

## Provenance and limits

The host inventory was read after measurement: Apple M4 Max, 16 logical
CPUs, 128 GiB physical RAM; macOS 27.0.1 ARM64. Docker Desktop 28.0.4
reported 16 vCPUs, 8,217,190,400 bytes memory and Linux 6.10.14-linuxkit.
This inventory is not proof of start-time resource isolation. Each measured
storage container had a 2 CPU / 1 GiB limit, memtx 256 MiB and Vinyl 32 MiB.
RF1 used `wal_mode=write`; the separate RF3 read control used fsync.

| Input | Exact source or SHA-256 |
|---|---|
| Native runtime source | `05349d8b56fe96dedd9b26f8971dc401a5535a13`, 22 matched inputs |
| Native instrumented `.so` | `8b9700a9e91328dffb1e70af4d889ef9964d715c04d61b46abfe59b25632887b` |
| Gateway source | `4d94cc35ad30424b83cefb38cfdb4be1f2ebbc91`, 70 matched production inputs |
| Gateway binary | `f5dd498aebaf71298473cc31d9635d095201d25d5611588610eeac759f0c4e84` |
| Rust client source | `68ddb228c570a20ae5652402a23545d722649c44` |
| Rust client binary | `b6d0665bf6a9796aa8133cd08ffb8bd8c1308028333f72b8f3ba702c23b71564` |
| Direct driver checkpoint | `90e174a8bf0687c7b374bfacbb522bee5a626a9d` |
| Balanced SQL driver checkpoint | `2614131646d62dfaff333d7b2749ca3f69177484` |
| Guard-control harness | `2641f69`, frozen source and hashes in bundle |
| Tarantool image | `tarantool/tarantool@sha256:1a64f298fd5494728c660d221c4c5331e994ac4c57082246ca7c87c6497b0873` |
| Actual Tarantool | `3.9.0-0-g8b9a0f59a9`, Linux ARM64 |

Later commits add harnesses, test observation and documentation; measured
production runtime inputs remain unchanged. Native was built with Rust
1.98.1 in the pinned Linux ARM64 builder; build commands and before/after
source receipts are included. Rebuilding may produce a different binary.
These results do not establish speed of an uninstrumented build.

The historical 30-phase SQL cohort had imbalanced mode order and is
explicitly nonqualifying. It is retained with its method note beside the
fresh balanced cohort, not silently discarded. The older selection profile
used a different build and enabled test hooks: setting `TARN_TEST_HOOKS=0`
is truthy because it is nonempty. Its environment note, raw results and
selection-only scope are retained; no cross-build gain uses it. A separate
prepared-control adapter failed to JSON-encode a binary lock key before
any timed phase. That setup failure is retained; successful fresh controls
use a base64 witness. No mutation was replayed after uncertain transport.

TLA+ checks ordered duplicate/miss behavior, missing-key claims, one-image
results and complete fallback, with three failing controls. The actual-used
Verus kernel proves bounded work/rows/bytes and overflow safety. The
[verification document](../docs/verification.md#bounded-native-primary-key-batches-issue-141)
states the trusted CAPI, no-yield and correctly published mirror premises.
These are bounded contracts, not a proof of all unsafe FFI or Tarantool C.
Keep Lua's ownership/waiting/recovery wrapper; select further kernels from
complete workload profiles. A native APPLY kernel requires a separate
ownership, durability, lazy-index and cancellation analysis.

## Raw evidence and reproduction

[Download the immutable data bundle](https://github.com/cstheorist/tarn/releases/download/bench-native-key-batches-141-v1/tarn-native-key-batch-evidence.tar.gz)
(83,160,037 bytes), SHA-256:

```text
13b8a8e96cacf4dcf6b91e85d2e8f1f7b8c28638a3891ee6a2a42b71a395f389
```

The data-only prerelease contains all primary phases, raw u64LE nanosecond
latencies, u8 outcomes, MessagePack routes, allocation/GC/CPU counters,
guard controls, historical appendices, frozen harnesses and build receipts.
Its `INDEX.json` covers 2,826 files, with 878 recorded raw-digest checks.
Only irrelevant host home/tmp path prefixes and expired test lease owners
in text were pseudonymized; original metadata hashes and the transformation
policy are recorded. Raw bytes,
numeric values and digests were not changed. Verify the download hash,
extract it, then run `python3 tarn-native-key-batch-evidence/VERIFY.py`.
The small [phase/pair tables and receipt](evidence/native-key-batch/) are
also tracked in Git.

Use a clean checkout of `2641f69` to reproduce the complete harness,
Rust 1.98.1, Docker, Python 3 and `PyMySQL==1.2.3`. Build the host gateway
and Rust client first, outside all measurement windows:

```sh
cargo build --locked --release -p tarn-server
cargo build --locked --release -p tarn-iproto --example native_batch
```

Build `tarn-storage` with `--features alloc-metrics` in matching **Linux
ARM64**, not as a macOS dylib. The original native builder image was
`rust:1.98.1-slim-bookworm@sha256:ff521445a372125ed4f76e1453a1f8098f2d05332d1601d30db1c1f62757e730`;
its exact build commands are in `build-receipts/native/manifest.json`.
After building, record the input/artifact hashes in `native-manifest.json`
and verify that sources did not change during the build. The fixture
requires this minimal manifest shape (it is a hash receipt, not a compiler
attestation):

```python
import hashlib, json, pathlib, subprocess
sha = lambda p: hashlib.sha256(pathlib.Path(p).read_bytes()).hexdigest()
paths = subprocess.check_output([
    'git', 'ls-files', 'crates/tarn-storage', 'crates/tarn-verified'
]).decode().splitlines()
pathlib.Path('native-manifest.json').write_text(json.dumps({
    'artifact_sha256': sha('libtarn_storage.so'),
    'runtime_sources': {p: sha(p) for p in paths},
}, indent=2))
```

Set the following paths to the built artifacts; use the same files for
every fixture. Copy `direct/core/harness/case-plan.json` from the bundle
to `case-plan.json` for the exact 12-case matrix.

```sh
image=tarantool/tarantool@sha256:1a64f298fd5494728c660d221c4c5331e994ac4c57082246ca7c87c6497b0873
gateway=$PWD/target/release/tarn-server
client=$PWD/target/release/examples/native_batch
native=$PWD/libtarn_storage.so
manifest=$PWD/native-manifest.json

python3 bench/native_batch_fixture.py --source-ref 2641f69 --image "$image" \
  --gateway "$gateway" --native "$native" --native-manifest "$manifest" \
  --client "$client" --tenant 42 --cases-json case-plan.json \
  --native-allocs --modes lua,native --trials 5 --warmup 2 --seconds 5 --deadline 1100

python3 bench/native_batch_sql_fixture.py --source-ref 2641f69 --image "$image" \
  --gateway "$gateway" --native "$native" --native-manifest "$manifest" \
  --client "$client" --replicas 1 --batches 8,32,128 --shapes hits \
  --trials 5 --warmup 1 --seconds 5 --deadline 420
```

Run SQL RF1 duplicates/misses separately with `--batches 128
--shapes duplicates,miss`; RF3 hits with `--replicas 3 --batches 128
--shapes hits`. Run the direct prepared controls with
`--prepared-w-controls --native-allocs --tenant 42 --modes lua,native
--trials 5 --warmup .5 --seconds 3 --deadline 60`. Run Vinyl with
`--engine vinyl --dump-before-measure --native-allocs --tenant 42
--route-modes buckets --batches 32 --shapes hits,duplicates,miss
--workers 1 --modes lua,native --trials 1 --warmup .5 --seconds 1`.
Pass the same required image/artifact arguments for each direct command.
Run fixtures sequentially; each owns only its fresh labelled resources,
retains raw results and independently verifies cleanup. Do not use global
stop/build scripts or drop another database's caches.
