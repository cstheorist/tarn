# Strong-read freshness and matched cost (#130)

This study compares Lua and native read paths at a pinned checkpoint with the same
strong-read contract. It does not compare against the earlier unbarriered
Tarn path or establish a TiDB/YugabyteDB advantage. Historical ABI11 results
in [NATIVE_BATCH.md](NATIVE_BATCH.md) retain their original scope.

The completed measurement below uses `62cf4c6`. A subsequent withdrawal
dependency fix changes promise admission and removal accounting. Its live
regressions and model are described below; a fresh complete matched study
of that updated runtime is pending. The checkpoint measurements do not
establish the updated revision's performance.

## Contract and proof boundary

Before first data access, both paths prove a fresh quorum-confirmed prefix
using an empty transaction and Tarantool's public linearizable isolation
setter. The setter collects fresh peer vclocks, catches up WAL and settles
the synchronous queue. Autocommit discards only that empty transaction;
first stream reads retain the exact transaction and its MVCC read set.
Replicated stream completion obtains another prefix in a separate fiber
before validating the original read set. Native RF3 completion hands back
before effects. Unmarked ambient transactions are refused unchanged.

Memtx and Vinyl use the pinned empty-barrier/confirmed-view composition;
direct Vinyl LINEARIZABLE access is unsupported. Raw native reads retain
local visibility. Opt-in lazy indexes can omit recent index matches. The
[design](../docs/design.md), [operations](../docs/operations.md#strong-reads-and-storage-rollout)
and [verification](../docs/verification.md#strong-read-freshness-stream-provenance-and-locking-replies-issue-130)
describe the engine restrictions and the trusted runtime, WAL, quorum,
membership-stability and clock premises. Administrative changes must
preserve guarantees through in-flight synchronous commits.

Persisted immutable timing policy and relevant-writer certification fence
uncertain new votes and GC. The GC high-water mark and deletions share one
transaction. Decided completion remains available under clock faults and
preserves inherited receipt timestamps and expiry horizons. Non-idempotent
mutations are never automatically replayed after outcome uncertainty;
idempotent protocol recovery can redeliver requests.

Accepted wall-clock corrections stay within the unchanged elapsed-time drift
budget. Monotonic reversals still fault; publication retains the maximum of
the previous logical wall and current raw wall. Raw persisted-floor checks
precede clipping, and one probe publishes one accepted wall/monotonic pair.
The expiry argument requires all relevant storage and timestamp issuers to
stay in one fixed reference band of width configured skew over history;
current pairwise agreement cannot establish that premise. The model checks
this sufficient contract and reaches contradictory NO outcomes when its
floor or reference-band premises are broken. No common-mode clock-quality
or end-to-end runtime refinement proof is claimed.

The complete registered matrix passed in [CI run 40794098539](https://github.com/cstheorist/tarn/actions/runs/40794098539):
246 cases, comprising 66 positive checks and 180 expected controls. All
310 model/verified-source inputs match the actual CI merge tree, which
is identical to runtime revision `18cd3a9`. Verus reports 140 verified,
zero errors. The clock arithmetic body is shared with native execution;
Lua requires the matching exact integer bounds and retains an additional
conservative seconds check. These are bounded contracts and executable-body
proofs, not a complete runtime refinement.

The checkpoint's source-equivalent local native run passed 1,738 printed checks across
58 sections, including 85 exact-clock checks, 12 deterministic membership
readiness checks and 360 native differential checks. Counts are not unique
formal properties. That revision's both-engine RF3 pause, partition, asymmetric-link
and repeated-leadership tests witness raw stale data and strong-path refusal
after replacement ACKs. Their old leader is known read-only; the model's
final-prefix controls and positive live fences remain separate evidence.
No live fractured-commit reproduction is claimed.

Membership ACKs restore confirmed local policy/H outside the bucket latch.
They retain the invalidated epoch and discard old peer evidence; later
reads still obtain a fresh data prefix. A post-commit readiness failure
reports recorded membership with nonretryable 1180/HY000. The regression
stops the clock worker and checks immediate reads, inherited H and untouched
ambient transaction identity/read-set conflicts. Durable PREPARE/NO tests
wait for their actual relevant-writer evidence before their single attempt.

Earlier failed checkpoints remain available. These include a pre-bridge
LuaJIT SEGV of unproved cause, strict-clock refusals, and a test that attempted
a fresh NO before peer coverage was renewed. The final local RF1 cohort
completed 24 phases, but RF3 stopped after eight accepted phases: raw wall
advanced 712,247,099,861 ns while monotonic advanced 76,999,708 ns. The
unchanged 0.25-second drift guard correctly latched closed. No platform
clock cause is inferred. All owned local fixtures were cleaned and WAL
archives verified. Those incomplete ARM studies are excluded from the final
Linux results; no fault reset or timing-bound increase was used.

## Matched measurement method

The measured source is `62cf4c68018311f522a44a1dea71ea8362c0f950`
(native ABI14, 392 bytes); its production/model inputs are unchanged from
runtime revision `18cd3a9`. A manual Linux x86_64 runner builds the release
gateway, allocator-instrumented native module and observer from that same
checkout before timings. Builds use the pinned AMD64 Rust 1.98.1 Bookworm image; Tarantool uses
the pinned AMD64 sibling of the same 3.9.0 release. That exact image/binary
pair was exercised by every accepted phase. Runtime version, ELF architecture, artifact hashes,
source hashes and locked dependencies are retained. All three cohorts are
rerun together on that platform, without pooling local ARM samples.

The first isolated Linux attempt also remains excluded: all 24 RF1 phases
were accepted, but removing container-owned data failed under the runner
user. The corrected workflow runs measurement and cleanup under one recorded
UID for both paths and all cohorts, then grants uploader read access only
to retained evidence. Raw data and incomplete archives are excluded from
upload; verified closed archives are retained separately. The complete study
starts afresh rather than filling missing cells from earlier runs. That
first failed Linux run has no uploaded verified WAL export. A second attempt
completed RF1 and cleanup but failed an assertion that used the ARM build
version abbreviation for x86; its complete raw scope is retained separately.

Each cell has six paired trials, three in each Lua/native order. Case order
is shuffled per trial; pair order alternates by trial and stable case index.
Each phase has a one-second warmup and five
timed seconds. All phases, raw latencies, outcomes and rejected setups are
retained. Neither path receives a weaker guarantee or selectively omitted
trial. CPU covers the queried leader's transaction thread, including common probe
boundary overhead; idle work is not subtracted. It does not aggregate peer,
WAL-thread or gateway CPU. Differences include entry guards, codecs,
allocation and lookup work rather than isolating one loop. Direct results
retain client CPU and IPROTO bytes; SQL results do not establish gateway CPU
or SQL wire bandwidth.

Each private fixture uses 100,000 committed rows with composite primary
key `(tenant_id,id)`, fixed tenant 7, a 64-byte payload and one logical
shard. Exact tuple hashes and replica catch-up are checked before and
after. Direct cells use B1/C1 and B128/C64 through one physical Rust client
connection; SQL RF3 cells use B32/C1 for hits and misses through the same
release gateway build with only native batch selection changed. Direct
fixtures retain two idle SQL gateways after loading the corpus.

Both paths load the same `alloc-metrics` module, enable lock-copy auditing,
leave normal LuaJIT JIT/GC running, and disable test hooks. These are
instrumented component timings. RF1 uses WAL `write`; RF3 uses synchronous
quorum 2, RF3 and WAL `fsync`. These are separate cohorts: their difference
does not isolate replication or barrier overhead. The workload measures
reads of an established corpus, not durable-write or failover throughput.

The runner's CPU, memory, affinity, kernel and resource receipts accompany
the raw evidence. Each storage replica is capped at two CPUs and 1 GiB,
with 256 MiB memtx and 32 MiB Vinyl memory. RF3 plus host clients and two
gateways can oversubscribe the runner. One job runs all cohorts sequentially;
no build, model check or test fixture runs on that job during timings.
Physical-host isolation and production capacity are not established.

## Results

All 36 paired leader TX CPU ratios were below one. Each entry is the median
of six native/Lua ratios, followed by its observed range. Lower CPU/p99 and
higher rate ratios favor native. These are within-cell component results.

| Cell | Leader TX CPU ratio | Operation-rate ratio | p99 ratio |
| --- | ---: | ---: | ---: |
| direct-rf1 B1/C1 hits | 0.600 (0.587–0.622) | 1.156 (1.144–1.184) | 0.741 (0.717–0.793) |
| direct-rf1 B128/C64 hits | 0.437 (0.424–0.460) | 2.289 (2.172–2.360) | 0.402 (0.381–0.429) |
| direct-rf3 B1/C1 hits | 0.869 (0.865–0.878) | 1.068 (1.043–1.089) | 0.805 (0.790–0.862) |
| direct-rf3 B128/C64 hits | 0.527 (0.518–0.544) | 1.852 (1.802–1.898) | 0.506 (0.427–0.518) |
| sql-rf3 B32/C1 hits | 0.720 (0.712–0.731) | 1.100 (1.079–1.120) | 0.830 (0.809–0.914) |
| sql-rf3 B32/C1 miss | 0.872 (0.865–0.879) | 1.041 (1.013–1.069) | 0.889 (0.867–0.933) |

The corresponding native/Lua leader TX CPU medians, in microseconds per
operation, are 38.3/62.9, 143.1/325.4, 170.0/195.3, 193.2/366.4,
199.4/276.7 and 186.3/214.0 in table order. Direct operations are storage
RPC batches; SQL operations are statements. They are not interchangeable
capacity units. All observed p99 ratios in these six cells were below one.

The runner reported four CPUs/affinity slots and 16,367,524 kB total memory,
with Linux 7.0.0-1017-azure x86_64/glibc2.39. Per-replica limits still apply;
this does not establish physical-host isolation or an SSD/network ceiling.

The native path trades Lua-managed allocation for Rust allocator requests.
These counters have different scopes and exclude C/tuple allocation and
RSS; their difference is not a total-memory saving. All raw values, fallback
counters, latency arrays and wire measurements remain available.

The final PR also tightens the native yielding-input test precondition:
it refreshes the public bucket map before pausing a replica and records
cache/status changes. The production and benchmark implementations are
unchanged. The 32-check focused run requires exact payloads and actual native
service after the forced yield; the combined pre-withdrawal-fix local run passed 1,743
printed checks across 58 sections. Its source/log binding is in the separate
[final validation attestation](https://github.com/cstheorist/tarn/releases/download/bench-strong-read-freshness-130-v1/validation-attestation.json).
The measured revision `62cf4c6` ordinary CI test-setup failure preceded this
fixture correction and is retained separately from the successful read study.

Another retained Lua-only CI failure at `1640271` saw both identical alias
replays return `ok` while the global synchronous queue contained one entry.
Its writer identity was not logged. A deterministic control delayed the real
clock loader and reproduced that assertion with `clock:policy:1` written by
the clock worker, while the alias replays performed no replacement. The
corrected fixture waits for confirmed policy and local read readiness before
holding quorum, keeps the worker live and both original deadlines, and
retains the empty-queue assertion with an additional zero-metadata-write
check. A forced-startup control proves policy installation precedes alias
creation; ordinary native and Lua-only alias runs each pass all nine checks.
These later failures, controls and current source bindings belong to the
separate final validation evidence, rather than the immutable measured-data
archive. The control establishes a matching failure mechanism, not the
unlogged historical writer's identity.

The later `6984b45` CI passed both storage modes and the SQL/recovery/join
stages, then grow/shrink left a survivor trying to recover through stopped
shard 2. Its recorded dshard/parts were unavailable. A deterministic
three-shard baseline on that immutable source independently demonstrates
the missing contract: both engines retain a PREPARED/STAGING transaction
on shard 3 with decision shard 3 and participants `{2,3}`, while shard 2
is aborted and has no locks; the all-shard removal counters return zero.
The baseline records 14 passing and 15 expected failed assertions, with no
fixture exception. Three earlier startup/oracle discoveries remain excluded.

The fix fences supported new promises on every draining dependency and
tracks their full local/decision/participant scope through confirmation.
Every shard counts unresolved PREPARED and STAGING references before
withdrawal. Existing receipts and decided completion remain available;
recovery still obtains an authoritative missing-participant fence. The
bounded `DepartureDependencies` positive and five sensitivity checks pass;
the registry now has 252 cases (67 positive, 185 expected controls), with
317 proof inputs. A complete execution of that new matrix is pending.
Verus bodies are unchanged from the 140/0 checkpoint.

Both native-enabled and Lua-only focused runs pass 63 checks: setup,
authoritative recovery, zero-count and removed-membership acknowledgements,
actual owned-node-2 loss, survivor strong reads/bucket fencing, and decided
completion after a final latched clock fault. The local harness uses the
same public removal gates as Rust; actual SQL withdrawal remains an
integration-CI gate. An entry-hook case isolates in-flight accounting; its
later refusal also depends on membership/clock fencing. No exact historical
transaction reconstruction or native PREPARE kernel proof is claimed.
The full native suite after the reviewed controller changes passes 1,806
printed checks across 61 sections, with all 67 frozen Lua inputs recorded.

## Evidence and publication gates

[Summary](evidence/strong-read-freshness/summary.json),
[all phases](evidence/strong-read-freshness/phases.csv) and
[all pairs](evidence/strong-read-freshness/pairs.csv) retain every cell and
paired ratio. The [data-only archive](https://github.com/cstheorist/tarn/releases/download/bench-strong-read-freshness-130-v1/strong-read-freshness-130-linux-v1.tar.gz)
contains raw latency/outcome/route observations, source/build/harness hashes,
scoped functional gates, the whole ABI13 checkpoint, five excluded local
ABI14 cohorts and both excluded Linux studies. Original code/raw bytes are
unchanged; metadata path redactions retain original and public hashes.
Derived bytecode, binaries, vendor trees and WAL are omitted from this
public bundle. Private Actions artifacts retain verified offline WAL where
available; the first Linux permission failure is explicitly incomplete.

Archive: **48,388,800 bytes**, SHA-256
`a1aa3e0e8906831596097abc73c0f5da3156e6147c6280aebc96ec6a023e2f82`; 2,551 file members. The successful
[manual study](https://github.com/cstheorist/tarn/actions/runs/40812839124)
is separate from the passing source-equivalent functional CI at `18cd3a9`.
The measured-source ordinary CI failure is retained in the archive. Final
PR CI is attested separately in
[validation-attestation.json](https://github.com/cstheorist/tarn/releases/download/bench-strong-read-freshness-130-v1/validation-attestation.json)
after the final head passes; the measurement archive does not assert that
later gate. Publication and merge require that separate successful check.

## Interpretation

This short, hot-corpus read experiment does not cover steady-state GC,
SSD-scale data, compaction, overload arrival rates, distributed writes or
production capacity. Those require the matched study in #140. No result
here establishes a general advantage over TiDB or YugabyteDB.
