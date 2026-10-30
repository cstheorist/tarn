# Typed range-read component study (#135)

On this wide, selective scan, the existing executor transferred about
12.55 MB of stored tuples for each aggregate or two-row projected SQL result. Moving exact integer
reductions and filtered column projection to storage removed that transfer.
This is evidence for these two read shapes on one shared host, with the
same validated SQL results and isolation on both sides.

## Matched setup and provenance

- Two RF1 memtx shards, 256 MiB memtx memory each, WAL `fsync`, Tarantool
  3.9.0, 12,000 rows with a 1024-byte unused TEXT column, 16 tenant keys.
  Two rows match `tag = 1`; nullable signed integers and unsigned values
  above 2^53/at the uint64 maximum exercise exact result values.
- The same release gateway/native binaries and storage processes serve both
  variants. The gateways have one worker each, four connections per shard,
  and SERIALIZABLE isolation. `query_pushdown` is false/true; unique node IDs
  and proxy/listen ports distinguish the two gateways. Data, topology,
  durability, native module and limits match.
- One closed-loop PyMySQL client per sample, three warmups per variant and
  query, five pairs per query in alternating variant order, 30 executions
  per sample. Every result digest must equal the control. Every enabled
  sample must actually invoke the corresponding optional RPC; every
  disabled sample must omit it.
- Docker ARM64 on a shared host, three-CPU quota and 5 GiB container limit.
  Rust 1.98.1, release opt-level 3/thin LTO/four codegen units, debug symbols
  disabled to fit the compiler in the test container. Storage, gateways,
  load generator and counting proxies share that quota.

The complete 20 qualified samples, per-query latencies, exact SQL/configs,
result digests, source-file and binary/native/Lua hashes are in
[`results_pushdown.json`](results_pushdown.json). Source commit
`721d420` contains every hashed implementation/harness source unchanged and integrates main at `b461e5b`.
The environment receipt was inspected from the same container after the
measurement. `bench/pushdown.py` reproduces the fixture and alternating run.

## Observed work

Per query, median over the five samples:

| Shape | Ordinary storage response bytes | Pushed response bytes | Ordinary data RPCs | Pushed data RPCs |
|---|---:|---:|---:|---:|
| SUM(n), MIN(n), MAX(n), SUM(u), COUNT(n) | 12,551,953 | 208 | 5 | 2 reductions |
| tenant, id, n projection | 12,551,951 | 327 | 5 | 4 projected pages |

The byte counts include IPROTO framing and the window's control traffic.
Both variants additionally issue two read-validation RPCs per query; small
background traffic makes the total request counts slightly nonintegral
when divided by the query count. The ordinary five data calls comprise
two native first-page attempts and three Lua SELECT calls, including the
native path's normal handback behavior. The enabled path's counters show
exactly two reduction calls or four projected-page calls per query and no
ordinary SELECT calls. No already-existing COUNT or ordered-page path is
counted as a new optimization.

## CPU and timing observations

Values are median (minimum–maximum) of per-sample means, milliseconds/query:

| Shape / variant | Gateway CPU | Storage CPU | Client elapsed |
|---|---:|---:|---:|
| Reduction / ordinary | 3.33 (3.33–3.33) | 9.00 (6.67–10.33) | 15.77 (11.66–17.03) |
| Reduction / pushed | 0.00 (0.00–0.33) | 7.00 (6.67–7.67) | 5.83 (5.63–6.55) |
| Projection / ordinary | 3.33 (3.00–3.67) | 8.33 (6.67–10.67) | 12.99 (9.50–22.59) |
| Projection / pushed | 0.00 (0.00–0.67) | 7.33 (4.33–11.67) | 6.08 (4.20–9.61) |

CPU comes from Linux process counters, with 100 Hz ticks; a zero means
below the sampling resolution, not zero work. Storage CPU ranges overlap.
Elapsed time includes the counting proxy, which adds copying/framing work
especially to the larger ordinary replies. These short, shared-host,
instrumented samples do not establish an uninstrumented latency ratio,
sustained throughput, tail latency, RF3/durable OLTP or disk-bound advantage.
They establish the removed transfer/RPC work for this matched fixture and
report the associated CPU/timing observations with their limits.

## Discovery control

[`results_pushdown_initial.json`](results_pushdown_initial.json) retains
all 20 initial samples (15 queries/sample) from the uncommitted discovery
snapshot, with its file/binary hashes. Numeric predicates inherited the
connection's fallback text collation, so neither variant used the new path:
both transferred the same full tuples. Their timing drift is not evidence
of optimization. The corrected type-aware gate ignores text collation for
integer comparisons, retains complete fallback for case-insensitive text,
and has both a unit regression and public RPC/counter qualification.

Two earlier active studies are also retained in
[`results_pushdown_first_qualified.json`](results_pushdown_first_qualified.json)
and [`results_pushdown_rc_refinement.json`](results_pushdown_rc_refinement.json).
Each contains all 20 qualified samples and its exact source commit/hashes.
They preceded the RC eligibility refinement and main integration, respectively;
the table above uses only the final integrated study. Timing samples from
different builds are not pooled.

## Correctness boundary

Eligible scans retain original page coverage, even when no row qualifies;
partials exclude stored versions of buffered writes and add the own-write
overlay in Rust. Small multi-shard READ COMMITTED statements retain ordinary reads;
reductions may run inside the validated statement snapshot when paging
requires one. Unsupported SQL and absent optional procedures use
complete ordinary execution. An explicitly incomplete fold discards all
partials; malformed replies fail. The existing lock-history, DDL/routing
and MVCC-validation contracts remain prerequisites. See
[verification](../docs/verification.md#typed-range-reads-issue-135) for the
bounded model, sensitivity controls and actual SQL/storage tests.

## Live validation and unresolved variability

The [validation receipt](pushdown-validation/summary.json) records the measured
`721d420` checkpoint and its test results: 262 Rust tests; 1,073 differential cases,
47 exact pushdown controls, 27 transaction tests and 30 READ COMMITTED tests
in each of native memtx, Lua-only memtx and uncached Vinyl; 1,000 randomized
queries; and six lost/invalid-reply mutation faults. The complete native/Lua
storage suites used byte-identical Lua and native modules. RF3 memtx passed.

RF3 Vinyl was variable on this shared three-CPU container. An initial short
run missed bucket-move progress. A later 45-second run suffered prolonged
leader unavailability and the moving-bank finalization failed; its
[full output is retained](pushdown-validation/integrated-failover-vinyl.log).
The subsequent [unmodified-main control](pushdown-validation/main-control-vinyl.log)
and [candidate repeat](pushdown-validation/candidate-vinyl-repeat.log) both
passed the same 45-second-interval test, including all bank/counter/ordered
read, moving-bucket, lock-copy and direct native totals. The task's idle
MariaDB was stopped before both of those runs; runtime CPU/memory limits
were unchanged. The control's release build used one LLVM thread after a
VM-memory kill. The exact cause of the earlier failures was not isolated;
the passing repetitions do not erase them or establish an availability or
RF3 performance guarantee.

## Later main integration

After this study, the branch integrated main `700ce887`, including routing
diagnostics, RPC admission/deadlines, decision placement and recovery
fairness. The typed-read kernel keeps its existing read contract and now
feeds the new query observations. The performance tables remain evidence
for `721d420`; they are not a measurement of the later integrated build.
The [merged-code receipt](pushdown-validation/latest-main.json) records
validation at `56d9ed8`: 285 workspace/IPROTO tests, the focused storage
controls, and all three storage modes passing 1,106 differential cases,
47 pushdown comparisons, routing diagnostics, 27 transaction tests and
30 READ COMMITTED tests each. All six lost/invalid-reply mutation faults
pass. The typed-read positive and five negative models, existing local
read validation, grouped snapshots and DDL checks have expected outcomes.

This matrix uses a debug gateway because optimized compilation exceeded
the shared VM memory limit. Its first default-stack run aborted on a plain
`SET @variable` with a worker stack overflow; the isolated statement and
complete matrix pass with `RUST_MIN_STACK=16777216`. The
[failed debug trial](pushdown-validation/latest-debug-default-stack.log)
is retained. These results do not establish release/default-stack
behavior; release CI runs separately. Performance and RF3 evidence remain
pinned to the earlier checkpoint.

## Expanded release study

`typed-read-study.yml` builds the normal release profile, then checks the
focused SQL/overlay/fallback histories against pinned MySQL 8.4.13 on RF3
memtx and Vinyl with native procedures enabled and disabled. MariaDB and
the complete existing fault/model checks remain in ordinary CI. This
workflow is additional validation; it does not make the older measurements
evidence for the newly integrated source.

`pushdown_matrix.py` uses 1,024 rows with 256 B, 4 KiB or 64 KiB payloads
(other columns and tuple framing add to those widths). Each width/engine
has five alternating off/on pairs per shape: narrow/full projection and
integer reduction at 1/128 and 127/128 selectivity, a tenant/index-range
reduction, and a grouped-aggregate fallback control. Each sample contains
ten queries. Full projection and grouped aggregation must remain fallback;
both enabled optimization paths must be witnessed by production counters.

Both gateways share the same source-bound binary, RF3 storage, fsync,
SERIALIZABLE isolation and fixture. The instrumented phase counts actual
IPROTO frames/bytes across all replicas and records Linux gateway/storage
CPU. A separate direct phase removes counting proxies from the timing
comparison. Returned rows and value bytes are separate from encoded IPROTO
bytes; value bytes exclude MySQL protocol framing. All result digests must
match the ordinary executor on every iteration. Runtime receipts record
actual engine, durability, leadership and native mode. Five raw paired
ratios, their median and complete range retain regressions; the summary
does not infer statistical confidence or a general performance advantage.

The fixture fits in memory and imposes no native I/O pressure, even with
Vinyl cache disabled. This is shared-runner, single-client evidence, not
a disk-bound or throughput-capacity claim. CPU zeros can fall below the
100 Hz counter resolution. Partial runs retain their original error and
completed samples. The existing tables above remain pinned to their
earlier source.

## Expanded release results

After integrating the bounded-query executor, the initial current-main
study [41051026290](https://github.com/cstheorist/tarn/actions/runs/41051026290)
refused a wide-row setup INSERT with 1041: its conservative SQL allocation
charge was 125,902,336 bytes against the default 64 MiB query envelope.
This failed study is retained separately; its partial samples are not a
complete comparison. The study now declares identical finite 2 GiB query
and 4 GiB gateway envelopes for both lanes at every width and phase. These
also admit the full 64 MiB result and its conservative decoded-row charges.
The caps are included in every configuration/receipt; production defaults,
transaction limits, wire bounds, and admission-refusal behavior are unchanged.
These elevated matched caps are a benchmark condition, not evidence that
the full-projection control fits the default production budget.

[Run 41052066119](https://github.com/cstheorist/tarn/actions/runs/41052066119)
then completes all four correctness modes and all 960 paired samples
(9,600 queries) with these identical caps, at compiled merge source
`7a7a106f3dc7967d71a53eea07bd864ca1156cb8` (head `162a2bb`). Independent
checks confirm every expected pair/phase, exact result digest and path
counter. The [retained archive and receipt](pushdown-validation/run-41052066119.json)
keep every configuration, comparison, regression and source/binary hash.
Direct 64 KiB narrow-projection median elapsed ratios are 0.128 on memtx
and 0.336 on Vinyl; 256 B high-selectivity reductions regress to 2.511
and 1.899, respectively. Pushdown stays default-off. The associated
ordinary CI was superseded by subsequent integrity/warning review commits;
this successful source-bound study does not qualify those later commits
or establish a complete release pass.

[Run 40935962215](https://github.com/cstheorist/tarn/actions/runs/40935962215)
passes all four RF3/fsync correctness configurations: 47 exact MySQL
comparisons plus overlay, missing/malformed-procedure and OCC controls
in each. The paired study completes 960 samples (9,600 queries) across
both engines, all three payload sizes and both measurement phases.
[Ordinary CI](https://github.com/cstheorist/tarn/actions/runs/40935962457)
also passes its full MariaDB/storage/SQL/live-fault, TLA+ and Verus gates.
Actual compiled merge source is `fe1180b8ea13e2778e79e82832d995289fe2dd6f`,
from PR head `9e26213`. All raw samples, configuration/runtime receipts,
source/binary hashes and correctness outputs are
[retained here](pushdown-rf3/40935962215/index.json); the
[complete summary](pushdown-rf3/40935962215/summary.csv) includes every
query shape, bytes and gateway/storage CPU. Samples from different
widths, engines or phases are not pooled.

Direct elapsed ratios (enabled/ordinary), median and full range of the
five paired means; lower is faster:

| Engine | Payload | Low-selectivity narrow projection | High-selectivity integer reduction |
|---|---:|---:|---:|
| memtx | 256 B | 1.18 (1.10–1.43) | 2.77 (2.43–3.02) |
| Vinyl | 256 B | 1.07 (0.23–1.11) | 2.48 (1.89–3.15) |
| memtx | 4 KiB | 0.69 (0.60–0.84) | 1.48 (1.42–1.89) |
| Vinyl | 4 KiB | 0.56 (0.50–0.59) | 1.57 (1.33–1.64) |
| memtx | 64 KiB | 0.19 (0.15–0.21) | 0.20 (0.17–0.22) |
| Vinyl | 64 KiB | 0.36 (0.33–0.42) | 0.52 (0.44–0.58) |

Every paired full-table projection/reduction at 64 KiB is faster in this
study. This does not extend to every range: one Vinyl 64 KiB range-reduction
pair regresses (range 0.64–1.15). Both engines consistently regress on the
256 B/4 KiB range reductions and high-selectivity reductions. Low-selectivity
256 B reductions and several other small-row cases are mixed. Grouped/full
projection controls remain ordinary execution and also show runner noise.
These observations motivate making the new paths **opt in**, defaulting
`query_pushdown` to `false`. There is no selectivity/row-width cost model
yet; enabling this gateway-wide switch can hurt narrow queries.

The work reduction is real but insufficient alone to establish a timing
win. At 64 KiB, the low-selectivity memtx reduction transfers about
67,134,284 IPROTO response bytes/query versus 297 enabled. Median measured
gateway/storage CPU changes from 31/40 ms to below-resolution/4 ms per
query. The matching Vinyl figures are 67,134,251 versus 301 bytes and
23/72 ms versus below-resolution/19 ms. Counts include the measured
window's control traffic. At 256 B, high-selectivity reduction still
removes almost all response bytes, yet increases storage CPU from 4 to
8 ms (memtx) or 4 to 11 ms (Vinyl) and is slower end to end.

These are ten-query samples on a shared CI host. The full paired ranges
describe observed variability, not statistical confidence intervals.
CPU uses Linux ticks; zero is below resolution. Direct timing excludes
the counting proxies and result hashing, but includes the SQL client
receiving/decoding rows. No saturation, throughput-capacity, disk-bound or
multi-host advantage is established. The later default-off change alters
rollout policy; these measurements remain evidence for the explicit off/on
paths at the recorded source, with new-head validation kept separate.

The resource-integrated release study uses the same explicit 2 GiB query
and 4 GiB gateway memory policy on both paths. Other resource settings are
inherited unchanged from the fixture configuration; the study does not add
query-work or gateway-work overrides. The complete 64 KiB × 1024-row ordinary control exceeds the default
64 MiB logical envelope; this isolated study does not raise production defaults
or omit/refuse that control. Each retained configuration records these limits.
Earlier samples keep their original source and policy; do not combine them
with this integration as a single before/after measurement.
