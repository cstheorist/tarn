# Typed range read qualification: run 41729770781

[CI run](https://github.com/cstheorist/tarn/actions/runs/41729770781) ·
[PR #291](https://github.com/cstheorist/tarn/pull/291) · issue #135 / #126.
This records one completed run; it makes no policy/default change.

## Retained evidence and reproduction

The four original result ZIPs (330,607 bytes total) retain all six raw
measurement JSON files, all four full wire captures, inputs, configuration,
correctness logs, build hash receipts and cluster/measurement logs.
`source-receipts.tar.gz` retains the original GitHub run/artifact/PR/commit
receipts, original download digest manifest, build text receipts and exact
workflow, harness, proxy, probes and Cargo.lock source. No server/native
binary or build ZIP is retained (the build ZIP alone was 34,442,513 bytes;
its two binaries were 140,661,240 bytes). No raw cores are retained.
`SHA256SUMS` covers the retained files. From this directory:

```sh
shasum -a 256 -c SHA256SUMS
python3 verify.py > /tmp/typed-read-41729770781.csv
```

The standard-library verifier checks result ZIPs against GitHub artifact
SHA256 receipts, every retained original member/source against the original
download manifest, source identities, passing wire checks, path counters,
result digests and runtime premises. It emits all 96 phase/engine/width/shape
cells with median/min/max direct and instrumented ratios, process CPU and
instrumented bytes/RPC per query. Raw samples include all five pairs and ten
latencies per block, SQL shapes, fixture, warmups and path counters. CSV
units are seconds for CPU, bytes for transfer and calls for requests/responses.

Run head is `06a9e5aea71ecbf915732876091ce96e242fc351`; build/measurement
source is merge commit `abac747590fef82b99c8273e76fd37c6fe49f987`.
Both have tree `30857f60092b6a67dc54562785acda4b8cfbf45b`.
The original analysis checked the downloaded server/native bytes against
all receipts: respectively
`a18493899bc07b03cd495a919b1e0733bebfb61d6ec03556dcb2baeb907cfacc` and
`1ac18f31dc0b2a9ccee9efcf601c99d8d99a520437fcb63a3cd3ec79771cd0ae`.
The retained verifier checks receipt consistency; omitted binary bytes cannot
be rehashed from this bundle. Workflow/receipt provenance and identical
source trees do not establish an independently reproduced build.

## Correctness and measurement boundary

All four memtx/Vinyl × native enabled/disabled wire lanes pass 33 cases and
230 checks each (132/920 total), complete and not observation-only, plus
47 exact SQL comparisons and overlay/page/fill/OCC controls per lane.
All 33 off/on full-wire comparisons pass per lane. This does **not** mean
full equality with both references: 30/33 equal the MariaDB wire capture,
29/33 equal MySQL. Three diagnostic cases admit no reference equivalence
(division warning and text/binary MySQL SUM refusal); `float_fallback` also
differs from MySQL. Authoritative case checks pass; raw differences remain.

Performance is native-enabled only: memtx/Vinyl, payload widths
256/4096/65536 bytes, 1024 rows, 16 tenants, two shards with RF3, fsync,
quorum 2 and synchronous user spaces. Six files contain 960 ten-query block
samples: eight shapes × five alternating pairs × two switch states × two
phases × six engine/width cells = 9600 queries, half direct and half through
the copying proxy. Three warmups per state/shape/phase are excluded.

All 72 before/after node observations report check_locks/check_yields/
test_hooks false, valid actual RF3 membership, one leader per shard and
native-ready leaders; PIDs are stable. Snapshots bracket the entire two-phase
measurement, not each query. Owned-PID checks are attested by the source and
successful harness; PID files were not uploaded. Correctness fixture storage
processes exited before fresh performance processes started.

Equal explicit query/server caps are 2/4 GiB, needed for the full 64 KiB
control; these are not production defaults. The host is shared CI and the
client is one closed-loop connection. This is no capacity, concurrency,
disk-bound, or automatic-admission claim. Width is payload-column width,
with tuple framing and other columns additional.

## Direct elapsed on/off ratios

Each cell is median [min,max] of five paired ten-query elapsed-sum ratios;
less than one favors enabled. A range crossing one is mixed, even with a
favorable median. These timings exclude the copying proxy.

### memtx

| Shape | 256 B | 4096 B | 65536 B |
|---|---:|---:|---:|
| reduce_low | 0.928 [0.905, 1.363] | 0.511 [0.417, 0.634] | 0.070 [0.064, 0.100] |
| narrow_low | 1.094 [0.975, 1.219] | 0.561 [0.493, 0.636] | 0.138 [0.134, 0.141] |
| full_low | 0.992 [0.968, 1.104] | 0.999 [0.486, 1.240] | 0.952 [0.906, 0.991] |
| reduce_high | 1.946 [1.738, 3.969] | 1.155 [1.020, 1.293] | 0.169 [0.150, 0.172] |
| narrow_high | 1.057 [0.912, 1.216] | 0.819 [0.786, 0.904] | 0.278 [0.258, 0.313] |
| full_high | 1.014 [0.992, 1.033] | 1.018 [0.942, 1.115] | 0.983 [0.970, 0.999] |
| range_reduce | 1.207 [1.088, 1.574] | 1.082 [1.023, 1.285] | 0.363 [0.345, 0.386] |
| grouped_fallback | 0.950 [0.899, 0.997] | 1.068 [0.940, 1.111] | 0.991 [0.960, 1.015] |

### vinyl

| Shape | 256 B | 4096 B | 65536 B |
|---|---:|---:|---:|
| reduce_low | 0.928 [0.513, 1.819] | 0.583 [0.504, 0.886] | 0.364 [0.241, 0.838] |
| narrow_low | 0.878 [0.718, 0.978] | 0.579 [0.352, 0.849] | 0.275 [0.242, 0.315] |
| full_low | 0.755 [0.584, 1.066] | 1.101 [0.959, 1.167] | 0.926 [0.819, 0.969] |
| reduce_high | 2.078 [1.469, 2.234] | 1.213 [1.060, 1.907] | 0.326 [0.290, 0.360] |
| narrow_high | 1.010 [0.849, 1.155] | 0.853 [0.728, 1.955] | 0.332 [0.327, 0.594] |
| full_high | 1.007 [0.990, 1.178] | 0.996 [0.881, 1.017] | 1.014 [0.977, 1.039] |
| range_reduce | 1.335 [1.139, 2.208] | 1.441 [0.966, 2.992] | 0.666 [0.529, 0.799] |
| grouped_fallback | 1.036 [0.786, 1.114] | 1.044 [0.739, 1.177] | 0.999 [0.838, 1.362] |

Across 48 direct cells, 20 are faster in every pair, seven slower in every
pair and 21 mixed; 19 have median regressions. All ten eligible 64 KiB
shape/engine cells are faster in every pair. The seven all-slower cells are
`reduce_high` at 256/4096 B in both engines and `range_reduce` at 256 B in
both engines and 4096 B memtx. Fallback controls retain ordinary execution;
their timing movement is not a typed-path gain.

## Transfer, RPC and CPU

Transfer comes only from the instrumented phase. Counters include IPROTO
protocol/control traffic, exclude TCP overhead, and do not imply direct
latency. The verifier emits all off/on byte, request and response medians
and ranges per query. For example, memtx `reduce_high` at 256 B drops median
response bytes/query from 284,560 to 324 while requests remain 6→6/query,
yet direct elapsed regresses 1.946× [1.738,3.969]. At 64 KiB, memtx
`reduce_low` drops 67,134,284→297 response bytes/query and 24.3→6 requests/query;
`narrow_low` drops 67,134,280→1,251 bytes but only 24.2→22 requests/query.
`range_reduce` retains 1→1 request/query at all widths. Savings in returned
bytes need not reduce RPCs; narrowing still pages the underlying range.

CPU is gateway user+system time and the sum over all six storage processes,
sampled from `/proc` at 100 Hz (10 ms ticks), per ten-query block. Zero means
below tick resolution. No ratios over zero are meaningful; small narrow/range
differences are weakly resolved. Background work and measurement overhead
may enter the interval. CSV retains both phases and paired on-minus-off
median/min/max CPU deltas. Direct `reduce_high` storage CPU at 256 B rises
30→80 ms in both engines (paired median delta +60 ms memtx, +50 ms Vinyl;
both ranges +30 ms or higher). At 4096 B it rises 60→90 ms memtx and
50→80 ms Vinyl. At 64 KiB, the four full-scan eligible shapes per engine
reduce gateway and storage CPU in every observed pair; the small
`range_reduce` CPU differences remain quantized. This run supports the
reported wide-payload gains and explicitly retains narrow-payload regressions;
it does not decide opt-in versus automatic gating.
