# SQL guard qualification evidence

Run `41532311462`, measured source `b1e7ce88358e15437fdf82d10b2a0e961513ee0d`.
Four single-host SQL modes: memtx/vinyl × native disabled/enabled, effective
checks false, RF3/fsync. Five alternating pairs per mode; three measured
15-second shapes and three 3-second warmups per session. All observations,
including high variance vinyl/native-enabled pairs, are retained.

Independent audit reconciled 240 samples and 1,473,690 attempts including
warmups; 120 measured samples contain 1,228,714 attempts. All acknowledged;
zero refused/unknown. Exact token identity ledgers and eight-key effects,
raw SHA256, p50/p99, allocation, waits, TX/process CPU, alternating pair/shape
order, flags/configuration and sampled PID/role identity were checked.
All 240 node logs record one PID/startup and only observed RAFT term 2;
this is archived-log evidence, not continuous term telemetry.
All 111 source files per archived arm match the source or exact two-file
experimental guard transform. Original downloaded server/native binary
hashes and source/harness/worker receipts were independently checked.

Native census uses after-minus-before counters within each sample window.
Both measured and warmup-inclusive native deltas are zero. A supplemental
per-node check found zero individual native counter deltas in all 1,440
node/sample windows, ruling out cancellation of nonzero deltas. Absolute
setup counts do not demonstrate measured native service. These SQL results
do not qualify a direct-native workload.

CPU models differ across mode jobs: memtx-disabled and vinyl-disabled use
AMD EPYC 9V45; memtx-enabled uses EPYC 7763; vinyl-enabled uses EPYC 9V74.
Vinyl/native-enabled paired goodput ratios range .6092–1.9422; p99 ratios
reach 3.7925. Seven slow-WAL log entries fall inside measured diagnostic
brackets: pair0 candidate/unrelated (.672–.676 seconds, replica nodes),
pair3 control/unrelated (.580 seconds, leader). Other W/E/F node-log lines
fall outside sampled diagnostic brackets. The retained evidence cannot
attribute the full variance to a specific cause. No pairs were discarded.
CPU intervals include background work; no WAL/engine CPU attribution or
host utilization trace is available. Cross-mode job results are not a
matched native-enabled versus disabled experiment.

Four archives contain every comparison/session JSON, raw attempt JSONL,
mechanism/startup log, host/runtime receipt, source/hash receipt, complete
control/candidate source copy and cluster logs/config/params/startup scripts.
Nested fixture archives are expanded into `fixtures/` subtrees. Bulk database
WAL/snapshot/data and executable binaries are excluded; individual omissions
are listed in the manifest. All archive members were independently reopened
and checked against their manifest bytes and SHA256 after packaging.

`results.json` contains independent descriptive medians/ranges and all raw
warning summaries; `log-context.json` records warning timing associations.
`audit.py` and `log_context.py` retain their original absolute paths. They
require the original `/private/tmp/tarn-186-qualification/run-41532311462/`
layout (including original nested fixtures and shared downloaded binaries)
and the measured Git source in `/private/tmp/tarn-186-20261102`. Compact
archives alone cannot rerun those scripts. Binary contents are omitted;
retained hash receipts do not replace hashing the original executables.
The manifest binds the successful audit code/results/log, all four archives
and every retained member by byte count and SHA256.
