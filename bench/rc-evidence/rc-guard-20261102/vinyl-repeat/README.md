# Fixed SQL vinyl/native-enabled repeat

Run `41536728436`, source `51075aa861999ea16b9a1dd63ceebd8a3222049d`.
Independent Git tree comparison to qualification source
`b1e7ce88358e15437fdf82d10b2a0e961513ee0d` finds only the workflow's
`pilot_engine` input and engine-selector change. Production and benchmark
harness files are byte-identical. The exact diff is in source-comparison.json.

One predeclared repeat, single-host SQL vinyl/native-enabled, RF3/fsync,
effective checks false. Five alternating pairs and three measured 15-second
shapes plus three 3-second warmups per lane. All observations retained.
No additional study, exclusion or rerun was performed by this audit.

All 60 samples / 428,149 warmup-inclusive attempts reconcile exactly. All
30 measured samples / 358,218 attempts acknowledged; zero refused/unknown.
Checks cover raw hashes, exact token/key ledgers and eight-key effects,
quantiles, allocation/waits, TX/process CPU, source/binary/harness receipts,
complete 111-file source copies per arm and exact two-file guard transform.
Sampled PID/role identity is stable; all 60 node logs have one PID/startup
and only recorded term 2. This is not continuous election telemetry.
Native census uses individual after-minus-before counters: every delta is
zero, including warmups. Absolute setup native calls are excluded.

Candidate/control paired ratios, median [min,max]:

| Shape | Goodput | Lua allocation/ack | TX CPU/ack | p99 |
|---|---|---|---|---|
| Uncontended | 1.00116 [.90853,1.26167] | .86640 [.85597,.87237] | 1.00052 [.91312,1.06512] | .98217 [.16915,3.51854] |
| Unrelated | 1.00963 [.90782,1.10929] | .87144 [.86678,.87639] | .96896 [.91458,1.02037] | .99750 [.90118,2.90750] |
| Hot | .97592 [.86834,1.08936] | .87717 [.87343,.88163] | .96662 [.94682,1.04052] | 1.02518 [.57511,5.14668] |

Host CPU receipt: AMD EPYC 9V45 96-Core Processor. No slow-WAL warnings
appear; all 564 W/E/F node-log entries fall outside sampled diagnostic
brackets. Retained evidence does not establish a cause for latency/goodput
variability. All goodput ranges cross 1; hot median is below 1. Background
work and short closed-loop intervals remain limits; no engine/WAL CPU
attribution, direct-native service or full fault qualification is claimed.

The archive includes every raw sample, session/comparison JSON, mechanism
and startup log, runtime/build receipt, complete fixture source copies,
node logs/config/params/startup scripts. Every retained member was reopened
and checked against manifest byte counts and SHA256. Bulk database files and
executable binaries are omitted individually in the manifest. Fixtures are
expanded into a `fixtures/` subtree.

The original audit code/results/log and packaging helper are retained and
bound to this run/source. Auditor scripts retain absolute paths and require
the original `/private/tmp/tarn-186-qualification/run-41536728436/` layout,
original nested fixtures/shared downloaded binaries, and measured Git source
in `/private/tmp/tarn-186-20261102`. This compact archive alone cannot rerun
those scripts because binary contents and the original fixture layout are
omitted. Full independent medians/ranges are in results.json; warning timing
classification is in log-context.json.
