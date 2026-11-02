# Direct guard qualification evidence

Run `41532314628`, source `b1e7ce88358e15437fdf82d10b2a0e961513ee0d`.
Four single-host direct-driver modes, memtx/vinyl × native disabled/enabled,
RF3/fsync, effective checks false. Five alternating control/candidate pairs
per mode; three 15-second measured shapes plus 3-second warmups.
Compare candidate/control within this direct driver. Direct-versus-SQL
measurements are not a controlled measurement of gateway overhead.

All 240 samples / 1,397,326 attempts including warmups reconciled exactly;
120 measured samples / 1,164,524 attempts. All acknowledged; zero refused or
unknown outcomes. Audit checked raw hashes, exact token/key ledgers, eight-key
counters, quantiles, allocation/waits, TX/process CPU deltas, source/binary
receipts, and the exact intended two-file guard transformation. Complete
111-file source copies per arm are present. Sampled PID/role identity is
stable; 240 archived node logs contain one startup/PID and only recorded term
2. This is archived-log evidence, not continuous election telemetry. Native
census is after-minus-before for every sample: all individual native counter
deltas are zero, including warmups. Configuring native support does not
establish measured native service.

All 4,191,978 journal events form exactly start -> commit -> finish for their
1,397,326 attempts. Tokens and txids are unique within samples; per-worker
sequences are contiguous, exact config run/number/worker/sequence identities
match, no start is unmatched and no commit dispatch is repeated. Each
successful terminal result records exactly three calls and ['ok',2]. Driver
admitted/finished/CALL totals, attempts and terminal latencies match; admission
times lie inside configured admission windows, and inferred terminal times
fit the elapsed summary. Worker states finish disconnected.

Real tuple format/field maps, primary parts, engine/sync flags, empty ledger,
reset eight-row fixture, owning leader/bucket and catalog versions match the
saved configuration. Fence arrays are exactly the sorted sid/version pairs.
The SHA-bound driver reads the first lock generation, checks the second lock
generation equals it, and sends that generation plus saved fence to commit.
The journals do not include either individual lock reply or generation value.
Thus generation evidence consists of source plus successful runtime assertions,
not an independently inspectable per-attempt generation transcript. The
commit event precedes the source's CALL, but journals are not server-side RPC
traces. Evidence of no replay consists of unique journal identities, single
dispatch markers, exact three-CALL totals and checked driver source.

All retained node-log W/E/F entries fall outside sampled before/after diagnostic
brackets. No slow-WAL entry is observed inside these measured brackets.
This does not establish absence of uninstrumented stalls. Fresh process/JIT
per lane, short closed-loop samples and background CPU/allocation work remain
limits; no full concurrent/fault or engine/WAL CPU attribution is claimed.

Four archives retain comparison/session JSON, all raw attempt and event JSONL,
driver configs/summaries/stdout/stderr, mechanism/startup logs, runtime/build
receipts, and fixture source/log/config/params/startup files. Fixtures are
expanded into `fixtures/`. Bulk database files and executable binaries are
omitted and individually listed in the manifest. Every retained archive/member
was reopened and checked against manifest bytes/SHA256. Additional measured
harness/driver/patcher source copies are bound in the manifest.

`audit.py`, `log_context.py` and their results/logs retain original absolute
paths and require `/private/tmp/tarn-186-qualification/run-41532314628/`, its
original nested fixture archives and shared downloaded binaries, plus measured
Git source in `/private/tmp/tarn-186-20261102`. These compact archives alone
cannot rerun those scripts; executable contents are omitted. `results.json`
contains full independent paired medians/ranges; no observations were discarded.
