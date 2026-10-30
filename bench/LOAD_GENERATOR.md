# Scheduled-arrival SQL load generator

`loadgen.py` addresses #140. Physical-host measurements are tracked separately
in [#211](https://github.com/cstheorist/tarn/issues/211). A local functional run
or a positive microbenchmark may support a correctly scoped implementation;
it does not establish physical scale-out, PB capacity or comparative leadership.

The generator supports exact point reads and transactions that increment
one to eight distinct keys and insert a per-key transaction ledger. Payloads
range from one byte to 64 KiB. Key selection is seeded uniform or a declared
hot prefix. These are SQL workloads; keys per transaction do not imply a
particular number of participating shards. There are no raw/eventual reads,
joins, Zipf distribution, global-index or lazy-index workloads in this version.
Those broader physical cohorts remain specified in #211.

## Prepare an isolated fixture

Install Python 3.9+ and PyMySQL 1.1.2 on the generator host. The benchmark
account needs permission to create a fresh database and its tables, then
SELECT/INSERT/UPDATE on that database. Existing databases are never dropped,
truncated or reused by `prepare`. A namespace must start with `tarn_load_`.

```sh
python3 -m pip install PyMySQL==1.1.2
python3 bench/loadgen.py prepare --endpoint 127.0.0.1:4000 \
  --database tarn_load_trial01 --engine memtx --rows 128 --row-bytes 256 \
  --out /tmp/load-prepare-01
python3 bench/loadgen.py run --endpoint 127.0.0.1:4000 \
  --database tarn_load_trial01 --engine memtx --rows 128 --row-bytes 256 \
  --keys-per-transaction 2 --isolation SERIALIZABLE \
  --rate 100 --workers 8 --queue 64 --warmup 3 --seconds 15 \
  --p99-ms 100 --max-error-rate .001 --out /tmp/load-run-01
python3 bench/loadgen.py audit --database tarn_load_trial01 --out /tmp/load-run-01
```

Use `--engine vinyl` for Tarn Vinyl or `--engine InnoDB` for the MySQL-protocol
reference. Use a new fixture for each independent trial. A successfully audited
fixture can also be reused: the next transaction run records its exact initial
counters. Point reads require zero counters, so run them before mutations or
prepare a fresh namespace. Each read verifies the key, zero counter and full
payload. Transaction readback checks every payload and exact per-key delta.

For a remote target, supply `--endpoint host:port --user benchmark`, set the
password through `TARN_BENCH_PASSWORD` (or name another variable using
`--password-env`), and use `--tls-ca /path/to/ca.pem` where the service supports
TLS. Certificate and hostname verification are enabled together. Passwords
are not command arguments or receipt fields. The same CLI can run on a separate
load host; it does not provision machines or change target configuration.

`--target-receipt deployment.json` attaches at most 64 KiB of declared,
nonsensitive deployment metadata and its hash. Include source/build hashes,
hardware, resources, topology, replication/quorum, WAL, isolation, TLS and
native/fallback settings. This is explicitly **a declaration**, not runtime
verification. The generator records the SQL server version but cannot infer
physical independence, durability or native eligibility from it. Attach
independently collected runtime/deployment evidence for those claims. Never
put credentials in the metadata file.

## Timing and accounting

Arrivals have fixed offsets `(token - 1) / rate`, independent of completions.
Workers connect and set the requested isolation before the clock starts. The
scheduler never blocks on the work queue: a full queue produces a `queue_full`
record for that arrival. Late scheduling and queue wait remain part of latency.
There is one final record per planned slot, including warmups, rejections and
cancellations; gaps or duplicate identities fail the audit.

Warmup cohorts are identified by scheduled time and excluded from measurement
summaries. Acknowledged latency runs from the **scheduled arrival** to completion;
service latency starts when a worker takes the slot. Both p50/p99/p99.9/max are
retained. Refusals and generator rejections count against the error objective.
Verified goodput includes any time needed to drain measured work beyond the
planned window. Goodput within the declared latency SLO is also reported.
This avoids inflating goodput by completing an accumulated queue after timing.

There is no retry. Only recognized 1213/40001 or 1205/HY000/40001 refusals with
a confirmed ROLLBACK are counted as refused. Wire SQLSTATE is retained; 40003,
lost replies, unknown errors or failed rollback remain unknown. An unknown
stops new admission; already running calls may settle and are all accounted.
No speculative rollback or repeat follows a transport-uncertain mutation.

The run owns a fixture metadata reservation. It is released only after settled
workers and successful exact accounting. An uncertain or interrupted run leaves
the fixture reserved and keeps its raw evidence; prepare a new namespace for
further tests. Do not clear that reservation to conceal an unknown outcome.
Readback must not compete with independent external writers to this fixture.

Exit status is 0 for a complete run meeting its declared objectives, 2 for a
complete/effect-correct run missing those objectives, and nonzero for incomplete
execution or invalid evidence. A successful later run never replaces a failure.
The offline auditor checks raw-file hashes, complete scheduled coverage, exact
ledger identities, per-key effects and recomputed quantiles. It cannot relabel
an incomplete collector run as complete.

## Resource and evidence boundaries

Workers and the queue are capped explicitly (defaults: 8 and 64). Evidence is
streamed to per-worker JSONL files. Readback uses 128-row keyset pages; the audit
uses an 8 MiB SQLite page cache and disk-backed tables instead of an in-memory
attempt list. Defaults cap planned operations at one million, raw evidence at
1 GiB and the audit database at 1 GiB. Exceeding a limit fails visibly. SQLite
temporary sort/journal files and database/driver overhead are additional disk
and memory costs; use an isolated filesystem quota for a strict total disk cap.
An interrupted generator cannot promise durable accounting for its final
in-flight calls; its incomplete receipt and missing slots prohibit qualification.

RPC timeouts and a finite drain deadline bound waiting. If workers remain alive
at the deadline, the journal is sealed and no final readback is claimed. The
target fixture remains reserved. The generator reports whole-command CPU and
peak process RSS, including setup/readback/audit; these are not storage CPU or
timed-phase resource measurements. Generator-side rejections and scheduling
delay expose client saturation. Increase client capacity only in a separately
declared run, never by silently removing slow observations.

For a comparison, predeclare target versions, exact settings, workload/seed,
offered rates, latency/error objectives and resource budgets. Run at least five
independent pairs, alternating control/candidate order, using fresh fixtures.
Keep ordinary timing separate from profiling and retain every raw run, failure,
receipt and hardware record. Do not compare unmatched isolation, native/raw
semantics, durability or admission settings. Tarn SERIALIZABLE is strictly
serializable; a common SQL spelling does not prove identical vendor contracts.

For a one-hour physical run, raise `--max-operations` and evidence limits
explicitly to cover warmup plus scheduled arrivals and provision disk capacity.
Actual independent hosts, beyond-RAM data, long retention/compaction cycles,
fixed-load scale curves, failure-domain drills and competitor results remain
acceptance work in #211. No extrapolated capacity claim closes that issue.

## Validation

```sh
python3 bench/loadgen_test.py
ENGINE=memtx bash bench/loadgen_smoke.sh /tmp/load-smoke-memtx
ENGINE=vinyl TARN_NATIVE=0 bash bench/loadgen_smoke.sh /tmp/load-smoke-vinyl-lua
```

The smoke runner owns a fresh two-shard RF3/fsync cluster, checks runtime engine,
quorum and native mode, exercises point/multiple-key/hot-key transactions, and
requires visible queue rejection under excessive offered load. Its deliberately
loose ordinary latency limits test functionality, not performance. All output,
database state and logs remain in the supplied evidence directory. Existing
ports cause refusal rather than stopping another service. Deterministic controls
cover wrong-key/equal-sum effects, refused/missing/duplicate ledger entries,
missing arrivals, rebased timing, unknown replies, SQLSTATE, drain accounting
and bounded journals.
