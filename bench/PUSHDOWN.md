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
