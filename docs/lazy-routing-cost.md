# Composite lazy routing cost study

This study accompanies [the routing contract](lazy-indexes.md) and issue
[#185](https://github.com/cstheorist/tarn/issues/185). It compares
`COMMENT 'tarn:lazy=1'` with `COMMENT 'tarn:lazy=2'` over the same
`(k,lane,n,id)` index. Both arms execute the same gateway and native storage
binaries. The comparison therefore isolates the routing configuration;
it does not attribute the new reader's common improvements to composite
routing.

## Fixture and interpretation

The local fully bound fixture has four shards with three real replicas
each, synchronous quorum two, and `wal_mode=fsync`. One Docker host supplies
12 CPU cores of quota and 6 GiB of memory. Each storage instance has
96 MiB MemTX, 16 MiB Vinyl memory and zero Vinyl cache; user tables use
MemTX. The gateway has four worker threads and an explicit 1 GiB server
admission pool. Thirty-two client threads run in four processes. The
normal 512 MiB gateway pool was insufficient for that client count in a
pilot: both arms returned an explicit resource error. That pilot is
retained and is not counted as a successful comparison.

The local optimized build disables LTO, strips debug information and uses
64 codegen units, after the standard release dependency compilation was
killed with SIGKILL in the bounded-memory local environment. This is not a
measurement of the standard release profile. CI uses the normal release profile
and a separate host configuration. Comparisons are within each fixture;
results from different configurations are never pooled.

The final isolated-source matrix (`841aee2`, including main's MySQL contract
changes) has 60 successful arms with zero workload errors, each below the
fixed p99 ceiling. Results are medians across five arms per
configuration; the paired interval is calculated separately and is not a
ratio of those medians.

| Shape | Lanes | Baseline / composite ops/s (median) | Paired ratio (95% interval) | Baseline / composite p99 ms (median) |
|---|---:|---:|---:|---:|
| uniform | 1 | 14,514 / 14,551 | 1.009 (0.953–1.068) | 17.80 / 17.81 |
| uniform | 4 | 13,467 / 13,546 | 1.003 (0.967–1.042) | 17.85 / 18.14 |
| uniform | 16 | 14,632 / 14,495 | 1.002 (0.960–1.047) | 17.69 / 17.66 |
| skewed | 1 | 11,240 / 11,407 | 1.010 (0.986–1.036) | 17.82 / 17.91 |
| skewed | 4 | 11,173 / 15,097 | 1.355 (1.325–1.385) | 17.99 / 12.19 |
| skewed | 16 | 15,982 / 16,084 | 1.036 (0.969–1.108) | 14.23 / 12.02 |

The skewed four-lane case improved in every pair, with its largest target's
median index-read share falling from 96.5% to 26.2%. The sixteen-lane case
also distributed reads (96.4% to 25.2%), but its throughput interval includes
one, as do the uniform and single-lane controls. Only the four-lane case
supports improvement beyond measured uncertainty in this final matrix.

The first sixteen-lane attempt stopped after three complete pairs when a
leader statistics call timed out amid replica leadership churn. A fresh
fixture then refused CREATE DATABASE because clock safety was unavailable,
before any timed arm. Neither attempt qualifies. The table's sixteen-lane
case is a new complete five-pair run after all four leaders reported healthy
clock safety; it does not splice together partial runs. The root cause of
the first interruption was not established. No timeout or clock guard was
relaxed. Full node logs and failed attempts are retained.

The earlier `d233805` matrix, also retained, found 1.296 (1.208–1.390) at four
lanes and 1.345 (1.288–1.405) at sixteen. The later sixteen-lane result does
not reproduce that advantage. These separate runs are not pooled; routing
distribution alone is insufficient to predict throughput on a shared host.

The improvement consumes more storage CPU per completed operation:
0.405 to 0.436 ms at four lanes and 0.362 to 0.401 ms at sixteen. Median
process physical writes increased from 720 to 749 bytes/op and 716 to
741 bytes/op respectively. Gateway RPCs remained about 2.04/op. Encoded
apply traffic was about 5 bytes/op, amortized over the read-heavy mix.
Median observed maximum source lag was 91.7 to 84.8 ms at four lanes and
39.1 to 86.5 ms at sixteen; median outage catch-up stayed near one second,
including control overhead. Full per-arm counters and extrema are in the
[machine-readable summary](../tests/evidence/lazy-routing/summary-isolated-final.json).

Every case uses five paired ten-second arms in alternating AB/BA order.
The fixed latency ceiling is 250 ms at p99; any workload error or an arm
above that ceiling gives zero qualifying goodput. The fully bound fixture
writes an eight-row batch on its first operation and every 100th operation.
Each write includes commit, session-frontier catch-up, the lazy lookup and
a current base-row recheck. Other operations repeat a fully bound lookup
of the client's most recently written row. There are no prefix-only reads
in this fixture. Thus this is approximately 99% reads, not a write-heavy
capacity claim. Payloads are 64 bytes per row; the working set fits memory.

The uniform fixture selects `k` uniformly from 128 values. The skewed
fixture selects `k=7` with 95% probability and otherwise uses that same
uniform distribution. Both fixtures use fixed per-client random seeds.
The application stores explicit lane values. The harness searches for
lane values that balance the known hot key over the four index owners:
one owner for one lane, one lane per owner for four, and four per owner
for sixteen. The single lane uses the legacy hot owner's shard. Base
tenants independently cover the base/target owner cross-product. This
declares the distribution being tested; it is not evidence that arbitrary
lane values always balance a real workload.

## Accounting and failure controls

The harness retains all operation latencies, errors and missed scheduled
slots. Raw latencies are streamed to hashed compressed sidecars after
measurement, so earlier arms do not accumulate in the load generator's
heap. The first full attempt was killed during raw-sample retention;
its incomplete record remains separate from qualification.

Measurements include gateway RPC and wire-byte counters; source capture,
target apply, frontier, index-select and base-fetch calls and elapsed time;
encoded apply assignment bytes; all twelve storage processes, gateway and
client CPU; and their `/proc` physical and logical I/O counters. These are
end-to-end costs with capture and shipping enabled, not index-read-only
timings. Stage elapsed times can overlap and must not be added as CPU time.
Leader IPROTO counters do not represent every replica's network traffic.
Process physical I/O counters depend on the host and filesystem and are
not device-level write-amplification measurements.

Each source samples queue depth, bytes and oldest age every 50 ms. The
summary reports maximum source lag and the sum of per-source peaks; that
sum is an upper bound, not a simultaneous cluster peak. Per-target apply
assignments and index reads expose whether load was actually distributed.
Durable metadata sizes are retained after the timed workload drains.

After every arm, the harness makes every destination refuse lazy apply,
commits a new row, observes its retained outbox entry, restores apply,
waits for drain and verifies the exact row. Catch-up timing includes the
control RPCs. This measures a controlled destination refusal; independent
leader-death and restart histories provide the separate RF3 fault evidence.
There is no replay of an uncertain mutation.

The summary reports the geometric mean of the five paired goodput ratios
and a two-sided 95% Student-t interval on log ratios (four degrees of
freedom). Five pairs give limited precision. An interval above one supports
improvement only for this declared finite fixture. It establishes neither
independent-host durability nor production capacity or behavior beyond
RAM. Exploratory probes guided the fully bound fixture and are retained
with their regressions, resource failures and different settings.

## Reproduction

Use `tests/sql/lazy_routing_cost.py` on an exclusively owned cluster. The
local qualification's command and exact configuration, source/binary
hashes, raw samples and logs are retained with the evidence. To regenerate
the summaries after extracting an archive, run:

```sh
python3 bench/summarize_lazy_routing.py extracted/*.json --output summary.json
```

The summary refuses to qualify missing pairs, incomplete arms, workload
errors, failed latency ceilings or mismatched raw-sample hashes. Keep
report JSON files beside their `.samples` directories.

The `Composite lazy routing costs` CI workflow separately exercises
write-heavy, read-heavy and scheduled low-load cases with prefix-only
fanout reads. Its retained artifacts describe the normal release build
and runner configuration. The low-load schedule is 50 complete operations
per second; missed slots are retained rather than retried or concealed.

## Prefix fanout and whole-host limits

The first completed standard-release CI case used a four-CPU x86_64 runner,
sixteen client threads in two processes, and the default 512 MiB gateway
pool. It tested the skewed four-lane case with five pairs for each workload.
These are separate comparisons from the twelve-CPU fully bound study.

| Workload | Write frequency / prefix frequency | Paired throughput ratio (95% interval) | Median p99 ms, baseline / composite |
|---|---|---:|---:|
| Write-heavy | every operation / every 10th | 0.881 (0.862–0.900) | 62.25 / 68.05 |
| Read-heavy | every 10th / every 20th | 0.944 (0.896–0.995) | 38.52 / 46.96 |
| Scheduled low-load | every 10th / every 20th | 1.000 (approximately 1.000–1.000) | 19.79 / 18.06 |

Composite routing distributed reads in these cases too, but did not improve
saturated goodput on this resource envelope. Write-heavy RPCs increased
from 6.32 to 6.95/op, storage CPU from 5.76 to 6.71 ms/op, and physical
process writes from 53.8 to 66.7 kB/op. Read-heavy RPCs increased from 2.53
to 2.70/op and storage CPU from 1.03 to 1.17 ms/op. These measured regressions
are consistent with fragmented shipping and extra prefix-page reads; the
experiment does not isolate each component's causal contribution.

Low-load goodput remained at the offered schedule. Its storage CPU rose
from 4.56 to 5.02 ms/op, including fixed background and sampling costs.
The latency medians alone do not establish a repeatable latency advantage.
These results favor explicit opt-in routing and workload-specific sizing,
not a universal recommendation to add lanes. The
[retained CI summary](../tests/evidence/lazy-routing/summary-ci-41073441278.json)
includes all per-arm costs and exact settings.

A later standard-release run on source `841aee2` completed the uniform and
skewed single-lane cases before an external branch update cancelled its
remaining jobs. Uniform write-heavy work is **unqualified**: eight of ten
arms exceeded 250 ms p99 (including all five composite arms), despite zero
workload errors. No positive throughput ratio is assigned to that case.
Uniform read-heavy was 0.963 (0.875–1.060); skewed write-heavy was 1.015
(0.990–1.040), and skewed read-heavy was 0.962 (0.911–1.016). Scheduled
low-load ratios stayed near one. These results and the cancellation record
remain separate from the local fully bound comparisons.
