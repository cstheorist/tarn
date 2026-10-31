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
64 codegen units, after the standard release dependency compilation
exceeded the container's memory limit. This is not a measurement of the
standard release profile. CI uses the repository's normal release profile
and a separate host configuration. Comparisons are within each fixture;
results from different configurations are never pooled.

The complete local matrix has 60 successful arms with zero workload errors,
each below the fixed p99 ceiling. Results are medians across five arms per
configuration; the paired interval is calculated separately and is not a
ratio of those medians.

| Shape | Lanes | Baseline / composite ops/s (median) | Paired ratio (95% interval) | Baseline / composite p99 ms (median) |
|---|---:|---:|---:|---:|
| uniform | 1 | 15,342 / 14,564 | 0.982 (0.943–1.022) | 17.30 / 17.37 |
| uniform | 4 | 15,203 / 14,868 | 0.983 (0.927–1.042) | 16.88 / 17.39 |
| uniform | 16 | 14,632 / 14,895 | 1.025 (0.984–1.067) | 17.53 / 17.36 |
| skewed | 1 | 10,958 / 11,033 | 1.005 (0.997–1.014) | 18.10 / 18.26 |
| skewed | 4 | 10,905 / 14,546 | 1.296 (1.208–1.390) | 18.57 / 13.31 |
| skewed | 16 | 10,963 / 14,537 | 1.345 (1.288–1.405) | 18.44 / 13.30 |

The skewed four- and sixteen-lane cases improved in every pair. Their
largest target's median index-read share fell from 96.4% to 26.1%; the
assignment distribution moved similarly. The uniform and single-lane
intervals include one. These controls support the narrower conclusion
that distributed targets relieve this fully bound hot-prefix workload.

The improvement consumes more storage CPU per completed operation:
0.407 to 0.437 ms at four lanes and 0.408 to 0.439 ms at sixteen. Median
process physical writes increased from 716 to 748 bytes/op and 713 to
749 bytes/op respectively. Gateway RPCs remained about 2.04/op. Encoded
apply traffic was about 5 bytes/op, amortized over the read-heavy mix.
Median observed maximum source lag was 68.5 to 81.8 ms at four lanes and
107.5 to 60.8 ms at sixteen; median outage catch-up stayed near one second,
including control overhead. Full per-arm counters and extrema are in the
[machine-readable summary](../tests/evidence/lazy-routing/summary-bound.json).

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
