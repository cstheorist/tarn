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

## Final runtime: single-row lookups

The final runtime is `a20867b`, including only reviewed changes already merged
into main (MySQL contracts, typed reads, outcome handling and the lazy ledger).
Its sixty successful single-row arms have zero workload errors and satisfy
the fixed p99 ceiling. No throughput interval excludes one. Earlier gains
must not be attributed to this final runtime.

| Shape | Lanes | Baseline / composite ops/s (median) | Paired ratio (95% interval) | Baseline / composite p99 ms (median) |
|---|---:|---:|---:|---:|
| uniform | 1 | 16,221 / 15,800 | 0.998 (0.931–1.069) | 17.07 / 17.56 |
| uniform | 4 | 15,000 / 14,950 | 1.013 (0.975–1.053) | 17.60 / 17.36 |
| uniform | 16 | 14,761 / 15,010 | 1.024 (0.966–1.085) | 17.60 / 17.47 |
| skewed | 1 | 10,700 / 10,878 | 1.009 (0.987–1.031) | 18.63 / 18.56 |
| skewed | 4 | 16,352 / 16,461 | 1.005 (0.966–1.045) | 12.38 / 11.79 |
| skewed | 16 | 16,661 / 16,604 | 1.006 (0.954–1.061) | 12.49 / 11.75 |

The first four cases share one fixture. Skewed four and sixteen lanes each
use a fresh complete five-pair run after an interruption. The first skewed
four-lane attempt failed in the post-workload outage verification with a
leader-change refusal. The second fixture completed four lanes, then timed
out collecting lazy statistics after the first composite sixteen-lane arm.
Both histories show replica relay timeouts and leadership changes; their
root causes remain unresolved, and the cgroup OOM counters were zero.
No clock, quorum or timeout guard was relaxed. Failed cases are unqualified;
partial runs are never spliced into the successful comparisons.

The second interruption exposed a retention bug: worker results were not
attached to the report until after the drain RPC. That failed composite
arm's raw samples are unavailable. Its failure, earlier complete arm and
full node logs remain retained. The harness now attaches received results
before post-workload RPCs, attempts every installed-hook cleanup even when
one fails, and persists raw samples in a finally block. Unit controls and a
live RF3 post-workload timeout injection verify retention and hook restoration.
The successful fresh sixteen-lane run uses this correction.

## Final runtime: eight-row lookups

After the single-row study, a separate two-pair, five-second pilot tested
`--lookup-batch`: each lookup returns and verifies all eight rows of the
client's most recent inserted batch, ordered by id. A per-worker unique
batch number prevents another client's rows from satisfying the lookup.
The pilot's four-lane ratios were 1.162 and 1.198; it is retained as
exploration and is not qualification.

The subsequent holdout fixed five ten-second pairs for all six cases,
with a fresh RF3/fsync cluster per case, the same final runtime binaries,
32 clients/four processes and the resource envelope above. Both arms use
identical queries and data generation. All sixty arms completed without
workload errors, missing raw samples or a p99-ceiling violation.

| Shape | Lanes | Baseline / composite ops/s (median) | Paired ratio (95% interval) | Baseline / composite p99 ms (median) |
|---|---:|---:|---:|---:|
| uniform | 1 | 13,670 / 13,109 | 1.018 (0.958–1.082) | 17.86 / 17.50 |
| uniform | 4 | 14,037 / 13,845 | 0.985 (0.967–1.004) | 17.33 / 17.53 |
| uniform | 16 | 14,017 / 14,127 | 0.991 (0.961–1.022) | 17.12 / 17.55 |
| skewed | 1 | 12,549 / 12,730 | 1.000 (0.926–1.081) | 16.07 / 15.77 |
| skewed | 4 | 10,022 / 11,639 | 1.161 (1.113–1.211) | 19.08 / 16.71 |
| skewed | 16 | 9,396 / 11,549 | 1.234 (1.160–1.313) | 20.27 / 15.88 |

Composite routing is faster in every skewed four- and sixteen-lane pair.
The largest target's median index-read share falls from 96.4% to 25.3–25.4%.
These cases support a throughput improvement beyond measured uncertainty
for this declared eight-row workload. Uniform and single-lane intervals
include one. Medians are descriptive; the paired interval is calculated
from the five pair ratios, not from the ratio of medians.

The gain consumes more storage CPU: 0.460 to 0.510 ms/op at four lanes and
0.469 to 0.516 ms/op at sixteen. Process physical writes rise from about
680 to 748 bytes/op. Gateway RPCs remain about 2.04/op, encoded apply bytes
about 5.1/op, and controlled outage catch-up about one second. Median
observed maximum source lag is 71 to 63 ms at four lanes and 48 to 62 ms
at sixteen. These are whole-operation measurements with capture, shipping,
apply, session catch-up and current base-row rechecks enabled.

The [batch summary](../tests/evidence/lazy-routing/summary-batch-final.json)
and [single-row summary](../tests/evidence/lazy-routing/summary-final-ledger.json)
retain every arm and cost. They are distinct experiments and are never
pooled. Selecting the batch workload after the pilot limits the claim to
that declared workload; this is not a general speedup or independent-host
replication. Earlier interruptions also prevent an availability claim.


## Earlier runtime comparisons

The retained `d233805` single-row matrix found skewed ratios of 1.296
(1.208–1.390) at four lanes and 1.345 (1.288–1.405) at sixteen. The subsequent
`841aee2` matrix found 1.355 (1.325–1.385) at four and 1.036 (0.969–1.108)
at sixteen. These source-specific results are historical; the final
single-row study above does not reproduce them. Every uniform and
single-lane interval in those earlier matrices included one.

The `841aee2` sixteen-lane case also required a fresh complete fixture after
an interrupted three-pair attempt with a statistics timeout and a separate
pre-measurement clock-safety refusal. All unsuccessful attempts and node
logs remain separate. Distributed routing alone does not predict throughput
on this shared host. The machine-readable summaries retain per-arm CPU,
RPCs, physical and encoded bytes, target shares, backlog and outage catch-up
for every historical and final case.

## Workload definition

Every case uses five paired ten-second arms in alternating AB/BA order.
The fixed latency ceiling is 250 ms at p99; any workload error or an arm
above that ceiling gives zero qualifying goodput. The fully bound fixture
writes an eight-row batch on its first operation and every 100th operation.
Each write includes commit, session-frontier catch-up, the lazy lookup and
a current base-row recheck. Other operations repeat a fully bound lookup
of the client's most recently written row (single-row study) or eight-row
batch (batch study). There are no prefix-only reads
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

Successful studies retain all operation latencies, errors and missed scheduled
slots. The failed-arm retention gap described above is explicitly excluded
from qualification. Raw latencies are streamed to hashed compressed sidecars after
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

The isolated PR's complete standard-release workflow
[`41092212161`](https://github.com/cstheorist/tarn/actions/runs/41092212161)
then completed all six cases, with five pairs for each of three workloads:
180 arms, all error-free and below the p99 ceiling. Its merge candidate
`3f6bd03` has the same tree as `841aee2`. The earlier unqualified case remains
retained; a later passing run does not erase its latency failure.

| Shape | Lanes | Write-heavy paired ratio (95% interval) | Read-heavy paired ratio (95% interval) |
|---|---:|---:|---:|
| uniform | 1 | 1.000 (0.978–1.022) | 0.987 (0.974–1.001) |
| uniform | 4 | 0.992 (0.967–1.018) | 0.955 (0.906–1.007) |
| uniform | 16 | 0.977 (0.955–1.001) | 0.966 (0.931–1.002) |
| skewed | 1 | 0.984 (0.942–1.028) | 0.986 (0.971–1.000) |
| skewed | 4 | 0.869 (0.815–0.926) | 0.937 (0.907–0.969) |
| skewed | 16 | 0.858 (0.836–0.879) | 0.925 (0.911–0.938) |

Every scheduled low-load ratio remained near one. The skewed single-lane
read-heavy upper bound is 1.0001 before rounding. This complete matrix
repeats the saturated prefix/write-heavy cost at four and sixteen lanes.
The [full summary](../tests/evidence/lazy-routing/summary-ci-41092212161.json)
retains per-arm CPU, RPCs, bytes, lag, catch-up, latency extrema and offered
load; each archive now also contains all cluster node logs. These
measurements describe their recorded source revisions, not later main
integrations or a universal performance guarantee.

## Final-runtime standard-release matrix

The complete final-runtime workflow
[`41101768733`](https://github.com/cstheorist/tarn/actions/runs/41101768733)
measured merge candidate `9950f4f`, whose tree exactly matches `f06a2bd`;
its production source is unchanged from `a20867b`. All 180 arms completed
without workload errors. Three arms exceeded 250 ms p99, making two of the
eighteen five-pair workload cases unqualified. CI job success means the
measurements completed, not that every performance case qualified.

| Shape | Lanes | Write-heavy paired ratio (95% interval) | Read-heavy paired ratio (95% interval) |
|---|---:|---:|---:|
| uniform | 1 | **Unqualified** | 0.915 (0.807–1.038) |
| uniform | 4 | 0.982 (0.933–1.034) | 0.955 (0.945–0.965) |
| uniform | 16 | 0.988 (0.975–1.001) | 0.957 (0.933–0.981) |
| skewed | 1 | 1.016 (0.960–1.075) | 0.970 (0.904–1.041) |
| skewed | 4 | 0.859 (0.827–0.892) | 0.923 (0.917–0.930) |
| skewed | 16 | 0.864 (0.844–0.884) | 0.929 (0.906–0.953) |

Uniform single-lane write-heavy work had one composite arm at 395 ms p99.
Uniform single-lane low-load work is also **unqualified**: two composite
arms reached 266 and 322 ms p99 and missed three and five scheduled slots.
Every other low-load case qualified near its offered schedule. No positive
ratio is assigned to either unqualified case. These observations preserve
the single-lane and low-load limitations alongside the eight-row benefit.

The [complete final CI summary](../tests/evidence/lazy-routing/summary-ci-41101768733.json)
and all six archives retain raw samples, source and executable hashes,
node logs and per-arm costs. The four-CPU prefix/write-heavy fixture and
the twelve-CPU fully bound fixtures answer different workload questions.
They are not pooled, and none is an independent-host durability or
production-scale capacity claim.
