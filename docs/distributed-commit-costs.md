# Distributed commit cost study (#187)

This study is in progress. No distributed optimization is enabled and no
performance improvement is established. Physical-host comparisons are the
separate #211 follow-up. The complete non-physical acceptance criteria in
#187 remain required, including a measured reduction, exact recovery/fault
evidence and uncertainty over matched end-to-end runs.

## Baseline and hypotheses

At baseline source `6a120f1b7ce2f59672100039b8b93dbec1c6a40f`, the gateway
sends full replacement tuples in PREPARE. A primary W-lock record stores
that canonical intent; the decision record stores membership/status, not
another row copy. APPLY reads the intent and replaces the committed user
tuple. Thus full values appear in both the intent and user-space WAL, even
for a narrow counter update to a wide row. Parallel PREPARE/APPLY fanout and
the existing one-shard paths already exist and are not new optimizations.

Measure these separately: gateway encoding/wire work, Lua dispatch/allocation,
the durable intent, user-space APPLY, quorum wait and cleanup. Reducing one
does not imply reducing another. WAL records are not counts of fsync calls.

The first candidate hypothesis is using standard Tarantool UPDATE operations
for eligible APPLY rows while retaining the complete canonical intent.
Unchanged string/varbinary values can potentially be omitted from the user
write; numeric, NULL and other values must retain their exact final values.
A full-row mutation, missing base, unsupported layout or unproven condition
keeps the full replacement. This is a hypothesis to model and measure, not
an implemented or qualified policy. A compact intent that older readers
could misinterpret is not an acceptable shortcut under current format 1.

Before a candidate changes storage actions, a local refinement must establish
the same final tuple under the existing durable-lock, atomicity and replay
premises, with broken controls for incorrect omissions, lost base protection,
schema changes and repeated application. Preserve the canonical full-row
recovery path, confirmed receipts, immutable votes/membership, pending APPLY,
clock/expiry admission, uniqueness, lazy promises and strong/raw visibility.
Keep Verus at zero errors and exercise the existing protocol model and live
fault obligations. Explicitly qualify mixed-version restart/rollback.

## Transport observation

`TARN_LOG=tarn.commit_cost=trace` selects opt-in events from the IPROTO client.
Normal log levels disable them. Known procedure names map to a fixed phase
set; arbitrary function names, arguments, tuples and error-message contents
are excluded. A bounded first string argument of known transaction procedures
is hashed for correlation; absent, malformed or oversized identities remain
`unattributed`. This is a diagnostic join key, not an authentication mechanism.

- `request` records process/connection/SYNC, configured owner scope (zero
  when absent), stream, request type, phase,
  correlation hash, enqueue time and the complete encoded packet length,
  including its actual prefix/header. It proves encoding and admission,
  not complete socket transmission or an effect at storage.
- `frame` records the actual encoded length/type/SYNC of every fully parsed
  response frame. CHUNK frames remain separate observations. An unknown SYNC
  or a malformed/truncated frame cannot certify a completed request.
- `terminal` records matched terminal drainage, enqueue-to-delivery duration
  and whether the oneshot receiver still existed. It does not certify an SQL
  acknowledgement or a durable vote: callers still interpret the reply.
- `retired` records a connection's pending count and deadline retirement.
  Requests without matched terminal evidence remain incomplete/uncertain;
  absence of a reply is never evidence that a mutation took no effect.

Times use one process-local monotonic origin. Event emission can be reordered
by concurrent tasks; join by process/connection/SYNC and use recorded times.
Logging does not run under the connection-state mutex. A trace-enabled pass
is diagnostic, separate from ordinary timing, and can perturb scheduling.
Other configured logging targets are outside this event schema.

The peer control checks actual received request lengths, fragmented and
different-length-prefix replies, CHUNK ownership, cancelled receiver drainage,
socket byte counters and content exclusion. The existing transport refusal,
admission, deadline and response-ownership tests remain required.

## Required evidence before qualification

Implement a bounded collector/auditor before retaining performance evidence:
cap raw bytes and line sizes, preserve hashes and failed/incomplete traces,
reconcile request/frame/terminal identities and compare aggregate socket
counters. Add transaction phase/participant timelines and storage observations
for logical WAL/replication bytes, kernel disk-write bytes where available,
lock duration, CPU, capture and cleanup. Report unavailable measurements
explicitly. No trace-only result establishes complete byte amplification.

Predeclare the matrix: 2/4/8 actual writing groups, 256 B/4 KiB/64 KiB rows,
narrow/full mutations, global-index and real lazy-capture cohorts, both engines
and RF3/fsync. Verify placement and actual participant counts. Use five
alternating control/candidate pairs with fixed scheduled arrivals and p99/error
objectives; separate profiling from timing and retain every regression.
Reconcile exact tokens, per-key effects and complete payloads. Include local
transaction controls and faults through PREPARE, decision, APPLY and cleanup,
with unknown outcomes recorded and no mutation replay. Publish useful goodput,
latency, resource/byte costs and uncertainty before choosing an enabling policy.
