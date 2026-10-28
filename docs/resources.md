# SQL resource limits

Issue [#134](https://github.com/cstheorist/tarn/issues/134) adds admission
limits to gateway SQL execution. Large materializations are refused;
there is no disk spill implementation. An eligible unindexed
`ORDER BY ... LIMIT` scan instead keeps bounded candidates while scanning.
Neither path sends successful result rows before the existing autocommit
validation completes.

## Configuration and failures

Add a `resources` object to `cluster.json`. Omitted fields use these defaults:

```json
{
  "resources": {
    "query_bytes": 67108864,
    "transaction_bytes": 33554432,
    "server_bytes": 536870912,
    "query_work": 100000000,
    "server_work": 1000000000,
    "read_timeout_ms": 10000,
    "topk": true
  }
}
```

All numeric limits must be positive. Bytes and work are logical admission
units, with conservative allocation multipliers. A query shares its meter
across nested queries, UNION branches, SQL/binary prepared execution and
execution retries. Work counts decoded bytes/cells, expression evaluations,
join candidates, estimated sort comparisons and LIKE's input/pattern product.
It is cumulative until query ownership ends; it is not a CPU-time deadline.

Transaction accounting covers read-set keys/ranges, buffered writes, lock and
stream metadata, statement snapshots and savepoints. It is deliberately
cumulative until commit/rollback, including replaced writes and released
savepoints. Session variables and prepared definitions have a retained
account limited by `query_bytes`. All these accounts share `server_bytes`;
plan-cache entries, SQL connection input buffers and binary long parameters
also reserve against that gateway-wide cap. `server_work` covers concurrent
queries, rather than a replenishing per-second rate.

Execution admission failures return **1041/HY000**, with a message starting
`Tarn resource limit:`. The statement has not committed any DML. An explicit
transaction is rolled back in full, freeing its buffered writes, read set,
savepoints and locks; start a new transaction before retrying. An oversized
MySQL input buffer can instead close the connection before SQL dispatch.
COM_STMT_SEND_LONG_DATA has no response: its admission failure is remembered
and returned by EXECUTE, and RESET/close releases its retained parameters.

Completion acquires no new execution budget after the decision boundary.
Commit workers and fast autocommit workers retain ownership if their SQL
caller is cancelled. An uncertain mutation still returns **1180/40003** and
is never blindly replayed. A safe read/validation conflict remains
**1213/40001**. The new cap does not change those retry distinctions.

## Allocation ownership

Storage frames for gateway connections are capped at **8 MiB**, checked from
the length prefix before payload reservation. This allows the normal 4 MiB
pager target and a boundary row; a larger single row is refused on reads.
Each active read reserves 16 MiB plus its encoded arguments before dispatch,
covering buffer growth. A query has one to four concurrent read slots,
derived from its byte limit, so small batched joins wait for their own wire
slots instead of multiplying maximum-frame reservations without bound.

A cancelled read retains its reservation until reply or deadline. The read
deadline retires its physical connection. Fast autocommit has a deadline of
at least `read_timeout_ms` and at least the lock-wait timeout plus one second;
after dispatch, expiry is an unknown outcome. Socket retirement can make
other in-flight mutations on that shared connection unknown too. They are
not retried. Durable transaction completion retains the existing recovery
protocol and is not refused when the execution cap fills.

Decoders admit row slots and the complete backing frame before allocation.
Ordinary query values can therefore retain zero-copy slices. Transaction
keys/writes and retained top-K/join rows copy their string payload before a
page's temporary account is released. Join candidate/predicate scratch and
discarded heap pages are refunded; retained output, sort keys, groups and
DISTINCT state remain charged. Expression expansion is admitted before
REPEAT, padding, replacement, case conversion and related allocations.

Result ownership travels through the MySQL server. It remains charged while
encoding or blocked on socket delivery, including the final buffered packet,
and is released by flush or disconnect. This bounds aggregate logical SQL
ownership across slow consumers as well as executing queries.

## Bounded ORDER BY/LIMIT

The initial heap path is intentionally narrow: one real table, a full/range
or single-prefix access, finite positive OFFSET+LIMIT, no aggregation,
DISTINCT, HAVING or locking clause, and no buffered writes to that table.
Existing index-ordered paging takes priority. Sort keys must be direct
columns, possibly wrapped in collation keys; FLOAT/DOUBLE and computed or
volatile keys use the normal executor.

Each participating shard retains at most **OFFSET+LIMIT** candidates and one
storage page. The final stable sort merges those candidates. ASC/DESC,
NULL ordering and existing collation comparison are preserved, as are the
same range recording, snapshot and OCC validation requirements. Large
OFFSET is admitted before heap allocation and may be refused even for an
empty input. Other shapes run with the same limits and may be refused;
`topk: false` is a diagnostic control for matched materialization experiments.

## Monitoring and verification boundaries

`SHOW STATUS` reports `Tarn_resource_bytes`, `Tarn_resource_peak_bytes`,
`Tarn_resource_work`, `Tarn_resource_peak_work`, `Tarn_resource_queries`,
`Tarn_resource_refusals`, `Tarn_resource_read_deadlines` and
`Tarn_topk_scans`. Peaks are gateway-lifetime values; live queries include
results awaiting delivery. Idle sessions and cache entries can keep bytes
charged when no query is active.

These counters bound the admitted SQL envelope, **not total process RSS**.
They are conservative estimates rather than an allocator proof. Allocator
arenas, thread/runtime infrastructure, kernel sockets, catalog state and
native storage memory require separate process/container limits and sizing.
The cap does not bound time inside Tarantool execution or prove fairness
between small and large queries. Immediate global refusals can reject useful
point reads under pressure. Lower limits can reject previously accepted SQL;
increase appropriate caps or split the operation if that policy is unsuitable.

`QueryBudget.tla` checks abstract ownership, predecision refusal and validated
output, with three failing mutation controls. It does not prove concrete
allocation sizes or scheduling. The Verus crate is unchanged. Runtime tests
cover accounting/refunds, stable heap selection, an abandoned established
read, exact MariaDB comparisons, oversized scans/joins/groups/offsets,
no partial DML, explicit-transaction cleanup, slow consumers, binary long
data, work caps and concurrent overload.

Run the dedicated fixture with native procedures or Lua fallback:

```sh
TARN_RESOURCE_REF=127.0.0.1:3407 tests/resources.sh
TARN_NATIVE=0 TARN_RESOURCE_REF=127.0.0.1:3407 tests/resources.sh
```

It owns a separate two-shard cluster and six gateways. Its JSONL records the
executable hash, effective profiles, matched query timings/RSS, overload
useful completions/refusals/tails and accounting peaks. It changes no running
cluster. `TARN_RESOURCE_PORT`, `TARN_RESOURCE_BASE`, `TARN_RESOURCE_EVIDENCE`
and `TARN_RESOURCE_BUILD` select isolated ports, raw-log destination and a
build label. Matched top-K/materialization gateways use the same binary,
data, limits, SQL and OCC, with three measured repetitions each. Treat these
short, shared-host runs as resource-policy evidence, not capacity or SLA
claims. Repeated measurements for this change are in
[bench/resources](../bench/resources/README.md).
