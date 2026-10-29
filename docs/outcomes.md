# Outcomes, transaction state and retries

A retry decision needs the operation and the evidence of its outcome, not
just a numeric error code. This contract applies to text SQL and binary
prepared statements. It also describes the internal native/Lua boundary;
raw Tarantool operations do not acquire SQL transaction semantics merely
by accessing the same tuples.

## Outcome table

| Observed outcome | Code / SQLSTATE | Transaction state | Permitted next action |
|---|---|---|---|
| RPC admission refused before enqueue | 1205 / HY000 | This RPC has no effect. A statement error preserves prior explicit-transaction work; a failed COMMIT ends that transaction. | Relieve pressure, then retry the statement or restart the ended transaction. Refusal of one PREPARE does not prove that every participant refused; the coordinator still resolves/aborts the distributed transaction. |
| Complete pre-effect ownership/leader fence | 1213 / 40001 if surfaced | Affected explicit transaction ends; no user mutation committed from the refused attempt. | Refresh ownership and restart the transaction. Tarn may retry bounded autocommit work after this proof. |
| Complete native handback | Internal `['lua']` or `['lua','all']`; no SQL error | Original stream/read set retained; this native attempt made no effects. | Gateway calls the Lua implementation on the selected connection. Extra fields/returns, malformed bodies and missing DATA do not authorize handback. |
| Known serialization abort or deadlock | 1213 / 40001 | Whole transaction rolled back. Prior buffered statements did not commit. | Restart the whole transaction with bounded backoff. |
| Lock wait timeout during a statement | 1205 / HY000 | Failed statement rolled back; preceding work remains active. | Retry the statement after the blocker clears, or explicitly ROLLBACK. A COMMIT-time timeout instead ends the transaction. |
| Matched duplicate key | 1062 / 23000 | Failed statement rolled back; preceding work remains active. | Correct the statement, explicitly roll back, or resolve the matching durable operation record. A duplicate is not itself proof that an unrelated business effect occurred. |
| Table definition changed | 1412 / HY000 | Explicit transaction ends. | Refresh/replan and restart the transaction. |
| Confirmed logical COMMIT, APPLY incomplete | OK, no error | Client transaction ended and committed. Durable recovery owns remaining intents/claims. | Do not replay the transaction. Strong reads wait/refuse unfinished rows; raw native reads may still return previous committed tuples. |
| Lost mutation reply, invalid write envelope, or native uncertainty after commit began | 1180 / 40003 | Local COMMIT/implicit statement has ended from the session's perspective; its effects may have committed. Distributed recovery may still be running. | Resolve using a durable application operation ID or other authoritative evidence. Never blindly replay the statement or the whole transaction. ROLLBACK cannot undo an already committed effect. |
| SQL connection lost, client deadline/cancellation after dispatch | Driver-specific, often 2013 / HY000 or no SQLSTATE; no server ERR packet guaranteed | Session may still be executing; eventual disconnection cleanup does not reverse a committed operation. | Treat the mutation as unknown until resolved. Reconnecting does not authorize replay. |
| Read-only transport failure | Usually 1213 / 40001 for a lost SQL storage read; native clients retain their transport domain | No user mutation from the read, but a transaction's snapshot/locks may be lost. | Retry an independent read or restart the transaction. Do not replay preceding writes merely because the last operation was a read. |
| Other matched database error | Original mapped code/state | Depends on the statement and commit phase. | Follow that error's contract; do not infer rollback/retry eligibility from an arbitrary `HY000` or storage number. |

An ERR packet carries code, SQLSTATE and message but **no transaction status
bits**. After a recoverable statement error a successful response refreshes
the driver's `IN_TRANS` status. PyMySQL exposes the numeric code but discards
SQLSTATE from its exception; Connector/Python exposes both. The executable
fixture observes PyMySQL's actual ERR packet without altering its exception
classification. Prepared executions use the same SQL error envelope.

Known rollback replies are accepted only after the complete expected write
envelope is checked: one return value, recognized status, exact field
count/types, bounded integer code and no trailing data. Successful local/RC
commits also require the complete numeric-count acknowledgement. A native
`['error',1180,message]` maps to **40003**, including when no socket was lost.
The decoder cannot protect against a storage server deliberately forging an
otherwise valid, false acknowledgement; storage correctness and authenticated
transport remain trusted boundaries.

## Recovery is not replay

The coordinator may repeat idempotent APPLY, ABORT and status operations for
the same transaction identity. It may resolve a lost PREPARE receipt from
durable votes/decision state. It must not turn an unknown PREPARE into a NO
vote or re-execute an application `UPDATE v=v+1`. An expired/absent protocol
record is not necessarily evidence that the transaction never committed.

The IPROTO client retains request identity and count/byte charges when its
caller abandons the wait. Only the matching terminal reply or physical
connection retirement releases them; a canceled oldest request still has its
original deadline. A matched database error differs from an unknown SYNC,
duplicate final reply, unsupported response type or truncated frame, which
retire the connection and leave pending mutations uncertain.

## Durable operation IDs

A generic pattern uses an ordinary unique primary key and the same SQL
transaction for the operation record and the effect:

```sql
CREATE TABLE operations (
    op_id BIGINT PRIMARY KEY,
    target_id BIGINT NOT NULL,
    delta BIGINT NOT NULL,
    result BIGINT NOT NULL
);

BEGIN;
INSERT INTO operations VALUES (123, 7, 1, 0);
SELECT v FROM t WHERE id=7 FOR UPDATE;
UPDATE t SET v=v+1 WHERE id=7;
SELECT v FROM t WHERE id=7;
-- Bind that returned value into this prepared statement:
UPDATE operations SET result=? WHERE op_id=123;
COMMIT;
```

Bind the final read into the operation record within the same transaction.
Store and compare the full request identity/payload (here `target_id,delta`),
and the result needed by the caller. Use a generated operation ID that is
never reused for a different operation. Routing the record and target to
different shards still requires the distributed commit contract.

After an uncertain reply, inspect that record with strong SQL. If a repeated
attempt to insert the same ID returns 1062, **ROLLBACK that attempt** before
reading and validating the existing record. Execute no effect after the
duplicate. A matching committed record resolves the original result; a
mismatched payload is an application error. A read refusal, unreachable
shard, missing record while the original may still be in flight, or expired
retention evidence leaves the outcome unresolved. A new transaction may
attempt the unique insert as the fence against a concurrent original; it
must perform the effect only after owning that insert, and must still handle
its own commit uncertainty.

Keep records for at least the entire permitted retry/reconciliation window,
and prevent replay of expired IDs (for example with a durable epoch cutoff
that rejects them). Deleting a record while a client may reuse its ID removes
the deduplication guarantee. Tarn's bounded internal transaction-record GC
is not an application deduplication store. This pattern does not make external
side effects atomic; publish such work via an application outbox with its own
delivery/deduplication contract.

## Supported KV and raw access

The public strong KV API is being developed under #181; this branch does
not publish or certify that API. Its planned read-only scope has no mutation
or ambient SQL transaction to replay. Its errors must retain their namespaces
(contract, MySQL or Tarantool), cause/code and SQLSTATE when supplied; a native
numeric code must not be relabeled as a MySQL code. A retry of an independent
point/batch read may observe a newer image. Ownership rediscovery does not
silently refresh a stale schema handle or authorize writes. A bounded batch
must fail explicitly rather than acknowledge a clipped prefix as complete.
That API's executable integration is gated on #181's published version.

Raw GET/SELECT can observe replica lag or the interval between a logical
COMMIT and physical APPLY. Raw writes bypass SQL constraints, captures and
transaction coordination and are not a supported substitute for SQL writes.
Do not infer safe mutation retries from a successful raw read of an old row.

## Executable release gate and proof boundary

`contracts/outcomes.json` registers the cases in `tests/sql/outcomes.py` and
names the existing transport/decision fault gates. The runner exercises
PyMySQL text and Connector/Python text/binary prepared SQL on two RF3 groups
with fsync, independently for memtx/vinyl and native/Lua storage. It records
code/state, refreshed transaction state, exact per-key effects, every matched
mutation completion, source/build/config hashes and all failures. The proxy
consumes the matched backend success before losing/replacing its reply;
`before_send` deliberately drops an already enqueued RPC without forwarding
it, which is still unknown to the gateway rather than a proven admission
refusal. Actual admission/refusal and cancellation accounting are separately
checked by `tests/rpcs.sh` and the IPROTO socket tests.

`NativeHandback` now includes malformed procedure envelopes. The positive
stream/autocommit configurations preserve `AtMostOnce` and `ReplyHolds`;
`Bug_HandbackMalformedReply*` accepts a plausible prefix and violates the
contract. The existing commit, PREPARE-confirmation, pipeline-admission and
APPLY/freshness models remain required. These are bounded contracts, not a
proof of the Rust parser, Python clients, byte corruption detection, TLS,
Tarantool storage engine, scheduling or end-to-end implementation refinement.
