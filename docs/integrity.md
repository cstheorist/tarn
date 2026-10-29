# Integrity declarations and legacy migration

New `CREATE TABLE` and `ALTER TABLE` reject FOREIGN KEY, CHECK and ENUM/SET
value-list declarations with **1235 / 42000**, before publishing a schema.
This is an intentional MySQL compatibility refusal. Tarn does not advertise
referential actions, CHECK truth evaluation, ENUM ordinals or SET bit masks.
`FOREIGN_KEY_CHECKS=0`, `INSERT IGNORE`, prepared statements and the legacy
session option do not enable those declarations. Partitioned-table foreign
keys are also rejected; MySQL's own partition/FK restrictions remain separate.

| Declaration | Supported contract | Errors / limits |
|---|---|---|
| FOREIGN KEY, column REFERENCES, all ON DELETE/UPDATE actions | Rejected on new CREATE/ADD | 1235/42000; no cascade, parent check or distributed referential locking |
| Column/table CHECK, ENFORCED and NOT ENFORCED | Rejected; ALTER enforcement is rejected too | 1235/42000; no CHECK predicate or NULL/UNKNOWN truth semantics are advertised |
| ENUM / SET | Rejected on CREATE, ADD, MODIFY and CHANGE | 1235/42000; use an unrestricted text type only when that is the intended contract |
| NOT NULL | SQL writes reject explicit NULL; ALTER validates old rows before publication | 1048/23000 for a NULL write; existing-NULL ALTER refusal is 1265/HY000 (the legacy Tarn error contract), versus MySQL 1138/22004 and MariaDB 1265/01000; failed ALTER retains the old usable schema |
| PRIMARY / UNIQUE | SQL local and global indexes enforce non-NULL uniqueness; a nullable unique key permits multiple NULLs | 1062/23000 for duplicates; failed index validation retains the old schema |

The machine-readable [integrity contract](../tests/sql/data/integrity-contract.json)
and [live differential runner](../tests/sql/integritytest.py) record exact
refusals and supported mutation controls. MySQL **8.4.4/InnoDB** is the pinned
reference; MariaDB 10.11 is tested separately, retaining its syntax differences.
The runner covers single/multiple rows, prepared execution, ON DUPLICATE KEY
UPDATE, duplicate INSERT IGNORE, savepoints, statement rollback, NULL/unique
keys, existing-data validation and concurrent conflicts. This manifest is the
integrity subset of the broader compatibility work in #178, not a claim that
all MySQL syntax or coercions are supported.

## Inspect an existing deployment

**Upgrade all SQL gateways before relying on declaration/write admission.**
Older gateways do not implement this gate. The storage protocol, native ABI,
commit, lock and recovery algorithms are unchanged. A rollback to an older
gateway restores its old unsupported-constraint behavior.

```sql
SHOW TARN INTEGRITY;                  -- current database
SHOW TARN INTEGRITY FROM db;
SHOW CREATE TABLE db.t;
SELECT @@tarn_legacy_integrity;        -- REJECT by default
```

New tables carry `integrity_policy=1` in catalog JSON. It records that their
DDL passed this declaration gate; **it is not a data-validation certificate**.
`SHOW TARN INTEGRITY` reports `REJECT_UNSUPPORTED_V1`, legacy foreign-key
count, declaration history and `Data_validation=NOT_CERTIFIED`.

Missing, old or unknown provenance reports `LEGACY_UNENFORCED`, and SHOW
CREATE appends the same explicit notice. Known foreign keys remain in that
output for inspection/removal, but have no enforcement. Historically CHECKs
were discarded and ENUM/SET lists were normalized to TEXT. Their absence in
a catalog cannot prove they never existed, recover a predicate/list, or prove
old rows valid. All such tables therefore retain an unknown historical status,
even after their last recorded foreign key is dropped or another ALTER succeeds.

SQL INSERT, REPLACE, UPDATE and DELETE on a legacy/unknown table fail with
1235/42000 by default, before the fast path, row locks, buffered mutations or
storage effects. SELECT, inspection, DROP and migration DDL remain available.
This includes parameterized execution, INSERT SELECT, upserts and IGNORE.
A temporary, **session-only** acknowledgement allows continuing the old
unenforced behavior while migrating:

```sql
SET SESSION tarn_legacy_integrity='ALLOW';
SELECT @@tarn_legacy_integrity;        -- ALLOW; schema stays LEGACY_UNENFORCED
-- Existing SQL writes may now continue; no FK/CHECK/value-list guarantee exists.
SET SESSION tarn_legacy_integrity=DEFAULT; -- REJECT
```

Only ALLOW/REJECT/DEFAULT are accepted. SET GLOBAL is rejected. The option
controls admission of later DML statements, not an already admitted transaction's
commit, and never certifies data or permits a new unsupported declaration.
There is no implicit legacy import mode. Do not use a connection-pool initializer
to enable it without deliberately accepting that boundary.

## Migrate deliberately

1. Save the old catalog/schema and original DDL history. Use SHOW CREATE to
   inventory recorded foreign keys; recover discarded CHECK/list definitions
   from external schema history. If they are unavailable, keep that uncertainty
   explicit. `ALTER TABLE t DROP FOREIGN KEY name` removes known metadata,
   retaining its ordinary supporting index; it does not certify data.
2. Choose the intended contract. If referential actions, CHECK or value-list
   enforcement are required, retain a database that implements them or wait for
   supported enforcement. Removing a declaration changes the contract; it does
   not implement it. The server refuses an unmodified dump containing them.
3. For an intentionally unrestricted text / NOT NULL / UNIQUE schema, create
   a fresh destination with only supported declarations. Copy bounded primary-
   key batches under an application write fence. New NOT NULL/UNIQUE checks
   reject invalid data as it is copied; handle errors explicitly. Keep a partial
   destination unpublished and retain the original until every batch and exact
   row/key comparison has passed. Do not infer original CHECK/list validity.
4. During the maintenance fence, finish the normal DDL job/recovery workflow,
   switch the application to the checked destination, verify names, data and
   indexes, then reopen writes. No atomic multi-table rename or online
   cross-cluster migration guarantee is introduced here.

An in-place supported NOT NULL or UNIQUE addition validates existing data
before its normal fenced DDL publication. A failed validation leaves the prior
schema and rows usable. This uses the existing implementation, including its
large-table/backfill limitations; no unbounded online constraint validator is
introduced. Ordinary ALTER never promotes legacy provenance to current.

## SQL and privileged storage writes

These are SQL admission/enforcement contracts across the native and Lua SQL
paths and memtx/vinyl engines. Storage administrative credentials, direct
`box.space` / IPROTO writes, arbitrary EVAL and catalog edits are privileged
bypasses; they can violate SQL column, index/capture and schema assumptions.
Restrict them operationally. They are not a supported integrity-preserving KV
mutation API. #181 tracks the supported external KV API and #139 deployment
privilege controls; ancestry lint does not enforce referential integrity.

The legacy test injects old metadata only into fresh owned fixtures, then checks
real catalog reload, default refusal, explicit opt-in, metadata removal, exact
rows and retained unknown provenance. No commit/locking/recovery transition or
verified body changes, so this gate does not add a distributed enforcement
model. The existing model/Verus and full runtime fault matrix remain release
checks, not proofs of SQL parser or legacy-data validity.
