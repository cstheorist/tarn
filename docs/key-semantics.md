# Key equality and ordering

`SET SESSION tarn_key_contract='MYSQL'` selects a bounded, checked key
profile. First use `SET NAMES utf8mb4 COLLATE utf8mb4_0900_bin`. The default,
also restored by `DEFAULT`, is explicitly named `LEGACY`; inspect
`@@tarn_key_contract` and `EXPLAIN FORMAT=JSON`'s `key_contract` object.
The latter reports declared/effective text collations and their physical
key encoding. This setting does not change isolation or index consistency.

The profile checks CREATE, ALTER, reads and writes before execution or a
DDL implicit commit. Prepared execution rechecks the current profile,
catalog and parameter types. A legacy prepared plan cannot bypass it.
Unsupported definitions, collation names, conversions and expressions
fail with 1235/42000. Changing the setting commits no transaction and
rewrites no row. It does not certify legacy data, foreign keys or CHECKs.

## Supported matrix

The reference is MySQL **8.4.13**, pinned by image digest in
[`contracts/key-semantics.json`](../contracts/key-semantics.json). The
executable cases in [`key_semantics.py`](../tests/sql/key_semantics.py)
compare exact rows, ordering and errors through PyMySQL text and MySQL
Connector/Python binary prepared statements. These are registered shapes,
not a claim of complete MySQL expression or protocol compatibility.

| Input/storage family | MYSQL key profile | Boundary |
|---|---|---|
| `VARCHAR`, `utf8mb4_0900_bin` | UTF-8 byte order, NO PAD; case, accents and combining spellings remain distinct | Valid UTF-8; explicit collation on definitions or a matching gateway default |
| `VARBINARY` | Opaque bytes, including zero and non-UTF-8 bytes; trailing spaces significant | Use hex SQL literals or binary parameters; VARBINARY enforces its byte limit |
| Signed/unsigned TINYINT through BIGINT, BOOL | Exact integer identity, range checks, integer/decimal comparisons | YEAR and floating-point conversions are outside this profile |
| `DECIMAL(p,s)` | Exact comparisons, DISTINCT/group identity, MIN/MAX, integer assignment rounding, typed index probes | `1 <= p <= 38`, `0 <= s <= min(p,30)`; base shard keys remain integer/string only |
| `utf8mb4_bin` | Refused | Tarn's existing encoding counts trailing spaces; MySQL's PAD SPACE collation does not |
| General/unicode/language/0900 case-insensitive names | Refused | Existing weights and aliases remain legacy behavior, including PAD ordering differences |
| Other character sets, CHAR, fixed BINARY, TEXT/BLOB families, FLOAT/DOUBLE, YEAR, JSON and temporal columns | Refused | No claim for their padding, conversion, validation or time-zone rules |
| Prefix indexes, LIKE, mixed numeric/string comparisons, explicit casts, general functions, arithmetic, SUM/AVG, subqueries/UNION | Refused | The legacy executor still supports its documented subset; exact key mode does not certify it |

The supported query subset includes direct projections, comparisons,
NULL predicates, IN/BETWEEN, joins, ORDER BY with explicit tie breakers,
LIMIT/keyset pagination, DISTINCT/GROUP BY and COUNT/MIN/MAX. DML values
may be bound or literal exact numbers, strings, binary values or NULL.
Assignments use the destination type's checked conversion. An approximate
FLOAT parameter is refused even if this particular value could be exact.
Decimal SQL literals use a decimal point; exponent SQL literals are
approximate under MySQL's rules and refused by this profile. A decimal
string assigned to an exact column is parsed without a float intermediate.

MySQL permits larger DECIMAL precision. Tarantool's native decimal has 38
digits, so the profile refuses wider definitions before schema publication.
It also refuses an unsupported base DECIMAL shard key before DDL commits
prior work. Decimal local and global indexes on an integer-sharded table
remain supported. Lookup values are expressed at the column's declared
scale; inexact equality probes match nothing, and inexact range bounds
are dropped while retaining the residual predicate.

## Stored and normalized keys

Rows retain their public values. Under `utf8mb4_0900_bin`, the stored UTF-8
bytes are also index and routing bytes; the new name has MySQL collation
ID 309. Binary fields use their raw bytes. Signed and unsigned nonnegative
integers use the same minimal MessagePack encoding. DECIMAL index values
use the declared scale and native MP_DECIMAL, without binary floating-point
conversion. The existing verified CRC-32C routing body is unchanged.

Legacy case-insensitive indexes still use their existing `_weight_N`
fields. Their collation names, weights and buckets are not rewritten when
the session selects MYSQL. A table using them is refused in that mode.
Migrate by creating a supported table and copying validated values under
a maintenance fence; resolve newly distinct/conflicting values explicitly.
Do not change a stored catalog collation or recalculate existing buckets
in place. ALTER preserves the established refusal of changes that require
rewriting keys (including changing a populated DECIMAL scale).

Exact decimal comparisons and integer assignments now preserve values
above 2^53 in both modes. Earlier writes that already rounded a value
cannot be reconstructed by upgrading. Existing application data needs an
independent source of truth if it may have passed through that old path.
The original regression equates `9007199254740992.01` and
`9007199254740992.02`; the retained negative control demonstrates the bug.

## Protocol and consistency boundaries

All gateways reject invalid UTF-8 COM_QUERY/COM_STMT_PREPARE text with
1300/HY000 before dispatching any statement of that command. They no longer
replace invalid bytes inside SQL with U+FFFD. This is an explicit restriction
relative to MySQL's binary string handling in text commands. Use `X'ff00'`
or a binary prepared parameter for arbitrary bytes. A refusal leaves prior
transaction work intact; it does not certify any earlier uncertain write.

The profile does not strengthen raw native reads or a schema's explicit
`COMMENT 'tarn:lazy'` eventual-read extension. The lazy key comparison
controls enable session catch-up and compare settled results; they do not
claim that unsettled lazy SELECTs match MySQL. Ordinary strong paths retain
their existing read validation. Raw clients must use physical field/index
metadata, existing ownership/freshness rules and native type decoding;
privileged writes bypassing SQL remain outside this profile. The supported
KV API work is tracked separately in #181.

Every gateway used by a client must implement this profile. Older gateways
may accept unknown SET variables inertly: SET/readback alone is insufficient
feature negotiation. Require the EXPLAIN `key_contract` object with
`requested="MYSQL"` for the intended statement, and reject a missing field.
Downgrading restores the previous conversion behavior; neither an old
gateway nor direct native writes enforce this admission policy.

## Evidence and proof boundary

`tests/sql/key_semantics.py` retains exact per-server traces and source
hashes, including failures. Its separate legacy temporal control checks
equivalent DATETIME spellings and uniqueness without claiming the temporal
type is admitted by MYSQL mode. Existing MariaDB differential tests,
native/Lua storage suites, global uniqueness and movement/restart faults,
the TLA+ registry and shared Verus checks remain required release gates.
The `Exact key contracts` workflow runs both clients in all four
memtx/vinyl × native/Lua configurations, then kills an owned RF3 leader
during writes, observes promotion, restores the killed clock peer, reconciles
every acknowledged/refused/uncertain attempt and moves a populated bucket. It compares the
final SQL values with native decoded tuples. Mutations are never retried.
Artifacts retain traces, build hashes, client versions, fault outcomes and
cluster logs even on failure; a failing or absent matrix cell is not evidence
for that configuration.
No storage protocol action, native ABI or verified executable body changes.
No throughput or whole-engine correctness claim follows from these tests.
