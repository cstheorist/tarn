-------------------------- MODULE ExternalStrongKV --------------------------
(* External read-only frozen-handle adapter (#181), not a new freshness or
 * commit algorithm. Three positional independent points a,b,a include a
 * duplicate and an initial miss. Per-point confirmed physical reads, native
 * quorum/clock/role admission, authenticated authorization, no ambient
 * transaction, complete canonical keys and existing DDL/engine read guards
 * are compositional premises. This model does not prove those primitives.
 *
 * It DOES read physical owner images, not an oracle's desired logical
 * value. An acknowledged insert can retain a prepared claim while physical
 * APPLY is absent. A moved owner's confirmed old image can be empty. The
 * mandatory claim/owner/binding checks derive the public point meaning.
 * Expected values below only record the logical image at the atomic point
 * for the invariant; they are never consulted by a serving guard.
 *
 * Schema/incarnation mismatch is terminal for the original frozen handle.
 * Owner-only refresh restarts the WHOLE vector under the SAME handle and
 * finite shared work/retry budgets. No partial vector is public on error.
 * Independent points, including duplicates, can see different revisions.
 * Atomic/snapshot mode is unsupported, not inferred from fresh points.
 * Native MVCC, route publication, DDL lifecycle, authentication, wire
 * encoding, client deadline and Tarantool API behavior remain external.
 *)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Profile, BugIgnoreOwner, BugIgnoreBinding, BugSchemaAsOwner,
          BugClipComplete, BugIgnoreClaim, BugAdvertiseAtomic

Keys == {"a", "b"}
Shards == {0, 1}
Request == <<"a", "b", "a">>
Profiles == {"normal", "owner", "incarnation", "schema", "cap", "pending", "atomic"}
ActiveProfiles == IF Profile = "all" THEN Profiles ELSE {Profile}
Original == [a |-> 1, b |-> 0]
Inserted == [a |-> 1, b |-> 2]
Changed == [a |-> 2, b |-> 2]
Images == {Original, Inserted, Changed}
Frozen == [instance |-> 0, schema |-> 0]
Tokens == {Frozen, [instance |-> 0, schema |-> 1], [instance |-> 1, schema |-> 0]}
Column(schema) == IF schema = 0 THEN "v" ELSE "w"
WorkLimit == 6
RetryLimit == 1
Values(rows) == [i \in 1..Len(rows) |-> rows[i].value]
ImageValues(image) == [i \in 1..Len(Request) |-> image[Request[i]]]
Placed(image, routes) == [s \in Shards |-> [k \in Keys |-> IF routes[k] = s THEN image[k] ELSE 0]]

VARIABLES profile, metadata, owners, physical, logical, claims, writer,
          eventDone, history, c
world == <<profile, metadata, owners, physical, logical, claims, writer, eventDone, history>>
vars == <<world, c>>

Init ==
    /\ profile \in ActiveProfiles /\ metadata = Frozen
    /\ owners = [a |-> 0, b |-> 1]
    /\ physical = Placed(Original, owners) /\ logical = Original
    /\ claims = {} /\ writer = "idle" /\ eventDone = FALSE /\ history = {Original}
    /\ c \in {[phase |-> "idle", pos |-> 1, buffer |-> <<>>, out |-> <<>>,
               route |-> owners, handle |-> Frozen, work |-> 0, retries |-> 0,
               requested |-> mode, advertised |-> "none", error |-> "none"] :
               mode \in IF profile = "atomic" THEN {"independent_points", "atomic"}
                        ELSE {"independent_points"}}

Error(state, why) == [state EXCEPT !.phase = "error", !.buffer = <<>>,
                                   !.out = <<>>, !.error = why]
Success(state) == [state EXCEPT !.phase = "done", !.out = state.buffer,
                              !.advertised = state.requested, !.error = "none"]
Active == c.phase \notin {"done", "error"}

Start ==
    /\ c.phase = "idle"
    /\ c' = IF c.requested # "independent_points" /\ ~BugAdvertiseAtomic
            THEN Error(c, "UnsupportedMode") ELSE [c EXCEPT !.phase = "reading"]
    /\ UNCHANGED world

ReadPoint ==
    /\ c.phase = "reading" /\ c.pos <= Len(Request)
    /\ c' = IF c.work = WorkLimit THEN Error(c, "Budget") ELSE
       LET k == Request[c.pos]
           next == [c EXCEPT !.work = @ + 1]
           native == physical[c.route[k]][k]
           (* A stale physical codec would decode the new padding field.
            * The correct public binding check refuses before that read. *)
           decoded == IF native = 0 THEN 0 ELSE
                      IF c.handle.schema = metadata.schema THEN native ELSE 9
           row == [key |-> k, found |-> native # 0, value |-> decoded,
                   column |-> Column(c.handle.schema), instance |-> metadata.instance,
                   schema |-> metadata.schema, expected |-> logical[k]]
       IN CASE c.handle # metadata /\ ~BugIgnoreBinding ->
                    [next EXCEPT !.phase = "schema_error", !.error = "SchemaChanged"]
            [] c.route[k] # owners[k] /\ ~BugIgnoreOwner ->
                    [next EXCEPT !.phase = "owner_error", !.error = "Moved"]
            [] k \in claims /\ ~BugIgnoreClaim -> Error(next, "PendingWrite")
            [] Len(c.buffer) >= (IF profile = "cap" THEN 2 ELSE 3) ->
                    IF BugClipComplete THEN Success(next) ELSE Error(next, "TooLarge")
            [] OTHER -> [next EXCEPT !.buffer = Append(@, row), !.pos = @ + 1]
    /\ UNCHANGED world

OwnerRefresh ==
    /\ c.phase = "owner_error"
    /\ c' = IF c.retries = RetryLimit THEN Error(c, "RetryBudget") ELSE
            [c EXCEPT !.phase = "reading", !.pos = 1, !.buffer = <<>>, !.out = <<>>,
                      !.route = owners, !.retries = @ + 1, !.error = "none"]
    /\ UNCHANGED world

SchemaFailure ==
    /\ c.phase = "schema_error"
    /\ c' = IF BugSchemaAsOwner /\ c.retries < RetryLimit
            (* Incorrect error classification reloads another schema and
             * retains already-returned fields from the original prefix. *)
            THEN [c EXCEPT !.phase = "reading", !.handle = metadata,
                           !.route = owners, !.retries = @ + 1, !.error = "none"]
            ELSE Error(c, "SchemaChanged")
    /\ UNCHANGED world

Finish ==
    /\ c.phase = "reading" /\ c.pos = Len(Request) + 1
    /\ c' = Success(c) /\ UNCHANGED world

Move ==
    /\ profile = "owner" /\ ~eventDone /\ Active
    /\ owners' = [owners EXCEPT !["a"] = 1]
    /\ physical' = [physical EXCEPT ![0]["a"] = 0, ![1]["a"] = logical["a"]]
    /\ eventDone' = TRUE
    /\ UNCHANGED <<profile, metadata, logical, claims, writer, history, c>>

Recreate ==
    /\ profile = "incarnation" /\ ~eventDone /\ Active
    /\ metadata' = [instance |-> 1, schema |-> 0]
    /\ logical' = Changed /\ physical' = Placed(Changed, owners)
    /\ history' = history \cup {Changed} /\ eventDone' = TRUE
    /\ UNCHANGED <<profile, owners, claims, writer, c>>

Alter ==
    /\ profile = "schema" /\ ~eventDone /\ c.phase = "reading" /\ c.pos = 2
    /\ metadata' = [instance |-> 0, schema |-> 1]
    /\ eventDone' = TRUE
    /\ UNCHANGED <<profile, owners, physical, logical, claims, writer, history, c>>

PrepareInsert ==
    /\ profile = "pending" /\ writer = "idle" /\ Active
    /\ writer' = "prepared" /\ claims' = {"b"}
    /\ UNCHANGED <<profile, metadata, owners, physical, logical, eventDone, history, c>>
AckInsert ==
    /\ profile = "pending" /\ writer = "prepared" /\ Active
    /\ writer' = "acked" /\ logical' = Inserted /\ history' = history \cup {Inserted}
    /\ UNCHANGED <<profile, metadata, owners, physical, claims, eventDone, c>>
ApplyInsert ==
    /\ profile = "pending" /\ writer = "acked" /\ Active
    /\ writer' = "done" /\ physical' = Placed(Inserted, owners) /\ claims' = {}
    /\ UNCHANGED <<profile, metadata, owners, logical, eventDone, history, c>>

AtomicWrite ==
    /\ profile = "atomic" /\ ~eventDone /\ c.phase = "reading" /\ c.pos = 2
    /\ logical' = Changed /\ physical' = Placed(Changed, owners)
    /\ history' = history \cup {Changed} /\ eventDone' = TRUE
    /\ UNCHANGED <<profile, metadata, owners, claims, writer, c>>

Next == Start \/ ReadPoint \/ OwnerRefresh \/ SchemaFailure \/ Finish
        \/ Move \/ Recreate \/ Alter \/ PrepareInsert \/ AckInsert \/ ApplyInsert \/ AtomicWrite
Spec == Init /\ [][Next]_vars

RowsType(rows) ==
    /\ Len(rows) <= Len(Request)
    /\ \A i \in 1..Len(rows) :
          /\ rows[i].key \in Keys /\ rows[i].found \in BOOLEAN
          /\ rows[i].value \in {0,1,2,9} /\ rows[i].column \in {"v","w"}
          /\ rows[i].instance \in 0..1 /\ rows[i].schema \in 0..1
          /\ rows[i].expected \in 0..2
TypeOK ==
    /\ profile \in Profiles /\ metadata \in Tokens
    /\ owners \in [Keys -> Shards] /\ physical \in [Shards -> [Keys -> 0..2]]
    /\ logical \in Images /\ claims \subseteq Keys
    /\ writer \in {"idle", "prepared", "acked", "done"}
    /\ eventDone \in BOOLEAN /\ history \subseteq Images
    /\ c.phase \in {"idle", "reading", "owner_error", "schema_error", "done", "error"}
    /\ c.pos \in 1..4 /\ RowsType(c.buffer) /\ RowsType(c.out)
    /\ c.route \in [Keys -> Shards] /\ c.handle \in Tokens
    /\ c.work \in 0..WorkLimit /\ c.retries \in 0..RetryLimit
    /\ c.requested \in {"independent_points", "atomic"}
    /\ c.advertised \in {"none", "independent_points", "atomic"}
    /\ c.error \in {"none", "UnsupportedMode", "Budget", "SchemaChanged", "Moved",
                    "PendingWrite", "TooLarge", "RetryBudget"}
FullVector == c.phase = "done" =>
    Len(c.out) = Len(Request) /\ \A i \in 1..Len(c.out) : c.out[i].key = Request[i]
NoPartialError == c.phase = "error" => c.out = <<>> /\ c.buffer = <<>>
PointMeaning == c.phase = "done" =>
    \A i \in 1..Len(c.out) : c.out[i].value = c.out[i].expected
                           /\ c.out[i].found = (c.out[i].expected # 0)
FrozenColumns == c.phase = "done" =>
    \A i \in 1..Len(c.out) : c.out[i].found => c.out[i].column = Column(Frozen.schema)
(* In this profile the original incarnation is immutable. New values2 are
 * actual rows of a recreated table, never writes to the frozen object. *)
FrozenObjectData == c.phase = "done" /\ profile = "incarnation" => Values(c.out) = ImageValues(Original)
AdvertisedSnapshot == c.phase = "done" /\ c.advertised = "atomic" =>
    Values(c.out) \in {ImageValues(image) : image \in history}
=============================================================================
