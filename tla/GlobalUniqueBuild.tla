-------------------------- MODULE GlobalUniqueBuild --------------------------
(* An online build of a global unique index (issue #292): a unique key
   without the shard key, whose entries live in a space of their own,
   sharded by the key (one entry per row whose key has no NULL part). The
   table's definition goes through four versions:
     0  without the index;
     1  with the index building (write-only): every write maintains its
        entries in the write's own transaction, and a row whose key another
        row's entry holds is recorded as a conflict, not refused, as
        MariaDB's online ADD UNIQUE refuses no concurrent write. When the
        holder leaves the key, a conflicting row takes the entry over
        (GlobalUniqueRules);
     2  with the index enforcing: a write whose key another row's entry
        holds is refused (1062);
     3  without the index, once the change has failed: each shard drops its
        part of the index's spaces as it publishes it.
   A step to a new version fences every shard, durably, and waits for the
   writes in flight there (the drain) before any shard publishes it, as an
   ALTER's PREPARE and APPLY do. Between the steps to 1 and 2, a backfill
   gives the rows written before version 1 their entries, in pk order: it
   reads a row, and its write commits only if the row is unchanged and no
   write is prepared on it (the commit protocol validates the read); its
   cursor passes the row once the write commits. The step to 2 decides, once
   every shard is fenced and drained and the backfill has passed every row,
   to publish the index only if no conflict is recorded. Otherwise the
   change fails (1062) and takes the step to 3, as a cancel during the
   backfill does.

   Rows (primary keys 1 to 3) live on shards A and B, and a key's entry and
   conflicts on the key's shard. Rows don't move between shards: bucket
   moves wait for schema jobs, and one job spans the whole build. A write of
   version 0, 1 or 3 prepares on every shard it touches -- the row's, and
   with version 1 those of its old and new keys -- which refuses a write
   made with an older version than its own, or while it is fenced; it
   commits later, with no further check (the commit protocol's PREPARE and
   COMMIT, TarnCommit). A row has one write in flight at a time. A write's
   reads of the index and its effects are one step at its commit:
   GlobalUniqueWrites checks that the commit protocol makes them so
   (Serialized). A write of version 2 is one step. A leader change keeps a
   shard's fence: the coordinator counts a fence only once it is durable.

   NoStale: while the index lives, every entry and conflict is its row's
     current key.
   WellFormed: a key with no entry has no conflict, and the holder of an
     entry is not among its key's conflicts.
   Enforced: once a shard has published version 2, every row with a key
     holds its key's entry, and no conflict is left. With NoStale, that is
     one row per key: the index is exact and the key unique.
   Dropped: once a key's shard has published version 3, the key has no
     entry and no conflict: no write of the index commits after its spaces
     are dropped. *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugBackfillUnvalidated, \* a backfill write commits a row read earlier, changed since
          BugPublishConflicts,    \* version 2 is published with conflicts left
          BugEarlyPublish,        \* the step to version 2 begins before the backfill passed every row
          BugNoDrainToEnforcing,  \* the step to version 2 does not wait for the writes in flight
          BugNoDrainToAbort,      \* nor the step to version 3
          BugStaleWrite,          \* a shard takes a write made with an older version than its own
          BugVolatileFence        \* a shard's fence is lost when its leader changes

Pks == {1, 2, 3}
Done == 4                         \* the backfill's cursor once it has passed every row
Keys == {1, 2}
NoKey == 0                        \* no row, or a key with a NULL part: no entry
KeyOf == Keys \cup {NoKey}
Shards == {"A", "B"}
RowShard(p) == IF p = 2 THEN "B" ELSE "A"
KeyShard(k) == IF k = 1 THEN "A" ELSE "B"
KeyShards(ks) == {KeyShard(k) : k \in ks \ {NoKey}}
NoWrite == [active |-> FALSE, ver |-> 0, key |-> NoKey]
NoRead == [pk |-> 0, key |-> NoKey]
Phases == {"to1", "backfill", "to2", "enforcing", "to3", "aborted"}

R == INSTANCE GlobalUniqueRules
         WITH BugSkipConflict <- FALSE, BugNoPromote <- FALSE,
              BugBlindLeave <- FALSE, BugJoinReplace <- FALSE

VARIABLES rows,      \* pk -> its row's key (NoKey: no row, or no entry)
          entry,     \* key -> the pk whose entry holds it (0: none)
          conflict,  \* <<key, pk>> pairs: rows with a key another row's entry holds
          ver,       \* shard -> the definition version it published
          fenced,    \* shard -> fenced for the step under way
          acked,     \* shard -> the coordinator counts it fenced
          decided,   \* the step under way is decided: its shards may publish it
          inflight,  \* pk -> the write prepared on its row, not committed
          cursor,    \* the next pk the backfill reads
          pending,   \* the row the backfill has read, and its key (NoRead: none)
          phase
vars == <<rows, entry, conflict, ver, fenced, acked, decided, inflight, cursor, pending, phase>>
Target == CASE phase = "to1" -> 1 [] phase = "to2" -> 2 [] OTHER -> 3
State == [entry |-> entry, conflict |-> conflict]

Init ==
    /\ rows \in [Pks -> KeyOf]               \* the table's rows before the ALTER
    /\ entry = [k \in Keys |-> 0] /\ conflict = {}
    /\ ver = [s \in Shards |-> 0] /\ fenced = [s \in Shards |-> FALSE]
    /\ acked = [s \in Shards |-> FALSE] /\ decided = FALSE
    /\ inflight = [p \in Pks |-> NoWrite]
    /\ cursor = 1 /\ pending = NoRead
    /\ phase = "to1"

(* A write of version gv (0 and 3: without the index; 1: the building one)
   to row p, setting its key to k: it prepares on the shards it touches. *)
Touched(p, k, gv) == {RowShard(p)} \cup (IF gv = 1 THEN KeyShards({rows[p], k}) ELSE {})
Takes(s, gv) == ~fenced[s] /\ (gv >= ver[s] \/ BugStaleWrite)
Prepare(p, k, gv) ==
    /\ phase \in {"to1", "backfill", "to2", "to3"}
    /\ ~inflight[p].active
    \* (a gateway writes with a version some shard has published)
    /\ \E s \in Shards : ver[s] >= gv
    /\ \A s \in Touched(p, k, gv) : Takes(s, gv)
    /\ inflight' = [inflight EXCEPT ![p] = [active |-> TRUE, ver |-> gv, key |-> k]]
    /\ UNCHANGED <<rows, entry, conflict, ver, fenced, acked, decided, cursor, pending, phase>>
Commit(p) ==
    LET w == inflight[p]
        st == IF w.ver = 1 THEN R!Moved(State, p, rows[p], w.key) ELSE State
    IN /\ w.active
       /\ rows' = [rows EXCEPT ![p] = w.key]
       /\ entry' = st.entry /\ conflict' = st.conflict
       /\ inflight' = [inflight EXCEPT ![p] = NoWrite]
       /\ UNCHANGED <<ver, fenced, acked, decided, cursor, pending, phase>>

(* A write of the enforcing version, once a shard has published it: a key
   another row's entry holds refuses it (1062). *)
WriteEnforcing(p, k) ==
    /\ phase \in {"to2", "enforcing"} /\ \E s \in Shards : ver[s] = 2
    /\ ~inflight[p].active
    /\ \A s \in {RowShard(p)} \cup KeyShards({rows[p], k}) : ~fenced[s]
    /\ IF k = NoKey THEN TRUE ELSE entry[k] \in {0, p}
    /\ rows' = [rows EXCEPT ![p] = k]
    /\ entry' = [x \in Keys |-> IF x = k THEN p ELSE IF x = rows[p] /\ entry[x] = p THEN 0 ELSE entry[x]]
    /\ UNCHANGED <<conflict, ver, fenced, acked, decided, inflight, cursor, pending, phase>>

(* The steps to versions 1, 2 and 3: every shard fences, the coordinator
   decides once it counts every shard fenced and drained, then each shard
   publishes the version and lifts its fence. *)
Fence(s) ==
    /\ phase \in {"to1", "to2", "to3"} /\ ~decided /\ ~acked[s]
    /\ fenced' = [fenced EXCEPT ![s] = TRUE]
    /\ acked' = [acked EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<rows, entry, conflict, ver, decided, inflight, cursor, pending, phase>>
LeaderChange(s) ==
    /\ BugVolatileFence /\ fenced[s]
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ UNCHANGED <<rows, entry, conflict, ver, acked, decided, inflight, cursor, pending, phase>>
Decide ==
    /\ phase \in {"to1", "to2", "to3"} /\ ~decided
    /\ \A s \in Shards : acked[s]
    /\ \/ \A p \in Pks : ~inflight[p].active
       \/ phase = "to2" /\ BugNoDrainToEnforcing
       \/ phase = "to3" /\ BugNoDrainToAbort
    /\ phase # "to2" \/ cursor = Done \/ BugEarlyPublish
    \* (the step to 2 fails on a conflict, and takes the step to 3)
    /\ IF phase = "to2" /\ conflict # {} /\ ~BugPublishConflicts
       THEN phase' = "to3" /\ UNCHANGED decided
       ELSE decided' = TRUE /\ UNCHANGED phase
    /\ UNCHANGED <<rows, entry, conflict, ver, fenced, acked, inflight, cursor, pending>>
Publish(s) ==
    /\ decided /\ ver[s] < Target
    /\ ver' = [ver EXCEPT ![s] = Target]
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ IF Target = 3
       THEN /\ entry' = [k \in Keys |-> IF KeyShard(k) = s THEN 0 ELSE entry[k]]
            /\ conflict' = {c \in conflict : KeyShard(c[1]) # s}
       ELSE UNCHANGED <<entry, conflict>>
    /\ IF \A x \in Shards \ {s} : ver[x] = Target
       THEN /\ decided' = FALSE /\ acked' = [x \in Shards |-> FALSE]
            /\ phase' = CASE phase = "to1" -> "backfill" [] phase = "to2" -> "enforcing"
                          [] OTHER -> "aborted"
       ELSE UNCHANGED <<decided, acked, phase>>
    /\ UNCHANGED <<rows, inflight, cursor, pending>>
BeginEnforcing ==
    /\ phase = "backfill" /\ (cursor = Done \/ BugEarlyPublish)
    /\ phase' = "to2"
    /\ UNCHANGED <<rows, entry, conflict, ver, fenced, acked, decided, inflight, cursor, pending>>
Cancel ==
    /\ phase = "backfill"
    /\ phase' = "to3" /\ pending' = NoRead
    /\ UNCHANGED <<rows, entry, conflict, ver, fenced, acked, decided, inflight, cursor>>

(* The backfill, through the commit protocol: a read of the row at its
   cursor, then a write of version 1 of the row's entry (or conflict). The
   write commits only if the row still has the key it read and no write is
   prepared on it; otherwise the backfill reads the row again. A row that
   already has its entry or conflict there is left as it is. *)
BackfillRead ==
    /\ phase = "backfill" /\ cursor # Done /\ pending = NoRead
    /\ pending' = [pk |-> cursor, key |-> rows[cursor]]
    /\ UNCHANGED <<rows, entry, conflict, ver, fenced, acked, decided, inflight, cursor, phase>>
BackfillWrite ==
    LET p == pending.pk
        k == pending.key
    IN /\ phase \in {"backfill", "to2"} /\ pending # NoRead
       /\ \A s \in {RowShard(p)} \cup KeyShards({k}) : ~fenced[s] /\ ver[s] = 1
       /\ IF (rows[p] = k /\ ~inflight[p].active) \/ BugBackfillUnvalidated
          THEN LET st == R!Joined(State, p, k) IN
               /\ entry' = st.entry /\ conflict' = st.conflict
               /\ cursor' = cursor + 1
          ELSE UNCHANGED <<entry, conflict, cursor>>
       /\ pending' = NoRead
       /\ UNCHANGED <<rows, ver, fenced, acked, decided, inflight, phase>>

Next ==
    \/ \E p \in Pks, k \in KeyOf, gv \in {0, 1, 3} : Prepare(p, k, gv)
    \/ \E p \in Pks : Commit(p)
    \/ \E p \in Pks, k \in KeyOf : WriteEnforcing(p, k)
    \/ \E s \in Shards : Fence(s) \/ Publish(s) \/ LeaderChange(s)
    \/ Decide \/ BeginEnforcing \/ Cancel \/ BackfillRead \/ BackfillWrite
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ rows \in [Pks -> KeyOf] /\ entry \in [Keys -> {0} \cup Pks]
    /\ conflict \subseteq Keys \X Pks
    /\ ver \in [Shards -> 0..3] /\ fenced \in [Shards -> BOOLEAN]
    /\ acked \in [Shards -> BOOLEAN] /\ decided \in BOOLEAN
    /\ inflight \in [Pks -> [active : BOOLEAN, ver : {0, 1, 3}, key : KeyOf]]
    /\ cursor \in Pks \cup {Done}
    /\ pending \in {NoRead} \cup [pk : Pks, key : KeyOf]
    /\ phase \in Phases
Live == phase \in {"to1", "backfill", "to2", "enforcing"} \/ (phase = "to3" /\ ~decided)
NoStale ==
    Live =>
        /\ \A k \in Keys : entry[k] # 0 => rows[entry[k]] = k
        /\ \A c \in conflict : rows[c[2]] = c[1]
WellFormed ==
    \A k \in Keys :
        /\ entry[k] = 0 => \A p \in Pks : <<k, p>> \notin conflict
        /\ entry[k] # 0 => <<k, entry[k]>> \notin conflict
Enforced ==
    (\E s \in Shards : ver[s] = 2) =>
        /\ conflict = {}
        /\ \A p \in Pks : rows[p] # NoKey => entry[rows[p]] = p
Dropped ==
    \A k \in Keys : ver[KeyShard(k)] = 3 =>
        /\ entry[k] = 0
        /\ \A p \in Pks : <<k, p>> \notin conflict
=============================================================================
