-------------------------- MODULE GlobalUniqueBuild --------------------------
(* An online build of a global unique index (issue #292): a unique key
   without the shard key, whose entries live in a space of their own,
   sharded by the key (one entry per row whose key has no NULL part). The
   table's definition goes through three versions:
     0  without the index;
     1  with the index building (write-only): every write maintains its
        entries in the write's own transaction, and a row whose key another
        row's entry holds is recorded as a conflict, not refused, as
        MariaDB's online ADD UNIQUE refuses no concurrent write. When the
        holder leaves the key, a conflicting row takes the entry over;
     2  with the index enforcing: a write whose key another row's entry
        holds is refused (1062).
   A step to a new version fences every shard and waits for the writes in
   flight there (the drain) before any shard publishes it, as an ALTER's
   PREPARE and APPLY do. Between the two steps a backfill gives the rows
   written before version 1 their entries: it reads a row, and its write
   commits only if the row is unchanged (the commit protocol validates the
   read). The step to version 2 decides, once every shard is fenced and
   drained and the backfill has passed every row, to publish the index only
   if no conflict is recorded; otherwise the change fails (1062).

   Rows (primary keys 1 and 2) live on shards A and B, and a key's entry and
   conflicts on the key's shard. A write of version 0 or 1 prepares on every
   shard it touches -- the row's, and with version 1 those of its old and
   new keys -- which refuses a write made with an older version than its
   own, or while it is fenced; it commits later, with no further check (the
   commit protocol's PREPARE and COMMIT, TarnCommit). A row has one write
   in flight at a time. The commit protocol's serializability makes a
   write's reads of the entries and its effects one step at its commit.

   NoStale: every entry and conflict is its row's current key.
   WellFormed: a key with no entry has no conflict, and the holder of an
     entry is not among its key's conflicts.
   Enforced: once a shard has published version 2, every row with a key
     holds its key's entry, and no conflict is left. With NoStale, that is
     one row per key: the index is exact and the key unique. *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugBackfillUnvalidated, \* a backfill write commits a row read earlier, changed since
          BugSkipConflict,        \* a write of version 1 drops a row whose key another row's entry holds
          BugNoPromote,           \* the holder leaves its key, and no conflict takes the entry over
          BugPublishConflicts,    \* version 2 is published with conflicts left
          BugEarlyPublish,        \* the step to version 2 begins before the backfill passed every row
          BugNoDrainToBuilding,   \* the step to version 1 does not wait for the writes in flight
          BugNoDrainToEnforcing,  \* nor the step to version 2
          BugStaleWrite           \* a shard takes a write made with an older version than its own

Pks == {1, 2, 3}
Keys == {1, 2}
NoKey == 0                        \* no row, or a key with a NULL part: no entry
KeyOf == Keys \cup {NoKey}
Unread == 3                       \* the backfill has not read the row
Shards == {"A", "B"}
RowShard(p) == IF p = 2 THEN "B" ELSE "A"
KeyShards(ks) == {IF k = 1 THEN "A" ELSE "B" : k \in ks \ {NoKey}}
NoWrite == [active |-> FALSE, ver |-> 0, key |-> NoKey]
Phases == {"to1", "backfill", "to2", "enforcing", "failed"}

VARIABLES rows,      \* pk -> its row's key (NoKey: no row, or no entry)
          entry,     \* key -> the pk whose entry holds it (0: none)
          conflict,  \* <<key, pk>> pairs: rows with a key another row's entry holds
          ver,       \* shard -> the definition version it published
          fenced,    \* shard -> fenced for the step under way
          decided,   \* the step under way is decided: its shards may publish it
          inflight,  \* pk -> the write prepared on its row, not committed
          read,      \* pk -> the key the backfill read for its row (Unread: none)
          visited,   \* the pks the backfill has passed
          phase
vars == <<rows, entry, conflict, ver, fenced, decided, inflight, read, visited, phase>>
Target == IF phase = "to1" THEN 1 ELSE 2

Init ==
    /\ rows \in [Pks -> KeyOf]               \* the table's rows before the ALTER
    /\ entry = [k \in Keys |-> 0] /\ conflict = {}
    /\ ver = [s \in Shards |-> 0] /\ fenced = [s \in Shards |-> FALSE] /\ decided = FALSE
    /\ inflight = [p \in Pks |-> NoWrite]
    /\ read = [p \in Pks |-> Unread] /\ visited = {}
    /\ phase = "to1"

(* The index's entries and conflicts as a record, and the version 1 rules
   (uix::leave_key and uix::join_key in crates/tarn-verified): row p leaves
   key o, then joins key n. *)
State == [entry |-> entry, conflict |-> conflict]
Left(st, p, o) ==
    IF o = NoKey THEN st
    ELSE IF st.entry[o] = p THEN
        LET others == {q \in Pks : <<o, q>> \in st.conflict} IN
        IF others # {} /\ ~BugNoPromote
        THEN LET q == CHOOSE q \in others : TRUE IN
             [entry |-> [st.entry EXCEPT ![o] = q], conflict |-> st.conflict \ {<<o, q>>}]
        ELSE [entry |-> [st.entry EXCEPT ![o] = 0], conflict |-> st.conflict]
    ELSE [entry |-> st.entry, conflict |-> st.conflict \ {<<o, p>>}]
Joined(st, p, n) ==
    IF n = NoKey THEN st
    ELSE IF st.entry[n] = 0 THEN [entry |-> [st.entry EXCEPT ![n] = p], conflict |-> st.conflict]
    ELSE IF st.entry[n] = p \/ BugSkipConflict THEN st
    ELSE [entry |-> st.entry, conflict |-> st.conflict \cup {<<n, p>>}]
(* (A write that keeps its row's key writes no entry.) *)
Moved(p, o, n) == IF o = n THEN State ELSE Joined(Left(State, p, o), p, n)

(* A write of version gv (0: the old definition's, 1: the building one's)
   to row p, setting its key to k: it prepares on the shards it touches. *)
Touched(p, k, gv) == {RowShard(p)} \cup (IF gv >= 1 THEN KeyShards({rows[p], k}) ELSE {})
Takes(s, gv) == ~fenced[s] /\ (gv >= ver[s] \/ BugStaleWrite)
Prepare(p, k, gv) ==
    /\ phase \in {"to1", "backfill", "to2"}
    /\ ~inflight[p].active
    \* (a gateway writes with a version some shard has published)
    /\ \E s \in Shards : ver[s] >= gv
    /\ \A s \in Touched(p, k, gv) : Takes(s, gv)
    /\ inflight' = [inflight EXCEPT ![p] = [active |-> TRUE, ver |-> gv, key |-> k]]
    /\ UNCHANGED <<rows, entry, conflict, ver, fenced, decided, read, visited, phase>>
Commit(p) ==
    LET w == inflight[p]
        st == IF w.ver >= 1 THEN Moved(p, rows[p], w.key) ELSE State
    IN /\ w.active
       /\ rows' = [rows EXCEPT ![p] = w.key]
       /\ entry' = st.entry /\ conflict' = st.conflict
       /\ inflight' = [inflight EXCEPT ![p] = NoWrite]
       /\ UNCHANGED <<ver, fenced, decided, read, visited, phase>>

(* A write of the enforcing version, once a shard has published it: a key
   another row's entry holds refuses it (1062). As one step: no later step
   waits for it. *)
WriteEnforcing(p, k) ==
    /\ phase \in {"to2", "enforcing"} /\ \E s \in Shards : ver[s] = 2
    /\ ~inflight[p].active
    /\ \A s \in {RowShard(p)} \cup KeyShards({rows[p], k}) : ~fenced[s]
    /\ IF k = NoKey THEN TRUE ELSE entry[k] \in {0, p}
    /\ rows' = [rows EXCEPT ![p] = k]
    /\ entry' = [x \in Keys |-> IF x = k THEN p ELSE IF x = rows[p] /\ entry[x] = p THEN 0 ELSE entry[x]]
    /\ UNCHANGED <<conflict, ver, fenced, decided, inflight, read, visited, phase>>

(* The steps to versions 1 and 2: every shard fences, the coordinator
   decides once every shard is fenced and drained, then each shard
   publishes the version and lifts its fence. The step to version 2 decides
   to publish only once the backfill has passed every row, and only with no
   conflict left; otherwise the change fails. *)
Fence(s) ==
    /\ phase \in {"to1", "to2"} /\ ~decided /\ ~fenced[s] /\ ver[s] < Target
    /\ fenced' = [fenced EXCEPT ![s] = TRUE]
    /\ UNCHANGED <<rows, entry, conflict, ver, decided, inflight, read, visited, phase>>
Decide ==
    /\ phase \in {"to1", "to2"} /\ ~decided
    /\ \A s \in Shards : fenced[s]
    /\ (phase = "to1" /\ BugNoDrainToBuilding) \/ (phase = "to2" /\ BugNoDrainToEnforcing)
       \/ \A p \in Pks : ~inflight[p].active
    /\ phase = "to1" \/ visited = Pks \/ BugEarlyPublish
    /\ IF phase = "to2" /\ conflict # {} /\ ~BugPublishConflicts
       THEN /\ phase' = "failed" /\ fenced' = [s \in Shards |-> FALSE]
            /\ UNCHANGED <<decided>>
       ELSE /\ decided' = TRUE
            /\ UNCHANGED <<phase, fenced>>
    /\ UNCHANGED <<rows, entry, conflict, ver, inflight, read, visited>>
Publish(s) ==
    /\ decided /\ fenced[s]
    /\ ver' = [ver EXCEPT ![s] = Target]
    /\ fenced' = [fenced EXCEPT ![s] = FALSE]
    /\ IF \A x \in Shards \ {s} : ~fenced[x]
       THEN /\ decided' = FALSE
            /\ phase' = IF phase = "to1" THEN "backfill" ELSE "enforcing"
       ELSE UNCHANGED <<decided, phase>>
    /\ UNCHANGED <<rows, entry, conflict, inflight, read, visited>>
BeginEnforcing ==
    /\ phase = "backfill" /\ (visited = Pks \/ BugEarlyPublish)
    /\ phase' = "to2"
    /\ UNCHANGED <<rows, entry, conflict, ver, fenced, decided, inflight, read, visited>>

(* The backfill, through the commit protocol: a read of a row, then a write
   of version 1 of its entry (or conflict), which commits only if the row
   still has the key it read; otherwise it reads the row again. A row that
   already has its entry or conflict there is left as it is. *)
BackfillRead(p) ==
    /\ phase = "backfill" /\ p \notin visited /\ read[p] = Unread
    /\ read' = [read EXCEPT ![p] = rows[p]]
    /\ UNCHANGED <<rows, entry, conflict, ver, fenced, decided, inflight, visited, phase>>
BackfillWrite(p) ==
    LET k == read[p] IN
    /\ phase \in {"backfill", "to2"} /\ k # Unread
    /\ \A s \in {RowShard(p)} \cup KeyShards({k}) : ~fenced[s] /\ ver[s] = 1
    /\ IF rows[p] = k \/ BugBackfillUnvalidated
       THEN LET st == Joined(State, p, k) IN
            /\ entry' = st.entry /\ conflict' = st.conflict
            /\ visited' = visited \cup {p}
       ELSE UNCHANGED <<entry, conflict, visited>>
    /\ read' = [read EXCEPT ![p] = Unread]
    /\ UNCHANGED <<rows, ver, fenced, decided, inflight, phase>>

Next ==
    \/ \E p \in Pks, k \in KeyOf, gv \in {0, 1} : Prepare(p, k, gv)
    \/ \E p \in Pks : Commit(p) \/ BackfillRead(p) \/ BackfillWrite(p)
    \/ \E p \in Pks, k \in KeyOf : WriteEnforcing(p, k)
    \/ \E s \in Shards : Fence(s) \/ Publish(s)
    \/ Decide \/ BeginEnforcing
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ rows \in [Pks -> KeyOf] /\ entry \in [Keys -> {0} \cup Pks]
    /\ conflict \subseteq Keys \X Pks
    /\ ver \in [Shards -> 0..2] /\ fenced \in [Shards -> BOOLEAN] /\ decided \in BOOLEAN
    /\ inflight \in [Pks -> [active : BOOLEAN, ver : 0..1, key : KeyOf]]
    /\ read \in [Pks -> KeyOf \cup {Unread}] /\ visited \subseteq Pks
    /\ phase \in Phases
NoStale ==
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
=============================================================================
