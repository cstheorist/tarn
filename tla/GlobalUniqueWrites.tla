------------------------- MODULE GlobalUniqueWrites -------------------------
(* The writes to a table while its global unique index builds: version 1 of
   GlobalUniqueBuild (issue #292), as transactions. A transaction's
   statement decides the index's entries and conflicts (GlobalUniqueRules)
   from the transaction's view: the committed rows and index, overlaid with
   the transaction's own earlier writes. It buffers its writes, which the
   commit applies. Its decision still holds at the commit only if what it
   read is unchanged by then: the row, and the entry and conflicts of the
   row's old and new keys. The commit protocol's PREPARE validates these
   reads and locks them until the COMMIT. Under READ COMMITTED a statement
   locks them as it reads them instead, which allows fewer interleavings
   still. A transaction whose validation fails is retried: no write of the
   building version fails with 1062.

   One transaction of MaxStmts statements runs beside the backfill and the
   writes of other transactions, each of which commits as one step, on Rows
   rows that take any keys of two (or none), from any initial table. The
   backfill pages through the rows in pk order, with up to MaxInFlight
   pages in flight (issue #303): it reads the next row, then writes its
   entry or conflict, which commits only if the row is unchanged and the
   prepared transaction locks neither the row nor its key; otherwise it
   reads the row again. Its cursor, which the job stores, passes the first
   page in flight once that page's write commits, so it never passes a page
   that has not; a server that takes the job over (Takeover) reads on from
   the stored cursor. The server that had the job learns that it lost it
   only when it next stores the cursor, which it no longer can: until it
   stops (OrphanStop), up to MaxOrphans of the pages it had in flight go
   on, written as any page is, but its cursor is never stored again. (A
   page it reads after the takeover meets the others as one it had in
   flight does: as one whose row changed since it was read, and which it
   reads again. The shards of a table page at once too: their rows differ,
   so two of their pages meet as two pages of one shard do, over the entry
   or conflicts of a key two rows share.)

   NoStale and WellFormed: as in GlobalUniqueBuild.
   Passed: every row the backfill's stored cursor has passed holds its
     key's entry or is among its conflicts, unless a page in flight is
     writing it or the prepared transaction, whose commit leaves it so, has
     locked it; whatever the pages of the server that lost the job write.
     GlobalUniqueBuild's step to the enforcing version waits until the
     cursor has passed every row.
   Serialized: a prepared transaction's writes are those its statements
     would make if they ran now, as one step. GlobalUniqueBuild takes a
     write's effects as one step at its commit. *)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS Rows,                   \* the table's rows
          MaxStmts,               \* statements in the transaction
          BugKeyUnserialized,     \* a statement's reads of the index are neither validated nor locked
          BugNoOverlay,           \* a statement reads the committed index, not the transaction's writes
          MaxInFlight,            \* pages of the backfill in flight at once
          MaxOrphans,             \* pages a server that lost the job keeps going
          BugOrphanUnvalidated,   \* that server's writes commit without checking their row
          BugCursorEarly,         \* the backfill's cursor passes a row when it reads it
          BugCursorOutOfOrder,    \* the backfill's cursor passes a written page before an earlier one
          BugSkipConflict, BugNoPromote, BugBlindLeave, BugJoinReplace  \* GlobalUniqueRules

Pks == 1..Rows
Done == Rows + 1                  \* the backfill's cursor once it has passed every row
Keys == {1, 2}
NoKey == 0                        \* no row, or a key with a NULL part: no entry
KeyOf == Keys \cup {NoKey}
Untouched == 3                    \* the transaction has not written the row (no key)
Same == Rows + 1                  \* nor the key's entry (no pk)
Page == [pk : Pks, key : KeyOf, st : {"read", "written", "stale"}]

R == INSTANCE GlobalUniqueRules

VARIABLES rows,      \* pk -> its row's key
          entry,     \* key -> the pk whose entry holds it (0: none)
          conflict,  \* <<key, pk>> pairs: rows with a key another row's entry holds
          tx,        \* the transaction: its writes, what it read, its statements
          cursor,    \* the pk the backfill's stored cursor is at: it has passed those below
          next,      \* the next pk the backfill reads
          inflight,  \* its pages in flight, in pk order: the row, the key it read, and
                     \* whether its write is to come ("read"), committed ("written"), or
                     \* failed, the row to be read again ("stale")
          orphans    \* the pages of the server that lost the job, still going
ovars == <<orphans>>
vars == <<rows, entry, conflict, tx, cursor, next, inflight, orphans>>

(* The transaction: st, idle, exec or prepared; rw, its writes of rows; ew,
   of entries; cw, of conflicts: none (0), added (1) or deleted (2); keys,
   the keys whose entry and conflicts it read; stmts, its statements, as
   <<pk, key>>; stale, its validation fails. *)
NoTx == [st |-> "idle", rw |-> [p \in Pks |-> Untouched], ew |-> [k \in Keys |-> Same],
         cw |-> [c \in Keys \X Pks |-> 0], keys |-> {}, stmts |-> <<>>, stale |-> FALSE]
State == [entry |-> entry, conflict |-> conflict]
TxRows == {p \in Pks : tx.rw[p] # Untouched}
VRows == [p \in Pks |-> IF tx.rw[p] = Untouched THEN rows[p] ELSE tx.rw[p]]
VState == [entry |-> [k \in Keys |-> IF tx.ew[k] = Same THEN entry[k] ELSE tx.ew[k]],
           conflict |-> {c \in Keys \X Pks : tx.cw[c] = 1 \/ (tx.cw[c] = 0 /\ c \in conflict)}]
(* The keys of the index a write moving a row from key o to key n reads. *)
ReadKeys(o, n) == IF o = n THEN {} ELSE {o, n} \ {NoKey}
(* The keys whose entry or conflicts st changes. *)
Changed(st) ==
    {k \in Keys : st.entry[k] # entry[k]
                  \/ \E p \in Pks : (<<k, p>> \in st.conflict) # (<<k, p>> \in conflict)}
(* What the prepared transaction locks. *)
LockedRows == IF tx.st = "prepared" THEN TxRows ELSE {}
LockedKeys == IF tx.st = "prepared" /\ ~BugKeyUnserialized THEN tx.keys ELSE {}
(* A commit that changes rows ps and keys ks fails the transaction's
   validation if the transaction read them. *)
Staled(ps, ks) ==
    IF tx.st = "exec" /\ (ps \cap TxRows # {} \/ (~BugKeyUnserialized /\ ks \cap tx.keys # {}))
    THEN [tx EXCEPT !.stale = TRUE] ELSE tx

Init ==
    /\ rows \in [Pks -> KeyOf]   \* the table's rows when version 1 is published
    /\ entry = [k \in Keys |-> 0] /\ conflict = {}
    /\ tx = NoTx
    /\ cursor = 1 /\ next = 1 /\ inflight = <<>>
    /\ orphans = {}

(* A statement of the transaction setting row p's key to n. *)
Stmt(p, n) ==
    LET o == VRows[p]
        base == IF BugNoOverlay THEN State ELSE VState
        st == R!Moved(base, p, o, n)
    IN /\ tx.st \in {"idle", "exec"} /\ Len(tx.stmts) < MaxStmts
       /\ tx' = [tx EXCEPT
                   !.st = "exec",
                   !.rw = [@ EXCEPT ![p] = n],
                   !.ew = [k \in Keys |-> IF st.entry[k] # base.entry[k] THEN st.entry[k] ELSE tx.ew[k]],
                   !.cw = [c \in Keys \X Pks |->
                             IF (c \in st.conflict) = (c \in base.conflict) THEN tx.cw[c]
                             ELSE IF c \in st.conflict THEN 1 ELSE 2],
                   !.keys = @ \cup ReadKeys(o, n),
                   !.stmts = Append(@, <<p, n>>)]
       /\ UNCHANGED <<rows, entry, conflict, cursor, next, inflight>> /\ UNCHANGED ovars
Prepare ==
    /\ tx.st = "exec" /\ ~tx.stale
    /\ tx' = [tx EXCEPT !.st = "prepared"]
    /\ UNCHANGED <<rows, entry, conflict, cursor, next, inflight>> /\ UNCHANGED ovars
Commit ==
    /\ tx.st = "prepared"
    /\ rows' = VRows /\ entry' = VState.entry /\ conflict' = VState.conflict
    /\ tx' = NoTx
    /\ UNCHANGED <<cursor, next, inflight>> /\ UNCHANGED ovars
Abort ==
    /\ tx.st = "exec"
    /\ tx' = NoTx
    /\ UNCHANGED <<rows, entry, conflict, cursor, next, inflight>> /\ UNCHANGED ovars

(* Another transaction's write, which commits as one step once the
   prepared transaction's locks let it. *)
Write(p, n) ==
    LET st == R!Moved(State, p, rows[p], n) IN
    /\ p \notin LockedRows /\ ReadKeys(rows[p], n) \cap LockedKeys = {}
    /\ rows' = [rows EXCEPT ![p] = n]
    /\ entry' = st.entry /\ conflict' = st.conflict
    /\ tx' = Staled({p}, Changed(st))
    /\ UNCHANGED <<cursor, next, inflight>> /\ UNCHANGED ovars

BackfillRead ==
    /\ next # Done /\ Len(inflight) < MaxInFlight
    /\ inflight' = Append(inflight, [pk |-> next, key |-> rows[next], st |-> "read"])
    /\ next' = next + 1
    /\ cursor' = IF BugCursorEarly THEN next + 1 ELSE cursor
    /\ UNCHANGED <<rows, entry, conflict, tx>> /\ UNCHANGED ovars
BackfillWrite(i) ==
    LET p == inflight[i].pk
        k == inflight[i].key
        st == R!Joined(State, p, k)
    IN /\ inflight[i].st = "read"
       /\ IF rows[p] = k /\ p \notin LockedRows /\ k \notin LockedKeys
          THEN /\ entry' = st.entry /\ conflict' = st.conflict
               /\ inflight' = [inflight EXCEPT ![i].st = "written"]
               /\ tx' = Staled({}, Changed(st))
          ELSE /\ inflight' = [inflight EXCEPT ![i].st = "stale"]
               /\ UNCHANGED <<entry, conflict, tx>>
       /\ UNCHANGED <<rows, cursor, next>> /\ UNCHANGED ovars
BackfillReread(i) ==
    /\ inflight[i].st = "stale"
    /\ inflight' = [inflight EXCEPT ![i].key = rows[inflight[i].pk], ![i].st = "read"]
    /\ UNCHANGED <<rows, entry, conflict, tx, cursor, next>> /\ UNCHANGED ovars
(* The cursor passes the first page in flight once its write has committed
   (with BugCursorOutOfOrder, any written page). *)
BackfillPass(i) ==
    /\ inflight[i].st = "written" /\ (i = 1 \/ BugCursorOutOfOrder)
    /\ cursor' = IF BugCursorEarly THEN cursor ELSE inflight[i].pk + 1
    /\ inflight' = SubSeq(inflight, 1, i - 1) \o SubSeq(inflight, i + 1, Len(inflight))
    /\ UNCHANGED <<rows, entry, conflict, tx, next>> /\ UNCHANGED ovars
(* Another server takes the job over: it reads on from the stored cursor.
   The server that had it keeps up to MaxOrphans of the pages it had in
   flight whose writes are to come. (One such server at a time.) *)
Takeover ==
    /\ inflight # <<>> /\ orphans = {}
    /\ \E kept \in SUBSET {inflight[i] : i \in 1..Len(inflight)} :
          /\ Cardinality(kept) <= MaxOrphans /\ \A o \in kept : o.st # "written"
          /\ orphans' = kept
    /\ inflight' = <<>> /\ next' = cursor
    /\ UNCHANGED <<rows, entry, conflict, tx, cursor>>
(* The server that lost the job, until it learns so: its pages write
   (validated as any page's write) and read again, and it stops at any
   time; none of it moves the stored cursor. *)
OrphanWrite(o) ==
    LET st == R!Joined(State, o.pk, o.key)
    IN /\ o.st = "read"
       /\ IF (rows[o.pk] = o.key \/ BugOrphanUnvalidated) /\ o.pk \notin LockedRows /\ o.key \notin LockedKeys
          THEN /\ entry' = st.entry /\ conflict' = st.conflict
               /\ orphans' = orphans \ {o}
               /\ tx' = Staled({}, Changed(st))
          ELSE /\ orphans' = (orphans \ {o}) \cup {[o EXCEPT !.st = "stale"]}
               /\ UNCHANGED <<entry, conflict, tx>>
       /\ UNCHANGED <<rows, cursor, next, inflight>>
OrphanReread(o) ==
    /\ o.st = "stale"
    /\ orphans' = (orphans \ {o}) \cup {[o EXCEPT !.key = rows[o.pk], !.st = "read"]}
    /\ UNCHANGED <<rows, entry, conflict, tx, cursor, next, inflight>>
OrphanStop ==
    /\ orphans # {} /\ orphans' = {}
    /\ UNCHANGED <<rows, entry, conflict, tx, cursor, next, inflight>>

Next ==
    \/ \E p \in Pks, n \in KeyOf : Stmt(p, n) \/ Write(p, n)
    \/ Prepare \/ Commit \/ Abort \/ BackfillRead \/ Takeover \/ OrphanStop
    \/ \E i \in 1..Len(inflight) : BackfillWrite(i) \/ BackfillReread(i) \/ BackfillPass(i)
    \/ \E o \in orphans : OrphanWrite(o) \/ OrphanReread(o)
Spec == Init /\ [][Next]_vars

(* The transaction's statements, run one after another from s as one step. *)
RECURSIVE Run(_, _)
Run(s, ss) ==
    IF ss = <<>> THEN s
    ELSE LET p == Head(ss)[1]
             n == Head(ss)[2]
             st == R!Moved([entry |-> s.entry, conflict |-> s.conflict], p, s.rows[p], n)
         IN Run([rows |-> [s.rows EXCEPT ![p] = n], entry |-> st.entry, conflict |-> st.conflict],
                Tail(ss))

TypeOK ==
    /\ rows \in [Pks -> KeyOf] /\ entry \in [Keys -> {0} \cup Pks]
    /\ conflict \subseteq Keys \X Pks
    /\ tx.st \in {"idle", "exec", "prepared"} /\ tx.stale \in BOOLEAN
    /\ tx.rw \in [Pks -> KeyOf \cup {Untouched}] /\ tx.ew \in [Keys -> {0, Same} \cup Pks]
    /\ tx.cw \in [Keys \X Pks -> 0..2] /\ tx.keys \subseteq Keys
    /\ Len(tx.stmts) <= MaxStmts
    /\ cursor \in Pks \cup {Done} /\ next \in Pks \cup {Done}
    /\ inflight \in Seq(Page) /\ Len(inflight) <= MaxInFlight
    /\ orphans \subseteq Page /\ Cardinality(orphans) <= MaxOrphans
NoStale ==
    /\ \A k \in Keys : entry[k] # 0 => rows[entry[k]] = k
    /\ \A c \in conflict : rows[c[2]] = c[1]
WellFormed ==
    \A k \in Keys :
        /\ entry[k] = 0 => \A p \in Pks : <<k, p>> \notin conflict
        /\ entry[k] # 0 => <<k, entry[k]>> \notin conflict
InFlight == {inflight[i].pk : i \in 1..Len(inflight)}
Passed ==
    \A p \in Pks : p < cursor /\ p \notin InFlight /\ p \notin LockedRows =>
        rows[p] = NoKey \/ entry[rows[p]] = p \/ <<rows[p], p>> \in conflict
Serialized ==
    tx.st = "prepared" =>
        Run([rows |-> rows, entry |-> entry, conflict |-> conflict], tx.stmts)
            = [rows |-> VRows, entry |-> VState.entry, conflict |-> VState.conflict]
=============================================================================
