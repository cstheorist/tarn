------------------------ MODULE ReplicatedVinylBuild ------------------------
(* A vinyl index that ALTER TABLE builds on a replicated shard (issues     *)
(* #263 and #254). One leader and one replica, synchronous replication     *)
(* with a quorum of two: the leader confirms a write once the replica      *)
(* holds it, and the replica commits the write when it applies the         *)
(* CONFIRM that follows it in the leader's log.                            *)
(*                                                                         *)
(* The Tarantool behaviour it rests on, abstracted:                        *)
(*  - A build reads the table as of its confirmed writes and maintains the *)
(*    new index for the writes made after it starts. A synchronous write   *)
(*    made before it and confirmed after it is in neither.                 *)
(*  - The leader writes the build's DDL row as soon as the build ends,     *)
(*    behind any write still waiting for its quorum and so ahead of that   *)
(*    write's CONFIRM (TXN_WAIT_SYNC).                                     *)
(*  - A replica applies the log in order. Applying the DDL row runs the    *)
(*    build, whose final dump waits until no write the replica applied     *)
(*    still waits for its CONFIRM; the rows after it wait meanwhile.       *)
(*                                                                         *)
(* Tarn's two protections: the build runs behind the table's fence, once   *)
(* its writes have drained (BugBuildBesidePending skips the drain and lets *)
(* table writes go on), and while it runs the leader logs a row only once  *)
(* every earlier write is confirmed (BugDDLAheadOfConfirm skips that).     *)
(* One row; a table write gives it the key that is the write's number.     *)
(* Leader changes, aborted writes and the leader's own final dump, which   *)
(* only waits, are not modelled.                                           *)
EXTENDS Integers, Sequences, FiniteSets

CONSTANTS TableWrites, OtherWrites, BugBuildBesidePending, BugDDLAheadOfConfirm
ASSUME /\ TableWrites \subseteq (Nat \ {0}) /\ OtherWrites \subseteq (Nat \ {0})
       /\ TableWrites \cap OtherWrites = {}
       /\ BugBuildBesidePending \in BOOLEAN /\ BugDDLAheadOfConfirm \in BOOLEAN

Writes == TableWrites \cup OtherWrites

VARIABLES
    wst,      \* write -> "idle", "logged" (waiting for its quorum) or "confirmed"
    log,      \* the leader's log: writes, CONFIRMs and the DDL row, in order
    build,    \* "none", "building" or "built" (its DDL row is logged)
    index,    \* the key the leader's new index holds for the row; -1 before the build
    applied,  \* how many log entries the replica has applied
    pending   \* writes the replica applied whose CONFIRM it has not applied
vars == <<wst, log, build, index, applied, pending>>

Entries == [t : {"write", "confirm"}, w : Writes] \cup [t : {"ddl"}]
Max(S) == CHOOSE x \in S : \A y \in S : y <= x

\* The row's key as of the confirmed table writes: the last one's, in log order.
Committed ==
    LET done == {i \in 1..Len(log) : log[i].t = "write" /\ log[i].w \in TableWrites
                                      /\ wst[log[i].w] = "confirmed"}
    IN IF done = {} THEN 0 ELSE log[Max(done)].w

\* A write still waiting for its quorum is ahead of any row logged now.
Unconfirmed == {w \in Writes : wst[w] = "logged"}

Init ==
    /\ wst = [w \in Writes |-> "idle"]
    /\ log = <<>>
    /\ build = "none"
    /\ index = -1
    /\ applied = 0
    /\ pending = {}

\* A write executes and reaches the log. Behind the table's fence no table
\* write runs while the build does; while it runs, a write is logged only
\* once the ones before it are confirmed (replication_synchro_queue_max_size).
Write(w) ==
    /\ wst[w] = "idle"
    /\ (w \in TableWrites /\ build = "building") => BugBuildBesidePending
    /\ (build = "building" /\ ~BugDDLAheadOfConfirm) => Unconfirmed = {}
    /\ wst' = [wst EXCEPT ![w] = "logged"]
    /\ log' = Append(log, [t |-> "write", w |-> w])
    \* The new index takes the writes made after the build starts.
    /\ index' = IF w \in TableWrites /\ build # "none" THEN w ELSE index
    /\ UNCHANGED <<build, applied, pending>>

\* The first unconfirmed write is confirmed once the replica holds it.
Confirm ==
    \E i \in 1..Len(log) :
        /\ log[i].t = "write" /\ wst[log[i].w] = "logged"
        /\ \A j \in 1..(i - 1) : log[j].t = "write" => wst[log[j].w] # "logged"
        /\ applied >= i
        /\ wst' = [wst EXCEPT ![log[i].w] = "confirmed"]
        /\ log' = Append(log, [t |-> "confirm", w |-> log[i].w])
        /\ UNCHANGED <<build, index, applied, pending>>

\* The build reads the confirmed table, once the table's writes have drained.
StartBuild ==
    /\ build = "none"
    /\ ~BugBuildBesidePending => \A w \in TableWrites : wst[w] # "logged"
    /\ build' = "building"
    /\ index' = Committed
    /\ UNCHANGED <<wst, log, applied, pending>>

\* The DDL row, logged once every earlier write is confirmed.
LogDDL ==
    /\ build = "building"
    /\ ~BugDDLAheadOfConfirm => Unconfirmed = {}
    /\ build' = "built"
    /\ log' = Append(log, [t |-> "ddl"])
    /\ UNCHANGED <<wst, index, applied, pending>>

\* The replica applies the next entry. Its build waits for its pending writes.
Apply ==
    /\ applied < Len(log)
    /\ LET e == log[applied + 1] IN
        /\ e.t = "ddl" => pending = {}
        /\ pending' = CASE e.t = "write" -> pending \cup {e.w}
                        [] e.t = "confirm" -> pending \ {e.w}
                        [] OTHER -> pending
    /\ applied' = applied + 1
    /\ UNCHANGED <<wst, log, build, index>>

Next == (\E w \in Writes : Write(w)) \/ Confirm \/ StartBuild \/ LogDDL \/ Apply
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ wst \in [Writes -> {"idle", "logged", "confirmed"}]
    /\ log \in Seq(Entries)
    /\ build \in {"none", "building", "built"}
    /\ index \in {-1, 0} \cup TableWrites
    /\ applied \in 0..Len(log)
    /\ pending \subseteq Writes

\* Once built, and with no table write waiting for its quorum, the leader's
\* index holds the row's committed key.
IndexComplete ==
    (build = "built" /\ \A w \in TableWrites : wst[w] # "logged") => index = Committed

\* The replica never faces a DDL row while a write it applied waits for a
\* CONFIRM: that CONFIRM is behind the DDL row, so it would wait for good.
ReplicaApplies ==
    ~(applied < Len(log) /\ log[applied + 1].t = "ddl" /\ pending # {})
=============================================================================
