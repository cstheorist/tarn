----------------------- MODULE StrongStreamCompletion -----------------------
(* Final validation of an already-certified stream (#130). The first X read
 * has a valid first-read certificate, but local A may miss a replacement's
 * writes and term while its observed writable flag remains true. Another
 * logical shard Y can expose the replacement writer's new value.
 *
 * A writer stores WAL on an RF3 write quorum before ACK. A separate owned
 * child transaction collects fresh RF3 read-quorum replies, catches up A,
 * and settles its sync queue. It never replaces the caller's transaction
 * or read set. Existing MVCC probe/observed-term validation then rejects a
 * stale stream. Quorum coverage follows from intersection and observed WAL,
 * not from testing the desired committed-view invariant.
 *
 * The two controls skip the final prefix or restart the caller's read set
 * after it. Both must reach an actual committed X_old/Y_new observation.
 * Native MVCC invalidation, confirmed WAL/queue semantics, first-read
 * certification, healthy clock/configuration and stable in-flight quorum
 * guarantees are trusted premises, not implementations in this model.
 *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugSkipFinalPrefix, BugDropReadset
Replicas == {"A", "B", "C"}
AllLinks == Replicas \X Replicas
Graph(p) == CASE p = 0 -> AllLinks
             [] p = 1 -> {e \in AllLinks : e[1] # "A" /\ e[2] # "A"} \cup {<<"A", "A">>}
             [] p = 2 -> {e \in AllLinks : e[2] # "A" \/ e[1] = "A"}
             [] OTHER -> {e \in AllLinks : e[1] # "A" \/ e[2] = "A"}
Max(a, b) == IF a >= b THEN a ELSE b

VARIABLES wal, acked, confirmed, y, net, healed, r, b
vars == <<wal, acked, confirmed, y, net, healed, r, b>>
Talk(a, c) == <<a, c>> \in net /\ <<c, a>> \in net
Writers == {a \in Replicas : wal[a] = 1}

Init ==
    /\ wal = [a \in Replicas |-> 0] /\ acked = FALSE
    /\ confirmed = 0 /\ y = 0
    /\ net \in {Graph(p) : p \in 0..3} /\ healed = FALSE
    /\ r = [phase |-> "start", tx |-> 1, entryTerm |-> 1, observedTerm |-> 1,
             hasRead |-> FALSE, trackX |-> 0, invalid |-> FALSE,
             gotX |-> 2, gotY |-> 2, outcome |-> "none"]
    /\ b = [phase |-> "idle", replies |-> {}, max |-> 0, ackAtStart |-> FALSE]

ReadX ==
    /\ r.phase = "start"
    /\ r' = [r EXCEPT !.phase = "first", !.gotX = confirmed,
                     !.trackX = confirmed, !.hasRead = TRUE]
    /\ UNCHANGED <<wal, acked, confirmed, y, net, healed, b>>

Store(a) ==
    /\ r.phase # "start" /\ ~acked /\ wal[a] = 0 /\ Talk("B", a)
    /\ wal' = [wal EXCEPT ![a] = 1]
    /\ UNCHANGED <<acked, confirmed, y, net, healed, r, b>>

AckWriter ==
    /\ ~acked /\ Cardinality(Writers) >= 2
    /\ acked' = TRUE /\ y' = 1
    /\ UNCHANGED <<wal, confirmed, net, healed, r, b>>

ReadY ==
    /\ r.phase = "first"
    /\ r' = [r EXCEPT !.phase = "final", !.gotY = y]
    /\ UNCHANGED <<wal, acked, confirmed, y, net, healed, b>>

Heal ==
    /\ ~healed /\ net # AllLinks /\ net' = AllLinks /\ healed' = TRUE
    /\ UNCHANGED <<wal, acked, confirmed, y, r, b>>

StartFence ==
    /\ r.phase = "final" /\ b.phase = "idle"
    /\ b' = [b EXCEPT !.phase = IF BugSkipFinalPrefix THEN "skipped" ELSE "collect",
                     !.ackAtStart = acked]
    /\ r' = IF BugSkipFinalPrefix THEN [r EXCEPT !.phase = "validate"] ELSE r
    /\ UNCHANGED <<wal, acked, confirmed, y, net, healed>>

Reply(a) ==
    /\ b.phase = "collect" /\ a \notin b.replies /\ Talk("A", a)
    /\ b' = [b EXCEPT !.replies = @ \cup {a}, !.max = Max(@, wal[a])]
    /\ UNCHANGED <<wal, acked, confirmed, y, net, healed, r>>

Quorum ==
    /\ b.phase = "collect" /\ Cardinality(b.replies) >= 2
    /\ b' = [b EXCEPT !.phase = "catchup"]
    /\ UNCHANGED <<wal, acked, confirmed, y, net, healed, r>>

CatchUp ==
    /\ b.phase = "catchup"
    /\ wal' = [wal EXCEPT !["A"] = Max(@, b.max)]
    /\ b' = [b EXCEPT !.phase = "settle"]
    /\ UNCHANGED <<acked, confirmed, y, net, healed, r>>

Settle ==
    /\ b.phase = "settle" /\ (wal["A"] = 0 \/ acked)
    /\ confirmed' = wal["A"]
    /\ r' = [r EXCEPT !.invalid = @ \/ (r.hasRead /\ r.trackX # wal["A"])]
    /\ b' = [b EXCEPT !.phase = "done"]
    /\ UNCHANGED <<wal, acked, y, net, healed>>

FinishFence ==
    /\ r.phase = "final" /\ b.phase = "done"
    /\ r' = IF BugDropReadset
             THEN [r EXCEPT !.phase = "validate", !.tx = 3,
                            !.entryTerm = r.observedTerm, !.hasRead = FALSE,
                            !.trackX = confirmed, !.invalid = FALSE]
             ELSE [r EXCEPT !.phase = "validate"]
    /\ UNCHANGED <<wal, acked, confirmed, y, net, healed, b>>

Validate ==
    /\ r.phase = "validate"
    /\ r' = [r EXCEPT !.phase = "done",
                     !.outcome = IF ~r.invalid /\ r.entryTerm = r.observedTerm
                                 THEN "commit" ELSE "refuse"]
    /\ UNCHANGED <<wal, acked, confirmed, y, net, healed, b>>

(* Native application and local term observation can happen independently.
 * Neither is assumed to occur before the old leader tries to validate. *)
ApplyLocal ==
    /\ acked /\ confirmed < wal["A"]
    /\ confirmed' = wal["A"]
    /\ r' = [r EXCEPT !.invalid = @ \/ (r.hasRead /\ r.trackX # wal["A"])]
    /\ UNCHANGED <<wal, acked, y, net, healed, b>>
ObserveTerm ==
    /\ acked /\ r.observedTerm = 1
    /\ r' = [r EXCEPT !.observedTerm = 2]
    /\ UNCHANGED <<wal, acked, confirmed, y, net, healed, b>>

Next == ReadX \/ (\E a \in Replicas : Store(a)) \/ AckWriter \/ ReadY \/ Heal
        \/ StartFence \/ (\E a \in Replicas : Reply(a)) \/ Quorum \/ CatchUp
        \/ Settle \/ FinishFence \/ Validate \/ ApplyLocal \/ ObserveTerm
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ wal \in [Replicas -> 0..1] /\ acked \in BOOLEAN
    /\ confirmed \in 0..1 /\ y \in 0..1
    /\ net \in {Graph(p) : p \in 0..3} /\ healed \in BOOLEAN
    /\ r.phase \in {"start", "first", "final", "validate", "done"}
    /\ r.tx \in {1, 3} /\ r.entryTerm \in 1..2 /\ r.observedTerm \in 1..2
    /\ r.hasRead \in BOOLEAN /\ r.trackX \in 0..1 /\ r.invalid \in BOOLEAN
    /\ r.gotX \in 0..2 /\ r.gotY \in 0..2
    /\ r.outcome \in {"none", "commit", "refuse"}
    /\ b.phase \in {"idle", "skipped", "collect", "catchup", "settle", "done"}
    /\ b.replies \subseteq Replicas /\ b.max \in 0..1 /\ b.ackAtStart \in BOOLEAN
AckedHasQuorum == acked => Cardinality(Writers) >= 2
CollectedCoversStartAck ==
    b.ackAtStart /\ b.phase \in {"catchup", "settle", "done"} => b.max = 1
NoFracturedCommit == r.outcome = "commit" => r.gotX = r.gotY
=============================================================================
