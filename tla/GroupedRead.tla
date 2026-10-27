---------------------------- MODULE GroupedRead ----------------------------
(* Refinement premise for a grouped local read (issue #83): every table  *)
(* is read from ONE MVCC snapshot, even when a scan yields between them. *)
(* A local writer atomically changes both tables. Separate snapshots can *)
(* return a vector that never existed during the RPC. This small model   *)
(* does not model lock-history implementation or claim RC transaction    *)
(* serializability; GroupedTables checks the protocol's union lock guard. *)
EXTENDS Naturals

CONSTANT BugSeparateSnapshots
ASSUME BugSeparateSnapshots \in BOOLEAN

Keys == {"outer", "inner"}
Values == 0..1
Unread == 2

VARIABLES data, writerDone, phase, snapshot, result, seen
vars == <<data, writerDone, phase, snapshot, result, seen>>

Init ==
    /\ data = [k \in Keys |-> 0]
    /\ writerDone = FALSE
    /\ phase = "idle"
    /\ snapshot = [k \in Keys |-> Unread]
    /\ result = [k \in Keys |-> Unread]
    /\ seen = {}

BeginRead ==
    /\ phase = "idle"
    /\ phase' = "outer"
    /\ snapshot' = data
    /\ seen' = {data}
    /\ UNCHANGED <<data, writerDone, result>>

ReadOuter ==
    /\ phase = "outer"
    /\ result' = [result EXCEPT !["outer"] =
                     IF BugSeparateSnapshots THEN data["outer"] ELSE snapshot["outer"]]
    /\ phase' = "inner"
    /\ UNCHANGED <<data, writerDone, snapshot, seen>>

ReadInner ==
    /\ phase = "inner"
    /\ result' = [result EXCEPT !["inner"] =
                     IF BugSeparateSnapshots THEN data["inner"] ELSE snapshot["inner"]]
    /\ phase' = "check"
    /\ UNCHANGED <<data, writerDone, snapshot, seen>>

FinishRead ==
    /\ phase = "check"
    /\ phase' = "done"
    /\ UNCHANGED <<data, writerDone, snapshot, result, seen>>

WriteBoth ==
    /\ ~writerDone
    /\ data' = [k \in Keys |-> 1]
    /\ writerDone' = TRUE
    /\ seen' = IF phase \in {"outer", "inner", "check"} THEN seen \cup {data'} ELSE seen
    /\ UNCHANGED <<phase, snapshot, result>>

Next == BeginRead \/ ReadOuter \/ ReadInner \/ FinishRead \/ WriteBoth
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ data \in [Keys -> Values]
    /\ writerDone \in BOOLEAN
    /\ phase \in {"idle", "outer", "inner", "check", "done"}
    /\ snapshot \in [Keys -> Values \cup {Unread}]
    /\ result \in [Keys -> Values \cup {Unread}]
    /\ seen \subseteq [Keys -> Values]

CoherentSnapshot == phase = "done" => result \in seen
=============================================================================
