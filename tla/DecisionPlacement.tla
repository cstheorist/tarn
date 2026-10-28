-------------------------- MODULE DecisionPlacement --------------------------
(* Local premises of transaction-identity-based decision placement. Hashing *)
(* is abstracted by any choice in the eligible set: Rust tests check the    *)
(* canonical ordering and spread. This does not model transaction outcomes; *)
(* Placement_* checks those in TarnCommit with another stable writer.       *)
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS BugReadOnlyDecision, BugIgnoreLeaving, BugReselect
Shards == {1, 2, 3}
None == 0
VARIABLES writers, readers, active, frozen, selected, durable, lookup,
          phase, changed
vars == <<writers, readers, active, frozen, selected, durable, lookup,
          phase, changed>>
Eligible(ws, members) == IF ws \cap members # {} THEN ws \cap members ELSE ws

Init ==
    /\ writers \in (SUBSET Shards) \ {{} }
    /\ readers \in SUBSET (Shards \ writers)
    /\ active \in SUBSET Shards
    /\ frozen = {} /\ selected = None /\ durable = None /\ lookup = None
    /\ phase = "new" /\ changed = FALSE

Freeze ==
    /\ phase = "new"
    /\ frozen' = Eligible(writers, active)
    /\ selected' \in IF BugReadOnlyDecision THEN writers \cup readers
                    ELSE IF BugIgnoreLeaving THEN writers ELSE frozen'
    /\ phase' = "frozen"
    /\ UNCHANGED <<writers, readers, active, durable, lookup, changed>>

MembershipChange ==
    /\ phase \in {"frozen", "prepared"} /\ ~changed
    /\ active' \in SUBSET Shards /\ changed' = TRUE
    /\ UNCHANGED <<writers, readers, frozen, selected, durable, lookup, phase>>

Prepare ==
    /\ phase = "frozen"
    /\ IF selected \in active
       THEN /\ durable' = selected /\ phase' = "prepared"
       ELSE /\ durable' = None /\ phase' = "refused"
    /\ UNCHANGED <<writers, readers, active, frozen, selected, lookup, changed>>

Recover ==
    /\ phase = "prepared"
    /\ lookup' \in IF BugReselect THEN Eligible(writers, active) ELSE {durable}
    /\ phase' = "recovered"
    /\ UNCHANGED <<writers, readers, active, frozen, selected, durable, changed>>

Next == Freeze \/ MembershipChange \/ Prepare \/ Recover
Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ writers \in (SUBSET Shards) \ {{} }
    /\ readers \subseteq Shards \ writers /\ active \subseteq Shards
    /\ frozen \subseteq writers
    /\ selected \in Shards \cup {None} /\ durable \in Shards \cup {None}
    /\ lookup \in Shards \cup {None}
    /\ phase \in {"new", "frozen", "prepared", "refused", "recovered"}
    /\ changed \in BOOLEAN
WritingParticipant == selected # None => selected \in writers
ActivePreferred == selected # None => selected \in frozen
DurableIdentity == durable # None => durable = selected
RecordedRecovery == lookup # None => lookup = durable
=============================================================================
