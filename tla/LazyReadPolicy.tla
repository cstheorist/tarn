-------------------------- MODULE LazyReadPolicy --------------------------
(* Local policy refinement, not a new snapshot/freshness algorithm. Base
   row 2 is committed; the lazy copy initially has only obsolete row 1.
   An own committed row may await delivery. Policy changes retain that
   obligation; causal catch-up is distinct from ordinary strong SQL. *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugIgnoreStrong, BugForgetOnSwitch
VARIABLES policy, transaction, sticky, causal, obligation, confirmed,
          indexRows, path, required, rows
vars == <<policy, transaction, sticky, causal, obligation, confirmed,
          indexRows, path, required, rows>>
Base == {2}
Init == /\ policy \in {"strong", "eventual"}
        /\ transaction \in BOOLEAN /\ sticky \in BOOLEAN /\ causal \in BOOLEAN
        /\ obligation = TRUE /\ confirmed = FALSE /\ indexRows = {1}
        /\ path = "none" /\ required = FALSE /\ rows = {}
Switch ==
    /\ policy' = IF policy = "strong" THEN "eventual" ELSE "strong"
    /\ obligation' = IF BugForgetOnSwitch THEN FALSE ELSE obligation
    /\ UNCHANGED <<transaction, sticky, causal, confirmed, indexRows, path, required, rows>>
Deliver ==
    /\ ~confirmed
    /\ confirmed' = TRUE /\ indexRows' = {1, 2}
    /\ UNCHANGED <<policy, transaction, sticky, causal, obligation, path, required, rows>>
Read ==
    /\ path = "none"
    /\ LET strong == transaction \/ sticky \/ (policy = "strong" /\ ~BugIgnoreStrong)
           timeout == ~strong /\ causal /\ obligation /\ ~confirmed
       IN /\ required' = (transaction \/ sticky \/ policy = "strong")
          /\ path' = IF timeout THEN "timeout" ELSE IF strong THEN "strong" ELSE "eventual"
          /\ rows' = IF timeout THEN {} ELSE IF strong THEN Base ELSE indexRows \cap Base
          /\ obligation' = IF ~strong /\ causal /\ ~timeout THEN FALSE ELSE obligation
    /\ UNCHANGED <<policy, transaction, sticky, causal, confirmed, indexRows>>
Next == Switch \/ Deliver \/ Read
Spec == Init /\ [][Next]_vars
TypeOK == /\ policy \in {"strong", "eventual"}
          /\ transaction \in BOOLEAN /\ sticky \in BOOLEAN /\ causal \in BOOLEAN
          /\ obligation \in BOOLEAN /\ confirmed \in BOOLEAN /\ required \in BOOLEAN
          /\ indexRows \subseteq {1,2} /\ rows \subseteq {1,2}
          /\ path \in {"none", "timeout", "strong", "eventual"}
StrongReadIsStrong == required => path = "strong"
PendingRetained == obligation \/ confirmed
CausalReadComplete == (path = "eventual" /\ causal) => rows = Base
RecheckedRows == rows \subseteq Base
=============================================================================
