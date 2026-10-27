-------------------------- MODULE LazyApplyConfirm --------------------------
(* A durable COMMITTED decision and YES exist before this model starts.
   One physical participant APPLY may still roll back while a duplicate
   observes its provisional `applied` record. LazyBarrier's own pending
   obligation can be released only by a CONFIRMED participant completion.
   This is a local native-visibility refinement, not a new commit protocol. *)
EXTENDS Naturals
CONSTANT BugUnconfirmedApply
VARIABLES first, applied, delivered, ownPending, reply, read
vars == <<first, applied, delivered, ownPending, reply, read>>
Init == /\ first = "idle" /\ applied = FALSE /\ delivered = FALSE
        /\ ownPending = TRUE /\ reply = "none" /\ read = "none"
StartApply ==
    /\ first = "idle"
    /\ first' = "pending"
    /\ UNCHANGED <<applied, delivered, ownPending, reply, read>>
DuplicatePending ==
    /\ first = "pending" /\ reply = "none"
    /\ reply' = IF BugUnconfirmedApply THEN "applied" ELSE "retry"
    /\ UNCHANGED <<first, applied, delivered, ownPending, read>>
FinishApply(committed) ==
    /\ first = "pending" /\ committed \in BOOLEAN
    /\ first' = "resolved" /\ applied' = committed
    /\ UNCHANGED <<delivered, ownPending, reply, read>>
(* An idempotent retry after native resolution either confirms the existing
   APPLY or completes it. The durable decision is never replayed as a new
   user mutation. Its reply follows native confirmation. *)
RetryConfirmed ==
    /\ first = "resolved" /\ ownPending /\ reply # "applied"
    /\ applied' = TRUE /\ reply' = "applied"
    /\ UNCHANGED <<first, delivered, ownPending, read>>
ReleaseOwnPending ==
    /\ ownPending /\ reply = "applied"
    /\ ownPending' = FALSE
    /\ UNCHANGED <<first, applied, delivered, reply, read>>
Deliver ==
    /\ applied /\ ~delivered
    /\ delivered' = TRUE
    /\ UNCHANGED <<first, applied, ownPending, reply, read>>
(* The source counter/outbox are read in one CONFIRMED view, then the
   target frontier is awaited BEFORE the index snapshot. Before physical
   APPLY confirms, that valid view has counter=0/pending=empty, so its
   barrier passes even though an own COMMITTED row is still missing. *)
LazyRead ==
    /\ ~ownPending /\ read = "none" /\ (~applied \/ delivered)
    /\ read' = IF applied THEN "row" ELSE "missing"
    /\ UNCHANGED <<first, applied, delivered, ownPending, reply>>
Next == StartApply \/ DuplicatePending \/ RetryConfirmed
        \/ ReleaseOwnPending \/ Deliver \/ LazyRead
        \/ (\E committed \in BOOLEAN : FinishApply(committed))
Spec == Init /\ [][Next]_vars
TypeOK == /\ first \in {"idle", "pending", "resolved"}
          /\ applied \in BOOLEAN /\ delivered \in BOOLEAN
          /\ ownPending \in BOOLEAN /\ reply \in {"none", "retry", "applied"}
          /\ read \in {"none", "row", "missing"}
OwnReadSafe == read # "missing"
=============================================================================
