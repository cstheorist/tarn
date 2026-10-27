-------------------------- MODULE PrepareConfirm --------------------------
(* Native-visibility refinement of TarnCommit!Revote. A first physical
   PREPARE may still roll back during its quorum wait. A duplicate YES
   receipt must certify a confirmed participant promise, not the pending
   transaction's provisional prepared record. The core protocol is unchanged. *)
EXTENDS Naturals
CONSTANT BugUnconfirmedPrepare
VARIABLES first, receipt, certified
vars == <<first, receipt, certified>>
Init == /\ first = "idle" /\ receipt = "none" /\ certified = FALSE
StartPrepare ==
    /\ first = "idle" /\ first' = "pending"
    /\ UNCHANGED <<receipt, certified>>
DuplicatePending ==
    /\ first = "pending" /\ receipt = "none"
    /\ receipt' = IF BugUnconfirmedPrepare THEN "yes" ELSE "retry"
    /\ UNCHANGED <<first, certified>>
FinishPrepare(committed) ==
    /\ first = "pending" /\ committed \in BOOLEAN
    /\ first' = IF committed THEN "confirmed" ELSE "rolledback"
    /\ UNCHANGED <<receipt, certified>>
DuplicateResolved ==
    /\ first \in {"confirmed", "rolledback"} /\ receipt # "yes"
    /\ receipt' = IF first = "confirmed" THEN "yes" ELSE "retry"
    /\ UNCHANGED <<first, certified>>
(* With no retained abort tombstone, a later same-identity PREPARE may
   establish a new CONFIRMED promise after the failed attempt. This action
   abstracts that successful native commit, not a new user mutation. *)
RetryAfterRollback ==
    /\ first = "rolledback" /\ receipt # "yes"
    /\ first' = "confirmed" /\ receipt' = "yes"
    /\ UNCHANGED certified
(* The coordinator's separate receipt-processing step consumes the YES.
   It must be entitled to rely on a durable participant promise NOW. *)
CertifyYes ==
    /\ receipt = "yes" /\ ~certified
    /\ certified' = TRUE
    /\ UNCHANGED <<first, receipt>>
Next == StartPrepare \/ DuplicatePending \/ DuplicateResolved
        \/ RetryAfterRollback \/ CertifyYes
        \/ (\E committed \in BOOLEAN : FinishPrepare(committed))
Spec == Init /\ [][Next]_vars
TypeOK == /\ first \in {"idle", "pending", "confirmed", "rolledback"}
          /\ receipt \in {"none", "yes", "retry"} /\ certified \in BOOLEAN
CertifiedYesDurable == certified => first = "confirmed"
=============================================================================
