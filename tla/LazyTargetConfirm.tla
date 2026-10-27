------------------------- MODULE LazyTargetConfirm -------------------------
(* One committed assignment remains in the source outbox after the first
   target CALL loses its receipt. A duplicate must not use that same CALL's
   unconfirmed marker/retirement as an ACK proof: the first native commit
   can still roll back. This refines LazyIndex's durable-target premise. *)
EXTENDS Naturals
CONSTANT BugUnconfirmedAck
Proofs == {"marker", "retired"}
VARIABLES first, provisional, durable, retained, reply
vars == <<first, provisional, durable, retained, reply>>
Init == /\ first = "idle" /\ provisional = "none" /\ durable = "none"
        /\ retained = TRUE /\ reply = "none"
Start(proof) ==
    /\ first = "idle" /\ proof \in Proofs
    /\ first' = "pending" /\ provisional' = proof
    /\ UNCHANGED <<durable, retained, reply>>
RetryPending ==
    /\ first = "pending" /\ reply = "none"
    /\ reply' = IF BugUnconfirmedAck THEN "ack" ELSE "retry"
    /\ UNCHANGED <<first, provisional, durable, retained>>
Finish(committed) ==
    /\ first = "pending" /\ committed \in BOOLEAN
    /\ first' = "resolved"
    /\ durable' = IF committed THEN provisional ELSE "none"
    /\ UNCHANGED <<provisional, retained, reply>>
(* After a definite failure, the source can retry its retained assignment.
   An absent confirmed proof is replaced by a newly CONFIRMED target write;
   an existing committed marker or retirement is already sufficient. *)
RetryConfirmed ==
    /\ first = "resolved" /\ retained /\ reply # "ack"
    /\ durable' = IF durable = "none" THEN "marker" ELSE durable
    /\ reply' = "ack"
    /\ UNCHANGED <<first, provisional, retained>>
DeleteSource ==
    /\ retained /\ reply = "ack"
    /\ retained' = FALSE
    /\ UNCHANGED <<first, provisional, durable, reply>>
Next == RetryPending \/ RetryConfirmed \/ DeleteSource
        \/ (\E proof \in Proofs : Start(proof))
        \/ (\E committed \in BOOLEAN : Finish(committed))
Spec == Init /\ [][Next]_vars
TypeOK == /\ first \in {"idle", "pending", "resolved"}
          /\ provisional \in Proofs \cup {"none"} /\ durable \in Proofs \cup {"none"}
          /\ retained \in BOOLEAN /\ reply \in {"none", "retry", "ack"}
(* The control reaches ACK -> source deletion -> native rollback, leaving
   neither a source obligation nor any durable target effect. *)
DrainedObligationSafe == retained \/ durable \in Proofs \/ first = "pending"
=============================================================================
