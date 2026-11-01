--------------------------- MODULE ClockBootstrap ---------------------------
EXTENDS Naturals, FiniteSets

(* Initial timing-policy installation follows a confirmed shard marker.
   The marker is durable, but alone does not establish current join readiness.
   This is a finite publication-order model, not Tarantool checkpoint, Raft,
   clock-certification, or whole-program liveness verification. *)
CONSTANTS BugNoMarker, BugNoJoin
Replicas == {1, 2, 3}
VARIABLES joined, marker, policy, loaded
vars == <<joined, marker, policy, loaded>>

Init == /\ joined = {1}
        /\ marker = FALSE
        /\ policy = "absent"
        /\ loaded = FALSE

Join(i) == /\ i \in Replicas \ joined
           (* A joining checkpoint cannot finish while waiting for an
              unconfirmed synchronous transaction from the initial cut. *)
           /\ policy # "pending"
           /\ joined' = joined \cup {i}
           /\ UNCHANGED <<marker, policy, loaded>>

(* Schema setup can eventually attempt its marker even when its initial
   follow wait expires. Do not assume the marker implies all peers joined. *)
Mark == /\ ~marker
        /\ marker' = TRUE
        /\ UNCHANGED <<joined, policy, loaded>>

Install == /\ policy = "absent"
           /\ (marker \/ BugNoMarker)
           /\ (joined = Replicas \/ BugNoJoin)
           /\ policy' = "pending"
           /\ UNCHANGED <<joined, marker, loaded>>

Confirm == /\ policy = "pending" /\ Cardinality(joined) >= 2
           /\ policy' = "confirmed"
           /\ UNCHANGED <<joined, marker, loaded>>

Load == /\ ~loaded /\ policy = "confirmed"
        /\ loaded' = TRUE
        /\ UNCHANGED <<joined, marker, policy>>

Restart == /\ loaded /\ loaded' = FALSE
           /\ UNCHANGED <<joined, marker, policy>>

Leave(i) == /\ policy = "confirmed" /\ i \in joined /\ Cardinality(joined) > 1
            /\ joined' = joined \ {i}
            /\ UNCHANGED <<marker, policy, loaded>>

Next == Mark \/ Install \/ Confirm \/ Load \/ Restart
        \/ (\E i \in Replicas : Join(i) \/ Leave(i))
Spec == Init /\ [][Next]_vars
TypeOK == /\ joined \subseteq Replicas /\ joined # {}
          /\ marker \in BOOLEAN /\ policy \in {"absent", "pending", "confirmed"}
          /\ loaded \in BOOLEAN
PolicyAfterMarker == policy # "absent" => marker
LoadedConfirmed == loaded => policy = "confirmed"
NoInitialCheckpointBlock == policy = "pending" => joined = Replicas
=============================================================================
