----------------------------- MODULE EngineAlias -----------------------------
(* Two healthy callers ensure the SAME immutable newSID -> canonical binding.
   Native WAL confirmation can outlive the read/replace step. ReadConfirmed
   sees only alias, not pending: an overlapping cold read followed by a write
   may conflict even though both requests want identical bytes. Serialize
   the whole ensure, read only after acquiring its central newSID mutex, and
   return an already-confirmed matching alias without another write.
   Healthy finite WAL and the MVCC conflict rule are refinement premises,
   established independently by actual overlapping RPCs. No role change,
   cancellation, transport retry or global/stale-owner claim is modeled. *)
EXTENDS Naturals, FiniteSets
CONSTANTS InitialInstalled, BugNoMutex
Workers == {"a", "b"}
None == "none"
VARIABLES phase, result, view, alias, revision, pending, owner, writes
vars == <<phase, result, view, alias, revision, pending, owner, writes>>
Release(w) == IF owner = w THEN None ELSE owner

Init == /\ phase = [w \in Workers |-> "idle"]
        /\ result = [w \in Workers |-> "none"]
        /\ view = [w \in Workers |-> 0]
        /\ alias = IF InitialInstalled THEN 1 ELSE 0
        /\ revision = 0 /\ pending = None /\ owner = None /\ writes = 0

Enter(w) == /\ phase[w] = "idle" /\ (BugNoMutex \/ owner = None)
            /\ phase' = [phase EXCEPT ![w] = "read"]
            /\ owner' = IF BugNoMutex THEN owner ELSE w
            /\ UNCHANGED <<result, view, alias, revision, pending, writes>>

Read(w) ==
    /\ phase[w] = "read"
    /\ IF alias = 1
       THEN /\ phase' = [phase EXCEPT ![w] = "done"]
            /\ result' = [result EXCEPT ![w] = "ok"]
            /\ owner' = Release(w) /\ UNCHANGED view
       ELSE /\ phase' = [phase EXCEPT ![w] = "write"]
            /\ view' = [view EXCEPT ![w] = revision]
            /\ UNCHANGED <<result, owner>>
    /\ UNCHANGED <<alias, revision, pending, writes>>

Write(w) ==
    /\ phase[w] = "write"
    /\ IF pending # None \/ view[w] # revision
       THEN /\ phase' = [phase EXCEPT ![w] = "done"]
            /\ result' = [result EXCEPT ![w] = "conflict"]
            /\ owner' = Release(w) /\ UNCHANGED <<pending, writes>>
       ELSE /\ phase' = [phase EXCEPT ![w] = "wal"]
            /\ pending' = w /\ writes' = writes + 1
            /\ UNCHANGED <<result, owner>>
    /\ UNCHANGED <<view, alias, revision>>

Confirm(w) == /\ phase[w] = "wal" /\ pending = w
              /\ alias' = 1 /\ revision' = revision + 1 /\ pending' = None
              /\ phase' = [phase EXCEPT ![w] = "done"]
              /\ result' = [result EXCEPT ![w] = "ok"] /\ owner' = Release(w)
              /\ UNCHANGED <<view, writes>>

Next == \E w \in Workers : Enter(w) \/ Read(w) \/ Write(w) \/ Confirm(w)
Spec == Init /\ [][Next]_vars
        /\ \A w \in Workers : WF_vars(Enter(w)) /\ WF_vars(Read(w))
                               /\ WF_vars(Write(w)) /\ WF_vars(Confirm(w))
TypeOK == /\ phase \in [Workers -> {"idle", "read", "write", "wal", "done"}]
          /\ result \in [Workers -> {"none", "ok", "conflict"}]
          /\ view \in [Workers -> 0..2] /\ alias \in {0, 1} /\ revision \in 0..2
          /\ pending \in Workers \cup {None} /\ owner \in Workers \cup {None}
          /\ writes \in 0..2
AckConfirmedBinding == (\E w \in Workers : result[w] = "ok") => alias = 1
ReplayReadOnly == InitialInstalled => writes = 0
BothCallersOK == (\A w \in Workers : phase[w] = "done") =>
                (\A w \in Workers : result[w] = "ok")
Completion == <>(\A w \in Workers : phase[w] = "done")
=============================================================================
