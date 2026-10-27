------------------------ MODULE EngineSourceDrain ------------------------
(* EngineCopy's final-drain refinement: a confirmed synchronous fence only
   settles its older queue prefix. A source write admitted while that fence
   awaits confirmation can be newer. Canceling its caller does not terminate
   the native transaction. The final ReadConfirmed tail must follow a native
   queue/terminal cut, not merely disappearance from the caller inflight map.
   A post-tail cut alone cannot repair an already captured old payload.
   One dirty PK is sufficient; capture completeness, final write exclusion,
   ownership and durable shadow effects are premises of the parent models.
   This is not a proof of the native limbo implementation or replicated DDL. *)
EXTENDS Naturals
CONSTANT BugCallerOnlyCut
VARIABLES phase, admitted, pending, caller, source, shadow
vars == <<phase, admitted, pending, caller, source, shadow>>
Phases == {"new", "fencePending", "fenced", "tail", "cut", "prepared"}

Init == /\ phase = "new" /\ admitted = FALSE /\ pending = FALSE /\ caller = FALSE
        /\ source = FALSE /\ shadow = FALSE

BeginFence == /\ phase = "new" /\ phase' = "fencePending"
              /\ UNCHANGED <<admitted, pending, caller, source, shadow>>

(* The earlier cache still admits a source transaction. Its synchronous
   native effect is queued AFTER the fence; source denotes confirmed data. *)
AdmitSource == /\ phase = "fencePending" /\ ~admitted
               /\ admitted' = TRUE /\ pending' = TRUE /\ caller' = TRUE
               /\ UNCHANGED <<phase, source, shadow>>

ConfirmFence == /\ phase = "fencePending" /\ phase' = "fenced"
                /\ UNCHANGED <<admitted, pending, caller, source, shadow>>

DetachCaller == /\ pending /\ caller /\ caller' = FALSE
                /\ UNCHANGED <<phase, admitted, pending, source, shadow>>

ResolveSource(commit) ==
    /\ pending /\ phase \in {"fenced", "tail"} /\ commit \in BOOLEAN
    /\ source' = commit /\ pending' = FALSE /\ caller' = FALSE
    /\ UNCHANGED <<phase, admitted, shadow>>

Tail == /\ phase = "fenced" /\ ~caller
        /\ (~pending \/ BugCallerOnlyCut)
        /\ shadow' = source /\ phase' = "tail"
        /\ UNCHANGED <<admitted, pending, caller, source>>

PostTailCut == /\ phase = "tail" /\ ~pending /\ phase' = "cut"
               /\ UNCHANGED <<admitted, pending, caller, source, shadow>>

Prepare == /\ phase = "cut" /\ phase' = "prepared"
           /\ UNCHANGED <<admitted, pending, caller, source, shadow>>

Next == BeginFence \/ AdmitSource \/ ConfirmFence \/ DetachCaller
        \/ (\E commit \in BOOLEAN : ResolveSource(commit))
        \/ Tail \/ PostTailCut \/ Prepare
Spec == Init /\ [][Next]_vars
TypeOK == /\ phase \in Phases
          /\ admitted \in BOOLEAN /\ pending \in BOOLEAN /\ caller \in BOOLEAN
          /\ source \in BOOLEAN /\ shadow \in BOOLEAN
          /\ (caller => pending) /\ (pending => admitted)
PreparedImage == phase = "prepared" => shadow = source
=============================================================================
