---------------------------- MODULE PlanGuardBatch ----------------------------
(* Local refinement premise for BK.fresh_plans: inspect current replication
 * mode/control-space properties once and each planned user space once inside
 * the existing nonyielding atomic check. This is not a freshness certificate:
 * the request's clock/prefix proofs, row reads, locks and commit remain outside
 * this model and unchanged. Configuration and space properties may change at
 * every yield boundary. No validated mode may survive such a boundary.
 *
 * Two spaces and one invocation bound this model. Mode 1 is unreplicated,
 * mode 2 is the supported intersecting-quorum mode, mode 3 is unsupported.
 * Scalar and batched guards must make the same decision. Broken controls
 * retain an old mode through a yield or omit a planned space check.
 *)
EXTENDS Naturals, FiniteSets
CONSTANTS Batched, BugYield, BugSkipSpace
Spaces == {"a", "b"}
VARIABLES mode, controls, sync, phase, savedMode, savedControls, accepted, required
vars == <<mode, controls, sync, phase, savedMode, savedControls, accepted, required>>

ControlOK(m, c) == m = 1 \/ (m = 2 /\ c)
SpaceOK(m, s) == m = 1 \/ (m = 2 /\ sync[s])
Required == ControlOK(mode, controls) /\ \A s \in Spaces : SpaceOK(mode, s)

Init ==
    /\ mode \in 1..3 /\ controls \in BOOLEAN /\ sync \in [Spaces -> BOOLEAN]
    /\ phase = "new" /\ savedMode = 3 /\ savedControls = FALSE
    /\ accepted = FALSE /\ required = FALSE

Change ==
    /\ phase # "done"
    /\ mode' \in 1..3 /\ controls' \in BOOLEAN /\ sync' \in [Spaces -> BOOLEAN]
    /\ UNCHANGED <<phase, savedMode, savedControls, accepted, required>>

Scalar ==
    /\ ~Batched /\ phase = "new"
    /\ accepted' = (ControlOK(mode, controls) /\ \A s \in Spaces : SpaceOK(mode, s))
    /\ required' = Required /\ phase' = "done"
    /\ UNCHANGED <<mode, controls, sync, savedMode, savedControls>>

Batch ==
    /\ Batched /\ ~BugYield /\ phase = "new"
    /\ LET checked == IF BugSkipSpace THEN {"a"} ELSE Spaces IN
       accepted' = (ControlOK(mode, controls) /\ \A s \in checked : SpaceOK(mode, s))
    /\ required' = Required /\ phase' = "done"
    /\ UNCHANGED <<mode, controls, sync, savedMode, savedControls>>

SaveAcrossYield ==
    /\ Batched /\ BugYield /\ phase = "new"
    /\ savedMode' = mode /\ savedControls' = controls /\ phase' = "yielded"
    /\ UNCHANGED <<mode, controls, sync, accepted, required>>

UseAfterYield ==
    /\ phase = "yielded"
    /\ accepted' = (ControlOK(savedMode, savedControls) /\ \A s \in Spaces : SpaceOK(savedMode, s))
    /\ required' = Required /\ phase' = "done"
    /\ UNCHANGED <<mode, controls, sync, savedMode, savedControls>>

Next == Change \/ Scalar \/ Batch \/ SaveAcrossYield \/ UseAfterYield
Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ mode \in 1..3 /\ savedMode \in 1..3 /\ controls \in BOOLEAN /\ savedControls \in BOOLEAN
    /\ sync \in [Spaces -> BOOLEAN] /\ phase \in {"new", "yielded", "done"}
    /\ accepted \in BOOLEAN /\ required \in BOOLEAN
SameDecision == phase = "done" => accepted = required
NoUnsafeAcceptance == phase = "done" /\ accepted => required
=============================================================================
