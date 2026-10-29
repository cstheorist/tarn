------------------------- MODULE RecordExpiryPolicy -------------------------
(* Local persisted-policy refinement, not a global clock-health proof (#130).
 * A shard retains immutable age/skew policy and a replicated GC wall high-
 * water mark. Each destructive GC chunk advances that mark atomically with
 * deletion; promotion/restart retains it. Fresh absent-record state creation
 * uses max(current wall, stored mark), given finite/past-issued input,
 * and requires current certified clock health and the persisted policy.
 * The guard runs at FINAL admission after planning may have yielded.
 * Existing durable receipts and decided finishing do not create fresh state
 * and are outside this guard. A timeout/unknown health never means fresh NO.
 *
 * This model proves local irreversibility after local GC, and the placement
 * of the admission guard. It DOES NOT establish initial/all-issuer skew,
 * historical rate bounds, cross-shard horizon propagation, the soundness of
 * clock-health certification, or atomic native replication. Those remain
 * external premises; RecordExpiryClock exposes failures of weaker premises.
 *)
EXTENDS Naturals

CONSTANTS BugNoWatermark, BugMutablePolicy, BugEntryOnly, BugIgnoreHealth

Age == 2
Skew == 1
Max(a, b) == IF a >= b THEN a ELSE b

VARIABLES wall, high, configuredAge, healthy,
          forgotten, phase, entryAllowed, unsafeAdmission, jumped,
          policyChanged, healthChanged, restarts
vars == <<wall, high, configuredAge, healthy,
          forgotten, phase, entryAllowed, unsafeAdmission, jumped,
          policyChanged, healthChanged, restarts>>

EffectiveNow == Max(wall, high)
PolicyOK == configuredAge = Age \/ BugMutablePolicy
FinalGuard ==
    /\ healthy \/ BugIgnoreHealth
    /\ PolicyOK
    /\ EffectiveNow <= (IF BugMutablePolicy THEN configuredAge ELSE Age)

Init ==
    /\ wall = 0 /\ high = 0 /\ configuredAge = Age /\ healthy = TRUE
    (* The immutable valid timestamp is zero, before record creation. *)
    /\ forgotten = FALSE /\ phase = "idle" /\ entryAllowed = FALSE
    /\ unsafeAdmission = FALSE /\ jumped = FALSE /\ policyChanged = FALSE
    /\ healthChanged = FALSE /\ restarts = 0

Tick ==
    /\ wall < 7 /\ wall' = wall + 1
    /\ UNCHANGED <<high, configuredAge, healthy,
                   forgotten, phase, entryAllowed, unsafeAdmission, jumped,
                   policyChanged, healthChanged, restarts>>

GC ==
    /\ ~forgotten /\ wall > Age + 2 * Skew
    /\ healthy /\ configuredAge = Age
    /\ high' = IF BugNoWatermark THEN high ELSE Max(high, wall)
    /\ forgotten' = TRUE
    /\ UNCHANGED <<wall, configuredAge, healthy,
                   phase, entryAllowed, unsafeAdmission, jumped,
                   policyChanged, healthChanged, restarts>>

Backward ==
    /\ forgotten /\ ~jumped /\ wall' = 0 /\ jumped' = TRUE
    /\ UNCHANGED <<high, configuredAge, healthy,
                   forgotten, phase, entryAllowed, unsafeAdmission,
                   policyChanged, healthChanged, restarts>>

ChangePolicy ==
    /\ ~policyChanged /\ configuredAge' = Age + 5 /\ policyChanged' = TRUE
    /\ UNCHANGED <<wall, high, healthy, forgotten,
                   phase, entryAllowed, unsafeAdmission, jumped,
                   healthChanged, restarts>>

UncertainClock ==
    /\ ~healthChanged /\ healthy' = FALSE /\ healthChanged' = TRUE
    /\ UNCHANGED <<wall, high, configuredAge, forgotten,
                   phase, entryAllowed, unsafeAdmission, jumped,
                   policyChanged, restarts>>

RestartOrPromote ==
    /\ restarts < 2 /\ restarts' = restarts + 1
    /\ phase' = "idle" /\ entryAllowed' = FALSE
    /\ UNCHANGED <<wall, high, configuredAge, healthy,
                   forgotten, unsafeAdmission, jumped, policyChanged, healthChanged>>

BeginAdmission ==
    /\ phase = "idle" /\ FinalGuard
    /\ entryAllowed' = TRUE /\ phase' = "planning"
    /\ UNCHANGED <<wall, high, configuredAge, healthy,
                   forgotten, unsafeAdmission, jumped, policyChanged,
                   healthChanged, restarts>>

FinishAdmission ==
    /\ phase = "planning"
    (* The entry-only control retains final policy/health checks, so its
     * witness must actually cross expiry/GC while planning is paused. *)
    /\ IF BugEntryOnly THEN entryAllowed /\ (healthy \/ BugIgnoreHealth) /\ PolicyOK
       ELSE FinalGuard
    /\ unsafeAdmission' = (unsafeAdmission \/ forgotten \/ ~healthy)
    /\ phase' = "done"
    /\ UNCHANGED <<wall, high, configuredAge, healthy,
                   forgotten, entryAllowed, jumped, policyChanged,
                   healthChanged, restarts>>

Next == Tick \/ GC \/ Backward \/ ChangePolicy \/ UncertainClock
        \/ RestartOrPromote \/ BeginAdmission \/ FinishAdmission
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ wall \in 0..7 /\ high \in 0..7 /\ configuredAge \in {Age, Age + 5}
    /\ healthy \in BOOLEAN
    /\ forgotten \in BOOLEAN /\ phase \in {"idle", "planning", "done"}
    /\ entryAllowed \in BOOLEAN /\ unsafeAdmission \in BOOLEAN
    /\ jumped \in BOOLEAN /\ policyChanged \in BOOLEAN /\ healthChanged \in BOOLEAN
    /\ restarts \in 0..2

NoUnsafeAdmission == ~unsafeAdmission
=============================================================================
