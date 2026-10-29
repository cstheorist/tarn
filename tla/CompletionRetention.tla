------------------------- MODULE CompletionRetention -------------------------
(* Terminal APPLY/ABORT/CAS may finish while clock health is unknown. A
 * successor can inherit a receipt written by a later good-clock leader;
 * its own raw/last-valid/policy-created/GC clocks need not bound that row.
 *
 * One terminal transition samples its local floor once, retains at least
 * the existing receipt timestamp, and never lowers its expiry. A volatile
 * clock fault can be lost before persistence. A healthy successor then
 * performs lifecycle-eligible GC with an atomic persistent H advance.
 * Another reboot retains H, and a late conflicting PREPARE checks age.
 *
 * BugForgetPriorReceipt retains ALL local clock floors but ignores BOTH
 * prior receipt fields, matching a blind terminal overwrite. Its witness
 * must actually collect terminal history and create a new abort receipt
 * over committed data, not merely shorten an intermediate timestamp.
 *
 * Initial receipt validity, immutable past-issued coordinator timestamp,
 * healthy successor evidence, fresh confirmed lookups, stable policy and
 * atomic durable replication remain external premises. This is a local
 * completion/retention refinement, not global clock certification.
 *)
EXTENDS Naturals
CONSTANT BugForgetPriorReceipt
Age == 10
Skew == 2
CoordTS == 1100
LastValid == 1000
PolicyCreated == 1000
Max(a, b) == IF a >= b THEN a ELSE b

VARIABLES row, dataCommitted, decisionCommitted, raw, high, goodNow,
          healthy, volatileFault, phase, outcome
vars == <<row, dataCommitted, decisionCommitted, raw, high, goodNow,
          healthy, volatileFault, phase, outcome>>

LocalFloor == Max(Max(raw, LastValid), Max(PolicyCreated, high))
EffectiveNow == Max(goodNow, high)
RetainedUntil == Max(row.expires, row.ts + Age + 2 * Skew)

Init ==
    /\ row \in {[state |-> "prepared", ts |-> t, expires |-> t + Age + 2 * Skew + extra]
                : t \in 1100..1102, extra \in {0, 2}}
    /\ dataCommitted = FALSE /\ decisionCommitted = TRUE
    /\ raw \in {900, 1001} /\ high \in {0, 1002} /\ goodNow = 1101
    /\ healthy = FALSE /\ volatileFault = TRUE
    /\ phase = "prepared" /\ outcome = "none"

FinishDecided ==
    /\ phase = "prepared"
    (* Finishing is permitted despite unknown clock health. Use one sample
     * for BOTH new fields; a local floor alone cannot replace row history. *)
    /\ LET sample == LocalFloor
           t == IF BugForgetPriorReceipt THEN sample ELSE Max(sample, row.ts)
           e == IF BugForgetPriorReceipt THEN t + Age + 2 * Skew
                ELSE Max(row.expires, t + Age + 2 * Skew)
       IN row' = [state |-> "applied", ts |-> t, expires |-> e]
    /\ dataCommitted' = TRUE /\ phase' = "completed"
    /\ UNCHANGED <<decisionCommitted, raw, high, goodNow,
                   healthy, volatileFault, outcome>>

CrashPromote ==
    /\ phase = "completed"
    (* The bad owner's volatile fault was not persisted before its crash.
     * The new owner has independently valid current clock evidence. *)
    /\ volatileFault' = FALSE /\ healthy' = TRUE /\ phase' = "healthy"
    /\ UNCHANGED <<row, dataCommitted, decisionCommitted, raw, high, goodNow, outcome>>

Tick ==
    /\ phase = "healthy" /\ goodNow < 1120 /\ goodNow' = goodNow + 1
    /\ UNCHANGED <<row, dataCommitted, decisionCommitted, raw, high,
                   healthy, volatileFault, phase, outcome>>

CollectTerminal ==
    /\ phase = "healthy" /\ healthy /\ decisionCommitted
    /\ row.state = "applied" /\ goodNow > RetainedUntil
    /\ high' = Max(high, goodNow)
    /\ row' = [row EXCEPT !.state = "absent"] /\ phase' = "collected"
    /\ UNCHANGED <<dataCommitted, decisionCommitted, raw, goodNow,
                   healthy, volatileFault, outcome>>

RebootAfterGC ==
    /\ phase = "collected" /\ phase' = "fresh"
    (* All metadata, particularly H, and the user tuple survive reboot. *)
    /\ UNCHANGED <<row, dataCommitted, decisionCommitted, raw, high, goodNow,
                   healthy, volatileFault, outcome>>

LatePrepare ==
    /\ phase = "fresh" /\ healthy /\ row.state = "absent"
    /\ CoordTS <= goodNow + Skew
    /\ IF EffectiveNow <= CoordTS + Age
       THEN /\ row' = [state |-> "aborted", ts |-> EffectiveNow,
                       expires |-> EffectiveNow + Age + 2 * Skew]
            /\ outcome' = "no"
       ELSE /\ row' = row /\ outcome' = "expired"
    /\ phase' = "done"
    /\ UNCHANGED <<dataCommitted, decisionCommitted, raw, high, goodNow,
                   healthy, volatileFault>>

Next == FinishDecided \/ CrashPromote \/ Tick \/ CollectTerminal \/ RebootAfterGC \/ LatePrepare
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ row.state \in {"prepared", "applied", "absent", "aborted"}
    /\ row.ts \in Nat /\ row.expires \in Nat
    /\ dataCommitted \in BOOLEAN /\ decisionCommitted \in BOOLEAN
    /\ raw \in {900, 1001} /\ high \in 0..1120 /\ goodNow \in 1101..1120
    /\ healthy \in BOOLEAN /\ volatileFault \in BOOLEAN
    /\ phase \in {"prepared", "completed", "healthy", "collected", "fresh", "done"}
    /\ outcome \in {"none", "no", "expired"}
NoContradictoryReopen == ~(dataCommitted /\ row.state = "aborted")
=============================================================================
