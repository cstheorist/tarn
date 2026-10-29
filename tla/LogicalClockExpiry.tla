------------------------- MODULE LogicalClockExpiry -------------------------
(* Accepted raw wall corrections are clipped against PREVIOUS LOGICAL wall.
 * Drift and gap checks are unchanged. The trusted raw-clock premise is a
 * FIXED common reference band [real+L, real+U] of WIDTH configured Skew,
 * over the full issue/retention/admission history. This is stronger than
 * contemporaneous pairwise raw skew; absolute +/-Skew is NOT this premise.
 *
 * An issuer's raw timestamp is issued at real=0 before the holder writes
 * its receipt at logical=0. Holder and peer observations are asynchronous.
 * The holder collects only after its fresh logical clock passes Age+2*S.
 * A fresh peer admission that conflicts with committed data would create a
 * new NO receipt. NoFalseNO checks that actual consequence after deletion.
 *
 * Max(previous logical, accepted raw) preserves the same reference band:
 * real is nondecreasing, so the old upper bound remains a current upper
 * bound; the current raw supplies the current lower bound. Fresh logical
 * readings therefore retain pairwise Skew; the two-skew expiry premise
 * is unchanged. Monotonic time never goes backward. Refused drift/gap
 * observations authorize no actions; recovery and certification are outside.
 *
 * Controls violate the post-check max rule or the fixed-band premise while
 * retaining the SAME local drift/gap checks. They must reach actual GC and
 * contradictory NO, not stop at a cosmetic clock-bound assertion. These
 * are boundary controls, not claims that production certificates accept
 * their clocks. Quorum cannot certify a common-mode/reference-clock bound.
 *)
EXTENDS Integers, FiniteSets
CONSTANTS BugFloorAdvance, BugRawBound
Nodes == {"holder", "peer"}
Age == 2
Skew == 2
Drift == 2
MaxGap == 3
MaxReal == 8
Expires == Age + 2 * Skew
Max(a, b) == IF a >= b THEN a ELSE b
Abs(a) == IF a >= 0 THEN a ELSE -a

VARIABLES real, logical, lastMono, rawAccepted, timestamp,
          forgotten, holderHigh, dataCommitted, newReceipt,
          corrected, inflated
vars == <<real, logical, lastMono, rawAccepted, timestamp,
          forgotten, holderHigh, dataCommitted, newReceipt,
          corrected, inflated>>

Init ==
    /\ real = 0 /\ logical = [n \in Nodes |-> 0]
    /\ lastMono = [n \in Nodes |-> 0] /\ rawAccepted = [n \in Nodes |-> 0]
    (* Issuer raw is within the same fixed band and issued before write. *)
    /\ timestamp \in 0..Skew
    /\ forgotten = FALSE /\ holderHigh = 0 /\ dataCommitted = TRUE
    /\ newReceipt = "none" /\ corrected = FALSE /\ inflated = FALSE

Tick ==
    /\ real < MaxReal /\ real' = real + 1
    /\ UNCHANGED <<logical, lastMono, rawAccepted, timestamp,
                   forgotten, holderHigh, dataCommitted, newReceipt,
                   corrected, inflated>>

Observe(n, raw) ==
    /\ raw \in real..(real + IF BugRawBound /\ n = "holder" THEN 8 ELSE Skew)
    /\ LET dm == real - lastMono[n]
       IN /\ dm >= 0 /\ dm <= MaxGap
          /\ Abs((raw - logical[n]) - dm) <= Drift
          /\ logical' = [logical EXCEPT ![n] =
                 Max(@, raw) + IF BugFloorAdvance /\ n = "holder" /\ ~inflated
                               THEN Expires + 1 ELSE 0]
          /\ corrected' = (corrected \/ (raw < rawAccepted[n] /\ dm > 0))
    /\ lastMono' = [lastMono EXCEPT ![n] = real]
    /\ rawAccepted' = [rawAccepted EXCEPT ![n] = raw]
    /\ inflated' = (inflated \/ (BugFloorAdvance /\ n = "holder"))
    /\ UNCHANGED <<real, timestamp, forgotten, holderHigh, dataCommitted, newReceipt>>

GC ==
    /\ ~forgotten /\ lastMono["holder"] = real
    /\ logical["holder"] > Expires
    /\ forgotten' = TRUE /\ holderHigh' = logical["holder"]
    /\ UNCHANGED <<real, logical, lastMono, rawAccepted, timestamp,
                   dataCommitted, newReceipt, corrected, inflated>>

FreshNo ==
    /\ forgotten /\ newReceipt = "none" /\ lastMono["peer"] = real
    /\ timestamp <= logical["peer"] + Skew
    /\ logical["peer"] - timestamp <= Age
    (* The old history is gone; a fresh conflicting PREPARE writes NO.
     * The GC holder's local H does not become a remote shard's clock. *)
    /\ newReceipt' = "aborted"
    /\ UNCHANGED <<real, logical, lastMono, rawAccepted, timestamp,
                   forgotten, holderHigh, dataCommitted, corrected, inflated>>

Next == Tick \/ (\E n \in Nodes, raw \in 0..16 : Observe(n, raw)) \/ GC \/ FreshNo
Spec == Init /\ [][Next]_vars
TypeOK ==
    /\ real \in 0..MaxReal /\ logical \in [Nodes -> 0..17]
    /\ lastMono \in [Nodes -> 0..MaxReal] /\ rawAccepted \in [Nodes -> 0..16]
    /\ timestamp \in 0..Skew /\ forgotten \in BOOLEAN /\ holderHigh \in 0..17
    /\ dataCommitted \in BOOLEAN /\ newReceipt \in {"none", "aborted"}
    /\ corrected \in BOOLEAN /\ inflated \in BOOLEAN
LogicalBand == \A n \in Nodes :
    logical[n] <= real + Skew /\ (lastMono[n] = real => logical[n] >= real)
NoFalseNO == ~(dataCommitted /\ newReceipt = "aborted")
=============================================================================
