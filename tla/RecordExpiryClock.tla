-------------------------- MODULE RecordExpiryClock -------------------------
(* Expiry's timing refinement, separate from quorum read freshness (#130).
 * A transaction timestamp is issued before a protocol record is written.
 * The record expires at the holder's write clock + Age + 2*Allowance. A fresh
 * PREPARE accepts while the receiving participant's age <= its max age.
 * Existing durable YES/NO receipts are deliberately outside fresh admission.
 *
 * The positive case has common elapsed time and MONOTONE CLOCKS with varying
 * offsets whose current spread <= Allowance, a finite past-issued immutable
 * timestamp, and fixed max age. Offsets range over 0..2; they may rise or
 * fall as elapsed time advances, provided no actual clock goes backward.
 * Forward clock steps are separate actions. A process pause is delayed action execution;
 * elapsed wall time continues during that pause.
 * Under those premises, collection irreversibly rules out fresh admission.
 * The controls remove ONE protection/premise: use only one skew margin,
 * excessive skew, backward clock after GC, larger max age after GC,
 * future timestamp, or omitted timestamp. A read quorum repairs none of them.
 *
 * This is not a production clock detector or proof that pairwise clock
 * probes establish historical/issuer bounds. With changing offsets, the
 * issuer can lead the holder at write and the holder lead the peer at GC;
 * a single allowance can be consumed twice. The core commit model's
 * permanent Expire abstraction needs a justified runtime retention/admission
 * rule; a role check or fresh read alone cannot supply that rule.
 *)
EXTENDS Integers, FiniteSets

CONSTANTS BugExcessSkew, BugSingleMargin, BugClockBackward,
          BugRaiseMaxAge, BugFutureTimestamp, BugMissingTimestamp

Nodes == {"issuer", "holder", "peer"}
Age == 2
Allowance == 1
Offsets == [Nodes -> 0..2]
Bounded(o) == \A a, b \in Nodes : o[a] - o[b] <= Allowance

VARIABLES elapsed, offset, timestamp, expires, forgotten, accepted,
          changed, maxAge
vars == <<elapsed, offset, timestamp, expires, forgotten, accepted,
          changed, maxAge>>

Now(n) == elapsed + offset[n]
Admissible == BugMissingTimestamp \/ Now("peer") - timestamp <= maxAge

Init ==
    /\ elapsed = 0 /\ offset \in Offsets
    /\ BugExcessSkew \/ Bounded(offset)
    /\ timestamp = IF BugFutureTimestamp THEN 5 ELSE offset["issuer"]
    /\ expires = offset["holder"] + Age + (IF BugSingleMargin THEN Allowance ELSE 2 * Allowance)
    /\ forgotten = FALSE /\ accepted = FALSE /\ changed = FALSE /\ maxAge = Age

Tick(o) ==
    /\ elapsed < 7 /\ o \in Offsets /\ (BugExcessSkew \/ Bounded(o))
    /\ \A n \in Nodes : elapsed + 1 + o[n] >= Now(n)
    /\ elapsed' = elapsed + 1 /\ offset' = o
    /\ UNCHANGED <<timestamp, expires, forgotten, accepted, changed, maxAge>>

GC ==
    /\ ~forgotten /\ Now("holder") > expires /\ forgotten' = TRUE
    /\ UNCHANGED <<elapsed, offset, timestamp, expires, accepted, changed, maxAge>>

ChangeOffsets(o) ==
    /\ o \in Offsets /\ o # offset /\ (BugExcessSkew \/ Bounded(o))
    /\ \A n \in Nodes : o[n] >= offset[n]
    /\ offset' = o
    /\ UNCHANGED <<elapsed, timestamp, expires, forgotten, accepted, changed, maxAge>>

Backward ==
    /\ BugClockBackward /\ forgotten /\ ~changed
    /\ offset' = [offset EXCEPT !["peer"] = @ - 5] /\ changed' = TRUE
    /\ UNCHANGED <<elapsed, timestamp, expires, forgotten, accepted, maxAge>>

RaiseMaxAge ==
    /\ BugRaiseMaxAge /\ forgotten /\ ~changed
    /\ maxAge' = Age + 5 /\ changed' = TRUE
    /\ UNCHANGED <<elapsed, offset, timestamp, expires, forgotten, accepted>>

FreshPrepare ==
    /\ forgotten /\ ~accepted /\ Admissible /\ accepted' = TRUE
    /\ UNCHANGED <<elapsed, offset, timestamp, expires, forgotten, changed, maxAge>>

Next == (\E o \in Offsets : Tick(o)) \/ GC \/ Backward \/ RaiseMaxAge \/ FreshPrepare
        \/ (\E o \in Offsets : ChangeOffsets(o))
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ elapsed \in 0..7 /\ offset \in [Nodes -> -5..2]
    /\ timestamp \in 0..5 /\ expires \in 3..6
    /\ forgotten \in BOOLEAN /\ accepted \in BOOLEAN /\ changed \in BOOLEAN
    /\ maxAge \in {Age, Age + 5}

NoResurrectedPrepare == ~accepted
=============================================================================
