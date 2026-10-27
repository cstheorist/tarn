-------------------------- MODULE VinylAdmission --------------------------
(* A local refinement of fresh-write admission, not a replacement for
   TarnCommit. The fixed pressure interval has abstract estimated byte
   costs, one meter per shard, and two gateways. PREPARE's durable NO and
   ABORT/tombstones are existing protocol steps; they are not business-row
   effects. Decided APPLY and durable YES duplicates need no fresh credit. *)
EXTENDS Integers, FiniteSets
CONSTANTS BugPerGatewayBudget, BugGateAfterEffect, BugNegativeCredit,
          BugThrottleExistingYes, BugThrottleDecidedApply,
          BugReplayUnknown, BugResetFullBurst
Shards == {"A", "B"}
Gateways == {"g1", "g2"}
Locals == {"x", "y"}
Burst == 2
Cost(t) == IF t = "x" THEN 2 ELSE 1
Gateway(t) == IF t = "y" THEN "g2" ELSE "g1"
VARIABLES credit, gwCredit, earned, spent, ticks, epoch,
          localStage, localRows, attemptBefore, retries,
          part, decision, revote, applyReply, rows
vars == <<credit, gwCredit, earned, spent, ticks, epoch,
          localStage, localRows, attemptBefore, retries,
          part, decision, revote, applyReply, rows>>
Init ==
    /\ credit = [s \in Shards |-> 0]
    /\ gwCredit = [g \in Gateways |-> [s \in Shards |-> 0]]
    /\ earned = [s \in Shards |-> 0] /\ spent = [s \in Shards |-> 0]
    /\ ticks = 0 /\ epoch = 0
    /\ localStage = [t \in Locals |-> "idle"]
    /\ localRows = [t \in Locals |-> 0]
    /\ attemptBefore = [t \in Locals |-> 0]
    /\ retries = [t \in Locals |-> 1]
    /\ part = [s \in Shards |-> "none"] /\ decision = "undecided"
    /\ revote = [s \in Shards |-> "none"]
    /\ applyReply = [s \in Shards |-> "none"]
    /\ rows = [s \in Shards |-> 0]
Min(a, b) == IF a < b THEN a ELSE b
Available(g, s) == IF BugPerGatewayBudget THEN gwCredit[g][s] ELSE credit[s]
Enough(g, s, n) == Available(g, s) >= n
Charge(g, s, n) ==
    /\ spent' = [spent EXCEPT ![s] = @ + n]
    /\ IF BugPerGatewayBudget
       THEN /\ gwCredit' = [gwCredit EXCEPT ![g][s] = @ - n]
            /\ UNCHANGED credit
       ELSE /\ credit' = [credit EXCEPT ![s] = @ - n]
            /\ UNCHANGED gwCredit
Refill ==
    /\ ticks < 3 /\ ticks' = ticks + 1
    /\ credit' = [s \in Shards |-> Min(Burst, credit[s] + 1)]
    /\ gwCredit' = [g \in Gateways |->
            [s \in Shards |-> Min(Burst, gwCredit[g][s] + 1)]]
    /\ earned' = [s \in Shards |-> earned[s] + Min(Burst, credit[s] + 1) - credit[s]]
    /\ UNCHANGED <<spent, epoch, localStage, localRows, attemptBefore, retries,
                   part, decision, revote, applyReply, rows>>
(* A new controller starts cold. Already admitted work may finish, but a
   promotion does not mint new credit or reset the historical bound. *)
Promote ==
    /\ epoch = 0 /\ epoch' = 1
    /\ credit' = [s \in Shards |-> IF BugResetFullBurst THEN Burst ELSE 0]
    /\ gwCredit' = [g \in Gateways |->
            [s \in Shards |-> IF BugResetFullBurst THEN Burst ELSE 0]]
    /\ UNCHANGED <<earned, spent, ticks, localStage, localRows, attemptBefore,
                   retries, part, decision, revote, applyReply, rows>>
LocalAdmission(t) ==
    /\ t \in Locals /\ localStage[t] = "idle"
    /\ IF Enough(Gateway(t), "A", Cost(t)) \/ BugNegativeCredit
       THEN /\ Charge(Gateway(t), "A", Cost(t))
            /\ localStage' = [localStage EXCEPT ![t] = "admitted"]
            /\ UNCHANGED localRows
       ELSE /\ localStage' = [localStage EXCEPT ![t] = "throttled"]
            /\ localRows' = [localRows EXCEPT ![t] =
                   IF BugGateAfterEffect THEN @ + 1 ELSE @]
            /\ UNCHANGED <<credit, gwCredit, spent>>
    /\ UNCHANGED <<earned, ticks, epoch, attemptBefore, retries, part,
                   decision, revote, applyReply, rows>>
LocalEffect(t) ==
    /\ t \in Locals /\ localStage[t] = "admitted"
    /\ localRows' = [localRows EXCEPT ![t] = @ + 1]
    /\ localStage' = [localStage EXCEPT ![t] = "effected"]
    /\ UNCHANGED <<credit, gwCredit, earned, spent, ticks, epoch,
                   attemptBefore, retries, part, decision, revote, applyReply, rows>>
LocalReceipt(t, received) ==
    /\ t \in Locals /\ localStage[t] = "effected" /\ received \in BOOLEAN
    /\ localStage' = [localStage EXCEPT ![t] = IF received THEN "known" ELSE "unknown"]
    /\ UNCHANGED <<credit, gwCredit, earned, spent, ticks, epoch, localRows,
                   attemptBefore, retries, part, decision, revote, applyReply, rows>>
(* One possible retry within a bounded caller deadline. The real elapsed
   time budget is a runtime obligation; unknown mutation outcomes are never
   retried, even though an explicit no-effect throttled request may be. *)
RetryLocal(t) ==
    /\ t \in Locals /\ retries[t] > 0
    /\ localStage[t] = "throttled" \/ (BugReplayUnknown /\ localStage[t] = "unknown")
    /\ localStage' = [localStage EXCEPT ![t] = "idle"]
    /\ attemptBefore' = [attemptBefore EXCEPT ![t] = localRows[t]]
    /\ retries' = [retries EXCEPT ![t] = @ - 1]
    /\ UNCHANGED <<credit, gwCredit, earned, spent, ticks, epoch, localRows,
                   part, decision, revote, applyReply, rows>>
FreshPrepare(s) ==
    /\ s \in Shards /\ part[s] = "none" /\ decision = "undecided"
    /\ IF Enough("g1", s, 1) \/ BugNegativeCredit
       THEN /\ Charge("g1", s, 1)
            /\ part' = [part EXCEPT ![s] = "prepared"]
       ELSE /\ part' = [part EXCEPT ![s] = "aborted"]
            /\ UNCHANGED <<credit, gwCredit, spent>>
    /\ UNCHANGED <<earned, ticks, epoch, localStage, localRows, attemptBefore,
                   retries, decision, revote, applyReply, rows>>
Decide ==
    /\ decision = "undecided"
    /\ (\A s \in Shards : part[s] = "prepared") \/
       (\E s \in Shards : part[s] = "aborted")
    /\ decision' = IF \A s \in Shards : part[s] = "prepared"
                    THEN "committed" ELSE "aborted"
    /\ UNCHANGED <<credit, gwCredit, earned, spent, ticks, epoch, localStage,
                   localRows, attemptBefore, retries, part, revote, applyReply, rows>>
AbortAt(s) ==
    /\ decision = "aborted" /\ s \in Shards /\ part[s] # "aborted"
    /\ part' = [part EXCEPT ![s] = "aborted"]
    /\ UNCHANGED <<credit, gwCredit, earned, spent, ticks, epoch, localStage,
                   localRows, attemptBefore, retries, decision, revote, applyReply, rows>>
ExistingYes(s) ==
    /\ s \in Shards /\ part[s] = "prepared" /\ revote[s] = "none"
    /\ revote' = [revote EXCEPT ![s] =
            IF BugThrottleExistingYes /\ ~Enough("g1", s, 1) THEN "throttled" ELSE "yes"]
    /\ UNCHANGED <<credit, gwCredit, earned, spent, ticks, epoch, localStage,
                   localRows, attemptBefore, retries, part, decision, applyReply, rows>>
ApplyAt(s) ==
    /\ s \in Shards /\ decision = "committed" /\ part[s] = "prepared"
    /\ IF BugThrottleDecidedApply /\ ~Enough("g1", s, 1)
       THEN /\ applyReply' = [applyReply EXCEPT ![s] = "throttled"]
            /\ UNCHANGED <<part, rows>>
       ELSE /\ part' = [part EXCEPT ![s] = "applied"]
            /\ rows' = [rows EXCEPT ![s] = 1]
            /\ applyReply' = [applyReply EXCEPT ![s] = "ok"]
    /\ UNCHANGED <<credit, gwCredit, earned, spent, ticks, epoch, localStage,
                   localRows, attemptBefore, retries, decision, revote>>
Next == Refill \/ Promote \/ Decide
        \/ (\E t \in Locals : LocalAdmission(t) \/ LocalEffect(t) \/ RetryLocal(t)
                              \/ (\E received \in BOOLEAN : LocalReceipt(t, received)))
        \/ (\E s \in Shards : FreshPrepare(s) \/ AbortAt(s) \/ ExistingYes(s) \/ ApplyAt(s))
Spec == Init /\ [][Next]_vars
TypeOK == /\ credit \in [Shards -> (-2..2)]
          /\ gwCredit \in [Gateways -> [Shards -> (-2..2)]]
          /\ earned \in [Shards -> 0..6] /\ spent \in [Shards -> 0..8]
          /\ ticks \in 0..3 /\ epoch \in 0..1
          /\ localStage \in [Locals -> {"idle", "admitted", "effected", "known", "unknown", "throttled"}]
          /\ localRows \in [Locals -> 0..2] /\ attemptBefore \in [Locals -> 0..2]
          /\ retries \in [Locals -> 0..1]
          /\ part \in [Shards -> {"none", "prepared", "aborted", "applied"}]
          /\ decision \in {"undecided", "committed", "aborted"}
          /\ revote \in [Shards -> {"none", "yes", "throttled"}]
          /\ applyReply \in [Shards -> {"none", "ok", "throttled"}]
          /\ rows \in [Shards -> 0..1]
NonnegativeCredit == /\ \A s \in Shards : credit[s] >= 0
                     /\ \A g \in Gateways : \A s \in Shards : gwCredit[g][s] >= 0
SharedBudget == \A s \in Shards : spent[s] <= earned[s]
NoEffectRefusal == \A t \in Locals : localStage[t] = "throttled" => localRows[t] = attemptBefore[t]
NoBusinessReplay == \A t \in Locals : localRows[t] <= 1
ExistingYesStable == \A s \in Shards : revote[s] # "throttled"
DecidedApplyNotThrottled == \A s \in Shards : applyReply[s] # "throttled"
AbortLeavesNoRows == decision = "aborted" => \A s \in Shards : rows[s] = 0
=============================================================================
