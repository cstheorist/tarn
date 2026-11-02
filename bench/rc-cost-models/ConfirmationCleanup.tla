----------------------- MODULE ConfirmationCleanup -----------------------
(* One RC commit and one queued locking reader of the same row. Start after
 * write_steps: the replacement is pending, the old confirmed row is visible,
 * the committer owns the row lock and its INFLIGHT entry is registered.
 *
 * Deferred cleanup models the existing path: on_commit releases row locks,
 * but INFLIGHT survives until the calling fiber resumes from box.commit.
 * Callback cleanup removes that entry immediately before releasing locks in
 * the existing callback. The callback action is nonyielding; the caller's
 * outer cleanup remains and removing an already absent entry is harmless.
 * A granted waiter still runs wait_commits before reading the confirmed row.
 *
 * Trusted Tarantool premises, NOT proved here: successful on_commit runs only
 * after irrevocable WAL/quorum confirmation, its writes are visible to this
 * locking reader, and the callback precedes successful box.commit return.
 * The callback's cleanup/release cannot yield, raise or be interrupted.
 * Failure below is a settled rollback before confirmation, not a rollback
 * of confirmed work. No clock, quorum implementation, native path, disconnect,
 * leader change, DDL, cross-shard commit or SQL retry behavior is modeled.
 *
 * This is a finite safety/possibility companion to TarnCommit's CommitRC,
 * ConfirmLocal and RCLock, not a refinement proof or a liveness/performance
 * claim. There is no fairness: confirmation, the caller and replies may wait
 * forever. The early-finish witness completes the queued LOCKING READ while
 * the committer's box.commit caller has not resumed (and cannot have replied).
 * It does not establish that another SQL transaction commits earlier.
 *)
EXTENDS Naturals

CONSTANTS CleanupAtCallback, BugEarlyRetirement, BugEarlyUnlock

NoValue == 2
VARIABLES data, committer, waiter, tracked, owner
vars == <<data, committer, waiter, tracked, owner>>

Init ==
    /\ data = [phase |-> "pending", pending |-> 1, visible |-> 0]
    /\ committer = [callback |-> FALSE, caller |-> "blocked"]
    /\ waiter = [phase |-> "queued", observed |-> NoValue, sawPending |-> FALSE]
    /\ tracked = TRUE
    /\ owner = "committer"

Confirm ==
    /\ data.phase = "pending"
    /\ data' = [phase |-> "confirmed", pending |-> NoValue, visible |-> data.pending]
    /\ UNCHANGED <<committer, waiter, tracked, owner>>

FailBeforeConfirmation ==
    /\ data.phase = "pending"
    /\ data' = [phase |-> "aborted", pending |-> NoValue, visible |-> 0]
    /\ UNCHANGED <<committer, waiter, tracked, owner>>

CommitCallback ==
    /\ data.phase = "confirmed" /\ ~committer.callback
    /\ committer' = [committer EXCEPT !.callback = TRUE]
    /\ tracked' = IF CleanupAtCallback THEN FALSE ELSE tracked
    /\ owner' = IF owner = "committer" THEN "free" ELSE owner
    /\ UNCHANGED <<data, waiter>>

CallerResume ==
    /\ committer.caller = "blocked"
    /\ data.phase = "aborted" \/ (data.phase = "confirmed" /\ committer.callback)
    /\ committer' = [committer EXCEPT !.caller = "resumed"]
    /\ UNCHANGED <<data, waiter, tracked, owner>>

(* Existing outer cleanup runs on success AND failure. Repetition does not
 * restore INFLIGHT or release the successor's lock. Abort release abstracts
 * the failed transaction's eventual rollback/unlock, not every error code. *)
OuterCleanup ==
    /\ committer.caller \in {"resumed", "cleaned", "replied"}
    /\ tracked' = FALSE
    /\ owner' = IF data.phase = "aborted" /\ owner = "committer"
                THEN "free" ELSE owner
    /\ committer' = [committer EXCEPT
          !.caller = IF @ = "resumed" THEN "cleaned" ELSE @]
    /\ UNCHANGED <<data, waiter>>

SendReply ==
    /\ committer.caller = "cleaned"
    /\ committer' = [committer EXCEPT !.caller = "replied"]
    /\ UNCHANGED <<data, waiter, tracked, owner>>

WaiterAcquire ==
    /\ waiter.phase = "queued" /\ owner = "free"
    /\ owner' = "waiter"
    /\ waiter' = [waiter EXCEPT !.phase = "granted"]
    /\ UNCHANGED <<data, committer, tracked>>

WaiterRead ==
    /\ waiter.phase = "granted" /\ owner = "waiter" /\ ~tracked
    /\ waiter' = [phase |-> "read", observed |-> data.visible,
                   sawPending |-> data.phase = "pending"]
    /\ UNCHANGED <<data, committer, tracked, owner>>

WaiterFinish ==
    /\ waiter.phase = "read"
    /\ waiter' = [waiter EXCEPT !.phase = "done"]
    /\ UNCHANGED <<data, committer, tracked, owner>>

RetireTooEarly ==
    /\ BugEarlyRetirement /\ data.phase = "pending" /\ tracked
    /\ tracked' = FALSE
    /\ UNCHANGED <<data, committer, waiter, owner>>

UnlockTooEarly ==
    /\ BugEarlyUnlock /\ data.phase = "pending" /\ owner = "committer"
    /\ owner' = "free"
    /\ UNCHANGED <<data, committer, waiter, tracked>>

Next == Confirm \/ FailBeforeConfirmation \/ CommitCallback \/ CallerResume
        \/ OuterCleanup \/ SendReply \/ WaiterAcquire \/ WaiterRead \/ WaiterFinish
        \/ RetireTooEarly \/ UnlockTooEarly
Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ data \in [phase : {"pending", "confirmed", "aborted"},
                 pending : {1, NoValue}, visible : 0..1]
    /\ committer \in [callback : BOOLEAN,
                      caller : {"blocked", "resumed", "cleaned", "replied"}]
    /\ waiter \in [phase : {"queued", "granted", "read", "done"},
                   observed : 0..2, sawPending : BOOLEAN]
    /\ tracked \in BOOLEAN /\ owner \in {"committer", "waiter", "free"}

DataSettles ==
    /\ (data.phase = "pending" => data.pending = 1 /\ data.visible = 0)
    /\ (data.phase = "confirmed" => data.pending = NoValue /\ data.visible = 1)
    /\ (data.phase = "aborted" => data.pending = NoValue /\ data.visible = 0)
PendingIsTracked == data.phase = "pending" => tracked
(* A settled abort may release locks without confirmation; pending writes
 * must retain them. Confirmed writes cannot enter the abort action. *)
NoUnlockBeforeSettlement == data.phase = "pending" => owner = "committer"
ConfirmationBeforeCallback == committer.callback => data.phase = "confirmed"
NoStaleLockRead ==
    waiter.phase \in {"read", "done"} =>
        /\ ~waiter.sawPending /\ waiter.observed = data.visible
        /\ owner = "waiter"
NoPrematureReply ==
    committer.caller = "replied" => data.phase # "pending" /\ ~tracked

(* Negated reachability target: expected to FAIL only for the callback
 * witness configuration; expected to HOLD for the deferred configuration.
 * No temporal eventuality or latency magnitude follows from this witness. *)
NoWaiterFinishBeforeCallerResume ==
    ~(data.phase = "confirmed" /\ waiter.phase = "done"
      /\ waiter.observed = 1 /\ committer.caller = "blocked")
=============================================================================
