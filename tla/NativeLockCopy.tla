---------------------------- MODULE NativeLockCopy ----------------------------
(***************************************************************************)
(* The native procedures' copy of the lock index (issues #28 and #131).   *)
(* The Lua module owns the lock index: the locks of prepared transactions *)
(* (LK and RL in lua/tarn.lua).  The native module keeps a copy of it,    *)
(* counts of the locks on each key by mode (crates/tarn-storage/src/      *)
(* locks.rs), which the Lua module changes in the same step as the index: *)
(* the step that takes a PREPARE's locks (lk_point, lk_range), the one    *)
(* that releases them (lk_release: APPLY, ABORT, a PREPARE whose local    *)
(* transaction rolled back), and the one that makes the index again from  *)
(* the durable lock records after an election (lk_reload).  A native step *)
(* runs between two steps of the Lua module, without a yield, and decides *)
(* from the copy alone; a Lua step decides from the index.                *)
(*                                                                         *)
(* One key per transaction, R or W: what its PREPARE read or writes there.*)
(* The PREPARE takes the lock in the step that checks the index, writes   *)
(* its lock record, and waits for its quorum: the index shows the lock    *)
(* from that step, and so does the record, which an election's rebuild    *)
(* reads (lua/tarn.lua "Volatile lock index").  The quorum confirms the   *)
(* PREPARE, or it fails and its locks go; APPLY or ABORT release them.    *)
(* An election makes the index stale: steps wait until it is made again.  *)
(*                                                                         *)
(* NativeFindsLocks: a native step never goes ahead while the index holds *)
(* a lock that the Lua check of the same step would find -- for a write   *)
(* of a key, any lock on it (write_conflict); for a read, a W lock        *)
(* (reads_w_locked).  CopyIsIndex: between steps the copy counts what the *)
(* index holds.                                                            *)
(*                                                                         *)
(* Each bug switch separates the copy from the index by one step, and TLC *)
(* must find a native step in between:                                    *)
(*   BugCopyAfterYield    a PREPARE copies its lock a step after it takes  *)
(*                        it (a yield between: vinyl reads, lua/tarn.lua   *)
(*                        "Yields");                                       *)
(*   BugCopyReleaseFirst  a release drops the copy's count a step before   *)
(*                        the index's lock;                                *)
(*   BugCopyOnQuorum      a PREPARE's lock reaches the copy once its       *)
(*                        quorum confirms it (the index has it before);    *)
(*   BugReloadNoCopy      the index made again from the lock records       *)
(*                        leaves the copy as it was cleared.               *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets

CONSTANTS
    Txns, Keys,
    BugCopyAfterYield, BugCopyReleaseFirst, BugCopyOnQuorum, BugReloadNoCopy

Modes == {"R", "W"}

VARIABLES
    st,       \* each transaction: idle, copying (BugCopyAfterYield: its lock not copied yet),
              \* locked (waits for its quorum), prepared, releasing (BugCopyReleaseFirst:
              \* the copy dropped its lock, the index not yet), done
    lockOf,   \* each transaction's lock: <<key, mode>>
    index,    \* the lock index: the transactions whose lock it holds
    records,  \* the durable lock records: the transactions whose record is written
    copy,     \* the copy: [Keys -> [Modes -> Nat]], locks on each key by mode
    stale,    \* the index is to be made again (an election); steps wait
    bad       \* a native step went ahead past a lock the Lua check finds

vars == <<st, lockOf, index, records, copy, stale, bad>>

NoLock == <<CHOOSE k \in Keys : TRUE, "R">>

\* The locks of the transactions in S on key k, of mode m.
Count(S, k, m) == Cardinality({t \in S : lockOf[t] = <<k, m>>})
Census(S) == [k \in Keys |-> [m \in Modes |-> Count(S, k, m)]]

\* (the copy's counts stop at zero, as tarn_verified::locks::moved does)
Inc(c, l) == [c EXCEPT ![l[1]][l[2]] = @ + 1]
Dec(c, l) == [c EXCEPT ![l[1]][l[2]] = IF @ > 0 THEN @ - 1 ELSE 0]

TypeOK ==
    /\ st \in [Txns -> {"idle", "copying", "locked", "prepared", "releasing", "done"}]
    /\ lockOf \in [Txns -> Keys \X Modes]
    /\ index \subseteq Txns /\ records \subseteq Txns
    /\ copy \in [Keys -> [Modes -> 0..Cardinality(Txns)]]
    /\ stale \in BOOLEAN /\ bad \in BOOLEAN

Init ==
    /\ st = [t \in Txns |-> "idle"]
    /\ lockOf = [t \in Txns |-> NoLock]
    /\ index = {} /\ records = {}
    /\ copy = Census({})
    /\ stale = FALSE
    /\ bad = FALSE

(* The Lua check of a PREPARE's lock (write_conflict, reads_w_locked): a W  *)
(* lock meets any other lock on its key, an R lock another's W lock.        *)
Free(t, k, m) ==
    \A u \in index \ {t} :
        lockOf[u][1] = k => (m = "R" /\ lockOf[u][2] = "R")

Prepare(t, k, m) ==
    /\ st[t] = "idle" /\ ~stale
    /\ Free(t, k, m)
    /\ lockOf' = [lockOf EXCEPT ![t] = <<k, m>>]
    /\ index' = index \cup {t}
    /\ records' = records \cup {t}
    /\ copy' = IF BugCopyAfterYield \/ BugCopyOnQuorum THEN copy ELSE Inc(copy, <<k, m>>)
    /\ st' = [st EXCEPT ![t] = IF BugCopyAfterYield THEN "copying" ELSE "locked"]
    /\ UNCHANGED <<stale, bad>>

\* (BugCopyAfterYield) the step after the yield
CopyLate(t) ==
    /\ st[t] = "copying"
    /\ copy' = IF t \in index THEN Inc(copy, lockOf[t]) ELSE copy
    /\ st' = [st EXCEPT ![t] = "locked"]
    /\ UNCHANGED <<lockOf, index, records, stale, bad>>

(* The quorum confirms the PREPARE (BugCopyOnQuorum: and only now copies). *)
Confirm(t) ==
    /\ st[t] = "locked"
    /\ copy' = IF BugCopyOnQuorum /\ t \in index THEN Inc(copy, lockOf[t]) ELSE copy
    /\ st' = [st EXCEPT ![t] = "prepared"]
    /\ UNCHANGED <<lockOf, index, records, stale, bad>>

(* Its quorum fails: the PREPARE's local transaction rolls back, its record *)
(* goes, and the undo of its locks releases them.                           *)
QuorumFails(t) ==
    /\ st[t] = "locked"
    /\ records' = records \ {t}
    /\ index' = index \ {t}
    /\ copy' = IF t \in index /\ ~BugCopyOnQuorum THEN Dec(copy, lockOf[t]) ELSE copy
    /\ st' = [st EXCEPT ![t] = "done"]
    /\ UNCHANGED <<lockOf, stale, bad>>

(* APPLY or ABORT releases the lock (BugCopyReleaseFirst: the copy first). *)
Release(t) ==
    /\ st[t] = "prepared" /\ ~stale
    /\ IF BugCopyReleaseFirst
       THEN /\ copy' = IF t \in index THEN Dec(copy, lockOf[t]) ELSE copy
            /\ st' = [st EXCEPT ![t] = "releasing"]
            /\ UNCHANGED <<index, records>>
       ELSE /\ copy' = IF t \in index THEN Dec(copy, lockOf[t]) ELSE copy
            /\ index' = index \ {t}
            /\ records' = records \ {t}
            /\ st' = [st EXCEPT ![t] = "done"]
    /\ UNCHANGED <<lockOf, stale, bad>>

\* (BugCopyReleaseFirst) the step after
ReleaseLate(t) ==
    /\ st[t] = "releasing"
    /\ index' = index \ {t}
    /\ records' = records \ {t}
    /\ st' = [st EXCEPT ![t] = "done"]
    /\ UNCHANGED <<lockOf, copy, stale, bad>>

(* An election: the index is to be made again before the next step that    *)
(* checks it.                                                               *)
Elect ==
    /\ ~stale
    /\ stale' = TRUE
    /\ UNCHANGED <<st, lockOf, index, records, copy, bad>>

(* lk_reload: the index from the lock records, the copy cleared and made    *)
(* again with it (BugReloadNoCopy: left cleared).                           *)
Reload ==
    /\ stale
    /\ index' = records
    /\ copy' = IF BugReloadNoCopy THEN Census({}) ELSE Census(records)
    /\ stale' = FALSE
    /\ UNCHANGED <<st, lockOf, records, bad>>

(* Native steps: one of a transaction that holds no lock here, decided from *)
(* the copy, while the index is not stale (lk_stale).                       *)
NativeWrite(k) ==
    /\ ~stale
    /\ copy[k]["R"] + copy[k]["W"] = 0
    /\ bad' = (bad \/ \E u \in index : lockOf[u][1] = k)
    /\ UNCHANGED <<st, lockOf, index, records, copy, stale>>

NativeRead(k) ==
    /\ ~stale
    /\ copy[k]["W"] = 0
    /\ bad' = (bad \/ \E u \in index : lockOf[u] = <<k, "W">>)
    /\ UNCHANGED <<st, lockOf, index, records, copy, stale>>

Next ==
    \/ \E t \in Txns, k \in Keys, m \in Modes : Prepare(t, k, m)
    \/ \E t \in Txns : CopyLate(t) \/ Confirm(t) \/ QuorumFails(t) \/ Release(t) \/ ReleaseLate(t)
    \/ Elect \/ Reload
    \/ \E k \in Keys : NativeWrite(k) \/ NativeRead(k)

Spec == Init /\ [][Next]_vars

NativeFindsLocks == ~bad

(* Between steps -- none of them split in two by a bug, the index not      *)
(* stale -- the copy counts what the index holds.                          *)
CopyIsIndex ==
    (~stale /\ \A t \in Txns : st[t] \notin {"copying", "releasing"} /\ ~BugCopyOnQuorum)
        => copy = Census(index)
=============================================================================
