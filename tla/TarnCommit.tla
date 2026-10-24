------------------------------ MODULE TarnCommit ------------------------------
(***************************************************************************)
(* Tarn's transaction protocol over Tarantool shards.                      *)
(*                                                                         *)
(* Every SQL table is a plain Tarantool space ("data" below).  Clients may *)
(* read those spaces directly (DirectRead).  SQL transactions run in two   *)
(* phases:                                                                 *)
(*                                                                         *)
(*  Execution  reads go to the owning shard inside a Tarantool IPROTO      *)
(*             stream; Tarantool's MVCC tracks them.  Writes are buffered  *)
(*             in the Tarn server, so nothing reaches a shard yet.         *)
(*                                                                         *)
(*  Commit     single-shard:  one local Tarantool transaction validates    *)
(*             the reads (MVCC), checks the lock table and applies the     *)
(*             writes (CommitLocal).  Single-shard read-only transactions  *)
(*             see one MVCC snapshot (SnapshotRead).                       *)
(*                                                                         *)
(*             multi-shard: "parallel commit" 2PC.  PREPARE on every       *)
(*             participant validates reads, takes durable R/W locks and,   *)
(*             on the decision shard D(t), writes a STAGING record.  The   *)
(*             transaction is committed exactly when the record is STAGING *)
(*             and every participant is prepared (or COMMITTED was         *)
(*             recorded).  The coordinator then sends APPLY, which moves   *)
(*             the buffered values into the plain spaces and releases      *)
(*             locks; the client is acknowledged only after every APPLY.   *)
(*                                                                         *)
(* Faults: lost messages, coordinator crashes, a recovery agent that may   *)
(* run concurrently with a slow coordinator, and shard leader failovers    *)
(* that destroy the volatile MVCC read tracking of open streams.  Local    *)
(* Tarantool transactions are atomic steps here: on one shard they are     *)
(* serializable (memtx MVCC) and acknowledged only after the synchronous   *)
(* replication quorum, so they survive a failover.                        *)
(*                                                                         *)
(* The Bug* constants switch on deliberately weakened variants so that TLC *)
(* can demonstrate why each piece of the protocol is needed.              *)
(***************************************************************************)
EXTENDS Naturals, FiniteSets, Sequences, TLC

CONSTANTS
    Shards, Keys, Owner,        \* Owner \in [Keys -> Shards]
    Txns, RSet, WSet,           \* the program of each transaction
    Readers, RKeys,             \* direct (non-transactional) Tarantool readers
    MaxFailovers, MaxCrashes, MaxLoss, MaxTimeouts,
    BugNoReadLocks,             \* PREPARE takes no R locks on the read set
    BugNoWLockCheckOnRead,      \* PREPARE ignores W locks held on its read set
    BugNoTombstone,             \* recovery aborts without fencing participants
    BugLocalIgnoresRLocks,      \* single-shard commit ignores other txns' R locks
    AckOnPrepare,               \* option: acknowledge once all PREPAREs succeed
    ROValidateOnly,             \* option: cross-shard read-only txns validate, no PREPARE
    BugLocalReadsIgnoreWLocks   \* single-shard reads don't check W locks

ASSUME Owner \in [Keys -> Shards]
ASSUME RSet \in [Txns -> SUBSET Keys] /\ WSet \in [Txns -> SUBSET Keys]
ASSUME RKeys \in [Readers -> SUBSET Keys]

NoVal  == "init"     \* value of a key before any transaction wrote it
Unread == "unread"

Part(t)        == {Owner[k] : k \in RSet[t] \cup WSet[t]}
SingleShard(t) == Cardinality(Part(t)) = 1
ReadOnly(t)    == WSet[t] = {}
\* Decision shard: holds the transaction record.  Prefer a shard t writes to.
D(t) == IF WSet[t] # {} THEN CHOOSE s \in {Owner[k] : k \in WSet[t]} : TRUE
                        ELSE CHOOSE s \in Part(t) : TRUE
KeysOn(S, s) == {k \in S : Owner[k] = s}

VARIABLES
    data,        \* data[k]: writer of the committed value in the plain space
    locks,       \* locks[s]: set of [t, k, m] lock records (durable)
    part,        \* part[s][t] \in {"none","prepared","applied","aborted"}
    rec,         \* rec[t] \in {"none","staging","committed","aborted"} on D(t)
    coord,       \* coordinator state of t
    obs,         \* obs[t][k]: writer observed by t's read of k (or Unread)
    lost,        \* lost[t]: shards whose leader failed while t's stream was open
    msgs,        \* network: set of messages (may be lost, re-delivered)
    local,       \* outcome of single-shard commits: "none"/"committed"/"aborted"
    wh,          \* wh[k]: order in which committed writes were applied to data[k]
    everC,       \* history: t has been committed at some point
    acked,       \* history: client received COMMIT OK for these txns
    started,     \* history: t has issued its first operation
    rtPred,      \* history: txns already acknowledged when t started
    dobs, dpos,  \* history: direct reader observations (value, position in wh)
    dacked,      \* history: txns already acknowledged when a direct read happened
    dirty,       \* history: direct reader saw a value of an uncommitted txn
    nFail, nCrash, nLoss,
    recovering,  \* txns whose commit timed out; a recovery agent works on them
    nTimeout,
    validated    \* validated[t]: shards where read-only t passed validation

vars == <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, everC,
          acked, started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
          recovering, nTimeout, validated>>

Msg(type, t, s, yes) == [type |-> type, t |-> t, s |-> s, yes |-> yes]

\* t is committed now: explicitly, implicitly (parallel commit), or locally.
CommittedNow(t) ==
    \/ local[t] = "committed"
    \/ rec[t] = "committed"
    \/ rec[t] = "staging" /\ \A s \in Part(t) : part[s][t] \in {"prepared", "applied"}

Init ==
    /\ data    = [k \in Keys |-> NoVal]
    /\ locks   = [s \in Shards |-> {}]
    /\ part    = [s \in Shards |-> [t \in Txns |-> "none"]]
    /\ rec     = [t \in Txns |-> "none"]
    /\ coord   = [t \in Txns |-> "exec"]
    /\ obs     = [t \in Txns |-> [k \in Keys |-> Unread]]
    /\ lost    = [t \in Txns |-> {}]
    /\ msgs    = {}
    /\ local   = [t \in Txns |-> "none"]
    /\ wh      = [k \in Keys |-> <<>>]
    /\ everC   = [t \in Txns |-> FALSE]
    /\ acked   = {}
    /\ started = {}
    /\ rtPred  = [t \in Txns |-> {}]
    /\ dobs    = [r \in Readers |-> [k \in Keys |-> Unread]]
    /\ dpos    = [r \in Readers |-> [k \in Keys |-> 0]]
    /\ dacked  = [r \in Readers |-> [k \in Keys |-> {}]]
    /\ dirty   = [r \in Readers |-> FALSE]
    /\ nFail = 0 /\ nCrash = 0 /\ nLoss = 0
    /\ recovering = {} /\ nTimeout = 0
    /\ validated = [t \in Txns |-> {}]

\* Record the real-time predecessors of t at its first operation.
Start(t) ==
    IF t \in started THEN UNCHANGED <<started, rtPred>>
    ELSE /\ started' = started \cup {t}
         /\ rtPred' = [rtPred EXCEPT ![t] = acked]

ReadsDone(t) == \A k \in RSet[t] : obs[t][k] # Unread

-----------------------------------------------------------------------------
(* Execution phase *)

Read(t, k) ==
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ ~(SingleShard(t) /\ ReadOnly(t))
    /\ k \in RSet[t] /\ obs[t][k] = Unread
    /\ Owner[k] \notin lost[t]
    /\ obs' = [obs EXCEPT ![t][k] = data[k]]
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, coord, lost, msgs, local, wh,
                   acked, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

\* No other transaction holds a W lock on any key in S: a W lock may belong
\* to a transaction that is committed but not yet applied on this shard.
NoWLocks(t, S) == \A k \in S : ~\E l \in locks[Owner[k]] : l.t # t /\ l.k = k /\ l.m = "W"

\* A single-shard read-only transaction sees one MVCC read view and then
\* checks the lock table in that same view (autocommit SELECTs check before
\* reading).  If a read key is W-locked the read is retried, so the action
\* is simply not enabled in that state.
SnapshotRead(t) ==
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ SingleShard(t) /\ ReadOnly(t)
    /\ BugLocalReadsIgnoreWLocks \/ NoWLocks(t, RSet[t])
    /\ obs' = [obs EXCEPT ![t] = [k \in Keys |-> IF k \in RSet[t] THEN data[k] ELSE Unread]]
    /\ local' = [local EXCEPT ![t] = "committed"]
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ acked' = acked \cup {t}
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, lost, msgs, wh, dobs, dpos, dacked, dirty,
                   nFail, nCrash, nLoss, recovering, nTimeout, validated>>

\* Single-shard read-write commit: one local Tarantool transaction.
CommitLocal(t) ==
    LET s      == D(t)
        valid  == /\ s \notin lost[t]
                  /\ \A k \in RSet[t] : data[k] = obs[t][k]
                  /\ BugLocalReadsIgnoreWLocks \/ NoWLocks(t, RSet[t])
        noConf == \A l \in locks[s] :
                     l.t # t => (l.k \notin WSet[t] \/ (BugLocalIgnoresRLocks /\ l.m = "R"))
    IN
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ SingleShard(t) /\ ~ReadOnly(t) /\ ReadsDone(t)
    /\ IF valid /\ noConf
       THEN /\ data' = [k \in Keys |-> IF k \in WSet[t] THEN t ELSE data[k]]
            /\ wh' = [k \in Keys |-> IF k \in WSet[t] THEN Append(wh[k], t) ELSE wh[k]]
            /\ local' = [local EXCEPT ![t] = "committed"]
            /\ acked' = acked \cup {t}
       ELSE /\ local' = [local EXCEPT ![t] = "aborted"]
            /\ UNCHANGED <<data, wh, acked>>
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ Start(t)
    /\ UNCHANGED <<locks, part, rec, obs, lost, msgs, dobs, dpos, dacked, dirty,
                   nFail, nCrash, nLoss, recovering, nTimeout, validated>>

\* Cross-shard read-only transactions (option ROValidateOnly): no PREPARE and
\* no locks.  Each participant checks, in the stream's MVCC view, that the
\* reads are unchanged and that no other transaction W-locks them.
ValidateRO(t, s) ==
    LET rk    == KeysOn(RSet[t], s)
        valid == /\ s \notin lost[t]
                 /\ \A k \in rk : data[k] = obs[t][k]
                 /\ BugLocalReadsIgnoreWLocks \/ NoWLocks(t, rk)
    IN
    /\ ROValidateOnly
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ ~SingleShard(t) /\ ReadOnly(t) /\ ReadsDone(t)
    /\ s \in Part(t) /\ s \notin validated[t]
    /\ IF valid
       THEN /\ validated' = [validated EXCEPT ![t] = @ \cup {s}]
            /\ UNCHANGED <<local, coord>>
       ELSE /\ local' = [local EXCEPT ![t] = "aborted"]
            /\ coord' = [coord EXCEPT ![t] = "finished"]
            /\ UNCHANGED validated
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, msgs, wh, acked, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout>>

FinishRO(t) ==
    /\ ROValidateOnly
    /\ coord[t] = "exec" /\ local[t] = "none" /\ ~SingleShard(t) /\ ReadOnly(t)
    /\ validated[t] = Part(t)
    /\ local' = [local EXCEPT ![t] = "committed"]
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ acked' = acked \cup {t}
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, msgs, wh, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

-----------------------------------------------------------------------------
(* Multi-shard commit: coordinator *)

SendPrepare(t) ==
    /\ coord[t] = "exec" /\ ~SingleShard(t) /\ ReadsDone(t)
    /\ ~(ROValidateOnly /\ ReadOnly(t))
    /\ coord' = [coord EXCEPT ![t] = "preparing"]
    /\ msgs' = msgs \cup {Msg("prepare", t, s, FALSE) : s \in Part(t)}
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, local, wh, acked,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

CoordDecide(t) ==
    /\ coord[t] = "preparing"
    /\ \/ /\ \A s \in Part(t) : Msg("vote", t, s, TRUE) \in msgs
          /\ coord' = [coord EXCEPT ![t] = "applying"]
          /\ msgs' = msgs \cup {Msg("apply", t, s, FALSE) : s \in Part(t)}
          /\ acked' = IF AckOnPrepare THEN acked \cup {t} ELSE acked
       \/ /\ \E s \in Part(t) : Msg("vote", t, s, FALSE) \in msgs
          /\ coord' = [coord EXCEPT ![t] = "aborting"]
          /\ msgs' = msgs \cup {Msg("abort", t, s, FALSE) : s \in Part(t)}
          /\ UNCHANGED acked
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, local, wh, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

\* Acknowledge the client once every participant applied; mark the record.
CoordFinish(t) ==
    /\ coord[t] = "applying"
    /\ \A s \in Part(t) : part[s][t] = "applied"
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ acked' = acked \cup {t}
    /\ rec' = [rec EXCEPT ![t] = IF @ = "staging" THEN "committed" ELSE @]
    /\ UNCHANGED <<data, locks, part, obs, lost, msgs, local, wh, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

CoordAbortDone(t) ==
    /\ coord[t] = "aborting"
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

CoordCrash(t) ==
    /\ nCrash < MaxCrashes
    /\ coord[t] \in {"preparing", "applying", "aborting"}
    /\ coord' = [coord EXCEPT ![t] = "crashed"]
    /\ nCrash' = nCrash + 1
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nLoss, recovering, nTimeout, validated>>

-----------------------------------------------------------------------------
(* Multi-shard commit: participants (each action is one local transaction) *)

Prepare(t, s) ==
    LET rk       == KeysOn(RSet[t], s)
        wk       == KeysOn(WSet[t], s)
        others   == {l \in locks[s] : l.t # t}
        readsOK  == /\ s \notin lost[t]
                    /\ \A k \in rk : data[k] = obs[t][k]
                    /\ BugNoWLockCheckOnRead \/ ~\E l \in others : l.k \in rk /\ l.m = "W"
        writesOK == ~\E l \in others : l.k \in wk
        recOK    == s = D(t) => rec[t] = "none"
        newLocks == {[t |-> t, k |-> k, m |-> "W"] : k \in wk}
                    \cup (IF BugNoReadLocks THEN {}
                          ELSE {[t |-> t, k |-> k, m |-> "R"] : k \in rk \ wk})
    IN
    /\ Msg("prepare", t, s, FALSE) \in msgs
    /\ part[s][t] = "none"
    /\ IF readsOK /\ writesOK /\ recOK
       THEN /\ locks' = [locks EXCEPT ![s] = @ \cup newLocks]
            /\ part' = [part EXCEPT ![s][t] = "prepared"]
            /\ rec' = IF s = D(t) THEN [rec EXCEPT ![t] = "staging"] ELSE rec
            /\ msgs' = msgs \cup {Msg("vote", t, s, TRUE)}
       ELSE /\ part' = [part EXCEPT ![s][t] = "aborted"]
            /\ rec' = IF s = D(t) /\ rec[t] = "none"
                      THEN [rec EXCEPT ![t] = "aborted"] ELSE rec
            /\ msgs' = msgs \cup {Msg("vote", t, s, FALSE)}
            /\ UNCHANGED locks
    /\ UNCHANGED <<data, coord, obs, lost, local, wh, acked, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

\* A re-delivered PREPARE re-sends the vote implied by durable state.
Revote(t, s) ==
    /\ Msg("prepare", t, s, FALSE) \in msgs
    /\ part[s][t] # "none"
    /\ msgs' = msgs \cup {Msg("vote", t, s, part[s][t] \in {"prepared", "applied"})}
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

ApplyAt(t, s) ==
    LET wk == KeysOn(WSet[t], s) IN
    /\ Msg("apply", t, s, FALSE) \in msgs
    /\ part[s][t] = "prepared"
    /\ data' = [k \in Keys |-> IF k \in wk THEN t ELSE data[k]]
    /\ wh' = [k \in Keys |-> IF k \in wk THEN Append(wh[k], t) ELSE wh[k]]
    /\ locks' = [locks EXCEPT ![s] = {l \in @ : l.t # t}]
    /\ part' = [part EXCEPT ![s][t] = "applied"]
    /\ UNCHANGED <<rec, coord, obs, lost, msgs, local, acked, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

\* Abort releases locks; on a participant that never prepared it leaves a
\* tombstone, so a PREPARE that arrives later is refused.
AbortAt(t, s) ==
    /\ Msg("abort", t, s, FALSE) \in msgs
    /\ part[s][t] \in {"none", "prepared"}
    /\ locks' = [locks EXCEPT ![s] = {l \in @ : l.t # t}]
    /\ part' = [part EXCEPT ![s][t] = "aborted"]
    /\ UNCHANGED <<data, rec, coord, obs, lost, msgs, local, wh, acked, started,
                   rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

-----------------------------------------------------------------------------
(* Recovery: when t's commit times out, any Tarn server may resolve it,    *)
(* possibly while the original coordinator is merely slow (not crashed).   *)

Timeout(t) ==
    /\ nTimeout < MaxTimeouts
    /\ coord[t] # "exec" /\ ~SingleShard(t) /\ t \notin recovering
    /\ ~(ROValidateOnly /\ ReadOnly(t))   \* validate-only txns leave no records
    /\ recovering' = recovering \cup {t}
    /\ nTimeout' = nTimeout + 1
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, validated>>

\* No record yet: fence D(t) by recording ABORTED (refuses a late PREPARE).
RecoverFenceRecord(t) ==
    /\ t \in recovering
    /\ rec[t] = "none"
    /\ rec' = [rec EXCEPT ![t] = "aborted"]
    /\ msgs' = msgs \cup {Msg("abort", t, s, FALSE) : s \in Part(t)}
    /\ UNCHANGED <<data, locks, part, coord, obs, lost, local, wh, acked, started,
                   rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

\* STAGING but some participant never prepared: fence it with a tombstone.
\* (Bug variant: give up on the transaction without fencing.)
RecoverFenceParticipant(t, s) ==
    /\ t \in recovering
    /\ rec[t] = "staging" /\ s \in Part(t) /\ part[s][t] = "none"
    /\ IF BugNoTombstone
       THEN /\ rec' = [rec EXCEPT ![t] = "aborted"]
            /\ msgs' = msgs \cup {Msg("abort", t, p, FALSE) : p \in Part(t) \ {s}}
            /\ UNCHANGED part
       ELSE /\ part' = [part EXCEPT ![s][t] = "aborted"]
            /\ UNCHANGED <<rec, msgs, recovering, nTimeout, validated>>
    /\ UNCHANGED <<data, locks, coord, obs, lost, local, wh, acked, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

RecoverDecide(t) ==
    /\ t \in recovering
    /\ \/ /\ rec[t] = "staging"
          /\ \A s \in Part(t) : part[s][t] \in {"prepared", "applied"}
          /\ rec' = [rec EXCEPT ![t] = "committed"]
          /\ msgs' = msgs \cup {Msg("apply", t, s, FALSE) : s \in Part(t)}
       \/ /\ rec[t] = "staging"
          /\ \E s \in Part(t) : part[s][t] = "aborted"
          /\ rec' = [rec EXCEPT ![t] = "aborted"]
          /\ msgs' = msgs \cup {Msg("abort", t, s, FALSE) : s \in Part(t)}
       \/ /\ rec[t] = "committed"
          /\ msgs' = msgs \cup {Msg("apply", t, s, FALSE) : s \in Part(t)}
          /\ UNCHANGED rec
       \/ /\ rec[t] = "aborted"
          /\ msgs' = msgs \cup {Msg("abort", t, s, FALSE) : s \in Part(t)}
          /\ UNCHANGED rec
    /\ UNCHANGED <<data, locks, part, coord, obs, lost, local, wh, acked, started,
                   rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

-----------------------------------------------------------------------------
(* Environment *)

\* A shard leader fails over: durable state survives (quorum-replicated),
\* the MVCC tracking of streams still in their execution phase does not.
Failover(s) ==
    /\ nFail < MaxFailovers
    /\ lost' = [t \in Txns |-> IF coord[t] = "exec" /\ local[t] = "none"
                               THEN lost[t] \cup {s} ELSE lost[t]]
    /\ nFail' = nFail + 1
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nCrash, nLoss, recovering, nTimeout, validated>>

Lose(m) ==
    /\ nLoss < MaxLoss
    /\ m \in msgs
    /\ msgs' = msgs \ {m}
    /\ nLoss' = nLoss + 1
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, recovering, nTimeout, validated>>

\* A plain Tarantool get on the owning shard, bypassing Tarn entirely.
DirectRead(r, k) ==
    /\ k \in RKeys[r] /\ dobs[r][k] = Unread
    /\ dobs' = [dobs EXCEPT ![r][k] = data[k]]
    /\ dpos' = [dpos EXCEPT ![r][k] = Len(wh[k])]
    /\ dacked' = [dacked EXCEPT ![r][k] = acked]
    /\ dirty' = [dirty EXCEPT ![r] = @ \/ ~(data[k] = NoVal \/ everC[data[k]] \/ CommittedNow(data[k]))]
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, acked,
                   started, rtPred, nFail, nCrash, nLoss, recovering, nTimeout, validated>>

-----------------------------------------------------------------------------

\* everC is a history variable: it latches CommittedNow.
H == everC' = [t \in Txns |-> everC[t] \/ CommittedNow(t)']

\* Each action also latches the history variable everC.
ReadH(t, k) == Read(t, k) /\ H
SnapshotReadH(t) == SnapshotRead(t) /\ H
CommitLocalH(t) == CommitLocal(t) /\ H
SendPrepareH(t) == SendPrepare(t) /\ H
CoordDecideH(t) == CoordDecide(t) /\ H
CoordFinishH(t) == CoordFinish(t) /\ H
CoordAbortDoneH(t) == CoordAbortDone(t) /\ H
CoordCrashH(t) == CoordCrash(t) /\ H
PrepareH(t, s) == Prepare(t, s) /\ H
RevoteH(t, s) == Revote(t, s) /\ H
ApplyAtH(t, s) == ApplyAt(t, s) /\ H
AbortAtH(t, s) == AbortAt(t, s) /\ H
RecoverFenceRecordH(t) == RecoverFenceRecord(t) /\ H
RecoverFenceParticipantH(t, s) == RecoverFenceParticipant(t, s) /\ H
RecoverDecideH(t) == RecoverDecide(t) /\ H
ValidateROH(t, s) == ValidateRO(t, s) /\ H
FinishROH(t) == FinishRO(t) /\ H
TimeoutH(t) == Timeout(t) /\ H
FailoverH(s) == Failover(s) /\ H
LoseH(m) == Lose(m) /\ H
DirectReadH(r, k) == DirectRead(r, k) /\ H

Next ==
    \/ \E t \in Txns, k \in Keys : ReadH(t, k)
    \/ \E t \in Txns : SnapshotReadH(t)
    \/ \E t \in Txns : CommitLocalH(t)
    \/ \E t \in Txns : SendPrepareH(t)
    \/ \E t \in Txns : CoordDecideH(t)
    \/ \E t \in Txns : CoordFinishH(t)
    \/ \E t \in Txns : CoordAbortDoneH(t)
    \/ \E t \in Txns : CoordCrashH(t)
    \/ \E t \in Txns, s \in Shards : PrepareH(t, s)
    \/ \E t \in Txns, s \in Shards : RevoteH(t, s)
    \/ \E t \in Txns, s \in Shards : ApplyAtH(t, s)
    \/ \E t \in Txns, s \in Shards : AbortAtH(t, s)
    \/ \E t \in Txns : RecoverFenceRecordH(t)
    \/ \E t \in Txns, s \in Shards : RecoverFenceParticipantH(t, s)
    \/ \E t \in Txns : RecoverDecideH(t)
    \/ \E t \in Txns : TimeoutH(t)
    \/ \E t \in Txns, s \in Shards : ValidateROH(t, s)
    \/ \E t \in Txns : FinishROH(t)
    \/ \E s \in Shards : FailoverH(s)
    \/ \E m \in msgs : LoseH(m)
    \/ \E r \in Readers, k \in Keys : DirectReadH(r, k)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

Vals == Txns \cup {NoVal}

TypeOK ==
    /\ data \in [Keys -> Vals]
    /\ \A s \in Shards : locks[s] \subseteq [t : Txns, k : Keys, m : {"R", "W"}]
    /\ part \in [Shards -> [Txns -> {"none", "prepared", "applied", "aborted"}]]
    /\ rec \in [Txns -> {"none", "staging", "committed", "aborted"}]
    /\ obs \in [Txns -> [Keys -> Vals \cup {Unread}]]
    /\ local \in [Txns -> {"none", "committed", "aborted"}]

\* Atomicity: writes reach a plain space only for committed transactions ...
AppliedOnlyIfCommitted ==
    \A s \in Shards, t \in Txns : part[s][t] = "applied" => everC[t]
\* ... and a committed transaction is never rolled back anywhere.
NoAbortAfterCommit ==
    \A t \in Txns : everC[t] => \A s \in Part(t) : part[s][t] # "aborted"

\* The plain Tarantool spaces only ever hold committed values, so direct
\* key/value readers never see uncommitted data.
PlainOnlyCommitted == \A k \in Keys : data[k] = NoVal \/ everC[data[k]]
DirectReadsClean   == \A r \in Readers : ~dirty[r]

\* Strict serializability of committed SQL transactions: some total order
\* (1) explains every value each transaction read,
\* (2) agrees with the order writes were applied to each plain space, and
\* (3) respects real time (acknowledged before t started => before t).
Committed == {t \in Txns : everC[t]}
Perms(S) == {f \in [1..Cardinality(S) -> S] :
                \A i, j \in 1..Cardinality(S) : i # j => f[i] # f[j]}
Max(S) == CHOOSE m \in S : \A n \in S : n <= m
LastWriter(f, i, k) ==
    LET ws == {j \in 1..(i - 1) : k \in WSet[f[j]]}
    IN IF ws = {} THEN NoVal ELSE f[Max(ws)]
Pos(f, t) == CHOOSE i \in DOMAIN f : f[i] = t
ReadsExplained(f) ==
    \A i \in DOMAIN f : \A k \in RSet[f[i]] : obs[f[i]][k] = LastWriter(f, i, k)
WritesOrdered(f) ==
    \A k \in Keys :
        LET seq == SelectSeq(f, LAMBDA t : k \in WSet[t])
        IN /\ Len(wh[k]) <= Len(seq)
           /\ \A i \in 1..Len(wh[k]) : wh[k][i] = seq[i]
RealTimeRespected(f) ==
    \A t2 \in Committed : \A t1 \in rtPred[t2] \cap Committed : Pos(f, t1) < Pos(f, t2)
StrictSerializable ==
    \E f \in Perms(Committed) :
        ReadsExplained(f) /\ WritesOrdered(f) /\ RealTimeRespected(f)

\* A direct read that starts after a transaction was acknowledged sees its
\* write (read-your-writes across the SQL and key/value interfaces).  Holds
\* when the client is acknowledged after APPLY; AckOnPrepare gives it up.
DirectReadYourWrites ==
    \A r \in Readers, k \in Keys :
        dobs[r][k] # Unread =>
            \A t \in dacked[r][k] :
                k \in WSet[t] => \E i \in 1..dpos[r][k] : wh[k][i] = t

\* NOT guaranteed (documented trade-off): a direct reader that reads several
\* keys on different shards may see a multi-shard transaction half applied.
DirectReadAtomic ==
    \A r \in Readers, t \in Txns :
        ~\E k1, k2 \in WSet[t] :
            /\ dobs[r][k1] # Unread /\ dobs[r][k2] # Unread
            /\ \E i \in 1..dpos[r][k1] : wh[k1][i] = t
            /\ ~\E i \in 1..dpos[r][k2] : wh[k2][i] = t

=============================================================================
