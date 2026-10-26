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
(* Record lifecycle (option GC).  A PREPARE is refused once its           *)
(* transaction has expired (it is older than max_txn_age; Expire models    *)
(* that moment), and records are garbage-collected only after expiry: an   *)
(* applied participant record once the decision record is final or gone,   *)
(* an aborted one (tombstone) any time, a decision record once it is final *)
(* and no participant is prepared.  Recovery fences a missing decision     *)
(* record only before expiry or while some participant is prepared:        *)
(* otherwise the record may have been collected.  With ApplyMarksCommitted *)
(* the decision shard's APPLY records COMMITTED (as the implementation     *)
(* does) instead of the coordinator after every APPLY.                     *)
(*                                                                         *)
(* READ COMMITTED (transactions in RC).  Writes and locking reads take    *)
(* pessimistic row locks (plocks) on the owning shard, queueing behind     *)
(* holders, and read the latest committed value under the lock (lobs);     *)
(* writes are computed from it and buffered as before.  Plain reads see    *)
(* the latest committed value per read and are never validated.  The lock *)
(* table is volatile: a leader failover empties it, so a commit first      *)
(* checks that the transaction still holds its locks.  Commits go through  *)
(* the same paths (a local commit, or PREPARE/APPLY, which releases the    *)
(* locks); serializable commits respect other transactions' plocks.        *)
(* A local commit is atomic here except for one window that matters to     *)
(* locks: until the WAL has it, readers outside it still see the previous  *)
(* versions (infl), so a lock granted meanwhile must wait before reading.  *)
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
    BugLocalReadsIgnoreWLocks,  \* single-shard reads don't check W locks
    BugROValidateNoReadCheck,   \* ValidateRO skips "reads unchanged" (memtx read views)
    ApplyMarksCommitted,        \* option: D's APPLY records COMMITTED (implementation)
    GC,                         \* option: expiry and garbage collection of records
    BugGCIgnoresDecision,       \* GC deletes applied participant records regardless of rec
    BugGCNoExpiry,              \* GC does not wait for expiry
    BugFenceAfterExpiry,        \* recovery fences a missing record even after expiry
    RC, LSet,                   \* READ COMMITTED txns; keys each one locks (WSet \subseteq LSet)
    BugRCNoLockCheck,           \* RC commit does not check that its locks survived
    BugRCLockIgnoresWLocks,     \* RC lock granted while a prepared txn W-locks the key
    BugRCReadBeforeLock,        \* RC write computed from the value read before locking
    BugIgnorePLocks,            \* serializable commits ignore other txns' plocks
    BugRCLockReadsInFlight,     \* RC lock reads a key while a local commit to it is in flight
    IKeys,                      \* IKeys[t] \subseteq WSet[t]: keys t inserts (fails if present)
    BugInsertNoExistCheck       \* commits do not check that inserted keys are free

ASSUME Owner \in [Keys -> Shards]
ASSUME RSet \in [Txns -> SUBSET Keys] /\ WSet \in [Txns -> SUBSET Keys]
ASSUME RKeys \in [Readers -> SUBSET Keys]
ASSUME RC \subseteq Txns /\ LSet \in [Txns -> SUBSET Keys]
ASSUME \A t \in Txns : IF t \in RC THEN WSet[t] \subseteq LSet[t] ELSE LSet[t] = {}
ASSUME IKeys \in [Txns -> SUBSET Keys] /\ \A t \in Txns : IKeys[t] \subseteq WSet[t]

NoVal  == "init"     \* value of a key before any transaction wrote it
Unread == "unread"

\* Participants: shards an RC transaction locks rows on (its plain reads are
\* not validated); shards a serializable transaction reads or writes.
Part(t)        == IF t \in RC THEN {Owner[k] : k \in LSet[t]}
                  ELSE {Owner[k] : k \in RSet[t] \cup WSet[t]}
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
    validated,   \* validated[t]: shards where read-only t passed validation
    expired,     \* txns whose PREPAREs are refused from now on (GC)
    plocks,      \* plocks[s]: set of [t, k] pessimistic row locks (volatile)
    lobs,        \* lobs[t][k]: writer of the value RC txn t read under its lock on k
    rcpos,       \* history: position in wh[k] seen by RC t's plain read of k
    rcacked,     \* history: txns already acknowledged when that read happened
    lostUpd,     \* history: an RC write was applied over a value it did not read
    infl         \* [t, k, prev]: t's local commit wrote k and is not in the WAL yet;
                 \* other readers still see prev (tracked only when RC # {})

rcvars == <<plocks, lobs, rcpos, rcacked, lostUpd, infl>>
vars == <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, everC,
          acked, started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
          recovering, nTimeout, validated, expired, plocks, lobs, rcpos, rcacked, lostUpd, infl>>

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
    /\ expired = {}
    /\ plocks  = [s \in Shards |-> {}]
    /\ lobs    = [t \in Txns |-> [k \in Keys |-> Unread]]
    /\ rcpos   = [t \in Txns |-> [k \in Keys |-> 0]]
    /\ rcacked = [t \in Txns |-> [k \in Keys |-> {}]]
    /\ lostUpd = FALSE
    /\ infl    = {}

\* Record the real-time predecessors of t at its first operation.
Start(t) ==
    IF t \in started THEN UNCHANGED <<started, rtPred>>
    ELSE /\ started' = started \cup {t}
         /\ rtPred' = [rtPred EXCEPT ![t] = acked]

ReadsDone(t) == /\ \A k \in RSet[t] : obs[t][k] # Unread
                /\ \A k \in LSet[t] : lobs[t][k] # Unread

-----------------------------------------------------------------------------
(* Execution phase *)

Read(t, k) ==
    /\ t \notin RC
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ ~(SingleShard(t) /\ ReadOnly(t))
    /\ k \in RSet[t] /\ obs[t][k] = Unread
    /\ Owner[k] \notin lost[t]
    /\ obs' = [obs EXCEPT ![t][k] = data[k]]
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, coord, lost, msgs, local, wh,
                   acked, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

\* No other transaction holds a W lock on any key in S: a W lock may belong
\* to a transaction that is committed but not yet applied on this shard.
NoWLocks(t, S) == \A k \in S : ~\E l \in locks[Owner[k]] : l.t # t /\ l.k = k /\ l.m = "W"

\* Inserts (IKeys): a key a transaction inserts must not hold a value yet.
\* This is how a unique key is enforced across shards: its global index
\* entry is a key on the shard its value hashes to, inserted in the same
\* transaction as the row, so two rows with one value insert one key.
InsertsFree(S) == BugInsertNoExistCheck \/ \A k \in S : data[k] = NoVal

\* A single-shard read-only transaction sees one MVCC read view and then
\* checks the lock table in that same view (autocommit SELECTs check before
\* reading).  If a read key is W-locked the read is retried, so the action
\* is simply not enabled in that state.
SnapshotRead(t) ==
    /\ t \notin RC
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ SingleShard(t) /\ ReadOnly(t)
    /\ BugLocalReadsIgnoreWLocks \/ NoWLocks(t, RSet[t])
    /\ obs' = [obs EXCEPT ![t] = [k \in Keys |-> IF k \in RSet[t] THEN data[k] ELSE Unread]]
    /\ local' = [local EXCEPT ![t] = "committed"]
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ acked' = acked \cup {t}
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, lost, msgs, wh, dobs, dpos, dacked, dirty,
                   nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

\* Single-shard read-write commit: one local Tarantool transaction.
CommitLocal(t) ==
    LET s      == D(t)
        valid  == /\ s \notin lost[t]
                  /\ \A k \in RSet[t] : data[k] = obs[t][k]
                  /\ BugLocalReadsIgnoreWLocks \/ NoWLocks(t, RSet[t])
        noConf == /\ \A l \in locks[s] :
                        l.t # t => (l.k \notin WSet[t] \/ (BugLocalIgnoresRLocks /\ l.m = "R"))
                  /\ BugIgnorePLocks \/ ~\E pl \in plocks[s] : pl.t # t /\ pl.k \in WSet[t]
    IN
    /\ t \notin RC
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ SingleShard(t) /\ ~ReadOnly(t) /\ ReadsDone(t)
    /\ IF valid /\ noConf /\ InsertsFree(IKeys[t])
       THEN /\ data' = [k \in Keys |-> IF k \in WSet[t] THEN t ELSE data[k]]
            /\ wh' = [k \in Keys |-> IF k \in WSet[t] THEN Append(wh[k], t) ELSE wh[k]]
            /\ local' = [local EXCEPT ![t] = "committed"]
            /\ acked' = acked \cup {t}
            /\ infl' = IF RC = {} THEN infl
                       ELSE infl \cup {[t |-> t, k |-> k, prev |-> data[k]] : k \in WSet[t]}
       ELSE /\ local' = [local EXCEPT ![t] = "aborted"]
            /\ UNCHANGED <<data, wh, acked, infl>>
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ Start(t)
    /\ UNCHANGED <<locks, part, rec, obs, lost, msgs, dobs, dpos, dacked, dirty,
                   nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

\* The WAL (and the replication quorum) has t's local commit: readers
\* outside it see its writes from now on.  (Atomic with CommitLocal for
\* every reader but RCLock: see InFlight.)
ConfirmLocal(t) ==
    /\ \E i \in infl : i.t = t
    /\ infl' = {i \in infl : i.t # t}
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
                   recovering, nTimeout, validated, expired, plocks, lobs, rcpos, rcacked, lostUpd>>

\* Cross-shard read-only transactions (option ROValidateOnly): no PREPARE and
\* no locks.  Each participant checks, in the stream's MVCC view, that the
\* reads are unchanged and that no other transaction W-locks them.
\* Implementation note: memtx MVCC does not abort a read-only transaction
\* whose reads were overwritten -- it moves it into a read view, where it
\* could still commit.  "Reads unchanged" is therefore enforced with a probe
\* write (refused from a read view); BugROValidateNoReadCheck models the
\* implementation without the probe.
ValidateRO(t, s) ==
    LET rk    == KeysOn(RSet[t], s)
        valid == /\ s \notin lost[t]
                 /\ BugROValidateNoReadCheck \/ \A k \in rk : data[k] = obs[t][k]
                 /\ BugLocalReadsIgnoreWLocks \/ NoWLocks(t, rk)
    IN
    /\ ROValidateOnly /\ t \notin RC
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
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, expired>>

FinishRO(t) ==
    /\ ROValidateOnly /\ t \notin RC
    /\ coord[t] = "exec" /\ local[t] = "none" /\ ~SingleShard(t) /\ ReadOnly(t)
    /\ validated[t] = Part(t)
    /\ local' = [local EXCEPT ![t] = "committed"]
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ acked' = acked \cup {t}
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, msgs, wh, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

-----------------------------------------------------------------------------
(* Multi-shard commit: coordinator *)

SendPrepare(t) ==
    /\ coord[t] = "exec" /\ local[t] = "none" /\ ~SingleShard(t) /\ ReadsDone(t)
    /\ ~(ROValidateOnly /\ ReadOnly(t))
    /\ t \in RC => ~ReadOnly(t)     \* read-only RC txns just release their locks
    /\ coord' = [coord EXCEPT ![t] = "preparing"]
    /\ msgs' = msgs \cup {Msg("prepare", t, s, FALSE) : s \in Part(t)}
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, local, wh, acked,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

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
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

\* Acknowledge the client once every participant applied; mark the record.
CoordFinish(t) ==
    /\ coord[t] = "applying"
    /\ \A s \in Part(t) : part[s][t] = "applied"
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ acked' = acked \cup {t}
    /\ rec' = [rec EXCEPT ![t] = IF @ = "staging" THEN "committed" ELSE @]
    /\ UNCHANGED <<data, locks, part, obs, lost, msgs, local, wh, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

CoordAbortDone(t) ==
    /\ coord[t] = "aborting"
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

CoordCrash(t) ==
    /\ nCrash < MaxCrashes
    /\ coord[t] \in {"preparing", "applying", "aborting"}
    /\ coord' = [coord EXCEPT ![t] = "crashed"]
    /\ nCrash' = nCrash + 1
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nLoss, recovering, nTimeout, validated, expired>>

-----------------------------------------------------------------------------
(* Multi-shard commit: participants (each action is one local transaction) *)

Prepare(t, s) ==
    LET rk       == KeysOn(RSet[t], s)
        wk       == KeysOn(WSet[t], s)
        others   == {l \in locks[s] : l.t # t}
        \* RC: no read validation, but the locks must have survived.
        readsOK  == IF t \in RC
                    THEN BugRCNoLockCheck \/ \A k \in KeysOn(LSet[t], s) : [t |-> t, k |-> k] \in plocks[s]
                    ELSE /\ s \notin lost[t]
                         /\ \A k \in rk : data[k] = obs[t][k]
                         /\ BugNoWLockCheckOnRead \/ ~\E l \in others : l.k \in rk /\ l.m = "W"
        writesOK == /\ ~\E l \in others : l.k \in wk
                    /\ BugIgnorePLocks \/ ~\E pl \in plocks[s] : pl.t # t /\ pl.k \in wk
        recOK    == s = D(t) => rec[t] = "none"
        newLocks == {[t |-> t, k |-> k, m |-> "W"] : k \in wk}
                    \cup (IF BugNoReadLocks \/ t \in RC THEN {}
                          ELSE {[t |-> t, k |-> k, m |-> "R"] : k \in rk \ wk})
    IN
    /\ Msg("prepare", t, s, FALSE) \in msgs
    /\ part[s][t] = "none"
    \* An expired transaction's PREPARE is refused without any effect: no
    \* record, and no vote either -- a (re-delivered) PREPARE may meet a
    \* participant whose records were already collected, so a NO here could
    \* contradict a commit.  The coordinator treats the reply as unknown.
    /\ t \notin expired
    /\ IF readsOK /\ writesOK /\ recOK /\ InsertsFree(KeysOn(IKeys[t], s))
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
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

\* A re-delivered PREPARE re-sends the vote implied by durable state.
Revote(t, s) ==
    /\ Msg("prepare", t, s, FALSE) \in msgs
    /\ part[s][t] # "none"
    /\ msgs' = msgs \cup {Msg("vote", t, s, part[s][t] \in {"prepared", "applied"})}
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

\* With ApplyMarksCommitted the decision shard's APPLY also records COMMITTED
\* (and refuses to apply if its record says ABORTED, which never happens).
ApplyAt(t, s) ==
    LET wk == KeysOn(WSet[t], s) IN
    /\ Msg("apply", t, s, FALSE) \in msgs
    /\ part[s][t] = "prepared"
    /\ ApplyMarksCommitted /\ s = D(t) => rec[t] # "aborted"
    /\ data' = [k \in Keys |-> IF k \in wk THEN t ELSE data[k]]
    /\ wh' = [k \in Keys |-> IF k \in wk THEN Append(wh[k], t) ELSE wh[k]]
    /\ locks' = [locks EXCEPT ![s] = {l \in @ : l.t # t}]
    /\ part' = [part EXCEPT ![s][t] = "applied"]
    /\ rec' = IF ApplyMarksCommitted /\ s = D(t) /\ rec[t] = "staging"
              THEN [rec EXCEPT ![t] = "committed"] ELSE rec
    /\ UNCHANGED <<coord, obs, lost, msgs, local, acked, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

\* Abort releases locks; on a participant that never prepared it leaves a
\* tombstone, so a PREPARE that arrives later is refused.
AbortAt(t, s) ==
    /\ Msg("abort", t, s, FALSE) \in msgs
    /\ part[s][t] \in {"none", "prepared"}
    /\ locks' = [locks EXCEPT ![s] = {l \in @ : l.t # t}]
    /\ part' = [part EXCEPT ![s][t] = "aborted"]
    /\ UNCHANGED <<data, rec, coord, obs, lost, msgs, local, wh, acked, started,
                   rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

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
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, validated, expired>>

\* No record yet: fence D(t) by recording ABORTED (refuses a late PREPARE).
\* After expiry a missing record may have been garbage-collected, so it is
\* only fenced while some participant is prepared (which rules that out).
RecoverFenceRecord(t) ==
    /\ t \in recovering
    /\ rec[t] = "none"
    /\ \/ t \notin expired
       \/ \E s \in Part(t) : part[s][t] = "prepared"
       \/ BugFenceAfterExpiry
    /\ rec' = [rec EXCEPT ![t] = "aborted"]
    /\ msgs' = msgs \cup {Msg("abort", t, s, FALSE) : s \in Part(t)}
    /\ UNCHANGED <<data, locks, part, coord, obs, lost, local, wh, acked, started,
                   rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

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
            /\ UNCHANGED <<rec, msgs, recovering, nTimeout, validated, expired>>
    /\ UNCHANGED <<data, locks, coord, obs, lost, local, wh, acked, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

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
                   rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

-----------------------------------------------------------------------------
(* READ COMMITTED transactions (t \in RC) *)

\* A plain read: the latest committed value.  Like an autocommit read it
\* waits while a prepared transaction W-locks the key (that transaction may
\* be committed and acknowledged but not applied here yet).  Never validated.
RCRead(t, k) ==
    /\ t \in RC /\ coord[t] = "exec" /\ local[t] = "none"
    /\ k \in RSet[t] /\ obs[t][k] = Unread
    /\ NoWLocks(t, {k})
    /\ obs' = [obs EXCEPT ![t][k] = data[k]]
    /\ rcpos' = [rcpos EXCEPT ![t][k] = Len(wh[k])]
    /\ rcacked' = [rcacked EXCEPT ![t][k] = acked]
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, coord, lost, msgs, local, wh, acked,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout,
                   validated, expired, plocks, lobs, lostUpd, infl>>

\* A local commit that wrote k is in flight; readers see the value before it.
InFlight(k) == \E i \in infl : i.k = k
Confirmed(k) == IF InFlight(k) THEN (CHOOSE i \in infl : i.k = k).prev ELSE data[k]

\* Take the pessimistic lock on k (granted when nobody else holds it) and
\* read the latest committed value in the same step.  A prepared
\* transaction's W lock also makes the request wait: its value may already
\* be committed; so does a local commit of k still in flight (writers that
\* hold no lock check for locks before they write, so only commits already
\* in flight when the lock is granted can still change k).  (Bug variants:
\* the write is computed from the value of an earlier plain read; the lock
\* reads k while a local commit to it is in flight, i.e. the value before.)
RCLock(t, k) ==
    /\ t \in RC /\ coord[t] = "exec" /\ local[t] = "none"
    /\ k \in LSet[t] /\ lobs[t][k] = Unread
    /\ BugRCReadBeforeLock => (k \in RSet[t] => obs[t][k] # Unread)
    /\ ~\E pl \in plocks[Owner[k]] : pl.t # t /\ pl.k = k
    /\ BugRCLockIgnoresWLocks \/ NoWLocks(t, {k})
    /\ BugRCLockReadsInFlight \/ ~InFlight(k)
    /\ plocks' = [plocks EXCEPT ![Owner[k]] = @ \cup {[t |-> t, k |-> k]}]
    /\ lobs' = [lobs EXCEPT ![t][k] = IF BugRCReadBeforeLock /\ k \in RSet[t]
                                      THEN obs[t][k] ELSE Confirmed(k)]
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, acked,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout,
                   validated, expired, rcpos, rcacked, lostUpd, infl>>

HoldsLocks(t) == \A k \in LSet[t] : [t |-> t, k |-> k] \in plocks[Owner[k]]

\* Commit of a read-only RC transaction (on any shards: it just releases its
\* locks) or of a single-shard RC transaction with writes: one local
\* transaction that checks the locks survived, waits out prepared
\* transactions' locks on what it writes, applies the writes and releases
\* the locks.
CommitRC(t) ==
    LET intact == BugRCNoLockCheck \/ HoldsLocks(t) IN
    /\ t \in RC /\ coord[t] = "exec" /\ local[t] = "none" /\ ReadsDone(t)
    /\ ReadOnly(t) \/ SingleShard(t)
    /\ ~\E s \in Shards : \E l \in locks[s] : l.t # t /\ l.k \in WSet[t]
    /\ IF ReadOnly(t) \/ (intact /\ InsertsFree(IKeys[t]))
       THEN /\ data' = [k \in Keys |-> IF k \in WSet[t] THEN t ELSE data[k]]
            /\ wh' = [k \in Keys |-> IF k \in WSet[t] THEN Append(wh[k], t) ELSE wh[k]]
            /\ lostUpd' = (lostUpd \/ \E k \in WSet[t] : data[k] # lobs[t][k])
            /\ local' = [local EXCEPT ![t] = "committed"]
            /\ acked' = acked \cup {t}
       ELSE /\ local' = [local EXCEPT ![t] = "aborted"]
            /\ UNCHANGED <<data, wh, acked, lostUpd>>
    /\ plocks' = [s \in Shards |-> {l \in plocks[s] : l.t # t}]
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ Start(t)
    /\ UNCHANGED <<locks, part, rec, obs, lost, msgs, dobs, dpos, dacked, dirty,
                   nFail, nCrash, nLoss, recovering, nTimeout, validated, expired,
                   lobs, rcpos, rcacked, infl>>

\* Rollback during execution (by the client, a deadlock or a lock wait
\* timeout): release the locks.
RCAbort(t) ==
    /\ t \in RC /\ coord[t] = "exec" /\ local[t] = "none"
    /\ local' = [local EXCEPT ![t] = "aborted"]
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ plocks' = [s \in Shards |-> {l \in plocks[s] : l.t # t}]
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, msgs, wh, acked, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout,
                   validated, expired, lobs, rcpos, rcacked, lostUpd, infl>>

\* What APPLY / ABORT do to the RC state on shard s: release t's locks; an
\* RC write applied over a value t did not read under its lock is a lost
\* update.
RCApplied(t, s) ==
    /\ plocks' = [plocks EXCEPT ![s] = {l \in @ : l.t # t}]
    /\ lostUpd' = (lostUpd \/ (t \in RC /\ part[s][t] = "prepared"
                               /\ \E k \in KeysOn(WSet[t], s) : data[k] # lobs[t][k]))
    /\ UNCHANGED <<lobs, rcpos, rcacked, infl>>
RCReleased(t, s) ==
    /\ plocks' = [plocks EXCEPT ![s] = {l \in @ : l.t # t}]
    /\ UNCHANGED <<lobs, rcpos, rcacked, lostUpd, infl>>

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
                   started, rtPred, dobs, dpos, dacked, dirty, nCrash, nLoss, recovering, nTimeout, validated, expired>>

Lose(m) ==
    /\ nLoss < MaxLoss
    /\ m \in msgs
    /\ msgs' = msgs \ {m}
    /\ nLoss' = nLoss + 1
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, recovering, nTimeout, validated, expired>>

\* A plain Tarantool get on the owning shard, bypassing Tarn entirely.
DirectRead(r, k) ==
    /\ k \in RKeys[r] /\ dobs[r][k] = Unread
    /\ dobs' = [dobs EXCEPT ![r][k] = data[k]]
    /\ dpos' = [dpos EXCEPT ![r][k] = Len(wh[k])]
    /\ dacked' = [dacked EXCEPT ![r][k] = acked]
    /\ dirty' = [dirty EXCEPT ![r] = @ \/ ~(data[k] = NoVal \/ everC[data[k]] \/ CommittedNow(data[k]))]
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, acked,
                   started, rtPred, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

-----------------------------------------------------------------------------
(* Record lifecycle (option GC) *)

\* max_txn_age has passed since t started: its PREPAREs are refused now.
Expire(t) ==
    /\ GC /\ ~BugGCNoExpiry
    /\ t \in started /\ t \notin expired
    /\ expired' = expired \cup {t}
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
                   recovering, nTimeout, validated>>

GCAllowed(t) == t \in expired \/ BugGCNoExpiry

\* Delete a participant record.  Prepared records are never collected; an
\* applied one only once the decision record is final (or itself gone).
GCPart(t, s) ==
    /\ GC /\ GCAllowed(t)
    /\ part[s][t] \in {"applied", "aborted"}
    /\ part[s][t] = "applied" => (BugGCIgnoresDecision \/ rec[t] \in {"committed", "none"})
    /\ part' = [part EXCEPT ![s][t] = "none"]
    /\ UNCHANGED <<data, locks, rec, coord, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
                   recovering, nTimeout, validated, expired>>

\* Delete a final decision record once no participant is prepared.
GCRec(t) ==
    /\ GC /\ GCAllowed(t)
    /\ rec[t] \in {"committed", "aborted"}
    /\ \A s \in Part(t) : part[s][t] # "prepared"
    /\ rec' = [rec EXCEPT ![t] = "none"]
    /\ UNCHANGED <<data, locks, part, coord, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
                   recovering, nTimeout, validated, expired>>

-----------------------------------------------------------------------------

\* everC is a history variable: it latches CommittedNow.
H == everC' = [t \in Txns |-> everC[t] \/ CommittedNow(t)']

\* Each action also latches the history variable everC; actions of the
\* serializable protocol leave the RC state alone, except APPLY and ABORT
\* (which release plocks) and failovers (which empty the lock table).
U == UNCHANGED rcvars
ReadH(t, k) == Read(t, k) /\ H /\ U
SnapshotReadH(t) == SnapshotRead(t) /\ H /\ U
CommitLocalH(t) == CommitLocal(t) /\ H /\ UNCHANGED <<plocks, lobs, rcpos, rcacked, lostUpd>>
ConfirmLocalH(t) == ConfirmLocal(t) /\ H
SendPrepareH(t) == SendPrepare(t) /\ H /\ U
CoordDecideH(t) == CoordDecide(t) /\ H /\ U
CoordFinishH(t) == CoordFinish(t) /\ H /\ U
CoordAbortDoneH(t) == CoordAbortDone(t) /\ H /\ U
CoordCrashH(t) == CoordCrash(t) /\ H /\ U
PrepareH(t, s) == Prepare(t, s) /\ H /\ U
RevoteH(t, s) == Revote(t, s) /\ H /\ U
ApplyAtH(t, s) == ApplyAt(t, s) /\ H /\ RCApplied(t, s)
AbortAtH(t, s) == AbortAt(t, s) /\ H /\ RCReleased(t, s)
RecoverFenceRecordH(t) == RecoverFenceRecord(t) /\ H /\ U
RecoverFenceParticipantH(t, s) == RecoverFenceParticipant(t, s) /\ H /\ U
RecoverDecideH(t) == RecoverDecide(t) /\ H /\ U
ValidateROH(t, s) == ValidateRO(t, s) /\ H /\ U
FinishROH(t) == FinishRO(t) /\ H /\ U
TimeoutH(t) == Timeout(t) /\ H /\ U
FailoverH(s) == Failover(s) /\ H /\ plocks' = [plocks EXCEPT ![s] = {}]
                /\ UNCHANGED <<lobs, rcpos, rcacked, lostUpd, infl>>
LoseH(m) == Lose(m) /\ H /\ U
DirectReadH(r, k) == DirectRead(r, k) /\ H /\ U
ExpireH(t) == Expire(t) /\ H /\ U
GCPartH(t, s) == GCPart(t, s) /\ H /\ U
GCRecH(t) == GCRec(t) /\ H /\ U
RCReadH(t, k) == RCRead(t, k) /\ H
RCLockH(t, k) == RCLock(t, k) /\ H
CommitRCH(t) == CommitRC(t) /\ H
RCAbortH(t) == RCAbort(t) /\ H

Next ==
    \/ \E t \in Txns, k \in Keys : ReadH(t, k)
    \/ \E t \in Txns : SnapshotReadH(t)
    \/ \E t \in Txns : CommitLocalH(t)
    \/ \E t \in Txns : ConfirmLocalH(t)
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
    \/ \E t \in Txns : ExpireH(t)
    \/ \E t \in Txns, s \in Shards : GCPartH(t, s)
    \/ \E t \in Txns : GCRecH(t)
    \/ \E t \in Txns, k \in Keys : RCReadH(t, k)
    \/ \E t \in Txns, k \in Keys : RCLockH(t, k)
    \/ \E t \in Txns : CommitRCH(t)
    \/ \E t \in Txns : RCAbortH(t)

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
    /\ expired \subseteq Txns
    /\ \A s \in Shards : plocks[s] \subseteq [t : RC, k : Keys]
    /\ lobs \in [Txns -> [Keys -> Vals \cup {Unread}]]
    /\ lostUpd \in BOOLEAN
    /\ infl \subseteq [t : Txns, k : Keys, prev : Vals]

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
\* RC transactions' plain reads need no explanation (READ COMMITTED); the
\* values they read under their locks do, which makes their writes ordered
\* exactly like strict two-phase locking.
ReadsExplained(f) ==
    \A i \in DOMAIN f :
        IF f[i] \in RC THEN \A k \in LSet[f[i]] : lobs[f[i]][k] = LastWriter(f, i, k)
        ELSE \A k \in RSet[f[i]] : obs[f[i]][k] = LastWriter(f, i, k)
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

\* Unique keys: at most one committed transaction inserted each key (keys
\* are never deleted in the model).
UniqueHolds ==
    \A k \in Keys : Cardinality({t \in Txns : everC[t] /\ k \in IKeys[t]}) <= 1

\* No lost updates: every RC write lands on the value its transaction read
\* under the lock (SET x = x - 1 never loses a concurrent decrement).
NoLostUpdate == ~lostUpd

\* Commits are visible in real time to RC plain reads: a read that starts
\* after a transaction was acknowledged sees its write.
RCReadsSeeAcked ==
    \A t \in RC, k \in Keys :
        obs[t][k] # Unread =>
            \A u \in rcacked[t][k] :
                k \in WSet[u] => \E i \in 1..rcpos[t][k] : wh[k][i] = u

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
