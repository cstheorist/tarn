------------------------------ MODULE TarnCommit ------------------------------
(***************************************************************************)
(* Tarn's transaction protocol over Tarantool shards.                      *)
(*                                                                         *)
(* Query fanout diagnostics (issue #133) observe existing actions only.    *)
(* They do not add protocol state, storage calls, retries or lock changes. *)
(* Observer accuracy/bounds are executable tests, outside these invariants. *)
(*                                                                         *)
(* PipelineAdmission.tla checks resource admission outside these actions:  *)
(* before-enqueue refusal sends no message; after-enqueue retirement is    *)
(* message/connection loss, never permission to replay a mutation.         *)
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
(* Bucket moves (MKeys: the rows of one bucket, moved from MFrom to MTo).  *)
(* Every row has a place, the shard that stores and serves it; a request   *)
(* for a row reaches the shard it was routed to, which refuses rows it     *)
(* does not own.  A move fences the bucket on the sender (writes, row      *)
(* locks and the read locks of PREPAREs are refused; the row locks it      *)
(* holds are broken), waits until no prepared transaction holds a point    *)
(* lock on its rows and no local commit to them is in flight, and copies   *)
(* the rows; the receiver holds the bucket meanwhile.  Then the sender     *)
(* deletes the rows (they are nowhere for a moment), and the receiver      *)
(* inserts the copy and releases the hold.  Reads of a row where it no     *)
(* longer is see it gone: MVCC fails the validation of a read a move       *)
(* changed.  Scans (SSet: a range read on every shard, as a secondary-     *)
(* index lookup or a full scan) record what they saw on each shard, so a   *)
(* row seen twice or not at all is a violation.                           *)
(*                                                                         *)
(* A shard that joins (MJoin: the receiver, MTo, joins the cluster).  A   *)
(* transaction routed with the membership from before it joined (OldTxns) *)
(* scans every shard but MTo.  Every shard keeps a membership version      *)
(* (mver), which the join raises on each before any bucket moves to MTo;  *)
(* a transaction that scanned names the version it was routed with in    *)
(* its commit requests (in its read, when it reads one shard), and a       *)
(* shard with a newer one refuses them: the scan may have missed MTo.     *)
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
    BugValidateLostStream,      \* ValidateRO accepts a shard whose leader failed over (lost)
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
    BugInsertNoExistCheck,      \* commits do not check that inserted keys are free
    SSet,                       \* SSet[t]: keys t reads by scanning every shard
    MKeys, MFrom, MTo,          \* a bucket move: its rows, the sender, the receiver
    BugMoveNoDrain,             \* the move neither waits for prepared txns' locks and local
                                \* commits in flight on the bucket nor breaks its row locks
    BugMoveNoHold,              \* the receiver does not hold the bucket during the move
    BugMoveServeStale,          \* the sender serves requests for the bucket after sending it
    MJoin,                      \* MTo joins the cluster: the move starts once every shard
                                \* has the membership with it (mver)
    OldTxns,                    \* transactions routed with the membership from before MTo
                                \* joined: their scans read every shard but MTo
    BugJoinNoCheck              \* shards accept requests routed with an old membership

ASSUME Owner \in [Keys -> Shards]
ASSUME RSet \in [Txns -> SUBSET Keys] /\ WSet \in [Txns -> SUBSET Keys]
ASSUME RKeys \in [Readers -> SUBSET Keys]
ASSUME RC \subseteq Txns /\ LSet \in [Txns -> SUBSET Keys]
ASSUME \A t \in Txns : IF t \in RC THEN WSet[t] \subseteq LSet[t] ELSE LSet[t] = {}
ASSUME IKeys \in [Txns -> SUBSET Keys] /\ \A t \in Txns : IKeys[t] \subseteq WSet[t]
ASSUME SSet \in [Txns -> SUBSET Keys] /\ \A t \in RC : SSet[t] = {}
ASSUME MKeys \subseteq Keys /\ MFrom \in Shards /\ MTo \in Shards
ASSUME MKeys # {} => MFrom # MTo /\ \A k \in MKeys : Owner[k] = MFrom
ASSUME MJoin \in BOOLEAN /\ OldTxns \subseteq Txns /\ (MJoin => MKeys # {})

NoVal   == "init"     \* value of a key before any transaction wrote it
Unread  == "unread"
Gone    == "gone"     \* what a shard returns for a row it does not have
Nowhere == "none"     \* place of a row between the two flips of a move

\* The shards t's server knows: all of them, or for a transaction routed
\* before MTo joined, all but MTo.
KnownShards(t) == IF MJoin /\ t \in OldTxns THEN Shards \ {MTo} ELSE Shards

\* The shards a transaction touches when every row is where it starts
\* (Owner): what picks the path of a read-only transaction, an atomic
\* SnapshotRead on one shard or reads in streams on several.  A scan reads
\* every shard its server knows.
StaticParts(t) == IF t \in RC THEN {Owner[k] : k \in LSet[t]}
                  ELSE {Owner[k] : k \in RSet[t] \cup WSet[t]}
                       \cup (IF SSet[t] # {} THEN KnownShards(t) ELSE {})
SingleShard(t) == Cardinality(StaticParts(t)) = 1
ReadOnly(t)    == WSet[t] = {}

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
    infl,        \* [t, k, prev]: t's local commit wrote k and is not in the WAL yet;
                 \* other readers still see prev (tracked only with RC txns or a move)
    place,       \* place[k]: the shard that stores and serves k (Nowhere mid-move)
    loc,         \* loc[t][k]: where t read k (serializable) or locked it (RC)
    wt,          \* wt[t][k]: the shard t's PREPAREs write k on (fixed at SendPrepare)
    parts,       \* parts[t]: the participants of t's PREPAREs (fixed at SendPrepare)
    scanned,     \* scanned[t]: shards t's scan has read
    sobs,        \* sobs[t][k]: what t's scan saw of k: a set of [s, v], one per shard it saw k on
    mv,          \* the move: idle, fencing, fenced, sent, done, aborted
    staged,      \* staged[k]: the receiver's copy of moving row k
    mver         \* mver[s]: the membership version s has (1: with MTo, when MJoin)

rcvars == <<plocks, lobs, rcpos, rcacked, lostUpd, infl>>
mvars  == <<place, mv, staged, mver>>
tvars  == <<loc, wt, parts, scanned, sobs>>
vars == <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, everC,
          acked, started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
          recovering, nTimeout, validated, expired, plocks, lobs, rcpos, rcacked, lostUpd, infl,
          place, loc, wt, parts, scanned, sobs, mv, staged, mver>>

Msg(type, t, s, yes) == [type |-> type, t |-> t, s |-> s, yes |-> yes]

\* The shards t touches now: where it read or locked rows, every shard its
\* scan read, and where the rows it writes are (a write is routed when the
\* commit begins).  At the commit, this decides between one local
\* transaction and PREPARE/APPLY; SendPrepare fixes it as parts[t].
CurParts(t) ==
    IF t \in RC THEN {loc[t][k] : k \in LSet[t]}
    ELSE {loc[t][k] : k \in RSet[t]} \cup scanned[t] \cup {place[k] : k \in WSet[t]}
\* Selection is a stable function of the transaction identity and frozen
\* candidates. Implementations may distribute records instead of choosing
\* the first writer. MC also checks a different transaction-specific choice.
\* This abstracts the hash, not membership admission or its durable identity
\* (DecisionPlacement checks those local premises).
DecisionParticipant(t, candidates) == CHOOSE s \in candidates : TRUE
D(t) == DecisionParticipant(t, IF WSet[t] # {} THEN {wt[t][k] : k \in WSet[t]} ELSE parts[t])
RKeysOn(t, s) == {k \in RSet[t] : loc[t][k] = s}    \* point reads made on s
LKeysOn(t, s) == {k \in LSet[t] : loc[t][k] = s}    \* RC row locks taken on s
WKeysOn(t, s) == {k \in WSet[t] : wt[t][k] = s}     \* writes PREPAREd on s
IKeysOn(t, s) == {k \in IKeys[t] : wt[t][k] = s}

\* What shard s returns for row k: its value where k is, else nothing.
ValAt(k, s) == IF place[k] = s THEN data[k] ELSE Gone

\* A move fences writes, row locks and the read locks of PREPAREs on the
\* moving rows (some of K) on the sender, from its start until the sender
\* sends them (or the move is aborted).
Fenced(s, K) == MKeys # {} /\ s = MFrom /\ mv \in {"fencing", "fenced"} /\ K \cap MKeys # {}
\* The receiver holds the bucket from the fence until it takes the rows
\* over: the validation of a scan of it fails meanwhile (the rows may
\* already be gone from the sender, and are not here yet).
HoldOK(t, s) ==
    \/ BugMoveNoHold
    \/ ~(MKeys # {} /\ s = MTo /\ mv \in {"fencing", "fenced", "sent"} /\ s \in scanned[t])

\* A transaction that scanned names the membership it was routed with; a
\* shard that has a newer one refuses it (its scan may have missed a shard
\* that joined since).
MemberOK(t, s) ==
    \/ BugJoinNoCheck
    \/ ~(MJoin /\ t \in OldTxns /\ SSet[t] # {} /\ mver[s] = 1)

\* The keys t's scan saw on s.
ScanSeen(t, s) == {k \in SSet[t] : \E o \in sobs[t][k] : o.s = s}
\* Validation of t's scan of s (if t scanned s): s still holds exactly the
\* rows it saw there, with the values it saw (MVCC: an insert, a delete or
\* an update in the range fails it), and no other transaction W-locks a
\* row of the range (unless noW).  noRead: skip the first part.
ScanValid(t, s, noRead, noW) ==
    s \in scanned[t] =>
        /\ s \notin lost[t]
        /\ noRead \/ /\ {k \in SSet[t] : place[k] = s} = ScanSeen(t, s)
                     /\ \A k \in ScanSeen(t, s) : \A o \in sobs[t][k] : o.s = s => o.v = data[k]
        /\ noW \/ ~\E l \in locks[s] : l.t # t /\ l.k \in SSet[t] /\ l.m = "W"

\* t is committed now: explicitly, implicitly (parallel commit), or locally.
CommittedNow(t) ==
    \/ local[t] = "committed"
    \/ rec[t] = "committed"
    \/ rec[t] = "staging" /\ \A s \in parts[t] : part[s][t] \in {"prepared", "applied"}

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
    /\ place   = Owner
    /\ loc     = [t \in Txns |-> [k \in Keys |-> Nowhere]]
    /\ wt      = [t \in Txns |-> [k \in Keys |-> Nowhere]]
    /\ parts   = [t \in Txns |-> {}]
    /\ scanned = [t \in Txns |-> {}]
    /\ sobs    = [t \in Txns |-> [k \in Keys |-> {}]]
    /\ mv      = "idle"
    /\ staged  = [k \in Keys |-> NoVal]
    /\ mver    = [s \in Shards |-> 0]

\* Record the real-time predecessors of t at its first operation.
Start(t) ==
    IF t \in started THEN UNCHANGED <<started, rtPred>>
    ELSE /\ started' = started \cup {t}
         /\ rtPred' = [rtPred EXCEPT ![t] = acked]

ReadsDone(t) == /\ \A k \in RSet[t] : obs[t][k] # Unread
                /\ \A k \in LSet[t] : lobs[t][k] # Unread
                /\ SSet[t] # {} => scanned[t] = KnownShards(t)

-----------------------------------------------------------------------------
(* Execution phase *)

\* A point read in t's stream, on the shard where k is.
Read(t, k) ==
    /\ t \notin RC
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ ~(SingleShard(t) /\ ReadOnly(t))
    /\ k \in RSet[t] /\ obs[t][k] = Unread
    /\ place[k] # Nowhere /\ place[k] \notin lost[t]
    /\ obs' = [obs EXCEPT ![t][k] = data[k]]
    /\ loc' = [loc EXCEPT ![t][k] = place[k]]
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, coord, lost, msgs, local, wh,
                   acked, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired,
                   wt, parts, scanned, sobs>>

\* A read routed with a stale map to the sender after it sent the bucket,
\* which the sender serves (BugMoveServeStale): the row is gone there.
StaleRead(t, k) ==
    /\ BugMoveServeStale /\ k \in MKeys /\ mv \in {"sent", "done"}
    /\ t \notin RC
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ ~(SingleShard(t) /\ ReadOnly(t))
    /\ k \in RSet[t] /\ obs[t][k] = Unread
    /\ MFrom \notin lost[t]
    /\ obs' = [obs EXCEPT ![t][k] = Gone]
    /\ loc' = [loc EXCEPT ![t][k] = MFrom]
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, coord, lost, msgs, local, wh,
                   acked, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired,
                   wt, parts, scanned, sobs>>

\* t's scan of shard s (a range read on every shard, in t's stream there):
\* the rows of SSet[t] that s holds now.
Scan(t, s) ==
    /\ t \notin RC /\ SSet[t] # {}
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ s \in KnownShards(t) /\ s \notin scanned[t] /\ s \notin lost[t]
    /\ scanned' = [scanned EXCEPT ![t] = @ \cup {s}]
    /\ sobs' = [sobs EXCEPT ![t] = [k \in Keys |->
                    IF k \in SSet[t] /\ place[k] = s THEN @[k] \cup {[s |-> s, v |-> data[k]]} ELSE @[k]]]
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, msgs, local, wh,
                   acked, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired,
                   loc, wt, parts>>

\* No other transaction holds a W lock on s on any key in S: a W lock may
\* belong to a transaction that is committed but not yet applied there.
NoWLocksAt(t, S, s) == \A k \in S : ~\E l \in locks[s] : l.t # t /\ l.k = k /\ l.m = "W"

\* Inserts (IKeys): a key a transaction inserts must not hold a value yet.
\* This is how a unique key is enforced across shards: its global index
\* entry is a key on the shard its value hashes to, inserted in the same
\* transaction as the row, so two rows with one value insert one key.
InsertsFree(S) == BugInsertNoExistCheck \/ \A k \in S : data[k] = NoVal

\* A single-shard read-only transaction sees one MVCC read view and then
\* checks the lock table in that same view (autocommit SELECTs check before
\* reading).  If a read key is W-locked the read is retried, so the action
\* is simply not enabled in that state.  A scan reads one shard here when
\* its server knows one (a transaction routed before MTo joined).
SnapshotRead(t) ==
    LET at == {place[k] : k \in RSet[t]} \cup (IF SSet[t] # {} THEN KnownShards(t) ELSE {})
        s  == CHOOSE x \in at : TRUE
    IN
    /\ t \notin RC
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ SingleShard(t) /\ ReadOnly(t)
    /\ Cardinality(at) = 1 /\ s # Nowhere
    /\ BugLocalReadsIgnoreWLocks \/ NoWLocksAt(t, RSet[t] \cup SSet[t], s)
    /\ MemberOK(t, s)
    /\ obs' = [obs EXCEPT ![t] = [k \in Keys |-> IF k \in RSet[t] THEN data[k] ELSE Unread]]
    /\ loc' = [loc EXCEPT ![t] = [k \in Keys |-> IF k \in RSet[t] THEN s ELSE Nowhere]]
    /\ scanned' = [scanned EXCEPT ![t] = IF SSet[t] # {} THEN {s} ELSE @]
    /\ sobs' = [sobs EXCEPT ![t] = [k \in Keys |->
                    IF k \in SSet[t] /\ place[k] = s THEN {[s |-> s, v |-> data[k]]} ELSE @[k]]]
    /\ local' = [local EXCEPT ![t] = "committed"]
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ acked' = acked \cup {t}
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, lost, msgs, wh, dobs, dpos, dacked, dirty,
                   nFail, nCrash, nLoss, recovering, nTimeout, validated, expired,
                   wt, parts>>

\* Local commits in flight are tracked when something reads around them:
\* row locks (RC) and moves.
Track == RC # {} \/ MKeys # {}

\* Read-write commit on one shard: one local Tarantool transaction.
CommitLocal(t) ==
    LET ps     == CurParts(t)
        s      == CHOOSE x \in ps : TRUE
        valid  == /\ s \notin lost[t]
                  /\ \A k \in RSet[t] : ValAt(k, s) = obs[t][k]
                  /\ BugLocalReadsIgnoreWLocks \/ NoWLocksAt(t, RSet[t], s)
                  /\ ScanValid(t, s, FALSE, BugLocalReadsIgnoreWLocks)
                  /\ MemberOK(t, s)
        noConf == /\ \A l \in locks[s] :
                        l.t # t => (l.k \notin WSet[t] \/ (BugLocalIgnoresRLocks /\ l.m = "R"))
                  /\ BugIgnorePLocks \/ ~\E pl \in plocks[s] : pl.t # t /\ pl.k \in WSet[t]
    IN
    /\ t \notin RC
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ ~ReadOnly(t) /\ ReadsDone(t)
    /\ Cardinality(ps) = 1 /\ s # Nowhere
    /\ IF valid /\ noConf /\ ~Fenced(s, WSet[t]) /\ InsertsFree(IKeys[t])
       THEN /\ data' = [k \in Keys |-> IF k \in WSet[t] THEN t ELSE data[k]]
            /\ wh' = [k \in Keys |-> IF k \in WSet[t] THEN Append(wh[k], t) ELSE wh[k]]
            /\ local' = [local EXCEPT ![t] = "committed"]
            /\ acked' = acked \cup {t}
            /\ infl' = IF Track THEN infl \cup {[t |-> t, k |-> k, prev |-> data[k]] : k \in WSet[t]}
                       ELSE infl
       ELSE /\ local' = [local EXCEPT ![t] = "aborted"]
            /\ UNCHANGED <<data, wh, acked, infl>>
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ Start(t)
    /\ UNCHANGED <<locks, part, rec, obs, lost, msgs, dobs, dpos, dacked, dirty,
                   nFail, nCrash, nLoss, recovering, nTimeout, validated, expired, tvars>>

\* A local commit routed with a stale map to the sender after it sent the
\* bucket, which the sender serves (BugMoveServeStale): its writes of the
\* moving rows land where no row lives any more, and nobody reads them.
StaleCommitLocal(t) ==
    /\ BugMoveServeStale /\ mv \in {"sent", "done"}
    /\ t \notin RC /\ SSet[t] = {}
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ ~ReadOnly(t) /\ ReadsDone(t) /\ WSet[t] \cap MKeys # {}
    /\ \A k \in RSet[t] : loc[t][k] = MFrom /\ ValAt(k, MFrom) = obs[t][k]
    /\ \A k \in WSet[t] \ MKeys : place[k] = MFrom
    /\ data' = [k \in Keys |-> IF k \in WSet[t] \ MKeys THEN t ELSE data[k]]
    /\ wh' = [k \in Keys |-> IF k \in WSet[t] \ MKeys THEN Append(wh[k], t) ELSE wh[k]]
    /\ local' = [local EXCEPT ![t] = "committed"]
    /\ acked' = acked \cup {t}
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ Start(t)
    /\ UNCHANGED <<locks, part, rec, obs, lost, msgs, dobs, dpos, dacked, dirty,
                   nFail, nCrash, nLoss, recovering, nTimeout, validated, expired, infl, tvars>>

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
\* A shard whose leader failed over while t's stream was open (lost) lost
\* the MVCC tracking of its reads, and may miss its successor's writes: no
\* check there finds them changed. The implementation refuses it by the
\* election term the stream began in (TERM_TXN); BugValidateLostStream
\* models the implementation without that check.
ValidateRO(t, s) ==
    LET rk    == RKeysOn(t, s)
        valid == /\ s \notin lost[t] \/ BugValidateLostStream
                 /\ \/ s \in lost[t]
                    \/ BugROValidateNoReadCheck
                    \/ \A k \in rk : ValAt(k, s) = obs[t][k]
                 /\ BugLocalReadsIgnoreWLocks \/ NoWLocksAt(t, rk, s)
                 /\ ScanValid(t, s, BugROValidateNoReadCheck, BugLocalReadsIgnoreWLocks)
                 /\ HoldOK(t, s)
                 /\ MemberOK(t, s)
    IN
    /\ ROValidateOnly /\ t \notin RC
    /\ coord[t] = "exec" /\ local[t] = "none"
    /\ ~SingleShard(t) /\ ReadOnly(t) /\ ReadsDone(t)
    /\ s \in CurParts(t) /\ s \notin validated[t]
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
    /\ ReadsDone(t) /\ validated[t] = CurParts(t)
    /\ local' = [local EXCEPT ![t] = "committed"]
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ acked' = acked \cup {t}
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, msgs, wh, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

-----------------------------------------------------------------------------
(* Multi-shard commit: coordinator *)

\* The rows t writes are routed to where they are now (an RC transaction's
\* to where it locked them); the participants are fixed from here on.
SendPrepare(t) ==
    LET ps == CurParts(t) IN
    /\ coord[t] = "exec" /\ local[t] = "none" /\ ReadsDone(t)
    /\ Cardinality(ps) > 1 /\ Nowhere \notin ps
    /\ ~(ROValidateOnly /\ ReadOnly(t))
    /\ t \in RC => ~ReadOnly(t)     \* read-only RC txns just release their locks
    /\ coord' = [coord EXCEPT ![t] = "preparing"]
    /\ parts' = [parts EXCEPT ![t] = ps]
    /\ wt' = [wt EXCEPT ![t] = [k \in Keys |-> IF k \notin WSet[t] THEN Nowhere
                                              ELSE IF t \in RC THEN loc[t][k] ELSE place[k]]]
    /\ msgs' = msgs \cup {Msg("prepare", t, s, FALSE) : s \in ps}
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, local, wh, acked,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired,
                   loc, scanned, sobs>>

CoordDecide(t) ==
    /\ coord[t] = "preparing"
    /\ \/ /\ \A s \in parts[t] : Msg("vote", t, s, TRUE) \in msgs
          /\ coord' = [coord EXCEPT ![t] = "applying"]
          /\ msgs' = msgs \cup {Msg("apply", t, s, FALSE) : s \in parts[t]}
          /\ acked' = IF AckOnPrepare THEN acked \cup {t} ELSE acked
       \/ /\ \E s \in parts[t] : Msg("vote", t, s, FALSE) \in msgs
          /\ coord' = [coord EXCEPT ![t] = "aborting"]
          /\ msgs' = msgs \cup {Msg("abort", t, s, FALSE) : s \in parts[t]}
          /\ UNCHANGED acked
    /\ UNCHANGED <<data, locks, part, rec, obs, lost, local, wh, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

\* Acknowledge the client once every participant applied; mark the record.
CoordFinish(t) ==
    /\ coord[t] = "applying"
    /\ \A s \in parts[t] : part[s][t] = "applied"
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

\* A scan's read locks (mode "S") cover its range on the shard: they keep
\* writes out of it, but a move does not wait for them -- the scan read
\* every shard, so it holds them on the receiver too.
Prepare(t, s) ==
    LET rk       == RKeysOn(t, s)
        wk       == WKeysOn(t, s)
        others   == {l \in locks[s] : l.t # t}
        \* RC: no read validation, but the locks must have survived.
        readsOK  == IF t \in RC
                    THEN BugRCNoLockCheck \/ \A k \in LKeysOn(t, s) : [t |-> t, k |-> k] \in plocks[s]
                    ELSE /\ s \notin lost[t]
                         /\ \A k \in rk : ValAt(k, s) = obs[t][k]
                         /\ BugNoWLockCheckOnRead \/ ~\E l \in others : l.k \in rk /\ l.m = "W"
                         /\ ScanValid(t, s, FALSE, BugNoWLockCheckOnRead)
                         /\ HoldOK(t, s)
                         /\ MemberOK(t, s)
        writesOK == /\ ~\E l \in others : l.k \in wk
                    /\ BugIgnorePLocks \/ ~\E pl \in plocks[s] : pl.t # t /\ pl.k \in wk
        recOK    == s = D(t) => rec[t] = "none"
        \* s owns every row the PREPARE names, and none is fenced by a move
        ownOK    == /\ \A k \in wk \cup rk : place[k] = s
                    /\ ~Fenced(s, wk \cup rk)
        newLocks == {[t |-> t, k |-> k, m |-> "W"] : k \in wk}
                    \cup (IF BugNoReadLocks \/ t \in RC THEN {}
                          ELSE {[t |-> t, k |-> k, m |-> "R"] : k \in rk \ wk}
                               \cup {[t |-> t, k |-> k, m |-> "S"] :
                                       k \in {x \in SSet[t] : place[x] = s} \ (wk \cup rk)})
    IN
    /\ Msg("prepare", t, s, FALSE) \in msgs
    /\ part[s][t] = "none"
    \* An expired transaction's PREPARE is refused without any effect: no
    \* record, and no vote either -- a (re-delivered) PREPARE may meet a
    \* participant whose records were already collected, so a NO here could
    \* contradict a commit.  The coordinator treats the reply as unknown.
    \* Every fresh implementation NO/tombstone guard, including a missing
    \* stream, follows this expiry check; old committed records may be GCed.
    /\ t \notin expired
    /\ IF readsOK /\ writesOK /\ recOK /\ ownOK /\ InsertsFree(IKeysOn(t, s))
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
\* (A row a move took away from s meanwhile -- only possible without the
\* move's drain -- is written where it no longer lives: nobody sees it.)
ApplyAt(t, s) ==
    LET wk   == WKeysOn(t, s)
        live == {k \in wk : place[k] = s}
    IN
    /\ Msg("apply", t, s, FALSE) \in msgs
    /\ part[s][t] = "prepared"
    /\ ApplyMarksCommitted /\ s = D(t) => rec[t] # "aborted"
    /\ data' = [k \in Keys |-> IF k \in live THEN t ELSE data[k]]
    /\ wh' = [k \in Keys |-> IF k \in live THEN Append(wh[k], t) ELSE wh[k]]
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

\* (Recovery finds a transaction by its records, which only PREPAREs write:
\* a transaction that sent none -- it committed locally, validated its
\* reads, or rolled back during execution -- is never recovered.)
Timeout(t) ==
    /\ nTimeout < MaxTimeouts
    /\ parts[t] # {} /\ t \notin recovering
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
       \/ \E s \in parts[t] : part[s][t] = "prepared"
       \/ BugFenceAfterExpiry
    /\ rec' = [rec EXCEPT ![t] = "aborted"]
    /\ msgs' = msgs \cup {Msg("abort", t, s, FALSE) : s \in parts[t]}
    /\ UNCHANGED <<data, locks, part, coord, obs, lost, local, wh, acked, started,
                   rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

\* STAGING but some participant never prepared: fence it with a tombstone.
\* (Bug variant: give up on the transaction without fencing.)
RecoverFenceParticipant(t, s) ==
    /\ t \in recovering
    /\ rec[t] = "staging" /\ s \in parts[t] /\ part[s][t] = "none"
    /\ IF BugNoTombstone
       THEN /\ rec' = [rec EXCEPT ![t] = "aborted"]
            /\ msgs' = msgs \cup {Msg("abort", t, p, FALSE) : p \in parts[t] \ {s}}
            /\ UNCHANGED part
       ELSE /\ part' = [part EXCEPT ![s][t] = "aborted"]
            /\ UNCHANGED <<rec, msgs, recovering, nTimeout, validated, expired>>
    /\ UNCHANGED <<data, locks, coord, obs, lost, local, wh, acked, started, rtPred,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout, validated, expired>>

RecoverDecide(t) ==
    /\ t \in recovering
    /\ \/ /\ rec[t] = "staging"
          /\ \A s \in parts[t] : part[s][t] \in {"prepared", "applied"}
          /\ rec' = [rec EXCEPT ![t] = "committed"]
          /\ msgs' = msgs \cup {Msg("apply", t, s, FALSE) : s \in parts[t]}
       \/ /\ rec[t] = "staging"
          /\ \E s \in parts[t] : part[s][t] = "aborted"
          /\ rec' = [rec EXCEPT ![t] = "aborted"]
          /\ msgs' = msgs \cup {Msg("abort", t, s, FALSE) : s \in parts[t]}
       \/ /\ rec[t] = "committed"
          /\ msgs' = msgs \cup {Msg("apply", t, s, FALSE) : s \in parts[t]}
          /\ UNCHANGED rec
       \/ /\ rec[t] = "aborted"
          /\ msgs' = msgs \cup {Msg("abort", t, s, FALSE) : s \in parts[t]}
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
    /\ place[k] # Nowhere
    /\ NoWLocksAt(t, {k}, place[k])
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
\* (On the shard where k is; a move's fence refuses it.)
\* WaiterAllocation separately checks the condition-publication premise of
\* the local wait implementation. It adds no action to this owner protocol.
RCLock(t, k) ==
    LET s == place[k] IN
    /\ t \in RC /\ coord[t] = "exec" /\ local[t] = "none"
    /\ k \in LSet[t] /\ lobs[t][k] = Unread
    /\ s # Nowhere /\ ~Fenced(s, {k})
    /\ BugRCReadBeforeLock => (k \in RSet[t] => obs[t][k] # Unread)
    /\ ~\E pl \in plocks[s] : pl.t # t /\ pl.k = k
    /\ BugRCLockIgnoresWLocks \/ NoWLocksAt(t, {k}, s)
    /\ BugRCLockReadsInFlight \/ ~InFlight(k)
    /\ plocks' = [plocks EXCEPT ![s] = @ \cup {[t |-> t, k |-> k]}]
    /\ loc' = [loc EXCEPT ![t][k] = s]
    /\ lobs' = [lobs EXCEPT ![t][k] = IF BugRCReadBeforeLock /\ k \in RSet[t]
                                      THEN obs[t][k] ELSE Confirmed(k)]
    /\ Start(t)
    /\ UNCHANGED <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, acked,
                   dobs, dpos, dacked, dirty, nFail, nCrash, nLoss, recovering, nTimeout,
                   validated, expired, rcpos, rcacked, lostUpd, infl>>

HoldsLocks(t) == \A k \in LSet[t] : loc[t][k] # Nowhere /\ [t |-> t, k |-> k] \in plocks[loc[t][k]]

\* Commit of a read-only RC transaction (on any shards: it just releases its
\* locks) or of a single-shard RC transaction with writes: one local
\* transaction that checks the locks survived, waits out prepared
\* transactions' locks on what it writes, applies the writes and releases
\* the locks.
\* (The shard where its locks are must own every row it writes, none fenced
\* by a move.  Like CommitLocal, its commit is in flight until confirmed:
\* tracked here only with a move, which must wait for it -- no row lock
\* can be granted on its rows before it is confirmed.)
CommitRC(t) ==
    LET ps     == CurParts(t)
        s      == CHOOSE x \in ps : TRUE
        intact == BugRCNoLockCheck \/ HoldsLocks(t)
        owned  == \A k \in WSet[t] : place[k] = s
    IN
    /\ t \in RC /\ coord[t] = "exec" /\ local[t] = "none" /\ ReadsDone(t)
    /\ ReadOnly(t) \/ Cardinality(ps) = 1
    /\ ~\E x \in Shards : \E l \in locks[x] : l.t # t /\ l.k \in WSet[t]
    /\ IF ReadOnly(t) \/ (intact /\ owned /\ ~Fenced(s, WSet[t]) /\ InsertsFree(IKeys[t]))
       THEN /\ data' = [k \in Keys |-> IF k \in WSet[t] THEN t ELSE data[k]]
            /\ wh' = [k \in Keys |-> IF k \in WSet[t] THEN Append(wh[k], t) ELSE wh[k]]
            /\ lostUpd' = (lostUpd \/ \E k \in WSet[t] : data[k] # lobs[t][k])
            /\ local' = [local EXCEPT ![t] = "committed"]
            /\ acked' = acked \cup {t}
            /\ infl' = IF MKeys # {} THEN infl \cup {[t |-> t, k |-> k, prev |-> data[k]] : k \in WSet[t]}
                       ELSE infl
       ELSE /\ local' = [local EXCEPT ![t] = "aborted"]
            /\ UNCHANGED <<data, wh, acked, lostUpd, infl>>
    /\ plocks' = [x \in Shards |-> {l \in plocks[x] : l.t # t}]
    /\ coord' = [coord EXCEPT ![t] = "finished"]
    /\ Start(t)
    /\ UNCHANGED <<locks, part, rec, obs, lost, msgs, dobs, dpos, dacked, dirty,
                   nFail, nCrash, nLoss, recovering, nTimeout, validated, expired,
                   lobs, rcpos, rcacked>>

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
                               /\ \E k \in WKeysOn(t, s) : data[k] # lobs[t][k]))
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

\* A plain Tarantool get on the shard where the row is, bypassing Tarn
\* entirely (a reader that asks a shard the row has left finds nothing
\* there and asks again).
DirectRead(r, k) ==
    /\ k \in RKeys[r] /\ dobs[r][k] = Unread
    /\ place[k] # Nowhere
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
    /\ \A s \in parts[t] : part[s][t] # "prepared"
    /\ rec' = [rec EXCEPT ![t] = "none"]
    /\ UNCHANGED <<data, locks, part, coord, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
                   recovering, nTimeout, validated, expired>>

-----------------------------------------------------------------------------
(* A bucket move (MKeys from MFrom to MTo), in the steps one SQL server    *)
(* drives.  Each step is one local transaction on one shard; the fence and *)
(* the hold are durable, so leader failovers leave them in place.          *)

Others == <<data, locks, part, rec, coord, obs, lost, msgs, local, wh, acked,
            started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
            recovering, nTimeout, validated, expired, lobs, rcpos, rcacked, lostUpd, infl, tvars>>

\* The sender fences the bucket (writes, row locks and the read locks of
\* PREPAREs on its rows are refused from now on) and breaks the row locks
\* held on its rows, whose transactions then cannot commit; the receiver
\* holds it.  (Before this, the rows are copied while the sender serves
\* them; nothing a transaction can see changes.)
MoveFence ==
    /\ MKeys # {} /\ mv = "idle"
    /\ MJoin => \A s \in Shards : mver[s] = 1
    /\ mv' = "fencing"
    /\ plocks' = IF BugMoveNoDrain THEN plocks
                 ELSE [plocks EXCEPT ![MFrom] = {pl \in @ : pl.k \notin MKeys}]
    /\ UNCHANGED <<place, staged, mver>>
    /\ UNCHANGED Others

\* Drained: no prepared transaction holds a point lock on a row of the
\* bucket on the sender (its APPLY may still write the row) and no local
\* commit that writes one is in flight (the copy would miss it).  The
\* last copy of the rows is taken now.
MoveDrained ==
    /\ mv = "fencing"
    /\ BugMoveNoDrain \/ ~\E l \in locks[MFrom] : l.k \in MKeys /\ l.m \in {"R", "W"}
    /\ BugMoveNoDrain \/ ~\E i \in infl : i.k \in MKeys
    /\ mv' = "fenced"
    /\ staged' = [k \in Keys |-> IF k \in MKeys THEN Confirmed(k) ELSE NoVal]
    /\ UNCHANGED <<place, plocks, mver>>
    /\ UNCHANGED Others

\* The sender deletes the rows: from now on it refuses the bucket.  Until
\* the receiver takes them over, the rows are nowhere.
MoveSendFlip ==
    /\ mv = "fenced"
    /\ mv' = "sent"
    /\ place' = [k \in Keys |-> IF k \in MKeys THEN Nowhere ELSE place[k]]
    /\ UNCHANGED <<staged, plocks, mver>>
    /\ UNCHANGED Others

\* The receiver inserts the copy, owns the bucket and releases the hold.
MoveRecvFlip ==
    /\ mv = "sent"
    /\ mv' = "done"
    /\ place' = [k \in Keys |-> IF k \in MKeys THEN MTo ELSE place[k]]
    /\ data' = [k \in Keys |-> IF k \in MKeys THEN staged[k] ELSE data[k]]
    /\ UNCHANGED <<staged, plocks, mver>>
    /\ UNCHANGED <<locks, part, rec, coord, obs, lost, msgs, local, wh, acked,
                   started, rtPred, dobs, dpos, dacked, dirty, nFail, nCrash, nLoss,
                   recovering, nTimeout, validated, expired, lobs, rcpos, rcacked, lostUpd, infl, tvars>>

\* A move that cannot drain in time is given up before the sender sends:
\* the fence and the hold are released (and the copy dropped).
MoveAbort ==
    /\ mv \in {"fencing", "fenced"}
    /\ mv' = "aborted"
    /\ UNCHANGED <<place, staged, plocks, mver>>
    /\ UNCHANGED Others

\* MTo joins: shard s learns the membership with it (a SQL server writes it
\* on every shard, then the move may start).  Servers that learn it route
\* new transactions with it; those of OldTxns were routed before.
MemberJoin(s) ==
    /\ MJoin /\ mver[s] = 0
    /\ mver' = [mver EXCEPT ![s] = 1]
    /\ UNCHANGED <<place, mv, staged, plocks>>
    /\ UNCHANGED Others

-----------------------------------------------------------------------------

\* everC is a history variable: it latches CommittedNow.
H == everC' = [t \in Txns |-> everC[t] \/ CommittedNow(t)']

\* Each action also latches the history variable everC; actions of the
\* serializable protocol leave the RC state alone, except APPLY and ABORT
\* (which release plocks) and failovers (which empty the lock table).
U == UNCHANGED rcvars
M == UNCHANGED mvars    \* the move's state: only the Move* actions change it
T == UNCHANGED tvars
ReadH(t, k) == Read(t, k) /\ U /\ M /\ H
StaleReadH(t, k) == StaleRead(t, k) /\ U /\ M /\ H
ScanH(t, s) == Scan(t, s) /\ U /\ M /\ H
SnapshotReadH(t) == SnapshotRead(t) /\ U /\ M /\ H
CommitLocalH(t) == CommitLocal(t) /\ UNCHANGED <<plocks, lobs, rcpos, rcacked, lostUpd>> /\ M /\ H
StaleCommitLocalH(t) == StaleCommitLocal(t) /\ UNCHANGED <<plocks, lobs, rcpos, rcacked, lostUpd>> /\ M /\ H
ConfirmLocalH(t) == ConfirmLocal(t) /\ M /\ T /\ H
SendPrepareH(t) == SendPrepare(t) /\ U /\ M /\ H
CoordDecideH(t) == CoordDecide(t) /\ U /\ M /\ T /\ H
CoordFinishH(t) == CoordFinish(t) /\ U /\ M /\ T /\ H
CoordAbortDoneH(t) == CoordAbortDone(t) /\ U /\ M /\ T /\ H
CoordCrashH(t) == CoordCrash(t) /\ U /\ M /\ T /\ H
PrepareH(t, s) == Prepare(t, s) /\ U /\ M /\ T /\ H
RevoteH(t, s) == Revote(t, s) /\ U /\ M /\ T /\ H
ApplyAtH(t, s) == ApplyAt(t, s) /\ RCApplied(t, s) /\ M /\ T /\ H
AbortAtH(t, s) == AbortAt(t, s) /\ RCReleased(t, s) /\ M /\ T /\ H
RecoverFenceRecordH(t) == RecoverFenceRecord(t) /\ U /\ M /\ T /\ H
RecoverFenceParticipantH(t, s) == RecoverFenceParticipant(t, s) /\ U /\ M /\ T /\ H
RecoverDecideH(t) == RecoverDecide(t) /\ U /\ M /\ T /\ H
ValidateROH(t, s) == ValidateRO(t, s) /\ U /\ M /\ T /\ H
FinishROH(t) == FinishRO(t) /\ U /\ M /\ T /\ H
TimeoutH(t) == Timeout(t) /\ U /\ M /\ T /\ H
FailoverH(s) == Failover(s) /\ plocks' = [plocks EXCEPT ![s] = {}]
                /\ UNCHANGED <<lobs, rcpos, rcacked, lostUpd, infl>> /\ M /\ T /\ H
LoseH(m) == Lose(m) /\ U /\ M /\ T /\ H
DirectReadH(r, k) == DirectRead(r, k) /\ U /\ M /\ T /\ H
ExpireH(t) == Expire(t) /\ U /\ M /\ T /\ H
GCPartH(t, s) == GCPart(t, s) /\ U /\ M /\ T /\ H
GCRecH(t) == GCRec(t) /\ U /\ M /\ T /\ H
RCReadH(t, k) == RCRead(t, k) /\ M /\ T /\ H
RCLockH(t, k) == RCLock(t, k) /\ M /\ UNCHANGED <<wt, parts, scanned, sobs>> /\ H
CommitRCH(t) == CommitRC(t) /\ M /\ T /\ H
RCAbortH(t) == RCAbort(t) /\ M /\ T /\ H
MoveFenceH == MoveFence /\ H
MoveDrainedH == MoveDrained /\ H
MoveSendFlipH == MoveSendFlip /\ H
MoveRecvFlipH == MoveRecvFlip /\ H
MoveAbortH == MoveAbort /\ H
MemberJoinH(s) == MemberJoin(s) /\ H

Next ==
    \/ \E t \in Txns, k \in Keys : ReadH(t, k)
    \/ \E t \in Txns, k \in Keys : StaleReadH(t, k)
    \/ \E t \in Txns, s \in Shards : ScanH(t, s)
    \/ \E t \in Txns : SnapshotReadH(t)
    \/ \E t \in Txns : CommitLocalH(t)
    \/ \E t \in Txns : StaleCommitLocalH(t)
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
    \/ MoveFenceH \/ MoveDrainedH \/ MoveSendFlipH \/ MoveRecvFlipH \/ MoveAbortH
    \/ \E s \in Shards : MemberJoinH(s)

Spec == Init /\ [][Next]_vars

-----------------------------------------------------------------------------
(* Properties *)

Vals == Txns \cup {NoVal}

TypeOK ==
    /\ data \in [Keys -> Vals]
    /\ \A s \in Shards : locks[s] \subseteq [t : Txns, k : Keys, m : {"R", "W", "S"}]
    /\ part \in [Shards -> [Txns -> {"none", "prepared", "applied", "aborted"}]]
    /\ rec \in [Txns -> {"none", "staging", "committed", "aborted"}]
    /\ obs \in [Txns -> [Keys -> Vals \cup {Unread, Gone}]]
    /\ local \in [Txns -> {"none", "committed", "aborted"}]
    /\ expired \subseteq Txns
    /\ \A s \in Shards : plocks[s] \subseteq [t : RC, k : Keys]
    /\ lobs \in [Txns -> [Keys -> Vals \cup {Unread}]]
    /\ lostUpd \in BOOLEAN
    /\ infl \subseteq [t : Txns, k : Keys, prev : Vals]
    /\ place \in [Keys -> Shards \cup {Nowhere}]
    /\ loc \in [Txns -> [Keys -> Shards \cup {Nowhere}]]
    /\ \A t \in Txns : sobs[t] \in [Keys -> SUBSET [s : Shards, v : Vals]]
    /\ mv \in {"idle", "fencing", "fenced", "sent", "done", "aborted"}
    /\ mver \in [Shards -> {0, 1}]

\* Atomicity: writes reach a plain space only for committed transactions ...
AppliedOnlyIfCommitted ==
    \A s \in Shards, t \in Txns : part[s][t] = "applied" => everC[t]
\* ... and a committed transaction is never rolled back anywhere.
NoAbortAfterCommit ==
    \A t \in Txns : everC[t] => \A s \in parts[t] : part[s][t] # "aborted"

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
\* exactly like strict two-phase locking.  A scan must have seen each row
\* exactly once (on one shard), with its value.
ReadsExplained(f) ==
    \A i \in DOMAIN f :
        IF f[i] \in RC THEN \A k \in LSet[f[i]] : lobs[f[i]][k] = LastWriter(f, i, k)
        ELSE /\ \A k \in RSet[f[i]] : obs[f[i]][k] = LastWriter(f, i, k)
             /\ \A k \in SSet[f[i]] : \E o \in sobs[f[i]][k] :
                    sobs[f[i]][k] = {o} /\ o.v = LastWriter(f, i, k)
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
