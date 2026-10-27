----------------------------- MODULE StreamMarks -----------------------------
(***************************************************************************)
(* A step that commits the reads of a stream's transaction                *)
(* (TarnCommit!ValidateRO, CommitLocal, Prepare) must know whether the     *)
(* stream began before the node's election term changed (TarnCommit:      *)
(* lost[t]), and before a TRUNCATE or DROP TABLE of a table it read was    *)
(* applied (design §2.10: memtx MVCC aborts such a reader, vinyl does      *)
(* not).  Tarantool numbers transactions as they begin, so the Lua module  *)
(* keeps one number per event, of a transaction begun once the event       *)
(* happened: TERM_TXN once the node saw its term, DDL_TXN[table] once a    *)
(* TRUNCATE or DROP was applied (and the last of those for the native      *)
(* procedures).  A stream whose number is below began before.              *)
(*                                                                         *)
(* A local refinement model, as GroupedDDL: one node of a replicated       *)
(* shard, which loses and regains the leadership; a table that TRUNCATEs   *)
(* hold, clear and release, here or on another leader; streams that begin, *)
(* read it, and whose reads a step commits: the Lua procedures             *)
(* (tarn_validate, tarn_commit_local, tarn_prepare) or the native ones     *)
(* (ts_validate, ts_commit_local), which hand back what they cannot        *)
(* decide.  A step whose checks pass records what it saw, and              *)
(* CommittedReadsCurrent says such a stream began in the term it was       *)
(* committed in, and read the table as it still was then, with no hold on  *)
(* it.                                                                     *)
(*                                                                         *)
(* The native procedures read neither the term nor the stored holds.  They *)
(* rely on two premises of Tarantool's elections (design §1.5), each with  *)
(* a switch here: a change of the term makes the node read-only at once    *)
(* (BugRoAfterTermChange), and the election triggers run before the node   *)
(* is writable in the new term (BugWritableBeforeTrigger).  The Lua        *)
(* trigger marks the lock index and the holds' cache stale, then notes      *)
(* TERM_TXN; that may fail, and then the next lock check notes it, as the  *)
(* lock index is stale.  After a leader change the holds' cache is         *)
(* anything: only reading the holds from storage again (fence_fresh) makes *)
(* it right.  Tables are vinyl, whose readers stay open: memtx MVCC would  *)
(* only abort more.  Whether the rows a stream read were overwritten is    *)
(* TarnCommit's business (MVCC, the probe write), not this model's.        *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    Streams, Tables, MaxTerms, MaxDDL,
    BugRoAfterTermChange,       \* the node stays writable for a while in a new term
    BugWritableBeforeTrigger,   \* ... or is writable in it before its election triggers ran
    BugNativeNoTermCheck,       \* the native step ignores TERM_TXN
    BugNativeIgnoresLkStale,    \* ... runs with the lock index stale
    BugLuaNoTermCheck,          \* the Lua step does not check the term the stream began in
    BugLuaTermCheckEarly,       \* ... checks it before its last yield
    BugValidateNoFenceFresh,    \* ... checks the holds' cache as it is
    BugCommitInNewTxn,          \* ... commits reads whose stream's transaction is gone in a new one
    BugDoomBeforeClear,         \* APPLY notes DDL_TXN before it clears the table
    BugDoomAfterCommit          \* ... after its catalog commit, and not when it runs again

ASSUME MaxTerms \in Nat /\ MaxDDL \in Nat

Unread == 1000    \* (no version reaches it)

VARIABLES
    next,       \* the number of the next transaction to begin
    term,       \* the node's election term
    ro,         \* the node is read-only
    trig,       \* the election triggers of the last term change have not run
    other,      \* another node led in this term (this one cannot, until the next)
    termTxn,    \* TERM_TXN (ns.term_txn)
    lastTerm,   \* LAST_TERM
    lkStale,    \* LK_STALE (ns.lk_stale)
    fenceStale, \* FENCE_STALE (ns.fence_stale)
    heldStore,  \* the holds of TRUNCATEs, as stored (_tarn_meta)
    heldCache,  \* DDL_HELD: the holds this node knows (ns.ddl_n counts them)
    ddlTxn,     \* DDL_TXN
    ddlLast,    \* the last DDL_TXN (BK.ddl_last, ns.ddl_txn)
    ver,        \* each table's contents: a TRUNCATE makes new ones
    apply,      \* where the TRUNCATE of each table is
    terms, ddls,
    st,         \* each stream: none, open, gone (its transaction ended), checking, done, refused
    sid,        \* the number of its transaction
    began,      \* the term it began in
    seen,       \* what it read of each table
    vterm, vver, vheld   \* the term, contents and holds when its reads were committed

nodeVars == <<term, ro, trig, other, terms>>
markVars == <<next, termTxn, lastTerm>>
cacheVars == <<lkStale, fenceStale, heldCache>>
ddlVars == <<heldStore, ddlTxn, ddlLast, ver, apply, ddls>>
streamVars == <<st, sid, began, seen, vterm, vver, vheld>>
vars == <<nodeVars, markVars, cacheVars, ddlVars, streamVars>>

Init ==
    /\ next = 1 /\ term = 1 /\ ro = FALSE /\ trig = FALSE /\ other = FALSE
    /\ termTxn = 0 /\ lastTerm = 1
    /\ lkStale = FALSE /\ fenceStale = FALSE
    /\ heldStore = [t \in Tables |-> FALSE] /\ heldCache = [t \in Tables |-> FALSE]
    /\ ddlTxn = [t \in Tables |-> 0] /\ ddlLast = 0
    /\ ver = [t \in Tables |-> 0] /\ apply = [t \in Tables |-> "none"]
    /\ terms = 0 /\ ddls = 0
    /\ st = [s \in Streams |-> "none"] /\ sid = [s \in Streams |-> 0]
    /\ began = [s \in Streams |-> 0]
    /\ seen = [s \in Streams |-> [t \in Tables |-> Unread]]
    /\ vterm = [s \in Streams |-> 0]
    /\ vver = [s \in Streams |-> [t \in Tables |-> 0]]
    /\ vheld = [s \in Streams |-> [t \in Tables |-> FALSE]]

(* BK.term_mark: after a change of the term, the number of a transaction  *)
(* begun now (BK.txn_mark begins one and rolls it back).                  *)
TermMark ==
    IF lastTerm # term
    THEN /\ termTxn' = next /\ lastTerm' = term /\ next' = next + 1
    ELSE UNCHANGED markVars

(* doom_readers: the number of a transaction begun now. *)
Doom(t) ==
    /\ ddlTxn' = [ddlTxn EXCEPT ![t] = next] /\ ddlLast' = next /\ next' = next + 1
    /\ UNCHANGED <<termTxn, lastTerm>>

(* --- streams ------------------------------------------------------------ *)

(* The first read of a stream begins its transaction. *)
Begin(s) ==
    /\ st[s] = "none"
    /\ st' = [st EXCEPT ![s] = "open"] /\ sid' = [sid EXCEPT ![s] = next]
    /\ began' = [began EXCEPT ![s] = term] /\ next' = next + 1
    /\ UNCHANGED <<nodeVars, termTxn, lastTerm, cacheVars, ddlVars, seen, vterm, vver, vheld>>

(* A read in the stream: a demoted node may serve it too, and nothing     *)
(* refuses it while a TRUNCATE holds the table (the commit does).         *)
Read(s, t) ==
    /\ st[s] = "open" /\ seen[s][t] = Unread
    /\ seen' = [seen EXCEPT ![s][t] = ver[t]]
    /\ UNCHANGED <<nodeVars, markVars, cacheVars, ddlVars, st, sid, began, vterm, vver, vheld>>

(* The stream's transaction ends while the stream goes on (a request that  *)
(* rolled it back): its reads are in no transaction any more.             *)
TxnGone(s) ==
    /\ st[s] = "open" /\ st' = [st EXCEPT ![s] = "gone"]
    /\ UNCHANGED <<nodeVars, markVars, cacheVars, ddlVars, sid, began, seen, vterm, vver, vheld>>

Committed(s) ==
    /\ st' = [st EXCEPT ![s] = "done"]
    /\ vterm' = [vterm EXCEPT ![s] = term]
    /\ vver' = [vver EXCEPT ![s] = ver]
    /\ vheld' = [vheld EXCEPT ![s] = heldStore]

(* ts_validate, ts_commit_local: commits the reads when nothing would make *)
(* the Lua procedure refuse the step or do more (else it hands the step    *)
(* back, having changed nothing): the stream's transaction, a writable     *)
(* node, a fresh lock index and holds' cache, no hold (ddl_n), a stream    *)
(* begun after TERM_TXN and after the last DDL_TXN.                        *)
NativeStep(s) ==
    /\ st[s] = "open" /\ ~ro
    /\ ~lkStale \/ BugNativeIgnoresLkStale
    /\ ~fenceStale
    /\ \A t \in Tables : ~heldCache[t]
    /\ sid[s] >= termTxn \/ BugNativeNoTermCheck
    /\ sid[s] >= ddlLast
    /\ Committed(s)
    /\ UNCHANGED <<nodeVars, markVars, cacheVars, ddlVars, sid, began, seen>>

(* The Lua procedure, first part: writable(), the stream's transaction     *)
(* (else GONE_MSG; tarn_commit_local and tarn_prepare used to begin a new  *)
(* one), then lk_fresh() (lk_reload notes the term) and fence_fresh(),     *)
(* which may yield: other steps may run before LuaFinish.                  *)
LuaFresh(s) ==
    /\ st[s] \in {"open", "gone"} /\ ~ro
    /\ lkStale' = FALSE
    /\ IF st[s] = "gone" /\ BugCommitInNewTxn
       THEN \* (a transaction begun now, then lk_fresh)
            /\ sid' = [sid EXCEPT ![s] = next]
            /\ IF lkStale /\ lastTerm # term
               THEN termTxn' = next + 1 /\ lastTerm' = term /\ next' = next + 2
               ELSE next' = next + 1 /\ UNCHANGED <<termTxn, lastTerm>>
       ELSE /\ UNCHANGED sid
            /\ IF lkStale THEN TermMark ELSE UNCHANGED markVars
    /\ IF fenceStale /\ ~BugValidateNoFenceFresh
       THEN heldCache' = heldStore /\ fenceStale' = FALSE
       ELSE UNCHANGED <<heldCache, fenceStale>>
    /\ st' = [st EXCEPT ![s] =
                IF \/ st[s] = "gone" /\ ~BugCommitInNewTxn
                   \/ BugLuaTermCheckEarly /\ (term # lastTerm' \/ sid'[s] < termTxn')
                THEN "refused" ELSE "checking"]
    /\ UNCHANGED <<nodeVars, ddlVars, began, seen, vterm, vver, vheld>>

(* The Lua procedure, the rest, which does not yield until the commit:     *)
(* writable(), the term (BK.stream_lost: the node has been the leader      *)
(* since the stream began), then the tables read: not held, and no         *)
(* TRUNCATE applied since the stream began (reads_ddl_conflict).           *)
LuaFinish(s) ==
    /\ st[s] = "checking"
    /\ IF \/ ro
          \/ ~BugLuaNoTermCheck /\ ~BugLuaTermCheckEarly
             /\ (term # lastTerm \/ sid[s] < termTxn)
          \/ \E t \in Tables : seen[s][t] # Unread /\ (heldCache[t] \/ sid[s] < ddlTxn[t])
       THEN /\ st' = [st EXCEPT ![s] = "refused"] /\ UNCHANGED <<vterm, vver, vheld>>
       ELSE Committed(s)
    /\ UNCHANGED <<nodeVars, markVars, cacheVars, ddlVars, sid, began, seen>>

(* --- leadership --------------------------------------------------------- *)

(* raft's update triggers make the node read-only as its term changes;    *)
(* what it knows of the holds may be anything from now on.               *)
TermChange ==
    /\ terms < MaxTerms /\ terms' = terms + 1
    /\ term' = term + 1 /\ trig' = TRUE /\ other' = FALSE
    /\ ro' = (ro \/ ~BugRoAfterTermChange)
    /\ heldCache' \in [Tables -> BOOLEAN]
    /\ UNCHANGED <<markVars, lkStale, fenceStale, ddlVars, streamVars>>

LateReadOnly ==     \* (BugRoAfterTermChange)
    /\ ~ro /\ trig /\ ro' = TRUE
    /\ UNCHANGED <<term, trig, other, terms, markVars, cacheVars, ddlVars, streamVars>>

(* The Lua election trigger: LK_STALE, FENCE_STALE, then BK.term_mark      *)
(* (in a pcall: if it fails, lk_reload makes it).                         *)
Trigger(markOK) ==
    /\ trig /\ trig' = FALSE
    /\ lkStale' = TRUE /\ fenceStale' = TRUE
    /\ IF markOK THEN TermMark ELSE UNCHANGED markVars
    /\ UNCHANGED <<term, ro, other, terms, heldCache, ddlVars, streamVars>>

(* The node is elected: Tarantool promotes the synchro queue after the    *)
(* election triggers ran, and the node is writable.                      *)
BecomeWritable ==
    /\ ro /\ ~other /\ (~trig \/ BugWritableBeforeTrigger)
    /\ ro' = FALSE
    /\ UNCHANGED <<term, trig, other, terms, markVars, cacheVars, ddlVars, streamVars>>

(* Other requests: a lock check reloads the lock index (lk_reload notes   *)
(* the term), a write or a DDL step reads the holds again.               *)
LkReload ==
    /\ ~ro /\ lkStale /\ lkStale' = FALSE /\ TermMark
    /\ UNCHANGED <<nodeVars, fenceStale, heldCache, ddlVars, streamVars>>

FenceRebuild ==
    /\ fenceStale /\ fenceStale' = FALSE /\ heldCache' = heldStore
    /\ UNCHANGED <<nodeVars, markVars, lkStale, ddlVars, streamVars>>

(* --- TRUNCATE ------------------------------------------------------------ *)
(* PREPARE stores the hold, then caches it (fence_fresh, ddl_hold).  APPLY  *)
(* clears the table, notes DDL_TXN, commits the catalog (which may fail,   *)
(* and then the coordinator sends APPLY again, which finishes), and        *)
(* releases the hold.  While this node is read-only, a TRUNCATE on the      *)
(* leader reaches it by replication: its hold, then its new contents.      *)

Hold(t) ==
    /\ ~ro /\ apply[t] = "none" /\ ddls < MaxDDL /\ ddls' = ddls + 1
    /\ heldStore' = [heldStore EXCEPT ![t] = TRUE]
    /\ heldCache' = [heldStore EXCEPT ![t] = TRUE] /\ fenceStale' = FALSE
    /\ apply' = [apply EXCEPT ![t] = "held"]
    /\ UNCHANGED <<nodeVars, markVars, lkStale, ddlTxn, ddlLast, ver, streamVars>>

RemoteHold(t) ==
    /\ ro /\ apply[t] = "none" /\ ddls < MaxDDL /\ ddls' = ddls + 1
    /\ heldStore' = [heldStore EXCEPT ![t] = TRUE]
    /\ apply' = [apply EXCEPT ![t] = "held"]     \* (its APPLY may come to this node later)
    /\ other' = TRUE
    /\ UNCHANGED <<term, ro, trig, terms, markVars, cacheVars, ddlTxn, ddlLast, ver, streamVars>>

RemoteApply(t) ==
    /\ ro /\ apply[t] = "held"
    /\ ver' = [ver EXCEPT ![t] = ver[t] + 1]
    /\ heldStore' = [heldStore EXCEPT ![t] = FALSE]
    /\ apply' = [apply EXCEPT ![t] = "none"]
    /\ other' = TRUE
    /\ UNCHANGED <<term, ro, trig, terms, markVars, cacheVars, ddlTxn, ddlLast, ddls, streamVars>>

LocalStep == UNCHANGED <<nodeVars, lkStale, ddls, streamVars>>

Clear(t) ==
    /\ ~ro /\ apply[t] = IF BugDoomBeforeClear THEN "doomedEarly" ELSE "held"
    /\ ver' = [ver EXCEPT ![t] = ver[t] + 1]
    /\ apply' = [apply EXCEPT ![t] = IF BugDoomBeforeClear THEN "doomed" ELSE "cleared"]
    /\ UNCHANGED <<markVars, fenceStale, heldCache, heldStore, ddlTxn, ddlLast>> /\ LocalStep

DoomStep(t) ==
    /\ ~ro /\ ~BugDoomAfterCommit
    /\ apply[t] = IF BugDoomBeforeClear THEN "held" ELSE "cleared"
    /\ apply' = [apply EXCEPT ![t] = IF BugDoomBeforeClear THEN "doomedEarly" ELSE "doomed"]
    /\ Doom(t)
    /\ UNCHANGED <<fenceStale, heldCache, heldStore, ver>> /\ LocalStep

CatalogCommit(t, ok) ==
    /\ ~ro /\ apply[t] = IF BugDoomAfterCommit THEN "cleared" ELSE "doomed"
    /\ apply' = [apply EXCEPT ![t] = IF ok THEN "committed" ELSE "failed"]
    /\ UNCHANGED <<markVars, fenceStale, heldCache, heldStore, ddlTxn, ddlLast, ver>> /\ LocalStep

Release(t) ==
    /\ heldStore' = [heldStore EXCEPT ![t] = FALSE]
    /\ heldCache' = [heldCache EXCEPT ![t] = FALSE]
    /\ apply' = [apply EXCEPT ![t] = "none"]

Finish(t) ==        \* after the catalog commit (BugDoomAfterCommit: notes DDL_TXN now)
    /\ ~ro /\ apply[t] = "committed"
    /\ Release(t)
    /\ IF BugDoomAfterCommit THEN Doom(t) ELSE UNCHANGED <<markVars, ddlTxn, ddlLast>>
    /\ UNCHANGED <<fenceStale, ver>> /\ LocalStep

Rerun(t) ==         \* APPLY again: finishes what the failed one left
    /\ ~ro /\ apply[t] = "failed"
    /\ Release(t)
    /\ IF BugDoomAfterCommit THEN UNCHANGED <<markVars, ddlTxn, ddlLast>> ELSE Doom(t)
    /\ UNCHANGED <<fenceStale, ver>> /\ LocalStep

Next ==
    \/ \E s \in Streams : Begin(s) \/ TxnGone(s) \/ NativeStep(s) \/ LuaFresh(s) \/ LuaFinish(s)
    \/ \E s \in Streams, t \in Tables : Read(s, t)
    \/ TermChange \/ LateReadOnly \/ Trigger(TRUE) \/ Trigger(FALSE) \/ BecomeWritable
    \/ LkReload \/ FenceRebuild
    \/ \E t \in Tables : \/ Hold(t) \/ RemoteHold(t) \/ RemoteApply(t) \/ Clear(t)
                         \/ DoomStep(t) \/ CatalogCommit(t, TRUE) \/ CatalogCommit(t, FALSE)
                         \/ Finish(t) \/ Rerun(t)

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ next \in Nat /\ term \in Nat /\ ro \in BOOLEAN /\ trig \in BOOLEAN /\ other \in BOOLEAN
    /\ termTxn < next /\ lastTerm <= term
    /\ ddlLast < next /\ \A t \in Tables : ddlTxn[t] <= ddlLast
    /\ apply \in [Tables -> {"none", "held", "doomedEarly", "cleared", "doomed", "committed", "failed"}]
    /\ st \in [Streams -> {"none", "open", "gone", "checking", "done", "refused"}]
    /\ \A s \in Streams : st[s] # "none" => sid[s] < next

(* The reads a step committed were made in the term it committed them in,  *)
(* of each table as it still was then, with no TRUNCATE holding it.       *)
CommittedReadsCurrent ==
    \A s \in Streams : st[s] = "done" =>
        /\ vterm[s] = began[s]
        /\ \A t \in Tables : seen[s][t] # Unread =>
               vver[s][t] = seen[s][t] /\ ~vheld[s][t]
=============================================================================
