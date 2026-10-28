---------------------------- MODULE NativeHandback ----------------------------
(***************************************************************************)
(* The contract between the SQL server and a shard's native procedure for  *)
(* a step that commits writes (ts_autocommit, ts_commit_local; issue #28): *)
(* the native procedure makes the step as the Lua procedure would, or      *)
(* hands it back (["lua"]) having changed nothing -- never once its commit *)
(* began; the SQL server calls the Lua procedure after a hand-back, in the *)
(* same stream, and never sends again a call whose outcome it does not     *)
(* know (Cluster::call_once): it reports "outcome unknown" (1180).          *)
(*                                                                         *)
(* One transaction's writes W on one shard: in its stream, whose           *)
(* transaction holds its reads (InStream), or an autocommit write.  The    *)
(* native procedure takes a savepoint (or begins a transaction of its      *)
(* own), makes W, and commits; or it gives up, takes W back to the         *)
(* savepoint, and hands the step back -- should that rollback fail, it     *)
(* rolls the whole transaction back and replies 1213 (txn_lost).  Its      *)
(* commit succeeds, or fails having made nothing (a conflict, a rollback   *)
(* by a new leader), or ends so that W may or may not be made (a panic     *)
(* once it began: outcome_unknown).  The Lua procedure is one step here:   *)
(* in the stream's transaction it makes W and commits them with what that  *)
(* transaction holds; outside a stream, in a transaction of its own;       *)
(* reads whose stream's transaction is gone, it refuses (StreamMarks.tla   *)
(* checks that).  The connection may drop at any moment: the replies on it *)
(* are lost, the requests already sent may still run, and a stream's       *)
(* transaction still open when its session ends is rolled back.            *)
(*                                                                         *)
(* Writes are counted, not valued.  AtMostOnce: W were committed at most   *)
(* once.  ReplyHolds: the client's ok means once, its error never; only an *)
(* unknown outcome leaves both open.                                       *)
(* Autocommit Lua fallback retains its selected physical connection; only *)
(* an explicit read-only refusal permits leader rediscovery. Pool choice  *)
(* and distribution are outside this model (real Session tests, #155).    *)
(***************************************************************************)
EXTENDS Naturals

CONSTANTS
    InStream,                      \* the step commits a stream's transaction (else autocommit)
    BugKeepWritesOnHandback,       \* a hand-back leaves the writes in the stream's transaction
    BugHandbackAfterCommitBegan,   \* ... and one once the commit began, when it fails
    BugResendAfterLoss             \* the SQL server calls again after a lost connection

VARIABLES
    txn,        \* the stream's transaction: none, open, gone (rolled back), committed
    pend,       \* the times W were made in it, not committed
    applied,    \* the times W were committed
    called,     \* the calls the SQL server sent: native, lua
    nat,        \* the native procedure: idle, writing, wrote, done
    lua,        \* the Lua procedure: idle, done
    conn,       \* the connection: up, lost
    sq,         \* the SQL server: start, native, lua (a call out), resend, ok, err, unknown
    reply       \* the reply on its way: none, ok, err, lua, unknown

vars == <<txn, pend, applied, called, nat, lua, conn, sq, reply>>

Init ==
    /\ txn = IF InStream THEN "open" ELSE "none"
    /\ pend = 0 /\ applied = 0 /\ called = {}
    /\ nat = "idle" /\ lua = "idle"
    /\ conn = "up" /\ sq = "start" /\ reply = "none"

Reply(v) == reply' = IF conn = "up" THEN v ELSE "none"

(* --- the SQL server ------------------------------------------------------ *)

CallNative ==
    /\ sq = "start" /\ sq' = "native" /\ called' = called \cup {"native"}
    /\ UNCHANGED <<txn, pend, applied, nat, lua, conn, reply>>

(* A reply: ["lua"] -> the Lua procedure, on the same stream. *)
Receive ==
    /\ sq \in {"native", "lua"} /\ reply # "none" /\ conn = "up"
    /\ sq' = CASE reply = "lua" /\ sq = "native" -> "lua"
               [] reply = "ok"                  -> "ok"
               [] reply = "err"                 -> "err"
               [] OTHER                         -> "unknown"
    /\ called' = IF reply = "lua" /\ sq = "native" THEN called \cup {"lua"} ELSE called
    /\ reply' = "none"
    /\ UNCHANGED <<txn, pend, applied, nat, lua, conn>>

(* The connection drops while a call is out: its outcome is unknown. *)
Lose ==
    /\ conn = "up" /\ sq \in {"native", "lua"}
    /\ conn' = "lost" /\ reply' = "none"
    /\ sq' = IF BugResendAfterLoss THEN "resend" ELSE "unknown"
    /\ UNCHANGED <<txn, pend, applied, called, nat, lua>>

(* (BugResendAfterLoss) the call again, on a new connection: outside any   *)
(* stream, so the Lua procedure, there, refuses reads (their stream's      *)
(* transaction is gone) and makes an autocommit write.                     *)
Resend ==
    /\ sq = "resend"
    /\ IF InStream
       THEN sq' = "err" /\ UNCHANGED applied
       ELSE applied' = applied + 1 /\ sq' = "ok"
    /\ UNCHANGED <<txn, pend, called, nat, lua, conn, reply>>

(* A stream's transaction still open when its session ends is rolled back. *)
SessionEnd ==
    /\ conn = "lost" /\ txn = "open" /\ nat \in {"idle", "done"}
    /\ txn' = "gone" /\ pend' = 0
    /\ UNCHANGED <<applied, called, nat, lua, conn, sq, reply>>

(* --- the native procedure ------------------------------------------------ *)

(* Its checks: it serves the step, or hands it back at once.  In a stream, *)
(* it needs the stream's transaction (else it hands back).                 *)
NativeStart(serve) ==
    /\ "native" \in called /\ nat = "idle"
    /\ IF serve /\ (~InStream \/ txn = "open")
       THEN nat' = "writing" /\ UNCHANGED reply
       ELSE nat' = "done" /\ Reply("lua")
    /\ UNCHANGED <<txn, pend, applied, called, lua, conn, sq>>

(* The writes, under the savepoint. *)
NativeWrite ==
    /\ nat = "writing" /\ nat' = "wrote"
    /\ pend' = pend + 1
    /\ UNCHANGED <<txn, applied, called, lua, conn, sq, reply>>

(* A write or a check fails: W back to the savepoint, and the step handed *)
(* back; or (txn_lost) the whole transaction rolled back, and 1213.        *)
NativeGiveUp(rollbackOK) ==
    /\ nat \in {"writing", "wrote"} /\ nat' = "done"
    /\ IF rollbackOK
       THEN /\ pend' = IF nat = "wrote" /\ ~BugKeepWritesOnHandback THEN pend - 1 ELSE pend
            /\ Reply("lua") /\ UNCHANGED txn
       ELSE /\ pend' = 0 /\ txn' = IF InStream THEN "gone" ELSE txn
            /\ Reply("err")
    /\ UNCHANGED <<applied, called, lua, conn, sq>>

(* The commit: made; or failed having made nothing; or of unknown outcome *)
(* (outcome_unknown).  Once it began, the procedure does not hand back.   *)
NativeCommit(outcome) ==
    /\ nat = "wrote" /\ nat' = "done"
    /\ CASE outcome = "ok" ->
              /\ applied' = applied + pend /\ pend' = 0
              /\ txn' = IF InStream THEN "committed" ELSE txn
              /\ Reply("ok")
         [] outcome = "failed" ->
              /\ UNCHANGED applied /\ pend' = 0
              /\ txn' = IF InStream THEN "gone" ELSE txn
              /\ Reply(IF BugHandbackAfterCommitBegan THEN "lua" ELSE "err")
         [] OTHER ->
              /\ applied' \in {applied, applied + pend} /\ pend' = 0
              /\ txn' = IF InStream THEN "gone" ELSE txn
              /\ Reply(IF BugHandbackAfterCommitBegan THEN "lua" ELSE "unknown")
    /\ UNCHANGED <<called, lua, conn, sq>>

(* --- the Lua procedure --------------------------------------------------- *)

LuaStep ==
    /\ "lua" \in called /\ lua = "idle" /\ lua' = "done"
    /\ IF ~InStream
       THEN applied' = applied + 1 /\ Reply("ok") /\ UNCHANGED <<txn, pend>>
       ELSE IF txn = "open"
       THEN \* W, committed with the stream's transaction and what it holds
            /\ applied' = applied + pend + 1 /\ pend' = 0 /\ txn' = "committed"
            /\ Reply("ok")
       ELSE /\ Reply("err") /\ UNCHANGED <<txn, pend, applied>>
    /\ UNCHANGED <<called, nat, conn, sq>>

Next ==
    \/ CallNative \/ Receive \/ Lose \/ Resend \/ SessionEnd
    \/ NativeStart(TRUE) \/ NativeStart(FALSE) \/ NativeWrite
    \/ NativeGiveUp(TRUE) \/ NativeGiveUp(FALSE)
    \/ NativeCommit("ok") \/ NativeCommit("failed") \/ NativeCommit("maybe")
    \/ LuaStep

Spec == Init /\ [][Next]_vars

TypeOK ==
    /\ txn \in {"none", "open", "gone", "committed"}
    /\ pend \in 0..2 /\ applied \in 0..3
    /\ called \subseteq {"native", "lua"}
    /\ nat \in {"idle", "writing", "wrote", "done"} /\ lua \in {"idle", "done"}
    /\ conn \in {"up", "lost"}
    /\ sq \in {"start", "native", "lua", "resend", "ok", "err", "unknown"}
    /\ reply \in {"none", "ok", "err", "lua", "unknown"}

AtMostOnce == applied <= 1
ReplyHolds ==
    /\ sq = "ok" => applied = 1
    /\ sq = "err" => applied = 0
=============================================================================
