----------------------------- MODULE EngineCopy -----------------------------
(* Local online engine-copy refinement (#57), not the distributed commit
   protocol. A source write has an unconfirmed interval. Its on_replace
   captures old AND new PKs before it can commit; rollback may leave extra
   keys. Only a confirmed reread after fence/drain supplies final payloads.
   Baseline read and target write are separate actions. The shadow's
   secondary indexes may be created before its first page (as the
   implementation does since issue #267, so that no replica builds an index
   on a populated space) or behind the fence; either way PREPARED needs
   them. Rows here carry no secondary key: ShadowUnique checks the unique
   conflicts a stale copy meets in an index that exists during the copy.
   A term/capture-generation loss before PREPARED requires a full recopy;
   a durable PREPARED image instead survives and is never rebuilt by APPLY.
   An old request may still be delivered, but cannot pass current ownership.
   WritePage is a CONFIRMED atomic native effect. A detached commit does not
   disappear when its RPC ends: before reset/drop/certification, an engine
   terminal-ticket/queue cut must settle all old shadow effects. This is a
   runtime refinement premise mapped to LazyMeter's drain/queue/epoch cuts.
   Native DDL quiescence/owner certification and confirmed terminal receipts
   are runtime premises composed with BackfillDDL and PrepareConfirm.
   A catch-up pass (issue #184), during the copy or before the fence, takes
   the keys captured so far and leaves a fresh set to the writes from then
   on; once the writes noted in its set have terminated, it removes those
   keys' copies and puts their confirmed rows back, so that the final pass
   rereads only the keys written since. A pass that fails puts its keys
   back into the capture.
   The durable capture (issue #184) notes every key a committed write
   touched since the copy's capture began, in the write's own transaction,
   with the generation it was written in; a pass's swap starts the next
   generation, and the pass records the one it settled once it has reread
   its keys. The capture in memory also holds keys whose writes rolled
   back, which the durable one does not: a pass, and the final pass, clear
   and reread only the keys the durable capture notes above the settled
   generation (the others' copies are the source's rows), and a copy whose
   holder rule takes a captured row's copy out of the shadow notes that
   key as a hole, in the same transaction. A capture lost before PREPARED
   (a restart, a leader change) then leaves the copy where it was: once the
   writes in flight have terminated, the capture in memory is rebuilt from
   the keys and holes above the settled generation (Resume), or the copy
   starts over (Reset), as before the durable capture. *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugBeforeCapture, BugSkipPrefixDrain, BugCaptureOnlyNewKeys,
          BugUnconfirmedSource, BugTailIgnoresDeletes, BugSkipFinalDrain,
          BugLostCaptureContinues, BugStaleBatch, BugRebuildPrepared,
          BugPrepareWithoutIndexes, BugLosePersistentFence,
          BugPassWithoutSettle, BugPassDropsKeys, BugAbandonLosesKeys,
          BugCaptureNotDurable, BugResumeBeforeSettle, BugSettleBeforeReread,
          BugNoGenerationBump, BugClearUnnoted, BugHolderNoHole

Keys == {1, 2}
Values == 0..2                 \* 0: absent, 1/2: committed row versions
Empty == [k \in Keys |-> 0]
Rows(a, b) == [k \in Keys |-> IF k = 1 THEN a ELSE b]
MaxGen == 3                     \* generations of the durable capture
\* A write: its keys and row versions, and the durable capture's generation
\* it noted its keys in (0: none, no durable capture when it began).
NoWrite == [active |-> FALSE, keys |-> {}, values |-> Empty, gen |-> 0]
Mutations == {
  [active |-> TRUE, keys |-> {1}, values |-> Rows(0, 0)],
  [active |-> TRUE, keys |-> {1}, values |-> Rows(2, 0)],
  [active |-> TRUE, keys |-> {2}, values |-> Rows(0, 2)],
  [active |-> TRUE, keys |-> Keys, values |-> Rows(0, 2)]}
NoPage == [active |-> FALSE, key |-> 1, value |-> 0, gen |-> 0, dirty |-> FALSE]
\* lost: the capture in memory was lost during the copy or behind the fence
Phases == {"new", "capture", "copy", "fenced", "lost", "prepared", "published"}

VARIABLES source, shadow, copied, dirty, indexes,
          phase, capture, generation, restarted, fenced,
          pending, started, page, late, unconfirmedCopied,
          swept, cleared, unsettled, tailCleared,
          durable, entry, hole, gen, settled, swap
rvars == <<source, shadow, copied, dirty, indexes>>
cvars == <<phase, capture, generation, restarted, fenced>>
wvars == <<pending, started>>
qvars == <<page, late, unconfirmedCopied>>
pvars == <<swept, cleared, unsettled, tailCleared>>
\* The durable capture: whether there is one, each key's entry (the
\* generation of the last committed write noted, 0: none) and hole (the
\* generation the copy took the key's row out of the shadow in, 0: none),
\* the current generation, the settled one (in the job's record), and the
\* generation the pass under way settles (0: none).
dvars == <<durable, entry, hole, gen, settled, swap>>
vars == <<rvars, cvars, wvars, qvars, pvars, dvars>>
NoEntries == [k \in Keys |-> 0]
\* A key the durable capture notes above the settled generation.
Noted(k) == entry[k] > settled \/ hole[k] > settled
\* The keys of a set a pass or the final pass clears and rereads: those the
\* durable capture notes (all of them without one, or BugClearUnnoted).
Kept(set) == IF durable /\ ~BugClearUnnoted THEN {k \in set : Noted(k)} ELSE set

Init ==
    /\ source = Rows(1, 0) /\ shadow = Empty
    /\ copied = {} /\ dirty = {} /\ indexes = FALSE
    /\ phase = "new" /\ capture = FALSE /\ generation = 1
    /\ restarted = FALSE /\ fenced = FALSE
    /\ pending = NoWrite /\ started = 0
    /\ page = NoPage /\ late = NoPage /\ unconfirmedCopied = FALSE
    /\ swept = {} /\ cleared = FALSE /\ unsettled = FALSE /\ tailCleared = FALSE
    /\ durable = FALSE /\ entry = NoEntries /\ hole = NoEntries /\ gen = 1 /\ settled = 0 /\ swap = 0

(* Two writes permit delete/reinsert and a PK move. A pending operation can
   stand for a local/RC/native commit or a previously prepared APPLY: once
   admitted it is allowed to finish even after the final fresh-write fence.
   Its on_replace notes its keys in the capture in memory, if there is one,
   and in the durable capture, with the current generation: an entry its
   own transaction writes, so there only if it commits (BugCaptureNotDurable
   notes nothing there). Writes go on while the capture in memory is lost. *)
StartWrite(m) ==
    /\ phase \in {"new", "capture", "copy", "lost"} /\ ~fenced
    /\ ~pending.active /\ started < 2 /\ m \in Mutations
    /\ pending' = [active |-> TRUE, keys |-> m.keys, values |-> m.values,
                    gen |-> IF durable /\ ~BugCaptureNotDurable THEN gen ELSE 0]
    /\ started' = started + 1
    /\ dirty' = IF capture
                 THEN dirty \cup (IF BugCaptureOnlyNewKeys
                                  THEN {k \in m.keys : m.values[k] # 0} ELSE m.keys)
                 ELSE dirty
    /\ UNCHANGED <<source, shadow, copied, indexes, cvars, qvars, pvars, dvars>>

FinishWrite(commit) ==
    /\ pending.active
    /\ source' = IF commit THEN [k \in Keys |->
                      IF k \in pending.keys THEN pending.values[k] ELSE source[k]]
                   ELSE source
    /\ entry' = IF commit /\ pending.gen > 0
                 THEN [k \in Keys |-> IF k \in pending.keys THEN pending.gen ELSE entry[k]]
                 ELSE entry
    /\ pending' = NoWrite
    \* (A pass waits for this write if its set noted it. Only one write is
    \* pending at a time, so its end settles the pass.)
    /\ unsettled' = FALSE
    /\ UNCHANGED <<shadow, copied, dirty, indexes, cvars, started, qvars, swept, cleared, tailCleared,
                   durable, hole, gen, settled, swap>>

(* A capture: in memory, and a durable one of its own, empty (the reset
   before a copy, or a copy started over). *)
InstallCapture ==
    /\ ~capture /\ phase \in {"new", "capture", "copy"}
    /\ capture' = TRUE /\ phase' = IF phase = "new" THEN "capture" ELSE phase
    /\ durable' = TRUE /\ entry' = NoEntries /\ hole' = NoEntries /\ gen' = 1 /\ settled' = 0 /\ swap' = 0
    /\ UNCHANGED <<rvars, generation, restarted, fenced, wvars, qvars, pvars>>

BeginCopy ==
    /\ phase \in {"new", "capture"}
    /\ capture \/ BugBeforeCapture
    /\ ~pending.active \/ BugSkipPrefixDrain
    /\ phase' = "copy"
    /\ UNCHANGED <<rvars, capture, generation, restarted, fenced, wvars, qvars, pvars, dvars>>

ReadPage(k) ==
    /\ phase = "copy" /\ ~page.active /\ k \in Keys \ copied /\ swept = {}
    /\ LET unconfirmed == BugUnconfirmedSource /\ pending.active /\ k \in pending.keys
                              /\ pending.values[k] # source[k]
       IN page' = [active |-> TRUE, key |-> k,
                   value |-> IF unconfirmed THEN pending.values[k] ELSE source[k],
                   gen |-> generation, dirty |-> unconfirmed]
    /\ UNCHANGED <<rvars, cvars, wvars, late, unconfirmedCopied, pvars, dvars>>

WritePage ==
    /\ page.active
    /\ IF page.gen = generation /\ phase = "copy"
       THEN /\ shadow' = [shadow EXCEPT ![page.key] = page.value]
            /\ copied' = copied \cup {page.key}
            /\ unconfirmedCopied' = (unconfirmedCopied \/ page.dirty)
       ELSE UNCHANGED <<shadow, copied, unconfirmedCopied>>
    /\ page' = NoPage
    /\ UNCHANGED <<source, dirty, indexes, cvars, wvars, late, pvars, dvars>>

Fence ==
    /\ phase = "copy" /\ copied = Keys /\ ~page.active /\ swept = {} /\ swap = 0
    /\ phase' = "fenced" /\ fenced' = TRUE
    /\ UNCHANGED <<rvars, capture, generation, restarted, wvars, qvars, pvars, dvars>>

(* The final pass: first the captured keys the durable capture notes leave
   the shadow (BugTailIgnoresDeletes leaves the copies of rows the source
   deleted), then their confirmed rows go in, one by one. *)
TailClear ==
    /\ phase = "fenced" /\ ~tailCleared
    /\ ~pending.active \/ BugSkipFinalDrain
    /\ LET keep == Kept(dirty)
       IN /\ shadow' = [k \in Keys |-> IF k \in keep /\ ~(BugTailIgnoresDeletes /\ source[k] = 0)
                                       THEN 0 ELSE shadow[k]]
          /\ dirty' = keep
    /\ tailCleared' = TRUE
    /\ UNCHANGED <<source, copied, indexes, cvars, wvars, qvars, swept, cleared, unsettled, dvars>>
Tail(k) ==
    /\ phase = "fenced" /\ tailCleared /\ k \in dirty
    /\ ~pending.active \/ BugSkipFinalDrain
    /\ shadow' = IF BugTailIgnoresDeletes /\ source[k] = 0
                  THEN shadow ELSE [shadow EXCEPT ![k] = source[k]]
    /\ dirty' = dirty \ {k}
    /\ UNCHANGED <<source, copied, indexes, cvars, wvars, qvars, pvars, dvars>>

BuildIndexes ==
    /\ ~indexes
    /\ \/ phase = "copy"
       \/ phase = "fenced" /\ tailCleared /\ dirty = {} /\ (~pending.active \/ BugSkipFinalDrain)
    /\ indexes' = TRUE
    /\ UNCHANGED <<source, shadow, copied, dirty, cvars, wvars, qvars, pvars, dvars>>

Prepare ==
    /\ phase = "fenced" /\ tailCleared /\ dirty = {}
    /\ ~pending.active \/ BugSkipFinalDrain
    /\ indexes \/ BugPrepareWithoutIndexes
    /\ phase' = "prepared" /\ capture' = FALSE
    /\ UNCHANGED <<rvars, generation, restarted, fenced, wvars, qvars, pvars, dvars>>

Publish ==
    /\ phase = "prepared" /\ phase' = "published"
    /\ UNCHANGED <<rvars, capture, generation, restarted, fenced, wvars, qvars, pvars, dvars>>

(* A leader/capture loss may leave an old unconfirmed write to resolve.
   The new prefix drain must include that queue/inflight effect. The held/fenced state is
   durable; loss of volatile keys cannot lower it. A delayed old RPC is
   kept separately so the stale-owner control has a real late delivery.
   The copy (shadow, cursor) and the durable capture are durable too: a
   capture lost during the copy, or behind the fence, leaves them for
   Resume or Reset; one lost before the copy began starts it again. A pass
   under way is lost with it, unsettled. *)
Restart ==
    /\ ~restarted /\ phase # "new"
    /\ LET certified == phase \in {"prepared", "published"}
           wipe == certified /\ BugRebuildPrepared
       IN /\ shadow' = IF wipe THEN Empty ELSE shadow
          /\ copied' = IF wipe THEN {} ELSE copied
          /\ indexes' = IF wipe THEN FALSE ELSE indexes
          /\ phase' = IF certified \/ phase = "capture" THEN phase ELSE "lost"
          /\ fenced' = IF certified /\ BugLosePersistentFence THEN FALSE ELSE fenced
    /\ dirty' = {} /\ capture' = FALSE
    /\ generation' = 2 /\ restarted' = TRUE
    /\ late' = IF page.active THEN page ELSE NoPage
    /\ page' = NoPage
    /\ swept' = {} /\ cleared' = FALSE /\ unsettled' = FALSE /\ tailCleared' = FALSE /\ swap' = 0
    /\ UNCHANGED <<source, pending, started, unconfirmedCopied, durable, entry, hole, gen, settled>>

(* The capture in memory rebuilt from the durable one: the keys and holes
   above the settled generation. The writes noted only there may still be in flight,
   which it waits for, as a pass does, before it reads the durable capture
   (BugResumeBeforeSettle reads it with one in flight, whose entry it then
   misses). BugLostCaptureContinues goes on without the durable capture's
   keys. *)
Resume ==
    /\ phase = "lost" /\ durable
    /\ ~pending.active \/ BugResumeBeforeSettle
    /\ capture' = TRUE
    /\ dirty' = IF BugLostCaptureContinues THEN {} ELSE {k \in Keys : Noted(k)}
    /\ phase' = IF fenced THEN "fenced" ELSE "copy"
    /\ UNCHANGED <<source, shadow, copied, indexes, generation, restarted, fenced, wvars, qvars, pvars, dvars>>

(* Or the copy starts over (past the capture's limits, or no durable
   capture), with a capture of its own (InstallCapture). *)
Reset ==
    /\ phase = "lost"
    /\ shadow' = Empty /\ copied' = {} /\ indexes' = FALSE
    /\ phase' = "capture" /\ durable' = FALSE
    /\ UNCHANGED <<source, dirty, capture, generation, restarted, fenced, wvars, qvars, pvars,
                   entry, hole, gen, settled, swap>>

DeliverOldPage ==
    /\ late.active
    /\ IF BugStaleBatch
       THEN shadow' = [shadow EXCEPT ![late.key] = late.value]
       ELSE UNCHANGED shadow
    /\ late' = NoPage
    /\ UNCHANGED <<source, copied, dirty, indexes, cvars, wvars, page, unconfirmedCopied, pvars, dvars>>

(* A catch-up pass: its set is the keys captured so far, and a fresh one
   takes the writes from now on. A write noted in its set may still be in
   flight (unsettled): a caller still in its commit, which the pass waits
   for, or a write whose caller left it in the synchronous queue, which the
   pass's marker commits after. Once it has terminated (BugPassWithoutSettle
   does not wait), the pass removes the set's copies and puts their
   confirmed rows back (BugPassDropsKeys removes them only). Not between a
   page's read and its write: the copy's fiber runs the pass between pages.
   Its swap also starts the durable capture's next generation (not with
   BugNoGenerationBump), and once its keys are reread it records the one it
   settles (BugSettleBeforeReread records it before). *)
BeginPass ==
    /\ phase = "copy" /\ ~fenced /\ ~page.active /\ swept = {} /\ dirty # {}
    /\ swap = 0 /\ gen < MaxGen
    /\ swept' = dirty /\ dirty' = {} /\ unsettled' = pending.active
    /\ swap' = gen /\ gen' = IF BugNoGenerationBump THEN gen ELSE gen + 1
    /\ UNCHANGED <<source, shadow, copied, indexes, cvars, wvars, qvars, cleared, tailCleared,
                   durable, entry, hole, settled>>
ClearPass ==
    /\ swept # {} /\ ~cleared /\ (~unsettled \/ BugPassWithoutSettle)
    /\ LET keep == Kept(swept)
       IN /\ shadow' = [k \in Keys |-> IF k \in keep THEN 0 ELSE shadow[k]]
          /\ swept' = keep
          /\ cleared' = (keep # {})
    /\ UNCHANGED <<source, copied, dirty, indexes, cvars, wvars, qvars, unsettled, tailCleared, dvars>>
RereadPass(k) ==
    /\ k \in swept /\ cleared /\ (~unsettled \/ BugPassWithoutSettle)
    /\ shadow' = IF BugPassDropsKeys THEN shadow ELSE [shadow EXCEPT ![k] = source[k]]
    /\ swept' = swept \ {k}
    /\ cleared' = (swept' # {})
    /\ UNCHANGED <<source, copied, dirty, indexes, cvars, wvars, qvars, unsettled, tailCleared, dvars>>
SettlePass ==
    /\ swap > 0 /\ (swept = {} \/ BugSettleBeforeReread)
    /\ settled' = IF swap > settled THEN swap ELSE settled
    /\ swap' = 0
    /\ UNCHANGED <<rvars, cvars, wvars, qvars, pvars, durable, entry, hole, gen>>

(* The holder rule (tla/ShadowUnique.tla), here for any captured row: a
   page's row, or a pass's reread one, needs a unique key a captured row's
   copy holds, which leaves the shadow until a pass or the final pass puts
   that row back. The same transaction notes the key as a hole in the
   durable capture (BugHolderNoHole does not). *)
HolderDelete(k) ==
    /\ phase = "copy" /\ capture /\ ~page.active /\ k \in dirty /\ k \in copied
    /\ shadow' = [shadow EXCEPT ![k] = 0]
    /\ hole' = IF durable /\ ~BugHolderNoHole THEN [hole EXCEPT ![k] = gen] ELSE hole
    /\ UNCHANGED <<source, copied, dirty, indexes, cvars, wvars, qvars, pvars, durable, entry, gen, settled, swap>>

(* A pass that fails at any step (a lost cut, a deadline, a cancelled
   fiber) puts the keys it has not reread back into the capture: they may
   be stale, or missing, in the shadow. (The implementation puts back all of
   them, within the capture's limits; past those the capture is lost, which
   Restart covers.) BugAbandonLosesKeys drops them. It settles nothing. *)
AbandonPass ==
    /\ swept # {} \/ swap > 0
    /\ dirty' = IF BugAbandonLosesKeys THEN dirty ELSE dirty \cup swept
    /\ swept' = {} /\ cleared' = FALSE /\ unsettled' = FALSE /\ swap' = 0
    /\ UNCHANGED <<source, shadow, copied, indexes, cvars, wvars, qvars, tailCleared,
                   durable, entry, hole, gen, settled>>

Next == (\E m \in Mutations : StartWrite(m)) \/ FinishWrite(TRUE) \/ FinishWrite(FALSE)
        \/ InstallCapture \/ BeginCopy \/ (\E k \in Keys : ReadPage(k)) \/ WritePage
        \/ Fence \/ TailClear \/ (\E k \in Keys : Tail(k)) \/ BuildIndexes \/ Prepare \/ Publish
        \/ Restart \/ Resume \/ Reset \/ DeliverOldPage
        \/ BeginPass \/ ClearPass \/ (\E k \in Keys : RereadPass(k)) \/ SettlePass \/ AbandonPass
        \/ (\E k \in Keys : HolderDelete(k))
Spec == Init /\ [][Next]_vars

TypeOK == /\ source \in [Keys -> Values] /\ shadow \in [Keys -> Values]
          /\ copied \subseteq Keys /\ dirty \subseteq Keys /\ indexes \in BOOLEAN
          /\ phase \in Phases /\ capture \in BOOLEAN /\ generation \in 1..2
          /\ restarted \in BOOLEAN /\ fenced \in BOOLEAN
          /\ pending \in [active : BOOLEAN, keys : SUBSET Keys, values : [Keys -> Values], gen : 0..MaxGen]
          /\ started \in 0..2
          /\ page \in [active : BOOLEAN, key : Keys, value : Values, gen : 0..2, dirty : BOOLEAN]
          /\ late \in [active : BOOLEAN, key : Keys, value : Values, gen : 0..2, dirty : BOOLEAN]
          /\ unconfirmedCopied \in BOOLEAN
          /\ swept \subseteq Keys /\ cleared \in BOOLEAN /\ unsettled \in BOOLEAN
          /\ tailCleared \in BOOLEAN
          /\ durable \in BOOLEAN /\ entry \in [Keys -> 0..MaxGen] /\ hole \in [Keys -> 0..MaxGen]
          /\ gen \in 1..MaxGen
          /\ settled \in 0..MaxGen /\ swap \in 0..MaxGen
ConfirmedCopies == ~unconfirmedCopied
PreparedImage == phase \in {"prepared", "published"} =>
                  /\ shadow = source /\ copied = Keys /\ indexes /\ fenced
=============================================================================
