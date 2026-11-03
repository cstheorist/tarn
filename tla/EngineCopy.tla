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
   back into the capture. *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugBeforeCapture, BugSkipPrefixDrain, BugCaptureOnlyNewKeys,
          BugUnconfirmedSource, BugTailIgnoresDeletes, BugSkipFinalDrain,
          BugLostCaptureContinues, BugStaleBatch, BugRebuildPrepared,
          BugPrepareWithoutIndexes, BugLosePersistentFence,
          BugPassWithoutSettle, BugPassDropsKeys, BugAbandonLosesKeys

Keys == {1, 2}
Values == 0..2                 \* 0: absent, 1/2: committed row versions
Empty == [k \in Keys |-> 0]
Rows(a, b) == [k \in Keys |-> IF k = 1 THEN a ELSE b]
NoWrite == [active |-> FALSE, keys |-> {}, values |-> Empty]
Mutations == {
  [active |-> TRUE, keys |-> {1}, values |-> Rows(0, 0)],
  [active |-> TRUE, keys |-> {1}, values |-> Rows(2, 0)],
  [active |-> TRUE, keys |-> {2}, values |-> Rows(0, 2)],
  [active |-> TRUE, keys |-> Keys, values |-> Rows(0, 2)]}
NoPage == [active |-> FALSE, key |-> 1, value |-> 0, gen |-> 0, dirty |-> FALSE]
Phases == {"new", "capture", "copy", "fenced", "prepared", "published"}

VARIABLES source, shadow, copied, dirty, indexes,
          phase, capture, generation, restarted, fenced,
          pending, started, page, late, unconfirmedCopied,
          swept, cleared, unsettled
rvars == <<source, shadow, copied, dirty, indexes>>
cvars == <<phase, capture, generation, restarted, fenced>>
wvars == <<pending, started>>
qvars == <<page, late, unconfirmedCopied>>
pvars == <<swept, cleared, unsettled>>
vars == <<rvars, cvars, wvars, qvars, pvars>>

Init ==
    /\ source = Rows(1, 0) /\ shadow = Empty
    /\ copied = {} /\ dirty = {} /\ indexes = FALSE
    /\ phase = "new" /\ capture = FALSE /\ generation = 1
    /\ restarted = FALSE /\ fenced = FALSE
    /\ pending = NoWrite /\ started = 0
    /\ page = NoPage /\ late = NoPage /\ unconfirmedCopied = FALSE
    /\ swept = {} /\ cleared = FALSE /\ unsettled = FALSE

(* Two writes permit delete/reinsert and a PK move. A pending operation can
   stand for a local/RC/native commit or a previously prepared APPLY: once
   admitted it is allowed to finish even after the final fresh-write fence. *)
StartWrite(m) ==
    /\ phase \in {"new", "capture", "copy"} /\ ~fenced
    /\ ~pending.active /\ started < 2 /\ m \in Mutations
    /\ pending' = m /\ started' = started + 1
    /\ dirty' = IF capture
                 THEN dirty \cup (IF BugCaptureOnlyNewKeys
                                  THEN {k \in m.keys : m.values[k] # 0} ELSE m.keys)
                 ELSE dirty
    /\ UNCHANGED <<source, shadow, copied, indexes, cvars, qvars, pvars>>

FinishWrite(commit) ==
    /\ pending.active
    /\ source' = IF commit THEN [k \in Keys |->
                      IF k \in pending.keys THEN pending.values[k] ELSE source[k]]
                   ELSE source
    /\ pending' = NoWrite
    \* (A pass waits for this write if its set noted it. Only one write is
    \* pending at a time, so its end settles the pass.)
    /\ unsettled' = FALSE
    /\ UNCHANGED <<shadow, copied, dirty, indexes, cvars, started, qvars, swept, cleared>>

InstallCapture ==
    /\ ~capture /\ phase \in {"new", "capture", "copy"}
    /\ capture' = TRUE /\ phase' = IF phase = "new" THEN "capture" ELSE phase
    /\ UNCHANGED <<rvars, generation, restarted, fenced, wvars, qvars, pvars>>

BeginCopy ==
    /\ phase \in {"new", "capture"}
    /\ capture \/ BugBeforeCapture
    /\ ~pending.active \/ BugSkipPrefixDrain
    /\ phase' = "copy"
    /\ UNCHANGED <<rvars, capture, generation, restarted, fenced, wvars, qvars, pvars>>

ReadPage(k) ==
    /\ phase = "copy" /\ ~page.active /\ k \in Keys \ copied /\ swept = {}
    /\ LET unconfirmed == BugUnconfirmedSource /\ pending.active /\ k \in pending.keys
                              /\ pending.values[k] # source[k]
       IN page' = [active |-> TRUE, key |-> k,
                   value |-> IF unconfirmed THEN pending.values[k] ELSE source[k],
                   gen |-> generation, dirty |-> unconfirmed]
    /\ UNCHANGED <<rvars, cvars, wvars, late, unconfirmedCopied, pvars>>

WritePage ==
    /\ page.active
    /\ IF page.gen = generation /\ phase = "copy"
       THEN /\ shadow' = [shadow EXCEPT ![page.key] = page.value]
            /\ copied' = copied \cup {page.key}
            /\ unconfirmedCopied' = (unconfirmedCopied \/ page.dirty)
       ELSE UNCHANGED <<shadow, copied, unconfirmedCopied>>
    /\ page' = NoPage
    /\ UNCHANGED <<source, dirty, indexes, cvars, wvars, late, pvars>>

Fence ==
    /\ phase = "copy" /\ copied = Keys /\ ~page.active /\ swept = {}
    /\ phase' = "fenced" /\ fenced' = TRUE
    /\ UNCHANGED <<rvars, capture, generation, restarted, wvars, qvars, pvars>>

Tail(k) ==
    /\ phase = "fenced" /\ k \in dirty
    /\ ~pending.active \/ BugSkipFinalDrain
    /\ shadow' = IF BugTailIgnoresDeletes /\ source[k] = 0
                  THEN shadow ELSE [shadow EXCEPT ![k] = source[k]]
    /\ dirty' = dirty \ {k}
    /\ UNCHANGED <<source, copied, indexes, cvars, wvars, qvars, pvars>>

BuildIndexes ==
    /\ ~indexes
    /\ \/ phase = "copy"
       \/ phase = "fenced" /\ dirty = {} /\ (~pending.active \/ BugSkipFinalDrain)
    /\ indexes' = TRUE
    /\ UNCHANGED <<source, shadow, copied, dirty, cvars, wvars, qvars, pvars>>

Prepare ==
    /\ phase = "fenced" /\ dirty = {}
    /\ ~pending.active \/ BugSkipFinalDrain
    /\ indexes \/ BugPrepareWithoutIndexes
    /\ phase' = "prepared" /\ capture' = FALSE
    /\ UNCHANGED <<rvars, generation, restarted, fenced, wvars, qvars, pvars>>

Publish ==
    /\ phase = "prepared" /\ phase' = "published"
    /\ UNCHANGED <<rvars, capture, generation, restarted, fenced, wvars, qvars, pvars>>

(* A leader/capture loss may leave an old unconfirmed write to resolve.
   The new prefix drain must include that queue/inflight effect. The held/fenced state is
   durable; loss of volatile keys cannot lower it. A delayed old RPC is
   kept separately so the stale-owner control has a real late delivery. *)
Restart ==
    /\ ~restarted /\ phase # "new"
    /\ LET certified == phase \in {"prepared", "published"}
           keep == IF certified THEN ~BugRebuildPrepared ELSE BugLostCaptureContinues
       IN /\ shadow' = IF keep THEN shadow ELSE Empty
          /\ copied' = IF keep THEN copied ELSE {}
          /\ indexes' = IF keep THEN indexes ELSE FALSE
          /\ phase' = IF certified THEN phase ELSE "capture"
          /\ fenced' = IF certified /\ BugLosePersistentFence THEN FALSE ELSE fenced
    /\ dirty' = {} /\ capture' = FALSE
    /\ generation' = 2 /\ restarted' = TRUE
    /\ late' = IF page.active THEN page ELSE NoPage
    /\ page' = NoPage
    /\ swept' = {} /\ cleared' = FALSE /\ unsettled' = FALSE
    /\ UNCHANGED <<source, pending, started, unconfirmedCopied>>

DeliverOldPage ==
    /\ late.active
    /\ IF BugStaleBatch
       THEN shadow' = [shadow EXCEPT ![late.key] = late.value]
       ELSE UNCHANGED shadow
    /\ late' = NoPage
    /\ UNCHANGED <<source, copied, dirty, indexes, cvars, wvars, page, unconfirmedCopied, pvars>>

(* A catch-up pass: its set is the keys captured so far, and a fresh one
   takes the writes from now on. A write noted in its set may still be in
   flight (unsettled): a caller still in its commit, which the pass waits
   for, or a write whose caller left it in the synchronous queue, which the
   pass's marker commits after. Once it has terminated (BugPassWithoutSettle
   does not wait), the pass removes the set's copies and puts their
   confirmed rows back (BugPassDropsKeys removes them only). Not between a
   page's read and its write: the copy's fiber runs the pass between pages. *)
BeginPass ==
    /\ phase = "copy" /\ ~fenced /\ ~page.active /\ swept = {} /\ dirty # {}
    /\ swept' = dirty /\ dirty' = {} /\ unsettled' = pending.active
    /\ UNCHANGED <<source, shadow, copied, indexes, cvars, wvars, qvars, cleared>>
ClearPass ==
    /\ swept # {} /\ ~cleared /\ (~unsettled \/ BugPassWithoutSettle)
    /\ shadow' = [k \in Keys |-> IF k \in swept THEN 0 ELSE shadow[k]]
    /\ cleared' = TRUE
    /\ UNCHANGED <<source, copied, dirty, indexes, cvars, wvars, qvars, swept, unsettled>>
RereadPass(k) ==
    /\ k \in swept /\ cleared /\ (~unsettled \/ BugPassWithoutSettle)
    /\ shadow' = IF BugPassDropsKeys THEN shadow ELSE [shadow EXCEPT ![k] = source[k]]
    /\ swept' = swept \ {k}
    /\ cleared' = (swept' # {})
    /\ UNCHANGED <<source, copied, dirty, indexes, cvars, wvars, qvars, unsettled>>

(* A pass that fails at any step (a lost cut, a deadline, a cancelled
   fiber) puts the keys it has not reread back into the capture: they may
   be stale, or missing, in the shadow. (The implementation puts back all of
   them, within the capture's limits; past those the capture is lost, which
   Restart covers.) BugAbandonLosesKeys drops them. *)
AbandonPass ==
    /\ swept # {}
    /\ dirty' = IF BugAbandonLosesKeys THEN dirty ELSE dirty \cup swept
    /\ swept' = {} /\ cleared' = FALSE /\ unsettled' = FALSE
    /\ UNCHANGED <<source, shadow, copied, indexes, cvars, wvars, qvars>>

Next == (\E m \in Mutations : StartWrite(m)) \/ FinishWrite(TRUE) \/ FinishWrite(FALSE)
        \/ InstallCapture \/ BeginCopy \/ (\E k \in Keys : ReadPage(k)) \/ WritePage
        \/ Fence \/ (\E k \in Keys : Tail(k)) \/ BuildIndexes \/ Prepare \/ Publish
        \/ Restart \/ DeliverOldPage
        \/ BeginPass \/ ClearPass \/ (\E k \in Keys : RereadPass(k)) \/ AbandonPass
Spec == Init /\ [][Next]_vars

TypeOK == /\ source \in [Keys -> Values] /\ shadow \in [Keys -> Values]
          /\ copied \subseteq Keys /\ dirty \subseteq Keys /\ indexes \in BOOLEAN
          /\ phase \in Phases /\ capture \in BOOLEAN /\ generation \in 1..2
          /\ restarted \in BOOLEAN /\ fenced \in BOOLEAN
          /\ pending \in Mutations \cup {NoWrite} /\ started \in 0..2
          /\ page \in [active : BOOLEAN, key : Keys, value : Values, gen : 0..2, dirty : BOOLEAN]
          /\ late \in [active : BOOLEAN, key : Keys, value : Values, gen : 0..2, dirty : BOOLEAN]
          /\ unconfirmedCopied \in BOOLEAN
          /\ swept \subseteq Keys /\ cleared \in BOOLEAN /\ unsettled \in BOOLEAN
ConfirmedCopies == ~unconfirmedCopied
PreparedImage == phase \in {"prepared", "published"} =>
                  /\ shadow = source /\ copied = Keys /\ indexes /\ fenced
=============================================================================
