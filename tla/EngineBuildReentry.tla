------------------------- MODULE EngineBuildReentry -------------------------
(* One original BUILD at a successful-return/pre-effect
 * checkpoint. Its saved baseline page predates a known cut invalidation.
 * The invalidation may also have lost capture. No exception, submitted
 * transaction, closure or RPC is caught/retried by this contract.
 *
 * The source may change while capture is absent; a full incomplete recopy
 * is then necessary. Q-only loss retains capture/dirty keys and does not
 * force that recopy. Current terminal-ticket AND native-queue evidence
 * precedes reset; old submitted shadow effects may outlive their caller.
 * Their reachability/terminal mapping is an external compositional input,
 * not a claim that the observed CI BUILD had such an old effect.
 *
 * The q-only case includes a cut already rebuilt in a newer epoch before
 * the checkpoint: READY alone does not preserve the old page. The
 * uncertified preparing case has the invalid track set by E.prepare and
 * must recopy, rather than being rejected or treated as certified.
 *
 * Generic transaction checks of current owner, cut epoch and capture lease
 * remain mandatory. The saved page's old payload is not made current by
 * relabeling its lease. A certified receipt is handled before any reset.
 * The final tail/source-cut and confirmed shadow effects retain the parent
 * EngineCopy/EngineSourceDrain premises. One finite invalidation and fair
 * native completion give abstract completion, not a wall-clock deadline or
 * progress through arbitrary mid-helper exceptions/uncertain commits.
 *)
EXTENDS Naturals, FiniteSets
CONSTANTS Profile, BugReusePage, BugCallerOnlyCut, BugKeepPrefix
Keys == {"A", "B"}
Modes == IF Profile = "all" THEN {"cut", "capture", "certified"} ELSE {Profile}
Certified == {"prepared", "applying", "published"}
Empty == [k \in Keys |-> 0]
Original == [k \in Keys |-> 1]
NoPage == [active |-> FALSE, key |-> "B", value |-> 0,
           sampledCapture |-> 0, lease |-> 0, cut |-> 0, owner |-> 0]
RequestOwner == 1

VARIABLES mode, phase, owner, receiptPhase, receiptCapture,
          source, shadow, copied, dirty, captureEpoch, captureValid,
          cutReady, cutEpoch, resetCount, page, preparedBy,
          oldPending, oldLocal, oldCaller, oldTicket, oldQueue, oldKey,
          gapDone
vars == <<mode,phase,owner,receiptPhase,receiptCapture,
          source,shadow,copied,dirty,captureEpoch,captureValid,
          cutReady,cutEpoch,resetCount,page,preparedBy,
          oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

Init ==
    /\ mode \in Modes /\ phase = "sampled" /\ owner = RequestOwner
    /\ receiptPhase \in IF mode = "certified" THEN Certified
                         ELSE IF mode = "capture" THEN {"copying", "preparing"} ELSE {"copying"}
    /\ receiptCapture = 1 /\ source = Original
    /\ shadow = IF mode = "certified" THEN Original ELSE [k \in Keys |-> IF k = "A" THEN 1 ELSE 0]
    /\ copied = IF mode = "certified" THEN Keys ELSE {"A"}
    /\ dirty = {} /\ captureEpoch = IF mode = "cut" THEN 1 ELSE 2
    /\ captureValid = (mode = "cut")
    /\ cutReady \in IF mode = "cut" THEN BOOLEAN ELSE {FALSE}
    /\ cutEpoch = IF cutReady THEN 2 ELSE 0
    /\ resetCount = 0 /\ preparedBy = 0
    /\ page = IF mode = "certified" THEN NoPage ELSE
                 [active |-> TRUE, key |-> "B", value |-> 1,
                  sampledCapture |-> 1, lease |-> 1, cut |-> 1, owner |-> RequestOwner]
    /\ oldPending \in IF mode = "certified" \/ cutReady THEN {FALSE} ELSE BOOLEAN
    /\ oldLocal \in IF oldPending THEN BOOLEAN ELSE {FALSE}
    /\ oldCaller = (oldPending /\ oldLocal) /\ oldTicket = (oldPending /\ oldLocal)
    /\ oldQueue = oldPending /\ oldKey \in Keys /\ gapDone = FALSE

RequestReentry ==
    /\ phase = "sampled" /\ phase' = "waiting"
    /\ page' = IF BugReusePage THEN page ELSE NoPage
    /\ UNCHANGED <<mode,owner,receiptPhase,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

AcquireCurrentCut ==
    /\ phase = "waiting"
    /\ ((~oldTicket /\ ~oldQueue) \/ (BugCallerOnlyCut /\ ~oldCaller))
    /\ cutReady' = TRUE /\ cutEpoch' = 2 /\ phase' = "owned"
    /\ UNCHANGED <<mode,owner,receiptPhase,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,resetCount,page,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

GenericChecks == cutReady /\ cutEpoch = 2 /\ owner = RequestOwner

RereadOwned ==
    /\ phase = "owned" /\ GenericChecks
    /\ IF receiptPhase \in Certified
       THEN /\ phase' = "prepared" /\ preparedBy' = RequestOwner
            /\ UNCHANGED <<receiptPhase,receiptCapture,shadow,copied,dirty,captureValid,resetCount,page>>
       ELSE /\ phase' = "copying" /\ preparedBy' = preparedBy
            /\ LET recopy == ~captureValid \/ receiptCapture # captureEpoch
               IN /\ receiptCapture' = captureEpoch /\ captureValid' = TRUE
                  /\ resetCount' = IF recopy THEN resetCount + 1 ELSE resetCount
                  /\ shadow' = IF recopy /\ ~BugKeepPrefix THEN Empty ELSE shadow
                  /\ copied' = IF recopy /\ ~BugKeepPrefix THEN {} ELSE copied
                  /\ dirty' = IF recopy THEN {} ELSE dirty
            /\ receiptPhase' = "copying"
            (* The unsafe control revalidates/relabels the lease but keeps
             * a payload sampled before lost capture. Final checks still run. *)
            /\ page' = IF BugReusePage /\ page.active
                        THEN [page EXCEPT !.lease = captureEpoch, !.cut = 2, !.owner = owner]
                        ELSE NoPage
    /\ UNCHANGED <<mode,owner,source,captureEpoch,cutReady,cutEpoch,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

GapWrite(k) ==
    /\ mode # "certified" /\ ~gapDone /\ phase \in {"waiting", "owned"} /\ k \in Keys
    /\ source' = [source EXCEPT ![k] = 2] /\ gapDone' = TRUE
    /\ dirty' = IF captureValid THEN dirty \cup {k} ELSE dirty
    /\ UNCHANGED <<mode,phase,owner,receiptPhase,receiptCapture,shadow,copied,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey>>

ReadPage ==
    /\ phase = "copying" /\ ~page.active /\ copied # Keys /\ GenericChecks
    /\ LET k == IF "A" \notin copied THEN "A" ELSE "B"
       IN page' = [active |-> TRUE, key |-> k, value |-> source[k],
                   sampledCapture |-> captureEpoch, lease |-> captureEpoch,
                   cut |-> cutEpoch, owner |-> owner]
    /\ UNCHANGED <<mode,phase,owner,receiptPhase,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

WritePage ==
    /\ phase = "copying" /\ page.active /\ GenericChecks
    /\ captureValid /\ receiptCapture = captureEpoch
    /\ page.lease = captureEpoch /\ page.cut = cutEpoch /\ page.owner = owner
    /\ shadow' = [shadow EXCEPT ![page.key] = page.value]
    /\ copied' = copied \cup {page.key} /\ page' = NoPage
    /\ UNCHANGED <<mode,phase,owner,receiptPhase,receiptCapture,source,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

BeginTail ==
    /\ phase = "copying" /\ copied = Keys /\ ~page.active /\ GenericChecks
    /\ ~oldTicket /\ ~oldQueue /\ phase' = "tail"
    /\ UNCHANGED <<mode,owner,receiptPhase,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

Tail(k) ==
    /\ phase = "tail" /\ k \in dirty /\ GenericChecks
    /\ shadow' = [shadow EXCEPT ![k] = source[k]] /\ dirty' = dirty \ {k}
    /\ UNCHANGED <<mode,phase,owner,receiptPhase,receiptCapture,source,copied,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

Prepare ==
    /\ phase = "tail" /\ dirty = {} /\ GenericChecks
    /\ captureValid /\ receiptCapture = captureEpoch /\ ~oldTicket /\ ~oldQueue
    /\ phase' = "prepared" /\ receiptPhase' = "prepared" /\ preparedBy' = owner
    /\ UNCHANGED <<mode,owner,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

Takeover ==
    /\ owner = 1 /\ phase \notin {"prepared", "refused"} /\ owner' = 2
    /\ UNCHANGED <<mode,phase,receiptPhase,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>
RejectOwner ==
    /\ owner # RequestOwner /\ phase \notin {"prepared", "refused"} /\ phase' = "refused"
    /\ UNCHANGED <<mode,owner,receiptPhase,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldQueue,oldKey,gapDone>>

DetachOld ==
    /\ oldCaller /\ oldCaller' = FALSE
    /\ UNCHANGED <<mode,phase,owner,receiptPhase,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,preparedBy,
                   oldPending,oldLocal,oldTicket,oldQueue,oldKey,gapDone>>
ResolveOld(commit) ==
    /\ oldPending /\ commit \in BOOLEAN
    /\ oldPending' = FALSE /\ oldCaller' = FALSE
    /\ shadow' = IF commit THEN [shadow EXCEPT ![oldKey] = 1] ELSE shadow
    /\ UNCHANGED <<mode,phase,owner,receiptPhase,receiptCapture,source,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,preparedBy,
                   oldLocal,oldTicket,oldQueue,oldKey,gapDone>>
OldCallback ==
    /\ ~oldPending /\ oldTicket /\ oldTicket' = FALSE
    /\ UNCHANGED <<mode,phase,owner,receiptPhase,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,preparedBy,
                   oldPending,oldLocal,oldCaller,oldQueue,oldKey,gapDone>>
QueueSettles ==
    /\ ~oldPending /\ oldQueue /\ oldQueue' = FALSE
    /\ UNCHANGED <<mode,phase,owner,receiptPhase,receiptCapture,source,shadow,copied,dirty,
                   captureEpoch,captureValid,cutReady,cutEpoch,resetCount,page,preparedBy,
                   oldPending,oldLocal,oldCaller,oldTicket,oldKey,gapDone>>

ResolveAny == \E commit \in BOOLEAN : ResolveOld(commit)
TailAny == \E k \in Keys : Tail(k)
Next == RequestReentry \/ AcquireCurrentCut \/ RereadOwned \/ (\E k \in Keys : GapWrite(k))
        \/ ReadPage \/ WritePage \/ BeginTail \/ TailAny \/ Prepare \/ Takeover \/ RejectOwner
        \/ DetachOld \/ ResolveAny \/ OldCallback \/ QueueSettles
Spec == Init /\ [][Next]_vars
        /\ WF_vars(RequestReentry) /\ WF_vars(AcquireCurrentCut) /\ WF_vars(RereadOwned)
        /\ WF_vars(ReadPage) /\ WF_vars(WritePage) /\ WF_vars(BeginTail) /\ WF_vars(TailAny)
        /\ WF_vars(Prepare) /\ WF_vars(RejectOwner)
        /\ WF_vars(ResolveAny) /\ WF_vars(OldCallback) /\ WF_vars(QueueSettles)

TypeOK ==
    /\ mode \in {"cut", "capture", "certified"}
    /\ phase \in {"sampled", "waiting", "owned", "copying", "tail", "prepared", "refused"}
    /\ owner \in {1,2} /\ receiptPhase \in Certified \cup {"copying", "preparing"} /\ receiptCapture \in {1,2}
    /\ source \in [Keys -> {1,2}] /\ shadow \in [Keys -> 0..2]
    /\ copied \subseteq Keys /\ dirty \subseteq Keys /\ captureEpoch \in {1,2}
    /\ captureValid \in BOOLEAN /\ cutReady \in BOOLEAN /\ cutEpoch \in {0,2}
    /\ resetCount \in {0,1} /\ preparedBy \in {0,1}
    /\ page \in [active : BOOLEAN, key : Keys, value : 0..2,
                  sampledCapture : 0..2, lease : 0..2, cut : 0..2, owner : 0..2]
    /\ oldPending \in BOOLEAN /\ oldLocal \in BOOLEAN /\ oldCaller \in BOOLEAN
    /\ oldTicket \in BOOLEAN /\ oldQueue \in BOOLEAN /\ oldKey \in Keys /\ gapDone \in BOOLEAN
    /\ (oldPending => oldTicket \/ oldQueue)
PreparedImage == phase = "prepared" => shadow = source /\ copied = Keys
OwnerCertified == phase = "prepared" => owner = preparedBy /\ preparedBy = RequestOwner
CertifiedRetained == mode = "certified" => shadow = Original /\ resetCount = 0
CutOnlyNoRecopy == mode = "cut" => resetCount = 0
OriginalBuildCompletes == <> (phase \in {"prepared", "refused"})
=============================================================================
