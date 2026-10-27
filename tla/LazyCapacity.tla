--------------------------- MODULE LazyCapacity ----------------------------
(* PREPARE reserves entries, worst encoded bytes AND remaining sequence
   numbers without allocating a delivery sequence. APPLY converts that
   reservation atomically with its row/outbox. Two transactions contend
   with one local commit; source handoff is outside this resource model. *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugNoEntryReserve, BugNoByteReserve, BugNoSeqReserve, BugAbortLeaks, EntryLimit, ByteLimit, SeqLimit
Txs == {"p", "q"}
NeedE(t) == 1
NeedB(t) == IF t = "p" THEN 2 ELSE 1
CapE == EntryLimit
CapB == ByteLimit
CapS == SeqLimit
VARIABLES phase, pending, bytes, counter, localDone, leaked
vars == <<phase,pending,bytes,counter,localDone,leaked>>
SumE(S) == Cardinality(S)
SumB(S) == (IF "p" \in S THEN 2 ELSE 0) + (IF "q" \in S THEN 1 ELSE 0)
Reserved == {t \in Txs : phase[t] = "yes"} \cup leaked
Free(e,b,s) == /\ Cardinality(pending) + (IF BugNoEntryReserve THEN 0 ELSE SumE(Reserved)) + e <= CapE
               /\ bytes + (IF BugNoByteReserve THEN 0 ELSE SumB(Reserved)) + b <= CapB
               /\ counter + (IF BugNoSeqReserve THEN 0 ELSE SumE(Reserved)) + s <= CapS
Init == /\ phase = [t \in Txs |-> "idle"] /\ pending = {} /\ bytes = 0
        /\ counter = 0 /\ localDone = FALSE /\ leaked = {}
Prepare(t) ==
    /\ phase[t] = "idle" /\ Free(NeedE(t),NeedB(t),1)
    /\ phase' = [phase EXCEPT ![t] = "yes"]
    /\ UNCHANGED <<pending,bytes,counter,localDone,leaked>>
Abort(t) ==
    /\ phase[t] = "yes" /\ phase' = [phase EXCEPT ![t] = "abort"]
    /\ leaked' = IF BugAbortLeaks THEN leaked \cup {t} ELSE leaked
    /\ UNCHANGED <<pending,bytes,counter,localDone>>
Apply(t) ==
    /\ phase[t] = "yes"
    /\ phase' = [phase EXCEPT ![t] = "apply"]
    /\ pending' = pending \cup {t} /\ bytes' = bytes + NeedB(t) /\ counter' = counter+1
    /\ UNCHANGED <<localDone,leaked>>
Local ==
    /\ ~localDone /\ Free(1,1,1)
    /\ localDone' = TRUE /\ pending' = pending \cup {"local"}
    /\ bytes' = bytes+1 /\ counter' = counter+1
    /\ UNCHANGED <<phase,leaked>>
Ack(t) ==
    /\ t \in pending /\ pending' = pending \ {t}
    /\ bytes' = bytes - (IF t = "local" THEN 1 ELSE NeedB(t))
    /\ UNCHANGED <<phase,counter,localDone,leaked>>
Next == Local \/ (\E t \in Txs : Prepare(t) \/ Abort(t) \/ Apply(t))
        \/ (\E t \in Txs \cup {"local"} : Ack(t))
Spec == Init /\ [][Next]_vars
TypeOK == /\ phase \in [Txs -> {"idle","yes","abort","apply"}]
          /\ pending \subseteq Txs \cup {"local"} /\ bytes \in 0..5 /\ counter \in 0..3
          /\ localDone \in BOOLEAN /\ leaked \subseteq Txs
EntryPromisesFit == Cardinality(pending)+SumE(Reserved) <= CapE
BytePromisesFit == bytes+SumB(Reserved) <= CapB
SequencePromisesFit == counter+SumE(Reserved) <= CapS
AbortedReleased == \A t \in Txs : phase[t] = "abort" => t \notin Reserved
=============================================================================
