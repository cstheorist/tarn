--------------------------- MODULE ClusterBackup ---------------------------
(* Quiescent cluster cut and quarantined restore. A storage fence rejects new
   persistent effects and survives crashes. WAL catch-up precedes certification.
   FileBackup separately models immutable file/hash publication. Neither model
   proves fsync, Tarantool replication, the controller or external isolation. *)
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS BugEarlyCopy, BugSkipDrain, BugMissingReplica, BugLostFence,
          BugPartialRestore, BugSkipMetadata
Nodes == 1..4
Group(n) == IF n <= 2 THEN 1 ELSE 2
VARIABLES fenced, durable, live, data, pending, begun, acknowledged,
          cut, copied, image, metadata, complete, restored, checked, serving,
          failed, restartUsed
vars == <<fenced, durable, live, data, pending, begun, acknowledged, cut, copied,
          image, metadata, complete, restored, checked, serving, failed, restartUsed>>
Init == /\ fenced = {} /\ durable = {} /\ live = Nodes
        /\ data = [n \in Nodes |-> 0] /\ pending = {} /\ begun = FALSE
        /\ acknowledged = FALSE /\ cut = FALSE /\ copied = {}
        /\ image = [n \in Nodes |-> 0] /\ metadata = {}
        /\ complete = FALSE /\ restored = {} /\ checked = FALSE
        /\ serving = FALSE /\ failed = FALSE /\ restartUsed = {}

(* An acknowledged distributed write can still await physical APPLY. *)
Decide == /\ ~begun /\ fenced = {} /\ ~complete
          /\ begun' = TRUE /\ pending' = Nodes /\ acknowledged' = TRUE
          /\ UNCHANGED <<fenced, durable, live, data, cut, copied, image, metadata,
                         complete, restored, checked, serving, failed, restartUsed>>
Apply(n) == /\ n \in pending \cap live /\ n \notin fenced
            /\ data' = [data EXCEPT ![n] = 1] /\ pending' = pending \ {n}
            /\ UNCHANGED <<fenced, durable, live, begun, acknowledged, cut, copied,
                           image, metadata, complete, restored, checked, serving, failed, restartUsed>>
Fence(n) == /\ n \in live \ fenced /\ ~complete
            /\ fenced' = fenced \cup {n} /\ durable' = durable \cup {n}
            /\ UNCHANGED <<live, data, pending, begun, acknowledged, cut, copied,
                           image, metadata, complete, restored, checked, serving, failed, restartUsed>>
Crash(n) == /\ n \in live /\ n \notin restartUsed /\ ~complete
            /\ live' = live \ {n}
            /\ UNCHANGED <<fenced, durable, data, pending, begun, acknowledged, cut, copied,
                           image, metadata, complete, restored, checked, serving, failed, restartUsed>>
Restart(n) == /\ n \notin live
              /\ live' = live \cup {n} /\ restartUsed' = restartUsed \cup {n}
              /\ fenced' = IF BugLostFence THEN fenced \ {n} ELSE fenced \cup (durable \cap {n})
              /\ UNCHANGED <<durable, data, pending, begun, acknowledged, cut, copied,
                             image, metadata, complete, restored, checked, serving, failed>>
Certify == /\ ~cut /\ ~failed /\ live = Nodes
           /\ (BugEarlyCopy \/ fenced = Nodes)
           /\ (BugSkipDrain \/ pending = {})
           /\ cut' = TRUE
           /\ UNCHANGED <<fenced, durable, live, data, pending, begun, acknowledged,
                          copied, image, metadata, complete, restored, checked, serving, failed, restartUsed>>
Copy(n) == /\ cut /\ ~complete /\ ~failed /\ n \in live \ copied
           /\ copied' = copied \cup {n} /\ image' = [image EXCEPT ![n] = data[n]]
           /\ metadata' = IF BugSkipMetadata THEN metadata ELSE metadata \cup {n}
           /\ UNCHANGED <<fenced, durable, live, data, pending, begun, acknowledged,
                          cut, complete, restored, checked, serving, failed, restartUsed>>
Publish == /\ cut /\ ~failed /\ ~complete
           /\ (IF BugMissingReplica THEN {1,3} \subseteq copied ELSE copied = Nodes)
           /\ complete' = TRUE
           /\ UNCHANGED <<fenced, durable, live, data, pending, begun, acknowledged,
                          cut, copied, image, metadata, restored, checked, serving, failed, restartUsed>>
KillController == /\ ~complete /\ ~failed /\ failed' = TRUE
                  /\ UNCHANGED <<fenced, durable, live, data, pending, begun, acknowledged,
                                 cut, copied, image, metadata, complete, restored, checked, serving, restartUsed>>
Restore(n) == /\ complete /\ n \in copied \ restored
              /\ restored' = restored \cup {n}
              /\ UNCHANGED <<fenced, durable, live, data, pending, begun, acknowledged, cut,
                             copied, image, metadata, complete, checked, serving, failed, restartUsed>>
Verify == /\ complete /\ ~checked
          /\ (BugPartialRestore \/ restored = Nodes)
          /\ checked' = TRUE
          /\ UNCHANGED <<fenced, durable, live, data, pending, begun, acknowledged, cut,
                         copied, image, metadata, complete, restored, serving, failed, restartUsed>>
Activate == /\ checked /\ ~serving /\ serving' = TRUE
            /\ UNCHANGED <<fenced, durable, live, data, pending, begun, acknowledged, cut,
                           copied, image, metadata, complete, restored, checked, failed, restartUsed>>
Next == Decide \/ Certify \/ Publish \/ KillController \/ Verify \/ Activate
        \/ \E n \in Nodes: Apply(n) \/ Fence(n) \/ Crash(n) \/ Restart(n) \/ Copy(n) \/ Restore(n)
Spec == Init /\ [][Next]_vars
TypeOK == /\ fenced \subseteq Nodes /\ durable \subseteq Nodes /\ live \subseteq Nodes
          /\ data \in [Nodes -> 0..1] /\ pending \subseteq Nodes /\ copied \subseteq Nodes
          /\ image \in [Nodes -> 0..1] /\ metadata \subseteq Nodes /\ restored \subseteq Nodes
          /\ restartUsed \subseteq Nodes
          /\ \A b \in {begun, acknowledged, cut, complete, checked, serving, failed}: b \in BOOLEAN
FrozenCut == cut => fenced = Nodes
DrainedCut == cut => pending = {}
CompleteSet == complete => copied = Nodes
NoLostAcknowledgement == complete /\ acknowledged => \A n \in copied: image[n] = 1
CoherentData == complete => \A a,b \in copied: image[a] = image[b]
MetadataRetained == complete => metadata = Nodes
NoPartialActivation == serving => restored = Nodes /\ checked /\ complete
=============================================================================
