--------------------------- MODULE FileBackup ---------------------------
(* Agent publication and seeded-identity contract, not a Tarantool proof. *)
(* backup.start supplies an immutable coherent checkpoint/WAL file set.   *)
(* Filesystem fsync/hash/header behavior and WAL follow remain trusted.   *)
EXTENDS Naturals, FiniteSets
CONSTANTS BugPublishPartial, BugSkipHash, BugReuseIdentity, BugIgnoreLease
Files == {"snapshot", "run"}
VARIABLES lease, available, copied, sound, complete, restored, restoreComplete,
          registered, identity, following, writable, failed, restoredSound, publishedHeld
vars == <<lease, available, copied, sound, complete, restored, restoreComplete,
          registered, identity, following, writable, failed, restoredSound, publishedHeld>>
Init == /\ lease = "none" /\ available = Files /\ copied = {} /\ sound = {}
        /\ complete = FALSE /\ restored = {} /\ restoreComplete = FALSE
        /\ registered = FALSE /\ identity = "none" /\ following = FALSE
        /\ writable = FALSE /\ failed = FALSE /\ restoredSound = {} /\ publishedHeld = FALSE
Register == /\ ~registered /\ registered' = TRUE
            /\ UNCHANGED <<lease, available, copied, sound, complete, restored,
                           restoreComplete, identity, following, writable, failed, restoredSound, publishedHeld>>
Start == /\ lease = "none" /\ lease' = "held"
         /\ UNCHANGED <<available, copied, sound, complete, restored, restoreComplete,
                        registered, identity, following, writable, failed, restoredSound, publishedHeld>>
Expire == /\ lease = "held" /\ lease' = "expired"
          /\ UNCHANGED <<available, copied, sound, complete, restored, restoreComplete,
                         registered, identity, following, writable, failed, restoredSound, publishedHeld>>
Collect(f) == /\ lease # "held" /\ f \in available /\ available' = available \ {f}
              /\ UNCHANGED <<lease, copied, sound, complete, restored, restoreComplete,
                             registered, identity, following, writable, failed, restoredSound, publishedHeld>>
Copy(f) == /\ lease = "held"
           /\ ~complete /\ ~failed /\ f \notin copied
           /\ copied' = copied \cup {f}
           /\ sound' = IF f \in available THEN sound \cup {f} ELSE sound
           /\ UNCHANGED <<lease, available, complete, restored, restoreComplete,
                          registered, identity, following, writable, failed, restoredSound, publishedHeld>>
Publish == /\ ~complete /\ ~failed
           /\ (BugPublishPartial \/ copied = Files)
           /\ (BugIgnoreLease \/ lease = "held")
           /\ complete' = TRUE /\ lease' = "released" /\ publishedHeld' = (lease = "held")
           /\ UNCHANGED <<available, copied, sound, restored, restoreComplete,
                          registered, identity, following, writable, failed, restoredSound>>
KillCopier == /\ ~complete /\ failed' = TRUE
              /\ UNCHANGED <<lease, available, copied, sound, complete, restored,
                             restoreComplete, registered, identity, following, writable, restoredSound, publishedHeld>>
Corrupt(f) == /\ complete /\ ~restoreComplete /\ f \in sound /\ sound' = sound \ {f}
              /\ UNCHANGED <<lease, available, copied, complete, restored, restoreComplete,
                             registered, identity, following, writable, failed, restoredSound, publishedHeld>>
Restore(f) == /\ complete /\ f \in copied /\ f \notin restored
              /\ (BugSkipHash \/ f \in sound)
              /\ restored' = restored \cup {f}
              /\ restoredSound' = IF f \in sound THEN restoredSound \cup {f} ELSE restoredSound
              /\ UNCHANGED <<lease, available, copied, sound, complete, restoreComplete,
                             registered, identity, following, writable, failed, publishedHeld>>
FinishRestore == /\ complete /\ restored = Files /\ ~restoreComplete
                 /\ (BugReuseIdentity \/ registered)
                 /\ restoreComplete' = TRUE
                 /\ identity' = IF BugReuseIdentity THEN "source" ELSE "seed"
                 /\ UNCHANGED <<lease, available, copied, sound, complete, restored,
                                registered, following, writable, failed, restoredSound, publishedHeld>>
Follow == /\ restoreComplete /\ ~following /\ following' = TRUE
          /\ UNCHANGED <<lease, available, copied, sound, complete, restored,
                         restoreComplete, registered, identity, writable, failed, restoredSound, publishedHeld>>
Promote == /\ following /\ writable' = TRUE
           /\ UNCHANGED <<lease, available, copied, sound, complete, restored,
                          restoreComplete, registered, identity, following, failed, restoredSound, publishedHeld>>
Next == Register \/ Start \/ Expire \/ Publish \/ KillCopier \/ FinishRestore \/ Follow \/ Promote
        \/ \E f \in Files: Collect(f) \/ Copy(f) \/ Corrupt(f) \/ Restore(f)
Spec == Init /\ [][Next]_vars
TypeOK == /\ lease \in {"none", "held", "expired", "released"}
          /\ available \subseteq Files /\ copied \subseteq Files /\ sound \subseteq Files
          /\ restored \subseteq Files /\ restoredSound \subseteq Files /\ publishedHeld \in BOOLEAN /\ identity \in {"none", "source", "seed"}
          /\ complete \in BOOLEAN /\ restoreComplete \in BOOLEAN /\ registered \in BOOLEAN
          /\ following \in BOOLEAN /\ writable \in BOOLEAN /\ failed \in BOOLEAN
PublishedComplete == complete => copied = Files
SeedDistinct == restoreComplete => registered /\ identity = "seed"
RestoredChecked == restoreComplete => restoredSound = Files
PublicationLease == complete => publishedHeld
PromotionAfterFollow == writable => following /\ restoreComplete
=============================================================================
