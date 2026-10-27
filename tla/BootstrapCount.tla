-------------------------- MODULE BootstrapCount --------------------------
(* Storage reports and the coordinator's later map initialization are
   separate steps. Internal lazy transport metadata must not manufacture
   a legacy user-table report on a fresh cluster. Existing map reports are
   authoritative; explicit configuration overrides only the default. *)
EXTENDS Naturals, FiniteSets
CONSTANT BugCountsAuxMetadata
Shards == {"A", "B"}
Counts == {0, 512}
Configured == {0, 64, 4096}
LegacyCount == 3000
FreshCount == 30000
(* This is an already computed routing hash, not a CRC implementation.
   The stored legacy row has bucket 1002; using 30000 would route to 4002. *)
RowHash == 4001
Bucket(h, n) == (h % n) + 1
VARIABLES users, aux, legacyRows, configured, maps, reports, reported, chosen
vars == <<users, aux, legacyRows, configured, maps, reports, reported, chosen>>
Init == /\ users \in SUBSET Shards /\ aux \in SUBSET Shards
        /\ legacyRows \in SUBSET users
        /\ configured \in Configured /\ maps \in [Shards -> Counts]
        /\ reports = [s \in Shards |-> [count |-> 0, tables |-> FALSE]]
        /\ reported = {} /\ chosen = 0
Report(s) ==
    /\ s \in Shards \ reported /\ chosen = 0
    /\ reports' = [reports EXCEPT ![s] =
        [count |-> maps[s],
         tables |-> s \in users \/ (BugCountsAuxMetadata /\ s \in aux)]]
    /\ reported' = reported \cup {s}
    /\ UNCHANGED <<users, aux, legacyRows, configured, maps, chosen>>
ChooseMap ==
    /\ reported = Shards /\ chosen = 0
    /\ LET mapped == {s \in Shards : reports[s].count > 0}
       IN chosen' = IF mapped # {}
                     THEN reports[CHOOSE s \in mapped : TRUE].count
                     ELSE IF configured > 0 THEN configured
                     ELSE IF \E s \in Shards : reports[s].tables
                          THEN LegacyCount ELSE FreshCount
    /\ UNCHANGED <<users, aux, legacyRows, configured, maps, reports, reported>>
Next == (\E s \in Shards : Report(s)) \/ ChooseMap
Spec == Init /\ [][Next]_vars
TypeOK == /\ users \subseteq Shards /\ aux \subseteq Shards
          /\ legacyRows \subseteq users /\ configured \in Configured
          /\ maps \in [Shards -> Counts]
          /\ reports \in [Shards -> [count : Counts, tables : BOOLEAN]]
          /\ reported \subseteq Shards
          /\ chosen \in {0, 64, 512, 4096, LegacyCount, FreshCount}
NoMap == \A s \in Shards : maps[s] = 0
MapPrecedence == chosen = 0 \/ NoMap \/ chosen = 512
ConfiguredPrecedence ==
    (chosen > 0 /\ NoMap /\ configured > 0) => chosen = configured
LegacyRoutingPreserved ==
    (chosen > 0 /\ NoMap /\ configured = 0 /\ users # {}) =>
        /\ chosen = LegacyCount
        /\ \A s \in legacyRows : Bucket(RowHash, chosen) = Bucket(RowHash, LegacyCount)
NewClusterDefault ==
    (chosen > 0 /\ NoMap /\ configured = 0 /\ users = {}) => chosen = FreshCount
=============================================================================
