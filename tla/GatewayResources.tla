------------------------- MODULE GatewayResources -------------------------
EXTENDS Naturals, FiniteSets
CONSTANTS BugEarlyRelease, BugEvictPinned, BugReserve, BugResponseRelease
Sockets == {"a", "b", "c"}
Buffers == {"x", "y"}
VARIABLES state, charged, io, pinned, pending, evicted, badReserve,
          refs, reading, responseCharge
vars == <<state, charged, io, pinned, pending, evicted, badReserve,
          refs, reading, responseCharge>>
Init == /\ state = [s \in Sockets |-> "new"] /\ charged = {}
        /\ io = [s \in Sockets |-> 0] /\ pinned = {} /\ pending = {}
        /\ evicted = {} /\ badReserve = FALSE
        /\ refs = [b \in Buffers |-> 0] /\ reading = {} /\ responseCharge = {}
Connect(s, normal) ==
    /\ state[s] = "new"
    /\ Cardinality(charged) < IF normal /\ ~BugReserve THEN 1 ELSE 2
    /\ state' = [state EXCEPT ![s] = "connecting"]
    /\ charged' = charged \cup {s} /\ io' = [io EXCEPT ![s] = 2]
    /\ badReserve' = (badReserve \/ (normal /\ Cardinality(charged) >= 1))
    /\ UNCHANGED <<pinned, pending, evicted, refs, reading, responseCharge>>
Ready(s) == /\ state[s] = "connecting" /\ state' = [state EXCEPT ![s] = "pooled"]
            /\ UNCHANGED <<charged, io, pinned, pending, evicted, badReserve, refs, reading, responseCharge>>
Pin(s) == /\ state[s] = "pooled" /\ s \notin pinned /\ pinned' = pinned \cup {s}
          /\ UNCHANGED <<state, charged, io, pending, evicted, badReserve, refs, reading, responseCharge>>
Unpin(s) == /\ s \in pinned /\ pinned' = pinned \ {s}
            /\ UNCHANGED <<state, charged, io, pending, evicted, badReserve, refs, reading, responseCharge>>
Send(s) == /\ state[s] = "pooled" /\ s \in pinned /\ s \notin pending
           /\ pending' = pending \cup {s}
           /\ UNCHANGED <<state, charged, io, pinned, evicted, badReserve, refs, reading, responseCharge>>
Final(s) == /\ s \in pending /\ pending' = pending \ {s}
            /\ UNCHANGED <<state, charged, io, pinned, evicted, badReserve, refs, reading, responseCharge>>
Close(s) == /\ state[s] \in {"connecting", "pooled"}
            /\ state' = [state EXCEPT ![s] = "closing"]
            /\ charged' = IF BugEarlyRelease THEN charged \ {s} ELSE charged
            /\ UNCHANGED <<io, pinned, pending, badReserve, refs, reading, responseCharge>>
Evict(s) == /\ state[s] = "pooled"
            /\ BugEvictPinned \/ (s \notin pinned /\ s \notin pending)
            /\ Close(s) /\ evicted' = evicted \cup {s}
DropIO(s) == /\ state[s] = "closing" /\ io[s] > 0
             /\ io' = [io EXCEPT ![s] = @ - 1]
             /\ charged' = IF io[s] = 1 THEN charged \ {s} ELSE charged
             /\ state' = [state EXCEPT ![s] = IF io[s] = 1 THEN "closed" ELSE @]
             /\ pending' = IF io[s] = 1 THEN pending \ {s} ELSE pending
             /\ UNCHANGED <<pinned, evicted, badReserve, refs, reading, responseCharge>>
Read(b) == /\ refs[b] = 0 /\ b \notin reading /\ Cardinality(responseCharge) < 1
           /\ reading' = reading \cup {b} /\ responseCharge' = responseCharge \cup {b}
           /\ UNCHANGED <<state, charged, io, pinned, pending, evicted, badReserve, refs>>
Reply(b) == /\ b \in reading /\ reading' = reading \ {b}
            /\ refs' = [refs EXCEPT ![b] = 2]
            /\ UNCHANGED <<state, charged, io, pinned, pending, evicted, badReserve, responseCharge>>
DropSlice(b) == /\ refs[b] > 0 /\ refs' = [refs EXCEPT ![b] = @ - 1]
                /\ responseCharge' = IF refs[b] = 1 \/ BugResponseRelease THEN responseCharge \ {b} ELSE responseCharge
                /\ UNCHANGED <<state, charged, io, pinned, pending, evicted, badReserve, reading>>
Next == (\E s \in Sockets: Connect(s, TRUE) \/ Connect(s, FALSE) \/ Ready(s)
         \/ Pin(s) \/ Unpin(s) \/ Send(s) \/ Final(s) \/ (Close(s) /\ UNCHANGED evicted) \/ Evict(s) \/ DropIO(s))
        \/ (\E b \in Buffers: Read(b) \/ Reply(b) \/ DropSlice(b))
Spec == Init /\ [][Next]_vars
TypeOK == /\ state \in [Sockets -> {"new", "connecting", "pooled", "closing", "closed"}]
          /\ charged \subseteq Sockets /\ io \in [Sockets -> 0..2]
          /\ pinned \subseteq Sockets /\ pending \subseteq Sockets /\ evicted \subseteq Sockets
          /\ badReserve \in BOOLEAN /\ refs \in [Buffers -> 0..2]
          /\ reading \subseteq Buffers /\ responseCharge \subseteq Buffers
SocketBound == Cardinality({s \in Sockets: io[s] > 0}) <= 2
SocketCharged == \A s \in Sockets: io[s] > 0 => s \in charged
CompletionReserve == ~badReserve
EvictionSafe == evicted \cap (pinned \cup pending) = {}
ResponseOwned == \A b \in Buffers: (refs[b] > 0 \/ b \in reading) => b \in responseCharge
ResponseBound == Cardinality({b \in Buffers: refs[b] > 0 \/ b \in reading}) <= 1
=============================================================================
