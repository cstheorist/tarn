--------------------------- MODULE NativeKeyBatch ---------------------------
(* Bounded tarn_select_keys native refinement (#141), not a new transaction
   protocol. Requests A,B,A include a duplicate and an initially absent key.
   The native memtx body does not yield: its gets and final checks see one
   state. A blocked/unsupported/capped attempt publishes no partial success.
   Every requested full key, including a miss, is checked for prepared W
   claims. R range locks do not block these snapshot reads. Ready native
   state, routing/membership/DDL guards, confirmed CAPI reads and accurate
   mirrored locks are explicit premises supplied by the existing contracts.
   No stream, vinyl, ownership migration, CAPI or full serializability proof
   is claimed by this local result/handback refinement. *)
EXTENDS Naturals, Sequences, FiniteSets
CONSTANTS BugMissingClaim, BugMixedSnapshots, BugTruncatedComplete

Keys == {"a", "b"}
Requests == <<"a", "b", "a">>
Requested == {Requests[i] : i \in 1..Len(Requests)}
Old == [k \in Keys |-> IF k = "a" THEN 1 ELSE 0]
Inserted == [k \in Keys |-> IF k = "a" THEN 1 ELSE 2]
Changed == [k \in Keys |-> 2]
Images == {Old, Inserted, Changed}
RowBytes(k) == IF k = "a" THEN 2 ELSE 3
Caps == {[work |-> 3, rows |-> 3, bytes |-> 7],
         [work |-> 3, rows |-> 1, bytes |-> 2],
         [work |-> 2, rows |-> 3, bytes |-> 7]}
RECURSIVE FilterMap(_, _)
FilterMap(image, reqs) ==
    IF Len(reqs) = 0 THEN <<>>
    ELSE LET rest == FilterMap(image, Tail(reqs)) IN
         IF image[Head(reqs)] = 0 THEN rest
         ELSE << <<Head(reqs), image[Head(reqs)]>> >> \o rest

VARIABLES source, history, writer, target, claims,
          phase, pos, out, cap, work, bytes, mustSee, required, servedClaims
wvars == <<source, history, writer, target, claims>>
rvars == <<phase, pos, out, cap, work, bytes, mustSee, required, servedClaims>>
vars == <<wvars, rvars>>
MayWrite == phase # "reading" \/ BugMixedSnapshots

Init == /\ source = Old /\ history = {Old} /\ writer = "idle" /\ target = Old /\ claims = {}
        /\ phase = "idle" /\ pos = 1 /\ out = <<>> /\ cap \in Caps
        /\ work = 0 /\ bytes = 0 /\ mustSee = FALSE /\ required = Old /\ servedClaims = {}

PrepareWrite(image) ==
    /\ writer = "idle" /\ MayWrite /\ image \in {Inserted, Changed}
    /\ target' = image /\ claims' = IF image = Inserted THEN {"b"} ELSE Keys
    /\ writer' = "prepared" /\ UNCHANGED <<source, history, rvars>>
AckWrite == /\ writer = "prepared" /\ MayWrite /\ writer' = "acked"
            /\ UNCHANGED <<source, history, target, claims, rvars>>
ApplyWrite == /\ writer = "acked" /\ MayWrite
              /\ source' = target /\ history' = history \cup {target}
              /\ claims' = {} /\ writer' = "done" /\ UNCHANGED <<target, rvars>>
AbortWrite == /\ writer = "prepared" /\ MayWrite
              /\ claims' = {} /\ writer' = "done" /\ target' = source
              /\ UNCHANGED <<source, history, rvars>>

Start == /\ phase = "idle"
         /\ LET tested == IF BugMissingClaim THEN {k \in Requested : source[k] # 0} ELSE Requested
            IN phase' = IF Len(Requests) > cap.work \/ claims \cap tested # {}
                        THEN "fallback" ELSE "reading"
         /\ mustSee' = (writer = "acked" \/ writer = "done")
         /\ required' = IF writer = "acked" THEN target ELSE source
         /\ UNCHANGED <<wvars, pos, out, cap, work, bytes, servedClaims>>

Get ==
    /\ phase = "reading" /\ pos <= Len(Requests)
    /\ LET k == Requests[pos]
           hit == source[k] # 0
           size == IF hit THEN RowBytes(k) ELSE 0
           fits == work < cap.work /\ Len(out) < cap.rows /\ bytes < cap.bytes
                   /\ bytes + size <= cap.bytes
       IN IF fits
          THEN /\ out' = IF hit THEN Append(out, <<k, source[k]>>) ELSE out
               /\ pos' = pos + 1 /\ work' = work + 1 /\ bytes' = bytes + size
               /\ UNCHANGED <<phase, servedClaims>>
          ELSE /\ phase' = IF BugTruncatedComplete THEN "done" ELSE "fallback"
               /\ servedClaims' = claims /\ UNCHANGED <<pos, out, work, bytes>>
    /\ UNCHANGED <<wvars, cap, mustSee, required>>

Finish ==
    /\ phase = "reading" /\ pos = Len(Requests) + 1
    /\ LET tested == IF BugMissingClaim THEN {out[i][1] : i \in 1..Len(out)} ELSE Requested
       IN phase' = IF claims \cap tested = {} THEN "done" ELSE "fallback"
    /\ servedClaims' = claims
    /\ UNCHANGED <<wvars, pos, out, cap, work, bytes, mustSee, required>>

Next == (\E image \in {Inserted, Changed} : PrepareWrite(image))
        \/ AckWrite \/ ApplyWrite \/ AbortWrite \/ Start \/ Get \/ Finish
Spec == Init /\ [][Next]_vars
TypeOK == /\ source \in Images /\ history \subseteq Images /\ target \in Images
          /\ writer \in {"idle", "prepared", "acked", "done"} /\ claims \subseteq Keys
          /\ phase \in {"idle", "reading", "done", "fallback"}
          /\ pos \in 1..4 /\ out \in Seq(Keys \X {1, 2}) /\ Len(out) <= 3
          /\ cap \in Caps /\ work \in 0..3 /\ bytes \in 0..7
          /\ mustSee \in BOOLEAN /\ required \in Images /\ servedClaims \subseteq Keys
FullBatch == phase = "done" => pos = Len(Requests) + 1
OneSnapshot == phase = "done" => out \in {FilterMap(image, Requests) : image \in history}
NoMissingClaim == phase = "done" => servedClaims \cap Requested = {}
AckVisible == phase = "done" /\ mustSee => out = FilterMap(required, Requests)
=============================================================================
