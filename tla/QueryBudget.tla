---------------------------- MODULE QueryBudget -----------------------------
EXTENDS Naturals, FiniteSets, TLC
CONSTANTS Clients, PerQuery, Global, BugEarlyWireRelease,
          BugEarlyResultRelease, BugPostDecisionRefusal
VARIABLES phase, charged, wire, rows, staged, effect, failed, validated
vars == <<phase,charged,wire,rows,staged,effect,failed,validated>>
Total == Cardinality(UNION {{<<c,n>> : n \in 1..charged[c]} : c \in Clients})
Fits(c,n) == charged[c]+n <= PerQuery /\ Total+n <= Global
Init == /\ phase=[c \in Clients |-> "idle"]
        /\ charged=[c \in Clients |-> 0]
        /\ wire=[c \in Clients |-> FALSE]
        /\ rows=[c \in Clients |-> 0]
        /\ staged=[c \in Clients |-> FALSE]
        /\ effect=[c \in Clients |-> 0]
        /\ failed=[c \in Clients |-> FALSE]
        /\ validated=[c \in Clients |-> FALSE]
Start(c) == /\ phase[c]="idle" /\ Fits(c,1)
            /\ phase'=[phase EXCEPT ![c]="read"]
            /\ charged'=[charged EXCEPT ![c]=1]
            /\ wire'=[wire EXCEPT ![c]=TRUE]
            /\ UNCHANGED <<rows,staged,effect,failed,validated>>
Read(c) == /\ phase[c]="read" /\ Fits(c,1)
           /\ phase'=[phase EXCEPT ![c]="active"]
           /\ wire'=[wire EXCEPT ![c]=FALSE]
           /\ rows'=[rows EXCEPT ![c]=1]
           /\ charged'=[charged EXCEPT ![c]=2]
           /\ UNCHANGED <<staged,effect,failed,validated>>
Stage(c) == /\ phase[c]="active" /\ ~staged[c] /\ Fits(c,1)
            /\ staged'=[staged EXCEPT ![c]=TRUE]
            /\ charged'=[charged EXCEPT ![c]=@+1]
            /\ UNCHANGED <<phase,wire,rows,effect,failed,validated>>
Refuse(c) == /\ (phase[c]="active" \/ (BugPostDecisionRefusal /\ phase[c]="decided"))
             /\ ~Fits(c,2)
             /\ phase'=[phase EXCEPT ![c]="done"]
             /\ failed'=[failed EXCEPT ![c]=TRUE]
             /\ charged'=[charged EXCEPT ![c]=0]
             /\ rows'=[rows EXCEPT ![c]=0]
             /\ staged'=[staged EXCEPT ![c]=FALSE]
             /\ UNCHANGED <<wire,effect,validated>>
Decide(c) == /\ phase[c]="active"
             /\ phase'=[phase EXCEPT ![c]="decided"]
             /\ effect'=[effect EXCEPT ![c]=IF staged[c] THEN 1 ELSE 0]
             /\ validated'=[validated EXCEPT ![c]=TRUE]
             /\ UNCHANGED <<charged,wire,rows,staged,failed>>
Deliver(c) == /\ phase[c]="decided"
              /\ phase'=[phase EXCEPT ![c]="output"]
              /\ charged'=[charged EXCEPT ![c]=IF BugEarlyResultRelease THEN 0 ELSE @]
              /\ UNCHANGED <<wire,rows,staged,effect,failed,validated>>
Flush(c) == /\ phase[c]="output"
            /\ phase'=[phase EXCEPT ![c]="done"]
            /\ rows'=[rows EXCEPT ![c]=0]
            /\ charged'=[charged EXCEPT ![c]=0]
            /\ UNCHANGED <<wire,staged,effect,failed,validated>>
Cancel(c) == /\ phase[c]="read"
             /\ phase'=[phase EXCEPT ![c]="abandoned"]
             /\ charged'=[charged EXCEPT ![c]=IF BugEarlyWireRelease THEN 0 ELSE @]
             /\ UNCHANGED <<wire,rows,staged,effect,failed,validated>>
DrainOrRetire(c) == /\ phase[c]="abandoned"
                    /\ phase'=[phase EXCEPT ![c]="done"]
                    /\ wire'=[wire EXCEPT ![c]=FALSE]
                    /\ charged'=[charged EXCEPT ![c]=0]
                    /\ UNCHANGED <<rows,staged,effect,failed,validated>>
Next == \E c \in Clients : Start(c) \/ Read(c) \/ Stage(c) \/ Refuse(c) \/
        Decide(c) \/ Deliver(c) \/ Flush(c) \/ Cancel(c) \/ DrainOrRetire(c)
Spec == Init /\ [][Next]_vars
TypeOK == /\ charged \in [Clients -> 0..PerQuery]
          /\ phase \in [Clients -> {"idle","read","active","decided","output","abandoned","done"}]
          /\ effect \in [Clients -> 0..1]
Bounded == Total <= Global
Ownership == \A c \in Clients : charged[c] >= rows[c] + IF wire[c] THEN 1 ELSE 0
NoPartialBudgetDML == \A c \in Clients : failed[c] => effect[c]=0
ValidatedOutput == \A c \in Clients : phase[c]="output" => validated[c]
=============================================================================
