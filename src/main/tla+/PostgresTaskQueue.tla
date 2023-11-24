---- MODULE PostgresTaskQueue ----
EXTENDS Integers, Sequences, TLC, FiniteSets

\* The set of worker IDs
CONSTANT Workers

\* A reserved value for when there is no value
CONSTANT Nil

\* The maximum number of jobs that can be produced
\* (to stop the checker at some point)
CONSTANT MaxJobs

\* The maximum number of jobs that can be held by a worker at any time
\* (i.e. the parrallelism factor for a worker)
CONSTANT MaxJobsPerWorker

ASSUMPTION
    /\ Cardinality(Workers) > 0
    /\ MaxJobs \in Nat \ {0}
    /\ MaxJobsPerWorker \in Nat \ {0}
    /\ MaxJobsPerWorker <= MaxJobs

\* Monotonically increasing counter of jobs produced
VARIABLE produced

\* Per worker (functions with domain Server): jobs held
VARIABLE jobs

\* Seq of jobs to be processed
VARIABLE queue

\* All variables; used for stuttering/fairness conditions
vars == <<produced, jobs, queue>>

----
\* Helpers

\* Define a job initial state given an identifier
Job(id) == [id |-> id, lockedBy |-> Nil, timedOut |-> Nil]

\* Functional operators to make things easier sometimes but more specifically
\* TO MAINTAIN ORDER WHEN REDUCING OVER SEQUENCES!
RECURSIVE ReduceSeq(_,_,_)
ReduceSeq(Op(_,_), L, initial) == IF Len(L) = 0 THEN initial
                                   ELSE Op(ReduceSeq(Op, Tail(L), initial), Head(L))
MapSeq(Op(_), L) == ReduceSeq(LAMBDA acc, item: <<Op(item)>> \o acc, L, <<>>)
FilterSeq(Pred(_), L) == ReduceSeq(LAMBDA acc, item: IF Pred(item) THEN <<item>> \o acc ELSE acc, L, <<>>)

----
\* Define initial values for all variables

Init ==
    /\ produced = 0
    /\ queue = <<>>
    /\ jobs = [w \in Workers |-> {}]

----
\* Define next state transitions

\* A job is enqueued (by a producer)
Enqueue ==
    /\ produced < MaxJobs
    /\ queue' = Append(queue, Job(produced))
    /\ produced' = produced + 1
    /\ UNCHANGED <<jobs>>

\* Lock at most
\* MaxJobsPerWorker - count(jobs the worker holds the lock on)
\* by peeking at the queue or stealing from other workers which timed out.
Lock(w) ==
    /\ LET
        maxToPoll == MaxJobsPerWorker - Cardinality(jobs[w])
        jobsToLock == ReduceSeq(
            LAMBDA toBeLocked, job:
                IF /\ Cardinality(toBeLocked) < maxToPoll
                   /\ \/ job.timedOut = Nil
                      \/ job.timedOut = TRUE
                   /\ \/ job.lockedBy = Nil
                      \/ job.lockedBy # w
                THEN toBeLocked \union {job.id}
                ELSE toBeLocked,
            queue,
            {} \* aka "toBeLocked"
        )
       IN
        /\ jobs' = [jobs EXCEPT ![w] = jobs[w] \union jobsToLock]
        /\ queue' = [j \in DOMAIN queue |-> IF queue[j].id \in jobsToLock THEN [lockedBy |-> w, timedOut |-> FALSE] @@ queue[j] ELSE queue[j]]
        /\ UNCHANGED <<produced>>

\* Mark
\* MaxJobsPerWorker - count(jobs the worker holds the lock on)
\* jobs done (i.e. delete them).
Compute(w) ==
    /\ \E nbToComplete \in 0..Cardinality(jobs[w]) :
        \E jobsToComplete \in { js \in SUBSET jobs[w] : Cardinality(js) = nbToComplete } :
            \E jobToComplete \in jobsToComplete :
                /\ { j \in DOMAIN queue : queue[j].id = jobToComplete /\ queue[j].lockedBy = w } # {}
                /\ jobs' = [jobs EXCEPT ![w] = jobs[w] \ { jobToComplete }]
                /\ queue' = FilterSeq(LAMBDA job: job.id # jobToComplete, queue)
                /\ UNCHANGED <<produced>>

\* A worker times out due to not healthchecking in time
Timeout(w) ==
    /\ queue' = [j \in DOMAIN queue |-> IF queue[j].lockedBy = w THEN [timedOut |-> TRUE] @@ queue[j] ELSE queue[j]]
    /\ UNCHANGED <<produced, jobs>>

\* A worker healthchecks and refreshes its view of the jobs it still holds the lock on
Healthcheck(w) ==
    /\ queue' = [j \in DOMAIN queue |-> IF queue[j].lockedBy = w THEN [timedOut |-> FALSE] @@ queue[j] ELSE queue[j]]
    /\ jobs' = [jobs EXCEPT ![w] = { queue[j].id : j \in {i \in DOMAIN queue : queue[i].lockedBy = w}}]
    /\ UNCHANGED <<produced>>

Next ==
    \/ Enqueue
    \/ \E w \in Workers : Healthcheck(w)
    \/ \E w \in Workers : Timeout(w)
    \/ \E w \in Workers : Lock(w)
    \/ \E w \in Workers : Compute(w)

----
\* Fairness conditions

\* Some progress is necessary in order to verify QueueDrained
Progress == WF_vars(Next)

\* If we start queuing, we will eventually compute
EventuallyCompute == WF_vars(\E w \in Workers : Compute(w))

Fairness ==
    /\ Progress
    /\ EventuallyCompute

----
\* The specification

Spec ==
    /\ Init
    /\ [][Next]_vars
    /\ Fairness

----
\* Define safety properties (invariants)

\* no rollback on queueing
NoRollback ==
    produced >= 0

\* jobs are only held by workers
JobsHeldByWorkers ==
    DOMAIN jobs \ Workers = {}

TypeOK ==
    /\ NoRollback
    /\ JobsHeldByWorkers

AtMostMaxJobsPerWorker ==
    \A w \in Workers : Cardinality(jobs[w]) <= MaxJobsPerWorker

----
\* Define liveness properties (temporal formulas)

\* The queue will eventually be drained and will remain empty
QueueDrained ==
    <>[](queue = <<>>)

====
