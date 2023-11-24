-- OPEN IN NEW SESSION "poc_worker1"

-- SELECT gen_random_uuid(); => d30b37d3-6c3b-4291-bb84-d9cad02292e0

-- read from the work queue, selecting a few jobs to work on
-- #1. start transaction here first
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
-- #2. go start nemesis tx in other session
-- healthcheck: prevent the jobs this worker is still working on to be picked up by another worker
UPDATE task_queue
SET locked_at = NOW()--,
--     locked_by = 'd30b37d3-6c3b-4291-bb84-d9cad02292e0' -- me
WHERE locked_by = 'd30b37d3-6c3b-4291-bb84-d9cad02292e0' -- me
RETURNING *
;
-- then diff the number of rows from the jobs this worker is handling and cancel those who were stolen
-- #3. select for update
SELECT *
FROM task_queue
WHERE (locked_at IS NULL
    OR locked_at < NOW() - INTERVAL '5 minutes')
  AND (locked_by IS NULL
    OR locked_by <> 'd30b37d3-6c3b-4291-bb84-d9cad02292e0') -- exclude rows this worker has just health-checked
ORDER BY created_at
LIMIT 2
    FOR UPDATE
;
-- #4. go select for update in nemesis tx
-- #5. nemesis tx is blocked
-- #6. update rows
UPDATE task_queue
SET locked_at = NOW(),
    locked_by = 'd30b37d3-6c3b-4291-bb84-d9cad02292e0'
WHERE job_id IN (
                 'da4b398b-3701-4499-9119-e2a328bd3c80',
'35508f03-bfff-4d5f-b1b9-db3902a38054'
    )
RETURNING *
;
-- #7. commit
COMMIT;




INSERT INTO task_queue (job_id, created_at, locked_by, locked_at)
VALUES ('78cdab83-f068-44d4-9be2-7c7d9691be7d', '2023-10-26 00:15:24.838493 +00:00', 'd30b37d3-6c3b-4291-bb84-d9cad02292e0', '2023-10-26 11:20:25.958752 +00:00')
;
SELECT pg_sleep(10);

BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
WITH
    gen AS (SELECT 'd30b37d3-6c3b-4291-bb84-d9cad02292e0'::uuid AS id),
    done AS (
        -- block other txs trying to update these rows so we can signal 'job done'
        -- or be blocked by other txs trying to steal from this worker
        -- (meaning this worker has timeouted or was beaten to the punch when health-checking concurrently, too bad if result was actually computed \_o_/).
        -- Now: how do I ensure that the results of the jobs/works are not saved?
        -- Because if this tx doesn't abort thanks to read committed, then the WHERE will
        -- be re-evaluated and will exclude the one which were stole, but if the
        -- tx is not aborted, as it would with repeatable read, thentthe result will be
        -- saved even though the job is stolen, so re-reprocessed...
        DELETE
            FROM task_queue
            USING gen
            WHERE locked_by = gen.id
                AND job_id IN (
                    '78cdab83-f068-44d4-9be2-7c7d9691be7d'
                    )
            RETURNING *
        -- I guess the "saving the result" part of the tx should be done using the `done`
        -- CTE results, so that when the wait is over and the WHERE clause of the DELETE is
        -- re-evaluated, then the resulting rows are the one whose result can
        -- rightfully be updated with the results of the jobs!
    ),
    -- block other txs trying to update these rows so we can signal 'WIP'
    -- or be blocked by other txs trying to steal from this worker
    -- (meaning this worker has timed out or was beaten to the punch when health-checking concurrently, too bad if job was almost done \_o_/).
    locked AS (
        UPDATE task_queue
            SET locked_at = NOW()
            FROM gen
            WHERE locked_by = gen.id
            RETURNING *
        -- return the locked rows? not sure this is needed, well, maybe for side effects 'cause
        -- I kinda glimpsed that if the result of the CTE is not used in query (below) then the
        -- side effects are not applied :/
        -- Oh yeah I remember! Get the jobs still locked by this worker, so that the others
        -- can be cancelled on the worker side so as to not continue processing a job
        -- which was stolen (those potentially excluded from the WHERE clause after its
        -- re-evaluation once the tx is unlocked).
    )
SELECT locked.* FROM locked, done
;


-- Maybe this could be run outside of the transaction since
-- read committed is the default for single statements anyway.
-- The rest of the tx doesn't really depend on having theses rows
-- updated but not visible to other txs or even blocking concurrent
-- txs. I think?
-- Maybe a TX is still needed so as to encapsulate the deletion/healtchecking
-- of the rows with the update of the job result inside a specific table
-- by the business logic, atomically.
-- Keeping while still tinkering.

UPDATE task_queue
SET locked_at = NOW(),
    locked_by = 'd30b37d3-6c3b-4291-bb84-d9cad02292e0'
WHERE job_id IN (
    SELECT *
    FROM task_queue
    WHERE (locked_at IS NULL
        OR locked_at < NOW() - INTERVAL '3 seconds')
      AND (locked_by IS NULL
        OR locked_by <> 'd30b37d3-6c3b-4291-bb84-d9cad02292e0') -- exclude rows this worker has just health-checked
    ORDER BY created_at
    LIMIT 2
        FOR UPDATE SKIP LOCKED
)
RETURNING *
;

COMMIT;