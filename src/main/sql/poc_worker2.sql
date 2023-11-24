-- OPEN IN NEW SESSION "poc_worker2"

-- SELECT gen_random_uuid(); => d68f808e-f376-41f6-ac58-7da5dc5fd657

-- read from the work queue, selecting a few jobs to work on
-- #1. start other transaction before
-- #2. start here
BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;
UPDATE task_queue
SET locked_at = NOW()--,
--     locked_by = 'd90427ff-1175-4a91-a93d-86fd68962306' -- me
WHERE locked_by = 'd90427ff-1175-4a91-a93d-86fd68962306' -- me
RETURNING *
;
-- #3. select jobs in other tx
-- #4. select jobs here
SELECT *
FROM task_queue
WHERE (locked_at IS NULL
    OR locked_at < NOW() - INTERVAL '3 seconds')
  AND (locked_by IS NULL
    OR locked_by <> 'd90427ff-1175-4a91-a93d-86fd68962306') -- exclude rows this worker has just health-checked
ORDER BY created_at
LIMIT 2
    FOR UPDATE
;
-- #5. this tx is being locked by other tx
-- Could be worth to set a timeout...
-- #6. do update on rows selected in other tx
-- #7. commit other tx
-- #8. this select is being unlocked by commit of other tx
-- the results are the two following jobs in the queue
-- #9. do update on rows selected in this tx
UPDATE task_queue
SET locked_at = NOW(),
    locked_by = 'd90427ff-1175-4a91-a93d-86fd68962306'
WHERE job_id IN (
                 '87a0d764-c2f2-4797-819a-f918af90c0d9',
'0b77cbbf-16dd-4664-a21c-0770907dc904'
    )
RETURNING *
;
-- #10. commit
COMMIT;




BEGIN TRANSACTION ISOLATION LEVEL READ COMMITTED;

UPDATE task_queue
SET locked_at = NOW()--,
WHERE locked_by = 'd90427ff-1175-4a91-a93d-86fd68962306' -- me
RETURNING *
;

SELECT *
FROM task_queue
WHERE (locked_at IS NULL
    OR locked_at < NOW() - INTERVAL '3 seconds')
  AND (locked_by IS NULL
    OR locked_by <> 'd90427ff-1175-4a91-a93d-86fd68962306') -- exclude rows this worker has just health-checked
ORDER BY created_at
LIMIT 2
    FOR UPDATE SKIP LOCKED
;

UPDATE task_queue
SET locked_at = NOW(),
    locked_by = 'd90427ff-1175-4a91-a93d-86fd68962306'
WHERE job_id IN (
                 '3ec685b2-30f5-4128-8c7c-9802cbab39c2'
    )
RETURNING *
;

COMMIT;









BEGIN TRANSACTION ISOLATION LEVEL REPEATABLE READ;

UPDATE task_queue
SET locked_at = NOW()--,
WHERE locked_by = 'd90427ff-1175-4a91-a93d-86fd68962306' -- me
RETURNING *
;
/*
 In this isolation level, this could cause a rollback of this tx due to
 serialization failure error in case another worker is trying to steal
 some jobs from this worker because selecting all jobs like we do here
 conflicts with potential stealing of a subset of those jobs.
 But from here we can't know which jobs the other tx is trying to steal,
 so we can't really try to exclude them from the selection.

 Hypothesis: in read committed the update is locked until the stealing
 it done, but the jobs which have not been stolen (other workers may
 try to steal only a subset of the jobs of this worker) are re-evaluated here
 when this tx is unlocked by the commit of the stealing tx, and those
 can be health-checked without problem.
 Let's verify.

Yup! Verified!
 In read committed, the other worker is able to steal some (or all)
 of the jobs of this worker in case the latter failed to healthcheck on time,
 and without serialization failure because now the re-evaluation of the
 selection is only done on the jobs who still belong to this worker after
 the other one successfully stole some.
 This tx is still able to proceed with the update once its tx is
 unlocked, and can continue to claim its remaining jobs and fetch others
 from the queue without serialization error in this isolation level.
 */

SELECT *
FROM task_queue
WHERE (locked_at IS NULL
    OR locked_at < NOW() - INTERVAL '3 seconds')
  AND (locked_by IS NULL
    OR locked_by <> 'd90427ff-1175-4a91-a93d-86fd68962306') -- exclude rows this worker has just health-checked
ORDER BY created_at
LIMIT 2
    FOR UPDATE SKIP LOCKED
;

UPDATE task_queue
SET locked_at = NOW(),
    locked_by = 'd90427ff-1175-4a91-a93d-86fd68962306'
WHERE job_id IN (
                 '9ea4d69f-b424-4598-9093-1b6d8b1332fb',
                 'e64c9caa-ac3c-4c50-b20c-32ad80c7c783'
    )
RETURNING *
;

COMMIT;