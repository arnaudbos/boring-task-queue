-- OPEN IN NEW SESSION "poc_producer"

DROP TABLE IF EXISTS task_queue;
CREATE TABLE task_queue
(
    job_id uuid PRIMARY KEY, -- unique id for this job
    created_at timestamp with time zone NOT NULL DEFAULT now(), -- when the job was created
    locked_by uuid, -- id of the worker that has locked this job to work on, if any
    locked_at timestamp with time zone -- when the job was locked
);

-- push a new job down the work queue
INSERT INTO task_queue (job_id) VALUES (gen_random_uuid());
INSERT INTO task_queue (job_id) VALUES (gen_random_uuid());
INSERT INTO task_queue (job_id) VALUES (gen_random_uuid());
INSERT INTO task_queue (job_id) VALUES (gen_random_uuid());
INSERT INTO task_queue (job_id) VALUES (gen_random_uuid());

SELECT * FROM task_queue ORDER BY created_at;
