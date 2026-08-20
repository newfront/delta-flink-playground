-- 02 | Primary-key upserts + merge-on-read deletion vectors
--     PRs #6933 #6935 #6977 #6997 (PK upserts) and #6964 #6977 (MoR / DVs)
--
-- A single streaming aggregation over a bounded event stream keyed by user_id in
-- a SMALL domain (1..20). In upsert mode the sink processes the changelog by
-- primary key: INSERT = new key, UPDATE_AFTER = replace, UPDATE_BEFORE ignored.
--
-- The job runs long enough to CHECKPOINT several times. Keys committed in one
-- checkpoint keep getting UPDATE_AFTER in later ones, so the sink retires the old
-- row versions with DELETION VECTORS (merge-on-read) and appends the new rows -
-- no full-file rewrite. A primary key is REQUIRED in upsert mode.
--
-- NOTE: commit often enough to span multiple checkpoints, but not so fast that
-- two commits overlap in the single global committer. Start from a CLEAN table
-- (the connector caches snapshots in the JobManager; `just demo-upsert` recreates
-- state cleanly).
--
-- Verify:  just verify 02_upsert_pk_mor   (expects 20 keys + deletion-vector actions)

SET 'table.dml-sync' = 'true';
SET 'pipeline.name'  = 'dfp-02-upsert-pk-mor';
SET 'execution.checkpointing.interval' = '5s';

-- ~45s stream so the aggregate for each key is re-emitted across ~9 checkpoints.
CREATE TEMPORARY TABLE events (
  user_id  BIGINT,
  amount   DOUBLE
) WITH (
  'connector'          = 'datagen',
  'number-of-rows'     = '4500',
  'rows-per-second'    = '100',
  'fields.user_id.min' = '1',
  'fields.user_id.max' = '20',
  'fields.amount.min'  = '1',
  'fields.amount.max'  = '100'
);

CREATE TEMPORARY TABLE user_totals (
  user_id       BIGINT,
  event_count   BIGINT,
  total_amount  DOUBLE,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector'   = 'delta',
  'table_path'  = 'file:///opt/flink/data/02_upsert_pk_mor',
  'write.mode'  = 'upsert',
  'uid'         = 'dfp-02'
);

-- Streaming aggregation emits +I then repeated +U per user_id across checkpoints.
INSERT INTO user_totals
SELECT
  user_id,
  COUNT(*)    AS event_count,
  SUM(amount) AS total_amount
FROM events
GROUP BY user_id;
