-- 06 | Upsert CDC from Kafka - INSERT / UPDATE / DELETE end-to-end   (ADVANCED)
--     PRs #6933 #6935 #6977 #6997 (PK upserts) + #6964 #6977 (MoR / DVs)
--
-- Reads a keyed changelog from Redpanda via the upsert-kafka connector (a null
-- value is a tombstone = DELETE) and upserts it into a Delta table by primary
-- key. Unlike sql/02 (aggregation, no deletes), this shows DELETE handling too.
--
-- This source is UNBOUNDED, so the job runs continuously and commits on each
-- checkpoint (every 10s). `just demo-upsert-kafka` produces a small changelog,
-- submits this job detached, waits for a couple of checkpoints, verifies, then
-- cancels the job.
--
-- PREREQUISITE: the kafka profile must be running:  just kafka=on up-detached

SET 'pipeline.name' = 'dfp-06-upsert-kafka-cdc';

CREATE TEMPORARY TABLE user_events_kafka (
  user_id  BIGINT,
  status   STRING,
  amount   DOUBLE,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector'                     = 'upsert-kafka',
  'topic'                         = 'ecomm.v1.clickstream',
  'properties.bootstrap.servers'  = 'redpanda:29092',
  'properties.group.id'           = 'dfp-06',
  'key.format'                    = 'json',
  'value.format'                  = 'json'
);

CREATE TEMPORARY TABLE user_state (
  user_id  BIGINT,
  status   STRING,
  amount   DOUBLE,
  PRIMARY KEY (user_id) NOT ENFORCED
) WITH (
  'connector'   = 'delta',
  'table_path'  = 'file:///opt/flink/data/06_upsert_kafka_cdc',
  'write.mode'  = 'upsert',
  'uid'         = 'dfp-06'
);

INSERT INTO user_state SELECT user_id, status, amount FROM user_events_kafka;
