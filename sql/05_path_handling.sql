-- 05 | Path handling fix - underscore in the storage authority     PR #7027
--
-- Before #7027, URI normalization dropped storage authorities that contained
-- underscores (or user-info) - so a bucket like `flink_underscore_demo` broke.
-- This demo writes to exactly such a bucket; the write succeeding and the paths
-- round-tripping in _delta_log confirms the authority is preserved.
--
-- STATUS (see README "Connector limitations"): not runnable against local object
-- stores in this build - MinIO rejects underscore bucket names, RustFS accepts
-- them but NPEs the AWS SDK v2, and the SQL factory can't pass fs.*/path-style
-- for a custom endpoint. #7027 is primarily covered by the connector's unit
-- tests; this script documents the intended usage against a compatible S3 store.

SET 'table.dml-sync' = 'true';
SET 'pipeline.name'  = 'dfp-05-path-handling';

CREATE TEMPORARY TABLE src (
  id     BIGINT,
  label  STRING
) WITH (
  'connector'       = 'datagen',
  'number-of-rows'  = '500',
  'rows-per-second' = '500',
  'fields.id.kind'  = 'sequence',
  'fields.id.start' = '1',
  'fields.id.end'   = '500',
  'fields.label.length' = '5'
);

CREATE TEMPORARY TABLE delta_sink (
  id     BIGINT,
  label  STRING
) WITH (
  'connector'   = 'delta',
  -- authority (bucket) contains an underscore - the case PR #7027 fixes.
  -- S3A credentials/endpoint come from core-site.xml (see sql/04 note).
  'table_path'  = 's3a://flink_underscore_demo/05_path_handling',
  'uid'         = 'dfp-05'
);

INSERT INTO delta_sink SELECT id, label FROM src;
