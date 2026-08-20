-- 01 | Baseline append (sanity check)
-- Bounded datagen source -> path-based Delta table on the local filesystem.
-- Mirrors FlinkSqlTest.testLoadIntoHadoopTable. Needs no external services.
--
-- Verify:  just verify 01_path_append   (expects 2000 rows)

SET 'table.dml-sync' = 'true';
SET 'pipeline.name' = 'dfp-01-path-append';

CREATE TEMPORARY TABLE src (
  id        BIGINT,
  category  STRING,
  price     DOUBLE
) WITH (
  'connector'          = 'datagen',
  'number-of-rows'     = '2000',
  'rows-per-second'    = '2000',
  'fields.id.kind'     = 'sequence',
  'fields.id.start'    = '1',
  'fields.id.end'      = '2000',
  'fields.category.length' = '6',
  'fields.price.min'   = '1',
  'fields.price.max'   = '1000'
);

CREATE TEMPORARY TABLE delta_sink (
  id        BIGINT,
  category  STRING,
  price     DOUBLE
) WITH (
  'connector'   = 'delta',
  'table_path'  = 'file:///opt/flink/data/01_path_append',
  'uid'         = 'dfp-01'
);

INSERT INTO delta_sink SELECT id, category, price FROM src;
