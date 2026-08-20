-- 03 | UC Delta API integration          PRs #7229 #7244
--
-- Registers the OSS Unity Catalog server (from the neighbouring
-- unitycatalog-playground) as a Flink catalog and INSERTs into a UC-managed
-- Delta table. Catalog-managed loads, existence checks, commits, and
-- storage-credential vending go through the UC Delta API; data lands in the
-- RustFS object store that backs that UC.
--
-- PREREQUISITES (the Flink UC catalog is sink-only - it cannot CREATE the
-- table/schema, so create them once via Spark first):
--   1. In unitycatalog-playground:  just uc=local start
--   2. From THIS repo:              just uc-setup      (creates unity.flink_playground.clickstream)
-- Then run this demo:               just demo-uc       (or: just sql 03_uc_catalog)
--
-- The endpoint/token below match .env defaults (host.docker.internal:8080,
-- auth disabled). Edit if you point at a different UC.

SET 'table.dml-sync' = 'true';
SET 'pipeline.name'  = 'dfp-03-uc-catalog';

CREATE CATALOG uc WITH (
  'type'     = 'unitycatalog',
  'endpoint' = 'http://host.docker.internal:8080',
  'token'    = 'not-used-auth-disabled'
);

CREATE TEMPORARY TABLE src (
  id    BIGINT,
  name  STRING
) WITH (
  'connector'       = 'datagen',
  'number-of-rows'  = '1000',
  'rows-per-second' = '1000',
  'fields.id.kind'  = 'sequence',
  'fields.id.start' = '1',
  'fields.id.end'   = '1000',
  'fields.name.length' = '8'
);

INSERT INTO uc.`unity`.`flink_playground`.`clickstream`
SELECT id, name FROM src;
