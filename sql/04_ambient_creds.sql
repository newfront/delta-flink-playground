-- 04 | Ambient storage credentials (S3 path-based write)     PRs #7045 #7048
--
-- Writes a path-based Delta table to the RustFS S3 object store WITHOUT Unity
-- Catalog credential vending. For a path-based table the connector's Kernel
-- engine reads S3A settings (endpoint + keys) from core-site.xml - i.e. the
-- "ambient" credential source (credentials.source=ambient behaviour).
--
-- NOTE on this build (see README "Connector limitations"):
--   * The Flink SQL `delta` factory only accepts a fixed option set, so the
--     per-table `credentials.source` / `fs.s3a.*` options from #7045/#7048
--     cannot be set in a SQL WITH clause (they are DataStream-API options).
--     S3A settings are supplied globally via core-site.xml instead (rendered
--     from $S3_ENDPOINT_URL / $S3_ACCESS_KEY / $S3_SECRET_KEY).
--   * createEngine() hardcodes fs.s3a.path.style.access=false, so a custom S3
--     endpoint (MinIO/RustFS) fails with virtual-host DNS ('bucket.host'). This
--     demo therefore requires a REAL AWS S3 endpoint (virtual-host addressing).
--
-- PREREQUISITE: set S3_ENDPOINT_URL / S3_ACCESS_KEY / S3_SECRET_KEY to a real S3
-- bucket in .env, restart the cluster, then point table_path below at it.

SET 'table.dml-sync' = 'true';
SET 'pipeline.name'  = 'dfp-04-ambient-creds';

CREATE TEMPORARY TABLE src (
  id        BIGINT,
  region    STRING,
  amount    DOUBLE
) WITH (
  'connector'          = 'datagen',
  'number-of-rows'     = '1500',
  'rows-per-second'    = '1500',
  'fields.id.kind'     = 'sequence',
  'fields.id.start'    = '1',
  'fields.id.end'      = '1500',
  'fields.region.length' = '3',
  'fields.amount.min'  = '1',
  'fields.amount.max'  = '500'
);

CREATE TEMPORARY TABLE delta_sink (
  id        BIGINT,
  region    STRING,
  amount    DOUBLE
) WITH (
  'connector'   = 'delta',
  'table_path'  = 's3a://uc-warehouse/flink-playground/04_ambient_creds',
  'uid'         = 'dfp-04'
);

INSERT INTO delta_sink SELECT id, region, amount FROM src;
