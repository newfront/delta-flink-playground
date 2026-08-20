# AGENTS.md — delta-flink-playground

Conventions and gotchas for working in this repo.

## What this is

A Docker + `just` playground for the experimental **`delta-flink`** connector
(Delta 4.4.0). It runs a stock Flink session cluster and drives it from the SQL
client, then verifies results with delta-rs. See `README.md` for the feature ->
PR mapping.

## Golden rules

- **Prefer `just`** over raw `docker compose`. Recipes wire up profiles, the
  neighbour Spark container, and verification.
- **The connector is a private build.** It is not on Maven. `just assembly`
  builds it from `$DELTA_REPO` (a local `delta-io/delta` clone) via sbt.
- **Sink-only + Flink 2.0.** The connector has no source, and no `CREATE TABLE`
  in the Flink UC catalog (`createTable`/`createDatabase` are `notSupported`).
  UC tables for sql/03 must be created via Spark (`just uc-setup`).
- **Upsert always = merge-on-read.** `write.mode=upsert` uses deletion vectors
  (`MoRUpsert`); it requires a `PRIMARY KEY`.

## Proxy / firewall

- **Java builds inherit `$MAVEN_PROXY_URL` from the shell.** Delta's `build/sbt`
  consumes it and sets `-Dsbt.override.build.repos=true`, routing all Coursier/Ivy
  fetches (and the sbt-launch bootstrap) through the proxy. `scripts/assembly.sh`
  and `scripts/bundle-jars.sh` capture the shell value before sourcing `.env` so
  the shell wins; `.env` is only a fallback.
- `just bundle-jars` downloads the AWS SDK bundle + guava from
  `${MAVEN_PROXY_URL:-https://repo1.maven.org/maven2}`.

## Networking / cross-project

- Cross-project services (the OSS UC server and RustFS from
  `unitycatalog-playground`) are reached over **`host.docker.internal`** on the
  ports that playground publishes (`:8080` UC, `:9000` RustFS). All Flink
  containers set `extra_hosts: host.docker.internal:host-gateway`.
- `flink/usrlib/init.sh` renders `core-site.xml` from `core-site.template.xml`,
  substituting `@S3_ENDPOINT_URL@` from `$S3_ENDPOINT_URL`. To wire UC/RustFS via
  a shared docker network instead, set `S3_ENDPOINT_URL` to the in-network host
  (e.g. `http://rustfs:9000`) and attach the containers to that network.
- The `verify.py` script runs on the **host**, so it uses `localhost:9000` for
  RustFS (override with `VERIFY_S3_ENDPOINT`), while containers use
  `host.docker.internal:9000`.

## Jars on the classpath

- `init.sh` copies everything in `flink/usrlib/*.jar` into `/opt/flink/lib` on
  every container (jobmanager, taskmanagers, sql-client) at startup.
- Only one `delta-flink-*.jar` should be present; `assembly.sh` removes older
  ones before copying the new build in.
- The AWS SDK `bundle` is required at runtime for S3/UC demos and is excluded
  from the assembly on purpose — always run `just bundle-jars` too.

## SQL demos

- Local-FS tables live at `file:///opt/flink/data/<demo>` (bind-mounted to
  `./data/<demo>` on the host). Only sql/01, sql/02, sql/06 run locally; the S3/UC
  demos (03–05) are reference-only (see limitations below).

## Hard-won operational notes (read before changing demos)

- **`scripts/run_sql.sh`, not `sql-client -f` directly.** This build's SQL client
  does NOT exit after `-f` (it blocks at an interactive prompt even on EOF). The
  script submits in the background and polls the job's REST state (matched by
  `pipeline.name` = `dfp-<script>`), then kills the client. Only for BOUNDED jobs.
- **Recycle to clear the snapshot cache.** The connector caches table snapshots
  in the JobManager JVM. After wiping a table's files, you MUST recreate the
  containers (`just recycle` / `just reset`) or commits fail with
  `_delta_log/…N.json does not exist`. `recycle` also waits for the connector jar
  to be re-staged in the sql-client (else "no factory for 'delta'").
- **Deletion vectors need multiple commits.** A bounded job finishing before its
  first checkpoint commits once (no DVs). sql/02 uses a ~45s streaming aggregation
  + 5s checkpoint interval so keys are updated across commits → DVs.
- **SQL factory option allow-list.** `DeltaDynamicTableSinkFactory.optionalOptions()`
  rejects `credentials.source` / `fs.s3a.*` (DataStream-API only).
- **Custom-endpoint S3 doesn't work via SQL.** `AbstractKernelTable.createEngine()`
  hardcodes `fs.s3a.path.style.access=false`; a programmatic `set()` beats a
  `core-site.xml` value (even `<final>`), so MinIO/RustFS fail (virtual-host DNS).
  RustFS also NPEs the AWS SDK v2 (ListObjectsV2 isTruncated). S3 demos need real
  AWS S3. Full detail in README "Connector limitations".

## Editing

- Keep `.env.example` and the `verify.py` DEMOS registry in sync with the SQL
  scripts (table paths, expected row/key counts).
- `FLINK_VERSION` (image tag) and `FLINK_BUILD_VERSION` (sbt `-DflinkVersion`)
  are distinct: 2.0.1 image + 2.0.2 build spec is the tested pairing.
- Don't commit `.env`, jars (`flink/usrlib/*.jar`), or the `./data` tables.
