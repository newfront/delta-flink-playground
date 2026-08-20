# delta-flink-playground — task runner
#
# Run `just` (or `just help`) to see all recipes. Requires: docker (compose
# plugin). `just assembly` also needs a local delta-io/delta clone (DELTA_REPO);
# `just verify` needs `uv`.
#
# Typical first run:
#   just init            # create .env
#   just jars            # build the connector fat jar + download storage jars
#   just up-detached     # start the Flink session cluster
#   just demo-append     # run + verify the baseline demo
#   just demo-upsert     # run + verify PK upserts + merge-on-read deletion vectors

# ---- Configuration ----------------------------------------------------------

# Toggle the Redpanda (Kafka) profile for the CDC upsert demo (sql/06):
#   just kafka=on up-detached   /   just kafka=on demo-upsert-kafka
kafka := "off"
compose_profile := if kafka == "on" { "--profile kafka" } else if kafka == "off" { "" } else { error("kafka must be 'on' or 'off', got '" + kafka + "'") }
compose := "docker compose " + compose_profile

# Container name of the neighbouring unitycatalog-playground Spark container,
# used to create/read UC tables for the sql/03 demo (it is sink-only in Flink).
uc_spark_container := "marimo-spark"

# ---- Meta -------------------------------------------------------------------

# Show all available recipes (default when running bare `just`).
default:
    @just --list

alias help := default

# ---- Environment file -------------------------------------------------------

# Create .env from .env.example if it doesn't already exist (idempotent).
init:
    @test -f .env && echo ".env already exists — leaving it untouched." || { cp .env.example .env && echo "Created .env from .env.example — review DELTA_REPO / MAVEN_PROXY_URL."; }

# ---- Connector + storage jars -----------------------------------------------

# Build the delta-flink fat jar from $DELTA_REPO into flink/usrlib (uses $MAVEN_PROXY_URL).
assembly: init
    ./scripts/assembly.sh

# Download AWS SDK bundle + guava into flink/usrlib (append 'kafka' for sql/06's connector).
bundle-jars *args: init
    ./scripts/bundle-jars.sh {{args}}

# Convenience: build the connector jar AND download the storage jars.
jars: assembly bundle-jars

# ---- Cluster lifecycle ------------------------------------------------------

# Pull the Flink (and Redpanda, if kafka=on) images.
build: init
    {{compose}} pull

# Start the Flink session cluster in the foreground.
up: init
    {{compose}} up

# Start the Flink session cluster detached.
up-detached: init
    {{compose}} up -d
    @just ui

# Build/pull, start detached, and print the Flink UI URL.
start: build up-detached

# Print the Flink Web UI URL.
ui:
    @echo ""
    @echo "Flink Web UI:  http://localhost:8081"
    @echo ""

# Show running services.
ps:
    {{compose}} ps

# Follow all logs (Ctrl-C to stop).
logs:
    {{compose}} logs -f

# Follow JobManager logs only.
logs-jm:
    {{compose}} logs -f jobmanager

# Stop and remove containers (keeps the ./data tables and volumes).
down:
    {{compose}} down

alias stop := down

# Like down, but also remove named volumes.
down-volumes:
    {{compose}} down --volumes

# Tear down and wipe the local Delta tables under ./data.
clean: down
    -rm -rf data/[0-9]*_* data/_checkpoints

# Recreate the Flink containers to clear the JobManager's in-JVM snapshot cache
# (the connector caches table snapshots per path; wiping a table's files under a
# live cluster otherwise leaves the cache pointing at versions that no longer
# exist on disk), then wait for a TaskManager + the staged connector jar.
recycle:
    #!/usr/bin/env bash
    set -euo pipefail
    {{compose}} restart jobmanager taskmanager taskmanager2 sql-client
    for _ in $(seq 1 40); do
        n=$({{compose}} exec -T jobmanager bash -lc 'curl -s localhost:8081/taskmanagers' 2>/dev/null \
            | python3 -c "import sys,json;print(len(json.load(sys.stdin).get('taskmanagers',[])))" 2>/dev/null || echo 0)
        # Also wait until the SQL client has staged the connector jar (init.sh),
        # otherwise a job submitted immediately hits "no factory for 'delta'".
        jars=$({{compose}} exec -T sql-client bash -lc 'ls /opt/flink/lib/delta-flink-*.jar 2>/dev/null | wc -l' 2>/dev/null | tr -d ' ' || echo 0)
        if [ "${n:-0}" -ge 1 ] && [ "${jars:-0}" -ge 1 ]; then
            echo "cluster ready (${n} taskmanager(s), connector staged)"; exit 0
        fi
        sleep 2
    done
    echo "cluster did not become ready in time" >&2; exit 1

# Clean slate: recreate the cluster and wipe all local Delta tables under ./data.
reset:
    {{compose}} down
    -rm -rf data/[0-9]*_* data/_checkpoints
    {{compose}} up -d
    @just recycle

# ---- Running SQL ------------------------------------------------------------

# Run a SQL script on the cluster via the idle sql-client container.
# Usage: just sql 01_path_append   (the .sql suffix is optional)
#
# This build's SQL client does not self-exit after -f (it drops to an interactive
# prompt that never returns), so we submit in the background and drive completion
# off the job's REST state (matched by its pipeline.name = dfp-<script>), then
# stop the lingering client. Use this only for BOUNDED jobs; the unbounded Kafka
# demo (sql/06) is submitted detached by `just demo-upsert-kafka`.
# Run a bounded SQL script on the cluster; returns when its job finishes.
sql file:
    ./scripts/run_sql.sh {{file}}

# Open an interactive Flink SQL shell against the cluster.
sql-shell:
    {{compose}} exec sql-client /opt/flink/bin/sql-client.sh

# ---- Demos (run + verify) ---------------------------------------------------

# 01: baseline append -> local FS Delta table (starts from a clean table).
demo-append:
    rm -rf data/01_path_append
    @just recycle
    @just sql 01_path_append
    @just verify 01_path_append

# 02: PK upserts + merge-on-read deletion vectors -> local FS (~45s streaming job).
demo-upsert:
    rm -rf data/02_upsert_pk_mor
    @just recycle
    @just sql 02_upsert_pk_mor
    @just verify 02_upsert_pk_mor

# 04/03/05: S3 + UC demos. These are authored as reference (see sql/03..05) but
# are NOT runnable against local object stores in this experimental build:
#   * the SQL `delta` factory rejects `fs.*` / `credentials.source` options;
#   * createEngine() hardcodes fs.s3a.path.style.access=false, so custom-endpoint
#     stores (MinIO/RustFS) fail with virtual-host DNS ('bucket.host');
#   * RustFS additionally NPEs the AWS SDK v2 (ListObjectsV2 missing isTruncated).
# They require REAL AWS S3 (and, for UC, a UC backed by real S3 / Databricks), or
# the DataStream API for the fs.* passthrough. See README "Connector limitations".

# 04 (ambient S3 creds): reference-only in this build - needs real AWS S3.
demo-ambient:
    @echo "sql/04 (ambient S3 creds) needs a REAL AWS S3 endpoint - see README 'Connector limitations'."

# 05 (underscore-authority path fix #7027): reference-only - see README.
demo-path:
    @echo "sql/05 (underscore-authority path fix, #7027) needs a lenient + SDK-compatible S3 store - see README 'Connector limitations'."

# 03 (UC Delta API): reference-only - needs a UC backed by real S3 / Databricks.
demo-uc:
    @echo "sql/03 (UC Delta API) needs a UC backed by real S3 (or Databricks) + the table pre-created via Spark (just uc-setup). See README."

# 06: upsert CDC from Kafka (INSERT/UPDATE/DELETE). Requires kafka=on.
demo-upsert-kafka:
    #!/usr/bin/env bash
    set -euo pipefail
    if [ "{{kafka}}" != "on" ]; then
        echo "This demo needs the kafka profile. Re-run as: just kafka=on demo-upsert-kafka" >&2
        exit 1
    fi
    echo ">>> producing changelog to Redpanda ..."
    ./scripts/kafka_produce.sh
    echo ">>> submitting sql/06 (detached, unbounded) ..."
    {{compose}} exec -d sql-client /opt/flink/bin/sql-client.sh -f /sql/06_upsert_kafka_cdc.sql
    echo ">>> waiting ~30s for a couple of checkpoints to commit ..."
    sleep 30
    just verify 06_upsert_kafka_cdc

# ---- Unity Catalog helpers (run in the neighbour Spark container) -----------

# Create the schema + managed table the sql/03 Flink insert writes into.
uc-setup:
    #!/usr/bin/env bash
    set -euo pipefail
    if ! docker ps --format '{{{{.Names}}}}' | grep -qx "{{uc_spark_container}}"; then
        echo "Container '{{uc_spark_container}}' not running. Start unitycatalog-playground first:" >&2
        echo "  (in that repo)  just uc=local start" >&2
        exit 1
    fi
    docker exec -i -e UC_URI=http://unitycatalog:8080 -e S3_ENDPOINT=http://rustfs:9000 \
        {{uc_spark_container}} bash -lc 'cat > /tmp/uc_spark.py && python /tmp/uc_spark.py setup' < verify/uc_spark.py

# Read back the UC table to verify the Flink insert landed.
uc-read:
    #!/usr/bin/env bash
    set -euo pipefail
    docker exec -i -e UC_URI=http://unitycatalog:8080 -e S3_ENDPOINT=http://rustfs:9000 \
        {{uc_spark_container}} bash -lc 'cat > /tmp/uc_spark.py && python /tmp/uc_spark.py read' < verify/uc_spark.py

# ---- Verification -----------------------------------------------------------

# Read tables back (pyarrow) and assert outcomes (deletion vectors, counts).
# Usage: just verify   |   just verify 02_upsert_pk_mor   |   just verify all
# Verify demo tables: row/key counts + merge-on-read deletion vectors.
verify *demos:
    cd verify && uv run python verify.py {{demos}}

# Run the self-contained local demos end to end, then verify them.
test: demo-append demo-upsert
    @echo ""
    @echo "Local demos passed (append + PK upsert + merge-on-read deletion vectors)."
    @echo "The S3/UC demos (sql/03-05) are reference-only in this build - see README 'Connector limitations'."
