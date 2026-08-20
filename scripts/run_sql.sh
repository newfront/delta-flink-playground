#!/usr/bin/env bash
# Run a bounded SQL script on the Flink session cluster and return when its job
# reaches a terminal state.
#
# This build's SQL client does not self-exit after `-f` (it drops to an
# interactive prompt that never returns), so we submit it in the background and
# drive completion off the job's REST state, matched by its pipeline.name
# (dfp-<script>). Use only for BOUNDED jobs; the unbounded Kafka demo (sql/06) is
# submitted detached by `just demo-upsert-kafka`.
#
# Usage: scripts/run_sql.sh <script-name>   (the .sql suffix is optional)
set -uo pipefail

cd "$(dirname "${BASH_SOURCE[0]}")/.."

name="${1:?usage: run_sql.sh <script-name>}"; name="${name%.sql}"
jobname="dfp-${name//_/-}"
log="/tmp/dfp-sql-${name}.log"

echo ">>> running sql/${name}.sql (job ${jobname}) ..."

# Job ids already present, so we never match a stale run of the same name.
pre=$(docker compose exec -T jobmanager bash -lc "curl -s localhost:8081/jobs" 2>/dev/null \
    | python3 -c "import sys,json;print(','.join(j['id'] for j in json.load(sys.stdin).get('jobs',[])))" 2>/dev/null || echo "")

docker compose exec -T sql-client /opt/flink/bin/sql-client.sh -f "/sql/${name}.sql" </dev/null >"${log}" 2>&1 &
cpid=$!

state=""
for _ in $(seq 1 120); do
    state=$(docker compose exec -T jobmanager bash -lc "curl -s localhost:8081/jobs/overview" 2>/dev/null \
        | JOB="${jobname}" PRE="${pre}" python3 -c '
import os, sys, json
pre = set(filter(None, os.environ["PRE"].split(",")))
job = os.environ["JOB"]
d = json.load(sys.stdin)
for j in d.get("jobs", []):
    if j["name"] == job and j["jid"] not in pre and j["state"] in ("FINISHED", "FAILED", "CANCELED"):
        print(j["state"]); break
' 2>/dev/null || echo "")
    [ -n "${state}" ] && break
    grep -q "\[ERROR\]" "${log}" 2>/dev/null && { state="CLIENT_ERROR"; break; }
    sleep 2
done

kill "${cpid}" 2>/dev/null || true
docker compose exec -T sql-client bash -lc 'pkill -f sql-client.sh || true' >/dev/null 2>&1 || true

case "${state}" in
    FINISHED)     echo "job ${jobname} FINISHED." ;;
    CLIENT_ERROR) echo "SQL client reported an error:"; grep -m1 -A3 "\[ERROR\]" "${log}"; exit 1 ;;
    *)            echo "job ${jobname} did not finish (state='${state:-timeout}'). Client log tail:"; tail -12 "${log}"; exit 1 ;;
esac
