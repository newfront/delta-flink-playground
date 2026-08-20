#!/usr/bin/env bash
# Stage the delta-flink connector + storage jars into the Flink dist and render
# core-site.xml, then hand off to the standard Flink entrypoint (or idle, for the
# SQL-client container). Mirrors delta-io/delta flink/docker/2.0/usrlib/init.sh.
set -euo pipefail

USRLIB=/opt/flink/usrlib
CONF=/opt/flink/conf

# Copy every jar staged in usrlib (delta-flink fat jar, AWS SDK bundle, guava,
# and optionally the SQL kafka connector) onto the Flink classpath.
if ls "$USRLIB"/*.jar >/dev/null 2>&1; then
  echo "[init] staging jars into /opt/flink/lib:"
  for j in "$USRLIB"/*.jar; do echo "  - $(basename "$j")"; done
  cp -f "$USRLIB"/*.jar /opt/flink/lib/
else
  echo "[init] WARNING: no jars in $USRLIB. Run 'just assembly' and 'just bundle-jars' first." >&2
fi

# Render core-site.xml (S3A endpoint for the RustFS object store). The template
# carries an @S3_ENDPOINT_URL@ placeholder so the same file works whether RustFS
# is reached via host.docker.internal (default) or a shared-network hostname.
if [ -f "$USRLIB/core-site.template.xml" ]; then
  sed -e "s#@S3_ENDPOINT_URL@#${S3_ENDPOINT_URL:-http://host.docker.internal:9000}#g" \
      -e "s#@S3_ACCESS_KEY@#${S3_ACCESS_KEY:-rustfsadmin}#g" \
      -e "s#@S3_SECRET_KEY@#${S3_SECRET_KEY:-rustfsadmin}#g" \
    "$USRLIB/core-site.template.xml" > "$CONF/core-site.xml"
  echo "[init] rendered core-site.xml (fs.s3a.endpoint=${S3_ENDPOINT_URL:-http://host.docker.internal:9000})"
fi

# The SQL-client container has no server role: stage jars, then idle so we can
# `docker compose exec` sql-client.sh into it.
if [ "${1:-}" = "idle" ]; then
  echo "[init] usrlib staged; idling (sql-client). Run demos with: just sql <file>"
  exec tail -f /dev/null
fi

exec /docker-entrypoint.sh "$@"
