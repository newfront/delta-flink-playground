#!/usr/bin/env bash
# Build the delta-flink connector fat jar from a local delta-io/delta clone and
# stage it into flink/usrlib/.
#
#   build/sbt -DflinkVersion=$FLINK_BUILD_VERSION flink/assembly
#
# Java builds route through the corporate Maven mirror automatically: Delta's
# build/sbt consumes $MAVEN_PROXY_URL and sets -Dsbt.override.build.repos=true,
# so every Coursier/Ivy fetch (and the sbt-launch bootstrap) uses the proxy. We
# inherit MAVEN_PROXY_URL from the shell if it is exported there, otherwise from
# .env, otherwise Maven Central.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

# Capture a shell-exported MAVEN_PROXY_URL before sourcing .env, so the shell
# value takes precedence over the .env fallback.
MAVEN_PROXY_URL_SHELL="${MAVEN_PROXY_URL:-}"

# Load .env for defaults (shell-exported vars still win — see below).
if [ -f .env ]; then
  set -a; . ./.env; set +a
fi

FLINK_BUILD_VERSION="${FLINK_BUILD_VERSION:-2.0.2}"
DELTA_REPO_RAW="${DELTA_REPO:-../../oss/delta-io/delta}"

# Resolve DELTA_REPO relative to this repo's root when it is not absolute.
case "$DELTA_REPO_RAW" in
  /*) DELTA_REPO="$DELTA_REPO_RAW" ;;
  *)  DELTA_REPO="$(cd "$REPO_ROOT/$DELTA_REPO_RAW" 2>/dev/null && pwd || true)" ;;
esac

if [ -z "${DELTA_REPO:-}" ] || [ ! -f "$DELTA_REPO/build/sbt" ]; then
  echo "ERROR: DELTA_REPO does not look like a delta-io/delta clone: '${DELTA_REPO_RAW}'" >&2
  echo "       Set DELTA_REPO in .env to the path of your delta clone." >&2
  exit 1
fi

# A shell-exported MAVEN_PROXY_URL wins over the .env value. `set -a` above would
# have imported the .env value; only override it back if the shell had its own.
if [ -n "${MAVEN_PROXY_URL_SHELL:-}" ]; then
  export MAVEN_PROXY_URL="$MAVEN_PROXY_URL_SHELL"
fi

echo "==> delta repo:        $DELTA_REPO"
echo "==> flinkVersion:      $FLINK_BUILD_VERSION"
echo "==> MAVEN_PROXY_URL:   ${MAVEN_PROXY_URL:-<unset> (Maven Central)}"
echo

( cd "$DELTA_REPO" && ./build/sbt -batch -DflinkVersion="$FLINK_BUILD_VERSION" "flink/assembly" )

# The assembly jar is named delta-flink-<flinkVersion>-<deltaVersion>.jar.
JAR="$(ls -t "$DELTA_REPO"/flink/target/delta-flink-"$FLINK_BUILD_VERSION"-*.jar 2>/dev/null | head -n1 || true)"
if [ -z "$JAR" ]; then
  echo "ERROR: could not find the assembled jar under $DELTA_REPO/flink/target/" >&2
  echo "       (expected delta-flink-${FLINK_BUILD_VERSION}-*.jar)" >&2
  exit 1
fi

mkdir -p "$REPO_ROOT/flink/usrlib"
# Drop any previously staged connector jar so only one version is on the classpath.
rm -f "$REPO_ROOT"/flink/usrlib/delta-flink-*.jar
cp -v "$JAR" "$REPO_ROOT/flink/usrlib/"

echo
echo "Staged connector jar into flink/usrlib/:"
ls -1 "$REPO_ROOT"/flink/usrlib/delta-flink-*.jar | sed 's#^#  #'
echo
echo "Next: 'just bundle-jars' (AWS SDK bundle + guava), then 'just up'."
