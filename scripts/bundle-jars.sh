#!/usr/bin/env bash
# Download the extra runtime jars the delta-flink assembly deliberately excludes
# (the AWS SDK bundle must be supplied at runtime) into flink/usrlib/.
#
#   - software.amazon.awssdk:bundle       (S3/STS SDK for the Kernel engine)
#   - com.google.guava:guava              (required alongside the bundle)
#   - org.apache.flink:flink-sql-connector-kafka  (only with: bundle-jars.sh kafka)
#
# Artifacts are fetched from ${MAVEN_PROXY_URL:-Maven Central}. A shell-exported
# MAVEN_PROXY_URL wins over the .env fallback.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MAVEN_PROXY_URL_SHELL="${MAVEN_PROXY_URL:-}"
if [ -f .env ]; then set -a; . ./.env; set +a; fi
if [ -n "$MAVEN_PROXY_URL_SHELL" ]; then MAVEN_PROXY_URL="$MAVEN_PROXY_URL_SHELL"; fi

BASE="${MAVEN_PROXY_URL:-https://repo1.maven.org/maven2}"
BASE="${BASE%/}"   # strip trailing slash
USRLIB="$REPO_ROOT/flink/usrlib"
mkdir -p "$USRLIB"

AWS_SDK_BUNDLE_VERSION="${AWS_SDK_BUNDLE_VERSION:-2.23.19}"
GUAVA_VERSION="${GUAVA_VERSION:-33.5.0-jre}"

# fetch <group-path> <artifact> <version>  -> downloads <artifact>-<version>.jar
fetch() {
  local group_path="$1" artifact="$2" version="$3"
  local jar="${artifact}-${version}.jar"
  local url="${BASE}/${group_path}/${artifact}/${version}/${jar}"
  if [ -f "$USRLIB/$jar" ]; then
    echo "  = $jar (already present)"
    return 0
  fi
  echo "  + $jar"
  echo "      <- $url"
  curl -fsSL "$url" -o "$USRLIB/$jar.tmp"
  mv "$USRLIB/$jar.tmp" "$USRLIB/$jar"
}

echo "==> downloading runtime jars from: $BASE"
fetch "software/amazon/awssdk" "bundle" "$AWS_SDK_BUNDLE_VERSION"
fetch "com/google/guava" "guava" "$GUAVA_VERSION"

if [ "${1:-}" = "kafka" ]; then
  # FLINK_SQL_KAFKA_CONNECTOR is "group:artifact:version"; must match the Flink
  # minor line. Override in .env if the default coordinate is unavailable.
  COORD="${FLINK_SQL_KAFKA_CONNECTOR:-org.apache.flink:flink-sql-connector-kafka:4.0.1-2.0}"
  IFS=':' read -r g a v <<< "$COORD"
  fetch "$(echo "$g" | tr '.' '/')" "$a" "$v"
fi

echo
echo "flink/usrlib/ now contains:"
ls -1 "$USRLIB"/*.jar 2>/dev/null | sed 's#^#  #' || echo "  (no jars yet)"
