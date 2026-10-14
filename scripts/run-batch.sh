#!/usr/bin/env bash
set -euo pipefail
if (( $# > 1 )) || { (( $# == 1 )) && [[ ! "$1" =~ [^[:space:]] ]]; }; then
  printf 'Usage: bash scripts/run-batch.sh [catalog.json] (catalog path must not be blank)\n' >&2
  exit 2
fi
catalog_path="${1-config/rss-sources.json}"
cd "$(git rev-parse --show-toplevel)"
# Configuration is exported by the caller; this script never reads or prints secrets.
mvn -B -ntp -f ingestion/pom.xml package -DskipTests
exec java -cp 'ingestion/target/classes:ingestion/target/dependency/*' \
  platform.ingestion.batch.BatchIngestionCli "$catalog_path"
