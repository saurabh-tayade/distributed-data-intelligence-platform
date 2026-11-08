#!/usr/bin/env bash
# Separate opt-in public-network acceptance. Never sourced or invoked by default CI.
# Supply the reviewed fixed-commit URI, exact payload and packaged runtime hashes, and
# the single expected Atom entry title. This script does not publish or prefetch a fixture.
# PostgreSQL must be a disposable, exclusively reserved PostgreSQL 17 instance.
# Both successful and failed installation/database evidence remains for manual review.
set -euo pipefail
cd "$(dirname "$0")/.."
for key in DDIP_PUBLIC_FIXTURE_URI DDIP_PUBLIC_FIXTURE_SHA256 DDIP_PUBLIC_FIXTURE_TITLE \
  DDIP_PUBLIC_INGESTION_SHA256 DDIP_PUBLIC_PROVIDER_SHA256 DDIP_TEST_DB_URL \
  DDIP_TEST_DB_USER DDIP_TEST_DB_PASSWORD DDIP_TEST_PYTHON JAVA_HOME; do
  if [[ -z ${!key:-} ]]; then printf 'Required qualification input: %s\n' "$key" >&2; exit 2; fi
done
if [[ ! $DDIP_PUBLIC_FIXTURE_URI =~ ^https://raw\.githubusercontent\.com/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[0-9a-f]{40}/[A-Za-z0-9_./-]+$ ]]; then
  printf 'Fixture URI must be the exact reviewed raw.githubusercontent.com fixed-commit HTTPS URI.\n' >&2; exit 2
fi
for key in DDIP_PUBLIC_FIXTURE_SHA256 DDIP_PUBLIC_INGESTION_SHA256 DDIP_PUBLIC_PROVIDER_SHA256; do
  if [[ ! ${!key} =~ ^[0-9a-f]{64}$ ]]; then printf 'Invalid SHA-256 input: %s\n' "$key" >&2; exit 2; fi
done
if [[ $DDIP_PUBLIC_FIXTURE_URI == *'/../'* || $DDIP_PUBLIC_FIXTURE_URI == *'/./'* ]]; then
  printf 'Fixture URI must have canonical path segments.\n' >&2; exit 2
fi
for file in ingestion/target/ingestion-0.1.0-SNAPSHOT.jar ingestion/target/ingestion-0.1.0-SNAPSHOT-public-fetch-provider.jar; do
  [[ -f $file ]] || { printf 'Required frozen artifact is missing: %s\n' "$file" >&2; exit 2; }
done
[[ $(sha256sum ingestion/target/ingestion-0.1.0-SNAPSHOT.jar | cut -d' ' -f1) == "$DDIP_PUBLIC_INGESTION_SHA256" ]] || { echo 'Ingestion artifact digest mismatch.' >&2; exit 2; }
[[ $(sha256sum ingestion/target/ingestion-0.1.0-SNAPSHOT-public-fetch-provider.jar | cut -d' ' -f1) == "$DDIP_PUBLIC_PROVIDER_SHA256" ]] || { echo 'Public provider artifact digest mismatch.' >&2; exit 2; }
export PATH="$JAVA_HOME/bin:$PATH"
mkdir -p outputs
export DDIP_PUBLIC_QUALIFICATION_OUTPUT
DDIP_PUBLIC_QUALIFICATION_OUTPUT=$(mktemp -d "$PWD/outputs/public-content-qualification.XXXXXXXX")
printf 'Public acceptance evidence: %s\n' "$DDIP_PUBLIC_QUALIFICATION_OUTPUT"
printf 'This explicitly invokes public DNS/TLS/IPv4, preserves failed attempts, and performs no retry.\n'
# Only content-api is packaged. Do not replace already reviewed ingestion/provider artifacts.
mvn -q -f services/content-api/pom.xml -DskipUnitTests=true -DskipTests package > "$DDIP_PUBLIC_QUALIFICATION_OUTPUT/build.log" 2>&1
# A standalone main, not an unassigned JUnit test or a new Failsafe profile. It reuses
# only already-declared test dependencies and forks the actual production boot JAR.
mvn -q -f services/content-api/pom.xml org.apache.maven.plugins:maven-dependency-plugin:3.11.0:build-classpath \
  -DincludeScope=test -Dmdep.outputFile="$DDIP_PUBLIC_QUALIFICATION_OUTPUT/dependency-classpath.txt" \
  >> "$DDIP_PUBLIC_QUALIFICATION_OUTPUT/build.log" 2>&1
classpath="$PWD/services/content-api/target/test-classes:$PWD/services/content-api/target/classes:$(cat "$DDIP_PUBLIC_QUALIFICATION_OUTPUT/dependency-classpath.txt")"
if (cd services/content-api && "$JAVA_HOME/bin/java" -cp "$classpath" platform.contentapi.management.OrdinaryPublicContentQualification) > "$DDIP_PUBLIC_QUALIFICATION_OUTPUT/qualification.log" 2>&1; then
  printf 'Public packaged acceptance passed; review retained qualification.json and logs.\n'
else
  code=$?; printf 'Public acceptance FAILED. Preserve the database and files listed in qualification.json; do not promote an unavailable endpoint to success.\n' >&2
  exit "$code"
fi
