#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
if [[ "$(javac -version 2>&1)" != javac\ 21.* ]]; then
  printf 'Java 21 JDK is required; set PATH to its bin directory.\n' >&2
  exit 1
fi
if ! command -v mvn >/dev/null 2>&1; then
  printf 'Maven 3.9.16 or compatible Maven 3.9+ is required on PATH.\n' >&2
  exit 1
fi
exec mvn -B -ntp -f ingestion/pom.xml test "$@"
