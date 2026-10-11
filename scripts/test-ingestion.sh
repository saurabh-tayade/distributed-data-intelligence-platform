#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"
if [[ "$(javac -version 2>&1)" != javac\ 21.* ]]; then
  printf 'Java 21 JDK is required; set PATH to its bin directory.\n' >&2
  exit 1
fi
classes=$(mktemp -d "${TMPDIR:-/tmp}/rss-tests.XXXXXX")
trap 'rm -rf "$classes"' EXIT
find ingestion/src/main/java ingestion/src/test/java -name '*.java' -print > "$classes/sources"
javac --release 21 --add-modules jdk.httpserver -Xlint:all -Werror -d "$classes" @"$classes/sources"
java --add-modules jdk.httpserver -cp "$classes" platform.ingestion.rss.HttpFeedFetcherTest
