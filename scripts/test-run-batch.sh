#!/usr/bin/env bash
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

# Stub both external commands: these tests never build or launch ingestion.
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT
mkdir "$test_dir/bin"
export BATCH_SCRIPT_TEST_LOG="$test_dir/calls"
cat > "$test_dir/bin/mvn" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' mvn "$@" >> "$BATCH_SCRIPT_TEST_LOG"
EOF
cat > "$test_dir/bin/java" <<'EOF'
#!/usr/bin/env bash
printf '%s\n' java "$@" >> "$BATCH_SCRIPT_TEST_LOG"
EOF
chmod +x "$test_dir/bin/mvn" "$test_dir/bin/java"
export PATH="$test_dir/bin:$PATH"

check_valid() {
  local expected_path=$1
  shift
  : > "$BATCH_SCRIPT_TEST_LOG"
  bash scripts/run-batch.sh "$@"
  printf '%s\n' mvn -B -ntp -f ingestion/pom.xml package -DskipTests \
    java -cp 'ingestion/target/classes:ingestion/target/dependency/*' \
    platform.ingestion.batch.BatchIngestionCli "$expected_path" > "$test_dir/expected"
  cmp "$test_dir/expected" "$BATCH_SCRIPT_TEST_LOG"
}

check_invalid() {
  local status=0
  : > "$BATCH_SCRIPT_TEST_LOG"
  bash scripts/run-batch.sh "$@" > "$test_dir/stdout" 2> "$test_dir/stderr" || status=$?
  if [[ "$status" != 2 || -s "$BATCH_SCRIPT_TEST_LOG" || -s "$test_dir/stdout" ]]; then
    printf 'Invalid arguments must exit 2 without launching Maven or Java.\n' >&2
    exit 1
  fi
  if [[ "$(< "$test_dir/stderr")" != Usage:* ]]; then
    printf 'Invalid arguments must print usage to stderr.\n' >&2
    exit 1
  fi
}

check_valid config/rss-sources.json
check_valid 'config/custom catalog.json' 'config/custom catalog.json'
check_invalid ''
check_invalid $' \t\r\n '
check_invalid config/first.json config/second.json
printf 'Batch launch script: all 5 argument regression cases passed.\n'
