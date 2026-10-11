#!/usr/bin/env bash
set -euo pipefail

cd "$(git rev-parse --show-toplevel)"

# Catch force-added secrets, datasets, and build outputs covered by .gitignore.
ignored=$(git ls-files --cached --ignored --exclude-standard)
if [[ -n "$ignored" ]]; then
  printf 'Tracked files match ignore rules; remove them from Git:\n%s\n' "$ignored" >&2
  exit 1
fi

# Compare the whole tracked working tree, not just the latest commit, so this
# also works in a shallow CI checkout. Git checks whitespace and conflict markers.
empty_tree=$(git hash-object -t tree /dev/null)
git -c core.whitespace=blank-at-eol,blank-at-eof,space-before-tab diff --check "$empty_tree" --

# Include new, non-ignored files locally before they have been staged.
while IFS= read -r -d '' file; do
  status=0
  git -c core.whitespace=blank-at-eol,blank-at-eof,space-before-tab diff --no-index --check -- /dev/null "$file" || status=$?
  # --no-index returns 1 for a clean difference, 2 for check errors.
  if (( status > 1 )); then
    exit "$status"
  fi
done < <(git ls-files --others --exclude-standard -z)

printf 'Repository hygiene checks passed.\n'
