#!/usr/bin/env bash
# Explicit opt-in only. Never invoked by ordinary startup or default CI.
set -euo pipefail
exec python3 -I -B "$(dirname "$0")/qualify-packaged-public-content.py" "$@"
