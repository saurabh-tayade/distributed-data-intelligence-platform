"""Disposable test publication setup only. Never packaged or invoked by recovery code."""
from pathlib import Path
import sys
from reliability_preflight_fixtures import create
from test_reliability_execution import prepare, child

fixture = create(Path(sys.argv[1]), sys.argv[2])
process, result, _ = child(prepare(fixture), 'PUBLISH_RELIABILITY')
if process.returncode:
    raise SystemExit('RECOVERY_TEST_PUBLICATION_SETUP_FAILED')
print(fixture['bundle']['bindings']['output']['root'])
