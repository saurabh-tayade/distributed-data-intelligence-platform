"""Test-only pure plan probe. This is not a child bootstrap or production entry point."""
import base64
import json
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import narrow_evidence_sink as sink
from narrow_transport_json import canonical_envelope

for line in sys.stdin:
    value = json.loads(line)
    frozen = sink.Frozen('a' * 64, 'b' * 64, 'c' * 64)
    installation = '00000000-0000-4000-8000-000000000001'
    worker = '00000000-0000-4000-8000-000000000002'
    writer = sink.Writer(installation, worker, 1, value['runId'], value['attemptId'], 'VERIFY_SILVER', 0,
                         sink.checkpoint_id(value['runId'], value['attemptId'], 'VERIFY_SILVER', 0), 'd' * 64)
    expected = sink.Expected(installation, sink.POLICY, 'ORIGINAL', value['runId'], value['attemptId'], value['sourceId'],
                             Path('/unused-test-control'), sink.Identity(1, 1),
                             sink.Identity(int(value['rootDevice']), int(value['rootInode'])),
                             sink.Identity(1, 2), 'e' * 64, frozen, writer)
    sink.validate_expectations(expected, sink.Current(sink.POLICY, frozen, writer))
    plan = sink._plan(expected)
    sink._limits(plan)
    print(base64.b64encode(canonical_envelope(plan, sink.LEDGER_LIMIT)).decode('ascii'))
