"""Versioned fixed sidecar protocol; native execute/verification functions are unchanged."""
from hashlib import sha256
import json
import os
from pathlib import Path
import sys
sys.path.insert(0, str(Path(__file__).absolute().parent))
from publication_metadata import canonical_bytes
from bounded_evidence import Sink


def main():
    import artifact_import_verifier as p
    import silver_import_qualification as q
    os.umask(0o077)
    p.need(len(sys.argv) == 3 and sys.argv[1] in {'SILVER', 'GOLD', 'CONTENT', 'RECOVERY', 'RESUME'}, 'ADAPTER_INVALID')
    kind, path = sys.argv[1], Path(sys.argv[2])
    sink = Sink(path)  # No execution, even read-only verification, before the permit is readable.
    raw = sink.read_command(path)
    p.need(len(raw) <= 1024**2, 'RESOURCE_LIMIT')
    c = json.loads(raw)
    p.need(canonical_bytes(c) == raw, 'ADAPTER_INVALID')
    # Native RECHECK reads its retained evidence relative to this exact command.
    sys.argv = [sys.argv[0], str(path)]
    command = c.get('original', c.get('contentCommand') or c)
    if command.get('runtime', {}).get('scratchRoot'):
        os.chdir(command['runtime']['scratchRoot'])
    try:
        with q.quiet_stdout():
            if kind == 'SILVER':
                import managed_silver as native
            elif kind == 'GOLD':
                import managed_gold as native
            elif kind == 'CONTENT':
                import managed_content as native
            elif kind == 'RECOVERY':
                import content_recovery_verifier as native
            else:
                import content_resume_verifier as native
            value = native.execute(c)
        if kind == 'SILVER':
            if c['mode'] == 'VERIFY':
                sink.write('evidence.json', canonical_bytes(value['evidence']))
                from silver_inputs import discover, inventory
                from publication_metadata import fingerprint
                manifest = inventory(discover(c['binding']['root'], markers=[c['artifact']['relativeAddress']]))
                sink.write('selection.json', canonical_bytes(dict(manifest=manifest, inputSetFingerprint=fingerprint(manifest))))
            result = dict(protocolVersion=1, commandSha256=sha256(raw).hexdigest(), status=value['status'], cleanup=value.get('cleanup'), code=None)
        else:
            data = canonical_bytes(value)
            sink.write('evidence.json', data)
            result = dict(commandSha256=sha256(raw).hexdigest(), evidenceSha256=sha256(data).hexdigest(), status='VERIFIED' if kind in {'RECOVERY', 'RESUME'} else value['status'], cleanup='CONFIRMED', code=None)
        status = 0
    except Exception as error:
        if kind == 'SILVER':
            from bronze_to_silver import Failure
            code = getattr(error, 'code', None)
            if isinstance(error, Failure):
                code = 'BRONZE_UNAVAILABLE' if str(error) == 'OBJECT_READ_FAILED' else 'BRONZE_CHANGED' if str(error) == 'BRONZE_RECEIPT_CHANGED' else 'BRONZE_INVALID'
            if code not in p.CODES | q.CODES | {'RESOURCE_LIMIT', 'EVIDENCE_UNAVAILABLE'}:
                code = 'PROCESSING_FAILED'
            result = dict(protocolVersion=1, commandSha256=sha256(raw).hexdigest(), status='FAILED', cleanup=None, code=code)
        else:
            if kind in {'RECOVERY', 'RESUME'}:
                code = native.safe_code(error)
            else:
                code = getattr(error, 'code', 'INPUT_INVALID')
                from bronze_to_silver import Failure
                if isinstance(error, Failure):
                    code = 'BRONZE_CHANGED' if str(error) == 'BRONZE_RECEIPT_CHANGED' else 'BRONZE_UNAVAILABLE'
                import content_metadata as m
                if code not in p.CODES | set(m.MESSAGES) | {'SPARK_CLEANUP_UNCERTAIN', 'BRONZE_CHANGED', 'BRONZE_UNAVAILABLE', 'RESOURCE_LIMIT', 'EVIDENCE_UNAVAILABLE'}:
                    code = 'INPUT_INVALID'
            result = dict(commandSha256=sha256(raw).hexdigest(), evidenceSha256=None, status='FAILED', cleanup='UNCONFIRMED' if getattr(error, 'code', None) == 'SPARK_CLEANUP_UNCERTAIN' else 'CONFIRMED', code=code)
        status = 1
    sink.write('result.json', canonical_bytes(result))
    return status


if __name__ == '__main__':
    raise SystemExit(main())
