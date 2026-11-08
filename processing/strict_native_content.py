"""Inactive PID-bound, one-shot ORIGINAL ledger-v2 native candidate child.

No argv command path, ancestor discovery, recovery, dispatcher or SQL authority.
The independent parent supplies compact frozen facts, retains actual command bytes
and independently verifies every candidate before granting any successor.
"""
from hashlib import sha256
import os
from pathlib import Path
import sys

import narrow_evidence_sink as ledger
import narrow_startup_probe as startup
from narrow_transport_json import canonical_envelope, parse_envelope
from strict_native_sink import EffectSink
from strict_native_runtime import Cleanup, check_runtime, installed_versions, spark_session

PROTOCOL_MANIFEST = dict(strictContentRuntimeVersion=1, adapter='strict_native_content',
                        bootstrapVersion=2, commandProtocolVersion=2, ledgerVersion=2)
STRICT_RUNTIME_SHA256 = sha256(canonical_envelope(PROTOCOL_MANIFEST, 65535)).hexdigest()
ERROR = 'STRICT_CONTENT_REJECTED'
# Complete local import closure, including lazy native reader/importer branches.
# Historical runtime-v2 manifests are deliberately untouched.
MODULES = tuple(name + '.py' for name in (
    'artifact_import_verifier', 'bronze_to_silver', 'content_analytics', 'content_dependencies',
    'content_metadata', 'content_processing', 'content_publication', 'gold', 'gold_analytics',
    'gold_publication', 'gold_reader', 'managed_content', 'managed_gold', 'managed_silver',
    'narrow_content_profile', 'narrow_content_publication_v2', 'narrow_content_reports',
    'narrow_content_reports_v2', 'narrow_evidence_sink', 'narrow_gold_publication_v2',
    'narrow_projection_transport', 'narrow_publication_lifecycle_v2', 'narrow_publication_projection_v2',
    'narrow_silver_precompletion', 'narrow_startup_probe', 'narrow_transport_json',
    'publication_metadata', 'publisher_reference', 'reliability_format_inputs',
    'reliability_format_metadata', 'reliability_format_parquet', 'reliability_inputs',
    'reliability_metadata', 'service_reference', 'silver', 'silver_import_qualification',
    'silver_inputs', 'strict_native_content', 'strict_native_runtime', 'strict_native_sink',
))


def runtime_probe():
    """Import the registered closure, including lazy dependencies, before FETCH."""
    import importlib
    versions = installed_versions()
    for name in MODULES:
        importlib.import_module(name.removesuffix('.py'))
    return versions
_EXTRA = {'family', 'nativeMode', 'nativeRuntimeSha256', 'strictRuntimeSha256', 'expectations'}
_SILVER_FIELDS = set('protocolVersion mode runId attemptId checkpointId ownerEpoch runtimeSha256 '
                     'receipt artifact binding bronze runtime expectedCapture'.split())
_BRONZE_FIELDS = set('protocolVersion mode runId attemptId checkpointId ownerEpoch runtimeSha256 receipt bronze'.split())
_SILVER_EXPECTATIONS = set('receiptSha256 artifactSha256 bindingSha256 bronzeSha256 runtimeSha256 '
                          'credentialsSha256 expectedCaptureSha256'.split())
_PROJECTION_EXPECTATIONS = set('contentRuntime goldRuntime silverRuntime bindings silverInputSha256 '
                              'referenceSha256 goldProofSha256'.split())
_OPERATIONS = {
    ('SILVER', 'PUBLISH'): ('SILVER', 0, 'SILVER', 0),
    ('SILVER', 'VERIFY'): ('VERIFY_SILVER', 0, 'SILVER', 0),
    ('SILVER', 'RECHECK'): ('RECHECK_SILVER', 0, 'SILVER', 0),
    ('GOLD', 'PREPARE'): ('PREPARE_GOLD', 0, 'PREPARE_GOLD', 0),
    ('GOLD', 'PUBLISH'): ('PUBLISH_GOLD', 0, 'PUBLISH_GOLD', 0),
    ('GOLD', 'VERIFY'): ('VERIFY_RESULT', 0, 'PUBLISH_GOLD', 0),
    ('GOLD', 'RECHECK'): ('CHECK_INPUTS', 0, 'PUBLISH_GOLD', 0),
    ('CONTENT', 'PREPARE'): ('CHECK_INPUTS', 1, 'CHECK_INPUTS', 1),
    ('CONTENT', 'PUBLISH'): ('PUBLISH_CONTENT', 0, 'PUBLISH_CONTENT', 0),
    ('CONTENT', 'VERIFY'): ('VERIFY_RESULT', 1, 'VERIFY_RESULT', 1),
    ('CONTENT', 'RECHECK'): ('CHECK_INPUTS', 2, 'VERIFY_RESULT', 1),
}


def _need(value, code='INPUT_INVALID'):
    ledger._need(value, code)


def _digest(value):
    return sha256(canonical_envelope(value, 1024**2)).hexdigest()


def _fields(value, names):
    _need(type(value) is dict and set(value) == set(names))


def parse_bootstrap(raw):
    """Pure shape, type, PID and operation correspondence before any path IO."""
    value = parse_envelope(raw, startup.BOOTSTRAP_LIMIT - 1, large=False)
    _fields(value, startup._FIELDS | _EXTRA)
    _need(type(value['bootstrapVersion']) is int and value['bootstrapVersion'] == 2
          and value['mode'] == 'STRICT_CONTENT')
    ledger._hash(value['nativeRuntimeSha256'])
    _need(value['strictRuntimeSha256'] == STRICT_RUNTIME_SHA256)
    family, mode = value['family'], value['nativeMode']
    if (family, mode) == ('BRONZE', 'INSPECT'):
        _need(value['stage'] == 'INSPECT_BRONZE' and type(value['ordinal']) is int
              and 0 <= value['ordinal'] < 5)
    else:
        _need((family, mode) in _OPERATIONS)
        stage, ordinal, _, _ = _OPERATIONS[family, mode]
        _need(value['stage'] == stage and type(value['ordinal']) is int and value['ordinal'] == ordinal)
    # Reuse the exact lexical/PID/Expected validator, without changing its probe contract.
    probe = {key: value[key] for key in startup._FIELDS}
    actual_stage, actual_ordinal, actual_checkpoint = probe['stage'], probe['ordinal'], probe['checkpointId']
    probe.update(bootstrapVersion=1, mode='READ_ONLY_PROBE', stage='CHECK_INPUTS', ordinal=0,
        checkpointId=ledger.checkpoint_id(value['runId'], value['attemptId'], 'CHECK_INPUTS', 0))
    expected, current = startup.parse_bootstrap(canonical_envelope(probe, 65535, large=False))
    from dataclasses import replace
    writer = replace(expected.writer, stage=actual_stage, ordinal=actual_ordinal, checkpoint_id=actual_checkpoint)
    current_writer = replace(current.writer, stage=actual_stage, ordinal=actual_ordinal, checkpoint_id=actual_checkpoint)
    expected, current = replace(expected, writer=writer), replace(current, writer=current_writer)
    ledger.validate_expectations(expected, current)
    expectations = value['expectations']
    if family == 'SILVER':
        _fields(expectations, _SILVER_EXPECTATIONS)
        for digest in expectations.values():
            ledger._hash(digest)
    elif family == 'BRONZE':
        _fields(expectations, {'receiptSha256', 'bronzeSha256', 'credentialsSha256'})
        for digest in expectations.values():
            ledger._hash(digest)
    else:
        _fields(expectations, _PROJECTION_EXPECTATIONS)
        for key in ('silverInputSha256', 'referenceSha256', 'goldProofSha256'):
            if family == 'GOLD' and key != 'silverInputSha256':
                _need(expectations[key] is None)
            else:
                ledger._hash(expectations[key])
    return value, expected, current


def validate_command(raw, bootstrap):
    """Reconstruct large expectations only after their independent digest matches."""
    command = parse_envelope(raw, 1024**2)
    family, mode = bootstrap['family'], bootstrap['nativeMode']
    _need(type(command['protocolVersion']) is int and command['protocolVersion'] == 2
          and command['mode'] == mode)
    for key in ('runId', 'attemptId'):
        _need(command[key] == bootstrap[key])
    _need(type(command['ownerEpoch']) is int and command['ownerEpoch'] == int(bootstrap['ownerEpoch'])
          and command['runtimeSha256'] == bootstrap['nativeRuntimeSha256'])
    if family == 'BRONZE':
        origin, ordinal = 'INSPECT_BRONZE', bootstrap['ordinal']
    else:
        _, _, origin, ordinal = _OPERATIONS[family, mode]
    _need(command['checkpointId'] == ledger.checkpoint_id(bootstrap['runId'], bootstrap['attemptId'], origin, ordinal))
    compact = bootstrap['expectations']
    if family in {'SILVER', 'BRONZE'}:
        _fields(command, _SILVER_FIELDS if family == 'SILVER' else _BRONZE_FIELDS)
        for key in ('receipt', 'bronze', *(('artifact', 'binding', 'runtime', 'expectedCapture') if family == 'SILVER' else ())):
            _need(_digest(command[key]) == compact[key + 'Sha256'], 'INPUT_CHANGED')
        receipt = command['receipt']
        import managed_silver
        _need(receipt['runId'] == command['runId'] and receipt['attemptId'] == command['attemptId']
              and receipt['sourceId'] == bootstrap['sourceId'])
        if family == 'SILVER':
            _need(managed_silver.selection(receipt, command['artifact']['storageBindingId']) == command['artifact'])
            _need((command['expectedCapture'] is not None) == (mode == 'RECHECK'))
        else:
            # Validate every receipt field with the native selection validator.
            managed_silver.selection(receipt, 'strict-bronze-inspection')
        return command, None, None
    import narrow_projection_transport as transport
    gold = command if family == 'GOLD' else command['goldProof']['command']
    _need(type(gold['inputs']) is list and len(gold['inputs']) == 1)
    silver_input = gold['inputs'][0]
    _need(_digest(silver_input) == compact['silverInputSha256'], 'INPUT_CHANGED')
    reference = proof = None
    if family == 'CONTENT':
        reference, proof = command['reference'], command['goldProof']
        _need(_digest(reference) == compact['referenceSha256'] and
              _digest(proof) == compact['goldProofSha256'], 'INPUT_CHANGED')
    expectations = {key: compact[key] for key in ('contentRuntime', 'goldRuntime', 'silverRuntime', 'bindings')}
    expectations.update(policy=bootstrap['policy'], readyContentRuntimeSha256=bootstrap['runtimeSha256'],
        original=dict(runId=bootstrap['runId'], attemptId=bootstrap['attemptId'],
                      sourceId=bootstrap['sourceId'], silverInput=silver_input), reference=reference, goldProof=proof)
    bound = transport.read_command_v2(raw, expectations=expectations)
    _need(bound.kind == family)
    return command, bound, expectations


class CredentialPin:
    def __init__(self, configuration, digest):
        self.path = Path(configuration['credentialsFile'])
        self.raw, self.stamp = ledger._read(self.path, 8192)
        _need(sha256(self.raw).hexdigest() == digest, 'INPUT_CHANGED')
        self.value = parse_envelope(self.raw, 8192)
        _fields(self.value, {'accessKey', 'secretKey'})
        _need(all(type(value) is str and value for value in self.value.values()))

    def check(self):
        raw, stamp = ledger._read(self.path, 8192)
        _need(raw == self.raw and stamp == self.stamp, 'INPUT_CHANGED')


def _client(config, credentials):
    from bronze_to_silver import local_client
    _fields(config, {'importBronzeVersion', 'endpoint', 'region', 'bucket', 'credentialsFile'})
    _need(type(config['importBronzeVersion']) is int and config['importBronzeVersion'] == 1)
    env = {'BRONZE_S3_' + key: value for key, value in dict(ENDPOINT=config['endpoint'], REGION=config['region'],
        BUCKET=config['bucket'], ACCESS_KEY=credentials.value['accessKey'], SECRET_KEY=credentials.value['secretKey']).items()}
    return local_client(env)


def _silver(command, bootstrap, credential, cleanup):
    import artifact_import_verifier as p
    import managed_silver as native
    import narrow_silver_precompletion as narrow
    from pyspark import SparkContext
    client, bucket = _client(command['bronze'], credential)
    try:
        _need(bucket == command['receipt']['bucket'])
        exact = native.bronze(client, command['receipt'])
        credential.check()
        if command['mode'] == 'PUBLISH':
            with spark_session(command['runtime'], cleanup) as spark:
                value = narrow.process_one_v1(spark, exact, bucket, command['receipt']['expected']['metadataKey'],
                    command['binding']['root'], artifact=command['artifact'], binding=command['binding'])
                _need(value['status'] == 'PROCESSED', 'INPUT_CHANGED')
            evidence = selection = None
            status = value['status']
        else:
            request = dict(protocolVersion=2, operationId=bootstrap['operationId'],
                selectionSha256=_digest(command['artifact']), mode=command['mode'], artifact=command['artifact'],
                binding=command['binding'], bronze={key: command['bronze'][key] for key in ('endpoint', 'region', 'bucket')} | credential.value,
                runtime=command['runtime'], expectedCapture=command['expectedCapture'])
            if command['mode'] == 'VERIFY':
                _need(SparkContext._gateway is None and SparkContext._active_spark_context is None, 'RUNTIME_INCOMPATIBLE')
                # A failed gateway startup with no observable process is not
                # evidence that Spark never started or that cleanup succeeded.
                cleanup.status = 'UNCONFIRMED'
            try:
                status, evidence = narrow.reconcile_v1(request)
            finally:
                if command['mode'] == 'VERIFY':
                    cleanup.observe(SparkContext._gateway)
            selection = None
            if evidence is not None:
                from silver_inputs import discover, inventory
                from publication_metadata import fingerprint
                manifest = inventory(discover(command['binding']['root'], markers=[command['artifact']['relativeAddress']]))
                selection = dict(manifest=manifest, inputSetFingerprint=fingerprint(manifest))
                p.need(evidence['native']['sparkShutdown'] == 'CONFIRMED' and cleanup.status == 'CONFIRMED',
                       'SPARK_CLEANUP_UNCERTAIN')
        native.bronze(client, command['receipt'])
        return status, evidence, selection
    finally:
        client.close()


def _gold_prepare(spark, command):
    import managed_gold as native
    import narrow_publication_lifecycle_v2 as life
    from silver_inputs import load_inputs
    life.roots(command)
    markers = native.silver_check(command, spark)
    inputs = load_inputs(spark, command['silver']['root'], markers=markers)
    prepared = dict(manifest=inputs.manifest, inputSetFingerprint=inputs.input_set_fingerprint, counts=inputs.counts)
    if command['prepared'] is not None:
        life.equal(prepared, command['prepared'])
    inputs.verify_unchanged()
    native.silver_check(command)
    life.roots(command)
    return dict(status='PREPARED', prepared=prepared)


def _content_prepare(bound, expectations):
    import artifact_import_verifier as p
    import narrow_content_publication_v2 as native
    import narrow_gold_publication_v2 as gold
    import narrow_publication_lifecycle_v2 as life
    from content_dependencies import verify_dependencies
    command = bound.value()
    life.roots(command)
    gold.recheck_publication_v2(native._gold_command(command, expectations),
        command['goldProof']['artifact']['verification'], expectations=expectations)
    native._reference(command)
    selected = command['reference']['qualification']['selection']
    dep = verify_dependencies(gold_root=command['goldProof']['command']['gold']['root'],
        gold_fingerprint=command['goldProof']['artifact']['nativeFingerprint'], reference_root=command['referenceBinding']['root'],
        reference_id=selected['identity']['referenceId'], revision=selected['identity']['revision'],
        reference_fingerprint=selected['fingerprint'], output_root=command['content']['root'])
    prepared = dict(manifest=p.thaw(dep.manifest()), fingerprint=dep.fingerprint)
    if command['prepared'] is not None:
        life.equal(prepared, command['prepared'])
    native._check_dependencies(command, dep, expectations)
    return dict(status='PREPARED', prepared=prepared)


def execute_native(command, bootstrap, bound, expectations, credential, cleanup):
    family, mode = bootstrap['family'], bootstrap['nativeMode']
    if family == 'BRONZE':
        import managed_silver
        client, bucket = _client(command['bronze'], credential)
        try:
            _need(bucket == command['receipt']['bucket'])
            managed_silver.bronze(client, command['receipt'])
        finally:
            client.close()
        return 'VERIFIED', dict(status='VERIFIED', receipt=command['receipt']), None
    if family == 'SILVER':
        return _silver(command, bootstrap, credential, cleanup)
    import narrow_gold_publication_v2 as gold
    import narrow_content_publication_v2 as content
    import narrow_projection_transport as transport
    import narrow_publication_lifecycle_v2 as life
    # The versioned libraries consume publication contexts even for their
    # explicitly read-only entry points. This is not a legacy upgrade or effect
    # authorization: the actual command/mode/slot were already independently
    # validated, and read-only modes never select publish_v2.
    library_bound = bound
    if mode in {'VERIFY', 'RECHECK'}:
        library_bound = transport.read_command_v2(canonical_envelope(command | dict(mode='PUBLISH', verified=None),
            transport.STAGE_LIMIT), expectations=expectations)
    if family == 'GOLD' and mode == 'RECHECK':
        gold.recheck_publication_v2(library_bound, command['verified'], expectations=expectations)
        return 'UNCHANGED', dict(status='UNCHANGED'), None
    if family == 'CONTENT' and mode == 'PREPARE':
        value = _content_prepare(bound, expectations)
    else:
        # Content RECHECK deliberately repeats full native Gold/Spark and all-row
        # Content verification rather than trusting smoke counts or old captures.
        with spark_session(command['runtime'], cleanup) as spark:
            if family == 'GOLD' and mode == 'PREPARE':
                value = _gold_prepare(spark, command)
            else:
                native = gold if family == 'GOLD' else content
                action = native.publish_v2 if mode == 'PUBLISH' else native.verify_publication_v2
                value = action(spark, library_bound, expectations=expectations,
                               **({'require_fresh': True} if mode == 'PUBLISH' else {}))
                if command['verified'] is not None:
                    life.equal(value, command['verified'])
    return value['status'], value, None


def _safe_code(error):
    import artifact_import_verifier as p
    import content_metadata as m
    import silver_import_qualification as q
    from bronze_to_silver import Failure
    if isinstance(error, Failure):
        return 'BRONZE_CHANGED' if str(error) == 'BRONZE_RECEIPT_CHANGED' else 'BRONZE_UNAVAILABLE'
    code = getattr(error, 'code', None)
    allowed = p.CODES | q.CODES | set(m.MESSAGES) | {'INPUT_INVALID', 'SPARK_CLEANUP_UNCERTAIN', 'CONTENT_TRANSPORT_INVALID',
                                                 'BRONZE_CHANGED', 'BRONZE_UNAVAILABLE', 'PROCESSING_FAILED'}
    return code if code in allowed else 'PROCESSING_FAILED'


def run(fd):
    bootstrap, expected, current = parse_bootstrap(startup.receive_bootstrap(fd))
    sink = EffectSink(expected, current)
    try:
        raw = sink.read_command()
        command, bound, expectations = validate_command(raw, bootstrap)
        installed_versions()
        if bootstrap['family'] in {'SILVER', 'BRONZE'}:
            config, digest = command['bronze'], bootstrap['expectations']['credentialsSha256']
        else:
            item = (command if bootstrap['family'] == 'GOLD' else command['goldProof']['command'])['inputs'][0]
            config, digest = item['bronze'], item['credentialsSha256']
        credential = CredentialPin(config, digest)
        if 'runtime' in command:
            check_runtime(command['runtime'])
        cleanup = Cleanup()
        result = dict(protocolVersion=2, candidateOnly=True, family=bootstrap['family'], mode=bootstrap['nativeMode'],
            runId=bootstrap['runId'], attemptId=bootstrap['attemptId'], ownerEpoch=command['ownerEpoch'],
            checkpointId=bootstrap['checkpointId'], nativeCheckpointId=command['checkpointId'],
            runtimeSha256=bootstrap['nativeRuntimeSha256'], strictRuntimeSha256=STRICT_RUNTIME_SHA256,
            commandSha256=sha256(raw).hexdigest(), status='FAILED', evidenceSha256=None, selectionSha256=None,
            cleanup=cleanup.value(), code=None)
        # Qualify the actual expanded result shape before any native effect.
        canonical_envelope(result | dict(status='UNCHANGED', evidenceSha256='f' * 64,
            selectionSha256='f' * 64, cleanup=dict(status='UNCONFIRMED', gatewayPid='9' * 20),
            code='SPARK_CLEANUP_UNCERTAIN'), 65535, large=False)
        sink.claim()
        try:
            import silver_import_qualification as q
            credential.check()
            sink.inspect()
            with q.quiet_stdout():
                status, evidence, selection = execute_native(command, bootstrap, bound, expectations, credential, cleanup)
            credential.check()
            sink.inspect()
            payloads = {}
            for name, value in (('evidence', evidence), ('selection', selection)):
                if value is not None:
                    payloads[name] = canonical_envelope(value, ledger._slots(expected.writer.stage)[name + '.json'])
                    result[name + 'Sha256'] = sha256(payloads[name]).hexdigest()
            result.update(status=status, cleanup=cleanup.value())
            final = canonical_envelope(result, 65535, large=False)
            for name, data in payloads.items():
                sink.write(name + '.json', data)
            exit_code = 0
        except Exception as error:
            result.update(status='FAILED', evidenceSha256=None, selectionSha256=None,
                          cleanup=cleanup.value(), code=_safe_code(error))
            final = canonical_envelope(result, 65535, large=False)
            exit_code = 1
        # Finalization is itself one-shot. A failed/lost link acknowledgement is
        # not caught and retried as a second result publication.
        sink.finish(final)
        return exit_code
    finally:
        sink.close()


def main():
    os.umask(0o077)
    try:
        _need(len(sys.argv) == 1)
        return run(sys.stdin.fileno())
    except Exception:
        sys.stderr.write(ERROR + '\n')
        return 1


if __name__ == '__main__':
    raise SystemExit(main())
