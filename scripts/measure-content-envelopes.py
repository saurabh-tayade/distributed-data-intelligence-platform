"""Local #49 serializer/reader measurements, never managed admission or dispatch.

Generated native fixtures use the existing DuckDB consumer fixture helper. They
are not Bronze/Silver/Spark producer evidence. Nested commands carry a separately
labeled synthetic Silver proof and must never be submitted to a runtime.
"""
import argparse
import ast
from hashlib import sha256
import json
import os
from pathlib import Path
import platform
import subprocess
import sys
import time
from types import SimpleNamespace

REPO = Path(__file__).resolve().parents[1]
SLOT_BYTES = 1024**2
LEGACY_BYTES = 16 * 1024**2
CASES = ('small_nonempty', 'gold_902_empty_escaped', 'content_902_empty_escaped',
         'both_902_empty_escaped')


def imports():
    sys.path[:0] = [str(REPO / 'processing'), str(REPO / 'processing' / 'tests')]
    from publication_metadata import canonical_bytes
    return canonical_bytes


def characteristics(value):
    """Depth uses root zero, matching SourceJson's recursive encoder.

    Container nesting is also recorded because the Jackson reader counts open
    containers, including empty ones. String widths use Java UTF-16 code units.
    These observations predict nothing: Java executes its actual parser separately.
    """
    result = dict(max_value_depth=0, max_container_nesting=0,
                  max_string_utf16_units=0, max_key_utf16_units=0,
                  max_integer_token_chars=0)

    def units(text):
        return len(text.encode('utf-16-le')) // 2

    def visit(item, depth, containers):
        result['max_value_depth'] = max(result['max_value_depth'], depth)
        if isinstance(item, (dict, list)):
            containers += 1
            result['max_container_nesting'] = max(result['max_container_nesting'], containers)
            children = item.values() if isinstance(item, dict) else item
            if isinstance(item, dict):
                for key in item:
                    result['max_key_utf16_units'] = max(result['max_key_utf16_units'], units(key))
            for child in children:
                visit(child, depth + 1, containers)
        elif isinstance(item, str):
            result['max_string_utf16_units'] = max(result['max_string_utf16_units'], units(item))
        elif type(item) is int:
            result['max_integer_token_chars'] = max(result['max_integer_token_chars'], len(str(item)))
    visit(value, 0, 0)
    return result


def observation(value, canonical_bytes):
    raw = canonical_bytes(value)
    return dict(bytes=len(raw), sha256=sha256(raw).hexdigest(),
                fits_1mib_candidate_slot=len(raw) <= SLOT_BYTES,
                fits_16mib_legacy_byte_ceiling=len(raw) <= LEGACY_BYTES,
                **characteristics(value))


def inventory(root):
    """Stable read-only comparison; ignore access times, retain inode/mode/mtime."""
    result = {}
    for path in (root, *sorted(root.rglob('*'))):
        stat = path.lstat()
        result[path.relative_to(root).as_posix()] = dict(
            identity=[stat.st_dev, stat.st_ino, stat.st_mode, stat.st_nlink],
            mtime_ns=stat.st_mtime_ns, size=stat.st_size,
            sha256=sha256(path.read_bytes()).hexdigest() if path.is_file() else None)
    return result


def binding(root):
    root = root.resolve(strict=True)
    stat = root.stat()
    return dict(root=str(root), rootDevice=str(stat.st_dev), rootInode=str(stat.st_ino))


def production_expression(path, function, target, fields, environment):
    """Evaluate one exact production dict constructor, not its effectful caller.

    Require an exact unique field signature and record its source location/hash.
    This intentionally does not execute Spark or turn its result into authority.
    """
    source = path.read_text()
    functions = [node for node in ast.parse(source).body
                 if isinstance(node, ast.FunctionDef) and node.name == function]
    if len(functions) != 1:
        raise RuntimeError('production function changed')
    nodes = [node for node in ast.walk(functions[0]) if isinstance(node, ast.Assign)
             and len(node.targets) == 1 and isinstance(node.targets[0], ast.Name)
             and node.targets[0].id == target and isinstance(node.value, ast.Call)
             and isinstance(node.value.func, ast.Name) and node.value.func.id == 'dict'
             and not node.value.args and {key.arg for key in node.value.keywords} == set(fields)]
    if len(nodes) != 1:
        raise RuntimeError('production constructor changed')
    node = nodes[0]
    value = eval(compile(ast.Expression(node.value), str(path), 'eval'),
                 {'dict': dict, 'sha256': sha256, '__builtins__': {}}, environment)
    return value, dict(path=str(path.relative_to(REPO)), function=function,
                       first_line=node.lineno, last_line=node.end_lineno,
                       source_sha256=sha256(source.encode()).hexdigest(),
                       execution='constructor expression only; enclosing native operation not executed')


def expand_empty_dataset(directory, marker, tables, canonical_bytes):
    """902 actual empty Parquet files; legal quote-heavy filenames stress JSON.

    Each filename is 212 UTF-8 bytes, below the common 255-byte filesystem limit.
    These names are accepted by native readers, not claimed to be Spark-generated.
    The original generated empty part is replaced by 451 copies per dataset.
    """
    for table in tables:
        original = directory / table / 'part.parquet'
        raw = original.read_bytes()
        original.unlink()  # Newly generated disposable fixture, not retained user output.
        files = []
        for number in range(451):
            name = '"' * 200 + f'{number:04d}.parquet'
            target = directory / table / name
            target.write_bytes(raw)
            files.append(dict(path=target.relative_to(directory).as_posix(),
                              byte_length=len(raw), sha256=sha256(raw).hexdigest()))
        marker['files'][table] = files
    (directory / 'complete.json').write_bytes(canonical_bytes(marker))


def reference_fixture(args, canonical_bytes):
    import artifact_import_verifier as native
    selection = dict(kind='PUBLISHER_REFERENCE', productId='content-intelligence',
        storageBindingId='reference', relativeAddress='reference_id=fixture-publishers/revision=r1',
        contractVersion=1, processorRevision=1,
        identity=dict(referenceId='fixture-publishers', revision='r1'),
        fingerprint=args['reference_fingerprint'])
    bound = binding(args['reference_root'])
    capture = native.capture(selection, bound)
    qualification = dict(qualificationVersion=1, selection=selection,
        verification=dict(evidenceVersion=1, native=native.qualify(selection, bound), capture=capture))
    if native.capture(selection, bound, capture) != capture:
        raise RuntimeError('reference changed')
    return dict(artifactId='00000000-0000-4000-8000-000000000002',
        productId='content-intelligence', kind='PUBLISHER_REFERENCE', contractVersion=1,
        processorRevision=1, storageBindingId='reference', relativeAddress=selection['relativeAddress'],
        nativeIdentity=args['reference_fingerprint'], evidenceSha256=sha256(canonical_bytes(qualification)).hexdigest(),
        verifiedAt='2026-11-04T00:00:00.000000Z', qualification=qualification)


def synthetic_silver(canonical_bytes):
    """Existing synthetic serializer vector; no native Silver provenance claim."""
    resource = REPO / 'services/content-api/src/test/resources/narrow-content-profile-native-vectors.json'
    qualification = json.loads(resource.read_bytes())['qualification']
    identity = qualification['selection']['identity']
    bronze = qualification['verification']['capture']['bronze']
    receipt = dict(sourceId=identity['sourceId'], ingestionId=identity['ingestionId'],
        ingestedAt='2026-11-04T00:00:00.000000Z', storageBindingId='bronze',
        bucket=identity['bronzeBucket'], metadataKey=identity['bronzeMetadataKey'],
        payloadKey=identity['bronzePayloadKey'], metadataSha256=bronze['metadataSha256'],
        payloadSha256=bronze['payloadSha256'], payloadByteLength=bronze['payloadByteLength'])
    return [dict(artifactId='00000000-0000-4000-8000-000000000001',
        originRunId='00000000-0000-4000-8000-000000000003', sourceId=identity['sourceId'],
        evidenceSha256=sha256(canonical_bytes(qualification)).hexdigest(),
        qualification=qualification, receipt=receipt,
        bronze=dict(endpoint='http://127.0.0.1:8333', region='us-east-1', bucket=identity['bronzeBucket'],
                    credentialsFile='/measurement-only/not-a-credential-file'),
        credentialsSha256='0' * 64)]


def measure_case(case, output, canonical_bytes):
    import artifact_import_verifier as p
    import content_dependencies
    import content_metadata as m
    import gold_analytics
    import gold_reader
    import managed_content
    import managed_gold
    from narrow_content_test_fixtures import inputs, content

    output.mkdir()
    native_root = output / 'native'
    native_root.mkdir()
    args = inputs(native_root, empty=case != 'small_nonempty')
    gold_directory = args['gold_root'] / 'v1' / 'processor_revision=1' / ('input_set=' + args['gold_fingerprint'])
    marker = json.loads((gold_directory / 'complete.json').read_bytes())
    if case in ('gold_902_empty_escaped', 'both_902_empty_escaped'):
        expand_empty_dataset(gold_directory, marker, content_dependencies.GOLD_TABLES, canonical_bytes)
    dependency, content_directory, content_marker = content(args)
    if case in ('content_902_empty_escaped', 'both_902_empty_escaped'):
        expand_empty_dataset(content_directory, content_marker, m.TABLES, canonical_bytes)
    for name in ('silver', 'scratch'):
        (native_root / name).mkdir()
    before = inventory(native_root)
    checks = []
    metadata = gold_reader.verify_metadata(args['gold_root'], args['gold_fingerprint'])
    checks.append('gold_reader.verify_metadata')
    with gold_analytics.open_publication(args['gold_root'], args['gold_fingerprint'], memory_limit='512MiB') as opened:
        opened.report('quality')
    checks.append('gold_analytics.open_publication + quality')
    captured, capture = managed_gold.gold_capture(dict(gold=binding(args['gold_root'])), args['gold_fingerprint'])
    checks.append('managed_gold.gold_capture')
    prepared, prepared_origin = production_expression(REPO / 'processing/managed_gold.py',
        'spark_action', 'prepared', ('manifest', 'inputSetFingerprint', 'counts'),
        dict(inputs=SimpleNamespace(manifest=json.loads(metadata.manifest_bytes),
            input_set_fingerprint=args['gold_fingerprint'], counts=metadata.marker['input_counts'])))
    verification, verification_origin = production_expression(REPO / 'processing/managed_gold.py',
        'spark_action', 'value', ('status', 'prepared', 'completion', 'capture', 'completionSha256', 'verifiers'),
        dict(prepared=prepared, marker=metadata.marker, capture=capture, before=captured))
    content_prepared, content_origin = production_expression(REPO / 'processing/managed_content.py',
        'dependencies', 'prepared', ('manifest', 'fingerprint'), dict(p=p, dep=dependency))
    content_command = dict(content=binding(args['output_root']), prepared=content_prepared, verified=None)
    content_verification = managed_content.verify(content_command, args)
    checks.append('managed_content.verify (actual full native Content opener and all three smoke reports)')
    reference = reference_fixture(args, canonical_bytes)
    checks.append('artifact_import_verifier.capture + qualify for publisher reference')
    captured.check()
    metadata.verify_unchanged()
    if inventory(native_root) != before:
        raise RuntimeError('read-only qualification changed native fixture')
    checks.append('complete native fixture hash/identity/mode/mtime unchanged after qualification')
    envelopes = dict(
        gold_manifest=json.loads(metadata.manifest_bytes), gold_completion=metadata.marker,
        gold_capture=capture, gold_prepared=prepared, gold_verification=verification,
        content_prepared=content_prepared, content_completion=content_marker,
        content_capture=content_verification['capture'], content_verification=content_verification,
        reference_qualification=reference['qualification'])
    directory = output / 'envelopes'
    directory.mkdir()
    for name, value in envelopes.items():
        (directory / (name.replace('_', '-') + '.json')).write_bytes(canonical_bytes(value))
    fixture = dict(inputs=synthetic_silver(canonical_bytes), reference=reference,
        bindings={name: binding(root) for name, root in dict(silver=native_root / 'silver',
            scratch=native_root / 'scratch', gold=args['gold_root'], reference=args['reference_root'],
            content=args['output_root']).items()}, javaHome=os.environ.get('JAVA_HOME', '/measurement-only/java'),
        provenance='Native Gold/Content consumers verified. Silver command input is a synthetic serializer vector, unrelated to native Gold provenance; never execute these commands.')
    (output / 'fixture.json').write_bytes(canonical_bytes(fixture))
    result = dict(case=case, evidence_class='direct native-consumer fixture; not Spark producer or managed end-to-end proof',
        gold_parquet_files=sum(map(len, marker['files'].values())),
        content_parquet_files=sum(map(len, content_marker['files'].values())),
        gold_output_counts=marker['output_counts'], content_output_counts=content_marker['output_counts'],
        native_checks=checks, constructors=dict(gold_prepared=prepared_origin, gold_verification=verification_origin,
                          content_prepared=content_origin),
        measurements={name: observation(value, canonical_bytes) for name, value in envelopes.items()},
        limitations=['Gold verification evaluates the original constructor only; gold_publication.verify_publication, Spark shutdown and Bronze/Silver provenance are not executed.',
                     'Escaped filenames are legal consumer stress, not observed Spark output.',
                     'Successful consumers and observed sizes do not qualify maximum output, fixed-ledger admission, restart, recovery or dispatch.'])
    (output / 'python-measurements.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def measure(args):
    if not args.cases or any(case not in CASES for case in args.cases) or len(set(args.cases)) != len(args.cases):
        raise ValueError('choose distinct known measurement cases')
    output = Path(args.output).absolute()
    output.mkdir(parents=True, exist_ok=False)
    canonical_bytes = imports()
    import duckdb
    start = time.monotonic_ns()
    cases = [measure_case(case, output / case, canonical_bytes) for case in args.cases]
    result = dict(scope='measurement only; Gate B0 incomplete; ordinary Content dispatch disabled',
        python=platform.python_version(), duckdb=duckdb.__version__, platform=platform.platform(),
        git_head=subprocess.check_output(['git', 'rev-parse', 'HEAD'], cwd=REPO, text=True).strip(),
        duration_ms=(time.monotonic_ns() - start) // 1_000_000,
        cases=cases)
    (output / 'results.json').write_text(json.dumps(result, indent=2) + '\n')
    return result


def verify_java(output, cases):
    """Cross-check actual Java-emitted bytes with the production Python serializer."""
    canonical_bytes = imports()
    rows = []
    for case in cases:
        directory = Path(output) / case
        report = json.loads((directory / 'java-measurements.json').read_bytes())
        for row in report['measurements']:
            name = row['name']
            path = (directory / 'envelopes' / (name.removeprefix('python/') + '.json')
                    if name.startswith('python/') else directory / 'java-envelopes' / (name + '.json'))
            raw = path.read_bytes()
            value = json.loads(raw)
            if canonical_bytes(value) != raw or len(raw) != row['bytes'] or sha256(raw).hexdigest() != row['sha256']:
                raise RuntimeError('Python/Java canonical measurement mismatch: ' + name)
            measured = characteristics(value)
            for python_name, java_name in (('max_value_depth', 'maximumValueDepth'),
                    ('max_container_nesting', 'maximumContainerNesting'),
                    ('max_string_utf16_units', 'maximumStringValueUtf16Units'),
                    ('max_key_utf16_units', 'maximumKeyUtf16Units')):
                if measured[python_name] != row[java_name]:
                    raise RuntimeError('Python/Java structural measurement mismatch: ' + name)
            if row.get('sourceJsonAccepted') and not row.get('canonicalRoundtrip'):
                raise RuntimeError('Java accepted noncanonical fixture: ' + name)
            rows.append(dict(case=case, name=name, bytes=len(raw),
                source_json_accepted=row['sourceJsonAccepted'], **measured))
    result = dict(production_python_canonical_bytes_equal_to_java_bytes=True,
                  comparisons=len(rows), measurements=rows)
    with (Path(output) / 'cross-language-measurements.json').open('x') as stream:
        stream.write(json.dumps(result, indent=2) + '\n')
    return result


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True, help='new local directory; never overwrite a prior run')
    parser.add_argument('--cases', nargs='+', choices=CASES, default=list(CASES))
    parser.add_argument('--verify-java', action='store_true',
                        help='cross-check existing completed Java output; preserve all prior results')
    args = parser.parse_args()
    if args.verify_java:
        result = verify_java(args.output, args.cases)
        print(json.dumps(dict(output=str(args.output), comparisons=result['comparisons'])))
        return
    result = measure(args)
    print(json.dumps(dict(output=str(args.output), cases=[dict(case=case['case'],
        gold_verification_bytes=case['measurements']['gold_verification']['bytes'],
        content_verification_bytes=case['measurements']['content_verification']['bytes']) for case in result['cases']])))


if __name__ == '__main__':
    main()
