"""Disposable native-consumer projection parity fixtures, never executable commands.

Gold is a real consumer check plus the exact managed result constructor, not a
Spark/Silver provenance or gold_publication.verify_publication execution. Content
uses its actual independent verifier. Only this test helper renames fixture files.
"""
import argparse
from hashlib import sha256
import importlib.util
import json
from pathlib import Path
import sys

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]
import content_analytics as analytics
import content_metadata as m
import gold_analytics
import gold_reader
import managed_content
import managed_gold
import narrow_content_reports
import narrow_publication_projection as projection
from narrow_content_test_fixtures import inputs, content, snapshot
from publication_metadata import canonical_bytes

REPO = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location('projection_measurement_fixture', REPO / 'scripts/measure-content-envelopes.py')
measurement = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(measurement)
ID = '00000000-0000-4000-8000-000000000001'
DATA_TOP = {'inputs.json', 'gold-inputs.json', 'gold-complete.json', *m.TABLES}


def capture(kind, root, directory, temporary=True):
    tables = projection.GOLD_TABLES if kind == 'GOLD' else m.TABLES
    top = {'inputs.json', *tables} if kind == 'GOLD' else DATA_TOP
    return m.capture(directory, tables, top | ({'complete.tmp'} if temporary else set()),
        'PUBLICATION_INVALID', 'PUBLICATION_CHANGED', guards=m.namespace_guards(root, directory))


def common(prepared, configuration):
    return dict(protocolVersion=1, mode='PUBLISH', runId=ID, attemptId=ID, ownerEpoch=1,
        checkpointId=ID, runtimeSha256=sha256(canonical_bytes(configuration)).hexdigest(), inputSha256='0' * 64,
        runtime=dict(javaHome='/fixture-only/java', scratchRoot='/fixture-only/scratch', scratchDevice='1', scratchInode='1'),
        prepared=prepared, verified=None)


def gold_command(args, prepared):
    config = dict(goldBindingId='gold-projection-fixture', fixtureOnly=True)
    command = common(prepared, config) | dict(silverBindingId='silver', inputs=measurement.synthetic_silver(canonical_bytes),
        silver=dict(root='/fixture-only/silver', rootDevice='1', rootInode='1'), gold=measurement.binding(args['gold_root']))
    return command, config


def content_command(args, prepared, gold, verification):
    config = dict(contentBindingId='content-projection-fixture', fixtureOnly=True)
    artifact = dict(managedGoldVersion=1, kind='GOLD', productId=m.PRODUCT, contractVersion=1, processorRevision=1,
        nativeFingerprint=gold['prepared']['inputSetFingerprint'], storageBindingId='gold-projection-fixture',
        relativeAddress='v1/processor_revision=1/input_set=' + gold['prepared']['inputSetFingerprint'],
        inputs=[dict(artifactId=item['artifactId'], evidenceSha256=item['evidenceSha256']) for item in gold['inputs']],
        verification=verification)
    proof = dict(artifactId=ID, evidenceSha256=sha256(canonical_bytes(artifact)).hexdigest(), artifact=artifact, command=gold)
    command = common(prepared, config) | dict(goldProof=proof, reference=measurement.reference_fixture(args, canonical_bytes),
        referenceBindingId='reference', referenceBinding=measurement.binding(args['reference_root']),
        content=measurement.binding(args['output_root']))
    return command, config


def coverage(dependency, evidence, counts):
    """Actual public totals on a native-validated relation, not a completed opener."""
    conn = analytics.connection()
    try:
        analytics.register(conn, 'base_articles', dependency.paths('gold_articles'), m.GOLD_SCHEMA)
        for table, files in evidence.inventory().items():
            analytics.register(conn, table, tuple(str(evidence.directory / item['path']) for item in files), m.SCHEMAS[table])
        analytics.validate_tables(conn, dependency, counts)
        result = analytics.public(analytics.coverage_totals(conn))
        evidence.check(); dependency.check()
        return result
    finally:
        conn.close()


def actual_gold(args, prepared):
    metadata = gold_reader.verify_metadata(args['gold_root'], args['gold_fingerprint'])
    with gold_analytics.open_publication(args['gold_root'], args['gold_fingerprint'], memory_limit='512MiB') as opened:
        opened.report('quality')
    evidence, captured = managed_gold.gold_capture(dict(gold=measurement.binding(args['gold_root'])), args['gold_fingerprint'])
    value, _ = measurement.production_expression(REPO / 'processing/managed_gold.py', 'spark_action', 'value',
        ('status', 'prepared', 'completion', 'capture', 'completionSha256', 'verifiers'),
        dict(prepared=prepared, marker=metadata.marker, capture=captured, before=evidence))
    evidence.check(); metadata.verify_unchanged()
    return value


def case(directory, *, empty=False, gold_stress=False, content_stress=False, before_rename=None):
    directory.mkdir()
    args = inputs(directory, empty=empty)
    gold_directory = args['gold_root'] / 'v1' / 'processor_revision=1' / ('input_set=' + args['gold_fingerprint'])
    marker = json.loads((gold_directory / 'complete.json').read_bytes())
    if gold_stress:
        measurement.expand_empty_dataset(gold_directory, marker, projection.GOLD_TABLES, canonical_bytes)
    prepared_gold = dict(manifest=json.loads((gold_directory / 'inputs.json').read_bytes()),
        inputSetFingerprint=args['gold_fingerprint'], counts=marker['input_counts'])
    command_gold, config_gold = gold_command(args, prepared_gold)
    context_gold = projection.bind_context_v1('GOLD', command_gold, runtime_configuration=config_gold, output_binding=measurement.binding(args['gold_root']))
    (gold_directory / 'complete.json').rename(gold_directory / 'complete.tmp')
    temporary_gold = capture('GOLD', args['gold_root'], gold_directory)
    before = snapshot(directory)
    projected_gold = projection.project_gold_v1(context_gold, temporary_gold)
    assert snapshot(directory) == before
    if before_rename:
        before_rename('GOLD', args, temporary_gold, context_gold, projected_gold)
    temporary_gold.check()
    (gold_directory / 'complete.tmp').rename(gold_directory / 'complete.json')
    verified_gold = actual_gold(args, prepared_gold)
    projection.compare_actual_v1(projected_gold, verified_gold)

    dependency, content_directory, marker_content = content(args, complete=False)
    if content_stress:
        measurement.expand_empty_dataset(content_directory, marker_content, m.TABLES, canonical_bytes)
        # The shared expansion helper creates a complete marker. This is only a
        # disposable fixture; restore the intended absent-marker test boundary.
        (content_directory / 'complete.json').unlink()
    before_content = capture('CONTENT', args['output_root'], content_directory, temporary=False)
    narrow_content_reports.validate_precompletion_v1(dependency, before_content, marker_content['output_counts'])
    (content_directory / 'complete.tmp').write_bytes(canonical_bytes(marker_content))
    temporary_content = capture('CONTENT', args['output_root'], content_directory)
    totals = coverage(dependency, temporary_content, marker_content['output_counts'])
    prepared_content = dict(manifest=managed_content.p.thaw(dependency.manifest()), fingerprint=dependency.fingerprint)
    command_content, config_content = content_command(args, prepared_content, command_gold, verified_gold)
    context_content = projection.bind_context_v1('CONTENT', command_content, runtime_configuration=config_content, output_binding=measurement.binding(args['output_root']))
    before = snapshot(directory)
    projected_content = projection.project_content_v1(context_content, temporary_content, coverage=totals)
    assert snapshot(directory) == before
    if before_rename:
        before_rename('CONTENT', args, temporary_content, context_content, projected_content)
    temporary_content.check(); dependency.check()
    (content_directory / 'complete.tmp').rename(content_directory / 'complete.json')
    verified_content = managed_content.verify(command_content, args)
    projection.compare_actual_v1(projected_content, verified_content)
    return dict(args=args, gold=(context_gold, temporary_gold, projected_gold, verified_gold),
        content=(context_content, temporary_content, projected_content, verified_content), coverage=totals,
        configurations=dict(GOLD=config_gold, CONTENT=config_content))


def export(output):
    output.mkdir(parents=True, exist_ok=False)
    cases = [('small_nonempty', {}), ('gold_902_empty_escaped', dict(empty=True, gold_stress=True)),
             ('content_902_empty_escaped', dict(empty=True, content_stress=True)),
             ('both_902_empty_escaped', dict(empty=True, gold_stress=True, content_stress=True))]
    for name, options in cases:
        generated = case(output / name, **options)
        for kind in ('GOLD', 'CONTENT'):
            context, _, projected, actual = generated[kind.lower()]
            vector = dict(purpose='SYNTHETIC_SIZE_PARITY_ONLY_NOT_NATIVE_EVIDENCE_OR_EXECUTABLE_COMMAND', kind=kind,
                command=json.loads(context.command_bytes), outputBindingId=context.output_binding_id,
                sizingValue=projected.sizing_value(), actualValue=actual,
                fitsWholeStage=(name == 'small_nonempty' or kind == 'GOLD' and name == 'content_902_empty_escaped'))
            (output / name / (kind.lower() + '.sizing-fixture.json')).write_bytes(canonical_bytes(vector))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', required=True, type=Path)
    export(parser.parse_args().output)
