"""Inactive v2 Content library publisher, with native completion last.

Full projected publication/recovery/RESUME envelopes must fit before rename.
Actual native verification is obtained and qualified independently afterward.
No CLI, managed launch, registration, sink, SQL or dispatch adoption exists here.
"""
import artifact_import_verifier as p
import content_metadata as m
import managed_content as managed
import narrow_gold_publication_v2 as gold_publisher
import narrow_projection_transport as transport
import narrow_publication_lifecycle_v2 as life
import narrow_publication_projection_v2 as projection
from narrow_content_reports import verify_publication_v1 as verify_rows
from narrow_content_reports_v2 import validate_precompletion_v2
from publication_metadata import canonical_bytes


def _native():
    import content_publication
    return content_publication


def _context(bound, expectations):
    context = projection.bind_context_v2(bound, expectations=expectations)
    m.need(context.kind == 'CONTENT', 'ARGUMENT_INVALID')
    return context


def _gold_command(command, expectations):
    return transport.read_command_v2(canonical_bytes(command['goldProof']['command']), expectations=expectations)


def _reference(command):
    reference = command['reference']
    selection = reference['qualification']['selection']
    verification = reference['qualification']['verification']
    p.capture(selection, command['referenceBinding'], verification['capture'])
    life.equal(p.qualify(selection, command['referenceBinding']), verification['native'])


def _check_dependencies(command, dependency, expectations):
    gold_publisher.recheck_publication_v2(_gold_command(command, expectations),
        command['goldProof']['artifact']['verification'], expectations=expectations)
    _reference(command)
    dependency.check()
    life.roots(command)


def _dependencies(spark, command, expectations):
    actual_gold = gold_publisher.verify_publication_v2(spark, _gold_command(command, expectations),
                                                       expectations=expectations)
    life.equal(actual_gold, command['goldProof']['artifact']['verification'])
    _reference(command)
    reference = command['reference']['qualification']['selection']
    args = dict(gold_root=command['goldProof']['command']['gold']['root'],
        gold_fingerprint=command['goldProof']['artifact']['nativeFingerprint'],
        reference_root=command['referenceBinding']['root'], reference_id=reference['identity']['referenceId'],
        revision=reference['identity']['revision'], reference_fingerprint=reference['fingerprint'])
    from content_dependencies import verify_dependencies
    dependency = verify_dependencies(**args, output_root=command['content']['root'])
    prepared = dict(manifest=p.thaw(dependency.manifest()), fingerprint=dependency.fingerprint)
    life.equal(prepared, command['prepared'])
    _check_dependencies(command, dependency, expectations)
    return args, prepared, dependency


def _actual(command, args):
    # The existing independent verifier opens a real completed publication and
    # executes all three reports. Never substitute sizing smoke placeholders.
    return managed.verify(command, args)


def _check_actual(command, actual):
    evidence, captured = managed.capture(command)
    life.equal(captured, actual['capture'])
    evidence.check()


def _verify_rows(command, args):
    verify_rows(command['content']['root'], command['prepared']['fingerprint'],
                gold_root=args['gold_root'], reference_root=args['reference_root'])


def verify_publication_v2(spark, bound, *, expectations):
    """Read-only native qualification; absence/incompleteness is never repaired."""
    context = _context(bound, expectations)
    command = context.bound.value()
    life.roots(command)
    args, _, dependency = _dependencies(spark, command, expectations)
    _verify_rows(command, args)
    actual = _actual(command, args)
    life.qualify(context.bound, actual)
    _check_dependencies(command, dependency, expectations)
    _check_actual(command, actual)
    return actual


def publish_v2(spark, bound, *, expectations):
    """Produce native output or read-only verify replay; return real VERIFIED body.

    The caller owns Spark lifecycle and single-writer exclusion. Incomplete files
    and uncertain/post-rename results are retained, without cleanup or adoption.
    Independent expectations establish correspondence only, not operation authority.
    """
    context = _context(bound, expectations)
    command = context.bound.value()
    root, identity = life.roots(command)
    args, prepared, dependency = _dependencies(spark, command, expectations)
    native, guards = _native(), ()
    directory = m.publication_directory(root, dependency.fingerprint)
    try:
        existing, guards = life.reserve(root, directory, identity)
        if existing:
            _verify_rows(command, args)
            actual = _actual(command, args)
            life.qualify(context.bound, actual)
            _check_dependencies(command, dependency, expectations)
            _check_actual(command, actual)
            life.check_guards(guards)
            return actual
        gold, expected = native.build_content(spark, dependency)
        native.verify_frames(gold, expected, expected)
        _check_dependencies(command, dependency, expectations)
        life.check_guards(guards)
        for name, raw in (('inputs.json', dependency.manifest_bytes),
                          ('gold-inputs.json', dependency.gold.raw('inputs.json')),
                          ('gold-complete.json', dependency.gold.raw('complete.json'))):
            life.check_guards(guards)
            native._write(directory / name, raw)
        for name, frame in ((m.CATALOG, expected.catalog), (m.DAILY, expected.daily)):
            life.check_guards(guards)
            m.need(frame.schema.simpleString() == native.SCHEMAS[name].simpleString())
            frame.write.mode('errorifexists').parquet(str(directory / name))
        evidence, counts, files = native._verify(spark, dependency, gold, expected, directory, root, False)
        scan = validate_precompletion_v2(dependency, evidence, counts)
        evidence.check()
        _check_dependencies(command, dependency, expectations)
        life.check_guards(guards)
        completed = dict(completion_version=1, product_id=m.PRODUCT,
            product_fingerprint=dependency.fingerprint, manifest_byte_length=len(dependency.manifest_bytes),
            output_counts=counts, files=files, completed_at=native._now())
        raw = canonical_bytes(completed)
        m.need(len(raw) <= m.COMPLETION_LIMIT, 'RESOURCE_LIMIT')
        native._write(directory / 'complete.tmp', raw)
        temporary = m.capture(directory, m.TABLES, native.DATA_TOP | {'complete.tmp'},
            'PUBLICATION_FAILED', 'PUBLICATION_CHANGED', guards=m.namespace_guards(root, directory))
        native._unchanged_before_marker(evidence, temporary)
        m.need(temporary.raw('complete.tmp') == raw, 'PUBLICATION_CHANGED')
        m.validate_completion(raw, dependency.fingerprint, len(dependency.manifest_bytes))
        projected = projection.project_content_v2(context, temporary, coverage=scan.coverage)
        life.qualify(context.bound, projected.sizing_value())
        _check_dependencies(command, dependency, expectations)
        temporary.check()
        life.check_guards(guards)
        life.rename_last(directory)
        actual = _actual(command, args)
        projection.compare_actual_v2(projected, actual)
        life.qualify(context.bound, actual)
        _check_dependencies(command, dependency, expectations)
        _check_actual(command, actual)
        life.check_guards(guards)
        return actual
    except Exception:
        try:
            _check_dependencies(command, dependency, expectations)
        finally:
            life.check_guards(guards)
        raise
