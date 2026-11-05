"""Inactive v2 Gold library publisher with whole-envelope completion-last checks.

This does not launch a managed command or confer writer/owner/checkpoint authority.
The caller supplies an existing Spark session and independent runtime/dependency
expectations. Legacy native bytes, adapters and runtime hashes remain unchanged.
"""
from hashlib import sha256

import content_metadata as m
import managed_gold as managed
import narrow_publication_lifecycle_v2 as life
import narrow_publication_projection_v2 as projection
from publication_metadata import canonical_bytes

TABLES = ('gold_articles', 'gold_source_daily')
DATA_TOP = {'inputs.json', *TABLES}


def _native():
    import gold_publication
    return gold_publication


def _context(bound, expectations):
    context = projection.bind_context_v2(bound, expectations=expectations)
    m.need(context.kind == 'GOLD', 'ARGUMENT_INVALID')
    return context


def _check_inputs(command, inputs, spark=None):
    inputs.verify_unchanged()
    managed.silver_check(command, spark)
    life.equal(dict(manifest=inputs.manifest, inputSetFingerprint=inputs.input_set_fingerprint,
                    counts=inputs.counts), command['prepared'])
    life.roots(command)


def _load_inputs(spark, command):
    from silver_inputs import load_inputs
    markers = managed.silver_check(command, spark)
    inputs = load_inputs(spark, command['silver']['root'], markers=markers)
    _check_inputs(command, inputs)
    return inputs


def _capture(root, directory, marker=None):
    return m.capture(directory, TABLES, DATA_TOP | ({marker} if marker else set()),
        'PUBLICATION_INVALID', 'PUBLICATION_CHANGED', guards=m.namespace_guards(root, directory))


def _actual(command, inputs):
    """Real native Spark verification and independent native analytics opening."""
    import gold_analytics
    before, captured = managed.gold_capture(command, inputs.input_set_fingerprint)
    marker = _native().verify_publication(inputs, command['gold']['root'])
    with gold_analytics.open_publication(command['gold']['root'], inputs.input_set_fingerprint,
                                        memory_limit='512MiB') as opened:
        opened.report('quality')
    before.check()
    life.equal(managed.gold_capture(command, inputs.input_set_fingerprint)[1], captured)
    return dict(status='VERIFIED', prepared=command['prepared'], completion=marker, capture=captured,
        completionSha256=sha256(before.raw('complete.json')).hexdigest(),
        verifiers=['gold_publication.verify_publication', 'gold_analytics.open_publication'])


def _check_actual(command, actual):
    evidence, captured = managed.gold_capture(command, command['prepared']['inputSetFingerprint'])
    life.equal(captured, actual['capture'])
    evidence.check()


def recheck_publication_v2(bound, verification, *, expectations):
    """Read-only exact retained native evidence check; no Spark or producer call."""
    context = _context(bound, expectations)
    command = context.bound.value()
    life.roots(command)
    managed.silver_check(command)
    evidence, captured = managed.gold_capture(command, command['prepared']['inputSetFingerprint'])
    life.equal(captured, verification['capture'])
    from gold_reader import verify_metadata
    metadata = verify_metadata(command['gold']['root'], command['prepared']['inputSetFingerprint'])
    life.equal(metadata.marker, verification['completion'])
    m.need(metadata.manifest_bytes == canonical_bytes(command['prepared']['manifest']), 'PUBLICATION_CHANGED')
    m.need(sha256(evidence.raw('complete.json')).hexdigest() == verification['completionSha256'],
           'PUBLICATION_CHANGED')
    life.equal(verification['prepared'], command['prepared'])
    life.qualify(context.bound, verification)
    evidence.check()
    metadata.verify_unchanged()
    managed.silver_check(command)
    life.roots(command)
    _check_actual(command, verification)


def verify_publication_v2(spark, bound, *, expectations):
    """Fresh read-only native qualification, never creates or repairs output."""
    context = _context(bound, expectations)
    command = context.bound.value()
    life.roots(command)
    inputs = _load_inputs(spark, command)
    actual = _actual(command, inputs)
    life.qualify(context.bound, actual)
    _check_inputs(command, inputs)
    _check_actual(command, actual)
    return actual


def publish_v2(spark, bound, *, expectations):
    """Return only actual verification, after complete projected and actual checks.

    Any failure retains artifacts. A post-rename failure never removes or demotes
    native completion; callers must use read-only reconciliation, not retry writes.
    This library does not own or report Spark shutdown and emits no result sidecar.
    """
    context = _context(bound, expectations)
    command = context.bound.value()
    root, identity = life.roots(command)
    inputs = _load_inputs(spark, command)
    native = _native()
    directory = root / 'v1' / 'processor_revision=1' / ('input_set=' + inputs.input_set_fingerprint)
    guards = ()
    try:
        existing, guards = life.reserve(root, directory, identity)
        if existing:
            actual = _actual(command, inputs)
            life.qualify(context.bound, actual)
            _check_inputs(command, inputs)
            _check_actual(command, actual)
            life.check_guards(guards)
            return actual
        expected = native.build_gold(inputs)
        _check_inputs(command, inputs)
        life.check_guards(guards)
        life.write(directory / 'inputs.json', canonical_bytes(inputs.manifest))
        for name, frame in zip(native.SCHEMAS, (expected.articles, expected.source_daily), strict=True):
            life.check_guards(guards)
            m.need(frame.schema.simpleString() == native.SCHEMAS[name].simpleString(), 'PUBLICATION_INVALID')
            frame.select(native.SCHEMAS[name].fieldNames()).write.mode('errorifexists').parquet(str(directory / name))
        before = _capture(root, directory)
        completed = native._verify(inputs, directory, expected, canonical_bytes(inputs.manifest), False)
        before.check()
        _check_inputs(command, inputs)
        life.check_guards(guards)
        raw = canonical_bytes(completed)
        m.need(len(raw) <= m.COMPLETION_LIMIT, 'RESOURCE_LIMIT')
        life.write(directory / 'complete.tmp', raw)
        temporary = _capture(root, directory, 'complete.tmp')
        life.unchanged_before_marker(before, temporary)
        m.need(temporary.raw('complete.tmp') == raw, 'PUBLICATION_CHANGED')
        projected = projection.project_gold_v2(context, temporary)
        life.qualify(context.bound, projected.sizing_value())
        _check_inputs(command, inputs)
        temporary.check()
        life.check_guards(guards)
        life.rename_last(directory)
        actual = _actual(command, inputs)
        projection.compare_actual_v2(projected, actual)
        life.qualify(context.bound, actual)
        _check_inputs(command, inputs)
        _check_actual(command, actual)
        life.check_guards(guards)
        return actual
    except Exception:
        # Recheck retained dependencies/ancestors even on failed I/O. Never
        # restore, remove, relabel or retry an uncertain marker operation.
        try:
            _check_inputs(command, inputs)
        finally:
            life.check_guards(guards)
        raise
