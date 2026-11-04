"""Inactive narrow managed Content v1 publisher, with completion-last row guards.

This additive entry point owns its lifecycle; it does not patch native globals,
change standalone contracts or register with any dispatcher/runtime. Native
helpers remain authoritative for construction, read-back, metadata and paths.
"""
import content_metadata as m
from narrow_content_reports import validate_precompletion_v1
from publication_metadata import canonical_bytes


def _native():
    # Keep the separate read-only reconciliation path Spark-free.
    import content_publication
    return content_publication


def _publish_v1(spark, *, output_root, gold_root, gold_fingerprint, reference_root, reference_id,
                revision, reference_fingerprint):
    native = _native()
    m.match(gold_fingerprint)
    m.match(reference_fingerprint)
    m.match(reference_id, m.IDENTIFIER)
    m.match(revision, m.REVISION)
    root = m.root_path(output_root, 'PUBLICATION_UNAVAILABLE')
    info = root.lstat()
    root_identity = info.st_dev, info.st_ino, info.st_mode
    dependency = native.verify_dependencies(gold_root=gold_root, gold_fingerprint=gold_fingerprint,
        reference_root=reference_root, reference_id=reference_id, revision=revision,
        reference_fingerprint=reference_fingerprint, output_root=root)
    guards = ()
    try:
        gold, expected = native.build_content(spark, dependency)
        native.verify_frames(gold, expected, expected)
        dependency.check()
        m._info(root, True, 'PUBLICATION_FAILED', 'PUBLICATION_CHANGED', root_identity)
        directory = m.publication_directory(root, dependency.fingerprint)
        for parent in (root / 'v1', directory.parent):
            try:
                m._info(parent, True, 'PUBLICATION_UNAVAILABLE', 'PUBLICATION_CHANGED')
            except m.ContentFailure as error:
                if error.code != 'PUBLICATION_UNAVAILABLE':
                    raise
                try:
                    parent.lstat()
                except FileNotFoundError:
                    continue
                raise
        try:
            directory.lstat()
            m._info(directory, True, 'PUBLICATION_INVALID', 'PUBLICATION_CHANGED')
            existing = True
        except FileNotFoundError:
            existing = False
        if existing:
            try:
                (directory / 'complete.json').lstat()
            except FileNotFoundError:
                raise m.ContentFailure('PUBLICATION_INCOMPLETE') from None
        else:
            directory.parent.mkdir(parents=True, exist_ok=True)
            directory.mkdir(exist_ok=False)
        guards = [(root, root_identity)]
        for path in (*m.namespace_guards(root, directory)[1:], directory):
            info = m._info(path, True, 'PUBLICATION_FAILED', 'PUBLICATION_CHANGED')
            guards.append((path, (info.st_dev, info.st_ino, info.st_mode)))
        native._check_guards(guards)
        if not existing:
            for name, raw in (('inputs.json', dependency.manifest_bytes),
                              ('gold-inputs.json', dependency.gold.raw('inputs.json')),
                              ('gold-complete.json', dependency.gold.raw('complete.json'))):
                native._check_guards(guards)
                native._write(directory / name, raw)
            for name, frame in ((m.CATALOG, expected.catalog), (m.DAILY, expected.daily)):
                native._check_guards(guards)
                m.need(frame.schema.simpleString() == native.SCHEMAS[name].simpleString())
                frame.write.mode('errorifexists').parquet(str(directory / name))
        evidence, counts, files = native._verify(spark, dependency, gold, expected, directory, root, existing)
        validate_precompletion_v1(dependency, evidence, counts)
        # The new scan is inside the retained-evidence window. In particular a
        # scan success is not permission to publish after a concurrent mutation.
        evidence.check()
        dependency.check()
        native._check_guards(guards)
        if not existing:
            completed = dict(completion_version=1, product_id=m.PRODUCT,
                product_fingerprint=dependency.fingerprint, manifest_byte_length=len(dependency.manifest_bytes),
                output_counts=counts, files=files, completed_at=native._now())
            raw = canonical_bytes(completed)
            m.need(len(raw) <= m.COMPLETION_LIMIT, 'RESOURCE_LIMIT')
            evidence.check()
            dependency.check()
            native._check_guards(guards)
            native._write(directory / 'complete.tmp', raw)
            temporary = m.capture(directory, m.TABLES, native.DATA_TOP | {'complete.tmp'},
                'PUBLICATION_FAILED', 'PUBLICATION_CHANGED', guards=m.namespace_guards(root, directory))
            native._unchanged_before_marker(evidence, temporary)
            m.need(temporary.raw('complete.tmp') == raw, 'PUBLICATION_FAILED')
            m.validate_completion(raw, dependency.fingerprint, len(dependency.manifest_bytes))
            dependency.check()
            temporary.check()
            native._check_guards(guards)
            try:
                (directory / 'complete.json').lstat()
            except FileNotFoundError:
                pass
            else:
                raise m.ContentFailure('PUBLICATION_CHANGED')
            (directory / 'complete.tmp').rename(directory / 'complete.json')
        return m.freeze(dict(status='replayed' if existing else 'published', product_id=m.PRODUCT,
                             product_fingerprint=dependency.fingerprint, output_counts=counts))
    except m.ContentFailure:
        raise
    except Exception:
        dependency.check()
        native._check_guards(guards)
        raise m.ContentFailure('PUBLICATION_FAILED') from None


def publish_v1(spark, *, output_root, gold_root, gold_fingerprint, reference_root, reference_id,
               revision, reference_fingerprint):
    """Opt in to 60 KiB full public rows, without changing native publication bytes."""
    try:
        return _publish_v1(spark, output_root=output_root, gold_root=gold_root,
            gold_fingerprint=gold_fingerprint, reference_root=reference_root,
            reference_id=reference_id, revision=revision, reference_fingerprint=reference_fingerprint)
    except m.ContentFailure:
        raise
    except Exception:
        raise m.ContentFailure('PUBLICATION_FAILED') from None
