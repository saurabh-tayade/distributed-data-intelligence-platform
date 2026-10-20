"""Single-writer immutable Content v1 Spark publication, completion last."""
from datetime import datetime, timezone
import sys

import content_metadata as m
from content_dependencies import verify_dependencies
from content_processing import SCHEMAS, ContentFrames, build_content, read_frames, verify_frames
from publication_metadata import canonical_bytes

DATA_TOP = frozenset(('inputs.json', 'gold-inputs.json', 'gold-complete.json', *m.TABLES))


def _write(path, raw):
    with path.open('xb') as stream:
        m.need(stream.write(raw) == len(raw), 'PUBLICATION_FAILED')


def _now():
    return datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')


def _check_guards(guards):
    for path,identity in guards:
        m._info(path, True, 'PUBLICATION_FAILED', 'PUBLICATION_CHANGED', identity)


def _verify(spark, dependency, gold, expected, directory, root, existing):
    top = DATA_TOP | ({'complete.json'} if existing else set())
    evidence = m.capture(directory, m.TABLES, top,
                         'PUBLICATION_INVALID' if existing else 'PUBLICATION_FAILED', 'PUBLICATION_CHANGED',
                         guards=m.namespace_guards(root, directory))
    m.need(evidence.raw('inputs.json') == dependency.manifest_bytes)
    m.need(evidence.raw('gold-inputs.json') == dependency.gold.raw('inputs.json'))
    m.need(evidence.raw('gold-complete.json') == dependency.gold.raw('complete.json'))
    files = evidence.inventory()
    m.validate_inventory(files)
    m.admit(dependency.gold.inventory(), files)
    marker = m.validate_completion(evidence.raw('complete.json'), dependency.fingerprint,
                                  len(dependency.manifest_bytes)) if existing else None
    if marker:
        m.need(marker['files'] == files)
    paths = {table: tuple(str(directory / f['path']) for f in files[table]) for table in m.TABLES}
    actual = ContentFrames(*read_frames(spark, paths, SCHEMAS))
    counts = verify_frames(gold, expected, actual)
    if marker:
        m.need(marker['output_counts'] == counts)
    evidence.check()
    dependency.check()
    return evidence, counts, files


def _unchanged_before_marker(before, after):
    files = {item.path: item for item in after.files}
    m.need(all(files.get(item.path) == item for item in before.files), 'PUBLICATION_CHANGED')
    m.need(before.guards == after.guards, 'PUBLICATION_CHANGED')
    current = dict(after.directories)
    for path, (identity, names) in before.directories:
        new_identity, new_names = current[path]
        expected_names = set(names) | ({'complete.tmp'} if path == before.directory else set())
        m.need(identity == new_identity and expected_names == set(new_names), 'PUBLICATION_CHANGED')


def _publish(spark, *, output_root, gold_root, gold_fingerprint, reference_root, reference_id,
            revision, reference_fingerprint):
    m.match(gold_fingerprint)
    m.match(reference_fingerprint)
    m.match(reference_id, m.IDENTIFIER)
    m.match(revision, m.REVISION)
    root = m.root_path(output_root, 'PUBLICATION_UNAVAILABLE')
    info = root.lstat()
    root_identity = info.st_dev, info.st_ino, info.st_mode
    dependency = verify_dependencies(gold_root=gold_root, gold_fingerprint=gold_fingerprint,
        reference_root=reference_root, reference_id=reference_id, revision=revision,
        reference_fingerprint=reference_fingerprint, output_root=root)
    guards = ()
    try:
        gold, expected = build_content(spark, dependency)
        verify_frames(gold, expected, expected)
        dependency.check()
        m._info(root, True, 'PUBLICATION_FAILED', 'PUBLICATION_CHANGED', root_identity)
        directory = m.publication_directory(root, dependency.fingerprint)
        for parent in (root / 'v1', directory.parent):
            try:
                m._info(parent, True, 'PUBLICATION_UNAVAILABLE', 'PUBLICATION_CHANGED')
            except m.ContentFailure as error:
                if error.code != 'PUBLICATION_UNAVAILABLE':
                    raise
                # Absence is allowed only for namespace parents of a new reservation.
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
        _check_guards(guards)
        if not existing:
            for name,raw in (('inputs.json', dependency.manifest_bytes),
                             ('gold-inputs.json', dependency.gold.raw('inputs.json')),
                             ('gold-complete.json', dependency.gold.raw('complete.json'))):
                _check_guards(guards)
                _write(directory / name, raw)
            for name,frame in ((m.CATALOG, expected.catalog), (m.DAILY, expected.daily)):
                _check_guards(guards)
                m.need(frame.schema.simpleString() == SCHEMAS[name].simpleString())
                frame.write.mode('errorifexists').parquet(str(directory / name))
        evidence, counts, files = _verify(spark, dependency, gold, expected, directory, root, existing)
        _check_guards(guards)
        if not existing:
            completed = dict(completion_version=1, product_id=m.PRODUCT,
                product_fingerprint=dependency.fingerprint, manifest_byte_length=len(dependency.manifest_bytes),
                output_counts=counts, files=files, completed_at=_now())
            raw = canonical_bytes(completed)
            m.need(len(raw) <= m.COMPLETION_LIMIT, 'RESOURCE_LIMIT')
            _write(directory / 'complete.tmp', raw)
            temporary = m.capture(directory, m.TABLES, DATA_TOP | {'complete.tmp'},
                'PUBLICATION_FAILED', 'PUBLICATION_CHANGED', guards=m.namespace_guards(root, directory))
            _unchanged_before_marker(evidence, temporary)
            m.need(temporary.raw('complete.tmp') == raw, 'PUBLICATION_FAILED')
            m.validate_completion(raw, dependency.fingerprint, len(dependency.manifest_bytes))
            dependency.check()
            temporary.check()
            _check_guards(guards)
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
        # Retained observations can establish a mutation even when Spark/I/O failed.
        dependency.check()
        _check_guards(guards)
        raise m.ContentFailure('PUBLICATION_FAILED') from None


def publish(spark, *, output_root, gold_root, gold_fingerprint, reference_root, reference_id,
            revision, reference_fingerprint):
    try:
        return _publish(spark, output_root=output_root, gold_root=gold_root,
            gold_fingerprint=gold_fingerprint, reference_root=reference_root,
            reference_id=reference_id, revision=revision, reference_fingerprint=reference_fingerprint)
    except m.ContentFailure:
        raise
    except Exception:
        raise m.ContentFailure('PUBLICATION_FAILED') from None


def main(argv=None):
    spark = None
    try:
        options = m.arguments(sys.argv[1:] if argv is None else argv,
            {'output-root', 'gold-root', 'gold-fingerprint', 'reference-root',
             'reference-id', 'revision', 'reference-fingerprint'})
        if options is None:
            print('Content publisher: --output-root --gold-root --gold-fingerprint --reference-root '
                  '--reference-id --revision --reference-fingerprint')
            return 0
        m.match(options['gold_fingerprint'])
        m.match(options['reference_fingerprint'])
        m.match(options['reference_id'], m.IDENTIFIER)
        m.match(options['revision'], m.REVISION)
        from silver import session
        from content_analytics import public
        spark = session()
        result = public(publish(spark, **options))
        spark.stop()
        spark = None
        raw = canonical_bytes(result)
        m.need(len(raw)+1 <= 4096, 'RESPONSE_LIMIT')
        print(raw.decode())
        return 0
    except Exception as error:
        failure = error if isinstance(error, m.ContentFailure) else m.ContentFailure('PUBLICATION_FAILED')
        print(canonical_bytes(failure.diagnostic).decode(), file=sys.stderr)
        return 2 if failure.code == 'ARGUMENT_INVALID' else 1
    finally:
        if spark is not None:
            try:
                spark.stop()
            except Exception:
                pass  # Failure already emitted; never replace it with raw JVM details.


if __name__ == '__main__':
    raise SystemExit(main())
