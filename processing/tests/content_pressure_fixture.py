"""Test-only native pressure inputs and read-only inspection, never an operator API.

The row model only selects feed padding. Native end-to-end tests must measure the
actual produced catalog rows, including a rejected publication's retained rows.
"""
import csv
from datetime import datetime
from hashlib import sha256
import io
import json
from pathlib import Path
import sys
from xml.sax.saxutils import escape

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import artifact_import_verifier as verifier
import content_analytics as analytics
import content_metadata as metadata
import narrow_content_reports
from publication_metadata import canonical_bytes
import publisher_reference


def publisher_name(index):
    prefix = f'Publisher {index:05d} "\\ café 🙂 '
    return prefix + 'p' * (200 - len(prefix))


def reference_csv(count):
    out = io.StringIO(newline='')
    writer = csv.writer(out, lineterminator='\n')
    writer.writerow(publisher_reference.FIELDS)
    for index in range(count):
        source = 'demo-feed' if index == 0 else f'pressure-{index:05d}-'.ljust(128, 's')
        writer.writerow((source, publisher_name(index), 'science', 'en', 'CA'))
    return out.getvalue().encode('utf-8')


def reference(root, binding, count):
    count = int(count)
    if not 1 <= count <= 10001:
        raise ValueError('fixture count must be between 1 and 10001')
    root = Path(root)
    source = root.parent / (root.name + '-reference.csv')
    vocab = root.parent / (root.name + '-vocab.json')
    source.write_bytes(reference_csv(count))
    vocab.write_bytes(canonical_bytes(publisher_reference.VOCABULARY))
    result = publisher_reference.publish(root, source, vocab, 'fixture-publishers', 'r1', 'test-team')
    selection = dict(kind='PUBLISHER_REFERENCE', productId='content-intelligence',
        storageBindingId=binding, relativeAddress='reference_id=fixture-publishers/revision=r1',
        contractVersion=1, processorRevision=1,
        identity=dict(referenceId='fixture-publishers', revision='r1'),
        fingerprint=result['snapshot_fingerprint'])
    stat = root.stat()
    bound = dict(root=str(root), rootDevice=str(stat.st_dev), rootInode=str(stat.st_ino))
    before = verifier.capture(selection, bound)
    native = verifier.qualify(selection, bound)
    verifier.capture(selection, bound, before)
    return dict(qualificationVersion=1, selection=selection,
                verification=dict(evidenceVersion=1, native=native, capture=before))


def source_article_id(index):
    return f'urn:pressure:article:{index:06d}'


def article_id(index):
    # Same array framing as Silver's sha2(to_json(array(...)), 256).
    return sha256(canonical_bytes(['v1', 'demo-feed', 'atom_id', source_article_id(index)])).hexdigest()


def modeled_catalog_row(index, title):
    observed = datetime(2026, 11, 8, 0, 0, 0)
    return dict(article_id=article_id(index), source_id='demo-feed', identifier_kind='atom_id',
        source_article_id=source_article_id(index), feed_format='atom', title=title,
        url=f'https://example.test/pressure/{index:06d}', summary=None,
        published_at=None, updated_at=None, quality_warnings=['INVALID_UPDATED_AT'],
        first_observed_at=observed, last_observed_at=observed,
        observation_count=1, distinct_ingestion_count=1, discovered_at=observed,
        discovery_date=observed.date(), publisher_name=publisher_name(0),
        publisher_category='science', declared_language='en', country_code='CA',
        reference_status='MATCHED', reference_id='fixture-publishers', reference_revision='r1',
        reference_fingerprint='a' * 64)


def padded_title(index, target):
    prefix = 'Pressure "\\ café 🙂 '
    row = modeled_catalog_row(index, prefix)
    padding = target - len(canonical_bytes(analytics.public(row)))
    if padding < 0:
        raise ValueError('target is smaller than the fixed catalog row')
    return prefix + 'x' * padding


def feed_targets(target, count=None):
    if str(target) == 'late-oversize':
        count = 12 if count is None else int(count)
        if not 12 <= count <= 10000:
            raise ValueError('late-oversize needs 12 to 10000 rows')
        # Equal ingestion timestamps imply article_id ordering in native catalog.
        last = max(range(count), key=article_id)
        return [61441 if index == last else 2048 for index in range(count)]
    values = [int(value) for value in str(target).split(',')]
    if count is None:
        count = len(values)
    count = int(count)
    if not 1 <= count <= 10000 or len(values) not in (1, count):
        raise ValueError('provide one target or one target per row, up to 10000 rows')
    return values * count if len(values) == 1 else values


def feed(target, count=None):
    entries = []
    for index, size in enumerate(feed_targets(target, count)):
        title = padded_title(index, size)
        entries.append(f'<entry><id>{source_article_id(index)}</id><title>{escape(title)}</title>'
            f'<link href="https://example.test/pressure/{index:06d}"/>'
            '<updated>deliberately-invalid</updated></entry>')
    return ('<?xml version="1.0" encoding="UTF-8"?>'
            '<feed xmlns="http://www.w3.org/2005/Atom"><title>Pressure fixture</title>'
            + ''.join(entries) + '</feed>').encode('utf-8')


def observed_layout(directory):
    paths = list(directory.rglob('*'))
    return dict(files=sum(p.is_file() for p in paths),
        directories=1 + sum(p.is_dir() for p in paths),
        bytes=sum(p.stat().st_size for p in paths if p.is_file()),
        analyticalFileCounts={table: len(list((directory / table).glob('*.parquet')))
                              for table in metadata.TABLES},
        completionPresent=(directory / 'complete.json').is_file())


def summarize_catalog(rows):
    sizes = [len(canonical_bytes(analytics.public(row))) for row in rows]
    return dict(count=len(sizes), minCanonicalRowBytes=min(sizes, default=0),
                maxCanonicalRowBytes=max(sizes, default=0),
                rowBytes=[dict(articleId=row['article_id'], bytes=size)
                          for row, size in zip(rows, sizes)])


def catalog_summary_fields(summary):
    oversized = [rank for rank, row in enumerate(summary['rowBytes'], 1) if row['bytes'] > 61440]
    return dict(catalogRows=summary['count'], catalogMinBytes=summary['minCanonicalRowBytes'],
                catalogMaxBytes=summary['maxCanonicalRowBytes'], oversizedCatalogRows=len(oversized),
                oversizedFirstRank=oversized[0] if oversized else None)


def bounded_catalog(conn):
    """Measure every fixture row, refusing fixtures larger than 100 rows."""
    cursor = conn.execute('SELECT ' + ','.join(analytics.CATALOG_COLUMNS) + ' FROM '
        + metadata.CATALOG + ' ORDER BY last_observed_at DESC, article_id ASC LIMIT 101')
    rows = [dict(zip(analytics.CATALOG_COLUMNS, row, strict=True)) for row in cursor.fetchall()]
    if len(rows) > 100:
        raise ValueError('pressure inspection supports at most 100 catalog rows')
    return rows


def read_only_snapshot(directory):
    return {str(path.relative_to(directory)): (stat.st_dev, stat.st_ino, stat.st_size, stat.st_mtime_ns)
            for path in [directory, *sorted(directory.rglob('*'))]
            if path.exists() for stat in [path.stat()]}


def inspection(command, *, rejected=False):
    command = json.loads(Path(command).read_bytes())
    fingerprint = command['prepared']['fingerprint']
    root = Path(command['content']['root'])
    directory = metadata.publication_directory(root, fingerprint)
    before = read_only_snapshot(directory)
    result = dict(nativeVerified=not rejected, verified=not rejected, nativeLayout=observed_layout(directory))
    if rejected:
        # No marker fabrication or writer use: inspect retained native output only.
        if (directory / 'complete.json').exists():
            raise ValueError('rejected inspection requires incomplete native output')
        with analytics.connection() as conn:
            paths = sorted(str(p) for p in (directory / metadata.CATALOG).glob('*.parquet'))
            analytics.register(conn, metadata.CATALOG, paths, metadata.SCHEMAS[metadata.CATALOG])
            rows = bounded_catalog(conn)
        result.update(inspectionKind='INCOMPLETE_NATIVE_PARQUET_NOT_VERIFIED',
                      catalog=summarize_catalog(rows))
        result.update(catalog_summary_fields(result['catalog']))
        assert read_only_snapshot(directory) == before, 'inspection changed retained output'
        return result
    kwargs = dict(gold_root=command['goldProof']['command']['gold']['root'],
                  reference_root=command['referenceBinding']['root'])
    with analytics.open_publication(root, fingerprint, **kwargs) as reader:
        reader._check()
        try:
            # Full production row validation scans every relation once, without
            # expensive dependency requalification for each coverage page.
            counts = dict(narrow_content_reports._scan_reports(reader._conn))
            result['coverage'] = analytics.public(analytics.coverage_totals(reader._conn))
            result['coverageTotals'] = result['coverage']
            result['catalog'] = summarize_catalog(bounded_catalog(reader._conn))
            assert result['catalog']['count'] == counts['catalog']
            # Exercise the real report interface once per report. These pages
            # are compatibility evidence, never the source of full-row counts.
            for name in analytics.REPORTS:
                reader.report(name, limit=1, offset=0)
            result['reportCounts'] = counts
            result['referenceCoverageRows'] = counts['reference_coverage']
            result.update(catalog_summary_fields(result['catalog']))
        finally:
            reader._check()
    assert read_only_snapshot(directory) == before, 'inspection changed retained output'
    return result


if __name__ == '__main__':
    mode, *args = sys.argv[1:]
    if mode == 'reference':
        output = canonical_bytes(reference(*args))
    elif mode == 'feed':
        output = feed(*args)
    elif mode in ('inspection', 'inspect', 'inspect-rejected', 'inspection-rejected', 'incomplete'):
        output = canonical_bytes(inspection(args[0], rejected='rejected' in mode or mode == 'incomplete'))
    else:
        raise ValueError('unknown pressure fixture mode')
    sys.stdout.buffer.write(output)
