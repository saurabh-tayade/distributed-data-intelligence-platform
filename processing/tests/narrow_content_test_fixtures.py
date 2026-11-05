"""Tiny real native-consumer fixtures for additive narrow Content tests.

Direct DuckDB-authored Gold/Content fixtures are not Spark producer evidence.
"""
from hashlib import sha256
import json
from pathlib import Path

import duckdb

import content_dependencies as dependencies
import content_metadata as m
from content_fixtures import CSV, gold_fixture
from publication_metadata import canonical_bytes
import publisher_reference as reference


def _legacy_timestamps(path):
    """Emit the native-accepted legacy TIMESTAMP_MICROS fixture representation.

    DuckDB adds local-time logical annotations which Content deliberately rejects.
    Preserve the legacy UTC TIMESTAMP_MICROS annotation and all data, removing
    only its redundant newer logical annotation in the Thrift compact footer.
    This is a fixture encoding helper, not production migration or validation.
    """
    raw = path.read_bytes()
    size = int.from_bytes(raw[-8:-4], 'little')
    footer = raw[-8-size:-8]
    def integer(at):
        value = shift = 0
        while True:
            byte = footer[at]; at += 1
            value |= (byte & 127) << shift
            if byte < 128:
                return value, at
            shift += 7
    def sequence(at):
        header = footer[at]; at += 1
        count, kind = header >> 4, header & 15
        if count == 15:
            count, at = integer(at)
        return count, kind, at
    def skip(kind, at):
        if kind in (1, 2): return at
        if kind == 3: return at + 1
        if kind in (4, 5, 6): return integer(at)[1]
        if kind == 7: return at + 8
        if kind == 8:
            count, at = integer(at)
            return at + count
        if kind in (9, 10):
            count, child, at = sequence(at)
            for _ in range(count): at = at + 1 if child in (1, 2) else skip(child, at)
            return at
        if kind == 11:
            count, at = integer(at)
            if count:
                kinds = footer[at]; at += 1
                for _ in range(count):
                    at = skip(kinds >> 4, at); at = skip(kinds & 15, at)
            return at
        if kind == 12: return fields(at)[1]
        raise AssertionError('unexpected compact type')
    def fields(at):
        result, previous = [], 0
        while footer[at]:
            header = footer[at]; at += 1
            kind = header & 15
            if header >> 4:
                number = previous + (header >> 4)
            else:
                zigzag, at = integer(at)
                number = (zigzag >> 1) ^ -(zigzag & 1)
            end = skip(kind, at)
            result.append((number, kind, at, end))
            previous, at = number, end
        return result, at + 1
    def encode(items, replacement=None):
        result, previous = bytearray(), 0
        for number, kind, start, end in items:
            delta = number - previous
            assert 0 < delta <= 15
            result.append((delta << 4) | kind)
            result.extend(replacement[number] if replacement and number in replacement else footer[start:end])
            previous = number
        return bytes(result) + b'\0'
    outer, end = fields(0)
    assert end == len(footer)
    _, kind, start, _ = next(item for item in outer if item[0] == 2)
    assert kind == 9
    count, child, at = sequence(start)
    assert child == 12
    schema = bytearray(footer[start:at])
    for _ in range(count):
        item, at = fields(at)
        legacy = next((integer(s)[0] >> 1 for n,k,s,e in item if n == 6), None)
        schema.extend(encode([field for field in item if not (legacy == 10 and field[0] == 10)]))
    rewritten = encode(outer, {2: bytes(schema)})
    path.write_bytes(raw[:-8-size] + rewritten + len(rewritten).to_bytes(4, 'little') + b'PAR1')
    with duckdb.connect() as conn:
        physical = conn.execute('SELECT converted_type,logical_type FROM parquet_schema(?)', [str(path)]).fetchall()
        assert all(logical is None for converted,logical in physical if converted == 'TIMESTAMP_MICROS')


def snapshot(root):
    return {p.relative_to(root).as_posix():
            (m.stamp(p.lstat()), p.read_bytes() if p.is_file() else None)
            for p in (root, *sorted(root.rglob('*')))}


def inputs(base, *, empty=False, change=None, csv=CSV, large_counts=False):
    gold = base / 'gold'
    identity = gold_fixture(gold, empty=empty, large_counts=large_counts)
    directory = gold / 'v1' / 'processor_revision=1' / f'input_set={identity}'
    if change:
        file = directory / 'gold_articles' / 'part.parquet'
        with duckdb.connect() as conn:
            relation = conn.read_parquet(str(file))
            columns = relation.columns
            rows = [dict(zip(columns, row, strict=True)) for row in relation.fetchall()]
            change(rows)
            conn.execute('CREATE TABLE changed (' + ','.join(n + ' ' + t for n,t in m.GOLD_SCHEMA) + ')')
            conn.executemany('INSERT INTO changed VALUES (' + ','.join('?' for _ in columns) + ')',
                             [tuple(row[n] for n in columns) for row in rows])
            file.unlink()
            conn.table('changed').write_parquet(str(file))
    marker_path = directory / 'complete.json'
    marker = json.loads(marker_path.read_bytes())
    for table, files in marker['files'].items():
        for item in files:
            file = directory / item['path']
            _legacy_timestamps(file)
            item.update(byte_length=file.stat().st_size, sha256=sha256(file.read_bytes()).hexdigest())
    marker_path.write_bytes(canonical_bytes(marker))
    ref = base / 'reference'
    ref.mkdir()
    source = base / 'source.csv'
    source.write_bytes(csv)
    vocabulary = base / 'vocabulary.json'
    vocabulary.write_bytes(canonical_bytes(reference.VOCABULARY))
    published = reference.publish(ref, source, vocabulary, 'fixture-publishers', 'r1', 'example-team')
    root = base / 'content'
    root.mkdir()
    return dict(output_root=root, gold_root=gold, gold_fingerprint=identity, reference_root=ref,
                reference_id='fixture-publishers', revision='r1',
                reference_fingerprint=published['snapshot_fingerprint'])


def content(args, *, complete=True):
    dependency = dependencies.verify_dependencies(**args)
    directory = m.publication_directory(args['output_root'], dependency.fingerprint)
    directory.mkdir(parents=True)
    for name, raw in (('inputs.json', dependency.manifest_bytes),
                      ('gold-inputs.json', dependency.gold.raw('inputs.json')),
                      ('gold-complete.json', dependency.gold.raw('complete.json'))):
        (directory / name).write_bytes(raw)
    with duckdb.connect() as conn:
        conn.read_parquet(list(dependency.paths('gold_articles')), hive_partitioning=False).create_view('gold')
        conn.execute('CREATE TABLE reference_rows (source_id VARCHAR, publisher_name VARCHAR, '
                     'publisher_category VARCHAR, declared_language VARCHAR, country_code VARCHAR)')
        rows = [tuple(getattr(row, n) for n in ('source_id', *m.ATTRIBUTES)) for row in dependency.reference.rows]
        if rows:
            conn.executemany('INSERT INTO reference_rows VALUES (?,?,?,?,?)', rows)
        conn.execute(f'CREATE TABLE {m.CATALOG} AS SELECT g.*, 1::INTEGER AS content_contract_version, '
            'g.first_observed_at AS discovered_at, CAST(g.first_observed_at AS DATE) AS discovery_date, '
            + ','.join('r.' + name for name in m.ATTRIBUTES)
            + ", CASE WHEN r.source_id IS NULL THEN 'MISSING' ELSE 'MATCHED' END AS reference_status, "
            '?::VARCHAR AS reference_id, ?::VARCHAR AS reference_revision, ?::VARCHAR AS reference_fingerprint '
            'FROM gold g LEFT JOIN reference_rows r ON g.source_id=r.source_id',
            [dependency.reference.reference_id, dependency.reference.revision, dependency.reference.fingerprint])
        conn.execute(f'CREATE TABLE {m.DAILY} AS SELECT 1::INTEGER AS contract_version, source_id, discovery_date, '
            'count(*)::BIGINT AS discovered_article_count, '
            "count(*) FILTER (WHERE reference_status='MISSING')::BIGINT AS articles_missing_reference_count "
            f'FROM {m.CATALOG} GROUP BY source_id,discovery_date')
        counts = {}
        for table in m.TABLES:
            destination = directory / table
            destination.mkdir()
            conn.table(table).write_parquet(str(destination / 'part.parquet'))
            _legacy_timestamps(destination / 'part.parquet')
            counts[table] = conn.execute('SELECT count(*) FROM ' + table).fetchone()[0]
    files = {table: [dict(path=f'{table}/part.parquet', byte_length=(directory / table / 'part.parquet').stat().st_size,
                         sha256=sha256((directory / table / 'part.parquet').read_bytes()).hexdigest())]
             for table in m.TABLES}
    marker = dict(completion_version=1, product_id=m.PRODUCT, product_fingerprint=dependency.fingerprint,
                  manifest_byte_length=len(dependency.manifest_bytes), output_counts=counts, files=files,
                  completed_at='2026-11-04T00:00:00.000000Z')
    if complete:
        (directory / 'complete.json').write_bytes(canonical_bytes(marker))
    return dependency, directory, marker


def catalog_row(row, *, reference_fingerprint='a' * 64):
    """Public shape with real native fields and valid bounded selection identifiers."""
    from content_analytics import CATALOG_COLUMNS
    return {name: row.get(name) for name in CATALOG_COLUMNS} | dict(
        discovered_at=row['first_observed_at'], discovery_date=row['first_observed_at'].date(),
        publisher_name='Alpha Press', publisher_category='science', declared_language='en', country_code='CA',
        reference_status='MATCHED', reference_id='fixture-publishers', reference_revision='r1',
        reference_fingerprint=reference_fingerprint)
