"""Literal Spark-free coverage edges, independent of producer transforms.

Extends G1 with duplicate-only, empty and multi-reason rejected batches.
The native verifier establishes every raw/output correspondence.
"""
from hashlib import sha256
from json import loads
from pathlib import Path

from publication_metadata import canonical_bytes
from reliability_format_fixtures import completed, g1, reference, register
import reliability_format_inputs as inputs
import reliability_format_metadata as m
from reliability_format_parquet import connection


def completed_coverage(root, fmt, variant='coverage'):
    root = Path(root)
    output, original, batches, refs = completed(root, fmt)
    old = m.publication_directory(output, original)
    # Preserve exact SQL types; timestamps become integer microseconds here.
    with connection() as db:
        tables = {}
        for table, fields in m.SCHEMAS.items():
            projection = ','.join('epoch_us("' + n + '")' if kind == 'timestamp' else '"' + n + '"' for n, kind in fields)
            tables[table] = [dict(zip([n for n, _ in fields], row)) for row in
                             db.execute('SELECT ' + projection + ' FROM read_parquet(?)', [str(old / table / 'part.parquet')]).fetchall()]
    manifest = loads((old / 'inputs.json').read_text())
    selected = [('bench-source', 'b00', tables[m.OCCURRENCES][0]['batch_fingerprint'])]
    added = []
    valid = g1()[0]
    rejected = valid | dict(event_id='00000000-0000-0000-0000-000000000199', event_time=None, duration_ms=-1)
    specs = [('b01', [valid, valid], '2026-10-01T00:00:00.000000Z'),
             ('b02', [], '2026-10-02T00:05:00.000000Z'),
             ('b03', [rejected], '2026-10-02T00:05:00.000000Z'),
             ('b04', [valid], '0001-01-01T00:00:00.000000Z')]
    for batch_id, rows, receipt in specs:
        _, registered = register(root, fmt, rows, batch_id=batch_id, received_at=receipt)
        batch = inputs.open_batch(batches, fmt, 'bench-source', batch_id, expected_fingerprint=registered['batch_fingerprint'])
        selected.append(('bench-source', batch_id, batch.fingerprint))
        file = batch.manifest['files'][0]
        offset = 0
        for ordinal, row in enumerate(rows):
            raw = canonical_bytes(row) + b'\n'
            is_valid = batch_id != 'b03'
            business = tuple(m.time_us(row[n]) if n == 'event_time' else row[n] for n in m.EVENT_FIELDS) if is_valid else None
            added.append(dict(zip([n for n, _ in m.SCHEMAS[m.OCCURRENCES]],
                (2, batch.fingerprint, 'bench-source', batch_id, registered['ingestion_id'], 'events', file['sha256'], fmt,
                 ordinal, ordinal + 1 if fmt == 'JSONL' else None, offset if fmt == 'JSONL' else None,
                 len(raw) if fmt == 'JSONL' else None, 0 if fmt == 'PARQUET' else None,
                 ordinal if fmt == 'PARQUET' else None, m.time_us(receipt),
                 'DUPLICATE' if is_valid else 'REJECTED', 'svc00', row['event_id'],
                 sha256(canonical_bytes(business)).hexdigest() if is_valid else None,
                 [] if is_valid else ['FIELD_NULL', 'FIELD_VALUE']))))
            offset += len(raw)
    tables[m.OCCURRENCES].extend(added)
    target = next(row for row in tables[m.EVENTS] if row['service_id'] == 'svc00' and row['event_id'] == valid['event_id'])
    target.update(valid_delivery_count=5, first_received_at=m.MIN_US)
    if variant in ('empty-events', 'rejected-only'):
        batch_id = 'b02' if variant == 'empty-events' else 'b03'
        selected = [selection for selection in selected if selection[1] == batch_id]
        tables[m.OCCURRENCES] = [row for row in added if row['batch_id'] == batch_id]
        tables[m.EVENTS] = []
        tables[m.METRICS] = []
    reference_id = manifest['reference']['reference_id']
    revision = manifest['reference']['revision']
    reference_fingerprint = manifest['reference']['snapshot_fingerprint']
    if variant == 'header-reference':
        _, ref = reference(root, empty=True, revision='empty')
        revision, reference_fingerprint = 'empty', ref['snapshot_fingerprint']
        for table in (m.EVENTS, m.METRICS):
            for row in tables[table]:
                row.update(service_name=None, owner_team=None, service_tier=None, reference_status='MISSING',
                           reference_revision=revision, reference_fingerprint=reference_fingerprint)
    deps = inputs.verify_dependencies(batch_root=batches, input_format=fmt, batches=selected,
        reference_root=refs, reference_id=reference_id, revision=revision, reference_fingerprint=reference_fingerprint)
    directory = m.publication_directory(output, deps.fingerprint)
    directory.mkdir(parents=True)
    m.write(directory / 'inputs.json', deps.manifest_bytes)
    types = {'integer': 'INTEGER', 'long': 'BIGINT', 'timestamp': 'BIGINT', 'string': 'VARCHAR',
             'boolean': 'BOOLEAN', 'double': 'DOUBLE', 'decimal(38,0)': 'DECIMAL(38,0)', 'array<string>': 'VARCHAR[]'}
    with connection() as db:
        for table, rows in tables.items():
            fields = m.SCHEMAS[table]
            db.execute('CREATE TABLE seed(' + ','.join('"' + n + '" ' + types[k] for n, k in fields) + ')')
            if rows:
                db.executemany('INSERT INTO seed VALUES (' + ','.join('?' for _ in fields) + ')',
                               [[row[n] for n, _ in fields] for row in rows])
            projection = ','.join('make_timestamptz("' + n + '") AS "' + n + '"' if k == 'timestamp' else '"' + n + '"' for n, k in fields)
            (directory / table).mkdir()
            db.execute('COPY (SELECT ' + projection + ' FROM seed) TO ? (FORMAT PARQUET,COMPRESSION SNAPPY)',
                       [str(directory / table / 'part.parquet')])
            db.execute('DROP TABLE seed')
    evidence = m.capture(directory, {'inputs.json': m.MANIFEST_LIMIT}, m.TABLES, 'PUBLICATION_INVALID', 'PUBLICATION_CHANGED')
    physical = len(tables[m.OCCURRENCES])
    rejected_count = sum(row['disposition'] == 'REJECTED' for row in tables[m.OCCURRENCES])
    unique = len(tables[m.EVENTS])
    marker = dict(completion_version=2, product_id=m.PRODUCT, product_fingerprint=deps.fingerprint,
        manifest_byte_length=len(deps.manifest_bytes), occurrence_counts=dict(physical_occurrences=physical,
        valid_occurrences=physical-rejected_count, unique_valid_events=unique,
        identical_duplicate_occurrences=physical-rejected_count-unique, rejected_occurrences=rejected_count),
        output_counts={table: len(rows) for table, rows in tables.items()}, files=evidence.inventory(), completed_at=m.now())
    m.write(directory / 'complete.json', canonical_bytes(marker))
    return output, deps.fingerprint, batches, refs
