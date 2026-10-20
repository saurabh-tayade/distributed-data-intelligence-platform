"""Small literal Gold consumer fixtures; no live or retained publisher data."""
from datetime import date, datetime
from hashlib import sha256
from pathlib import Path

import duckdb

from publication_metadata import canonical_bytes
import content_metadata as m
from gold_analytics import ARTICLE_SCHEMA, DAILY_SCHEMA

CSV = (b'source_id,publisher_name,publisher_category,declared_language,country_code\n'
       b'alpha,Alpha Press,science,en,CA\nbeta,Beta Press,,,\ndelta,Delta Press,culture,fr,GB\n')
IDS = ('cd3921bf8c02907ea32b983e7acdc8949150d987a082a933e3f35a85c9a7cef1',
       '7a4b3986539d368dff0348f9cc3ea33be6baf80143d7edf7df8aa0a65ee58907',
       '169632af4e4ba1a58d6fc493b1afbad6d647b21ec7ed5b82fef933b82fcb8076',
       '3dd4843dc774fde61ebd8ffce4439d91ec199ac1a1541cba6a985b7f2c310247')


def article_rows():
    result = []
    for source, guid, number, entry, title, first, last, published, observations, warnings in (
        ('alpha', 'shared', 2, 0, 'Alpha latest', '2026-10-12T23:59:59.999999',
         '2026-10-13T00:00:00.000001', None, 2, ['INVALID_PUBLISHED_AT']),
        ('alpha', 'second', 1, 1, 'Alpha second', '2026-10-12T23:59:59.999999',
         '2026-10-12T23:59:59.999999', '2026-10-10T12:00:00', 1, []),
        ('beta', 'shared', 3, 0, 'Beta shared', '2026-10-13T00:00:00',
         '2026-10-13T00:00:00', None, 1, []),
        ('gamma', 'third', 4, 0, 'Gamma third', '2026-10-13T00:00:00.000001',
         '2026-10-13T00:00:00.000001', '2026-10-11T00:00:00', 1, [])):
        row = dict(contract_version=1,
                   article_id=sha256(canonical_bytes(['v1', source, 'rss_guid', guid])).hexdigest(),
                   source_id=source, identifier_kind='rss_guid', source_article_id=guid,
                   source_uri=f'https://example.invalid/{source}/feed', feed_format='rss',
                   title=title, url=None, summary=None,
                   published_at=datetime.fromisoformat(published) if published else None,
                   updated_at=None, quality_warnings=warnings,
                   first_observed_at=datetime.fromisoformat(first), last_observed_at=datetime.fromisoformat(last),
                   observation_count=observations, distinct_ingestion_count=observations,
                   selected_silver_contract_version=1,
                   selected_ingestion_id=f'00000000-0000-0000-0000-{number:012d}',
                   selected_ingested_at=datetime.fromisoformat(last), selected_entry_index=entry,
                   selected_input_uri=f's3://local/{source}/{number}', selected_payload_sha256=f'{number:064x}',
                   selected_payload_byte_length=10, selected_bronze_bucket='local',
                   selected_bronze_payload_key=f'{source}/{number}/payload.xml',
                   selected_bronze_metadata_key=f'{source}/{number}/metadata.json')
        result.append(row)
    return result


def gold_fixture(root, *, spark=None, empty=False, rejected=0, older=False,
                 large_counts=False, large_text=False, two_warnings=False):
    """Direct Gold rows with full accepted consumer metadata, not a Silver production proof."""
    root = Path(root)
    manifest = dict(manifest_version=1, gold_contract_versions=dict.fromkeys(m.TABLES, 1),
                    gold_processor_revision=1, silver_contract_version=1, silver_processor_revision=1)
    manifest['gold_contract_versions'] = {'gold_articles': 1, 'gold_source_daily': 1}
    path = 'source_id=fixture/complete.json'
    file = dict(path='source_id=fixture/accepted/part.parquet', byte_length=1, sha256='2' * 64)
    accepted = 0 if empty else 6 if older else 5
    if large_counts:
        accepted = 2**53+4
    manifest['inputs'] = [dict(marker_path=path, marker_sha256='3' * 64, representative_path=path,
        identity=dict(metadata_sha256='0' * 64, payload_sha256='1' * 64, contract_version=1,
                      processor_revision=1, bucket='local', metadata_key='fixture/metadata.json'),
        counts=dict(accepted=accepted, rejected=rejected), files=dict(accepted=[file],
        rejected=[file | {'path': 'source_id=fixture/rejected/part.parquet'}]))]
    raw = canonical_bytes(manifest)
    identity = sha256(raw).hexdigest()
    directory = root / 'v1' / 'processor_revision=1' / f'input_set={identity}'
    directory.mkdir(parents=True)
    (directory / 'inputs.json').write_bytes(raw)
    articles = [] if empty else article_rows()
    daily = [] if empty else [
        (1, 'alpha', date(2026, 10, 12), 2, 2, 1, 0, datetime(2026, 10, 12, 23, 59, 59, 999999)),
        (1, 'alpha', date(2026, 10, 13), 1, 1, 1, 1, datetime(2026, 10, 13, 0, 0, 0, 1)),
        (1, 'beta', date(2026, 10, 13), 1, 1, 1, 0, datetime(2026, 10, 13)),
        (1, 'gamma', date(2026, 10, 13), 1, 1, 1, 0, datetime(2026, 10, 13, 0, 0, 0, 1))]
    if older and not empty:
        articles[0].update(first_observed_at=datetime(2026, 10, 11, 23, 59, 59, 999999),
                           observation_count=3, distinct_ingestion_count=3)
        daily.append((1, 'alpha', date(2026, 10, 11), 1, 1, 1, 0,
                      datetime(2026, 10, 11, 23, 59, 59, 999999)))
    if large_counts:
        articles[0]['observation_count'] = 2**53+1
        daily[0] = (*daily[0][:3], 2**53+1, *daily[0][4:])
    if large_text:
        articles[0]['title'] = 'X' * (m.PAGE_BYTES+1)
    if two_warnings:
        articles[0]['quality_warnings'] = ['INVALID_PUBLISHED_AT','INVALID_UPDATED_AT']
        daily[1] = (*daily[1][:6], 2, daily[1][7])
    files = {}
    for table, schema, rows in (('gold_articles', ARTICLE_SCHEMA, articles),
                                 ('gold_source_daily', DAILY_SCHEMA, daily)):
        destination = directory / table
        if spark is not None:
            from gold import GOLD_ARTICLES_SCHEMA, GOLD_SOURCE_DAILY_SCHEMA
            spark_schema = GOLD_ARTICLES_SCHEMA if table == 'gold_articles' else GOLD_SOURCE_DAILY_SCHEMA
            frame = spark.createDataFrame(rows, spark_schema)
            frame.coalesce(1).write.mode('errorifexists').parquet(str(destination))
        else:
            destination.mkdir()
            with duckdb.connect() as connection:
                connection.execute('CREATE TABLE fixture (' + ','.join(f'{n} {t}' for n,t in schema) + ')')
                if rows:
                    values = [tuple(r[n] for n,t in schema) if isinstance(r, dict) else r for r in rows]
                    connection.executemany('INSERT INTO fixture VALUES (' + ','.join('?' for _ in schema) + ')', values)
                connection.table('fixture').write_parquet(str(destination / 'part.parquet'))
        files[table] = [dict(path=p.relative_to(directory).as_posix(), byte_length=p.stat().st_size,
                             sha256=sha256(p.read_bytes()).hexdigest())
                        for p in sorted(destination.iterdir()) if p.suffix == '.parquet']
    marker = dict(publication_version=1, gold_contract_versions=manifest['gold_contract_versions'],
        processor_revision=1, input_set_fingerprint=identity, input_manifest_sha256=identity,
        input_counts=dict(accepted=accepted, accepted_before_replay_collapse=accepted, rejected=rejected),
        duplicates=dict(duplicate_publications=0, duplicate_accepted_rows=0),
        output_counts=dict(gold_articles=len(articles), gold_source_daily=len(daily)), files=files)
    (directory / 'complete.json').write_bytes(canonical_bytes(marker))
    return identity
