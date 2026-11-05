"""Literal Content HTTP fixtures published by the existing Spark producer.

Run with PYTHONPATH=processing:processing/tests and processing/requirements.txt.
Generation uses Spark; CLI and HTTP reads are Spark-free.
Native CLI envelopes are the HTTP oracle. Only copied fixtures are damaged.
"""
import argparse
from hashlib import sha256
import json
import os
import time
from pathlib import Path
import shutil
import subprocess
import sys

import content_analytics as analytics
import content_metadata as m
from content_fixtures import CSV, gold_fixture
from publication_metadata import canonical_bytes
import publisher_reference as reference

SCRIPT = Path(__file__).resolve().parents[3] / 'processing/content_analytics.py'


def completed(root, *, spark, empty=False, large_counts=False):
    from content_publication import publish
    root.mkdir(parents=True, exist_ok=True)
    gold = gold_fixture(root / 'gold', spark=spark, empty=empty, large_counts=large_counts,
                        two_warnings=not empty)
    (root / 'references').mkdir()
    (root / 'source.csv').write_bytes(CSV)
    (root / 'vocabulary.json').write_bytes(canonical_bytes(reference.VOCABULARY))
    fingerprint = reference.publish(root / 'references', root / 'source.csv', root / 'vocabulary.json',
                                    'fixture-publishers', 'r1', 'example-team')['snapshot_fingerprint']
    (root / 'output').mkdir()
    publication = publish(spark, output_root=root / 'output', gold_root=root / 'gold', gold_fingerprint=gold,
        reference_root=root / 'references', reference_id='fixture-publishers', revision='r1',
        reference_fingerprint=fingerprint)['product_fingerprint']
    with analytics.open_publication(root / 'output', publication, gold_root=root / 'gold', reference_root=root / 'references'):
        pass
    return publication


def cli(root, publication, options):
    command = [sys.executable, str(SCRIPT), '--root', str(root / 'output'), '--publication', publication,
               '--gold-root', str(root / 'gold'), '--reference-root', str(root / 'references'), '--report', 'catalog']
    for key, value in options.items():
        command += ['--' + key.replace('_', '-'), str(value)]
    return subprocess.run(command, capture_output=True, text=True, check=False, timeout=30)


def scenarios():
    return [('defaults', {'limit':100}), ('first-page',dict(limit=1)), ('final-page',dict(limit=1,offset=3)),
        ('at-end',dict(limit=1,offset=4)), ('beyond-end',dict(limit=1,offset=5)),
        ('maximum-offset',dict(limit=1000,offset=1_000_000)), ('source',dict(source='alpha')),
        ('from',dict(start_date='2026-10-13')), ('to',dict(end_date='2026-10-12')),
        ('inclusive-date',dict(start_date='2026-10-12',end_date='2026-10-12')),
        ('matched',dict(reference_status='MATCHED')), ('missing',dict(reference_status='MISSING')),
        ('category',dict(publisher_category='science')), ('language',dict(declared_language='en')),
        ('country',dict(country_code='CA')), ('matched-null-attributes',dict(source='beta')),
        ('combined',dict(source='alpha',start_date='2026-10-12',end_date='2026-10-13',reference_status='MATCHED',
                         publisher_category='science',declared_language='en',country_code='CA',limit=1,offset=1)),
        ('no-source',dict(source='absent')), ('no-combination',dict(reference_status='MISSING',country_code='CA')),
        ('empty-date-range',dict(start_date='1969-12-31',end_date='1970-01-01'))]


def damage(root, publication, case, spark):
    directory = m.publication_directory(root / 'output', publication)
    if case == 'missing-marker':
        (directory / 'complete.json').unlink()
    elif case == 'tampered-manifest':
        with (directory / 'inputs.json').open('ab') as stream:
            stream.write(b' ')
    elif case in ('missing-gold', 'missing-reference'):
        shutil.rmtree(root / ('gold' if case == 'missing-gold' else 'references'))
    elif case in ('gold-daily', 'reference-csv', 'content-daily'):
        path = (next((root / 'gold').rglob('gold_source_daily/*.parquet')) if case == 'gold-daily' else
                next((root / 'references').rglob('reference.csv')) if case == 'reference-csv' else
                next((directory / m.DAILY).glob('*.parquet')))
        path.write_bytes(b'corrupt private diagnostic')
    else:
        # A valid Parquet file with updated inventory but a semantic error on the LAST row.
        # limit=1 and source=absent must still reject it, proving full-catalog verification.
        path = next((directory / m.CATALOG).glob('*.parquet'))
        temporary = root / 'replacement'
        from pyspark.sql import functions as F
        frame = spark.read.parquet(str(directory / m.CATALOG))
        frame.withColumn('title', F.when(F.col('source_article_id')=='second', F.lit('wrong')).otherwise(F.col('title'))) \
            .coalesce(1).write.parquet(str(temporary))
        next(temporary.glob('*.parquet')).replace(path)
        shutil.rmtree(temporary)
        marker = json.loads((directory / 'complete.json').read_text())
        marker['files'][m.CATALOG] = [dict(path=path.relative_to(directory).as_posix(), byte_length=path.stat().st_size,
                                         sha256=sha256(path.read_bytes()).hexdigest())]
        (directory / 'complete.json').write_bytes(canonical_bytes(marker))


def generate(output):
    output.mkdir(parents=True, exist_ok=False)
    # PySpark converts naive fixture datetimes using the Python process timezone.
    os.environ["TZ"] = "UTC"
    time.tzset()
    from silver import session
    spark = session()
    spark.sparkContext.setLogLevel("ERROR")
    publications, expected, failures = {}, [], []
    for kind in ('standard', 'empty', 'large-counts'):
        root = output / (kind + "'s snapshots")
        publication = completed(root, spark=spark, empty=kind=='empty', large_counts=kind=='large-counts')
        publications[kind] = dict(root=root.name, publication=publication)
        for name, options in scenarios():
            options = {'limit':100} | options
            result = cli(root, publication, options)
            assert result.returncode == 0, result.stderr
            envelope = json.loads(result.stdout)
            if name == 'defaults':
                rows = envelope['rows']
                assert [r['source_id'] for r in rows] == ([] if kind == 'empty' else ['gamma','alpha','beta','alpha'])
                if rows:
                    assert rows[0]['reference_status'] == 'MISSING' and rows[0]['publisher_name'] is None
                    assert rows[2]['reference_status'] == 'MATCHED' and rows[2]['publisher_category'] is None
                    assert rows[1]['observation_count'] == ('9007199254740993' if kind == 'large-counts' else '2')
                    assert rows[1]['quality_warnings'] == ['INVALID_PUBLISHED_AT','INVALID_UPDATED_AT']
                    assert rows[1]['last_observed_at'] == '2026-10-13T00:00:00.000001Z'
            expected.append(dict(kind=kind, name=name, options=options, envelope=envelope))
        if kind != 'standard':
            continue
        for case in ('missing-marker','tampered-manifest','missing-gold','missing-reference',
                     'gold-daily','reference-csv','content-daily','semantic-last-row'):
            damaged = output / 'cases' / case
            shutil.copytree(root, damaged)
            damage(damaged, publication, case, spark)
            for options in ({'limit':1}, {'source':'absent'}, {'offset':1_000_000}):
                result = cli(damaged, publication, options)
                assert result.returncode != 0 and not result.stdout, case
            failures.append(dict(case=case, root=str(damaged.relative_to(output)), publication=publication))
    spark.stop()
    unavailable = output / 'missing native dependency'
    unavailable.mkdir()
    shutil.copyfile(SCRIPT, unavailable / SCRIPT.name)
    (output / 'reference.json').write_text(json.dumps(dict(publications=publications,scenarios=expected,failures=failures),indent=2)+'\n')
    print(json.dumps(dict(output=str(output), scenarios=len(expected), damaged_cases=len(failures))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    generate(parser.parse_args().output.resolve())
