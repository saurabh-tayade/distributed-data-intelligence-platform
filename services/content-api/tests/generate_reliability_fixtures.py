"""Spark-free HTTP parity fixtures from the shared literal Reliability G1 publication.

Run with PYTHONPATH=processing:processing/tests and requirements-analytics.txt.
Expected envelopes are captured from the actual native CLI, not recreated in Java.
"""
import argparse
import json
from pathlib import Path
import shutil
import subprocess
import sys

import reliability_format_metadata as metadata
from reliability_format_fixtures import completed

START = '2026-10-01T00:00:00.000000Z'
END = '2026-10-01T00:10:00.000000Z'
SCRIPT = Path(__file__).resolve().parents[3] / 'processing' / 'reliability_format_analytics.py'


def cli(root, publication, options):
    command = [sys.executable, str(SCRIPT), '--root', str(root / 'output'),
               '--publication', publication, '--batch-root', str(root / 'batches'),
               '--reference-root', str(root / 'references'), '--report', 'requests_5m']
    for key, value in options.items():
        command += ['--' + key.replace('_', '-'), str(value)]
    return subprocess.run(command, capture_output=True, text=True, check=False, timeout=30)


def scenarios():
    defaults = dict(start=START, end=END)
    return [
        ('defaults', defaults),
        ('first-page', defaults | dict(limit=1, offset=0)),
        ('final-page', defaults | dict(limit=1, offset=1)),
        ('at-end', defaults | dict(limit=1, offset=2)),
        ('beyond-end', defaults | dict(limit=1, offset=3)),
        ('maximum-offset', defaults | dict(limit=1000, offset=1_000_000)),
        ('service', defaults | dict(service_id='svc00')),
        ('environment', defaults | dict(environment='test')),
        ('service-and-environment', defaults | dict(service_id='svc01', environment='test')),
        ('empty-service', defaults | dict(service_id='absent-service')),
        ('empty-environment', defaults | dict(environment='production')),
        ('empty-combined-filter', defaults | dict(service_id='svc00', environment='production')),
        ('inclusive-start-exclusive-end', dict(start=START, end='2026-10-01T00:05:00.000000Z')),
        ('next-window-start', dict(start='2026-10-01T00:05:00.000000Z', end=END)),
        ('exactly-31-days', dict(start=START, end='2026-11-01T00:00:00.000000Z')),
        ('pre-epoch', dict(start='1969-12-31T23:55:00.000000Z', end='1970-01-01T00:00:00.000000Z')),
        ('pre-epoch-filtered', dict(start='1969-12-31T23:55:00.000000Z',
                                    end='1970-01-01T00:00:00.000000Z', service_id='svc09', environment='production')),
    ]


def generate(output):
    output.mkdir(parents=True, exist_ok=False)
    publications, expected, failures = {}, [], []
    for fmt in ('JSONL', 'PARQUET'):
        root = output / (fmt.lower() + "'s snapshots")
        root.mkdir()
        _, publication, _, _ = completed(root, fmt)
        publications[fmt] = dict(root=root.name, publication=publication)
        for name, options in scenarios():
            result = cli(root, publication, options)
            if result.returncode:
                raise AssertionError(f'{fmt}/{name}: native CLI rejected literal fixture: {result.stderr}')
            envelope = json.loads(result.stdout)
            assert envelope['status'] == 'OK'
            expected.append(dict(input_format=fmt, name=name, options=options, envelope=envelope))
        # Copies alone are damaged. The valid fixtures and native producer code stay unchanged.
        for case in ('missing-marker', 'corrupt-parquet', 'tampered-manifest',
                     'missing-batch', 'missing-reference', 'corrupt-reference'):
            damaged_root = output / 'cases' / fmt / case
            shutil.copytree(root, damaged_root)
            directory = metadata.publication_directory(damaged_root / 'output', publication)
            if case == 'missing-marker':
                (directory / 'complete.json').unlink()
            elif case == 'corrupt-parquet':
                next((directory / metadata.METRICS).glob('*.parquet')).write_bytes(b'not parquet')
            elif case == 'tampered-manifest':
                with (directory / 'inputs.json').open('ab') as stream:
                    stream.write(b' ')
            elif case == 'missing-batch':
                shutil.rmtree(damaged_root / 'batches')
            elif case == 'missing-reference':
                shutil.rmtree(damaged_root / 'references')
            elif case == 'corrupt-reference':
                next((damaged_root / 'references').rglob('complete.json')).write_text('{invalid')
            result = cli(damaged_root, publication, dict(start=START, end=END))
            assert result.returncode != 0, f'Native CLI accepted damaged {fmt}/{case}'
            assert not result.stdout, f'Native CLI leaked success output for {fmt}/{case}'
            failures.append(dict(input_format=fmt, case=case, root=str(damaged_root.relative_to(output)),
                                 publication=publication, native_error=json.loads(result.stderr)['code']))
    # Actual Python import failure: executable and fixed entry point exist, dependency modules do not.
    unavailable = output / 'missing native dependency'
    unavailable.mkdir()
    shutil.copyfile(SCRIPT, unavailable / SCRIPT.name)
    reference = dict(publications=publications, scenarios=expected, failures=failures)
    (output / 'reference.json').write_text(json.dumps(reference, indent=2) + '\n', encoding='utf-8')
    print(json.dumps(dict(output=str(output), scenarios=len(expected), damaged_cases=len(failures))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    generate(parser.parse_args().output.resolve())
