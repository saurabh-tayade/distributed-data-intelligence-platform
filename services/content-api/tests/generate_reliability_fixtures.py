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
from reliability_coverage_fixtures import completed_coverage

START = '2026-10-01T00:00:00.000000Z'
END = '2026-10-01T00:10:00.000000Z'
SCRIPT = Path(__file__).resolve().parents[3] / 'processing' / 'reliability_format_analytics.py'


def cli(root, publication, options, report="requests_5m"):
    command = [sys.executable, str(SCRIPT), '--root', str(root / 'output'),
               '--publication', publication, '--batch-root', str(root / 'batches'),
               '--reference-root', str(root / 'references'), '--report', report]
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


def coverage_scenarios(report):
    common = [('defaults', {}), ('first-page', dict(limit=1)), ('second-page', dict(limit=1, offset=1)),
              ('final-page', dict(limit=1, offset=4 if report == 'input_coverage' else 3)),
              ('at-end', dict(offset=5 if report == 'input_coverage' else 4)),
              ('beyond-end', dict(offset=100)), ('maximum-offset', dict(limit=1000, offset=1_000_000))]
    if report == 'input_coverage':
        return common + [('source', dict(source_id='bench-source')), ('batch', dict(batch_id='b01')),
            ('combined', dict(source_id='bench-source', batch_id='b01')),
            ('duplicates-only', dict(batch_id='b01')), ('empty-batch', dict(batch_id='b02')),
            ('multiple-reasons', dict(batch_id='b03')), ('negative-delay', dict(batch_id='b04')),
            ('unknown-source', dict(source_id='absent')), ('unknown-batch', dict(batch_id='absent'))]
    return common + [('matched', dict(service_id='svc00')), ('optional-null', dict(service_id='svc01')),
                     ('missing', dict(service_id='svc09')), ('unused', dict(service_id='svc-unused')),
                     ('unknown-service', dict(service_id='absent'))]


def capture(root, publication, report, options):
    result = cli(root, publication, options, report)
    if result.returncode:
        raise AssertionError(f'{root.name}/{report}: native CLI rejected fixture: {result.stderr}')
    envelope = json.loads(result.stdout)
    assert envelope['status'] == 'OK'
    return envelope


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
            expected.append(dict(input_format=fmt, variant='g1', root=root.name, publication=publication,
                                 report='requests_5m', name=name, options=options, envelope=envelope))
        for variant in ('coverage', 'empty-events', 'rejected-only', 'header-reference'):
            coverage_root = output / (fmt.lower() + '-' + variant)
            coverage_root.mkdir()
            _, coverage_publication, _, _ = completed_coverage(coverage_root, fmt, variant)
            for report in ('input_coverage', 'reference_coverage'):
                for name, options in coverage_scenarios(report):
                    envelope = capture(coverage_root, coverage_publication, report, options)
                    expected.append(dict(input_format=fmt, variant=variant, root=coverage_root.name,
                        publication=coverage_publication, report=report, name=name, options=options, envelope=envelope))
            # Literal edge assertions prevent accidental drift into a weaker workload.
            if variant == 'coverage':
                duplicate = capture(coverage_root, coverage_publication, 'input_coverage', dict(source_id='bench-source', batch_id='b01'))
                assert duplicate['totals']['duplicate_occurrence_count'] == '2'
                assert duplicate['totals']['duplicate_excess_in_scope'] == '1'
                assert duplicate['rows'][0]['min_receipt_delay_us'] == '0'
                reasons = capture(coverage_root, coverage_publication, 'input_coverage', dict(batch_id='b03'))['rows'][0]
                assert reasons['rejected_occurrence_count'] == '1'
                assert reasons['rejected_reason_counts']['FIELD_NULL'] == reasons['rejected_reason_counts']['FIELD_VALUE'] == '1'
                negative = capture(coverage_root, coverage_publication, 'input_coverage', dict(batch_id='b04'))['rows'][0]
                assert negative['future_occurrence_count'] == '1'
                assert int(negative['min_receipt_delay_us']) < -(2 ** 53)
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
            for report in ('input_coverage', 'reference_coverage'):
                broken = cli(damaged_root, publication, {}, report)
                assert broken.returncode != 0 and not broken.stdout
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
