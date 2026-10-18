"""Offline JDBC fixtures from the shared M2.4 workflow, never retained publisher data."""
import argparse
from hashlib import sha256
import json
from pathlib import Path
import shutil
import tempfile

import duckdb
from gold_analytics import SCHEMAS, _json_value, open_publication
from gold_reader import AnalyticsFailure
from gold_fixtures import create_analytics_publications
from publication_metadata import canonical_bytes


def generate(output):
    output.mkdir(parents=True, exist_ok=False)
    with tempfile.TemporaryDirectory() as temporary:
        gold, publications = create_analytics_publications(temporary)
        shutil.copytree(gold, output / "gold's snapshots")
    root = output / "gold's snapshots"
    scenarios = []
    for snapshot in ('full', 'first', 'empty'):
        with open_publication(root, publications[snapshot]) as reference:
            for report in ('publishers', 'articles', 'daily', 'quality'):
                filters = [{}]
                if snapshot == 'full':
                    filters += [dict(source='alpha'), dict(source="alpha' OR 1=1 --"), dict(limit=1)]
                    if report in ('daily', 'quality'):
                        filters += [dict(start='2026-10-13', end='2026-10-13'),
                                    dict(source='alpha', start='2026-10-13')]
                for options in filters:
                    scenarios.append(dict(snapshot=snapshot, report=report, options=options,
                                          rows=reference.report(report, **options)))
    directory = root / publications['full']
    # These are deliberately damaged COPIES. Neither producers nor retained inputs change.
    failures = []
    for case, java_code in [
        ('missing-marker', 'INCOMPLETE_OUTPUT'), ('unsupported-version', 'UNSUPPORTED_VERSION'),
        ('invalid-json', 'PUBLICATION_READ_FAILED'), ('missing-data', 'INVALID_LAYOUT'),
        ('path-escape', 'INVALID_LAYOUT'), ('symlink-escape', 'INVALID_LAYOUT'),
        ('corrupt-parquet', 'QUERY_FAILED'),
        ('tampered-manifest', None), ('wrong-count', None), ('unexpected-file', None),
        ('modified-values', None), ('extra-column', None), ('invalid-required-value', None),
    ]:
        damaged_root = output / 'cases' / case
        damaged = damaged_root / publications['full']
        shutil.copytree(directory, damaged)
        marker_path = damaged / 'complete.json'
        marker = json.loads(marker_path.read_text())
        parquet = damaged / marker['files']['gold_articles'][0]['path']
        if case == 'missing-marker':
            marker_path.unlink()
        elif case == 'unsupported-version':
            marker['processor_revision'] = 2
        elif case == 'invalid-json':
            marker_path.write_text('{bad')
        elif case == 'missing-data':
            parquet.unlink()
        elif case == 'path-escape':
            marker['files']['gold_articles'][0]['path'] = '../outside.parquet'
        elif case == 'symlink-escape':
            outside = output / 'outside.parquet'
            shutil.copyfile(parquet, outside)
            parquet.unlink()
            parquet.symlink_to(outside.resolve())
        elif case == 'corrupt-parquet':
            parquet.write_bytes(b'corrupted')
        elif case == 'tampered-manifest':
            with (damaged / 'inputs.json').open('ab') as stream:
                stream.write(b' ')
        elif case == 'wrong-count':
            marker['output_counts']['gold_articles'] += 1
        elif case == 'unexpected-file':
            (damaged / 'extra.txt').write_text('unexpected')
        else:
            replacement = output / 'replacement.parquet'
            with duckdb.connect() as connection:
                connection.read_parquet(str(parquet), hive_partitioning=False).create_view('original')
                projection = {'modified-values': "* REPLACE ('changed' AS title)",
                              'extra-column': '* , 1 AS unexpected',
                              'invalid-required-value': '* REPLACE (NULL::VARCHAR AS title)'}[case]
                connection.sql('SELECT ' + projection + ' FROM original').write_parquet(str(replacement))
            replacement.replace(parquet)
            if case != 'modified-values':
                # Demonstrate schema/value rejection after file-byte integrity passes.
                marker['files']['gold_articles'][0].update(byte_length=parquet.stat().st_size,
                                                          sha256=sha256(parquet.read_bytes()).hexdigest())
        if case not in ('missing-marker', 'invalid-json'):
            marker_path.write_bytes(canonical_bytes(marker))
        try:
            with open_publication(damaged_root, publications['full']):
                raise AssertionError('Python incorrectly accepted corruption')
        except AnalyticsFailure as error:
            failures.append(dict(case=case, java_code=java_code, python_code=error.code))
    (output / 'reference.json').write_text(json.dumps(dict(publications=publications, scenarios=scenarios,
        failures=failures, schemas=SCHEMAS), default=_json_value, indent=2) + '\n')
    print(json.dumps(dict(scenarios=len(scenarios), corruption_cases=len(failures), output=str(output))))


if __name__ == '__main__':
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--output', type=Path, required=True)
    generate(parser.parse_args().output.resolve())
