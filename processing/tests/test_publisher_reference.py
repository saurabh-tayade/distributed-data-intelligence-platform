"""A01–A17: fictional, standalone E1a contracts; no producer/runtime imports."""
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import FrozenInstanceError
from hashlib import sha256
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import publisher_reference as pr
from publication_metadata import canonical_bytes


HEADER = b'source_id,publisher_name,publisher_category,declared_language,country_code\n'
VOCAB = b'{"country_code":["CA","GB","US"],"declared_language":["en","es","fr"],"publisher_category":["business","culture","general-news","science","technology"],"schema_version":1,"version":1}'
VECTOR = 'c6140d147ca67df65e761873d7ba88fb293192902b723feae706def89cc462db'
CSV_HASH = '9c742f47d8ba5e928ae2c1eb84648b87ae149a7137c7dd5d904da93d413b29e0'
VOCAB_HASH = '734af479169642f0b9fd699cb8cfec0d3edf40aec0da07f40cc235de3074b005'
MANIFEST = b'{"authoring":{"maintainer_id":"example-team","note":null},"csv":{"byte_length":75,"file":"reference.csv","row_count":0,"sha256":"9c742f47d8ba5e928ae2c1eb84648b87ae149a7137c7dd5d904da93d413b29e0"},"csv_schema_version":1,"manifest_version":1,"reference_id":"fictional-publishers","revision":"r1","semantic_config":{"csv_dialect":"publisher-csv-v1","null_policy":"empty-only-v1","text_policy":"scalar-text-v1"},"validator_revision":1,"vocabulary":{"byte_length":183,"file":"vocabulary.json","sha256":"734af479169642f0b9fd699cb8cfec0d3edf40aec0da07f40cc235de3074b005","version":1}}'
ROW = b'example-a,Example A,science,en,CA\n'
REPO = Path(__file__).resolve().parents[2]


class ReferenceTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.root = self.base / 'snapshots'
        self.root.mkdir()
        self.csv = self.base / 'source.csv'
        self.vocab = self.base / 'vocabulary.json'
        self.csv.write_bytes(HEADER)
        self.vocab.write_bytes(VOCAB)
        self.args = dict(csv_path=self.csv, vocabulary_path=self.vocab,
                         reference_id='fictional-publishers', revision='r1',
                         maintainer_id='example-team')
        self.dest = self.root / 'reference_id=fictional-publishers' / 'revision=r1'

    def error(self, code, function, *args, **kwargs):
        with self.assertRaises(pr.ReferenceFailure) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.code, code, caught.exception.diagnostic)
        return caught.exception

    def publish(self, **kwargs):
        return pr.publish(self.root, **(self.args | kwargs))

    def open(self, fingerprint=VECTOR):
        return pr.open_snapshot(self.root, 'fictional-publishers', 'r1', fingerprint)

    def state(self):
        return {str(p.relative_to(self.root)): (p.stat().st_mtime_ns,
                p.read_bytes() if p.is_file() else None)
                for p in [self.root, *self.root.rglob('*')]}

    def evidence(self, path):
        """Access times may change on reads; identity, metadata and bytes may not."""
        return (pr._stamp(path.stat()), pr._identity(path.parent.stat()), path.read_bytes())

    @contextmanager
    def capture_fault(self, target, occurrence, fault):
        """Fail actual I/O inside one selected capture, without changing its file."""
        capture, fdopen = pr._capture, os.fdopen
        count = 0
        failures = []
        message = f'{target}: secret credential read/close failure'

        def fail(*args, **kwargs):
            failures.append(fault)
            raise OSError(message)

        class Stream:
            def __init__(self, stream):
                self.stream = stream
            def __enter__(self):
                return self
            def fileno(self):
                return self.stream.fileno()
            def read(self, size):
                if fault == 'read':
                    fail()
                return self.stream.read(size)
            def __exit__(self, *args):
                self.stream.close()
                if fault == 'close':
                    fail()

        def selected(path, *args, **kwargs):
            nonlocal count
            if path == target:
                count += 1
                if count == occurrence:
                    if fault in ('read', 'close'):
                        with patch.object(os, 'fdopen', side_effect=lambda *a, **k: Stream(fdopen(*a, **k))):
                            return capture(path, *args, **kwargs)
                    owner, name = (Path, 'lstat') if fault == 'stat' else (os, fault)
                    with patch.object(owner, name, side_effect=fail):
                        return capture(path, *args, **kwargs)
            return capture(path, *args, **kwargs)

        with patch.object(pr, '_capture', side_effect=selected):
            yield
        self.assertTrue(failures, 'The selected I/O fault was not reached')

    def safe_error(self, code, function, *args, **kwargs):
        error = self.error(code, function, *args, **kwargs)
        self.assertEqual(error.diagnostic, {'code': code, 'message': pr.MESSAGES[code]})
        self.assertEqual(str(error), pr.MESSAGES[code])

    def rewrite(self, name, transform):
        path = self.dest / name
        value = json.loads(path.read_bytes())
        transform(value)
        path.write_bytes(canonical_bytes(value))

    def rebind(self):
        """Attacker recomputes self-consistent hashes; not an authenticity test."""
        data = (self.dest / 'manifest.json').read_bytes()
        digest = sha256(data).hexdigest()
        self.rewrite('complete.json', lambda c: c.update(snapshot_fingerprint=digest,
                                                        manifest_byte_length=len(data)))
        return digest

    def test_a01_valid_empty_and_fictional_rows(self):
        self.assertEqual(pr.validate(**self.args).rows, ())
        self.csv.write_bytes(HEADER + b'example-a,"Example, ""Arts""",culture,fr,GB\n'
                             + 'example-b,Café 😀,,,\n'.encode() + b'example-null,NULL,,,')
        result = pr.validate(**self.args)
        self.assertEqual(result.rows, (
            pr.ReferenceRow('example-a', 'Example, "Arts"', 'culture', 'fr', 'GB'),
            pr.ReferenceRow('example-b', 'Café 😀', None, None, None),
            pr.ReferenceRow('example-null', 'NULL', None, None, None)))
        self.assertEqual(list(self.root.iterdir()), [])

    def test_a01_invalid_never_reserves_destination(self):
        self.csv.write_bytes(HEADER + ROW + b'bad,\n')
        self.error('CSV_INVALID', self.publish)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_a02_headers_blank_records_counts_and_endings(self):
        for header in (HEADER, HEADER[:-1], HEADER.replace(b'\n', b'\r\n')):
            with self.subTest(header=header):
                self.assertEqual(pr._csv(header), ())
        for data in (b'', b'\n', HEADER + b'\n', HEADER + ROW + b'\n',
                     HEADER.replace(b'source_id', b'Source_id'), HEADER[:-1] + b'\r',
                     b'"' + HEADER[:-1] + b'"\n', HEADER + HEADER,
                     HEADER + b'a,A,,,extra,\n', HEADER + b'a,A,,\n',
                     b'publisher_name,source_id,publisher_category,declared_language,country_code\n'):
            with self.subTest(data=data):
                self.error('CSV_INVALID', pr._csv, data)
        self.assertEqual(len(pr._csv(HEADER + ROW.replace(b'\n', b'\r\n') + b'b,B,,,')), 2)

    def test_a03_quoting_and_parser_settings_restored(self):
        previous = pr.csv.field_size_limit()
        pr.csv.field_size_limit(1)
        try:
            row, = pr._csv(HEADER + b'"a","A, ""B""",,"",""')
            self.assertEqual(row.publisher_name, 'A, "B"')
            self.assertIsNone(row.country_code)
            self.assertEqual(pr.csv.field_size_limit(), 1)
            for data in (b'a,A"B,,,', b'a,"A"x,,,', b'a,"A" ,,,', b'a,"A,,,',
                         b'a,"A\nB",,,', b'a,"A\r\nB",,,', b'a,A\rB,,,'):
                self.error('CSV_INVALID', pr._csv, HEADER + data)
            self.assertEqual(pr.csv.field_size_limit(), 1)
            with patch.object(pr.csv, 'reader', side_effect=pr.csv.Error()):
                self.error('CSV_INVALID', pr._csv, HEADER + ROW)
            self.assertEqual(pr.csv.field_size_limit(), 1)
        finally:
            pr.csv.field_size_limit(previous)

    def test_a04_unicode_controls_whitespace_and_plain_text(self):
        forbidden = [*range(32), *range(127, 160), 8232, 8233, 65279]
        for point in forbidden:
            with self.subTest(point=point):
                self.error('CSV_INVALID', pr._csv, HEADER + f'a,A{chr(point)}B,,,'.encode())
        for space in pr.WHITESPACE:
            for name in (space + 'A', 'A' + space, space):
                self.error('CSV_INVALID', pr._csv, HEADER + f'a,{name},,,'.encode())
        for raw in (b'\xef\xbb\xbf' + HEADER, HEADER + b'a,\xff,,,',
                    HEADER + b'a,\xed\xa0\x80,,,', HEADER + b'a,\xc0\xaf,,,'):
            self.error('CSV_INVALID', pr._csv, raw)
        for name in ('<b>Example</b>', 'e\u0301', 'é', 'A B', 'A\u00a0B', 'A\\B'):
            row, = pr._csv(HEADER + f'a,{name},,,'.encode())
            self.assertEqual(row.publisher_name, name)
        for data in (b'a,A,NULL,,', b'a,A,,NULL,', b'a,A,,,NULL', b'a,,science,en,CA'):
            self.error('CSV_INVALID' if b'NULL' not in data[-4:] else 'RESOURCE_LIMIT', pr._csv, HEADER + data)

    def test_a05_field_boundaries_and_duplicates(self):
        for length in (1, 128):
            self.assertEqual(len(pr._csv(HEADER + b'a' * length + b',A,,,')), 1)
        self.error('RESOURCE_LIMIT', pr._csv, HEADER + b'a' * 129 + b',A,,,')
        for source in ('', '-a', '_a', 'A', 'a.b', 'a/b', 'é'):
            self.error('CSV_INVALID', pr._csv, HEADER + f'{source},A,,,'.encode())
        for name in ('😀' * 200, 'e\u0301' * 100):
            self.assertEqual(pr._csv(HEADER + f'a,{name},,,'.encode())[0].publisher_name, name)
        self.error('RESOURCE_LIMIT', pr._csv, HEADER + f'a,{"😀" * 201},,,'.encode())
        for index, limit in ((2, 64), (3, 32), (4, 2)):
            for length, code in ((limit, 'CSV_INVALID'), (limit + 1, 'RESOURCE_LIMIT')):
                cells = ['a', 'A', '', '', '']
                cells[index] = 'x' * length
                self.error(code, pr._csv, HEADER + ','.join(cells).encode())
        for second in (ROW, b'example-a,Other,culture,fr,GB\n'):
            error = self.error('CSV_INVALID', pr._csv, HEADER + ROW + second)
            self.assertEqual(error.diagnostic['record'], 3)
            self.assertEqual(error.diagnostic['first_record'], 2)

    def test_a06_byte_limits_and_bounded_reads(self):
        for limit in (pr.CSV_LIMIT, pr.VOCABULARY_LIMIT, pr.MANIFEST_LIMIT, pr.COMPLETION_LIMIT):
            with self.subTest(limit=limit):
                self.csv.write_bytes(b'x' * limit)
                self.assertEqual(len(pr._capture(self.csv, limit).data), limit)
                with self.csv.open('ab') as stream:
                    stream.write(b'x')
                self.error('RESOURCE_LIMIT', pr._capture, self.csv, limit)
        class BoundedStream(io.BytesIO):
            def read(self, size=-1):
                self.assert_size(size)
                return super().read(size)
        stream = BoundedStream(b'x' * 11)
        stream.assert_size = lambda size: self.assertTrue(0 < size <= 11)
        self.error('RESOURCE_LIMIT', pr._read_stream, stream, 10)
        self.assertEqual(stream.tell(), 11)
        self.error('CSV_INVALID', pr._csv, HEADER + b'x' * 4096)
        self.error('RESOURCE_LIMIT', pr._csv, HEADER + b'x' * 4097)
        self.error('RESOURCE_LIMIT', pr._csv, HEADER + b'x' * 4097 + b'\r\n')
        data = HEADER + b''.join(f's{i},N,,,\n'.encode() for i in range(10_000))
        self.assertEqual(len(pr._csv(data)), 10_000)
        self.error('RESOURCE_LIMIT', pr._csv, data + b'last,N,,,\n')

    def test_a06_growing_file_exceeds_bound_during_read(self):
        self.csv.write_bytes(b'abc')
        read = pr._read_stream
        def growing(stream, limit):
            with self.csv.open('ab') as writer:
                writer.write(b'de')
            return read(stream, limit)
        with patch.object(pr, '_read_stream', side_effect=growing):
            self.error('RESOURCE_LIMIT', pr._capture, self.csv, 4)

    def test_a07_vocabulary_file_exact_bytes_and_schema(self):
        self.assertEqual((REPO / 'config/publisher-reference-vocabulary-v1.json').read_bytes(), VOCAB)
        self.assertEqual(pr._vocabulary(VOCAB), json.loads(VOCAB))
        mutations = [lambda v: v.update(version=2), lambda v: v.update(schema_version=True),
                     lambda v: v.update(extra=1), lambda v: v.pop('country_code'),
                     lambda v: v.update(country_code=None), lambda v: v.update(country_code=[]),
                     lambda v: v.update(country_code=['US', 'CA']),
                     lambda v: v.update(country_code=['CA', 'CA']),
                     lambda v: v.update(country_code=['ZZ']),
                     lambda v: v.update(declared_language=['EN'])]
        for mutate in mutations:
            value = json.loads(VOCAB)
            mutate(value)
            self.error('VOCABULARY_INVALID', pr._vocabulary, canonical_bytes(value))
        for raw in (VOCAB + b'\n', b' ' + VOCAB, VOCAB.replace(b'"version":1', b'"version":1,"version":1')):
            self.error('VOCABULARY_INVALID', pr._vocabulary, raw)
        for size, code in ((32, 'VOCABULARY_INVALID'), (33, 'RESOURCE_LIMIT')):
            value = json.loads(VOCAB)
            value['publisher_category'] = [f'a{i:02d}' for i in range(size)]
            self.error(code, pr._vocabulary, canonical_bytes(value))
        for field, limit in (('publisher_category', 64), ('declared_language', 32), ('country_code', 2)):
            value = json.loads(VOCAB)
            value[field] = ['a' * (limit + 1)]
            self.error('RESOURCE_LIMIT', pr._vocabulary, canonical_bytes(value))

    def test_a08_literal_canonical_vectors(self):
        vector_a = b'{"a":{"b":false},"z":[1,"\xc3\xa9"]}'
        self.assertEqual(canonical_bytes({'z': [1, 'é'], 'a': {'b': False}}), vector_a)
        self.assertEqual(sha256(vector_a).hexdigest(), '73d069d0f3964dd58881492a708b60441e9e119d805019f1f4b4906e57f938b0')
        candidate = pr._candidate(**self.args, note=None)
        self.assertEqual(candidate.manifest_bytes, MANIFEST)
        self.assertEqual((len(HEADER), len(VOCAB), len(MANIFEST)), (75, 183, 577))
        self.assertEqual(candidate.reference.fingerprint, VECTOR)
        self.assertEqual(candidate.reference.manifest['csv']['sha256'], CSV_HASH)
        self.assertEqual(candidate.reference.manifest['vocabulary']['sha256'], VOCAB_HASH)
        reordered = dict(reversed(list(json.loads(MANIFEST).items())))
        self.assertEqual(canonical_bytes(reordered), MANIFEST)

    def test_a08_strict_nested_metadata(self):
        mutations = [lambda m: m.update(extra=1), lambda m: m.pop('authoring'),
                     lambda m: m.update(validator_revision=True),
                     lambda m: m.update(csv_schema_version=2),
                     lambda m: m['csv'].update(row_count=False),
                     lambda m: m['vocabulary'].update(version=2),
                     lambda m: m['csv'].update(file='../source.csv'),
                     lambda m: m['semantic_config'].update(extra='x'),
                     lambda m: m['authoring'].update(extra=None),
                     lambda m: m['authoring'].update(maintainer_id=None)]
        for mutate in mutations:
            value = json.loads(MANIFEST)
            mutate(value)
            self.error('SNAPSHOT_INVALID', pr._manifest, canonical_bytes(value))
        for token in (b'true', b'1.0', b'1e0', b'-1', b'-0', b'NaN', b'Infinity', b'999999999999999999999'):
            self.error('SNAPSHOT_INVALID', pr._manifest, MANIFEST.replace(b'"row_count":0', b'"row_count":' + token))
        for raw in (MANIFEST + b'\n', MANIFEST.replace(b'"note":null', b'"note":null,"note":null'),
                    MANIFEST.replace(b'"note":null', b'"note":"\\ud800"'), b'[' * 2000 + b']' * 2000,
                    MANIFEST.replace(b'example-team', b'example-\\u0074eam')):
            self.error('SNAPSHOT_INVALID', pr._manifest, raw)

    def test_a09_identity_and_operational_settings(self):
        original = pr.validate(**self.args).fingerprint
        for change in ({'revision': 'r2'}, {'maintainer_id': 'other-team'}, {'note': 'Reviewed'},
                       {'reference_id': 'other-reference'}):
            self.assertNotEqual(pr.validate(**(self.args | change)).fingerprint, original)
        copy = self.base / 'copy.csv'
        copy.write_bytes(HEADER)
        self.assertEqual(pr.validate(**(self.args | {'csv_path': copy})).fingerprint, original)
        for data in (HEADER[:-1], HEADER.replace(b'\n', b'\r\n')):
            self.csv.write_bytes(data)
            self.assertNotEqual(pr.validate(**self.args).fingerprint, original)
        self.csv.write_bytes(HEADER + b'a,A,,,\nb,B,,,\n')
        first = pr.validate(**self.args).fingerprint
        for data in (b'b,B,,,\na,A,,,\n', b'a,"A",,,\nb,B,,,\n'):
            self.csv.write_bytes(HEADER + data)
            self.assertNotEqual(pr.validate(**self.args).fingerprint, first)

    def test_a10_identity_and_authoring_boundaries(self):
        for field, limit in (('reference_id', 64), ('revision', 32), ('maintainer_id', 64)):
            for size in (1, limit):
                pr.validate(**(self.args | {field: 'a' * size}))
            for value in ('a' * (limit + 1), '', '.', '..', '../a', 'A', 'a b', 'a/b', 'é', None):
                self.error('ARGUMENT_INVALID', pr.validate, **(self.args | {field: value}))
        for note in (None, 'N', '😀' * 256):
            pr.validate(**(self.args | {'note': note}))
        self.error('RESOURCE_LIMIT', pr.validate, **(self.args | {'note': 'N' * 257}))
        for note in ('', ' N', 'N\t', '\ud800'):
            self.error('ARGUMENT_INVALID', pr.validate, **(self.args | {'note': note}))

    def test_a11_publish_replay_conflict_and_new_revision(self):
        with patch.object(pr, '_now', return_value='2026-10-19T12:00:00.000000Z'):
            self.assertEqual(self.publish()['status'], 'published')
        self.assertEqual({p.name for p in self.dest.iterdir()},
                         {'reference.csv', 'vocabulary.json', 'manifest.json', 'complete.json'})
        before = self.state()
        with patch.object(pr, '_now', side_effect=AssertionError('Replay used clock')):
            self.assertEqual(self.publish()['status'], 'replayed')
        self.assertEqual(self.state(), before)
        self.assertEqual(self.open().created_at, '2026-10-19T12:00:00.000000Z')
        self.csv.write_bytes(HEADER + ROW)
        self.error('REVISION_CONFLICT', self.publish)
        self.assertEqual(self.state(), before)
        self.assertEqual(self.publish(revision='r2')['status'], 'published')
        for name, state in before.items():
            if name not in ('.', 'reference_id=fictional-publishers'):
                self.assertEqual(self.state()[name], state)

    def test_a12_independent_open_removal_and_nested_immutability(self):
        self.csv.write_bytes(HEADER + ROW)
        fingerprint = self.publish()['snapshot_fingerprint']
        self.csv.unlink()
        self.vocab.unlink()
        result = self.open(fingerprint)
        with self.assertRaises(FrozenInstanceError):
            result.rows[0].publisher_name = 'Changed'
        with self.assertRaises(TypeError):
            result.manifest['csv']['row_count'] = 900
        with self.assertRaises(TypeError):
            result.manifest['authoring']['note'] = 'Changed'
        with self.assertRaises(FrozenInstanceError):
            result.fingerprint = '0' * 64
        (self.dest / 'reference.csv').write_bytes(b'bad')
        self.assertEqual(result.rows[0].publisher_name, 'Example A')
        self.error('SNAPSHOT_INVALID', self.open, fingerprint)

    def test_a12_standalone_import_without_site_or_producers(self):
        script = "import sys; import publisher_reference; assert not any(n.split('.')[0] in {'pyspark','duckdb','boto3','gold','gold_reader','silver'} for n in sys.modules)"
        result = subprocess.run([sys.executable, '-S', '-c', script], cwd=REPO,
                                env=os.environ | {'PYTHONPATH': str(REPO / 'processing')},
                                capture_output=True, text=True, timeout=10)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_a13_fingerprint_and_directory_selection(self):
        self.publish()
        self.error('IDENTITY_MISMATCH', self.open, '0' * 64)
        self.error('ARGUMENT_INVALID', self.open, 'bad')
        self.error('SNAPSHOT_UNAVAILABLE', pr.open_snapshot, self.root, 'other', 'r1', VECTOR)
        self.rewrite('manifest.json', lambda m: m.update(reference_id='other'))
        fingerprint = self.rebind()
        self.error('IDENTITY_MISMATCH', self.open, fingerprint)

    def test_a13_corrupt_missing_and_unexpected_artifacts(self):
        self.publish()
        before = self.state()
        for name in ('reference.csv', 'vocabulary.json', 'manifest.json', 'complete.json'):
            path = self.dest / name
            original = path.read_bytes()
            for data in (b'bad', b''):
                path.write_bytes(data)
                self.error('SNAPSHOT_INVALID', self.open)
                self.error('SNAPSHOT_INVALID', self.publish)
                self.assertEqual(path.read_bytes(), data)
            path.unlink()
            code = 'SNAPSHOT_INCOMPLETE' if name == 'complete.json' else 'SNAPSHOT_INVALID'
            self.error(code, self.open)
            path.write_bytes(original)
        for name in ('extra', 'complete.tmp'):
            path = self.dest / name
            path.write_bytes(b'x')
            self.error('SNAPSHOT_INVALID', self.open)
            path.unlink()
        self.assertEqual({name: state[1] for name, state in self.state().items()},
                         {name: state[1] for name, state in before.items()})

    def test_a13_semantics_revalidated_despite_recomputed_hashes(self):
        self.publish()
        data = VOCAB.replace(b'"US"', b'"ZZ"')
        (self.dest / 'vocabulary.json').write_bytes(data)
        self.rewrite('manifest.json', lambda m: m['vocabulary'].update(sha256=sha256(data).hexdigest()))
        self.error('SNAPSHOT_INVALID', self.open, self.rebind())
        (self.dest / 'vocabulary.json').write_bytes(VOCAB)
        self.rewrite('manifest.json', lambda m: m['vocabulary'].update(sha256=VOCAB_HASH))
        data = HEADER + ROW + ROW
        (self.dest / 'reference.csv').write_bytes(data)
        self.rewrite('manifest.json', lambda m: m['csv'].update(sha256=sha256(data).hexdigest(),
                                                              byte_length=len(data), row_count=2))
        self.error('SNAPSHOT_INVALID', self.open, self.rebind())

    def test_a13_completion_time_and_metadata(self):
        self.publish()
        original = (self.dest / 'complete.json').read_bytes()
        for timestamp in ('2026-02-30T00:00:00.000000Z', '0000-01-01T00:00:00.000000Z',
                          '2026-10-19T00:00:60.000000Z', '2026-10-19T00:00:00Z',
                          '2026-10-19T00:00:00.000000+00:00', None):
            self.rewrite('complete.json', lambda c: c.update(created_at=timestamp))
            self.error('SNAPSHOT_INVALID', self.open)
        for change in ({'completion_version': True}, {'manifest_byte_length': 1}, {'extra': 1},
                       {'snapshot_fingerprint': '0' * 64}):
            (self.dest / 'complete.json').write_bytes(original)
            self.rewrite('complete.json', lambda c: c.update(change))
            self.error('SNAPSHOT_INVALID', self.open)
        (self.dest / 'complete.json').write_bytes(original)
        self.rewrite('complete.json', lambda c: c.update(created_at='2000-02-29T00:00:00.000000Z'))
        self.assertEqual(self.open().fingerprint, VECTOR)
        self.assertEqual(self.open().created_at, '2000-02-29T00:00:00.000000Z')

    def test_a13_recorded_lengths_counts_and_resource_caps(self):
        self.publish()
        original_manifest = (self.dest / 'manifest.json').read_bytes()
        original_marker = (self.dest / 'complete.json').read_bytes()
        for section, key, value in (('csv', 'row_count', 1), ('csv', 'byte_length', 76),
                                    ('vocabulary', 'byte_length', 184)):
            (self.dest / 'manifest.json').write_bytes(original_manifest)
            self.rewrite('manifest.json', lambda m: m[section].update({key: value}))
            self.error('SNAPSHOT_INVALID', self.open, self.rebind())
        (self.dest / 'manifest.json').write_bytes(original_manifest)
        (self.dest / 'complete.json').write_bytes(original_marker)
        for name, cap in (('manifest.json', 16384), ('complete.json', 4096),
                          ('vocabulary.json', 32768), ('reference.csv', 10485760)):
            path = self.dest / name
            original = path.read_bytes()
            path.write_bytes(b'x' * (cap + 1))
            self.error('RESOURCE_LIMIT', self.open)
            path.write_bytes(original)

    def test_a14_incomplete_and_corrupt_binding_never_repaired(self):
        self.dest.mkdir(parents=True)
        (self.dest / 'complete.tmp').write_bytes(b'incomplete')
        before = self.state()
        self.error('SNAPSHOT_INCOMPLETE', self.publish)
        self.assertEqual(self.state(), before)

    def test_a14_write_and_marker_faults_leave_incomplete(self):
        write = pr._write
        for index, name in enumerate(('reference.csv', 'vocabulary.json', 'manifest.json', 'complete.tmp')):
            root = self.base / f'fault-{index}'
            root.mkdir()
            def fail(path, data):
                if path.name == name:
                    raise OSError('secret path credential')
                write(path, data)
            with patch.object(pr, '_write', side_effect=fail):
                self.error('PUBLICATION_FAILED', pr.publish, root, **self.args)
            dest = root / 'reference_id=fictional-publishers' / 'revision=r1'
            self.assertFalse((dest / 'complete.json').exists())
            self.error('SNAPSHOT_INCOMPLETE', pr.publish, root, **self.args)

    def test_a14_create_close_readback_and_rename_faults(self):
        with patch.object(Path, 'mkdir', side_effect=OSError('secret')):
            self.error('PUBLICATION_FAILED', self.publish)
        real_open = Path.open
        class CloseFailure:
            def __init__(self, stream):
                self.stream = stream
            def __enter__(self):
                return self.stream
            def __exit__(self, *args):
                self.stream.close()
                raise OSError('close failed')
        def fail_close(path, *args, **kwargs):
            stream = real_open(path, *args, **kwargs)
            return CloseFailure(stream) if args == ('xb',) else stream
        with patch.object(Path, 'open', fail_close):
            self.error('PUBLICATION_FAILED', self.publish)
        self.assertFalse((self.dest / 'complete.json').exists())
        # Separate revisions keep failed evidence intact.
        with patch.object(pr, '_verify', side_effect=pr.ReferenceFailure('SNAPSHOT_INVALID')):
            self.error('PUBLICATION_FAILED', self.publish, revision='readback')
        with patch.object(Path, 'rename', side_effect=OSError('rename failed')):
            self.error('PUBLICATION_FAILED', self.publish, revision='rename')
        self.assertTrue((self.dest.parent / 'revision=rename' / 'complete.tmp').exists())

    def test_a14_interruption_after_rename_can_replay(self):
        rename = Path.rename
        def fail_after(path, target):
            rename(path, target)
            raise OSError('caller lost success')
        with patch.object(Path, 'rename', fail_after):
            self.error('PUBLICATION_FAILED', self.publish)
        self.assertEqual(self.open().fingerprint, VECTOR)
        self.assertEqual(self.publish()['status'], 'replayed')

    def test_a14_read_io_failure_is_not_misreported_as_mutation(self):
        with patch.object(pr, '_read_stream', side_effect=OSError('secret input error')):
            self.error('INPUT_UNAVAILABLE', pr.validate, **self.args)
        self.publish()
        with patch.object(pr, '_read_stream', side_effect=OSError('secret output error')):
            self.error('SNAPSHOT_INVALID', self.open)
        read = pr._read_stream
        def fail_output(stream, limit):
            # Original input captures still succeed; private read-back fails.
            if limit == pr.MANIFEST_LIMIT:
                raise OSError('private readback error')
            return read(stream, limit)
        with patch.object(pr, '_read_stream', side_effect=fail_output):
            self.error('PUBLICATION_FAILED', self.publish, revision='io-failure')
        self.assertEqual(self.open().fingerprint, VECTOR)

    def test_a14_unchanged_input_initial_and_recheck_io_failures(self):
        for path in (self.csv, self.vocab):
            for occurrence in (1, 2):
                for fault in ('read', 'close', 'stat', 'open', 'fstat'):
                    with self.subTest(path=path.name, occurrence=occurrence, fault=fault):
                        before = self.evidence(path)
                        with self.capture_fault(path, occurrence, fault):
                            self.safe_error('INPUT_UNAVAILABLE', pr.validate, **self.args)
                        self.assertEqual(self.evidence(path), before)
                        self.assertEqual(list(self.root.iterdir()), [])

    def test_a14_unchanged_snapshot_recheck_io_failures(self):
        self.publish()
        before = self.state()
        for path in sorted(self.dest.iterdir()):
            for fault in ('read', 'close', 'stat', 'open', 'fstat'):
                with self.subTest(path=path.name, fault=fault):
                    evidence = self.evidence(path)
                    with self.capture_fault(path, 2, fault):
                        self.safe_error('SNAPSHOT_INVALID', self.open)
                    self.assertEqual(self.evidence(path), evidence)
                    self.assertEqual(self.state(), before)
        self.assertEqual(self.open().fingerprint, VECTOR)

    def test_a14_publication_readback_and_all_recheck_io_phases(self):
        self.publish()
        before = {p: self.evidence(p) for p in self.dest.iterdir()}
        # Artifact captures: private verify/read/recheck, pre-marker recheck,
        # post-marker recheck, temporary verify/read/recheck, final recheck.
        targets = [(name, occurrence) for name in
                   ('reference.csv', 'vocabulary.json', 'manifest.json')
                   for occurrence in range(1, 8)]
        # Temporary marker: read-back, temporary verify/read/recheck, final recheck.
        targets += [('complete.tmp', occurrence) for occurrence in range(1, 5)]
        # Candidate rechecks after reservation, before/after the temporary marker.
        targets += [('input', occurrence) for occurrence in range(3, 6)]
        for index, (name, occurrence) in enumerate(targets):
            for fault in ('read', 'close'):
                revision = f'io-{index}-{fault}'
                dest = self.dest.parent / f'revision={revision}'
                path = self.csv if name == 'input' else dest / name
                with self.subTest(name=name, occurrence=occurrence, fault=fault):
                    inputs = {p: self.evidence(p) for p in (self.csv, self.vocab)}
                    observed = []
                    capture = pr._capture
                    def record(path, *args, **kwargs):
                        observed.append((path, self.evidence(path)))
                        return capture(path, *args, **kwargs)
                    with patch.object(pr, '_capture', side_effect=record):
                        with self.capture_fault(path, occurrence, fault):
                            self.safe_error('PUBLICATION_FAILED', self.publish, revision=revision)
                    self.assertFalse((dest / 'complete.json').exists())
                    self.assertEqual({p: self.evidence(p) for p in inputs}, inputs)
                    self.assertEqual({p: self.evidence(p) for p in before}, before)
                    for observed_path, evidence in observed:
                        self.assertEqual(self.evidence(observed_path), evidence)
                    retained = {p: self.evidence(p) for p in dest.iterdir()}
                    self.error('SNAPSHOT_INCOMPLETE', self.publish, revision=revision)
                    self.assertEqual({p: self.evidence(p) for p in dest.iterdir()}, retained)

    def test_a14_identical_replay_candidate_and_snapshot_recheck_io_failures(self):
        self.publish()
        before = self.state()
        for path, occurrence, code in (
                (self.csv, 3, 'INPUT_UNAVAILABLE'), (self.vocab, 3, 'INPUT_UNAVAILABLE'),
                (self.dest / 'reference.csv', 2, 'SNAPSHOT_INVALID')):
            for fault in ('read', 'close'):
                with self.subTest(path=path.name, fault=fault):
                    evidence = self.evidence(path)
                    with self.capture_fault(path, occurrence, fault):
                        self.safe_error(code, self.publish)
                    self.assertEqual(self.evidence(path), evidence)
                    self.assertEqual(self.state(), before)
        self.assertEqual(self.publish()['status'], 'replayed')

    def test_a14_metadata_failure_while_diagnosing_read_failure(self):
        self.publish()
        for path, operation, code in ((self.csv, lambda: pr.validate(**self.args), 'INPUT_UNAVAILABLE'),
                                      (self.dest / 'reference.csv', self.open, 'SNAPSHOT_INVALID')):
            before = self.evidence(path)
            read, lstat = pr._read_stream, Path.lstat
            failed = False
            def fail_read(stream, limit):
                nonlocal failed
                if pr._identity(os.fstat(stream.fileno())) == pr._identity(path.stat()):
                    failed = True
                    raise OSError('secret read failure')
                return read(stream, limit)
            def fail_stat(selected, *args, **kwargs):
                if failed and selected == path:
                    raise OSError('secret metadata failure')
                return lstat(selected, *args, **kwargs)
            with patch.object(pr, '_read_stream', side_effect=fail_read), patch.object(Path, 'lstat', fail_stat):
                self.safe_error(code, operation)
            self.assertTrue(failed)
            self.assertEqual(self.evidence(path), before)

    def test_a14_directory_and_inventory_recheck_io_failures(self):
        self.publish()
        before = self.state()
        # Fail only the final verification phase, after all artifact rechecks.
        for phase in ('guards', 'inventory', 'directory-stamp'):
            with self.subTest(phase=phase):
                recheck = pr._recheck
                armed = False
                def arm(*args, **kwargs):
                    nonlocal armed
                    result = recheck(*args, **kwargs)
                    armed = True
                    return result
                operation = {'guards': '_check_guards', 'inventory': '_inventory',
                             'directory-stamp': '_directory_stamp'}[phase]
                original = getattr(pr, operation)
                def fail(*args, **kwargs):
                    if armed:
                        owner, name = (os, 'scandir') if phase == 'inventory' else (Path, 'lstat')
                        with patch.object(owner, name, side_effect=OSError('secret directory failure')):
                            return original(*args, **kwargs)
                    return original(*args, **kwargs)
                with patch.object(pr, '_recheck', side_effect=arm), patch.object(pr, operation, side_effect=fail):
                    self.safe_error('SNAPSHOT_INVALID', self.open)
                self.assertTrue(armed)
                self.assertEqual(self.state(), before)

    def test_a14_exclusive_create_and_marker_corruption(self):
        write = pr._write
        def collision(path, data):
            if path.name == 'vocabulary.json':
                path.write_bytes(b'preexisting')
            write(path, data)
        with patch.object(pr, '_write', side_effect=collision):
            self.error('PUBLICATION_FAILED', self.publish)
        self.assertEqual((self.dest / 'vocabulary.json').read_bytes(), b'preexisting')
        def bad_marker(path, data):
            write(path, b'bad' if path.name == 'complete.tmp' else data)
        with patch.object(pr, '_write', side_effect=bad_marker):
            self.error('PUBLICATION_FAILED', self.publish, revision='bad-marker')
        self.assertFalse((self.dest.parent / 'revision=bad-marker' / 'complete.json').exists())

    def test_a15_input_mutation_during_capture_and_before_completion(self):
        read = pr._read_stream
        def mutate(stream, limit):
            data = read(stream, limit)
            self.csv.write_bytes(HEADER + ROW)
            return data
        with patch.object(pr, '_read_stream', side_effect=mutate):
            self.error('INPUT_CHANGED', pr.validate, **self.args)
        self.csv.write_bytes(HEADER)
        write = pr._write
        def mutate_before_marker(path, data):
            write(path, data)
            if path.name == 'manifest.json':
                self.csv.write_bytes(HEADER + ROW)
        with patch.object(pr, '_write', side_effect=mutate_before_marker):
            self.error('INPUT_CHANGED', self.publish)
        self.assertFalse((self.dest / 'complete.json').exists())

    def test_a15_recheck_observes_bytes_identity_metadata_and_disappearance(self):
        for kind in ('input', 'snapshot'):
            for mutation in ('bytes', 'replacement', 'mtime', 'disappearance', 'parent'):
                with self.subTest(kind=kind, mutation=mutation):
                    work = self.base / f'{kind}-{mutation}'
                    work.mkdir()
                    root = work / 'snapshots'
                    root.mkdir()
                    inputs = work / 'inputs'
                    inputs.mkdir()
                    csv = inputs / 'source.csv'
                    csv.write_bytes(HEADER + ROW)
                    args = self.args | {'csv_path': csv}
                    published = pr.publish(root, **args)
                    dest = root / 'reference_id=fictional-publishers' / 'revision=r1'
                    path = csv if kind == 'input' else dest / 'reference.csv'
                    recheck = pr._recheck
                    mutated = False
                    def change(capture, changed, unavailable):
                        nonlocal mutated
                        if capture.path == path and not mutated:
                            mutated = True
                            original = path.stat()
                            if mutation == 'bytes':
                                path.write_bytes(path.read_bytes().replace(b'Example A', b'Example B'))
                                os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
                                self.assertEqual(pr._stamp(path.stat()), capture.stamp)
                            elif mutation == 'replacement':
                                data = path.read_bytes()
                                path.rename(path.with_suffix('.old'))
                                path.write_bytes(data)
                                os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns))
                                self.assertNotEqual(pr._identity(path.stat()), capture.stamp[:2])
                            elif mutation == 'mtime':
                                os.utime(path, ns=(original.st_atime_ns, original.st_mtime_ns + 1_000_000))
                            elif mutation == 'disappearance':
                                path.unlink()
                            else:
                                # Replace only this input's immediate parent or selected snapshot directory.
                                parent = path.parent
                                moved = parent.with_name(parent.name + '-moved')
                                parent.rename(moved)
                                parent.mkdir()
                                for child in moved.iterdir():
                                    child.rename(parent / child.name)
                                self.assertNotEqual(pr._identity(parent.stat()), capture.parent_identity)
                        return recheck(capture, changed, unavailable)
                    with patch.object(pr, '_recheck', side_effect=change):
                        if kind == 'input':
                            self.error('INPUT_CHANGED', pr.validate, **args)
                        else:
                            self.error('SNAPSHOT_CHANGED', pr.open_snapshot, root,
                                       'fictional-publishers', 'r1', published['snapshot_fingerprint'])
                    self.assertTrue(mutated)

    def test_a15_observed_mutation_is_preserved_when_io_also_fails(self):
        for changed, unavailable in (('INPUT_CHANGED', 'INPUT_UNAVAILABLE'),
                                     ('SNAPSHOT_CHANGED', 'SNAPSHOT_INVALID')):
            for fault in ('read', 'close'):
                with self.subTest(changed=changed, fault=fault):
                    self.csv.write_bytes(HEADER + ROW)
                    capture = pr._capture(self.csv, pr.CSV_LIMIT)
                    if fault == 'close':
                        # A byte-only mutation must survive the close exception.
                        info = self.csv.stat()
                        self.csv.write_bytes(HEADER + ROW.replace(b'Example A', b'Example B'))
                        os.utime(self.csv, ns=(info.st_atime_ns, info.st_mtime_ns))
                        self.assertEqual(pr._stamp(self.csv.stat()), capture.stamp)
                        with self.capture_fault(self.csv, 1, 'close'):
                            self.error(changed, pr._recheck, capture, changed, unavailable)
                    else:
                        def disappear(*args):
                            self.csv.unlink()
                            raise OSError('secret read failure after disappearance')
                        with patch.object(pr, '_read_stream', side_effect=disappear):
                            self.error(changed, pr._recheck, capture, changed, unavailable)
                        self.assertFalse(self.csv.exists())

    def test_a15_replay_rechecks_candidate_inputs(self):
        self.publish()
        before = self.state()
        verify = pr._verify
        def mutate(*args, **kwargs):
            result = verify(*args, **kwargs)
            self.vocab.write_bytes(VOCAB + b'\n')
            return result
        with patch.object(pr, '_verify', side_effect=mutate):
            self.error('INPUT_CHANGED', self.publish)
        self.assertEqual(self.state(), before)

    def test_a15_changed_inventory_and_input_disappearance(self):
        read = pr._read_stream
        def disappear(stream, limit):
            data = read(stream, limit)
            self.csv.unlink()
            return data
        with patch.object(pr, '_read_stream', side_effect=disappear):
            self.error('INPUT_CHANGED', pr.validate, **self.args)
        self.csv.write_bytes(HEADER)
        self.publish()
        recheck = pr._recheck
        def extra(capture, code, unavailable):
            result = recheck(capture, code, unavailable)
            if code == 'SNAPSHOT_CHANGED':
                (self.dest / 'extra').write_bytes(b'changed inventory')
            return result
        with patch.object(pr, '_recheck', side_effect=extra):
            self.error('SNAPSHOT_CHANGED', self.open)

    def test_a15_root_replacement_during_validation(self):
        candidate = pr._candidate
        def replace_root(*args, **kwargs):
            result = candidate(*args, **kwargs)
            self.root.rename(self.base / 'old-root')
            self.root.mkdir()
            return result
        with patch.object(pr, '_candidate', side_effect=replace_root):
            self.error('SNAPSHOT_CHANGED', self.publish)
        self.assertEqual(list(self.root.iterdir()), [])

    def test_a15_selected_directory_disappears_after_initial_guard(self):
        self.publish()
        lstat = Path.lstat
        observed = 0
        def move_after_guard(path, *args, **kwargs):
            nonlocal observed
            if path == self.dest:
                observed += 1
                if observed == 2:
                    path.rename(path.parent / 'moved')
            return lstat(path, *args, **kwargs)
        with patch.object(Path, 'lstat', move_after_guard):
            self.error('SNAPSHOT_CHANGED', self.open)

    def test_a15_output_mutation_during_open_and_before_rename(self):
        self.publish()
        recheck = pr._recheck
        changed = False
        def mutate(capture, code, unavailable):
            nonlocal changed
            if code == 'SNAPSHOT_CHANGED' and not changed:
                changed = True
                (self.dest / 'reference.csv').write_bytes(HEADER + ROW)
            return recheck(capture, code, unavailable)
        with patch.object(pr, '_recheck', side_effect=mutate):
            self.error('SNAPSHOT_CHANGED', self.open)
        write = pr._write
        def marker_mutation(path, data):
            write(path, data)
            if path.name == 'complete.tmp':
                (path.parent / 'reference.csv').write_bytes(HEADER + ROW)
        with patch.object(pr, '_write', side_effect=marker_mutation):
            self.error('SNAPSHOT_CHANGED', self.publish, revision='mutation')
        self.assertFalse((self.dest.parent / 'revision=mutation' / 'complete.json').exists())

    def test_a16_path_policy_inputs_root_and_links(self):
        self.error('SNAPSHOT_UNAVAILABLE', pr.publish, self.base / 'absent', **self.args)
        self.error('PATH_UNSAFE', pr.validate, **(self.args | {'csv_path': self.base / '..' / 'source.csv'}))
        for name, target in (('linked.csv', self.csv), ('linked-parent', self.base), ('linked-root', self.root)):
            link = self.base / name
            link.symlink_to(target, target_is_directory=target.is_dir())
            if name == 'linked-root':
                self.error('PATH_UNSAFE', pr.publish, link, **self.args)
            else:
                value = link / 'source.csv' if name == 'linked-parent' else link
                self.error('PATH_UNSAFE', pr.validate, **(self.args | {'csv_path': value}))
        hard = self.base / 'hard.csv'
        os.link(self.csv, hard)
        self.error('PATH_UNSAFE', pr.validate, **self.args)
        hard.unlink()
        inside = self.root / 'source.csv'
        inside.write_bytes(HEADER)
        self.error('PATH_UNSAFE', self.publish, csv_path=inside)
        fifo = self.base / 'pipe'
        os.mkfifo(fifo)
        with patch.object(os, 'open', side_effect=AssertionError('Opened special file')):
            self.error('PATH_UNSAFE', pr.validate, **(self.args | {'csv_path': fifo}))

    def test_a16_output_descendant_symlinks_and_hardlinks(self):
        parent = self.dest.parent
        parent.symlink_to(self.base, target_is_directory=True)
        self.error('PATH_UNSAFE', self.publish)
        parent.unlink()
        parent.mkdir()
        self.dest.symlink_to(self.base, target_is_directory=True)
        self.error('PATH_UNSAFE', self.publish)
        self.dest.unlink()
        self.publish()
        artifact = self.dest / 'reference.csv'
        artifact.unlink()
        artifact.symlink_to(self.csv)
        self.error('PATH_UNSAFE', self.open)
        artifact.unlink()
        os.link(self.csv, artifact)
        self.error('PATH_UNSAFE', self.open)

    def cli(self, args):
        out, err = io.StringIO(), io.StringIO()
        with redirect_stdout(out), redirect_stderr(err):
            code = pr.main(args)
        self.assertLessEqual(len(out.getvalue().encode()), 4096)
        self.assertLessEqual(len(err.getvalue().encode()), 4096)
        return code, out.getvalue(), err.getvalue()

    def options(self, command='validate'):
        return [command, '--csv', str(self.csv), '--vocabulary', str(self.vocab),
                '--reference-id', 'fictional-publishers', '--revision', 'r1',
                '--maintainer-id', 'example-team']

    def test_a17_cli_summaries_and_safe_argument_errors(self):
        code, out, err = self.cli(self.options())
        self.assertEqual((code, err), (0, ''))
        self.assertEqual(json.loads(out)['snapshot_fingerprint'], VECTOR)
        for args in ([], ['secret'], self.options() + ['--unknown', 'credential'],
                     self.options() + ['--revision', '/secret'], self.options() + ['--note'],
                     ['validate', '--csv=secret'], self.options() + ['--note', '--secret']):
            code, out, err = self.cli(args)
            self.assertEqual((code, out), (2, ''))
            self.assertEqual(json.loads(err)['code'], 'ARGUMENT_INVALID')
            self.assertNotIn('secret', err)
            self.assertNotIn('credential', err)
        code, out, err = self.cli(self.options('publish') + ['--root', str(self.root)])
        self.assertEqual((code, err), (0, ''))
        self.assertEqual(json.loads(out)['status'], 'published')
        code, out, err = self.cli(['verify', '--root', str(self.root), '--reference-id',
                                 'fictional-publishers', '--revision', 'r1', '--expected-fingerprint', VECTOR])
        self.assertEqual((code, err), (0, ''))
        self.assertEqual(json.loads(out)['status'], 'verified')

    def test_a17_error_precedence_and_safe_failures(self):
        self.csv.write_bytes(HEADER + b'secret,/secret/path,unreviewed,,')
        self.vocab.write_bytes(b'bad secret')
        self.dest.mkdir(parents=True)
        self.error('VOCABULARY_INVALID', self.publish)
        self.vocab.write_bytes(VOCAB)
        self.error('CSV_INVALID', self.publish)
        code, out, err = self.cli(self.options())
        self.assertEqual((code, out), (1, ''))
        self.assertEqual(json.loads(err)['code'], 'CSV_INVALID')
        self.assertNotIn('secret', err)
        self.assertNotIn(str(self.base), err)
        self.csv.write_bytes(HEADER)
        self.error('SNAPSHOT_INCOMPLETE', self.publish)
        self.csv.unlink()
        code, out, err = self.cli(self.options())
        self.assertEqual((code, out), (1, ''))
        self.assertEqual(json.loads(err)['code'], 'INPUT_UNAVAILABLE')
        self.assertNotIn(str(self.csv), err)

    def test_a17_recheck_io_cli_exit_and_fixed_diagnostics(self):
        self.publish()
        before = self.state()
        verify = ['verify', '--root', str(self.root), '--reference-id', 'fictional-publishers',
                  '--revision', 'r1', '--expected-fingerprint', VECTOR]
        cases = [
            (self.options(), self.csv, 2, 'INPUT_UNAVAILABLE'),
            (verify, self.dest / 'reference.csv', 2, 'SNAPSHOT_INVALID'),
            (self.options('publish') + ['--root', str(self.root)],
             self.vocab, 3, 'INPUT_UNAVAILABLE'),
        ]
        for options, path, occurrence, expected in cases:
            for fault in ('read', 'close'):
                with self.subTest(expected=expected, fault=fault):
                    evidence = self.evidence(path)
                    with self.capture_fault(path, occurrence, fault):
                        code, out, err = self.cli(options)
                    self.assertEqual((code, out), (1, ''))
                    self.assertEqual(json.loads(err), {'code': expected, 'message': pr.MESSAGES[expected]})
                    self.assertEqual(self.evidence(path), evidence)
                    self.assertEqual(self.state(), before)
        for fault in ('read', 'close'):
            dest = self.dest.parent / f'revision=cli-{fault}'
            options = self.options('publish')
            options[options.index('--revision') + 1] = f'cli-{fault}'
            with self.capture_fault(dest / 'manifest.json', 2, fault):
                code, out, err = self.cli(options + ['--root', str(self.root)])
            self.assertEqual((code, out), (1, ''))
            self.assertEqual(json.loads(err), {'code': 'PUBLICATION_FAILED',
                                              'message': pr.MESSAGES['PUBLICATION_FAILED']})
            self.assertFalse((dest / 'complete.json').exists())


if __name__ == '__main__':
    unittest.main()
