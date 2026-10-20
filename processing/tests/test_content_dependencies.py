"""B01/B07/B09/B12: dependency, identity and ordinary-I/O contracts."""
from hashlib import sha256
import os
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest
from unittest.mock import patch

import duckdb

import content_dependencies as d
import content_metadata as m
import gold_analytics as ga
import publisher_reference as pr
from publication_metadata import canonical_bytes
from content_fixtures import CSV, gold_fixture


class DependencyTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.base = Path(self.tmp.name)
        self.gold = self.base / 'gold'
        self.reference = self.base / 'reference'
        self.reference.mkdir()
        self.csv = self.base / 'source.csv'
        self.csv.write_bytes(CSV)
        self.vocab = self.base / 'vocabulary.json'
        self.vocab.write_bytes(canonical_bytes(pr.VOCABULARY))
        fingerprint = pr.publish(self.reference, self.csv, self.vocab, 'fixture-publishers', 'r1', 'example-team')['snapshot_fingerprint']
        self.args = dict(gold_root=self.gold, gold_fingerprint=gold_fixture(self.gold),
                         reference_root=self.reference, reference_id='fixture-publishers',
                         revision='r1', reference_fingerprint=fingerprint)

    def failure(self, code, function, *args, **kwargs):
        with self.assertRaises(m.ContentFailure) as caught:
            function(*args, **kwargs)
        self.assertEqual(caught.exception.diagnostic, dict(code=code, message=m.MESSAGES[code]))

    def test_b01_full_gate_and_detached_inventory(self):
        with patch.object(ga, 'open_publication', wraps=ga.open_publication) as opened:
            dependency = d.verify_dependencies(**self.args)
        opened.assert_called_once_with(self.gold.resolve(), self.args['gold_fingerprint'], memory_limit='512MiB')
        self.assertEqual(len(dependency.paths('gold_articles')), 1)
        self.assertEqual(len(dependency.reference.rows), 3)
        with self.assertRaises(TypeError):
            dependency.gold_marker['output_counts']['gold_articles'] = 9
        dependency.check()
        self.assertEqual(m.validate_manifest(dependency.manifest_bytes), dependency.manifest())
        # Original authoring locations are irrelevant once E1a has published.
        self.csv.unlink()
        self.vocab.unlink()
        d.verify_dependencies(**self.args)

    def test_b01_metadata_only_cannot_replace_full_verification(self):
        with patch.object(ga, 'open_publication', side_effect=ga.AnalyticsFailure('INVALID_VALUES')):
            self.failure('DEPENDENCY_INVALID', d.verify_dependencies, **self.args)

    def test_b01_actual_gold_value_corruption_with_valid_metadata(self):
        directory = self.gold / 'v1' / 'processor_revision=1' / f'input_set={self.args["gold_fingerprint"]}'
        path = directory / 'gold_articles' / 'part.parquet'
        with duckdb.connect() as conn:
            conn.read_parquet(str(path)).create_view('original')
            conn.execute('CREATE TABLE bad AS SELECT * REPLACE (NULL::VARCHAR AS title) FROM original')
            path.unlink()
            conn.table('bad').write_parquet(str(path))
        marker = __import__('json').loads((directory / 'complete.json').read_bytes())
        marker['files']['gold_articles'][0].update(byte_length=path.stat().st_size,
                                                 sha256=sha256(path.read_bytes()).hexdigest())
        (directory / 'complete.json').write_bytes(canonical_bytes(marker))
        # The legacy metadata-only check succeeds; the required full gate must fail.
        d.gold_reader.verify_metadata(self.gold, self.args['gold_fingerprint'])
        self.failure('DEPENDENCY_INVALID', d.verify_dependencies, **self.args)

    def test_b12_close_failure_and_disappearance_after_observation(self):
        dependency = d.verify_dependencies(**self.args)
        before = {f.path: (m.stamp(f.path.stat()), f.path.read_bytes()) for f in dependency.gold.files}
        real = os.fdopen
        class CloseFailure:
            def __init__(self, stream):
                self.stream = stream
            def __enter__(self):
                return self.stream
            def __exit__(self, *args):
                self.stream.close()
                raise OSError('secret close failure')
        with patch.object(os, 'fdopen', side_effect=lambda *a,**k: CloseFailure(real(*a,**k))):
            self.failure('DEPENDENCY_INVALID', dependency.check)
        self.assertEqual({p:(m.stamp(p.stat()),p.read_bytes()) for p in before}, before)
        path = dependency.snapshot.directory / 'reference.csv'
        path.unlink()
        self.failure('DEPENDENCY_CHANGED', dependency.check)

    def test_b12_directory_replacement_and_io_during_recheck(self):
        dependency = d.verify_dependencies(**self.args)
        with patch.object(os, 'scandir', side_effect=OSError('/secret inventory')):
            self.failure('DEPENDENCY_INVALID', dependency.check)
        directory = dependency.snapshot.directory
        moved = directory.with_name('moved')
        directory.rename(moved)
        directory.mkdir()
        for child in moved.iterdir():
            child.rename(directory / child.name)
        self.failure('DEPENDENCY_CHANGED', dependency.check)

    def test_b12_mutation_evidence_survives_close_errors(self):
        previous = m.observe_file(self.csv, m.MANIFEST_LIMIT, 'DEPENDENCY_INVALID',
                                  'DEPENDENCY_CHANGED', retain=True)
        real = os.fdopen
        class CloseFailure:
            def __init__(self, stream):
                self.stream=stream
            def __enter__(self):
                return self.stream
            def __exit__(self,*args):
                self.stream.close()
                raise OSError('secret close')
        for changed in (False,True):
            if changed:
                info=self.csv.stat()
                self.csv.write_bytes(CSV.replace(b'science',b'sciance'))
                os.utime(self.csv,ns=(info.st_atime_ns,info.st_mtime_ns))
                self.assertEqual(m.stamp(self.csv.stat()),previous.stamp)
            with patch.object(os,'fdopen',side_effect=lambda *a,**k:CloseFailure(real(*a,**k))):
                self.failure('DEPENDENCY_CHANGED' if changed else 'DEPENDENCY_INVALID',
                    m.observe_file,self.csv,m.MANIFEST_LIMIT,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',
                    previous,retain=True)

    def test_b12_inventory_close_error_is_not_automatically_mutation(self):
        old=m._names(self.reference,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED')
        scandir=os.scandir
        class CloseFailure:
            def __init__(self,path):
                self.entries=scandir(path)
            def __enter__(self):
                return self.entries
            def __exit__(self,*args):
                self.entries.close()
                raise OSError('secret inventory close')
        for changed in (False,True):
            if changed:
                (self.reference/'extra').write_bytes(b'changed')
            with patch.object(os,'scandir',side_effect=CloseFailure):
                self.failure('DEPENDENCY_CHANGED' if changed else 'DEPENDENCY_INVALID',
                    m._names,self.reference,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',old)

    def test_b12_failed_stream_construction_closes_descriptor(self):
        with patch.object(os,'fdopen',side_effect=OSError('secret fdopen')), \
                patch.object(os,'close',wraps=os.close) as closed:
            self.failure('DEPENDENCY_INVALID',m.observe_file,self.csv,m.MANIFEST_LIMIT,
                         'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',retain=True)
        closed.assert_called_once()
        with self.assertRaises(OSError):
            os.fstat(closed.call_args.args[0])

    def test_b12_partial_inventory_preserves_observed_new_entry(self):
        old=m._names(self.reference,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED')
        class Partial:
            def __init__(self,name):
                self.name=name
            def __enter__(self):
                def entries():
                    yield SimpleNamespace(name=self.name)
                    raise OSError('secret iteration failure')
                return entries()
            def __exit__(self,*args):
                return False
        for name,code in ((old[0],'DEPENDENCY_INVALID'),('extra','DEPENDENCY_CHANGED')):
            if name=='extra':
                (self.reference/name).write_bytes(b'new observed entry')
            with patch.object(os,'scandir',side_effect=lambda p:Partial(name)):
                self.failure(code,m._names,self.reference,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',old)

    def test_b09_missing_and_unsafe_selections(self):
        self.failure('ARGUMENT_INVALID', d.verify_dependencies, **(self.args | {'gold_fingerprint': '../secret'}))
        self.failure('PATH_UNSAFE', d.verify_dependencies, **(self.args | {'reference_root': self.gold}))
        self.failure('DEPENDENCY_UNAVAILABLE', d.verify_dependencies, **(self.args | {'gold_fingerprint': 'a' * 64}))
        directory = self.reference / 'reference_id=fixture-publishers' / 'revision=r1'
        (directory / 'complete.json').unlink()
        self.failure('DEPENDENCY_INVALID', d.verify_dependencies, **self.args)

    def test_b12_mutations_and_io_failures(self):
        evidence = d.verify_dependencies(**self.args)
        path = evidence.paths('gold_articles')[0]
        with patch.object(os, 'open', side_effect=OSError('/secret credential')):
            self.failure('DEPENDENCY_INVALID', evidence.check)
        old = Path(path)
        replacement = old.with_suffix('.moved')
        old.rename(replacement)
        old.write_bytes(replacement.read_bytes())
        replacement.unlink()
        self.failure('DEPENDENCY_CHANGED', evidence.check)

    def test_b07_literal_identity_vectors(self):
        value = dict(manifest_version=1, product_id=m.PRODUCT, product_contract_version=1,
            processor_revision=1, dataset_contract_versions=dict(m.VERSIONS), semantic_config=dict(m.SEMANTICS),
            gold=dict(publication_fingerprint='0'*64,
                      input_manifest=dict(file='gold-inputs.json', byte_length=256, sha256='0'*64),
                      completion=dict(file='gold-complete.json', byte_length=512, sha256='1'*64)),
            reference=dict(reference_id='fixture-publishers', revision='r1',
                           snapshot_fingerprint='a98795fc7537ebcb31b10f2f1e8ec1bb6a8425bbd3871b9d92b1c3c94dfcaba1'))
        self.assertEqual(len(canonical_bytes(value)), 905)
        self.assertEqual(sha256(canonical_bytes(value)).hexdigest(), '3ef7a9603c8d558008026aa8a4e8ca8f66966e2db445d042f00531501c770dfb')
        b = value | {'reference': dict(reference_id='fixture-publishers', revision='r2',
            snapshot_fingerprint='487620b9287188e8d69b6ae67f027edeff259e6b0301315a32d3bee9b39c8da4')}
        self.assertEqual(sha256(canonical_bytes(b)).hexdigest(), '8b9235817778d0fb111074b36f1cca24937a46f63e4191d70207dca5b91bdde7')
        c = value | {'gold': value['gold'] | {'completion': value['gold']['completion'] | {'sha256': '2'*64}}}
        self.assertEqual(sha256(canonical_bytes(c)).hexdigest(), 'a679f85af47810745b4d6bc223ecb4d98350d4eb602462f2ddc9b940f3615ff1')
        self.assertEqual(m.validate_manifest(canonical_bytes(value)), value)
        for invalid in (canonical_bytes(value)+b'\n', canonical_bytes(value).replace(b'"processor_revision":1', b'"processor_revision":true')):
            self.failure('PUBLICATION_INVALID' if invalid.endswith(b'\n') else 'UNSUPPORTED_VERSION', m.validate_manifest, invalid)

    def test_gold_memory_budget_compatibility(self):
        with ga.open_publication(self.gold, self.args['gold_fingerprint']) as legacy:
            rows = legacy.report('articles')
        with ga.open_publication(self.gold, self.args['gold_fingerprint'], memory_limit='512MiB') as bounded:
            self.assertEqual(bounded.report('articles'), rows)
            self.assertEqual(bounded._connection.execute("SELECT current_setting('memory_limit')").fetchone()[0], '512.0 MiB')
        for invalid in (True, 512, '512MiB; secret', '', '0MiB'):
            with self.assertRaises(ga.AnalyticsFailure) as caught:
                ga.open_publication(self.gold, self.args['gold_fingerprint'], memory_limit=invalid)
            self.assertEqual(caught.exception.code, 'INVALID_ARGUMENT')
        with self.assertRaises(ga.AnalyticsFailure) as caught:
            ga.open_publication(self.gold, self.args['gold_fingerprint'], memory_limit='1KiB')
        self.assertEqual(caught.exception.code, 'RESOURCE_LIMIT')


if __name__ == '__main__':
    unittest.main()
