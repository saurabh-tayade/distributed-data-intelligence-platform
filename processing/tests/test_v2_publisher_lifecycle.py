"""Fault sequencing with real native files/readers and explicit Spark-only doubles.

The enclosing receipt/runtime fixture is synthetic. Actual Spark/Silver provenance
is exercised separately by test_v2_publishers_native, not claimed by these doubles.
"""
from copy import deepcopy
from pathlib import Path
from types import SimpleNamespace
import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import patch

import content_dependencies
import content_metadata as m
import narrow_content_publication_v2 as publisher
import narrow_projection_transport as transport
import narrow_publication_lifecycle_v2 as life
import projection_transport_fixtures as transport_fixtures
import publication_projection_fixtures as native_fixtures
from narrow_content_test_fixtures import snapshot
from publication_metadata import canonical_bytes
from test_narrow_content_publication import lifecycle_double


def bindings(native, scratch):
    expected, gold, gv, content, cv = transport_fixtures.with_native_bodies(native)
    scratch.mkdir()
    st = scratch.stat()
    binding = dict(root=str(scratch), rootDevice=str(st.st_dev), rootInode=str(st.st_ino))
    expected['bindings']['TEMP']['configuration'] = binding
    def fields(bound):
        value = bound.value()
        del value['protocolVersion']; del value['projectionContext']
        value['runtime'].update(scratchRoot=binding['root'], scratchDevice=binding['rootDevice'],
                                scratchInode=binding['rootInode'])
        return value
    gold = transport.create_command_v2('GOLD', fields(gold), expectations=expected)
    proof = transport.gold_proof_v2(gold, gv, artifact_id=transport_fixtures.ARTIFACT)
    expected['goldProof'] = deepcopy(proof)
    value = fields(content); value['goldProof'] = proof
    content = transport.create_command_v2('CONTENT', value, expectations=expected)
    return expected, gold, content


class ContentLifecycleV2Test(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name) / 'native'
        options = dict(empty=True, content_stress=True) if self._testMethodName == 'test_escaped_902_files_rejects_before_rename' else {}
        self.native_case = native_fixtures.case(self.base, **options)
        self.expected, self.gold, self.bound = bindings(self.native_case, self.base / 'scratch')
        self.args = self.native_case['args']
        self.command = self.bound.value()
        self.directory = m.publication_directory(self.args['output_root'], self.command['prepared']['fingerprint'])
        self.template = self.base / 'template'; self.directory.rename(self.template)
        self.dep = content_dependencies.verify_dependencies(**self.args)
        self.events = []
        self.native = lifecycle_double(self.template, self.events)
        args = {key: str(value) if isinstance(value, Path) else value
                for key, value in self.args.items() if key != 'output_root'}
        for patcher in (patch.object(publisher, '_native', return_value=self.native),
                patch.object(publisher, '_dependencies', return_value=(args, self.command['prepared'], self.dep)),
                patch.object(publisher, '_check_dependencies', side_effect=lambda *a: (self.dep.check(), life.roots(self.command)))):
            patcher.start(); self.addCleanup(patcher.stop)

    def publish(self, bound=None, expectations=None):
        return publisher.publish_v2(None, bound or self.bound, expectations=expectations or self.expected)

    def verify(self):
        return publisher.verify_publication_v2(None, self.bound, expectations=self.expected)

    def assert_incomplete(self):
        self.assertTrue((self.directory / 'complete.tmp').is_file())
        self.assertFalse((self.directory / 'complete.json').exists())

    def test_complete_qualifiers_before_rename_and_actual_after(self):
        stages = []
        original = life.qualify
        def qualify(bound, verification):
            stages.append('actual' if (self.directory / 'complete.json').exists() else 'projected')
            self.assertEqual(bound.value()['protocolVersion'], 2)
            return original(bound, verification)
        with patch.object(life, 'qualify', side_effect=qualify), \
                patch.object(transport, 'resume_v2', wraps=transport.resume_v2) as resumed:
            actual = self.publish()
        self.assertEqual(stages, ['projected', 'actual'])
        self.assertEqual([call.args[1] for call in resumed.call_args_list],
                         ['BEFORE_FETCH', 'GOLD', 'CONTENT_INPUTS'] * 2)
        self.assertEqual(actual['status'], 'VERIFIED')
        self.assertTrue(any(row['sha256'] != '0' * 64 for row in actual['smoke']))
        self.assertNotIn('cleanup', actual)
        before = snapshot(self.base)
        with patch.object(self.native, 'build_content', side_effect=AssertionError('replay wrote')):
            self.assertEqual(self.publish(), actual)
            self.assertEqual(self.verify(), actual)
        self.assertEqual(snapshot(self.base), before)

    def test_transport_reparse_before_io_and_forged_cache_rejected(self):
        forged = transport.CommandV2('CONTENT', self.bound._command, b'[]')
        for bound, expected in [(forged, self.expected),
                (self.bound, dict(self.expected, readyContentRuntimeSha256='0' * 64))]:
            with patch.object(publisher, '_dependencies', side_effect=AssertionError('I/O before reparse')):
                with self.assertRaises(ValueError):
                    self.publish(bound, expected)
        self.assertFalse(self.directory.exists())

    def test_each_qualification_failure_retains_temporary(self):
        with patch.object(transport, 'resume_v2', side_effect=transport.TransportError()):
            with self.assertRaises(transport.TransportError): self.publish()
        self.assert_incomplete()
        before = snapshot(self.base)
        with self.assertRaises(m.ContentFailure): self.publish()
        with self.assertRaises(m.ContentFailure): self.verify()
        self.assertEqual(snapshot(self.base), before)

    def test_all_complete_envelope_exact_and_plus_one_limits(self):
        # Compute the actual complete projected maximum, not per-component sums.
        measured = []
        original = life.qualify
        def stop(bound, verification):
            measured.extend(original(bound, verification))
            raise transport.TransportError()
        with patch.object(life, 'qualify', side_effect=stop):
            with self.assertRaises(transport.TransportError): self.publish()
        largest = max(map(len, measured))
        context = publisher._context(self.bound, self.expected)
        temporary = m.capture(self.directory, m.TABLES, self.native.DATA_TOP | {'complete.tmp'},
            'PUBLICATION_INVALID', 'PUBLICATION_CHANGED', guards=m.namespace_guards(self.args['output_root'], self.directory))
        coverage = self.native_case['coverage']
        value = publisher.projection.project_content_v2(context, temporary, coverage=coverage).sizing_value()
        with patch.object(transport, 'STAGE_LIMIT', largest):
            self.assertEqual(max(map(len, original(context.bound, value))), largest)
        with patch.object(transport, 'STAGE_LIMIT', largest - 1):
            with self.assertRaises(transport.TransportError): original(context.bound, value)
        self.assert_incomplete()

    def mutation(self, target):
        original = life.qualify
        def changed(bound, verification):
            result = original(bound, verification)
            if not (self.directory / 'complete.json').exists():
                if target == 'ancestor':
                    parent = self.directory.parent; moved = parent.with_name('moved')
                    parent.rename(moved); parent.mkdir()
                    for child in moved.iterdir(): child.rename(parent / child.name)
                elif target == 'foreign-marker':
                    (self.directory / 'complete.json').write_bytes(b'foreign')
                elif target == 'dependency':
                    path = self.dep.snapshot.directory / 'reference.csv'; path.write_bytes(path.read_bytes() + b'x')
                else:
                    path = self.directory / 'complete.tmp' if target in ('same-bytes-inode', 'mode', 'link') else next(self.directory.rglob('*.parquet'))
                    if target == 'same-bytes-inode':
                        raw = path.read_bytes(); saved = path.with_name('retained.tmp'); path.rename(saved); path.write_bytes(raw); saved.unlink()
                    elif target == 'mode': path.chmod(0o600 if path.stat().st_mode & 0o777 != 0o600 else 0o640)
                    elif target == 'link': os.link(path, self.template / 'linked-marker')
                    else: path.write_bytes(path.read_bytes() + b'x')
            return result
        with patch.object(life, 'qualify', side_effect=changed):
            with self.assertRaises(m.ContentFailure) as caught: self.publish()
        self.assertEqual(caught.exception.code, 'DEPENDENCY_CHANGED' if target == 'dependency' else 'PUBLICATION_CHANGED')
        if target != 'foreign-marker': self.assertFalse((self.directory / 'complete.json').exists())
        self.assertTrue((self.directory / 'complete.tmp').exists())

    def test_output_mutation_after_sizing(self): self.mutation('output')
    def test_marker_new_inode_same_bytes_after_sizing(self): self.mutation('same-bytes-inode')
    def test_marker_mode_mutation_after_sizing(self): self.mutation('mode')
    def test_marker_hardlink_after_sizing(self): self.mutation('link')
    def test_reference_mutation_after_sizing(self): self.mutation('dependency')
    def test_ancestor_swap_after_sizing(self): self.mutation('ancestor')
    def test_foreign_final_marker_is_not_overwritten(self): self.mutation('foreign-marker')

    def test_fault_before_rename_keeps_tmp(self):
        with patch.object(life, 'rename_last', side_effect=OSError('private')):
            with self.assertRaises(OSError): self.publish()
        self.assert_incomplete()

    def test_rename_acknowledgement_loss_retains_complete_and_reconciles_readonly(self):
        original = life.rename_last
        def lost(directory): original(directory); raise OSError('acknowledgement lost')
        with patch.object(life, 'rename_last', side_effect=lost):
            with self.assertRaises(OSError): self.publish()
        self.assertTrue((self.directory / 'complete.json').is_file())
        self.assertFalse((self.directory / 'complete.tmp').exists())
        before = snapshot(self.base)
        self.assertEqual(self.verify()['status'], 'VERIFIED')
        self.assertEqual(snapshot(self.base), before)

    def test_escaped_902_files_rejects_before_rename(self):
        with self.assertRaises(transport.TransportError): self.publish()
        self.assert_incomplete()
        self.assertEqual(len(list(self.directory.rglob('*.parquet'))), 902)

    def test_actual_envelope_failure_after_rename_keeps_completion(self):
        original = life.qualify
        def fail(bound, value):
            if (self.directory / 'complete.json').exists(): raise transport.TransportError()
            return original(bound, value)
        with patch.object(life, 'qualify', side_effect=fail):
            with self.assertRaises(transport.TransportError): self.publish()
        self.assertTrue((self.directory / 'complete.json').exists())
        before = snapshot(self.base)
        self.assertEqual(self.verify()['status'], 'VERIFIED')
        self.assertEqual(snapshot(self.base), before)

    def test_final_dependency_check_cannot_hide_output_mutation(self):
        original = publisher._check_dependencies
        def changed(*args):
            result = original(*args)
            if (self.directory / 'complete.json').exists():
                path = next(self.directory.rglob('*.parquet')); path.write_bytes(path.read_bytes() + b'x')
            return result
        with patch.object(publisher, '_check_dependencies', side_effect=changed):
            with self.assertRaises(m.ContentFailure): self.publish()
        self.assertTrue((self.directory / 'complete.json').exists())

    def test_actual_postrename_mismatch_is_not_rolled_back(self):
        original = publisher._actual
        def wrong(*args):
            value = original(*args); value['coverage']['total_sources'] = '999'; return value
        with patch.object(publisher, '_actual', side_effect=wrong):
            with self.assertRaises(publisher.projection.ProjectionError): self.publish()
        self.assertTrue((self.directory / 'complete.json').is_file())
        self.assertFalse((self.directory / 'complete.tmp').exists())

    def test_permission_error_is_not_absence(self):
        original = Path.lstat
        def denied(path):
            if path == self.directory: raise PermissionError('private')
            return original(path)
        before = snapshot(self.base)
        with patch.object(Path, 'lstat', denied):
            with self.assertRaises(PermissionError): self.publish()
        self.assertEqual(snapshot(self.base), before)


class GoldLifecycleV2Test(unittest.TestCase):
    def setUp(self):
        import narrow_gold_publication_v2 as gold
        self.publisher = gold
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name) / 'native'
        native_case = native_fixtures.case(self.base)
        self.expected, self.bound, _ = bindings(native_case, self.base / 'scratch')
        self.command = self.bound.value(); prepared = self.command['prepared']
        self.directory = Path(self.command['gold']['root']) / 'v1' / 'processor_revision=1' / ('input_set=' + prepared['inputSetFingerprint'])
        self.template = self.base / 'gold-template'; self.directory.rename(self.template)
        marker = json.loads((self.template / 'complete.json').read_bytes())
        self.inputs = SimpleNamespace(manifest=prepared['manifest'], input_set_fingerprint=prepared['inputSetFingerprint'], counts=prepared['counts'])
        schema = SimpleNamespace(simpleString=lambda: 'Spark-only-double', fieldNames=lambda: [])
        template = self.template
        class Frame:
            def __init__(self, table): self.table, self.schema, self.write = table, schema, self
            def select(self, _): return self
            def mode(self, value):
                assert value == 'errorifexists'; return self
            def parquet(self, path): shutil.copytree(template / self.table, path)
        import gold_reader
        self.native = SimpleNamespace(SCHEMAS=dict.fromkeys(gold.TABLES, schema),
            build_gold=lambda inputs: SimpleNamespace(articles=Frame(gold.TABLES[0]), source_daily=Frame(gold.TABLES[1])),
            _verify=lambda *a: marker,
            verify_publication=lambda inputs, root: gold_reader.verify_metadata(root, inputs.input_set_fingerprint).marker)
        for patcher in (patch.object(gold, '_native', return_value=self.native),
                patch.object(gold, '_load_inputs', return_value=self.inputs),
                patch.object(gold, '_check_inputs', side_effect=lambda *a: life.roots(self.command))):
            patcher.start(); self.addCleanup(patcher.stop)

    def publish(self): return self.publisher.publish_v2(None, self.bound, expectations=self.expected)
    def verify(self): return self.publisher.verify_publication_v2(None, self.bound, expectations=self.expected)

    def test_native_analytics_actual_projection_and_readonly_replay(self):
        with patch.object(self.publisher, '_actual', wraps=self.publisher._actual) as actual:
            value = self.publish()
        actual.assert_called_once()
        self.assertEqual(value['status'], 'VERIFIED')
        before = snapshot(self.base)
        with patch.object(self.native, 'build_gold', side_effect=AssertionError('replay wrote')):
            self.assertEqual(self.publish(), value)
            self.assertEqual(self.verify(), value)
        self.assertEqual(snapshot(self.base), before)

    def test_projected_rejection_before_rename_retains_every_file(self):
        with patch.object(life, 'qualify', side_effect=transport.TransportError()):
            with self.assertRaises(transport.TransportError): self.publish()
        self.assertTrue((self.directory / 'complete.tmp').exists())
        self.assertFalse((self.directory / 'complete.json').exists())
        before = snapshot(self.base)
        with self.assertRaises(m.ContentFailure): self.publish()
        self.assertEqual(snapshot(self.base), before)

    def test_same_bytes_marker_replacement_and_mutation_before_rename(self):
        original = life.qualify
        def replaced(*args):
            result = original(*args)
            path = self.directory / 'complete.tmp'; raw = path.read_bytes()
            retained = self.base / 'retained-marker'; path.rename(retained); path.write_bytes(raw)
            return result
        with patch.object(life, 'qualify', side_effect=replaced):
            with self.assertRaises(m.ContentFailure): self.publish()
        self.assertFalse((self.directory / 'complete.json').exists())

    def test_actual_fault_after_rename_keeps_completion(self):
        with patch.object(self.publisher, '_actual', side_effect=OSError('lost after rename')):
            with self.assertRaises(OSError): self.publish()
        self.assertTrue((self.directory / 'complete.json').exists())
        before = snapshot(self.base)
        self.assertEqual(self.verify()['status'], 'VERIFIED')
        self.assertEqual(snapshot(self.base), before)

    def test_final_dependency_check_cannot_hide_output_mutation(self):
        original = self.publisher._check_inputs
        def changed(*args):
            result = original(*args)
            if (self.directory / 'complete.json').exists():
                path = next(self.directory.rglob('*.parquet')); path.write_bytes(path.read_bytes() + b'x')
            return result
        with patch.object(self.publisher, '_check_inputs', side_effect=changed):
            with self.assertRaises(m.ContentFailure): self.publish()
        self.assertTrue((self.directory / 'complete.json').exists())


if __name__ == '__main__': unittest.main()
