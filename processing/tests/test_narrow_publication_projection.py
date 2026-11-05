"""Pure projection, real consumer parity and retained-evidence fault tests; no Spark."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
from hashlib import sha256
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import unittest
from unittest.mock import patch

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]
import content_metadata as m
import managed_content
import narrow_publication_projection as p
from publication_metadata import canonical_bytes
import publication_projection_fixtures as fixtures
from narrow_content_test_fixtures import snapshot


class ProjectionTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.addClassCleanup(cls.temp.cleanup)
        cls.root = Path(cls.temp.name)
        cls.case = fixtures.case(cls.root / 'nonempty')

    def bad(self, function, *args, **kwargs):
        with self.assertRaises(p.ProjectionError) as caught:
            function(*args, **kwargs)
        self.assertEqual(str(caught.exception), 'CONTENT_PROJECTION_INVALID')

    def project(self, kind, context, evidence, coverage=None):
        if kind == 'GOLD':
            return p.project_gold_v1(context, evidence)
        return p.project_content_v1(context, evidence, coverage=coverage or self.case['coverage'])

    def test_actual_six_field_capture_matches_after_real_same_directory_rename(self):
        for kind in ('GOLD', 'CONTENT'):
            context, evidence, projected, actual = self.case[kind.lower()]
            self.assertEqual(len(canonical_bytes(projected.sizing_value())), len(canonical_bytes(actual)))
            if kind == 'GOLD':
                self.assertEqual(canonical_bytes(projected.sizing_value()), canonical_bytes(actual))
            else:
                expected = deepcopy(actual); expected['smoke'] = p._smoke_shape()
                self.assertEqual(projected.sizing_value(), expected)
            p.compare_actual_v1(projected, actual)
            original_marker = next(item for item in evidence.files if item.path.name == 'complete.tmp')
            final_marker = next(item for item in actual['capture']['files'] if item['path'] == 'complete.json')
            self.assertEqual(len(original_marker.stamp), 6)
            self.assertEqual(final_marker['stamp'], [str(part) for part in original_marker.stamp])
            self.assertEqual(final_marker['sha256'], original_marker.digest)
            self.assertEqual(final_marker['byteLength'], original_marker.length)
            for item, final in zip(evidence.files, actual['capture']['files'], strict=True):
                expected = 'complete.json' if item.path.name == 'complete.tmp' and item.path.parent == evidence.directory else item.path.relative_to(evidence.directory).as_posix()
                self.assertEqual(final['path'], expected)
                self.assertEqual(final['stamp'], list(map(str, item.stamp)))
            for (_, (identity, names)), final in zip(evidence.directories, actual['capture']['directories'], strict=True):
                self.assertEqual(final['identity'], list(map(str, identity)))
                self.assertEqual(final['names'], sorted(('complete.json' if name == 'complete.tmp' else name for name in names), key=lambda s: s.encode()))

    def test_projection_has_no_io_and_never_mutates_inputs_or_publications(self):
        before = snapshot(self.root)
        for kind in ('GOLD', 'CONTENT'):
            context, evidence, expected, _ = self.case[kind.lower()]
            with patch.object(Path, 'open', side_effect=AssertionError('I/O')), \
                    patch.object(Path, 'lstat', side_effect=AssertionError('I/O')), \
                    patch.object(Path, 'resolve', side_effect=AssertionError('I/O')), \
                    patch.object(Path, 'rename', side_effect=AssertionError('I/O')), \
                    patch.object(m.Evidence, 'check', side_effect=AssertionError('I/O')):
                value = self.project(kind, context, evidence)
                self.assertEqual(value.sizing_value(), expected.sizing_value())
            changed = value.sizing_value(); changed['capture']['files'][0]['path'] = 'changed'
            self.assertNotEqual(changed, value.sizing_value())
            with self.assertRaises(FrozenInstanceError):
                value._sizing_bytes = b'{}'
            with self.assertRaises(TypeError):
                canonical_bytes(value)  # Not directly serializable as native evidence.
        self.assertEqual(snapshot(self.root), before)

    def test_context_is_bound_to_independent_complete_runtime_and_binding(self):
        for kind in ('GOLD', 'CONTENT'):
            context = self.case[kind.lower()][0]
            command = json.loads(context.command_bytes)
            binding = json.loads(context.binding_bytes)
            configuration = self.case['configurations'][kind]
            rebound = p.bind_context_v1(kind, command, runtime_configuration=configuration, output_binding=binding)
            self.assertEqual(rebound, context)
            for changed in (configuration | {'unrecognized': True}, configuration | {kind.lower() + 'BindingId': 'another'}):
                self.bad(p.bind_context_v1, kind, command, runtime_configuration=changed, output_binding=binding)
            for changed in (binding | {'root': binding['root'] + '/other'}, binding | {'rootInode': '999'}, binding | {'rootDevice': '999'}):
                self.bad(p.bind_context_v1, kind, command, runtime_configuration=configuration, output_binding=changed)
            for changed in (command | {'protocolVersion': True}, command | {'mode': 'VERIFY'}, command | {'verified': {}}, command | {'extra': 0}):
                self.bad(p.bind_context_v1, kind, changed, runtime_configuration=configuration, output_binding=binding)
            command['prepared'].clear(); binding['root'] = '/changed'
            self.assertEqual(rebound, context)

    def test_missing_partial_duplicate_or_visible_markers_are_rejected(self):
        for kind in ('GOLD', 'CONTENT'):
            context, evidence, _, _ = self.case[kind.lower()]
            marker = next(item for item in evidence.files if item.path == evidence.directory / 'complete.tmp')
            variants = [replace(evidence, top=evidence.top | {'complete.json'}),
                replace(evidence, files=tuple(item for item in evidence.files if item != marker)),
                replace(evidence, files=(*evidence.files, marker)),
                replace(evidence, guards=()), replace(evidence, tables=tuple(reversed(evidence.tables)))]
            for replacement in (replace(marker, data=marker.data[:-1]), replace(marker, digest='f' * 64),
                                replace(marker, stamp=(*marker.stamp, 0)),
                                replace(marker, path=marker.path.with_name('complete.json')),
                                replace(marker, stamp=(*marker.stamp[:5], 2))):
                variants.append(replace(evidence, files=tuple(replacement if item == marker else item for item in evidence.files)))
            for changed in variants:
                with self.subTest(kind=kind, changed=repr(changed)[:80]):
                    self.bad(self.project, kind, context, changed)

    def test_completion_manifest_and_inventory_correlations_are_not_omitted(self):
        for kind in ('GOLD', 'CONTENT'):
            context, evidence, _, _ = self.case[kind.lower()]
            marker_file = next(item for item in evidence.files if item.path.name == 'complete.tmp')
            marker = json.loads(marker_file.data)
            marker['files'][evidence.tables[0]][0]['sha256'] = 'f' * 64
            raw = canonical_bytes(marker)
            changed_file = replace(marker_file, data=raw, length=len(raw), digest=sha256(raw).hexdigest(),
                                   stamp=(*marker_file.stamp[:2], len(raw), *marker_file.stamp[3:]))
            changed = replace(evidence, files=tuple(changed_file if item == marker_file else item for item in evidence.files))
            self.bad(self.project, kind, context, changed)
            wrong = json.loads(context.command_bytes)
            wrong['prepared']['manifest']['wrong'] = 'changed'
            self.bad(self.project, kind, replace(context, command_bytes=canonical_bytes(wrong)), evidence)

    def test_exact_coverage_strings_and_fixed_smoke_grammar_not_fake_success(self):
        context, evidence, projected, actual = self.case['content']
        totals = self.case['coverage']
        self.assertEqual(len(canonical_bytes(p._smoke_shape())), 339)
        for rows in (0, 1):
            alternate = deepcopy(actual)
            alternate['smoke'] = [dict(name=name, sha256='f' * 64, rows=rows) for name in p.REPORTS]
            p.compare_actual_v1(projected, alternate)
        # The comparison checks sizing correspondence, not native report truth.
        for bad_value in (0, True, '-1', '01', str(m.MAX_COUNT + 1)):
            changed = dict(totals); changed['article_count'] = bad_value
            self.bad(p.project_content_v1, context, evidence, coverage=changed)
        self.bad(p.project_content_v1, context, evidence, coverage={})
        changed = dict(totals); changed['article_count'] = '999'
        altered = p.project_content_v1(context, evidence, coverage=changed)
        self.bad(p.compare_actual_v1, altered, actual)
        for rows in (True, 2, '1'):
            altered = deepcopy(actual); altered['smoke'][0]['rows'] = rows
            self.bad(p.compare_actual_v1, projected, altered)
        for change in ('order', 'missing', 'digest'):
            altered = deepcopy(actual)
            if change == 'order':
                altered['smoke'].reverse()
            elif change == 'missing':
                altered['smoke'].pop()
            else:
                altered['smoke'][0]['sha256'] = 'f' * 63
            self.bad(p.compare_actual_v1, projected, altered)

    def test_empty_native_publications_and_escaped_file_stress_keep_exact_parity(self):
        for name, options in [('empty', dict(empty=True)), ('both-902', dict(empty=True, gold_stress=True, content_stress=True))]:
            generated = fixtures.case(self.root / name, **options)
            for kind in ('gold', 'content'):
                _, _, projected, actual = generated[kind]
                p.compare_actual_v1(projected, actual)
                if name == 'both-902':
                    self.assertGreater(len(projected._sizing_bytes), 1024**2)
                # Native acceptance does not imply managed slot qualification.

    def test_post_rename_same_bytes_new_inode_is_not_repaired_or_accepted(self):
        generated = fixtures.case(self.root / 'new-inode')
        context, evidence, projected, _ = generated['gold']
        marker = evidence.directory / 'complete.json'
        replacement = marker.with_name('replacement.fixture')
        replacement.write_bytes(marker.read_bytes()); replacement.replace(marker)
        before = snapshot(self.root / 'new-inode')
        actual = fixtures.actual_gold(generated['args'], json.loads(context.command_bytes)['prepared'])
        self.bad(p.compare_actual_v1, projected, actual)
        self.assertEqual(snapshot(self.root / 'new-inode'), before)

    def test_injected_qualifier_failure_before_rename_retains_temporary_output(self):
        for kind in ('GOLD', 'CONTENT'):
            destination = self.root / (kind.lower() + '-qualifier-rejected')
            state = {}
            def reject(observed_kind, args, evidence, context, projected):
                if observed_kind == kind:
                    state['directory'] = evidence.directory
                    state['snapshot'] = snapshot(destination)
                    raise RuntimeError('injected sizing rejection, not a production guard')
            with self.assertRaisesRegex(RuntimeError, 'injected sizing rejection'):
                fixtures.case(destination, before_rename=reject)
            self.assertEqual(snapshot(destination), state['snapshot'])
            self.assertTrue((state['directory'] / 'complete.tmp').exists())
            self.assertFalse((state['directory'] / 'complete.json').exists())

    def test_completed_requalification_is_read_only_and_uses_actual_verifier(self):
        before = snapshot(self.root / 'nonempty')
        gold_context, _, projected_gold, _ = self.case['gold']
        content_context, _, projected_content, _ = self.case['content']
        actual_gold = fixtures.actual_gold(self.case['args'], json.loads(gold_context.command_bytes)['prepared'])
        actual_content = managed_content.verify(json.loads(content_context.command_bytes), self.case['args'])
        p.compare_actual_v1(projected_gold, actual_gold)
        p.compare_actual_v1(projected_content, actual_content)
        self.assertEqual(snapshot(self.root / 'nonempty'), before)

    def test_mutation_before_rename_is_vetoed_by_retained_native_evidence(self):
        for kind in ('GOLD', 'CONTENT'):
            for fault in ('partial', 'mode', 'visible'):
                destination = self.root / (kind.lower() + '-' + fault)
                state = {}
                def change(observed_kind, args, evidence, context, projected):
                    if observed_kind != kind:
                        return
                    marker = evidence.directory / 'complete.tmp'
                    if fault == 'partial':
                        marker.write_bytes(marker.read_bytes()[:-1])
                    elif fault == 'mode':
                        os.chmod(marker, 0o600 if stat.S_IMODE(marker.stat().st_mode) != 0o600 else 0o400)
                    else:
                        (evidence.directory / 'complete.json').write_bytes(b'foreign marker')
                    state['directory'] = evidence.directory
                    state['snapshot'] = snapshot(destination)
                with self.assertRaises(m.ContentFailure):
                    fixtures.case(destination, before_rename=change)
                self.assertEqual(snapshot(destination), state['snapshot'])
                self.assertTrue((state['directory'] / 'complete.tmp').exists())
                self.assertEqual((state['directory'] / 'complete.json').exists(), fault == 'visible')


if __name__ == '__main__':
    unittest.main()
