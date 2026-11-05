"""V2 context reparsing and immutable sizing against real native captures."""
from copy import deepcopy
from dataclasses import FrozenInstanceError, replace
import json
from pathlib import Path
import sys
import tempfile
import unittest

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]
import content_metadata as m
import narrow_projection_transport as transport
import narrow_publication_projection_v2 as projection
import projection_transport_fixtures as transport_fixtures
import publication_projection_fixtures as native_fixtures
from publication_metadata import canonical_bytes


class ProjectionV2Test(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.temp = tempfile.TemporaryDirectory()
        cls.native = native_fixtures.case(Path(cls.temp.name) / 'native')
        cls.expected, cls.gold, cls.actual_gold, cls.content, cls.actual_content = transport_fixtures.with_native_bodies(cls.native)

    @classmethod
    def tearDownClass(cls):
        cls.temp.cleanup()

    def test_complete_v2_capture_matches_actual_without_downgrade(self):
        for kind, bound, actual in [('gold', self.gold, self.actual_gold), ('content', self.content, self.actual_content)]:
            context = projection.bind_context_v2(bound, expectations=self.expected)
            self.assertEqual(context.bound, bound)
            self.assertEqual(context.command_bytes, bound._command)
            self.assertEqual(json.loads(context.command_bytes)['protocolVersion'], 2)
            evidence = self.native[kind][1]
            sized = (projection.project_gold_v2(context, evidence) if kind == 'gold' else
                     projection.project_content_v2(context, evidence, coverage=m.freeze(self.native['coverage'])))
            projection.compare_actual_v2(sized, actual)
            altered = sized.sizing_value()
            altered['prepared'].clear()
            self.assertEqual(sized.sizing_value()['prepared'], actual['prepared'])
            with self.assertRaises(FrozenInstanceError):
                context.kind = 'OTHER'
            with self.assertRaises(TypeError):
                json.dumps(sized)

    def test_reparse_rejects_forged_snapshot_and_independent_expectation_changes(self):
        for bound in (self.gold, self.content):
            for forged in (replace(bound, kind='OTHER'), replace(bound, _receipts=b'[]')):
                with self.assertRaises(projection.ProjectionError):
                    projection.bind_context_v2(forged, expectations=self.expected)
            for change in ('binding', 'original', 'runtime'):
                expected = deepcopy(self.expected)
                if change == 'binding':
                    expected['bindings'][bound.kind]['configuration']['rootInode'] = '999'
                elif change == 'original':
                    expected['original']['silverInput']['receipt']['ingestedAt'] = 'changed'
                else:
                    expected['readyContentRuntimeSha256'] = '0' * 64
                with self.assertRaises(projection.ProjectionError):
                    projection.bind_context_v2(bound, expectations=expected)

    def test_legacy_mixed_nested_and_non_publish_commands_are_refused(self):
        for bound in (self.gold, self.content):
            for mode in ('PREPARE', 'VERIFY', 'RECHECK'):
                command = bound.value() | dict(mode=mode)
                forged = replace(bound, _command=canonical_bytes(command))
                with self.assertRaises(projection.ProjectionError):
                    projection.bind_context_v2(forged, expectations=self.expected)
            command = bound.value()
            command['protocolVersion'] = 1
            del command['projectionContext']
            with self.assertRaises(projection.ProjectionError):
                projection.bind_context_v2(replace(bound, _command=canonical_bytes(command)), expectations=self.expected)
        command = self.content.value()
        command['goldProof']['command']['protocolVersion'] = 1
        with self.assertRaises(projection.ProjectionError):
            projection.bind_context_v2(replace(self.content, _command=canonical_bytes(command)), expectations=self.expected)

    def test_capture_and_actual_mutations_are_rejected(self):
        context = projection.bind_context_v2(self.gold, expectations=self.expected)
        evidence = self.native['gold'][1]
        with self.assertRaises(projection.ProjectionError):
            projection.project_gold_v2(context, replace(evidence, guards=()))
        with self.assertRaises(projection.ProjectionError):
            projection.project_gold_v2(replace(context, command_bytes=b'{}'), evidence)
        sized = projection.project_gold_v2(context, evidence)
        with self.assertRaises(projection.ProjectionError):
            projection.compare_actual_v2(sized, self.actual_gold | dict(completionSha256='f' * 64))


if __name__ == '__main__':
    unittest.main()
