"""Harness guards only; real native/HTTP measurements are a separate explicit run."""
import argparse
import importlib.util
import os
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location('serving_measurement',
    Path(__file__).resolve().parents[1] / 'measure-reliability-serving.py')
measurement = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(measurement)


class ServingMeasurementTests(unittest.TestCase):
    def test_summaries_retain_sample_count_and_range_without_tail_estimates(self):
        self.assertEqual(measurement.summary([4.0, 1.0, 3.0, 2.0]),
            dict(n=4, minimum_ms=1.0, median_ms=2.5, maximum_ms=4.0))

    def test_parity_compares_values_and_primitive_types(self):
        measurement.require_equal({'b': None, 'a': '1'}, {'a': '1', 'b': None})
        for actual, expected in ((True, 1), ('1', 1), (1.0, 1), ([], None)):
            with self.assertRaisesRegex(RuntimeError, 'primitive types'):
                measurement.require_equal(actual, expected)

    def test_sample_budget_is_checked_before_creating_output(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'uncreated'
            for count in (0, 21):
                with self.assertRaisesRegex(ValueError, '1..20'):
                    measurement.measure(argparse.Namespace(samples=count, output=output))
            self.assertFalse(output.exists())

    def test_existing_output_is_not_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            output = root / 'results'
            output.mkdir()
            sentinel = output / 'existing.txt'
            sentinel.write_text('preserve me')
            executable = root / 'java'
            executable.touch()
            jar = root / 'api.jar'
            jar.touch()
            with self.assertRaises(FileExistsError):
                measurement.measure(argparse.Namespace(samples=1, output=output, java=executable, jar=jar))
            self.assertEqual(sentinel.read_text(), 'preserve me')

    def test_deployment_and_jvm_overrides_do_not_enter_measurement_children(self):
        with patch.dict(os.environ, dict(DDIP_ROLE='api', SPRING_CONFIG_LOCATION='real.properties',
                RELIABILITY_ANALYTICS_ROOT='real-data', JAVA_TOOL_OPTIONS='-Xmx64m',
                PYTHONPATH='other-code', SAFE_SETTING='keep'), clear=True):
            self.assertEqual(measurement.child_env(), dict(SAFE_SETTING='keep',
                PYTHONDONTWRITEBYTECODE='1', PYTHONHASHSEED='0'))

    def test_inventory_detects_same_length_byte_changes(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            file = root / 'fixture'
            file.write_bytes(b'ab')
            before = measurement.inventory(root)
            file.write_bytes(b'cd')
            self.assertNotEqual(before['fixture']['sha256'], measurement.inventory(root)['fixture']['sha256'])

    def test_documented_workload_requires_real_first_and_final_pages(self):
        pages = {
            'first_page': dict(limit='1', offset='0', has_more=True,
                rows=[dict(service_id='svc00', request_count='2', duration_sum_ms='30')]),
            'later_page': dict(limit='1', offset='1', has_more=False,
                rows=[dict(service_id='svc01', request_count='1', duration_sum_ms='7')])}
        measurement.check_workload(pages)
        pages['later_page']['rows'] = []
        with self.assertRaises(RuntimeError):
            measurement.check_workload(pages)


if __name__ == '__main__':
    unittest.main()
