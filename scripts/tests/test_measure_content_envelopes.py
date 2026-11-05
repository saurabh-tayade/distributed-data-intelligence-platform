"""Harness/serializer guards; explicit native measurement runs remain separate."""
import argparse
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest

SPEC = importlib.util.spec_from_file_location('content_envelope_measurement',
    Path(__file__).resolve().parents[1] / 'measure-content-envelopes.py')
measurement = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(measurement)


def canonical(value):
    return json.dumps(value, sort_keys=True, separators=(',', ':'), ensure_ascii=False).encode()


class ContentEnvelopeMeasurementTests(unittest.TestCase):
    def test_exact_and_plus_one_bytes_are_separate_from_parser_acceptance(self):
        for size in (65536, measurement.SLOT_BYTES, measurement.LEGACY_BYTES):
            value = dict(text='x' * (size - 11))
            actual = measurement.observation(value, canonical)
            self.assertEqual(actual['bytes'], size)
            value['text'] += 'x'
            over = measurement.observation(value, canonical)
            self.assertEqual(over['bytes'], size + 1)
            if size == measurement.SLOT_BYTES:
                self.assertTrue(actual['fits_1mib_candidate_slot'])
                self.assertFalse(over['fits_1mib_candidate_slot'])
                self.assertTrue(over['fits_16mib_legacy_byte_ceiling'])
            self.assertNotIn('parser_accepted', actual)

    def test_widths_count_escaping_unicode_keys_and_signed_tokens(self):
        value = {'🙂': ['é🙂"\\\n', -(10**19)]}
        result = measurement.characteristics(value)
        self.assertEqual(result, dict(max_value_depth=2, max_container_nesting=2,
            max_string_utf16_units=6, max_key_utf16_units=2, max_integer_token_chars=21))
        self.assertGreater(len(canonical(value)), result['max_string_utf16_units'])

    def test_empty_container_nesting_differs_from_value_depth(self):
        item = []
        for _ in range(16):
            item = [item]
        result = measurement.characteristics(item)
        self.assertEqual(result['max_value_depth'], 16)
        self.assertEqual(result['max_container_nesting'], 17)

    def test_constructor_extraction_requires_unique_field_signature(self):
        with tempfile.TemporaryDirectory(dir=measurement.REPO) as root:
            source = Path(root) / 'source.py'
            source.write_text('def body():\n    value = dict(one=x, two=2)\n')
            value, origin = measurement.production_expression(source, 'body', 'value',
                                                               ('one', 'two'), dict(x=7))
            self.assertEqual(value, dict(one=7, two=2))
            self.assertEqual(origin['first_line'], 2)
            with self.assertRaisesRegex(RuntimeError, 'constructor changed'):
                measurement.production_expression(source, 'body', 'value', ('one',), dict(x=7))
            source.write_text('def body():\n    value = dict(one=x, two=2)\n    value = dict(one=3, two=4)\n')
            with self.assertRaisesRegex(RuntimeError, 'constructor changed'):
                measurement.production_expression(source, 'body', 'value', ('one', 'two'), dict(x=7))

    def test_invalid_case_list_is_rejected_before_output_creation(self):
        with tempfile.TemporaryDirectory() as root:
            output = Path(root) / 'absent'
            for cases in ([], ['unknown'], ['small_nonempty', 'small_nonempty']):
                with self.assertRaises(ValueError):
                    measurement.measure(argparse.Namespace(output=output, cases=cases))
                self.assertFalse(output.exists())

    def test_existing_output_is_never_overwritten(self):
        with tempfile.TemporaryDirectory() as root:
            sentinel = Path(root) / 'retained.txt'
            sentinel.write_text('preserve')
            with self.assertRaises(FileExistsError):
                measurement.measure(argparse.Namespace(output=Path(root), cases=['small_nonempty']))
            self.assertEqual(sentinel.read_text(), 'preserve')

    def test_read_only_inventory_detects_same_length_file_change(self):
        with tempfile.TemporaryDirectory() as root:
            root = Path(root)
            file = root / 'data'
            file.write_bytes(b'ab')
            before = measurement.inventory(root)
            file.write_bytes(b'cd')
            self.assertNotEqual(before, measurement.inventory(root))


if __name__ == '__main__':
    unittest.main()
