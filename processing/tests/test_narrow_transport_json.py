import tracemalloc
import unittest

from processing.narrow_transport_json import (
    TransportJsonError, canonical_envelope, parse_envelope,
)


class NarrowTransportJsonTest(unittest.TestCase):
    def test_canonical_roundtrip(self):
        value = {'😀': '\x00\b\f\n\r\t"\\', '\ue000': [None, True, False, -99999999999999999999]}
        raw = canonical_envelope(value, 1048576)
        self.assertTrue(raw.startswith('{"\ue000"'.encode()))
        self.assertIn(b'\\u0000\\b\\f\\n\\r\\t\\"\\\\', raw)
        self.assertEqual(value, parse_envelope(raw, 1048576))
        for item in (None, True, 0, '', [], {}):
            encoded = canonical_envelope(item, 65536)
            self.assertEqual(item, parse_envelope(encoded, len(encoded)))

    def test_string_key_and_number_boundaries(self):
        for large, maximum in ((False, 65536), (True, 131072)):
            for text in ('x' * maximum, '😀' * (maximum // 2)):
                raw = canonical_envelope(text, 1048576, large=large)
                self.assertEqual(text, parse_envelope(raw, 1048576, large=large))
                with self.assertRaises(TransportJsonError):
                    canonical_envelope(text + 'x', 1048576, large=large)
        canonical_envelope({'x' * 50000: 0}, 1048576)
        with self.assertRaises(TransportJsonError):
            canonical_envelope({'x' * 50001: 0}, 1048576)
        for number in (10 ** 20 - 1, -(10 ** 20 - 1)):
            self.assertEqual(number, parse_envelope(canonical_envelope(number, 64), 64))
        for number in (10 ** 20, -(10 ** 20)):
            with self.assertRaises(TransportJsonError):
                canonical_envelope(number, 64)

    def test_depth_including_empty_containers(self):
        for leaf in (0, [], {}):
            value = leaf
            for _ in range(16 if leaf == 0 else 15):
                value = [value]
            raw = canonical_envelope(value, 65536)
            self.assertEqual(value, parse_envelope(raw, 65536))
            with self.assertRaises(TransportJsonError):
                canonical_envelope([value], 65536)

    def test_large_selector_requires_boolean(self):
        for flag in (0, 1, None, 'true', [], {}):
            with self.assertRaises(TransportJsonError):
                canonical_envelope({}, 65536, large=flag)
            with self.assertRaises(TransportJsonError):
                parse_envelope(b'{}', 65536, large=flag)

    def test_existing_destination_limits(self):
        for limit in (65536, 1048576, 16 * 1024 * 1024):
            # A one-element empty list costs four bytes; each additional
            # element contributes three bytes plus its string contents.
            remaining = limit - 1
            value = []
            while remaining > 131075:
                value.append("x" * 131072)
                remaining -= 131075
            value.append("x" * (remaining - 3))
            raw = canonical_envelope(value, limit)
            self.assertEqual(limit, len(raw))
            self.assertEqual(value, parse_envelope(raw, limit))
            with self.assertRaises(TransportJsonError):
                canonical_envelope(value, limit - 1)

    def test_strict_invalid_inputs(self):
        for raw in (b'', b' ', b'{} ', b'{}{}', b'{"a":0,"a":1}', b'{"b":0,"a":1}',
                    b'\xef\xbb\xbf{}', b'"\xff"', b'"\\ud800"', b'"\\udc00"',
                    b'NaN', b'1.0', b'1e0', b'-0', b'"\\u0061"', b'"\\/"', b'[' * 17 + b']' * 17):
            with self.subTest(raw=raw):
                with self.assertRaises(TransportJsonError) as caught:
                    parse_envelope(raw, 1048576)
                self.assertEqual('CONTENT_TRANSPORT_INVALID', caught.exception.code)
        for value in (1.0, {'a': float('inf')}, {1: 2}, ('a',), '\ud800'):
            with self.assertRaises(TransportJsonError):
                canonical_envelope(value, 65536)

    def test_byte_budget_and_no_whole_encoded_temporary(self):
        value = ['x' * 131072] * 1000
        tracemalloc.start()
        try:
            with self.assertRaises(TransportJsonError):
                canonical_envelope(value, 32)
            _, peak = tracemalloc.get_traced_memory()
        finally:
            tracemalloc.stop()
        self.assertLess(peak, 65536)
        for value in ('😀', '\x00', [1, 2]):
            raw = canonical_envelope(value, 65536)
            self.assertEqual(raw, canonical_envelope(value, len(raw)))
            with self.assertRaises(TransportJsonError):
                canonical_envelope(value, len(raw) - 1)
            with self.assertRaises(TransportJsonError):
                parse_envelope(raw, len(raw) - 1)
        for limit in (0, -1, True, 16 * 1024 * 1024 + 1):
            with self.assertRaises(TransportJsonError):
                canonical_envelope({}, limit)


if __name__ == '__main__':
    unittest.main()
