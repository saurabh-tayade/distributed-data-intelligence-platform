"""Pure-profile tests; real capture/identity/projection serializers, no Spark runtime."""
import ast
import base64
from copy import deepcopy
from datetime import date, datetime
from decimal import Decimal
from hashlib import sha256
import json
from pathlib import Path
import re
import sys
import tempfile
import types
import unittest
from uuid import UUID
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import narrow_content_profile as n
import silver_import_qualification as q
from publication_metadata import canonical_bytes
import content_analytics as analytics


def native_inventory(root, address):
    """Execute unchanged Spark-independent production bodies without importing Spark.

    This is serializer coverage, not a substitute for load_inputs/Spark validation.
    """
    source = Path(q.__file__).with_name('silver_inputs.py')
    names = {'InputFailure', 'safe_path', 'path_identity', 'parse_marker', 'file_record', 'inventory'}
    parsed = ast.parse(source.read_text())
    body = [node for node in parsed.body if isinstance(node, (ast.FunctionDef, ast.ClassDef)) and node.name in names]
    namespace = dict(Path=Path, sha256=sha256, json=json, re=re, base64=base64, date=date,
                     UUID=UUID, SCHEMAS={'accepted': None, 'rejected': None})
    exec(compile(ast.Module(body=body, type_ignores=[]), str(source), 'exec'), namespace)
    return namespace['inventory'](types.SimpleNamespace(root=root, markers=[address]))


class ProfileAssertions(unittest.TestCase):
    def bad(self, function, *args, **kwargs):
        with self.assertRaises(n.ProfileError) as caught:
            function(*args, **kwargs)
        self.assertEqual(str(caught.exception), 'CONTENT_PROFILE_INVALID')


class EnvelopeTest(ProfileAssertions):
    def test_every_byte_cap_exact_and_plus_one(self):
        for limit in (n.COMPLETION_LIMIT, n.IDENTITY_LIMIT, n.CAPTURE_LIMIT, n.SELECTION_LIMIT,
                      n.VERIFICATION_LIMIT, n.QUALIFICATION_LIMIT, n.REPORT_ROW_LIMIT):
            with self.subTest(limit=limit):
                value = {'x': 'a' * (limit - 8)}
                raw = n.canonical_envelope(value, limit)
                self.assertEqual(len(raw), limit)
                self.assertEqual(n.parse_envelope(raw, limit), value)
                value['x'] += 'a'
                self.bad(n.canonical_envelope, value, limit)
                self.bad(n.parse_envelope, canonical_bytes(value), limit)

    def test_depth_counts_containers_and_scalar_depth(self):
        value = 0
        for _ in range(16):
            value = [value]
        n.parse_envelope(n.canonical_envelope(value, 1000), 1000)
        self.bad(n.canonical_envelope, [value], 1000)
        value = []
        for _ in range(15):
            value = [value]
        n.canonical_envelope(value, 1000)
        self.bad(n.canonical_envelope, [value], 1000)  # 17 empty containers also fail.

    def test_utf16_string_bounds_and_unicode_scalars(self):
        for large, cap in ((False, 65536), (True, 131072)):
            for text in ('a' * cap, '\U0001f642' * (cap // 2)):
                n.canonical_envelope(text, 1024**2, large=large)
                self.bad(n.canonical_envelope, text + 'a', 1024**2, large=large)
        for value in ('\ud800', '\udfff', {'\ud800': 1}):
            self.bad(n.canonical_envelope, value, 1000)
        self.bad(n.parse_envelope, b'"\\ud800"', 1000)

    def test_field_name_limit_is_independent_utf16_bound(self):
        for large in (False, True):
            for key in ('a' * 50000, 'é' * 50000, '🙂' * 25000):
                with self.subTest(large=large, utf8_bytes=len(key.encode())):
                    value = {key: None}
                    raw = n.canonical_envelope(value, 1024**2, large=large)
                    self.assertEqual(n.parse_envelope(raw, 1024**2, large=large), value)
                    for suffix in ('a', '🙂'):
                        self.bad(n.canonical_envelope, {key + suffix: None}, 1024**2, large=large)
                        self.bad(n.parse_envelope, canonical_bytes({key + suffix: None}), 1024**2, large=large)
            # A value of the same 50,001 units is legal; the property name is not.
            n.canonical_envelope({'key': 'a' * 50001}, 1024**2, large=large)

    def test_integer_token_bounds_and_forbidden_floats(self):
        for value in (10**19, -(10**18), True, False, None):
            raw = n.canonical_envelope(value, 100)
            self.assertEqual(n.parse_envelope(raw, 100), value)
        for value in (10**20, -(10**19), 1.0, float('nan'), Decimal('1'), (), {1: 'bad'}):
            self.bad(n.canonical_envelope, value, 100)
        for raw in (b'1.0', b'1e1', b'NaN', b'Infinity', b'-Infinity', b'100000000000000000000'):
            self.bad(n.parse_envelope, raw, 100)

    def test_noncanonical_malformed_duplicate_extra_bytes(self):
        for raw in (b'', b'\xef\xbb\xbf{}', b'{}\n', b'{}{}', b'{"a":1,"a":2}', b'01',
                    b'-0', b'"\\u0061"', b'"\xff"', b'{"b":0,"a":0}', b' null', b'[]x'):
            self.bad(n.parse_envelope, raw, 1000)
        value = {'quote': '"\\\n\x00é🙂', 'array': [None, True, 2]}
        self.assertEqual(n.parse_envelope(canonical_bytes(value), 1000), value)


class SilverTest(ProfileAssertions):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(); self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name).resolve()
        source, ingestion, bucket = 'é-source', '00000000-0000-4000-8000-000000000001', 'test-bucket'
        encoded = base64.urlsafe_b64encode(source.encode()).decode().rstrip('=')
        suffix = f'source_id={encoded}/ingestion_date=2026-11-04/ingestion_id={ingestion}/'
        key = 'bronze/rss/' + suffix + 'metadata.json'
        address = 'v1/' + sha256(bucket.encode()).hexdigest() + '/' + suffix + 'complete.json'
        self.selection = dict(kind='SILVER', productId='content-intelligence', storageBindingId='silver',
            relativeAddress=address, contractVersion=1, processorRevision=1,
            identity=dict(sourceId=source, ingestionId=ingestion, bronzeStorageBindingId='bronze',
                          bronzeBucket=bucket, bronzeMetadataKey=key, bronzePayloadKey=key.replace('metadata.json', 'payload.xml')))
        q.selection(self.selection)
        self.namespace = self.root / Path(address).parent
        for dataset in ('accepted', 'rejected'):
            (self.namespace / dataset).mkdir(parents=True)
            (self.namespace / dataset / 'part-000.parquet').write_bytes(b'fixture, not a Parquet semantic test')
        self.marker = dict(identity=dict(metadata_sha256='a'*64, payload_sha256='b'*64,
            contract_version=1, processor_revision=1, bucket=bucket, metadata_key=key), counts=dict(accepted=1,rejected=0))
        (self.namespace / 'complete.json').write_bytes(json.dumps(self.marker).encode())
        s = self.root.stat()
        self.binding = dict(root=str(self.root),rootDevice=str(s.st_dev),rootInode=str(s.st_ino))
        self.bronze = dict(bucket=bucket,metadataKey=key,metadataByteLength=100,metadataSha256='a'*64,
            payloadKey=key.replace('metadata.json','payload.xml'),payloadByteLength=100,payloadSha256='b'*64)

    def capture(self):
        return q.capture(self.selection, self.binding)

    def verification(self):
        local = self.capture()
        value, digest = q.identity(self.selection, local, self.bronze)
        return dict(evidenceVersion=2, native=dict(verifier='silver_inputs.load_inputs', counts=self.marker['counts'],
            schemaPolicy='native-nullable-schema-and-required-row-values-v1', bronzeProvenance='all-rows-exact-v1',
            semanticReconstruction=False, sparkShutdown='CONFIRMED'), capture=dict(local=local,bronze=self.bronze),
            inventoryIdentity=dict(kind='E5_SILVER_INVENTORY_V1',sha256=digest,value=value))

    def test_real_capture_identity_qualification_and_readonly(self):
        before = {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in self.namespace.rglob('*') if p.is_file()}
        verification = self.verification()
        qualification = dict(qualificationVersion=1,selection=self.selection,verification=verification)
        raw = n.validate_silver_qualification(qualification)
        self.assertEqual(raw, canonical_bytes(qualification))
        self.assertEqual(n.parse_envelope(raw, n.QUALIFICATION_LIMIT), qualification)
        self.assertEqual(before, {p: (p.read_bytes(), p.stat().st_mtime_ns) for p in before})
        self.assertEqual(len(verification['capture']['local']['guards']), 6)

    def test_complete_envelope_budget_routing_uses_real_serialized_shapes(self):
        # Lower a budget to the exact observed production serializer length, then
        # one byte below it. This proves each guard charges its complete envelope;
        # the separate 4/16/32/36/52/60 KiB tests prove the configured byte boundaries.
        verification = self.verification()
        qualification = dict(qualificationVersion=1,selection=self.selection,verification=verification)
        manifest = native_inventory(self.root, self.selection['relativeAddress'])
        selected = dict(manifest=manifest,inputSetFingerprint=sha256(canonical_bytes(manifest)).hexdigest())
        cases = [
            ('CAPTURE_LIMIT', verification['capture']['local'], n.validate_silver_capture,
             (verification['capture']['local'],)),
            ('IDENTITY_LIMIT', verification['inventoryIdentity']['value'], n.validate_silver_verification,
             (verification, self.selection)),
            ('VERIFICATION_LIMIT', verification, n.validate_silver_verification,
             (verification, self.selection)),
            ('QUALIFICATION_LIMIT', qualification, n.validate_silver_qualification, (qualification,)),
            ('SELECTION_LIMIT', selected, n.validate_native_selection, (selected,)),
        ]
        for name, value, validator, args in cases:
            measured = len(canonical_bytes(value))
            with self.subTest(budget=name, measured=measured):
                with patch.object(n, name, measured):
                    validator(*args)
                with patch.object(n, name, measured - 1):
                    self.bad(validator, *args)

    def test_native_inventory_complete_selection(self):
        manifest = native_inventory(self.root, self.selection['relativeAddress'])
        wrapper = dict(manifest=manifest,inputSetFingerprint=sha256(canonical_bytes(manifest)).hexdigest())
        self.assertEqual(n.validate_native_selection(wrapper), canonical_bytes(wrapper))
        for where in ((), ('manifest',), ('manifest','inputs',0), ('manifest','inputs',0,'identity'),
                      ('manifest','inputs',0,'counts'), ('manifest','inputs',0,'files'),
                      ('manifest','inputs',0,'files','accepted',0)):
            for remove in (True, False):
                changed = deepcopy(wrapper); target = changed
                for key in where: target = target[key]
                if remove: target.pop(next(iter(target)))
                else: target['unexpected'] = 'private'
                self.bad(n.validate_native_selection, changed)
        wrapper['inputSetFingerprint'] = '0' * 64
        self.bad(n.validate_native_selection, wrapper)

    def test_maximum_native_layout_and_plus_one_file(self):
        for dataset in ('accepted', 'rejected'):
            for i in range(1,4):
                (self.namespace / dataset / f'part-{i}.parquet').write_bytes(b'x')
        (self.namespace / '_hidden').mkdir()
        for i in range(15):
            (self.namespace / '_hidden' / (f'.{i}-' + 'é"' * 70)).write_bytes(b'')
        marker = self.namespace / 'complete.json'
        marker.write_bytes(marker.read_bytes() + b' ' * (4096 - marker.stat().st_size))
        local = self.capture()
        self.assertEqual((len(local['files']),len(local['directories']),len(local['guards'])), (24,4,6))
        self.assertEqual(local['analyticalFileCounts'], dict(accepted=4,rejected=4))
        n.validate_silver_qualification(dict(qualificationVersion=1,selection=self.selection,verification=self.verification()))
        (self.namespace / '.extra').write_bytes(b'')
        self.bad(n.validate_silver_capture, self.capture())

    def test_each_native_count_plus_one(self):
        for i in range(1,5):
            (self.namespace / 'accepted' / f'part-{i}.parquet').write_bytes(b'')
        self.bad(n.validate_silver_capture, self.capture())
        for i in range(1,5): (self.namespace / 'accepted' / f'part-{i}.parquet').unlink()
        (self.namespace / '_one').mkdir(); (self.namespace / '_two').mkdir()
        self.bad(n.validate_silver_capture, self.capture())
        (self.namespace / '_one').rmdir(); (self.namespace / '_two').rmdir()
        marker = self.namespace / 'complete.json'; marker.write_bytes(b' ' * 4097)
        self.bad(n.validate_silver_capture, self.capture())
        marker.write_bytes(b' ' * 4096); local = self.capture(); n.validate_silver_capture(local)
        local['guards'].append(dict(path='extra',identity=['1','2','3']))
        self.bad(n.validate_silver_capture, local)

    def test_nested_shapes_identity_tamper_and_fixed_diagnostics(self):
        original = dict(qualificationVersion=1,selection=self.selection,verification=self.verification())
        locations = [(), ('verification',), ('selection',), ('verification','native'),
            ('verification','capture'), ('verification','capture','bronze'), ('verification','capture','local'),
            ('verification','capture','local','files',0), ('verification','capture','local','directories',0),
            ('verification','capture','local','guards',0), ('verification','inventoryIdentity'),
            ('verification','inventoryIdentity','value')]
        for where in locations:
            for remove in (True,False):
                changed = deepcopy(original); target = changed
                for key in where: target = target[key]
                if remove: target.pop(next(iter(target)))
                else: target['private-source-data'] = 'secret'
                self.bad(n.validate_silver_qualification, changed)
        changed = deepcopy(original); changed['verification']['inventoryIdentity']['sha256'] = '0'*64
        self.bad(n.validate_silver_qualification, changed)
        for mutation in (lambda x: x['directories'][0]['names'].append('unlisted'),
                         lambda x: x['files'].append(x['files'][0]),
                         lambda x: x.update(completionBase64='!!!!'),
                         lambda x: x['analyticalFileCounts'].update(accepted=2)):
            local = self.capture(); mutation(local); self.bad(n.validate_silver_capture, local)


class ReportTest(ProfileAssertions):
    def row(self, report):
        row = dict.fromkeys(analytics.REPORTS[report][1])
        row['source_id'] = 'fixture-é🙂'
        if report == 'catalog':
            row.update(title='"\\\n\x01🙂', quality_warnings=['INVALID_UPDATED_AT'],
                       observation_count=Decimal('2'),distinct_ingestion_count=3,
                       first_observed_at=datetime(2026,11,4),discovery_date=date(2026,11,4))
        elif report == 'reference_coverage':
            row.update(reference_status='MATCHED',reference_usage='UNUSED',article_count=0,
                       articles_missing_reference_count=0,missing_optional_attributes=['language','country'])
        else:
            row.update(discovery_date=date(2026,11,4),discovered_article_count=1,articles_missing_reference_count=0)
        return row

    def test_every_exact_native_projection_including_reference_only(self):
        for report in analytics.REPORTS:
            row = self.row(report)
            raw = n.validate_report_row(report,row)
            self.assertEqual(raw, canonical_bytes(analytics.public(row)))
            self.assertEqual(set(json.loads(raw)), set(analytics.REPORTS[report][1]))
            self.bad(n.validate_report_row,report,dict(row,extra=None))
            del row['source_id']; self.bad(n.validate_report_row,report,row)

    def test_full_rows_exact_60k_and_plus_one_all_reports(self):
        for report in analytics.REPORTS:
            row = self.row(report)
            field = 'title' if report == 'catalog' else 'source_id'
            row[field] = ''
            remaining = n.REPORT_ROW_LIMIT - len(canonical_bytes(analytics.public(row)))
            # Embedded controls, escapes and multi-byte scalars are charged by actual serialization.
            prefix = '\x00"\\é🙂'
            charge = len(canonical_bytes(prefix)) - 2
            row[field] = prefix + 'a' * (remaining - charge)
            self.assertEqual(len(n.validate_report_row(report,row)), 60 * 1024)
            row[field] += 'a'; self.bad(n.validate_report_row,report,row)

    def test_arrays_nulls_and_reference_only_large_row_not_exempt(self):
        row = self.row('reference_coverage')
        row['missing_optional_attributes'] = ['\x00' * 11000]
        self.assertLess(len(row['missing_optional_attributes'][0].encode()), 60*1024)
        self.bad(n.validate_report_row,'reference_coverage',row)
        row = self.row('catalog'); row['quality_warnings'] = ['x'*31000,'x'*31000]
        self.bad(n.validate_report_row,'catalog',row)
        self.bad(n.validate_report_row,'unknown',{})


if __name__ == '__main__':
    unittest.main()
