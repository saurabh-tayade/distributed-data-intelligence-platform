"""Exact receipt adapter tests; actual Spark/SeaweedFS/PostgreSQL coverage is RssSilverIT."""
from copy import deepcopy
from hashlib import sha256
import unittest

import managed_silver as d3
import artifact_import_verifier as p
from test_bronze import Store, snapshot
from bronze_to_silver import Failure as BronzeFailure


class ManagedSelectionTest(unittest.TestCase):
    def setUp(self):
        self.store = Store(); key,m = snapshot(self.store)
        self.receipt = dict(receiptVersion=1,runId='run',attemptId='attempt',sourceId=m['sourceId'],revision=2,
            capturedConfigSha256='1'*64,ingestionId=m['ingestionId'],ingestedAt=m['ingestedAt'],
            payloadByteLength=m['payloadByteLength'],payloadSha256=m['payloadSha256'],journalSha256='2'*64,
            storageBindingId='bronze',storageBindingSha256='3'*64,bucket='local',
            expected=dict(metadataKey=key,payloadKey=m['payloadKey'],metadataByteLength=len(self.store.objects[key]),
                          metadataSha256=sha256(self.store.objects[key]).hexdigest()),
            payloadContentType='application/xml',metadataContentType='application/json')
        original=self.store.get_object
        def get_object(Bucket,Key):
            response=original(Bucket=Bucket,Key=Key)
            response.update(ContentType='application/json' if Key.endswith('metadata.json') else 'application/xml',ContentLength=len(self.store.objects[Key]))
            return response
        self.store.get_object=get_object

    def test_exact_identity_is_original_ingestion(self):
        a = d3.selection(self.receipt,'silver')
        self.assertEqual(a['identity']['ingestionId'],self.receipt['ingestionId'])
        self.assertNotIn(self.receipt['runId'],a['relativeAddress'])
        self.assertTrue(a['relativeAddress'].endswith('/complete.json'))

    def test_two_gets_only_no_listing_or_expansion(self):
        snapshot(self.store,number=2)
        d3.bronze(self.store,self.receipt)
        self.assertEqual(self.store.reads,list(self.receipt['expected'][k] for k in ('metadataKey','payloadKey')))

    def test_native_adapter_uses_validated_bytes_after_backend_changes(self):
        exact = d3.bronze(self.store,self.receipt)
        key = self.receipt['expected']['payloadKey']; before=self.store.objects[key]
        self.store.objects[key]=b'changed'
        self.assertEqual(exact.get_object(Bucket='local',Key=key)['Body'].read(),before)
        with self.assertRaises((p.Failure,BronzeFailure)):d3.bronze(self.store,self.receipt)

    def test_changed_metadata_fails_original_digest(self):
        self.store.objects[self.receipt['expected']['metadataKey']]+=b' '
        with self.assertRaises((p.Failure,BronzeFailure)):d3.bronze(self.store,self.receipt)

    def test_object_header_change_rejects_original_receipt(self):
        from bronze_to_silver import Failure
        original=self.store.get_object
        for field,value in [('ContentType','text/plain'),('ContentLength',0),('ContentEncoding','gzip')]:
            def get_object(Bucket,Key):return dict(original(Bucket=Bucket,Key=Key),**{field:value})
            self.store.get_object=get_object
            with self.subTest(field=field),self.assertRaises(Failure):d3.bronze(self.store,self.receipt)

    def test_missing_exact_object_does_not_fallback(self):
        del self.store.objects[self.receipt['expected']['payloadKey']]
        with self.assertRaises(Exception):d3.bronze(self.store,self.receipt)

    def test_conflicting_source_ingestion_or_key_fails(self):
        for field,value in [('sourceId','different'),('ingestionId','00000000-0000-4000-8000-000000000001')]:
            r=deepcopy(self.receipt);r[field]=value
            with self.subTest(field=field),self.assertRaises(p.Failure):d3.selection(r,'silver')

    def test_exact_reader_rejects_unselected_key_or_bucket(self):
        exact=d3.bronze(self.store,self.receipt)
        for bucket,key in [('other',self.receipt['expected']['payloadKey']),('local','unselected')]:
            with self.assertRaises(p.Failure):exact.get_object(Bucket=bucket,Key=key)

    def test_unknown_or_incomplete_receipt_is_not_accepted(self):
        for change in ('unknown','missing'):
            r=deepcopy(self.receipt)
            if change=='unknown':r['other']=True
            else:del r['journalSha256']
            with self.assertRaises(p.Failure):d3.selection(r,'silver')


if __name__ == '__main__':unittest.main()
