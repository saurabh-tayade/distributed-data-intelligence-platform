"""Selection and read-only Gold verification tests, plus actual native coverage in RssGoldIT."""
from copy import deepcopy
from hashlib import sha256
import unittest
import artifact_import_verifier as p
import managed_silver as d3
import managed_gold as d4
from test_bronze import Store,snapshot

class SelectionTest(unittest.TestCase):
    def setUp(self):
        store=Store();key,m=snapshot(store)
        self.r=dict(receiptVersion=1,runId='run',attemptId='attempt',sourceId=m['sourceId'],revision=2,capturedConfigSha256='1'*64,
            ingestionId=m['ingestionId'],ingestedAt=m['ingestedAt'],payloadByteLength=m['payloadByteLength'],payloadSha256=m['payloadSha256'],journalSha256='2'*64,
            storageBindingId='bronze',storageBindingSha256='3'*64,bucket='local',expected=dict(metadataKey=key,payloadKey=m['payloadKey'],metadataByteLength=len(store.objects[key]),metadataSha256=sha256(store.objects[key]).hexdigest()),payloadContentType='application/xml',metadataContentType='application/json')
        self.item=dict(artifactId='one',receipt=self.r,qualification=dict(selection=d3.selection(self.r,'silver')))
        self.c=dict(silverBindingId='silver',inputs=[self.item])
    def test_exact_selection_does_not_discover(self):self.assertEqual(d4.selected_markers(self.c),[self.item['qualification']['selection']['relativeAddress']])
    def test_empty_is_not_native_all_selection(self):
        self.c['inputs']=[]
        with self.assertRaises(p.Failure):d4.selected_markers(self.c)
    def test_duplicate_ids_and_paths_are_rejected(self):
        for same_id in (True,False):
            item=deepcopy(self.item)
            if not same_id:item['artifactId']='two'
            self.c['inputs']=[self.item,item]
            with self.assertRaises(p.Failure):d4.selected_markers(self.c)
    def test_incompatible_binding_or_identity_rejected(self):
        for field,value in [('storageBindingId','other'),('contractVersion',2)]:
            c=deepcopy(self.c);c['inputs'][0]['qualification']['selection'][field]=value
            with self.assertRaises(p.Failure):d4.selected_markers(c)
    def test_native_utf8_marker_order(self):
        item=deepcopy(self.item);item['artifactId']='two';r=item['receipt'];r['ingestionId']='00000000-0000-0000-0000-000000000002'
        for field in ('metadataKey','payloadKey'):r['expected'][field]=r['expected'][field].replace(self.r['ingestionId'],r['ingestionId'])
        item['qualification']['selection']=d3.selection(r,'silver');self.c['inputs']=[item,self.item]
        self.assertEqual(d4.selected_markers(self.c),sorted([i['qualification']['selection']['relativeAddress'] for i in self.c['inputs']]))

if __name__=='__main__':unittest.main()
