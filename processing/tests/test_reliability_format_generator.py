"""Bounded generator correctness; no 100K/1M generation or performance study."""
from collections import Counter
from hashlib import sha256
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch
import uuid
from publication_metadata import canonical_bytes
import reliability_benchmark_generator as g
import reliability_benchmark_oracle as oracle
import reliability_format_metadata as m


class GeneratorTests(unittest.TestCase):
    def test_moderate_quota_hash_bands_ids_routes_and_time(self):
        services=Counter();joint=Counter();ids=set()
        for i in range(2000):
            row=g.event(i);services[row['service_id']]+=1
            def independent(field):return int.from_bytes(sha256(f'20261019/{i}/{field}'.encode()).digest()[:8],'big')
            band=independent('status_band')%100
            self.assertEqual(2 if band<90 else 4 if band<95 else 5,row['status_code']//100)
            joint[row['service_id'],row['status_code']//100]+=1
            duration_band=independent('duration_band')%100
            self.assertTrue((1<=row['duration_ms']<=100) if duration_band<80 else (101<=row['duration_ms']<=1000) if duration_band<99 else (1001<=row['duration_ms']<=60000))
            self.assertEqual(1790812800000000+independent('event_time')%86400000000,row['event_time_epoch_us'])
            self.assertTrue(row['route_id'].startswith(row['service_id']+'-r'))
            self.assertLess(len(g.wire(row)),320)
            identifier=uuid.UUID(row['event_id']);self.assertEqual(8,identifier.version)
            self.assertNotIn(row['event_id'],ids);ids.add(row['event_id'])
        self.assertEqual(1200,services['svc00']);self.assertEqual([89]*8+[88],[services[f'svc{i:02d}'] for i in range(1,10)])
        self.assertEqual(2000,sum(joint.values()))

    def test_exact_duplicate_and_five_rejection_class_counts(self):
        selected=[];policy=g.profile('PARQUET',g.config(2000));valid=[];rejected=[]
        for batch in ('b00','b01'):
            for row in g.batch_records(2000,batch):
                values=dict(row);values['event_time']=values.pop('event_time_epoch_us')
                service,event,payload,reasons=oracle.typed(values,policy)
                if payload is not None:valid.append(canonical_bytes(payload))
                else:rejected.append(reasons)
                selected.append(batch)
        self.assertEqual(Counter(b00=1800,b01=260),Counter(selected))
        self.assertEqual((2060,2040,2000,40,20),(len(selected),len(valid),len(set(valid)),len(valid)-len(set(valid)),len(rejected)))
        self.assertEqual(Counter(FIELD_NULL=8,FIELD_VALUE=12),Counter(r[0] for r in rejected))

    def test_hot_and_equal_variants_and_identity_collision_gate(self):
        self.assertEqual(1800,sum(g.event(i,'hot')['service_id']=='svc00' for i in range(2000)))
        for i in range(100):
            self.assertTrue(1790856000000000<=g.event(i,'hot')['event_time_epoch_us']<1790856300000000)
            self.assertEqual(17,g.event(i,'equal')['duration_ms'])
        with tempfile.TemporaryDirectory() as root,patch.object(g,'event_id',return_value='00000000-0000-8000-8000-000000000000'):
            with self.assertRaises(m.ReliabilityFailure) as raised:g.prepare(root,g.config(100))
            self.assertEqual('BENCHMARK_INVALID',raised.exception.code);self.assertEqual([],list(Path(root).iterdir()))

    def test_small_preparation_is_exclusive_and_deterministic(self):
        with tempfile.TemporaryDirectory() as first,tempfile.TemporaryDirectory() as second:
            g.prepare(first,g.config(100));g.prepare(second,g.config(100))
            a={p.relative_to(first).as_posix():sha256(p.read_bytes()).hexdigest() for p in Path(first).rglob('*.jsonl')}
            b={p.relative_to(second).as_posix():sha256(p.read_bytes()).hexdigest() for p in Path(second).rglob('*.jsonl')}
            self.assertEqual(a,b);self.assertEqual(16,len(a))
            # No format-specific Parquet byte-equality promise is needed for logical parity.
            sample=Path(first)/'parquet/b00/part-00.parquet';before=sample.read_bytes()
            with self.assertRaises(m.ReliabilityFailure):g.write_parquet(sample,[])
            self.assertEqual(before,sample.read_bytes())


if __name__=='__main__':unittest.main()
