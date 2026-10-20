"""Independent reader-core/efficiency gate using bounded synthetic Parquet fixtures."""
from pathlib import Path
import json
import resource
import sys
import tempfile
import time
import unittest
from unittest.mock import patch

import duckdb
from publication_metadata import canonical_bytes
import reliability_metadata as m
import reliability_inputs as inputs
import reliability_analytics as analytics
from reliability_fixtures import event,register_fixture,reference_fixture


def direct_fixture(root,n):
    """Known unique events; SQL fixture construction is not the Spark implementation."""
    root=Path(root);rows=[canonical_bytes(event(i,duration_ms=i%3600001,status_code=500 if i%10==0 else 200)) for i in range(1,n+1)]
    batchroot,summary=register_fixture(root,rows)
    refroot,ref=reference_fixture(root)
    deps=inputs.verify_dependencies(batch_root=batchroot,batches=[('fixture-source','b1',summary['batch_fingerprint'])],reference_root=refroot,reference_id='fixture-services',revision='r1',reference_fingerprint=ref['snapshot_fingerprint'])
    directory=root/'pending';directory.mkdir();m.write(directory/'inputs.json',deps.manifest_bytes)
    with analytics.connection() as db:
        db.execute('CREATE TEMP TABLE seed(id BIGINT)');db.execute('INSERT INTO seed SELECT range FROM range(1,?)',[n+1])
        db.execute("CREATE TABLE events AS SELECT 1::INTEGER contract_version,1::INTEGER schema_version,printf('00000000-0000-0000-0000-%012d',id) event_id,'alpha' service_id,'test' environment,make_timestamptz(1792497600000000) event_time,'GET' AS \"method\",'health' route_id,CASE WHEN id%10=0 THEN 500 ELSE 200 END::INTEGER status_code,(id%3600001)::BIGINT duration_ms,true is_synthetic,1::BIGINT valid_delivery_count,make_timestamptz(1792498200000000) first_received_at,make_timestamptz(1792498200000000) last_received_at,'Alpha Service' service_name,'example-platform' owner_team,'critical' service_tier,'MATCHED' reference_status,'fixture-services' reference_id,'r1' reference_revision,?::VARCHAR reference_fingerprint FROM seed",[deps.reference.fingerprint])
        # Typed safe raw occurrence evidence, independently indexed from source bytes.
        batch=deps.batches[0];file=batch.manifest['files'][0]
        occurrence=[]
        for row in inputs.scan_file(batch.evidence.directory/file['file'],batch.manifest['profile']):
            occurrence.append((1,batch.fingerprint,'fixture-source','b1',batch.manifest['ingestion_id'],'events',file['sha256'],row['line_number'],row['byte_offset'],row['physical_byte_length'],1792498200000000,'ACCEPTED','alpha',row['recognized_event_id'],row['semantic_sha256'],[]))
        ddl=','.join(name+' '+('BIGINT' if kind=='timestamp' else analytics.DUCK_TYPES[kind]) for name,kind in m.SCHEMAS[m.OCCURRENCES])
        db.execute('CREATE TABLE occurrence_seed('+ddl+')')
        db.executemany('INSERT INTO occurrence_seed VALUES ('+','.join('?' for _ in range(16))+')',occurrence)
        columns=','.join('make_timestamptz(received_at) AS received_at' if name=='received_at' else name for name,kind in m.SCHEMAS[m.OCCURRENCES])
        db.execute('CREATE VIEW occurrences AS SELECT '+columns+' FROM occurrence_seed')
        p95=sorted(i%3600001 for i in range(1,n+1))[(95*n+99)//100-1] if n else None
        # Exact nearest-rank is a qualified candidate for a verifier fixture, not Spark p95 evidence.
        db.execute("CREATE TABLE metrics AS SELECT 1::INTEGER contract_version,'alpha' service_id,'test' environment,make_timestamptz(1792497600000000) window_start,?::BIGINT request_count,?::BIGINT server_error_count,?::DOUBLE server_error_rate,?::DECIMAL(38,0) duration_sum_ms,?::DOUBLE mean_duration_ms,?::BIGINT p95_duration_ms_approx,'Alpha Service' service_name,'example-platform' owner_team,'critical' service_tier,'MATCHED' reference_status,'fixture-services' reference_id,'r1' reference_revision,?::VARCHAR reference_fingerprint WHERE ?>0",[n,n//10,n//10/n if n else 0,n*(n+1)//2,(n+1)/2 if n else 0,p95,deps.reference.fingerprint,n])
        for table,view in ((m.EVENTS,'events'),(m.OCCURRENCES,'occurrences'),(m.METRICS,'metrics')):
            (directory/table).mkdir();db.table(view).write_parquet(str(directory/table/'part.parquet'))
    evidence=m.capture(directory,{'inputs.json':m.MANIFEST_LIMIT},m.TABLES,'PUBLICATION_INVALID','PUBLICATION_CHANGED')
    marker=dict(completion_version=1,product_id=m.PRODUCT,product_fingerprint=deps.fingerprint,manifest_byte_length=len(deps.manifest_bytes),
        occurrence_counts=dict(physical_occurrences=n,valid_occurrences=n,unique_valid_events=n,identical_duplicate_occurrences=0,rejected_occurrences=0),
        output_counts={m.EVENTS:n,m.OCCURRENCES:n,m.METRICS:1 if n else 0},files=evidence.inventory(),completed_at=m.now())
    return deps,evidence,marker


class VerificationTests(unittest.TestCase):
    def test_reader_efficiency_gate_one_ordered_query(self):
        observations=[]
        for n in (2051,6149):
            with tempfile.TemporaryDirectory() as root:
                start=time.perf_counter();deps,evidence,marker=direct_fixture(root,n)
                prepared=time.perf_counter()-start
                db,stats=analytics.verify_core(deps,evidence,marker,explain=True)
                try:
                    self.assertEqual(n,stats['rows']);self.assertEqual((n+1023)//1024,stats['chunks'])
                    self.assertLessEqual(stats['max_chunk_bytes'],m.CHUNK_BYTES)
                    nodes=[]
                    def visit(node):
                        nodes.append(node['name'].strip())
                        for child in node.get('children',[]):visit(child)
                    for node in json.loads(stats['plan']):visit(node)
                    self.assertEqual(2,nodes.count('PARQUET_SCAN'))
                    self.assertIn('ORDER_BY',nodes);self.assertIn('HASH_JOIN',nodes)
                    class Recording:
                        def __init__(self):self.queries=[]
                        def execute(self,sql,*args):self.queries.append(sql);return db.execute(sql,*args)
                    recording=Recording();checked=analytics.verify_raw(recording,deps)
                    self.assertEqual([analytics.raw_query()],recording.queries)
                    settings=db.execute("SELECT current_setting('threads'),current_setting('memory_limit'),current_setting('temp_directory')").fetchone()
                    peak=resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
                    observations.append(dict(rows=n,chunks=stats['chunks'],max_chunk_bytes=stats['max_chunk_bytes'],raw_verification_seconds=round(stats['elapsed_seconds'],4),preparation_seconds=round(prepared,4),
                        parquet_scan_nodes=2,ordered_queries=1,duckdb_settings=list(settings),python_peak_rss_bytes=peak if sys.platform=='darwin' else peak*1024))
                finally:db.close()
        print(json.dumps({'e2_reader_efficiency_gate':observations},sort_keys=True))

    def test_independent_rank_and_semantic_corruption_rejected(self):
        with tempfile.TemporaryDirectory() as root:
            deps,evidence,marker=direct_fixture(root,20)
            with analytics.connection() as db:
                for table in m.TABLES:analytics.register_table(db,table,evidence.paths(table))
                # A fresh output hash cannot make a wrong metric/rank valid.
                path=Path(root)/'bad.parquet'
                db.execute('CREATE TABLE bad AS SELECT * REPLACE(1::BIGINT AS p95_duration_ms_approx) FROM '+m.METRICS)
                db.table('bad').write_parquet(str(path))
            target=evidence.directory/m.METRICS/'part.parquet';target.write_bytes(path.read_bytes())
            altered=m.capture(evidence.directory,{'inputs.json':m.MANIFEST_LIMIT},m.TABLES,'PUBLICATION_INVALID','PUBLICATION_CHANGED')
            marker['files']=altered.inventory()
            with self.assertRaises(m.ReliabilityFailure) as caught:analytics.verify_core(deps,altered,marker)
            self.assertEqual('PUBLICATION_INVALID',caught.exception.code)

    def test_r14_known_mutation_is_not_remapped_by_io_cleanup(self):
        with tempfile.TemporaryDirectory() as root:
            deps,evidence,marker=direct_fixture(root,20)
            with patch.object(analytics,'verify_raw',side_effect=m.ReliabilityFailure('PUBLICATION_CHANGED')),patch.object(inputs.Dependencies,'check',side_effect=m.ReliabilityFailure('DEPENDENCY_INVALID')) as checked:
                with self.assertRaises(m.ReliabilityFailure) as caught:analytics.verify_core(deps,evidence,marker)
                self.assertEqual('PUBLICATION_CHANGED',caught.exception.code);checked.assert_not_called()

    def test_literal_product_vector(self):
        doc=(Path(__file__).resolve().parents[2]/'docs/architecture/application-reliability-publication.md').read_text()
        import re
        blocks=[json.loads(s) for s in re.findall(r'~~~json\n([^\n]+)\n~~~',doc)]
        vector=next(s for s in blocks if s.get('product_id')==m.PRODUCT)
        raw=canonical_bytes(vector);m.validate_manifest(raw)
        from hashlib import sha256
        self.assertEqual('d0bfa671ea0a1726de7b7bb72e5e1e157e97f13138f7f85be8bba7e1cca25996',sha256(raw).hexdigest())


if __name__=='__main__':unittest.main()
