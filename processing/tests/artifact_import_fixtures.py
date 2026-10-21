"""Small synthetic inputs only. Never used by the operator import implementation."""
from hashlib import sha256
from pathlib import Path
import json
import sys
from publication_metadata import canonical_bytes
from reliability_format_fixtures import g1, policy, reference


def create(base):
    base = Path(base).resolve()
    selections, roots = [], {}
    for fmt in ('JSONL', 'PARQUET'):
        work = base / fmt.lower()
        work.mkdir()
        import reliability_format_inputs as inputs
        import reliability_format_metadata as metadata
        from reliability_benchmark_generator import write_parquet
        incoming=work/'incoming';incoming.mkdir();root=work/'batches';root.mkdir()
        source='fixture-'+fmt.lower();profile=policy(fmt)|dict(synthetic_source_id=source)
        raw=incoming/('events.'+fmt.lower())
        if fmt=='JSONL':raw.write_bytes(b''.join(canonical_bytes(r)+b'\n' for r in g1()))
        else:
            def transport():
                for r in g1():
                    r=dict(r);t=r.pop('event_time');r['event_time_epoch_us']=metadata.time_us(t) if t else None;yield r
            write_parquet(raw,transport())
        result=inputs.register(root,input_root=incoming,source_id=source,batch_id='b00',input_format=fmt,
            files=[dict(artifact_id='events',input_file=raw.name)],received_at='2026-10-02T00:05:00.000000Z',profile=profile)
        manifest = json.loads((root / f'v2/format={fmt.lower()}/source_id={source}/batch_id=b00/manifest.json').read_bytes())
        binding = '30000000-0000-4000-8000-00000000000' + ('1' if fmt=='JSONL' else '2')
        roots[binding] = str(root)
        selections.append(dict(kind='RELIABILITY_BATCH_V2', productId='application-reliability', storageBindingId=binding,
            relativeAddress=f'v2/format={fmt.lower()}/source_id={source}/batch_id=b00', contractVersion=2, processorRevision=2,
            identity=dict(sourceId=source,sourceRevision=1,batchId='b00',inputFormat=fmt,
                          registeredProfileSha256=sha256(canonical_bytes(manifest['profile'])).hexdigest()),fingerprint=result['batch_fingerprint']))
    work = base / 'service'
    work.mkdir()
    root, result = reference(work)
    roots['service-refs'] = str(root)
    selections.append(dict(kind='SERVICE_REFERENCE',productId='application-reliability',storageBindingId='service-refs',
        relativeAddress='reference_id=bench-services/revision=r1',contractVersion=1,processorRevision=1,
        identity=dict(referenceId='bench-services',revision='r1'),fingerprint=result['snapshot_fingerprint']))
    import publisher_reference
    work = base / 'publisher'
    work.mkdir()
    root = work / 'references'
    root.mkdir()
    csv, vocab = work/'source.csv',work/'vocabulary.json'
    csv.write_bytes(b'source_id,publisher_name,publisher_category,declared_language,country_code\nexample-a,Example A,science,en,CA\n')
    vocab.write_bytes(b'{"country_code":["CA","GB","US"],"declared_language":["en","es","fr"],"publisher_category":["business","culture","general-news","science","technology"],"schema_version":1,"version":1}')
    result = publisher_reference.publish(root,csv,vocab,'fictional-publishers','r1','example-team')
    roots['publisher-refs'] = str(root)
    selections.append(dict(kind='PUBLISHER_REFERENCE',productId='content-intelligence',storageBindingId='publisher-refs',
        relativeAddress='reference_id=fictional-publishers/revision=r1',contractVersion=1,processorRevision=1,
        identity=dict(referenceId='fictional-publishers',revision='r1'),fingerprint=result['snapshot_fingerprint']))
    return dict(artifacts=selections,roots=roots)


if __name__ == '__main__':
    sys.stdout.buffer.write(canonical_bytes(create(sys.argv[1])))
