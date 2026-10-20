"""Fictional literal E2 acceptance inputs, independent of processing expressions."""
from pathlib import Path
import json
from publication_metadata import canonical_bytes

RECEIVED='2026-10-20T12:10:00.000000Z'
REFERENCE=(b'service_id,service_name,owner_team,service_tier\n'
           b'alpha,Alpha Service,example-platform,critical\nbeta,Beta Service,,\n'
           b'delta,Delta Service,example-service,standard\n')
VOCABULARY=b'{"owner_team":["example-platform","example-service"],"schema_version":1,"service_tier":["critical","experimental","standard"],"version":1}'


def profile():
    return dict(profile_id='request-jsonl-v1',schema_version=1,config_revision=1,origin='synthetic',
                synthetic_source_id='fixture-source',logical_dataset_id=None,
                allowed_service_ids=['alpha','beta','gamma'],allowed_route_ids=['health'],policy_revision=1)


def event(n,**changes):
    result=dict(schema_version=1,event_id=f'00000000-0000-0000-0000-{n:012d}',service_id='alpha',
                environment='test',event_time='2026-10-20T12:00:00.000000Z',method='GET',route_id='health',
                status_code=500 if n==1 else 503 if n==2 else 200,duration_ms=n,is_synthetic=True)
    return result|changes


def f1(): return [canonical_bytes(event(n)) for n in range(1,21)]


def f2():
    return f1()+[canonical_bytes(event(1)),
        canonical_bytes(event(1,service_id='beta',event_time='2026-10-20T12:05:00.000000Z',duration_ms=40,status_code=404)),
        canonical_bytes(event(21,service_id='gamma',event_time='2026-10-20T12:04:59.999999Z',duration_ms=0,status_code=599)),
        canonical_bytes(event(22,event_time='1969-12-31T23:59:59.999999Z',duration_ms=3600000,status_code=200)),
        b'{"service_id":"alpha","event_id":"00000000-0000-0000-0000-000000000001",',b'{',
        canonical_bytes(event(23,duration_ms=1))[:-1]+b',"status_code":500}',
        canonical_bytes(event(24,duration_ms=1)).replace(b'"duration_ms":1',b'"duration_ms":NaN'),
        canonical_bytes(event(25,duration_ms=-1)),
        canonical_bytes(event(26,event_time='2026-10-20T12:00:00.0000001Z',duration_ms=1)),
        canonical_bytes(event(27,event_time=None,duration_ms=1)),
        canonical_bytes(event(28,environment='invalid',duration_ms=1))]


def raw_file(root,rows=(),name='events',ending=b'\n'):
    path=Path(root)/(name+'.jsonl')
    path.write_bytes(ending.join(rows)+(ending if rows else b''))
    return path


def register_fixture(root,rows=None,batch_id='b1'):
    import reliability_inputs as inputs
    root=Path(root)
    incoming=root/'incoming'; batches=root/'batches'
    incoming.mkdir(exist_ok=True); batches.mkdir(exist_ok=True)
    raw_file(incoming,f2() if rows is None else rows)
    summary=inputs.register(batches,input_root=incoming,source_id='fixture-source',batch_id=batch_id,
        files=[dict(artifact_id='events',input_file='events.jsonl')],received_at=RECEIVED,profile=profile())
    return batches,summary


def reference_fixture(root, *, empty=False, revision='r1', correction=False):
    import service_reference as reference
    root=Path(root); refs=root/'references'; refs.mkdir(exist_ok=True)
    csv=root/'service.csv'; vocab=root/'vocabulary.json'
    csv.write_bytes(REFERENCE.splitlines(keepends=True)[0] if empty else REFERENCE.replace(b'example-platform',b'example-service') if correction else REFERENCE)
    vocab.write_bytes(VOCABULARY)
    summary=reference.publish(refs,csv,vocab,'fixture-services',revision,'example-team')
    return refs,summary
