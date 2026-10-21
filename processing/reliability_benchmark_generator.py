"""Indexed synthetic generator v1. Preparation is separate from pipeline timing."""
from hashlib import sha256
from pathlib import Path
import json
import time
import uuid
from publication_metadata import canonical_bytes
import reliability_format_metadata as m
from reliability_format_parquet import connection

SEED=20261019
METHODS=('GET','HEAD','POST','PUT','PATCH','DELETE','OPTIONS')
ENVIRONMENTS=('dev','test','production')


def config(n=100000,variant='base'):
    m.integer(n,1,1000000,'ARGUMENT_INVALID');m.need(variant in ('base','hot','equal'),'ARGUMENT_INVALID')
    return dict(generator_version=1,seed=SEED,unique_event_count=n,variant=variant,
        batch_policy='two-receipts-v1',file_policy='eight-per-batch-v1',logical_schema='typed-ten-v1',
        reference_fixture='services-v1',field_policy='sha256-mixed-quota-v1',identity_policy='sha256-uuid8-v1')


def digest(value):return sha256(canonical_bytes(value)).hexdigest()


def h(i,field):return int.from_bytes(sha256(f'{SEED}/{i}/{field}'.encode()).digest()[:8],'big')


def event_id(i,domain='event'):
    m.need(domain in ('event','invalid'),'ARGUMENT_INVALID')
    raw=bytearray(sha256(f'ddip-reliability-generator-v1/{SEED}/{domain}/{i}'.encode()).digest()[:16])
    raw[6]=(raw[6]&15)|128;raw[8]=(raw[8]&63)|128
    return str(uuid.UUID(bytes=bytes(raw)))


def event(i,variant='base'):
    m.integer(i,0,1010000,'ARGUMENT_INVALID')
    m.need(variant in ('base','hot','equal'),'ARGUMENT_INVALID')
    service=0 if i%10<6 else 1+(4*(i//10)+(i%10-6))%9
    instant=1790812800000000+h(i,'event_time')%86400000000
    if variant=='hot':
        service=0 if i%10<9 else 1+(i//10)%9
        instant=1790856000000000+h(i,'event_time')%300000000
    service=f'svc{service:02d}'
    band=h(i,'status_band')%100
    statuses=(200,201,204) if band<90 else (400,404,429) if band<95 else (500,503,599)
    band=h(i,'duration_band')%100;value=h(i,'duration_value')
    duration=1+value%100 if band<80 else 101+value%900 if band<99 else 1001+value%59000
    if variant=='equal':duration=17
    return dict(schema_version=1,event_id=event_id(i),service_id=service,
        environment=ENVIRONMENTS[h(i,'environment')%3],event_time_epoch_us=instant,
        method=METHODS[h(i,'method')%7],route_id=f'{service}-r{h(i,"route")%20:02d}',
        status_code=statuses[h(i,'status_code')%3],duration_ms=duration,is_synthetic=True)


def batch_records(n,batch,variant='base'):
    m.integer(n,1,1000000,'ARGUMENT_INVALID');m.need(batch in ('b00','b01'),'ARGUMENT_INVALID')
    for i in range(n):
        if (i%100<90)==(batch=='b00'):yield event(i,variant)
    if batch=='b01':
        for k in range(n//50):yield event(50*k+k%50,variant)
        for j in range(n//100):
            row=event(n+j,variant);row['event_id']=event_id(j,'invalid')
            field,value=(('event_time_epoch_us',None),('status_code',600),('duration_ms',-1),('environment','invalid'),('event_id',None))[j%5]
            row[field]=value;yield row


def wire(row):
    value=dict(row);instant=value.pop('event_time_epoch_us')
    value['event_time']=m.timestamp(instant) if instant is not None else None
    return canonical_bytes(value)+b'\n'


def write_parquet(path,records):
    """One bounded file at a time; integer timestamp construction and stable order."""
    m.need(not Path(path).exists(),'BATCH_CONFLICT')
    fields=('schema_version INTEGER,event_id VARCHAR,service_id VARCHAR,environment VARCHAR,'
        'event_time_epoch_us BIGINT,method VARCHAR,route_id VARCHAR,status_code INTEGER,'
        'duration_ms BIGINT,is_synthetic BOOLEAN')
    with connection() as db:
        db.execute('CREATE TABLE seed(position BIGINT,'+fields+')')
        chunk=[];position=0
        for row in records:
            values=(position,*[row[n] for n in m.TRANSPORT_FIELDS]);position+=1;chunk.append(values)
            if len(chunk)==1024:
                m.need(len(canonical_bytes(chunk))<=m.CHUNK_BYTES,'RESOURCE_LIMIT')
                db.executemany('INSERT INTO seed VALUES (?,?,?,?,?,?,?,?,?,?,?)',chunk);chunk=[]
        if chunk:db.executemany('INSERT INTO seed VALUES (?,?,?,?,?,?,?,?,?,?,?)',chunk)
        projection=','.join('make_timestamptz(event_time_epoch_us) AS event_time' if n=='event_time' else '"'+n+'"' for n in m.EVENT_FIELDS)
        db.execute('COPY (SELECT '+projection+' FROM seed ORDER BY position) TO ? (FORMAT PARQUET,COMPRESSION SNAPPY,ROW_GROUP_SIZE 8192)',[str(path)])


def profile(input_format,generator_config):
    return dict(profile_id='request-jsonl-v2' if input_format=='JSONL' else 'request-parquet-v1',
        schema_version=1,config_revision=1,origin='synthetic',synthetic_source_id='bench-source',
        logical_dataset_id=digest(generator_config),allowed_service_ids=[f'svc{i:02d}' for i in range(10)],
        allowed_route_ids=[f'svc{i:02d}-r{j:02d}' for i in range(10) for j in range(20)],
        policy_revision=2,decoder=m.DECODERS[input_format])


def prepare(root,generator_config):
    """Bounded E3 correctness preparation; large E4a datasets remain disabled."""
    n=generator_config['unique_event_count']
    m.need(generator_config==config(n,generator_config['variant']),'ARGUMENT_INVALID')
    m.need(n<=4096,'PREFLIGHT_BLOCKED')
    root=m.root_path(root);observations=[]
    # IDs, not event history: at most 4,136 reviewed UUIDs under the E3 cap.
    seen=set()
    for domain,count in (('event',n),('invalid',n//100)):
        for i in range(count):
            identity=event_id(i,domain);m.match(identity,m.UUID,'BENCHMARK_INVALID')
            m.need(identity not in seen,'BENCHMARK_INVALID');seen.add(identity)
    for fmt in ('JSONL','PARQUET'):
        destination=root/fmt.lower();destination.mkdir()
        for batch in ('b00','b01'):
            incoming=destination/batch;incoming.mkdir()
            # Count algebraically, without materializing a history or rescanning per file.
            b00=(n//100)*90+min(n%100,90)
            count=b00 if batch=='b00' else n-b00+n//50+n//100
            records=iter(batch_records(n,batch,generator_config['variant']))
            cuts=[count*f//8 for f in range(9)]
            for f in range(8):
                path=incoming/f'part-{f:02d}.{fmt.lower()}';length=cuts[f+1]-cuts[f]
                start=time.perf_counter_ns()
                def selected():
                    for _ in range(length):yield next(records)
                if fmt=='JSONL':
                    with path.open('xb') as stream:
                        for row in selected():
                            raw=wire(row);m.need(len(raw)<=320,'RESOURCE_LIMIT')
                            m.need(stream.write(raw)==len(raw),'PUBLICATION_FAILED')
                else:
                    m.need(not path.exists(),'BATCH_CONFLICT');write_parquet(path,selected())
                observations.append(dict(input_format=fmt,batch_id=batch,artifact_id=f'part-{f:02d}',
                    record_count=length,byte_length=path.stat().st_size,preparation_ns=time.perf_counter_ns()-start))
            m.need(next(records,None) is None,'BENCHMARK_INVALID')
    return observations


def main(argv=None):
    import sys
    try:
        values=m.arguments(sys.argv[1:] if argv is None else argv,{'config','root','tier'})
        if values is None:
            print('Synthetic correctness generator: --config CANONICAL_GENERATOR_JSON --root EXISTING_EMPTY_ROOT --tier correctness (1..4096 events). E4a 100K/1M preparation is deferred.');return 0
        m.need(values['tier'] in ('correctness','pilot'),'PREFLIGHT_BLOCKED')
        source=m.observe(Path(values['config']),m.PROFILE_LIMIT,'INPUT_UNAVAILABLE','INPUT_CHANGED',retain=True)
        selected=m.parse(source.raw,m.PROFILE_LIMIT,'ARGUMENT_INVALID')
        if values['tier']=='pilot':m.need(selected['unique_event_count']==4096,'ARGUMENT_INVALID')
        result=prepare(values['root'],selected)
        m.observe(source.path,m.PROFILE_LIMIT,'INPUT_UNAVAILABLE','INPUT_CHANGED',source)
        print(canonical_bytes(dict(status='PREPARED',origin='synthetic',label=m.LABEL,files=m.public(result))).decode());return 0
    except m.ReliabilityFailure as e:return m.cli_error(e)
    except Exception:return m.cli_error(m.ReliabilityFailure('BENCHMARK_INVALID'))


if __name__=='__main__':raise SystemExit(main())
