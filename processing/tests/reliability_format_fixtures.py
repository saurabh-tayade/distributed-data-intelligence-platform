"""Hand-authored ADR 0009 G1; no generator-derived expected values."""
from pathlib import Path
from publication_metadata import canonical_bytes


def g1():
    a=dict(schema_version=1,event_id='00000000-0000-0000-0000-000000000001',service_id='svc00',
        environment='test',event_time='2026-10-01T00:00:00.000000Z',method='GET',route_id='svc00-r00',
        status_code=200,duration_ms=10,is_synthetic=True)
    b=a|dict(event_id='00000000-0000-0000-0000-000000000002',event_time='2026-10-01T00:00:00.000001Z',status_code=500,duration_ms=20)
    c=a|dict(service_id='svc01',event_time='2026-10-01T00:05:00.000000Z',method='HEAD',route_id='svc01-r00',status_code=404,duration_ms=7)
    d=a|dict(service_id='svc09',environment='production',event_time='1969-12-31T23:59:59.999999Z',route_id='svc09-r00',status_code=503,duration_ms=0)
    bad=[]
    for j,(name,value) in enumerate((('event_time',None),('status_code',600),('duration_ms',-1),('environment','invalid'),('event_id',None))):
        bad.append(a|dict(event_id=f'00000000-0000-0000-0000-{101+j:012d}')|{name:value})
    return [a,b,c,d,a,*bad]


def policy(fmt):
    import reliability_format_metadata as m
    return dict(profile_id='request-jsonl-v2' if fmt=='JSONL' else 'request-parquet-v1',schema_version=1,
        config_revision=1,origin='synthetic',synthetic_source_id='bench-source',logical_dataset_id=None,
        allowed_service_ids=['svc00','svc01','svc09'],allowed_route_ids=['svc00-r00','svc01-r00','svc09-r00'],
        policy_revision=2,decoder=m.DECODERS[fmt])


def register(root,fmt,rows=None,*,batch_id='b00',received_at='2026-10-02T00:05:00.000000Z'):
    import reliability_format_inputs as inputs
    import reliability_format_metadata as m
    from reliability_benchmark_generator import write_parquet
    root=Path(root);incoming=root/('incoming-'+fmt+('' if batch_id=='b00' else '-'+batch_id));incoming.mkdir()
    batches=root/'batches';batches.mkdir(exist_ok=True)
    rows=g1() if rows is None else rows
    path=incoming/('events.'+fmt.lower())
    if fmt=='JSONL':path.write_bytes(b''.join(canonical_bytes(r)+b'\n' for r in rows))
    else:
        def transport():
            for row in rows:
                r=dict(row);t=r.pop('event_time');r['event_time_epoch_us']=m.time_us(t) if t else None;yield r
        write_parquet(path,transport())
    result=inputs.register(batches,input_root=incoming,source_id='bench-source',batch_id=batch_id,input_format=fmt,
        files=[dict(artifact_id='events',input_file=path.name)],received_at=received_at,profile=policy(fmt))
    return batches,result


def reference(root,*,empty=False,revision='r1',correction=False):
    import service_reference
    root=Path(root);refs=root/'references';refs.mkdir(exist_ok=True)
    csv=root/'services.csv';vocab=root/'vocabulary.json'
    csv.write_bytes(b'service_id,service_name,owner_team,service_tier\nsvc00,Service 00,example-platform,critical\nsvc01,Service 01,,\nsvc-unused,Unused Service,example-service,experimental\n')
    if empty:csv.write_bytes(csv.read_bytes().splitlines(keepends=True)[0])
    if correction:csv.write_bytes(csv.read_bytes().replace(b'example-platform',b'example-service'))
    vocab.write_bytes(canonical_bytes(service_reference.VOCABULARY))
    result=service_reference.publish(refs,csv,vocab,'bench-services',revision,'example-team')
    return refs,result


def completed(root,fmt='JSONL'):
    """Literal G1 completed verifier fixture; no Spark or producer transformations."""
    from hashlib import sha256
    import reliability_format_metadata as m
    import reliability_format_inputs as inputs
    from reliability_format_parquet import connection
    root=Path(root);batches,b=register(root,fmt);refs,r=reference(root)
    deps=inputs.verify_dependencies(batch_root=batches,input_format=fmt,batches=[('bench-source','b00',b['batch_fingerprint'])],
        reference_root=refs,reference_id='bench-services',revision='r1',reference_fingerprint=r['snapshot_fingerprint'])
    output=root/'output';output.mkdir();directory=m.publication_directory(output,deps.fingerprint);directory.mkdir(parents=True)
    m.write(directory/'inputs.json',deps.manifest_bytes)
    received=1790899500000000
    business=[(1,'00000000-0000-0000-0000-000000000001','svc00','test',1790812800000000,'GET','svc00-r00',200,10,True),
        (1,'00000000-0000-0000-0000-000000000002','svc00','test',1790812800000001,'GET','svc00-r00',500,20,True),
        (1,'00000000-0000-0000-0000-000000000001','svc01','test',1790813100000000,'HEAD','svc01-r00',404,7,True),
        (1,'00000000-0000-0000-0000-000000000001','svc09','production',-1,'GET','svc09-r00',503,0,True)]
    def context(service):
        fields=('Service 00','example-platform','critical') if service=='svc00' else ('Service 01',None,None) if service=='svc01' else (None,None,None)
        return (*fields,'MISSING' if service=='svc09' else 'MATCHED','bench-services','r1',r['snapshot_fingerprint'])
    events=[(1,*v,2 if i==0 else 1,received,received,*context(v[2])) for i,v in enumerate(business)]
    file=deps.batches[0].manifest['files'][0];occurrences=[];offset=0
    for i,row in enumerate(g1()):
        raw=canonical_bytes(row)+b'\n';valid=i<5;v=business[i if i<4 else 0] if valid else None
        service='svc00' if i<9 else None;identity=f'00000000-0000-0000-0000-{96+i:012d}' if i>=5 and i<9 else None
        if valid:service,identity=v[2],v[1]
        reason=[] if valid else [['FIELD_NULL'],['FIELD_VALUE'],['FIELD_VALUE'],['FIELD_VALUE'],['FIELD_NULL']][i-5]
        occurrences.append((2,b['batch_fingerprint'],'bench-source','b00',b['ingestion_id'],'events',file['sha256'],fmt,i,
            i+1 if fmt=='JSONL' else None,offset if fmt=='JSONL' else None,len(raw) if fmt=='JSONL' else None,
            0 if fmt=='PARQUET' else None,i if fmt=='PARQUET' else None,received,
            'ACCEPTED' if i<4 else 'DUPLICATE' if i==4 else 'REJECTED',service,identity,
            sha256(canonical_bytes(v)).hexdigest() if valid else None,reason));offset+=len(raw)
    metrics=[(1,'svc00','test',1790812800000000,2,1,0.5,30,15.0,20,*context('svc00')),
        (1,'svc01','test',1790813100000000,1,0,0.0,7,7.0,7,*context('svc01')),
        (1,'svc09','production',-300000000,1,1,1.0,0,0.0,0,*context('svc09'))]
    types={'integer':'INTEGER','long':'BIGINT','timestamp':'BIGINT','string':'VARCHAR','boolean':'BOOLEAN',
        'double':'DOUBLE','decimal(38,0)':'DECIMAL(38,0)','array<string>':'VARCHAR[]'}
    with connection() as db:
        for table,rows in ((m.EVENTS,events),(m.OCCURRENCES,occurrences),(m.METRICS,metrics)):
            fields=m.SCHEMAS[table];db.execute('CREATE TABLE seed('+','.join('"'+n+'" '+types[k] for n,k in fields)+')')
            db.executemany('INSERT INTO seed VALUES ('+','.join('?' for _ in fields)+')',rows)
            projection=','.join('make_timestamptz("'+n+'") AS "'+n+'"' if k=='timestamp' else '"'+n+'"' for n,k in fields)
            (directory/table).mkdir();db.execute('COPY (SELECT '+projection+' FROM seed) TO ? (FORMAT PARQUET,COMPRESSION SNAPPY)',[str(directory/table/'part.parquet')]);db.execute('DROP TABLE seed')
    evidence=m.capture(directory,{'inputs.json':m.MANIFEST_LIMIT},m.TABLES,'PUBLICATION_INVALID','PUBLICATION_CHANGED')
    marker=dict(completion_version=2,product_id=m.PRODUCT,product_fingerprint=deps.fingerprint,manifest_byte_length=len(deps.manifest_bytes),
        occurrence_counts=dict(physical_occurrences=10,valid_occurrences=5,unique_valid_events=4,identical_duplicate_occurrences=1,rejected_occurrences=5),
        output_counts={m.EVENTS:4,m.OCCURRENCES:10,m.METRICS:3},files=evidence.inventory(),completed_at=m.now())
    m.write(directory/'complete.json',canonical_bytes(marker))
    return output,deps.fingerprint,batches,refs


def benchmark_reference(root):
    """Reviewed ten-row reference for the bounded generator correctness fixture."""
    import service_reference
    root=Path(root);refs=root/'references';refs.mkdir()
    csv=root/'services.csv';vocabulary=root/'vocabulary.json'
    lines=['service_id,service_name,owner_team,service_tier']
    for i in range(9):
        owner='' if i==1 else 'example-platform' if i%2==0 else 'example-service'
        tier='' if i==2 else 'critical' if i==0 else 'standard'
        lines.append(f'svc{i:02d},Service {i:02d},{owner},{tier}')
    lines.append('svc-unused,Unused Service,example-service,experimental')
    csv.write_text('\n'.join(lines)+'\n',encoding='utf-8');vocabulary.write_bytes(canonical_bytes(service_reference.VOCABULARY))
    result=service_reference.publish(refs,csv,vocabulary,'bench-services','r1','example-team')
    return refs,result
