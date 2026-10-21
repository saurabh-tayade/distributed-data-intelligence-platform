"""Independent disk-backed business oracle. No producer, generator or Spark imports.

DuckDB is a shared Parquet codec, not an independent codec implementation.
All identity, rejection, calendar, arithmetic and rank rules below are independent.
"""
from bisect import bisect_right
from datetime import datetime,timezone
from hashlib import sha256
from pathlib import Path
import csv
import json
import math
import os
import re
import sqlite3
import sys
import time
import uuid
import duckdb
from publication_metadata import canonical_bytes
import reliability_format_metadata as m

NAMES=('schema_version','event_id','service_id','environment','event_time','method','route_id','status_code','duration_ms','is_synthetic')
ORDERED_REASONS=('BLANK_LINE','JSON_SYNTAX','JSON_NOT_OBJECT','JSON_DUPLICATE_KEY','JSON_NONFINITE','FIELD_MISSING','FIELD_NULL','FIELD_TYPE','FIELD_VALUE','TIMESTAMP_INVALID')
UUID=re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
IDENTIFIER=re.compile(r'[a-z0-9][a-z0-9_-]{0,127}')
ROUTE=re.compile(r'[a-z0-9][a-z0-9_-]{0,63}')
TIME=re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z')


def instant(value):
    if type(value) is not str or TIME.fullmatch(value) is None:raise ValueError()
    t=datetime.strptime(value,'%Y-%m-%dT%H:%M:%S.%fZ').replace(tzinfo=timezone.utc)
    delta=t-datetime(1970,1,1,tzinfo=timezone.utc)
    return (delta.days*86400+delta.seconds)*1000000+delta.microseconds


class Pairs(list):pass
class Fraction:pass
class Nonfinite:pass


def typed(value,policy,*,text=False):
    """Return safe identity, full validated tuple, or ordered fixed reasons."""
    if text:
        if not value.strip():return None,None,None,['BLANK_LINE']
        # Independent lexical allocation policy: four levels, 32 members, 64-byte numbers.
        depth=members=i=0;quoted=escaped=False
        while i<len(value):
            c=value[i]
            if quoted:
                if escaped:escaped=False
                elif c=='\\':escaped=True
                elif c=='"':quoted=False
            elif c=='"':quoted=True
            elif c in '{[':depth+=1;m.need(depth<=4,'BATCH_INVALID')
            elif c in '}]':depth-=1
            elif c==':':members+=1;m.need(members<=32,'BATCH_INVALID')
            elif c in '-0123456789':
                number=re.match(r'-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?',value[i:])
                if number:m.need(len(number[0])<=64,'BATCH_INVALID');i+=len(number[0])-1
            i+=1
        try:value=json.loads(value,object_pairs_hook=Pairs,parse_float=lambda _:Fraction(),parse_constant=lambda _:Nonfinite())
        except (ValueError,RecursionError):return None,None,None,['JSON_SYNTAX']
    def walk(item):
        if isinstance(item,Pairs):privacy(item)
        elif type(item) is dict:privacy(item.items())
        elif type(item) is list:
            for child in item:walk(child)
    def privacy(items):
        for key,item in items:
            m.need(key in NAMES,'INTAKE_NOT_APPROVED')
            if key in ('service_id','route_id') and type(item) is str:
                m.need(item in policy['allowed_service_ids' if key=='service_id' else 'allowed_route_ids'],'INTAKE_NOT_APPROVED')
            if key=='is_synthetic':m.need(item is not False and item not in ('false','operational','real','production'),'INTAKE_NOT_APPROVED')
            walk(item)
    walk(value)
    if text and not isinstance(value,Pairs):return None,None,None,['JSON_NOT_OBJECT']
    pairs=value if text else list(value.items())
    fields={};reasons=set()
    for name,item in pairs:
        m.need(name in NAMES,'INTAKE_NOT_APPROVED')
        if name in ('service_id','route_id') and type(item) is str:
            m.need(item in policy['allowed_service_ids' if name=='service_id' else 'allowed_route_ids'],'INTAKE_NOT_APPROVED')
        if name=='is_synthetic':m.need(item is not False and item not in ('false','operational','real','production'),'INTAKE_NOT_APPROVED')
        if name=='schema_version' and type(item) is int:m.need(item==1,'BATCH_INVALID')
        if name in fields:
            m.need(name not in ('service_id','event_id'),'BATCH_INVALID');reasons.add('JSON_DUPLICATE_KEY')
        fields.setdefault(name,[]).append(item)
    data={k:v[0] for k,v in fields.items() if len(v)==1}
    service,event=data.get('service_id'),data.get('event_id')
    recognized=type(service) is str and IDENTIFIER.fullmatch(service) and type(event) is str and UUID.fullmatch(event)
    if not recognized:service=event=None
    output=[]
    for name in NAMES:
        if name not in fields:reasons.add('FIELD_MISSING');continue
        if len(fields[name])!=1:continue
        item=data[name]
        if item is None:reasons.add('FIELD_NULL');continue
        if isinstance(item,Nonfinite):reasons.add('JSON_NONFINITE');continue
        kind=bool if name=='is_synthetic' else int if name in ('schema_version','status_code','duration_ms') or (name=='event_time' and not text) else str
        if type(item) is not kind:reasons.add('FIELD_TYPE');continue
        if name=='event_time':
            try:item=instant(item) if text else item
            except ValueError:reasons.add('TIMESTAMP_INVALID');continue
            if not -62135596800000000<=item<253402300800000000:reasons.add('TIMESTAMP_INVALID');continue
        valid=(name!='event_id' or UUID.fullmatch(item)) and (name!='service_id' or IDENTIFIER.fullmatch(item))
        valid=valid and (name!='route_id' or ROUTE.fullmatch(item)) and (name!='environment' or item in ('dev','test','production'))
        valid=valid and (name!='method' or item in ('GET','HEAD','POST','PUT','PATCH','DELETE','OPTIONS'))
        valid=valid and (name!='status_code' or 100<=item<=599) and (name!='duration_ms' or 0<=item<=3600000)
        valid=valid and (name!='is_synthetic' or item is True)
        if not valid:reasons.add('FIELD_VALUE')
        output.append(item)
    return service,event,None if reasons else tuple(output),[r for r in ORDERED_REASONS if r in reasons]


def codec():
    m.need(duckdb.__version__=='1.4.5','UNSUPPORTED_VERSION')
    c=duckdb.connect(config=dict(threads=2,memory_limit='512MiB',temp_directory='',
        autoinstall_known_extensions=False,autoload_known_extensions=False,allow_unsigned_extensions=False))
    c.execute("SET TimeZone='UTC'");return c


def records(path,fmt,policy,*,result=None):
    """Independent physical re-indexing; bounded chunks and original source ordinals."""
    if fmt=='JSONL':
        offset=0;number=0
        with path.open('rb') as stream:
            while raw:=stream.readline(16387):
                number+=1;body=raw[:-2] if raw.endswith(b'\r\n') else raw[:-1] if raw.endswith(b'\n') else raw
                m.need(len(body)<=16384,'RESOURCE_LIMIT');m.need(b'\r' not in body and not(number==1 and body.startswith(b'\xef\xbb\xbf')),'BATCH_INVALID')
                try:parsed=typed(body.decode('utf-8'),policy,text=True)
                except UnicodeError:raise m.ReliabilityFailure('BATCH_INVALID') from None
                yield (number-1,number,offset,len(raw),None,None),parsed
                offset+=len(raw)
        if result is not None:result.update(record_count=number,physical_line_count=number,row_groups=None)
    else:
        with codec() as db:
            with path.open('rb') as source:
                m.need(source.read(4)==b'PAR1','PARQUET_INVALID');source.seek(-8,2);tail=source.read(8)
                m.need(tail[4:]==b'PAR1' and 0<int.from_bytes(tail[:4],'little')<=1048576,'PARQUET_INVALID')
            schema=db.execute('SELECT name,type,repetition_type,logical_type,converted_type,field_id FROM parquet_schema(?)',[str(path)]).fetchmany(12)
            m.need(tuple(r[0] for r in schema[1:])==NAMES and len(schema)==11,'PARQUET_INVALID')
            m.need(tuple(r[1] for r in schema[1:])==('INT32','BYTE_ARRAY','BYTE_ARRAY','BYTE_ARRAY','INT64','BYTE_ARRAY','BYTE_ARRAY','INT32','INT64','BOOLEAN'),'PARQUET_INVALID')
            m.need(all(r[2]=='OPTIONAL' for r in schema[1:]) and 'isAdjustedToUTC=1' in schema[5][3] and 'MICROS=MicroSeconds()' in schema[5][3],'PARQUET_INVALID')
            m.need(all(r[5] is None for r in schema[1:]),'INTAKE_NOT_APPROVED')
            for i in (2,3,4,6,7):
                m.need(schema[i][4] in (None,'UTF8') and schema[i][3] in (None,'StringType()') and
                    (schema[i][4]=='UTF8' or schema[i][3]=='StringType()'),'PARQUET_INVALID')
            for i,bits in ((1,32),(8,32),(9,64)):
                m.need(schema[i][4] in (None,'INT_'+str(bits)) and (schema[i][3] is None or ('bitWidth='+str(bits) in schema[i][3] and 'isSigned=1' in schema[i][3])),'PARQUET_INVALID')
            m.need(schema[5][4] in (None,'TIMESTAMP_MICROS') and schema[10][3] is None and schema[10][4] is None,'PARQUET_INVALID')
            meta=db.execute('SELECT created_by,num_rows,num_row_groups,encryption_algorithm,footer_signing_key_metadata FROM parquet_file_metadata(?)',[str(path)]).fetchone()
            m.need(meta[0]=='DuckDB version v1.4.5 (build f31be57c18)' and meta[3:] == (None,None),'PARQUET_INVALID')
            pairs=db.execute('SELECT key,value FROM parquet_kv_metadata(?)',[str(path)]).fetchmany(3)
            m.need(len(pairs)<=2 and len({k for k,v in pairs})==len(pairs),'INTAKE_NOT_APPROVED')
            for k,v in pairs:
                m.need((k==b'ddip.logical_schema' and v==b'typed-ten-v1') or
                    (k==b'ddip.generator_config_sha256' and re.fullmatch(b'[0-9a-f]{64}',v)),'INTAKE_NOT_APPROVED')
            groups=db.execute('SELECT row_group_id,min(row_group_num_rows),sum(total_uncompressed_size),count(*),count(*) FILTER(WHERE compression<>\'SNAPPY\' OR num_values<>row_group_num_rows),sum(total_compressed_size) FROM parquet_metadata(?) GROUP BY row_group_id ORDER BY row_group_id',[str(path)]).fetchmany(65)
            m.need(len(groups)<=64,'RESOURCE_LIMIT');starts=[];total=0;details=[]
            for ordinal,n,size,columns,invalid,compressed in groups:
                m.need(ordinal==len(starts) and columns==10 and invalid==0,'PARQUET_INVALID')
                m.need(n<=32768 and size<=33554432,'RESOURCE_LIMIT')
                details.append(dict(row_group_ordinal=ordinal,record_start=total,record_count=n,compressed_byte_length=compressed,uncompressed_byte_length=size))
                starts.append(total);total+=n
            m.need(meta[1]==total and meta[2]==len(groups),'PARQUET_INVALID')
            expression='+'.join('octet_length(encode(coalesce("'+n+'",\'\')))' for n in ('event_id','service_id','environment','method','route_id'))
            maximum=db.execute('SELECT coalesce(max('+expression+'),0) FROM read_parquet(?)',[str(path)]).fetchone()[0]
            m.need(maximum<=16384,'RESOURCE_LIMIT');fetch=min(1024,max(1,2097152//(6*maximum+1024)))
            projection=','.join('epoch_us(event_time)' if n=='event_time' else '"'+n+'"' for n in NAMES)
            db.execute('SELECT file_row_number,'+projection+',event_time IS NULL FROM read_parquet(?,file_row_number=true,hive_partitioning=false) ORDER BY file_row_number',[str(path)])
            seen=0
            while chunk:=db.fetchmany(fetch):
                m.need(len(canonical_bytes(chunk))<=2097152,'RESOURCE_LIMIT')
                for p,*values in chunk:
                    time_null=values.pop()
                    if values[4] is None and not time_null:values[4]=253402300800000000
                    m.need(p==seen,'PROVENANCE_UNSUPPORTED');seen+=1;g=bisect_right(starts,p)-1
                    m.need(g>=0 and p<starts[g]+groups[g][1],'PROVENANCE_UNSUPPORTED')
                    yield (p,None,None,None,g,p-starts[g]),typed(dict(zip(NAMES,values,strict=True)),policy)
            m.need(seen==total,'PARQUET_INVALID')
        if result is not None:result.update(record_count=seen,physical_line_count=None,row_groups=details)


def qualified(candidate,n,below,at_or_below,present):
    rank=(95*n+99)//100;budget=max(1,(n+9999)//10000)
    displacement=max(below+1-rank,rank-at_or_below,0)
    return bool(present) and displacement<=budget,displacement,budget


def validate_batch(batch,selection,fmt,evidence):
    """Separate metadata validation; declared row counts never replace raw indexing."""
    code='DEPENDENCY_INVALID'
    m.keys(batch,('manifest_version','batch_contract_version','request_schema_version','registrar_revision',
        'input_format','source_id','batch_id','ingestion_id','received_at','profile','files'),code)
    for key,value in dict(manifest_version=2,batch_contract_version=2,request_schema_version=1,registrar_revision=2).items():m.integer(batch[key],value,value,code)
    m.need(batch['input_format']==fmt and all(batch[k]==selection[k] for k in ('source_id','batch_id')),code)
    expected=str(uuid.uuid5(uuid.NAMESPACE_URL,f"ddip:reliability:batch-v2:{fmt}:{batch['source_id']}:{batch['batch_id']}"))
    m.need(batch['ingestion_id']==expected,code);received=instant(batch['received_at'])
    policy=batch['profile'];m.keys(policy,('profile_id','schema_version','config_revision','origin','synthetic_source_id',
        'logical_dataset_id','allowed_service_ids','allowed_route_ids','policy_revision','decoder'),code)
    m.need(policy['origin']=='synthetic' and policy['synthetic_source_id']==batch['source_id'] and
        policy['profile_id']==('request-jsonl-v2' if fmt=='JSONL' else 'request-parquet-v1') and
        canonical_bytes(policy['decoder'])==canonical_bytes(m.DECODERS[fmt]),code)
    for k,v in dict(schema_version=1,config_revision=1,policy_revision=2).items():m.integer(policy[k],v,v,code)
    if policy['logical_dataset_id'] is not None:m.match(policy['logical_dataset_id'],code=code)
    for k,pattern,cap in (('allowed_service_ids',IDENTIFIER,128),('allowed_route_ids',ROUTE,256)):
        values=policy[k];m.need(type(values) is list and 1<=len(values)<=cap,code)
        for value in values:m.match(value,pattern,code)
        m.need(values==sorted(set(values)),code)
    m.need(len(canonical_bytes(policy))<=65536,'RESOURCE_LIMIT')
    marker=m.parse(evidence.raw('complete.json'),4096,code)
    m.keys(marker,('completion_version','batch_fingerprint','manifest_byte_length','completed_at'),code)
    m.integer(marker['completion_version'],2,2,code)
    m.need(marker['batch_fingerprint']==selection['batch_fingerprint'] and
        type(marker['manifest_byte_length']) is int and marker['manifest_byte_length']==len(evidence.raw('manifest.json')) and
        instant(marker['completed_at'])>=received,code)
    m.need(type(batch['files']) is list and 1<=len(batch['files'])<=16,code)
    items=[];ids=[]
    for f in batch['files']:
        m.keys(f,('artifact_id','file','byte_length','sha256','record_count','physical_line_count','row_groups'),code)
        m.match(f['artifact_id'],m.ID,code);m.match(f['sha256'],code=code)
        m.need(f['file']=='data/'+f['artifact_id']+'.'+fmt.lower(),code)
        m.integer(f['record_count'],0,2000000,code);m.integer(f['byte_length'],0,268435456,code)
        if fmt=='JSONL':m.need(type(f['physical_line_count']) is int and f['physical_line_count']==f['record_count'] and f['row_groups'] is None,code)
        else:
            m.need(f['physical_line_count'] is None and type(f['row_groups']) is list and len(f['row_groups'])<=64,code)
            start=0
            for i,g in enumerate(f['row_groups']):
                m.keys(g,('row_group_ordinal','record_start','record_count','compressed_byte_length','uncompressed_byte_length'),code)
                for v in g.values():m.integer(v,code=code)
                m.need(g['row_group_ordinal']==i and g['record_start']==start and g['record_count']<=32768 and g['uncompressed_byte_length']<=33554432,code)
                start+=g['record_count']
            m.need(start==f['record_count'],code)
        ids.append(f['artifact_id']);items.append(dict(path=f['file'],byte_length=f['byte_length'],sha256=f['sha256']))
    m.need(ids==sorted(set(ids)) and items==evidence.inventory()['data'],code)


def register_outputs(db,evidence):
    """Inspect each physical file before decoding; no schema overrides/coercion."""
    types={'integer':'INTEGER','long':'BIGINT','timestamp':'TIMESTAMP WITH TIME ZONE','string':'VARCHAR',
        'boolean':'BOOLEAN','double':'DOUBLE','decimal(38,0)':'DECIMAL(38,0)','array<string>':'VARCHAR[]'}
    physical={'integer':'INT32','long':'INT64','timestamp':'INT64','string':'BYTE_ARRAY',
        'boolean':'BOOLEAN','double':'DOUBLE','decimal(38,0)':'FIXED_LEN_BYTE_ARRAY'}
    for table,fields in m.SCHEMAS.items():
        for path in evidence.paths(table):
            relation=db.read_parquet(path,hive_partitioning=False,union_by_name=False)
            m.need(tuple(zip(relation.columns,map(str,relation.types)))==tuple((n,types[k]) for n,k in fields))
            cur=db.execute('SELECT * FROM parquet_schema(?)',[path]);names=[x[0] for x in cur.description]
            rows=[dict(zip(names,r)) for r in cur.fetchmany(32)]
            m.need(len(rows)==len(fields)+1+(2 if table==m.OCCURRENCES else 0))
            for n,k in fields:
                row=next((r for r in rows if r['name']==n),None);m.need(row is not None)
                if k=='array<string>':
                    j=rows.index(row);m.need(row['converted_type']=='LIST' and j+2<len(rows) and
                        rows[j+1]['repetition_type']=='REPEATED' and rows[j+2]['name']=='element' and rows[j+2]['converted_type']=='UTF8')
                else:
                    m.need(row['type']==physical[k])
                    if k=='string':m.need(row['converted_type']=='UTF8')
                    elif k=='timestamp':m.need('isAdjustedToUTC=1' in str(row['logical_type']) and 'MICROS=MicroSeconds()' in str(row['logical_type']))
                    elif k=='decimal(38,0)':m.need(row['precision']==38 and row['scale']==0 and str(row['type_length'])=='16')
        db.read_parquet(list(evidence.paths(table)),hive_partitioning=False,union_by_name=False).create_view(table)
        m.need(db.execute('SELECT count(*) FROM '+table).fetchone()[0]<=2000000,'RESOURCE_LIMIT')
        strings=[n for n,k in fields if k=='string']
        expr='+'.join('octet_length(encode(coalesce("'+n+'",\'\')))' for n in strings)
        m.need(db.execute('SELECT coalesce(max('+expr+'),0) FROM '+table).fetchone()[0]<=16384,'RESOURCE_LIMIT')
        if table==m.OCCURRENCES:
            m.need(db.execute('SELECT 1 FROM '+table+' WHERE reason_codes IS NULL OR len(reason_codes)>10 OR len(list_filter(reason_codes,x->x IS NULL OR NOT list_contains(?,x)))>0 LIMIT 1',[list(ORDERED_REASONS)]).fetchone() is None)


class Oracle:
    def __init__(self,scratch_root):
        self.directory=m.root_path(scratch_root)
        m.need(not any(self.directory.iterdir()),'BENCHMARK_INVALID')
        self.previous_environment=os.environ.get('SQLITE_TMPDIR')
        os.environ['SQLITE_TMPDIR']=str(self.directory)
        self.sql=None;self.previous_directory=None
        try:
            self.sql=sqlite3.connect(self.directory/'oracle.sqlite')
            self.previous_directory=self.sql.execute('PRAGMA temp_store_directory').fetchone()
            self.sql.execute('PRAGMA cache_size=-65536');self.sql.execute('PRAGMA mmap_size=0');self.sql.execute('PRAGMA temp_store=FILE')
            if hasattr(self.sql,'enable_load_extension'):self.sql.enable_load_extension(False)
            page_size=self.sql.execute('PRAGMA page_size').fetchone()[0]
            self.sql.execute('PRAGMA max_page_count='+str((2*1024**3)//page_size))
            # Optional deprecated facility is not a portability requirement. Large-run
            # monitoring/attribution belongs to E4a; the E3 main DB remains bounded.
            escaped=str(self.directory).replace("'","''")
            try:self.sql.execute("PRAGMA temp_store_directory='"+escaped+"'")
            except sqlite3.DatabaseError:pass
            self.dependencies=[]
            self.sql.executescript('''CREATE TABLE identities(service TEXT,event TEXT,payload BLOB,PRIMARY KEY(service,event));
                CREATE TABLE events(service TEXT,event TEXT,payload BLOB,deliveries INTEGER,first_us INTEGER,last_us INTEGER,
                    environment TEXT,window INTEGER,duration INTEGER,status INTEGER,PRIMARY KEY(service,event));
                CREATE TABLE raw(position INTEGER PRIMARY KEY,expected BLOB,payload BLOB,rejected BLOB);
                CREATE INDEX durations ON events(service,environment,window,duration,event);''')
            self.p=self.v=self.r=self.d=0
            self.max_chunk_bytes=self.output_chunks=0
        except Exception:
            self.close()
            raise

    def close(self):
        pending=sys.exc_info()[0] is not None
        try:
            self.sql.close()
            old=self.previous_directory[0] if self.previous_directory else ''
            with sqlite3.connect(':memory:') as reset:
                try:reset.execute("PRAGMA temp_store_directory='"+old.replace("'","''")+"'")
                except sqlite3.DatabaseError:pass
        except Exception:
            if not pending:raise
        finally:
            if self.previous_environment is None:os.environ.pop('SQLITE_TMPDIR',None)
            else:os.environ['SQLITE_TMPDIR']=self.previous_environment

    def ingest(self,manifest,batches):
        for selected in manifest['batches']:
            fp=selected['batch_fingerprint'];fmt=manifest['input_format']
            directory=batches/'v2'/('format='+fmt.lower())/('source_id='+selected['source_id'])/('batch_id='+selected['batch_id'])
            m.root_path(directory,'DEPENDENCY_UNAVAILABLE')
            ev=m.capture(directory,{'manifest.json':131072,'complete.json':4096},('data',),'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',guards=(batches,batches/'v2',directory.parent.parent,directory.parent))
            raw=ev.raw('manifest.json');m.need(sha256(raw).hexdigest()==fp,'DEPENDENCY_INVALID')
            batch=m.parse(raw,131072,'DEPENDENCY_INVALID')
            validate_batch(batch,selected,fmt,ev)
            self.dependencies.append(ev)
            m.need(sum(len(e.inventory()['data']) for e in self.dependencies)<=128,'RESOURCE_LIMIT')
            m.need(sum(f.length for e in self.dependencies for f in e.files if f.path.parent.name=='data')<=1073741824,'RESOURCE_LIMIT')
            m.need(m.descriptor(ev.raw('complete.json'))==selected['completion'],'DEPENDENCY_INVALID')
            original={f.path:f for f in ev.files};received=instant(batch['received_at'])
            for file in batch['files']:
                path=directory/file['file'];captured=original[path];count=0
                m.need(captured.digest==file['sha256'] and captured.length==file['byte_length'],'DEPENDENCY_INVALID')
                indexed={}
                for positions,(service,event,payload,reasons) in records(path,fmt,batch['profile'],result=indexed):
                    p,line,offset,length,group,row=positions;count+=1;self.p+=1
                    m.need(self.p<=2000000,'RESOURCE_LIMIT')
                    blob=canonical_bytes(payload) if payload is not None else None
                    old=self.sql.execute('SELECT payload FROM identities WHERE service=? AND event=?',(service,event)).fetchone() if service else None
                    if old is not None:m.need(blob is not None and old[0]==blob,'EVENT_IDENTITY_CONFLICT')
                    elif service:self.sql.execute('INSERT INTO identities VALUES (?,?,?)',(service,event,blob))
                    disposition='REJECTED' if blob is None else 'DUPLICATE' if old is not None else 'ACCEPTED'
                    self.r+=blob is None;self.v+=blob is not None;self.d+=disposition=='DUPLICATE'
                    if disposition=='ACCEPTED':
                        self.sql.execute('INSERT INTO events VALUES (?,?,?,?,?,?,?,?,?,?)',(service,event,blob,1,received,received,payload[3],(payload[4]//300000000)*300000000,payload[8],payload[7]))
                    elif disposition=='DUPLICATE':self.sql.execute('UPDATE events SET deliveries=deliveries+1,first_us=min(first_us,?),last_us=max(last_us,?) WHERE service=? AND event=?',(received,received,service,event))
                    expected=[2,fp,batch['source_id'],batch['batch_id'],batch['ingestion_id'],file['artifact_id'],file['sha256'],fmt,
                        p,line,offset,length,group,row,received,disposition,service,event,sha256(blob).hexdigest() if blob else None,reasons]
                    self.sql.execute('INSERT INTO raw VALUES (?,?,?,?)',(self.p,canonical_bytes(expected+(list(payload) if payload else [None]*10)),blob,canonical_bytes([service,event,reasons]) if blob is None else None))
                    if self.p%1024==0:self.sql.commit();self.check_budget()
                m.need(count==file['record_count'],'DEPENDENCY_INVALID')
                m.need(all(indexed[k]==file[k] for k in ('record_count','physical_line_count','row_groups')),'DEPENDENCY_INVALID')
                m.observe(path,m.FILE_LIMIT,'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',captured)
            ev.check()
        self.sql.commit();self.check_budget()

    def check_budget(self):
        visible=sum(p.stat().st_size for p in self.directory.iterdir() if p.is_file())
        m.need(visible<=8*1024**3,'RESOURCE_LIMIT')
        return visible

    def digests(self):
        result={}
        for scope,query in (('valid','SELECT payload FROM raw WHERE payload IS NOT NULL ORDER BY payload'),
                            ('unique','SELECT payload FROM events ORDER BY payload'),
                            ('rejected','SELECT rejected FROM raw WHERE rejected IS NOT NULL ORDER BY rejected')):
            digest=sha256(('ddip-reliability-'+scope+'-multiset-v1\n').encode())
            for (blob,) in self.sql.execute(query):digest.update(blob);digest.update(b'\n')
            result[scope+'_multiset_sha256']=digest.hexdigest()
        return result

    def compare(self,db,manifest,reference):
        e,o,a=m.EVENTS,m.OCCURRENCES,m.METRICS
        columns=','.join('epoch_us(o.received_at)' if n=='received_at' else 'o."'+n+'"' for n,t in m.SCHEMAS[o])
        business=','.join('epoch_us(e.event_time)' if n=='event_time' else 'e."'+n+'"' for n in NAMES)
        cursor=db.execute('SELECT '+columns+','+business+' FROM '+o+' o LEFT JOIN '+e+' e USING(service_id,event_id) ORDER BY o.source_id,o.batch_id,o.batch_fingerprint,o.artifact_id,o.record_ordinal')
        expected=iter(self.sql.execute('SELECT expected FROM raw ORDER BY position'));seen=0
        while chunk:=cursor.fetchmany(16):
            m.need(len(canonical_bytes(chunk))<=2097152,'RESOURCE_LIMIT')
            self.max_chunk_bytes=max(self.max_chunk_bytes,len(canonical_bytes(chunk)));self.output_chunks+=1
            for row in chunk:
                wanted=next(expected,None);m.need(wanted is not None and canonical_bytes(row)==wanted[0],'PUBLICATION_INVALID');seen+=1
        m.need(next(expected,None) is None and seen==self.p,'PUBLICATION_INVALID')
        ref=manifest['reference']
        def context(service):
            r=reference.get(service)
            return [*(r if r else (None,None,None)),'MATCHED' if r else 'MISSING',ref['reference_id'],ref['revision'],ref['snapshot_fingerprint']]
        projection=','.join('epoch_us("'+n+'")' if t=='timestamp' else '"'+n+'"' for n,t in m.SCHEMAS[e])
        cursor=db.execute('SELECT '+projection+' FROM '+e+' ORDER BY service_id,event_id')
        expected=iter(self.sql.execute('SELECT service,payload,deliveries,first_us,last_us FROM events ORDER BY service,event'))
        while chunk:=cursor.fetchmany(16):
            m.need(len(canonical_bytes(chunk))<=2097152,'RESOURCE_LIMIT')
            for row in chunk:
                wanted=next(expected,None);m.need(wanted is not None,'PUBLICATION_INVALID')
                service,blob,n,first,last=wanted
                m.need(list(row)==[1,*json.loads(blob),n,first,last,*context(service)],'PUBLICATION_INVALID')
        m.need(next(expected,None) is None,'PUBLICATION_INVALID')
        groups=iter(self.sql.execute('SELECT service,environment,window,count(*),sum(status BETWEEN 500 AND 599),sum(duration) FROM events GROUP BY service,environment,window ORDER BY service,environment,window'))
        projection=','.join('epoch_us(window_start)' if n=='window_start' else '"'+n+'"' for n,t in m.SCHEMAS[a])
        cursor=db.execute('SELECT '+projection+' FROM '+a+' ORDER BY service_id,environment,window_start')
        checked=max_rank=max_error=0
        while chunk:=cursor.fetchmany(16):
            m.need(len(canonical_bytes(m.public(chunk)))<=2097152,'RESOURCE_LIMIT')
            for row in chunk:
                wanted=next(groups,None);m.need(wanted is not None,'PUBLICATION_INVALID')
                service,env,window,n,errors,total=wanted;candidate=row[9]
                m.need(list(row[:6])==[1,service,env,window,n,errors] and row[7]==total and list(row[10:])==context(service),'PUBLICATION_INVALID')
                for actual,exact in ((row[6],errors/n),(row[8],total/n)):
                    m.need(math.isfinite(actual) and abs(actual-exact)<=max(1e-12,abs(exact)*1e-12),'PUBLICATION_INVALID')
                below,at_or_below,present=self.sql.execute('SELECT sum(duration<?),sum(duration<=?),sum(duration=?) FROM events WHERE service=? AND environment=? AND window=?',(candidate,candidate,candidate,service,env,window)).fetchone()
                ok,displacement,budget=qualified(candidate,n,below,at_or_below,present);m.need(ok,'PUBLICATION_INVALID')
                rank=(95*n+99)//100
                exact=self.sql.execute('SELECT duration FROM events WHERE service=? AND environment=? AND window=? ORDER BY duration,event LIMIT 1 OFFSET ?',(service,env,window,rank-1)).fetchone()[0]
                checked+=1;max_rank=max(max_rank,displacement);max_error=max(max_error,abs(exact-candidate))
        m.need(next(groups,None) is None,'PUBLICATION_INVALID')
        u=self.sql.execute('SELECT count(*) FROM events').fetchone()[0]
        m.need(self.p==self.v+self.r and self.v==u+self.d,'PUBLICATION_INVALID')
        return dict(physical_occurrences=self.p,valid_occurrences=self.v,unique_valid_events=u,
            identical_duplicate_occurrences=self.d,rejected_occurrences=self.r,groups_checked=checked,
            max_rank_displacement=max_rank,max_absolute_p95_error_ms=max_error,
            max_output_chunk_bytes=self.max_chunk_bytes,output_chunks=self.output_chunks,**self.digests())


def _verify_publication(root,fingerprint,*,batch_root,reference_root,scratch_root,selection):
    m.match(fingerprint)
    started=time.perf_counter_ns();root=m.root_path(root,'PUBLICATION_UNAVAILABLE');directory=m.publication_directory(root,fingerprint)
    m.root_path(directory,'PUBLICATION_UNAVAILABLE')
    try:(directory/'complete.json').lstat()
    except FileNotFoundError:raise m.ReliabilityFailure('PUBLICATION_INCOMPLETE') from None
    except OSError:raise m.ReliabilityFailure('PUBLICATION_INVALID') from None
    evidence=m.capture(directory,{'inputs.json':65536,'complete.json':1048576},m.TABLES,'PUBLICATION_INVALID','PUBLICATION_CHANGED',guards=m.namespace_guards(root,directory))
    raw=evidence.raw('inputs.json');manifest=m.validate_manifest(raw);selection['input_format']=manifest['input_format']
    m.need(sha256(raw).hexdigest()==fingerprint)
    marker=m.validate_completion(evidence.raw('complete.json'),fingerprint,len(raw));m.need(marker['files']==evidence.inventory())
    r=manifest['reference'];reference_root=m.root_path(reference_root);refs=reference_root/('reference_id='+r['reference_id'])/('revision='+r['revision'])
    m.root_path(refs,'DEPENDENCY_UNAVAILABLE')
    batch_root=m.root_path(batch_root);scratch_root=m.root_path(scratch_root)
    m.separate(root,batch_root,reference_root,scratch_root)
    ref=m.capture(refs,{'reference.csv':10485760,'vocabulary.json':32768,'manifest.json':16384,'complete.json':4096},(),'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',guards=(reference_root,refs.parent))
    m.need(sha256(ref.raw('manifest.json')).hexdigest()==r['snapshot_fingerprint'] and m.descriptor(ref.raw('complete.json'))==r['completion'],'DEPENDENCY_INVALID')
    # Accepted independent reference boundary; numerical/event logic stays separate.
    import service_reference
    import reliability_metadata as legacy_metadata
    try:service_reference.open_snapshot(reference_root,r['reference_id'],r['revision'],r['snapshot_fingerprint'])
    except legacy_metadata.ReliabilityFailure as error:raise m.ReliabilityFailure('DEPENDENCY_CHANGED' if error.code=='SNAPSHOT_CHANGED' else 'DEPENDENCY_INVALID') from None
    reference={}
    for row in csv.DictReader(ref.raw('reference.csv').decode('utf-8').splitlines()):
        service=row['service_id'];m.need(service not in reference,'DEPENDENCY_INVALID')
        reference[service]=tuple(row[n] or None for n in ('service_name','owner_team','service_tier'))
    oracle=Oracle(scratch_root)
    try:
        oracle.ingest(manifest,batch_root)
        m.need(sum(len(b['files']) for b in (m.parse(ev.raw('manifest.json'),131072,'DEPENDENCY_INVALID') for ev in oracle.dependencies))<=128,'RESOURCE_LIMIT')
        m.need(sum(f.length for ev in oracle.dependencies for f in ev.files if f.path.parent.name=='data')<=1073741824,'RESOURCE_LIMIT')
        with codec() as db:
            register_outputs(db,evidence)
            result=oracle.compare(db,manifest,reference)
        for k,v in marker['occurrence_counts'].items():m.need(result[k]==v)
        m.need(marker['output_counts']=={m.EVENTS:result['unique_valid_events'],m.OCCURRENCES:result['physical_occurrences'],m.METRICS:result['groups_checked']})
        result.update(status='PASSED',oracle_ns=time.perf_counter_ns()-started,scratch_visible_bytes=oracle.check_budget(),
            sqlite_cache_bytes=67108864,unlinked_temporary_bytes=None,unlinked_temporary_status='NOT_MEASURED',
            shared_parquet_codec=True,input_format=manifest['input_format'],product_contract_version=2,processor_revision=2)
        evidence.check();ref.check()
        for ev in oracle.dependencies:ev.check()
        return result
    except Exception as error:
        if isinstance(error,m.ReliabilityFailure) and error.code in ('DEPENDENCY_CHANGED','PUBLICATION_CHANGED'):raise
        evidence.check();ref.check()
        for ev in oracle.dependencies:ev.check()
        raise
    finally:oracle.close()


def verify_publication(root,fingerprint,*,batch_root,reference_root,scratch_root):
    selection={'input_format':None}
    try:return _verify_publication(root,fingerprint,batch_root=batch_root,reference_root=reference_root,scratch_root=scratch_root,selection=selection)
    except m.ReliabilityFailure as error:
        error.diagnostic['input_format']=selection['input_format'];raise
    except Exception as error:
        resource=isinstance(error,duckdb.OutOfMemoryException) or getattr(error,'sqlite_errorcode',None) in (sqlite3.SQLITE_FULL,sqlite3.SQLITE_NOMEM)
        failure=m.ReliabilityFailure('RESOURCE_LIMIT' if resource else 'PUBLICATION_INVALID')
        failure.diagnostic['input_format']=selection['input_format'];raise failure from None


def main(argv=None):
    try:
        options=m.arguments(sys.argv[1:] if argv is None else argv,{'root','publication','batch-root','reference-root','scratch-root'})
        if options is None:
            print('Independent E3 oracle: --root --publication --batch-root --reference-root --scratch-root (existing empty dedicated directory)');return 0
        result=verify_publication(options['root'],options['publication'],batch_root=options['batch_root'],reference_root=options['reference_root'],scratch_root=options['scratch_root'])
        result.update(origin='synthetic',label=m.LABEL,product_fingerprint=options['publication'])
        public=m.public(result);public.update(product_contract_version=2,processor_revision=2)
        encoded=canonical_bytes(public);m.need(len(encoded)<=m.RESPONSE_LIMIT,'RESPONSE_LIMIT')
        print(encoded.decode());return 0
    except m.ReliabilityFailure as error:return m.cli_error(error)
    except Exception:return m.cli_error(m.ReliabilityFailure('PUBLICATION_INVALID'))


if __name__=='__main__':raise SystemExit(main())
