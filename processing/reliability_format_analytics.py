"""Independent synchronous Reliability verifier/reports. Never imports Spark/producers."""
from hashlib import sha256
import json
import sys
import time
from threading import Lock
import duckdb
from publication_metadata import canonical_bytes
import reliability_format_metadata as m
from reliability_format_inputs import verify_dependencies,scan_file

DUCK_TYPES={'integer':'INTEGER','long':'BIGINT','string':'VARCHAR','boolean':'BOOLEAN',
            'timestamp':'TIMESTAMP WITH TIME ZONE','double':'DOUBLE','decimal(38,0)':'DECIMAL(38,0)','array<string>':'VARCHAR[]'}
PHYSICAL={'integer':'INT32','long':'INT64','string':'BYTE_ARRAY','boolean':'BOOLEAN','double':'DOUBLE',
          'timestamp':'INT64','decimal(38,0)':'FIXED_LEN_BYTE_ARRAY'}
E,O,A=m.EVENTS,m.OCCURRENCES,m.METRICS
GROUP=('service_id','environment','window_start')
REF_FIELDS=('service_name','owner_team','service_tier','reference_status','reference_id','reference_revision','reference_fingerprint')
WINDOW="(epoch_us(e.event_time)-(((epoch_us(e.event_time)%300000000)+300000000)%300000000))"


def connection():
    m.need(duckdb.__version__=='1.4.5','UNSUPPORTED_VERSION')
    db=duckdb.connect(':memory:',config=dict(threads=2,memory_limit='512MiB',temp_directory='',
        autoinstall_known_extensions=False,autoload_known_extensions=False,allow_unsigned_extensions=False))
    db.execute("SET TimeZone='UTC'")
    return db


def reject(db,sql,parameters=None):
    m.need(db.execute(sql,parameters or []).fetchone() is None)


def same(db,actual,expected):
    reject(db,f'SELECT 1 FROM (({actual}) EXCEPT ALL ({expected})) LIMIT 1')
    reject(db,f'SELECT 1 FROM (({expected}) EXCEPT ALL ({actual})) LIMIT 1')


def register_table(db,table,paths):
    schema=m.SCHEMAS[table];m.need(bool(paths))
    row_total=0
    for path in paths:
        relation=db.read_parquet(path,hive_partitioning=False,union_by_name=False)
        m.need(tuple(zip(relation.columns,map(str,relation.types)))==tuple((n,DUCK_TYPES[t]) for n,t in schema))
        cursor=db.execute('SELECT * FROM parquet_schema(?)',[path]);columns=[d[0] for d in cursor.description]
        physical=[dict(zip(columns,r)) for r in cursor.fetchall()]
        for name,kind in schema:
            row=next((r for r in physical if r['name']==name),None);m.need(row is not None)
            if kind=='array<string>':
                m.need(row['type'] is None and (row['converted_type']=='LIST' or 'ListType' in str(row['logical_type'])))
                # Spark standard three-level list, scalar UTF-8 element; never a map/struct/coerced array.
                i=physical.index(row);m.need(i+2<len(physical) and physical[i+1]['name']=='list' and physical[i+1]['repetition_type']=='REPEATED' and physical[i+2]['name']=='element' and physical[i+2]['type']=='BYTE_ARRAY' and physical[i+2]['converted_type']=='UTF8')
            else:
                m.need(row['type']==PHYSICAL[kind])
                if kind=='timestamp':
                    m.need('isAdjustedToUTC=1' in str(row['logical_type']) and 'MICROS=MicroSeconds()' in str(row['logical_type']))
                elif kind=='string':m.need(row['converted_type']=='UTF8')
                elif kind=='decimal(38,0)':m.need(row['precision']==38 and row['scale']==0 and str(row['type_length'])=='16')
        row_total+=db.execute('SELECT num_rows FROM parquet_file_metadata(?)',[path]).fetchone()[0]
        m.need(row_total<=m.MAX_ROWS,'RESOURCE_LIMIT')
    db.read_parquet(list(paths),hive_partitioning=False,union_by_name=False).create_view(table)


def _register_dependencies(db,dependencies):
    db.execute('CREATE TEMP TABLE reference_rows(service_id VARCHAR,service_name VARCHAR,owner_team VARCHAR,service_tier VARCHAR)')
    if dependencies.reference.rows:
        db.executemany('INSERT INTO reference_rows VALUES (?,?,?,?)',[(r.service_id,r.service_name,r.owner_team,r.service_tier) for r in dependencies.reference.rows])
    db.execute('CREATE TEMP TABLE reference_selection(reference_id VARCHAR,reference_revision VARCHAR,reference_fingerprint VARCHAR)')
    r=dependencies.reference
    db.execute('INSERT INTO reference_selection VALUES (?,?,?)',[r.reference_id,r.revision,r.fingerprint])
    db.execute('CREATE TEMP TABLE input_files(batch_fingerprint VARCHAR,source_id VARCHAR,batch_id VARCHAR,ingestion_id VARCHAR,artifact_id VARCHAR,input_file_sha256 VARCHAR,received_us BIGINT,expected_lines BIGINT,expected_bytes BIGINT,expected_format VARCHAR)')
    rows=[]
    for b in dependencies.batches:
        obj=b.manifest
        for f in obj['files']:
            rows.append((b.fingerprint,obj['source_id'],obj['batch_id'],obj['ingestion_id'],f['artifact_id'],f['sha256'],m.time_us(obj['received_at']),f['record_count'],f['byte_length'],obj['input_format']))
    db.executemany('INSERT INTO input_files VALUES (?,?,?,?,?,?,?,?,?,?)',rows)


def validate_tables(db,dependencies,marker):
    _register_dependencies(db,dependencies)
    actual_counts={}
    for name,fields in m.SCHEMAS.items():
        optional={'service_name','owner_team','service_tier'} if name!=O else {'service_id','event_id','semantic_sha256','line_number','byte_offset','physical_byte_length','row_group_ordinal','row_ordinal'}
        invalid=['contract_version<>'+str(2 if name==O else 1)]+['"'+n+'" IS NULL' for n,t in fields if n not in optional]
        invalid += [f'(epoch_us({n}) IS NULL OR epoch_us({n})<{m.MIN_US} OR epoch_us({n})>={m.END_US})' for n,t in fields if t=='timestamp']
        reject(db,f'SELECT 1 FROM {name} WHERE '+ ' OR '.join(invalid)+' LIMIT 1')
        keys=['service_id','event_id'] if name==E else ['batch_fingerprint','artifact_id','record_ordinal'] if name==O else list(GROUP)
        reject(db,f'SELECT 1 FROM {name} GROUP BY '+','.join(keys)+' HAVING count(*)<>1 LIMIT 1')
        count=db.execute(f'SELECT count(*) FROM {name}').fetchone()[0]
        m.need(count<=m.MAX_ROWS,'RESOURCE_LIMIT');m.need(count==marker['output_counts'][name]);actual_counts[name]=count
    reject(db,f"SELECT 1 FROM {E} e WHERE schema_version<>1 OR NOT is_synthetic OR status_code NOT BETWEEN 100 AND 599 OR duration_ms NOT BETWEEN 0 AND 3600000 OR environment NOT IN ('dev','test','production') OR e.method NOT IN ('GET','HEAD','POST','PUT','PATCH','DELETE','OPTIONS') OR valid_delivery_count<1 OR first_received_at>last_received_at LIMIT 1")
    for table,names in ((E,('service_id','event_id','route_id')),(O,('service_id','event_id','semantic_sha256'))):
        for name in names:
            pattern=m.SOURCE.pattern if name=='service_id' else m.UUID.pattern if name=='event_id' else m.ID.pattern if name=='route_id' else m.HEX.pattern
            reject(db,f"SELECT 1 FROM {table} WHERE {name} IS NOT NULL AND NOT regexp_full_match({name},?) LIMIT 1",[pattern])
    fixed='['+','.join(f"CASE WHEN list_contains(reason_codes,'{c}') THEN '{c}' END" for c in m.REASONS)+']'
    reject(db,f"SELECT 1 FROM {O} WHERE disposition NOT IN ('ACCEPTED','DUPLICATE','REJECTED') OR input_format NOT IN ('JSONL','PARQUET') OR record_ordinal<0 OR (input_format='JSONL' AND (line_number IS NULL OR byte_offset IS NULL OR physical_byte_length IS NULL OR line_number<>record_ordinal+1 OR byte_offset<0 OR physical_byte_length NOT BETWEEN 1 AND {m.LINE_LIMIT+2} OR row_group_ordinal IS NOT NULL OR row_ordinal IS NOT NULL)) OR (input_format='PARQUET' AND (line_number IS NOT NULL OR byte_offset IS NOT NULL OR physical_byte_length IS NOT NULL OR row_group_ordinal IS NULL OR row_ordinal IS NULL OR row_group_ordinal<0 OR row_ordinal<0)) OR reason_codes<>list_filter({fixed},x->x IS NOT NULL) OR (service_id IS NULL)<>(event_id IS NULL) OR (disposition='REJECTED' AND (semantic_sha256 IS NOT NULL OR len(reason_codes)=0)) OR (disposition<>'REJECTED' AND (service_id IS NULL OR event_id IS NULL OR semantic_sha256 IS NULL OR len(reason_codes)<>0)) LIMIT 1")
    reject(db,f"SELECT 1 FROM {O} o LEFT JOIN input_files f USING(batch_fingerprint,artifact_id) WHERE f.batch_fingerprint IS NULL OR o.source_id<>f.source_id OR o.batch_id<>f.batch_id OR o.ingestion_id<>f.ingestion_id OR o.input_file_sha256<>f.input_file_sha256 OR epoch_us(o.received_at)<>f.received_us OR o.input_format<>f.expected_format OR o.record_ordinal>=f.expected_lines LIMIT 1")
    reject(db,f"SELECT 1 FROM input_files f LEFT JOIN (SELECT batch_fingerprint,artifact_id,count(*) n,sum(physical_byte_length::DECIMAL(38,0)) bytes FROM {O} GROUP BY batch_fingerprint,artifact_id) o USING(batch_fingerprint,artifact_id) WHERE coalesce(n,0)<>expected_lines OR (expected_format='JSONL' AND coalesce(bytes,0)<>expected_bytes) LIMIT 1")
    reject(db,f"SELECT 1 FROM {O} WHERE service_id IS NOT NULL GROUP BY service_id,event_id HAVING count(*)>1 AND count(*) FILTER(WHERE disposition='REJECTED')>0 LIMIT 1")
    rank_sql=f"SELECT disposition,row_number() OVER(PARTITION BY service_id,event_id ORDER BY source_id,batch_id,batch_fingerprint,artifact_id,record_ordinal) n FROM {O} WHERE disposition<>'REJECTED'"
    reject(db,f"SELECT 1 FROM ({rank_sql}) WHERE disposition<>CASE WHEN n=1 THEN 'ACCEPTED' ELSE 'DUPLICATE' END LIMIT 1")
    reject(db,f"SELECT 1 FROM {O} o LEFT JOIN {E} e USING(service_id,event_id) WHERE disposition<>'REJECTED' AND e.service_id IS NULL LIMIT 1")
    delivery=f"SELECT service_id,event_id,count(*) valid_delivery_count,min(received_at) first_received_at,max(received_at) last_received_at FROM {O} WHERE disposition<>'REJECTED' GROUP BY service_id,event_id"
    base=','.join('e.'+n for n in m.EVENT_FIELDS)
    context="r.service_name,r.owner_team,r.service_tier,CASE WHEN r.service_id IS NULL THEN 'MISSING' ELSE 'MATCHED' END reference_status,s.reference_id,s.reference_revision,s.reference_fingerprint"
    expected=f"SELECT 1 contract_version,{base},d.valid_delivery_count,d.first_received_at,d.last_received_at,{context} FROM {E} e LEFT JOIN ({delivery}) d USING(service_id,event_id) LEFT JOIN reference_rows r USING(service_id) CROSS JOIN reference_selection s"
    same(db,f'SELECT * FROM {E}',expected)
    exact_columns=[n for n,t in m.SCHEMAS[A] if n not in ('p95_duration_ms_approx','server_error_rate','mean_duration_ms')]
    expected_groups=f"SELECT e.service_id,e.environment,{WINDOW} window_start,count(*)::BIGINT request_count,count(*) FILTER(WHERE status_code BETWEEN 500 AND 599)::BIGINT server_error_count,sum(duration_ms::DECIMAL(38,0))::DECIMAL(38,0) duration_sum_ms FROM {E} e GROUP BY e.service_id,e.environment,window_start"
    db.execute('CREATE TEMP VIEW expected_groups AS '+expected_groups)
    expected_metrics=f"SELECT 1 contract_version,g.service_id,g.environment,g.window_start,g.request_count,g.server_error_count,g.duration_sum_ms,{context} FROM expected_groups g LEFT JOIN reference_rows r USING(service_id) CROSS JOIN reference_selection s"
    actual=','.join('epoch_us(window_start) AS window_start' if n=='window_start' else n for n in exact_columns)
    same(db,f'SELECT {actual} FROM {A}',f'SELECT {",".join(exact_columns)} FROM ({expected_metrics})')
    reject(db,f"SELECT 1 FROM {A} WHERE request_count<1 OR request_count>{m.MAX_ROWS} OR server_error_count NOT BETWEEN 0 AND request_count OR duration_sum_ms<0 OR epoch_us(window_start)%300000000<>0 OR NOT isfinite(server_error_rate) OR NOT isfinite(mean_duration_ms) OR server_error_rate NOT BETWEEN 0 AND 1 OR mean_duration_ms NOT BETWEEN 0 AND 3600000 OR abs(server_error_rate-server_error_count::DOUBLE/request_count)>greatest(1e-12,abs(server_error_count::DOUBLE/request_count)*1e-12) OR abs(mean_duration_ms-duration_sum_ms::DOUBLE/request_count)>greatest(1e-12,abs(duration_sum_ms::DOUBLE/request_count)*1e-12) LIMIT 1")
    ranks=f"SELECT a.service_id,a.environment,a.window_start,a.request_count,a.p95_duration_ms_approx,count(*) FILTER(WHERE e.duration_ms<a.p95_duration_ms_approx)+1 lo,count(*) FILTER(WHERE e.duration_ms<=a.p95_duration_ms_approx) hi,count(*) FILTER(WHERE e.duration_ms=a.p95_duration_ms_approx) present FROM {A} a JOIN {E} e ON e.service_id=a.service_id AND e.environment=a.environment AND {WINDOW}=epoch_us(a.window_start) GROUP BY ALL"
    reject(db,f'SELECT 1 FROM ({ranks}) WHERE present=0 OR greatest(lo-(95*request_count+99)//100,(95*request_count+99)//100-hi,0)>greatest(1,(request_count+9999)//10000) LIMIT 1')
    p=actual_counts[O];u=actual_counts[E]
    d,r=db.execute(f"SELECT count(*) FILTER(WHERE disposition='DUPLICATE'),count(*) FILTER(WHERE disposition='REJECTED') FROM {O}").fetchone()
    counts=dict(physical_occurrences=p,valid_occurrences=p-r,unique_valid_events=u,identical_duplicate_occurrences=d,rejected_occurrences=r)
    m.need(counts==marker['occurrence_counts'] and p==u+d+r)
    return counts


def raw_query():
    columns=[('epoch_us(o.received_at) AS received_at') if n=='received_at' else 'o.'+n for n,t in m.SCHEMAS[O]]
    columns += ['epoch_us(e.event_time)' if n=='event_time' else 'e.'+n for n in m.EVENT_FIELDS]
    return f"SELECT {','.join(columns)} FROM {O} o LEFT JOIN {E} e ON o.service_id=e.service_id AND o.event_id=e.event_id ORDER BY o.source_id,o.batch_id,o.batch_fingerprint,o.artifact_id,o.record_ordinal"


def verify_raw(db,dependencies, *, explain=False):
    """One ordered join/cursor, never a Parquet rescan for each raw chunk."""
    started=time.perf_counter(); sql=raw_query()
    plan=db.execute('EXPLAIN (FORMAT JSON) '+sql).fetchone()[1] if explain else None
    cursor=db.execute(sql);buffer=[];index=0;seen=chunks=peak_bytes=0
    occurrence_columns=[n for n,t in m.SCHEMAS[O]]
    def next_actual():
        nonlocal buffer,index,chunks,peak_bytes
        if index==len(buffer):
            buffer=cursor.fetchmany(m.CHUNK_ROWS);index=0
            if buffer:
                size=len(canonical_bytes(m.public(buffer)))
                m.need(size<=m.CHUNK_BYTES,'RESOURCE_LIMIT');peak_bytes=max(peak_bytes,size);chunks+=1
        if not buffer:return None
        row=buffer[index];index+=1
        return row
    for batch in dependencies.batches:
        obj=batch.manifest; originals={f.path:f for f in batch.evidence.files}
        for file in obj['files']:
            path=batch.evidence.directory/file['file'];result={}
            for expected in scan_file(path,obj['profile'],previous=originals[path],unavailable='DEPENDENCY_INVALID',changed='DEPENDENCY_CHANGED',result=result):
                row=next_actual();m.need(row is not None);seen+=1
                actual=dict(zip(occurrence_columns,row[:20],strict=True))
                disposition=actual['disposition']
                m.need(disposition=='REJECTED' if expected['state']=='REJECTED' else disposition in ('ACCEPTED','DUPLICATE'))
                wanted=dict(contract_version=2,batch_fingerprint=batch.fingerprint,source_id=obj['source_id'],batch_id=obj['batch_id'],
                    ingestion_id=obj['ingestion_id'],artifact_id=file['artifact_id'],input_file_sha256=file['sha256'],
                    input_format=expected['input_format'],record_ordinal=expected['record_ordinal'],
                    line_number=expected['line_number'],byte_offset=expected['byte_offset'],physical_byte_length=expected['physical_byte_length'],
                    row_group_ordinal=expected['row_group_ordinal'],row_ordinal=expected['row_ordinal'],
                    received_at=m.time_us(obj['received_at']),disposition=disposition,service_id=expected['recognized_service_id'],
                    event_id=expected['recognized_event_id'],semantic_sha256=expected['semantic_sha256'],reason_codes=expected['reason_codes'])
                m.need(actual==wanted)
                if expected['state']=='VALID':m.need(tuple(row[20:])==tuple(expected['event'][n] for n in m.TRANSPORT_FIELDS))
            m.need(result['record_count']==file['record_count'] and m.freeze(result['row_groups'])==file['row_groups'])
    m.need(next_actual() is None)
    return dict(rows=seen,chunks=chunks,max_chunk_bytes=peak_bytes,ordered_query_executions=1,
                elapsed_seconds=time.perf_counter()-started,plan=plan)


def verify_core(dependencies,evidence,marker, *, explain=False):
    db=None
    try:
        db=connection()
        for table in m.TABLES:register_table(db,table,evidence.paths(table))
        validate_tables(db,dependencies,marker)
        stats=verify_raw(db,dependencies,explain=explain)
        dependencies.check();evidence.check()
        return db,stats
    except Exception as error:
        if db is not None:
            try:db.close()
            except Exception:pass
        if isinstance(error,m.ReliabilityFailure) and error.code in ('DEPENDENCY_CHANGED','PUBLICATION_CHANGED'):
            raise
        dependencies.check();evidence.check()
        if isinstance(error,m.ReliabilityFailure):raise
        raise m.ReliabilityFailure('RESOURCE_LIMIT' if isinstance(error,duckdb.OutOfMemoryException) else 'PUBLICATION_INVALID') from None


METRIC_COLUMNS=('window_start','service_id','environment','request_count','server_error_count','server_error_rate','duration_sum_ms','mean_duration_ms','p95_duration_ms_approx','service_name','owner_team','service_tier','reference_status')
INPUT_COLUMNS=('source_id','batch_id','batch_fingerprint','ingestion_id','received_at','input_file_count','input_byte_length','physical_occurrence_count','valid_occurrence_count','accepted_representative_count','duplicate_occurrence_count','rejected_occurrence_count','first_event_time','last_event_time','future_occurrence_count','min_receipt_delay_us','max_receipt_delay_us','rejected_reason_counts')
COVERAGE_COLUMNS=('service_id','reference_status','reference_usage','service_name','owner_team','service_tier','event_count','missing_reference_event_count','missing_optional_attributes')
REPORTS={'requests_5m':(A,METRIC_COLUMNS,'window_start,service_id,environment'),
         'input_coverage':('input_coverage',INPUT_COLUMNS,'source_id,batch_id,batch_fingerprint'),
         'reference_coverage':('reference_coverage',COVERAGE_COLUMNS,'service_id')}
TIME_COLUMNS={'window_start','received_at','first_event_time','last_event_time'}
REF_TOTALS=('event_count','matched_event_count','missing_event_count','event_service_count','matched_service_count','missing_service_count','matched_services_missing_optional_attributes_count','reference_service_count','unused_reference_service_count')
INPUT_TOTALS=('input_batch_count','input_file_count','input_byte_length','physical_occurrence_count','valid_occurrence_count','accepted_representative_count','duplicate_occurrence_count','rejected_occurrence_count','distinct_valid_identity_count','duplicate_excess_in_scope')


def prepare_reports(db):
    missing="list_filter([CASE WHEN r.owner_team IS NULL THEN 'owner_team' END,CASE WHEN r.service_tier IS NULL THEN 'service_tier' END],x->x IS NOT NULL)"
    db.execute(f"CREATE TEMP VIEW reference_coverage AS SELECT coalesce(e.service_id,r.service_id) service_id,CASE WHEN r.service_id IS NULL THEN 'MISSING' ELSE 'MATCHED' END reference_status,CASE WHEN r.service_id IS NULL THEN 'ABSENT' WHEN e.service_id IS NULL THEN 'UNUSED' ELSE 'USED' END reference_usage,r.service_name,r.owner_team,r.service_tier,coalesce(e.n,0)::BIGINT event_count,CASE WHEN r.service_id IS NULL THEN e.n ELSE 0 END::BIGINT missing_reference_event_count,CASE WHEN r.service_id IS NULL THEN []::VARCHAR[] ELSE {missing} END missing_optional_attributes FROM (SELECT service_id,count(*) n FROM {E} GROUP BY service_id) e FULL JOIN reference_rows r USING(service_id)")
    reasons=','.join(f"count(*) FILTER(WHERE list_contains(reason_codes,'{code}')) _reason_{i}" for i,code in enumerate(m.REASONS))
    sums=f"SELECT o.batch_fingerprint,count(*) physical_occurrence_count,count(*) FILTER(WHERE disposition<>'REJECTED') valid_occurrence_count,count(*) FILTER(WHERE disposition='ACCEPTED') accepted_representative_count,count(*) FILTER(WHERE disposition='DUPLICATE') duplicate_occurrence_count,count(*) FILTER(WHERE disposition='REJECTED') rejected_occurrence_count,min(epoch_us(e.event_time)) first_event_time,max(epoch_us(e.event_time)) last_event_time,count(*) FILTER(WHERE epoch_us(e.event_time)>epoch_us(o.received_at)) future_occurrence_count,min(epoch_us(o.received_at)-epoch_us(e.event_time)) min_receipt_delay_us,max(epoch_us(o.received_at)-epoch_us(e.event_time)) max_receipt_delay_us,{reasons} FROM {O} o LEFT JOIN {E} e ON o.disposition<>'REJECTED' AND o.service_id=e.service_id AND o.event_id=e.event_id GROUP BY o.batch_fingerprint"
    file_groups="SELECT batch_fingerprint,source_id,batch_id,ingestion_id,received_us received_at,count(*) input_file_count,sum(expected_bytes)::BIGINT input_byte_length FROM input_files GROUP BY ALL"
    counters=('physical_occurrence_count','valid_occurrence_count','accepted_representative_count','duplicate_occurrence_count','rejected_occurrence_count','future_occurrence_count')
    projection=','.join('coalesce(s.'+n+',0)::BIGINT '+n for n in counters)
    reason_cols=','.join(f'coalesce(s._reason_{i},0)::BIGINT _reason_{i}' for i in range(len(m.REASONS)))
    db.execute(f'CREATE TEMP VIEW input_coverage AS SELECT f.*,{projection},s.first_event_time,s.last_event_time,s.min_receipt_delay_us,s.max_receipt_delay_us,{reason_cols} FROM ({file_groups}) f LEFT JOIN ({sums}) s USING(batch_fingerprint)')


def filters(name, *,service_id=None,environment=None,start=None,end=None,source_id=None,batch_id=None,limit=100,offset=0):
    m.need(name in REPORTS,'ARGUMENT_INVALID');m.integer(limit,1,1000,'ARGUMENT_INVALID');m.integer(offset,0,1_000_000,'ARGUMENT_INVALID')
    provided=dict(service_id=service_id,environment=environment,start=start,end=end,source_id=source_id,batch_id=batch_id)
    allowed={'service_id','environment','start','end'} if name=='requests_5m' else {'source_id','batch_id'} if name=='input_coverage' else {'service_id'}
    m.need(all(v is None or n in allowed for n,v in provided.items()),'ARGUMENT_INVALID')
    for n,pattern in (('service_id',m.SOURCE),('source_id',m.SOURCE),('batch_id',m.ID)):
        if provided[n] is not None:m.match(provided[n],pattern)
    if environment is not None:m.need(type(environment) is str and environment in ('dev','test','production'),'ARGUMENT_INVALID')
    public_filters={n:provided[n] for n in provided if n in allowed};params=dict(public_filters)
    where=[]
    for n in allowed-{'start','end'}:where.append(f'(${n} IS NULL OR {n}=${n})')
    if name=='requests_5m':
        low,high=m.metric_bounds(start,end);params.pop('start');params.pop('end');params.update(start_us=low,end_us=high)
        where.extend(('epoch_us(window_start)>=$start_us','epoch_us(window_start)<$end_us'))
    return public_filters,params,' AND '.join(sorted(where)),limit,offset


def coverage_totals(db,name,where,params):
    if name=='reference_coverage':
        expressions=('sum(event_count)',"sum(CASE WHEN reference_status='MATCHED' THEN event_count ELSE 0 END)",
            'sum(missing_reference_event_count)',"count(*) FILTER(WHERE reference_usage<>'UNUSED')", "count(*) FILTER(WHERE reference_usage='USED')",
            "count(*) FILTER(WHERE reference_usage='ABSENT')","count(*) FILTER(WHERE reference_usage='USED' AND len(missing_optional_attributes)>0)",
            "count(*) FILTER(WHERE reference_status='MATCHED')","count(*) FILTER(WHERE reference_usage='UNUSED')")
        row=db.execute('SELECT '+','.join('coalesce('+x+',0)' for x in expressions)+' FROM reference_coverage WHERE '+where,params).fetchone()
        return dict(zip(REF_TOTALS,row,strict=True))
    names=('input_file_count','input_byte_length','physical_occurrence_count','valid_occurrence_count','accepted_representative_count','duplicate_occurrence_count','rejected_occurrence_count')
    row=db.execute('SELECT count(*),'+','.join('coalesce(sum('+n+'),0)' for n in names)+' FROM input_coverage WHERE '+where,params).fetchone()
    distinct=db.execute(f"SELECT count(DISTINCT (service_id,event_id)) FROM {O} WHERE disposition<>'REJECTED' AND {where}",params).fetchone()[0]
    totals=dict(zip(INPUT_TOTALS[:8],row,strict=True));totals['distinct_valid_identity_count']=distinct
    totals['duplicate_excess_in_scope']=totals['valid_occurrence_count']-distinct
    return totals


def _projection(name):
    _,columns,_=REPORTS[name];result=[]
    for n in columns:
        if n=='rejected_reason_counts':result.extend(f'_reason_{i}' for i in range(len(m.REASONS)))
        elif n in TIME_COLUMNS and name=='requests_5m':result.append('epoch_us('+n+') AS '+n)
        else:result.append(n)
    return result


class ReliabilityAnalytics:
    def __init__(self,root,identity,manifest,marker,dependency,evidence,db,stats):
        self.product_fingerprint=identity;self._root=root;self._manifest=m.freeze(manifest);self._marker=marker
        self._deps=dependency;self._evidence=evidence;self._db=db;self._lock=Lock();self._verification_stats=stats

    def _check(self):self._evidence.check();self._deps.check()

    def _close(self):
        if self._db is not None:
            db,self._db=self._db,None;db.close()

    def close(self):
        m.need(self._lock.acquire(blocking=False),'BUSY')
        try:self._close()
        except Exception:raise m.ReliabilityFailure('QUERY_FAILED') from None
        finally:self._lock.release()

    def report(self,name,**kwargs):
        try:values,params,where,limit,offset=filters(name,**kwargs)
        except TypeError:raise m.ReliabilityFailure('ARGUMENT_INVALID') from None
        m.need(self._lock.acquire(blocking=False),'BUSY')
        try:
            m.need(self._db is not None,'QUERY_FAILED');self._check()
            table,columns,order=REPORTS[name];projection=_projection(name)
            sql='SELECT '+','.join(projection)+' FROM '+table+' WHERE '+where+' ORDER BY '+order+' LIMIT $limit OFFSET $offset'
            bound=params|dict(limit=limit+1,offset=offset)
            strings={'requests_5m':('service_id','environment','service_name','owner_team','service_tier','reference_status'),
                'input_coverage':('source_id','batch_id','batch_fingerprint','ingestion_id'),
                'reference_coverage':('service_id','reference_status','reference_usage','service_name','owner_team','service_tier')}[name]
            expression='+'.join("octet_length(encode(coalesce("+n+",'')))" for n in strings)
            if name=='reference_coverage':expression+="+coalesce(list_sum(list_transform(missing_optional_attributes,x->octet_length(encode(x)))),0)"
            size=self._db.execute('SELECT coalesce(sum('+expression+'),0) FROM ('+sql+') p',bound).fetchone()[0]
            m.need(size<=m.PAGE_BYTES,'RESPONSE_LIMIT')
            rows=self._db.execute(sql,bound).fetchall();m.need(len(rows)<=limit+1,'RESPONSE_LIMIT')
            fields=[f'_reason_{i}' for i in range(len(m.REASONS))] if name=='input_coverage' else []
            actual_columns=[n for n in columns if n!='rejected_reason_counts']+fields
            output=[]
            for row in rows[:limit]:
                item=dict(zip(actual_columns,row,strict=True))
                for n in TIME_COLUMNS&item.keys():
                    if item[n] is not None:item[n]=m.timestamp(item[n])
                if name=='input_coverage':item['rejected_reason_counts']={c:item.pop(f'_reason_{i}') for i,c in enumerate(m.REASONS)}
                output.append(item)
            r=self._manifest['reference']
            result=dict(status='OK',product_id=m.PRODUCT,product_fingerprint=self.product_fingerprint,origin='synthetic',label=m.LABEL,
                product_contract_version=2,processor_revision=2,input_format=self._manifest['input_format'],
                reference=dict(reference_id=r['reference_id'],revision=r['revision'],snapshot_fingerprint=r['snapshot_fingerprint']),
                report=name,filters=values,limit=limit,offset=offset,has_more=len(rows)>limit,rows=output)
            if name!='requests_5m':result['totals']=coverage_totals(self._db,name,where,params)
            result=m.public(result)
            result.update(product_contract_version=2,processor_revision=2)
            m.need(len(canonical_bytes(result))+1<=m.RESPONSE_LIMIT,'RESPONSE_LIMIT');self._check()
            return result
        except Exception as error:
            if isinstance(error,m.ReliabilityFailure) and (error.code in ('DEPENDENCY_CHANGED','PUBLICATION_CHANGED') or (error.code=='QUERY_FAILED' and self._db is None)):
                try:self._close()
                except Exception:pass
                raise
            try:self._check()
            finally:
                try:self._close()
                except Exception:pass
            if isinstance(error,m.ReliabilityFailure):raise
            raise m.ReliabilityFailure('RESOURCE_LIMIT' if isinstance(error,duckdb.OutOfMemoryException) else 'QUERY_FAILED') from None
        finally:self._lock.release()

    def __enter__(self):return self
    def __exit__(self,*args):self.close()


def _open_publication(reliability_root,product_fingerprint, *,batch_root,reference_root):
    m.match(product_fingerprint);root=m.root_path(reliability_root,'PUBLICATION_UNAVAILABLE')
    directory=m.publication_directory(root,product_fingerprint);m.root_path(directory,'PUBLICATION_UNAVAILABLE')
    try:(directory/'complete.json').lstat()
    except FileNotFoundError:raise m.ReliabilityFailure('PUBLICATION_INCOMPLETE') from None
    except OSError:raise m.ReliabilityFailure('PUBLICATION_INVALID') from None
    evidence=m.capture(directory,{'inputs.json':m.MANIFEST_LIMIT,'complete.json':m.COMPLETION_LIMIT},m.TABLES,'PUBLICATION_INVALID','PUBLICATION_CHANGED',guards=m.namespace_guards(root,directory))
    raw=evidence.raw('inputs.json');manifest=m.validate_manifest(raw)
    try:
        m.need(sha256(raw).hexdigest()==product_fingerprint)
        marker=m.validate_completion(evidence.raw('complete.json'),product_fingerprint,len(raw));m.need(marker['files']==evidence.inventory())
        r=manifest['reference']
        dependency=verify_dependencies(batch_root=batch_root,input_format=manifest['input_format'],batches=[(b['source_id'],b['batch_id'],b['batch_fingerprint']) for b in manifest['batches']],
            reference_root=reference_root,reference_id=r['reference_id'],revision=r['revision'],reference_fingerprint=r['snapshot_fingerprint'],output_root=root)
        m.need(dependency.manifest_bytes==raw,'DEPENDENCY_INVALID')
        db,stats=verify_core(dependency,evidence,marker)
        try:
            prepare_reports(db);evidence.check();dependency.check()
            return ReliabilityAnalytics(root,product_fingerprint,manifest,marker,dependency,evidence,db,stats)
        except Exception:
            try:db.close()
            except Exception:pass
            raise
    except Exception as error:
        if not isinstance(error,m.ReliabilityFailure):
            error=m.ReliabilityFailure('RESOURCE_LIMIT' if isinstance(error,duckdb.OutOfMemoryException) else 'PUBLICATION_INVALID')
        error.diagnostic['input_format']=manifest['input_format']
        raise error from None


def open_publication(reliability_root,product_fingerprint, *,batch_root,reference_root,product_contract_version=2):
    try:
        m.integer(product_contract_version,2,2,'UNSUPPORTED_VERSION')
        return _open_publication(reliability_root,product_fingerprint,batch_root=batch_root,reference_root=reference_root)
    except m.ReliabilityFailure:raise
    except Exception as error:raise m.ReliabilityFailure('RESOURCE_LIMIT' if isinstance(error,duckdb.OutOfMemoryException) else 'PUBLICATION_INVALID') from None


def main(argv=None):
    try:
        options=m.arguments(sys.argv[1:] if argv is None else argv,{'root','publication','batch-root','reference-root','report'},
            {'service-id','environment','start','end','source-id','batch-id','limit','offset'})
        if options is None:
            print('Reliability reports: --root --publication --batch-root --reference-root --report requests_5m|input_coverage|reference_coverage [approved filters]');return 0
        kwargs={k:v for k,v in options.items() if k not in ('root','publication','batch_root','reference_root','report')}
        for n in ('limit','offset'):
            if n in kwargs:
                import re
                m.need(re.fullmatch(r'0|[1-9][0-9]{0,6}',kwargs[n]) is not None,'ARGUMENT_INVALID');kwargs[n]=int(kwargs[n])
        filters(options['report'],**kwargs)
        with open_publication(options['root'],options['publication'],batch_root=options['batch_root'],reference_root=options['reference_root']) as session:
            result=session.report(options['report'],**kwargs)
        print(canonical_bytes(result).decode());return 0
    except m.ReliabilityFailure as error:return m.cli_error(error)
    except Exception:return m.cli_error(m.ReliabilityFailure('QUERY_FAILED'))


if __name__=='__main__':raise SystemExit(main())
