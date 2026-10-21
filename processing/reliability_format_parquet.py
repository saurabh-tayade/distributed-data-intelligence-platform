"""Pinned Parquet-v1 physical adapter; no Spark and no inferred source positions."""
from bisect import bisect_right
import os
import duckdb
from publication_metadata import canonical_bytes
import reliability_format_metadata as m
import reliability_inputs as jsonl
import reliability_metadata as legacy_metadata

PHYSICAL=('INT32','BYTE_ARRAY','BYTE_ARRAY','BYTE_ARRAY','INT64',
          'BYTE_ARRAY','BYTE_ARRAY','INT32','INT64','BOOLEAN')


def connection():
    m.need(duckdb.__version__=='1.4.5','UNSUPPORTED_VERSION')
    db=duckdb.connect(':memory:',config=dict(threads=2,memory_limit='512MiB',temp_directory='',
        autoinstall_known_extensions=False,autoload_known_extensions=False,allow_unsigned_extensions=False))
    db.execute("SET TimeZone='UTC'")
    return db


def validate_groups(groups,count):
    m.need(type(groups) is list and len(groups)<=64,'PARQUET_INVALID')
    start=0
    for i,g in enumerate(groups):
        m.keys(g,('row_group_ordinal','record_start','record_count',
                  'compressed_byte_length','uncompressed_byte_length'),'PARQUET_INVALID')
        for n,v in g.items():m.integer(v,code='PARQUET_INVALID')
        m.need(g['row_group_ordinal']==i and g['record_start']==start,'PARQUET_INVALID')
        m.need(g['record_count']<=32768 and g['uncompressed_byte_length']<=32*1024**2,'RESOURCE_LIMIT')
        start+=g['record_count']
    m.need(start==count,'PARQUET_INVALID')


def inspect(db,path):
    """Bound footer bytes before SQL metadata decoding; enforce the literal raw schema."""
    with path.open('rb') as stream:
        stream.seek(0,os.SEEK_END)
        m.need(stream.tell()>=12,'PARQUET_INVALID');stream.seek(0)
        m.need(stream.read(4)==b'PAR1','PARQUET_INVALID')
        stream.seek(-8,os.SEEK_END);tail=stream.read(8)
        m.need(len(tail)==8 and tail[4:]==b'PAR1','PARQUET_INVALID')
        footer=int.from_bytes(tail[:4],'little')
        m.need(footer<=1024**2,'RESOURCE_LIMIT')
        m.need(footer+12<=stream.tell(),'PARQUET_INVALID')
    cur=db.execute('SELECT * FROM parquet_schema(?)',[str(path)])
    names=[d[0] for d in cur.description];rows=[dict(zip(names,r)) for r in cur.fetchmany(12)]
    m.need(len(rows)==11 and cur.fetchone() is None,'INTAKE_NOT_APPROVED')
    m.need(rows[0]['num_children']==10,'PARQUET_INVALID')
    m.need(tuple(r['name'] for r in rows[1:])==m.EVENT_FIELDS,'INTAKE_NOT_APPROVED')
    for name,physical,row in zip(m.EVENT_FIELDS,PHYSICAL,rows[1:],strict=True):
        m.need(row['field_id'] is None,'INTAKE_NOT_APPROVED')
        m.need(row['type']==physical and row['repetition_type']=='OPTIONAL' and row['num_children'] is None,'PARQUET_INVALID')
        logical=str(row['logical_type'])
        if name=='event_time':
            m.need(row['converted_type'] in (None,'TIMESTAMP_MICROS') and 'isAdjustedToUTC=1' in logical and
                   'MICROS=MicroSeconds()' in logical and row['duckdb_type']=='TIMESTAMP WITH TIME ZONE','PARQUET_INVALID')
        elif physical=='BYTE_ARRAY':
            m.need(row['converted_type'] in (None,'UTF8') and row['logical_type'] in (None,'StringType()') and
                (row['converted_type']=='UTF8' or row['logical_type']=='StringType()') and row['duckdb_type']=='VARCHAR','PARQUET_INVALID')
        elif physical in ('INT32','INT64'):
            bits=32 if physical=='INT32' else 64
            m.need(row['converted_type'] in (None,'INT_'+str(bits)) and
                (row['logical_type'] is None or ('bitWidth='+str(bits) in logical and 'isSigned=1' in logical)), 'PARQUET_INVALID')
        else:m.need(row['converted_type'] is None and row['logical_type'] is None,'PARQUET_INVALID')
    metadata=db.execute('SELECT created_by,num_rows,num_row_groups,encryption_algorithm,footer_signing_key_metadata FROM parquet_file_metadata(?)',[str(path)]).fetchone()
    writer,count,group_count,encryption,signing=metadata
    m.need(writer=='DuckDB version v1.4.5 (build f31be57c18)' and encryption is None and signing is None,'PARQUET_INVALID')
    m.integer(count,0,m.MAX_ROWS,'RESOURCE_LIMIT');m.integer(group_count,0,64,'RESOURCE_LIMIT')
    kv=db.execute('SELECT key,value FROM parquet_kv_metadata(?)',[str(path)]).fetchmany(3)
    m.need(len(kv)<=2 and len({k for k,v in kv})==len(kv),'INTAKE_NOT_APPROVED')
    for key,value in kv:
        if key==b'ddip.logical_schema':m.need(value==b'typed-ten-v1','INTAKE_NOT_APPROVED')
        elif key==b'ddip.generator_config_sha256':
            try:m.match(value.decode('ascii'),code='INTAKE_NOT_APPROVED')
            except UnicodeError:raise m.ReliabilityFailure('INTAKE_NOT_APPROVED') from None
        else:raise m.ReliabilityFailure('INTAKE_NOT_APPROVED')
    columns=db.execute('''SELECT row_group_id,min(row_group_num_rows),count(*),
        sum(total_compressed_size),sum(total_uncompressed_size),
        count(*) FILTER(WHERE compression<>'SNAPPY' OR row_group_num_columns<>10 OR num_values<>row_group_num_rows)
        FROM parquet_metadata(?) GROUP BY row_group_id ORDER BY row_group_id''',[str(path)]).fetchmany(65)
    m.need(len(columns)==group_count,'PARQUET_INVALID')
    groups=[];start=0
    for i,n,fields,compressed,uncompressed,invalid in columns:
        m.need(i==len(groups) and fields==10 and invalid==0,'PARQUET_INVALID')
        groups.append(dict(row_group_ordinal=i,record_start=start,record_count=n,
            compressed_byte_length=compressed,uncompressed_byte_length=uncompressed));start+=n
    validate_groups(groups,count)
    return groups,count


def scan_file(path,policy,*,previous=None,unavailable='INPUT_UNAVAILABLE',changed='INPUT_CHANGED',result=None):
    before=m.observe(path,m.FILE_LIMIT,unavailable,changed,previous)
    v=m.info(path.parent,True,unavailable,changed);parent=(v.st_dev,v.st_ino,v.st_mode)
    def check():
        m.observe(path,m.FILE_LIMIT,unavailable,changed,before)
        m.info(path.parent,True,unavailable,changed,parent)
    try:
        with connection() as db:
            groups,count=inspect(db,path)
            strings=[n for n,p in zip(m.EVENT_FIELDS,PHYSICAL) if p=='BYTE_ARRAY']
            expression='+'.join("octet_length(encode(coalesce(\""+n+"\",'')))" for n in strings)
            maximum=db.execute('SELECT coalesce(max('+expression+'),0) FROM read_parquet(?,hive_partitioning=false)',[str(path)]).fetchone()[0]
            m.need(maximum<=16*1024,'RESOURCE_LIMIT')
            # Six JSON bytes per UTF-8 source byte bounds escaping before Python fetch.
            fetch=min(m.CHUNK_ROWS,max(1,m.CHUNK_BYTES//(6*maximum+1024)))
            columns=','.join('epoch_us(event_time)' if n=='event_time' else '"'+n+'"' for n in m.EVENT_FIELDS)
            db.execute('SELECT file_row_number,'+columns+',event_time IS NULL FROM read_parquet(?,file_row_number=true,hive_partitioning=false,union_by_name=false) ORDER BY file_row_number',[str(path)])
            starts=[g['record_start'] for g in groups];seen=0
            while rows:=db.fetchmany(fetch):
                m.need(len(canonical_bytes(rows))<=m.CHUNK_BYTES,'RESOURCE_LIMIT')
                for values in rows:
                    p,*values=values;time_null=values.pop()
                    m.need(type(p) is int and p==seen,'PROVENANCE_UNSUPPORTED');seen+=1
                    group=bisect_right(starts,p)-1
                    m.need(group>=0 and 0<=p-starts[group]<groups[group]['record_count'],'PROVENANCE_UNSUPPORTED')
                    event=dict(zip(m.EVENT_FIELDS,values,strict=True));us=event['event_time']
                    invalid_time=not time_null and (us is None or not m.MIN_US<=us<m.END_US)
                    event['event_time']=None if time_null else 'invalid' if invalid_time else m.timestamp(us)
                    try:parsed=jsonl.parse_event(canonical_bytes(event).decode(),policy)
                    except legacy_metadata.ReliabilityFailure as e:raise m.ReliabilityFailure(e.code) from None
                    yield dict(input_format='PARQUET',record_ordinal=p,line_number=None,byte_offset=None,
                        physical_byte_length=None,row_group_ordinal=group,row_ordinal=p-starts[group],**parsed)
            m.need(seen==count,'PARQUET_INVALID')
        check()
    except Exception as error:
        if isinstance(error,m.ReliabilityFailure) and error.code==changed:raise
        check()
        if isinstance(error,m.ReliabilityFailure):raise
        if isinstance(error,OSError):raise m.ReliabilityFailure(unavailable) from None
        raise m.ReliabilityFailure('RESOURCE_LIMIT' if isinstance(error,duckdb.OutOfMemoryException) else 'PARQUET_INVALID') from None
    if result is not None:result.update(evidence=before,record_count=count,physical_line_count=None,row_groups=groups)
