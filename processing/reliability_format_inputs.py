"""Versioned synthetic format registration and exact physical staging; no Spark."""
from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sys
import uuid

from publication_metadata import canonical_bytes
import reliability_format_metadata as m
import reliability_inputs as legacy
import reliability_metadata as legacy_metadata

PROFILE_KEYS=legacy.PROFILE_KEYS+('decoder',)
STAGE_KEYS=('batch_fingerprint','artifact_id','input_format','record_ordinal','line_number',
    'byte_offset','physical_byte_length','row_group_ordinal','row_ordinal','state',
    'recognized_service_id','recognized_event_id','semantic_sha256','reason_codes','event')


def profile(value,source_id,input_format,code='INTAKE_NOT_APPROVED'):
    m.keys(value,PROFILE_KEYS,code);m.need(type(input_format) is str and input_format in m.DECODERS,code)
    m.need(value['profile_id']==('request-jsonl-v2' if input_format=='JSONL' else 'request-parquet-v1'),code)
    m.need(value['decoder']==m.DECODERS[input_format] and type(value['decoder']['revision']) is int,code)
    m.integer(value['policy_revision'],2,2,code)
    if value['logical_dataset_id'] is not None:m.match(value['logical_dataset_id'],code=code)
    # Reuse unchanged E2 admission policies through an explicit legacy-shaped view.
    old={k:v for k,v in value.items() if k!='decoder'}
    old.update(profile_id='request-jsonl-v1',policy_revision=1,logical_dataset_id=None)
    try:legacy.profile(old,source_id,code)
    except legacy_metadata.ReliabilityFailure as e:raise m.ReliabilityFailure(e.code) from None
    m.need(len(canonical_bytes(value))<=m.PROFILE_LIMIT,'RESOURCE_LIMIT')
    return value


def _scan_file(path,policy,*,previous=None,unavailable='INPUT_UNAVAILABLE',changed='INPUT_CHANGED',result=None):
    if policy['decoder']['implementation']=='jsonl-physical-v1':
        detail={}
        try:
            for record in legacy.scan_file(path,policy,previous=previous,unavailable=unavailable,changed=changed,result=detail):
                yield dict(input_format='JSONL',record_ordinal=record['line_number']-1,
                    row_group_ordinal=None,row_ordinal=None,**record)
        except legacy_metadata.ReliabilityFailure as e:raise m.ReliabilityFailure(e.code) from None
        if result is not None:result.update(detail,record_count=detail['physical_line_count'],row_groups=None)
    else:
        from reliability_format_parquet import scan_file as scan_parquet
        yield from scan_parquet(path,policy,previous=previous,unavailable=unavailable,changed=changed,result=result)


def scan_file(path,policy,**kwargs):
    try:yield from _scan_file(path,policy,**kwargs)
    except m.ReliabilityFailure as error:
        error.diagnostic['input_format']='JSONL' if policy['decoder']==m.DECODERS['JSONL'] else 'PARQUET'
        raise


def ingestion_id(input_format,source_id,batch_id):
    return str(uuid.uuid5(uuid.NAMESPACE_URL,f'ddip:reliability:batch-v2:{input_format}:{source_id}:{batch_id}'))


def validate_manifest(raw):
    code='BATCH_INVALID'; obj=m.parse(raw,m.BATCH_MANIFEST_LIMIT,code)
    m.keys(obj,('manifest_version','batch_contract_version','request_schema_version','registrar_revision',
                'input_format','source_id','batch_id','ingestion_id','received_at','profile','files'),code)
    for name in ('manifest_version','batch_contract_version','request_schema_version','registrar_revision'):
        m.integer(obj[name],1 if name=='request_schema_version' else 2,1 if name=='request_schema_version' else 2,'UNSUPPORTED_VERSION')
    m.need(type(obj['input_format']) is str and obj['input_format'] in m.DECODERS,code)
    m.match(obj['source_id'],m.SOURCE,code); m.match(obj['batch_id'],m.ID,code)
    m.need(obj['ingestion_id']==ingestion_id(obj['input_format'],obj['source_id'],obj['batch_id']),code)
    m.time_us(obj['received_at'],code); profile(obj['profile'],obj['source_id'],obj['input_format'])
    m.need(type(obj['files']) is list and 1<=len(obj['files'])<=m.MAX_BATCH_FILES,code)
    ids=[]; bytes_total=lines=0
    for item in obj['files']:
        m.keys(item,('artifact_id','file','byte_length','sha256','record_count','physical_line_count','row_groups'),code)
        m.match(item['artifact_id'],m.ID,code)
        m.need(item['file']=='data/'+item['artifact_id']+'.'+obj['input_format'].lower(),code)
        m.integer(item['byte_length'],0,m.FILE_LIMIT,code); m.match(item['sha256'],code=code)
        m.integer(item['record_count'],0,m.MAX_ROWS,code)
        if obj['input_format']=='JSONL':m.need(item['physical_line_count']==item['record_count'] and type(item['physical_line_count']) is int and item['row_groups'] is None,code)
        else:
            from reliability_format_parquet import validate_groups
            m.need(item['physical_line_count'] is None,code);validate_groups(item['row_groups'],item['record_count'])
        ids.append(item['artifact_id']); bytes_total+=item['byte_length']; lines+=item['record_count']
    m.need(ids==sorted(set(ids)),code)
    m.need(bytes_total<=m.MAX_RAW and lines<=m.MAX_ROWS,'RESOURCE_LIMIT')
    return obj


@dataclass(frozen=True)
class Batch:
    manifest: object
    fingerprint: str
    evidence: m.Evidence

    def check(self): self.evidence.check()

    def summary(self,status):
        obj=self.manifest
        return dict(status=status,source_id=obj['source_id'],batch_id=obj['batch_id'],
                    batch_fingerprint=self.fingerprint,ingestion_id=obj['ingestion_id'],received_at=obj['received_at'],
                    input_file_count=len(obj['files']),input_byte_length=sum(f['byte_length'] for f in obj['files']),
                    record_count=sum(f['record_count'] for f in obj['files']))


def _directory(root,input_format,source_id,batch_id): return root/'v2'/('format='+input_format.lower())/('source_id='+source_id)/('batch_id='+batch_id)


def _open(root,input_format,source_id,batch_id,expected, *, pending=False,temporary=False):
    m.match(source_id,m.SOURCE); m.match(batch_id,m.ID); m.match(expected)
    root=m.root_path(root,'BATCH_UNAVAILABLE'); directory=_directory(root,input_format,source_id,batch_id)
    m.root_path(directory,'BATCH_UNAVAILABLE')
    marker='complete.tmp' if temporary else 'complete.json'
    metadata={'manifest.json':m.BATCH_MANIFEST_LIMIT}
    if not pending or temporary:
        try: (directory/marker).lstat()
        except FileNotFoundError: raise m.ReliabilityFailure('BATCH_INCOMPLETE') from None
        except OSError: raise m.ReliabilityFailure('BATCH_INVALID') from None
        metadata[marker]=m.BATCH_COMPLETION_LIMIT
    evidence=m.capture(directory,metadata,('data',),'BATCH_INVALID','DEPENDENCY_CHANGED',guards=(root,root/'v2',directory.parent.parent,directory.parent))
    raw=evidence.raw('manifest.json'); obj=validate_manifest(raw)
    m.need(obj['input_format']==input_format and sha256(raw).hexdigest()==expected and obj['source_id']==source_id and obj['batch_id']==batch_id,'BATCH_INVALID')
    files=[dict(path=f['file'],byte_length=f['byte_length'],sha256=f['sha256']) for f in obj['files']]
    m.need(files==evidence.inventory()['data'],'BATCH_INVALID')
    if not pending or temporary:
        marker_obj=m.parse(evidence.raw(marker),m.BATCH_COMPLETION_LIMIT,'BATCH_INVALID')
        m.keys(marker_obj,('completion_version','batch_fingerprint','manifest_byte_length','completed_at'),'BATCH_INVALID')
        m.integer(marker_obj['completion_version'],2,2,'UNSUPPORTED_VERSION')
        m.need(marker_obj['batch_fingerprint']==expected and marker_obj['manifest_byte_length']==len(raw),'BATCH_INVALID')
        m.need(m.time_us(marker_obj['completed_at'],'BATCH_INVALID')>=m.time_us(obj['received_at'],'BATCH_INVALID'),'BATCH_INVALID')
    by_path={e.path:e for e in evidence.files}
    for item in obj['files']:
        result={}
        for _ in scan_file(directory/item['file'],obj['profile'],previous=by_path[directory/item['file']],
                           unavailable='BATCH_INVALID',changed='DEPENDENCY_CHANGED',result=result): pass
        m.need(all(result[k]==item[k] for k in ('record_count','physical_line_count','row_groups')),'BATCH_INVALID')
    evidence.check()
    return Batch(m.freeze(obj),expected,evidence)


def open_batch(batch_root,input_format,source_id,batch_id,expected_fingerprint):
    m.need(type(input_format) is str and input_format in m.DECODERS,'ARGUMENT_INVALID')
    try:return _open(batch_root,input_format,source_id,batch_id,expected_fingerprint)
    except m.ReliabilityFailure as error:
        error.diagnostic['input_format']=input_format;raise


def _register(batch_root, *, input_root,source_id,batch_id,input_format,files,received_at,profile):
    m.need(type(input_format) is str and input_format in m.DECODERS,'ARGUMENT_INVALID')
    m.match(source_id,m.SOURCE); m.match(batch_id,m.ID); m.time_us(received_at)
    policy=json.loads(canonical_bytes(globals()['profile'](profile,source_id,input_format)))
    root=m.root_path(batch_root,'BATCH_UNAVAILABLE'); incoming=m.root_path(input_root,'INPUT_UNAVAILABLE')
    m.separate(root,incoming)
    m.need(type(files) in (list,tuple) and 1<=len(files)<=m.MAX_BATCH_FILES,'ARGUMENT_INVALID')
    ids=[]; captures=[]; inventory=[]
    root_info=m.info(root,True,'BATCH_UNAVAILABLE','DEPENDENCY_CHANGED'); root_id=(root_info.st_dev,root_info.st_ino,root_info.st_mode)
    incoming_info=m.info(incoming,True,'INPUT_UNAVAILABLE','INPUT_CHANGED'); incoming_id=(incoming_info.st_dev,incoming_info.st_ino,incoming_info.st_mode)
    for item in files:
        m.keys(item,('artifact_id','input_file'),'ARGUMENT_INVALID'); m.match(item['artifact_id'],m.ID)
        m.need(item['input_file']==item['artifact_id']+'.'+input_format.lower(),'PATH_UNSAFE'); ids.append(item['artifact_id'])
    m.need(len(ids)==len(set(ids)),'ARGUMENT_INVALID')
    for item in sorted(files,key=lambda f:f['artifact_id']):
        result={}
        for _ in scan_file(incoming/item['input_file'],policy,result=result): pass
        ev=result['evidence']; captures.append(ev)
        inventory.append(dict(artifact_id=item['artifact_id'],file='data/'+item['input_file'],byte_length=ev.length,
                              sha256=ev.digest,record_count=result['record_count'],physical_line_count=result['physical_line_count'],row_groups=result['row_groups']))
        m.need(sum(f['byte_length'] for f in inventory)<=m.MAX_RAW and sum(f['record_count'] for f in inventory)<=m.MAX_ROWS,'RESOURCE_LIMIT')
    obj=dict(manifest_version=2,batch_contract_version=2,request_schema_version=1,registrar_revision=2,input_format=input_format,
             source_id=source_id,batch_id=batch_id,ingestion_id=ingestion_id(input_format,source_id,batch_id),received_at=received_at,profile=policy,files=inventory)
    raw=canonical_bytes(obj); validate_manifest(raw); fingerprint=sha256(raw).hexdigest()
    directory=_directory(root,input_format,source_id,batch_id)
    def check_inputs():
        m.info(root,True,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',root_id)
        m.info(incoming,True,'INPUT_UNAVAILABLE','INPUT_CHANGED',incoming_id)
        for capture in captures: m.observe(capture.path,m.FILE_LIMIT,'INPUT_UNAVAILABLE','INPUT_CHANGED',capture)
    check_inputs()
    try:
        directory.lstat()
    except FileNotFoundError: existing=False
    except OSError: raise m.ReliabilityFailure('BATCH_INVALID') from None
    else: existing=True
    if existing:
        m.info(directory,True,'BATCH_INVALID','DEPENDENCY_CHANGED')
        if not (directory/'complete.json').exists(): raise m.ReliabilityFailure('BATCH_INCOMPLETE')
        # Verify the old binding, not the candidate identity, before conflict classification.
        old=m.observe(directory/'manifest.json',m.BATCH_MANIFEST_LIMIT,'BATCH_INVALID','DEPENDENCY_CHANGED',retain=True)
        prior=_open(root,input_format,source_id,batch_id,sha256(old.raw).hexdigest())
        m.need(prior.fingerprint==fingerprint,'BATCH_CONFLICT'); check_inputs()
        return m.freeze(prior.summary('replayed'))
    try:
        for parent in (root/'v2',directory.parent.parent,directory.parent):
            try:parent.mkdir()
            except FileExistsError:pass
            m.info(parent,True,'PUBLICATION_FAILED','DEPENDENCY_CHANGED')
        m.info(directory.parent,True,'PUBLICATION_FAILED','DEPENDENCY_CHANGED')
        directory.mkdir(); (directory/'data').mkdir()
        parent=m.info(directory.parent,True,'PUBLICATION_FAILED','DEPENDENCY_CHANGED')
        guards=((root,root_id),(directory.parent,(parent.st_dev,parent.st_ino,parent.st_mode)))
        def guard():
            check_inputs()
            for path,identity in guards: m.info(path,True,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',identity)
        m.write(directory/'manifest.json',raw)
        for capture,item in zip(captures,inventory,strict=True):
            guard()
            with (directory/item['file']).open('xb') as sink:
                m.observe(capture.path,m.FILE_LIMIT,'INPUT_UNAVAILABLE','INPUT_CHANGED',capture,sink=sink)
        batch=_open(root,input_format,source_id,batch_id,fingerprint,pending=True); guard(); batch.check()
        completion=canonical_bytes(dict(completion_version=2,batch_fingerprint=fingerprint,manifest_byte_length=len(raw),completed_at=m.now()))
        m.write(directory/'complete.tmp',completion)
        checked=_open(root,input_format,source_id,batch_id,fingerprint,pending=True,temporary=True)
        guard(); checked.check(); m.need(not (directory/'complete.json').exists(),'DEPENDENCY_CHANGED')
        (directory/'complete.tmp').rename(directory/'complete.json')
        return m.freeze(batch.summary('registered'))
    except m.ReliabilityFailure as error:
        if error.code in ('INPUT_CHANGED','DEPENDENCY_CHANGED','INPUT_UNAVAILABLE','PATH_UNSAFE','RESOURCE_LIMIT'): raise
        raise m.ReliabilityFailure('PUBLICATION_FAILED') from None
    except OSError: raise m.ReliabilityFailure('PUBLICATION_FAILED') from None


def register(batch_root, *,input_root,source_id,batch_id,input_format,files,received_at,profile):
    try:return _register(batch_root,input_root=input_root,source_id=source_id,batch_id=batch_id,
        input_format=input_format,files=files,received_at=received_at,profile=profile)
    except m.ReliabilityFailure as error:
        error.diagnostic['input_format']=input_format if type(input_format) is str and input_format in m.DECODERS else None
        raise


def select(batch_root,input_format,selections):
    m.need(type(selections) in (list,tuple) and 1<=len(selections)<=m.MAX_BATCHES,'ARGUMENT_INVALID')
    unique=set(); bindings={}
    for item in selections:
        m.need(type(item) in (list,tuple) and len(item)==3,'ARGUMENT_INVALID')
        source,batch,fingerprint=item; m.match(source,m.SOURCE); m.match(batch,m.ID); m.match(fingerprint)
        m.need((source,batch) not in bindings or bindings[source,batch]==fingerprint,'BATCH_CONFLICT')
        bindings[source,batch]=fingerprint; unique.add(tuple(item))
    batches=tuple(open_batch(batch_root,input_format,*item) for item in sorted(unique))
    m.need(sum(len(b.manifest['files']) for b in batches)<=m.MAX_INPUT_FILES,'RESOURCE_LIMIT')
    m.need(sum(f['byte_length'] for b in batches for f in b.manifest['files'])<=m.MAX_RAW,'RESOURCE_LIMIT')
    m.need(sum(f['record_count'] for b in batches for f in b.manifest['files'])<=m.MAX_ROWS,'RESOURCE_LIMIT')
    return batches,len(selections)-len(unique)


def stage(batches,staging_root,product_fingerprint):
    root=m.root_path(staging_root,'INPUT_UNAVAILABLE'); m.match(product_fingerprint)
    directory=root/('attempt='+str(uuid.uuid4())); directory.mkdir(mode=0o700)
    guards=tuple((p,(v.st_dev,v.st_ino,v.st_mode)) for p in (root,directory) for v in (m.info(p,True,'PUBLICATION_FAILED','DEPENDENCY_CHANGED'),))
    files=[]; captures=[]; total=0
    for bi,batch in enumerate(batches,1):
        batch.check(); originals={f.path:f for f in batch.evidence.files}
        for fi,item in enumerate(batch.manifest['files'],1):
            path=directory/f'stage-{bi:02d}-{fi:02d}.jsonl'; count=0
            with path.open('xb') as stream:
                for row in scan_file(batch.evidence.directory/item['file'],batch.manifest['profile'],previous=originals[batch.evidence.directory/item['file']],unavailable='DEPENDENCY_INVALID',changed='DEPENDENCY_CHANGED'):
                    row=dict(batch_fingerprint=batch.fingerprint,artifact_id=item['artifact_id'],**row)
                    raw=canonical_bytes(row)+b'\n'; m.need(len(raw)<=m.STAGE_RECORD_LIMIT,'RESOURCE_LIMIT')
                    total+=len(raw); count+=1; m.need(total<=m.MAX_STAGE_BYTES,'RESOURCE_LIMIT')
                    m.need(stream.write(raw)==len(raw),'PUBLICATION_FAILED')
            ev=m.observe(path,m.MAX_STAGE_BYTES,'PUBLICATION_FAILED','DEPENDENCY_CHANGED')
            captures.append(ev)
            files.append(dict(file=path.name,byte_length=ev.length,sha256=ev.digest,row_count=count,batch_fingerprint=batch.fingerprint,artifact_id=item['artifact_id']))
        batch.check()
    raw=canonical_bytes(dict(staging_version=2,product_fingerprint=product_fingerprint,files=files))
    m.need(len(raw)<=m.STAGE_MANIFEST_LIMIT,'RESOURCE_LIMIT'); m.write(directory/'stage.json',raw)
    metadata=m.observe(directory/'stage.json',m.STAGE_MANIFEST_LIMIT,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',retain=True)
    result=Staging(directory,m.freeze(files),metadata,tuple(captures),guards)
    result.check()
    return result


def validate_stage(raw):
    """Strict safe transport schema before Spark's JSON decoder can coerce/ignore fields."""
    def pairs(items):
        result={}
        for k,v in items:m.need(k not in result);result[k]=v
        return result
    def invalid(_):raise m.ReliabilityFailure('PUBLICATION_INVALID')
    try:
        row=json.loads(raw,object_pairs_hook=pairs,parse_float=invalid,parse_constant=invalid)
        m.need(canonical_bytes(row)==raw);m.keys(row,STAGE_KEYS)
        m.match(row['batch_fingerprint'],code='PUBLICATION_INVALID');m.match(row['artifact_id'],m.ID,'PUBLICATION_INVALID')
        m.need(row['input_format'] in ('JSONL','PARQUET'))
        m.integer(row['record_ordinal'],0,m.MAX_ROWS-1)
        for n in ('line_number','byte_offset','physical_byte_length','row_group_ordinal','row_ordinal'):
            if row[n] is not None:m.integer(row[n])
        if row['input_format']=='JSONL':
            m.need(row['line_number']==row['record_ordinal']+1 and row['byte_offset'] is not None and
                type(row['physical_byte_length']) is int and 1<=row['physical_byte_length']<=m.LINE_LIMIT+2 and
                row['row_group_ordinal'] is None and row['row_ordinal'] is None)
        else:m.need(all(row[n] is None for n in ('line_number','byte_offset','physical_byte_length')) and
                    row['row_group_ordinal'] is not None and row['row_ordinal'] is not None)
        service,event=row['recognized_service_id'],row['recognized_event_id']
        m.need((service is None)==(event is None))
        if service is not None:m.match(service,m.SOURCE,'PUBLICATION_INVALID');m.match(event,m.UUID,'PUBLICATION_INVALID')
        m.need(type(row['reason_codes']) is list and row['reason_codes']==[c for c in m.REASONS if c in row['reason_codes']])
        if row['state']=='REJECTED':m.need(row['event'] is None and row['semantic_sha256'] is None and bool(row['reason_codes']))
        else:
            m.need(row['state']=='VALID' and not row['reason_codes']);m.keys(row['event'],m.TRANSPORT_FIELDS)
            m.match(row['semantic_sha256'],code='PUBLICATION_INVALID')
            data=row['event']
            for n in m.TRANSPORT_FIELDS:
                expected=bool if n=='is_synthetic' else int if n in ('schema_version','event_time_epoch_us','status_code','duration_ms') else str
                m.need(type(data[n]) is expected)
            m.need(data['service_id']==service and data['event_id']==event and
                sha256(canonical_bytes([data[n] for n in m.TRANSPORT_FIELDS])).hexdigest()==row['semantic_sha256'])
        return row
    except (ValueError,TypeError,UnicodeError,RecursionError):raise m.ReliabilityFailure('PUBLICATION_INVALID') from None


def main(argv=None):
    try:
        argv=sys.argv[1:] if argv is None else argv
        if argv==['--help']:
            print('Reliability batch: register|verify [fixed batch/profile options]'); return 0
        m.need(bool(argv) and argv[0] in ('register','verify'),'ARGUMENT_INVALID')
        required={'root','input-format','source-id','batch-id','expected-fingerprint'} if argv[0]=='verify' else {'root','input-root','input-format','source-id','batch-id','file','received-at','profile'}
        options=m.arguments(argv[1:],required,repeat=('file',))
        if argv[0]=='verify':
            root=options.pop('root'); result=open_batch(root,**options).summary('verified')
        else:
            config_path=Path(options.pop('profile')).absolute()
            capture=m.observe(config_path,m.PROFILE_LIMIT,'INPUT_UNAVAILABLE','INPUT_CHANGED',retain=True)
            policy=m.parse(capture.raw,m.PROFILE_LIMIT,'INTAKE_NOT_APPROVED')
            options['profile']=policy
            options['files']=[dict(artifact_id=i,input_file=i+'.'+options['input_format'].lower()) for i in options.pop('file')]
            root=options.pop('root'); result=register(root,**options)
            m.observe(config_path,m.PROFILE_LIMIT,'INPUT_UNAVAILABLE','INPUT_CHANGED',capture)
        output=canonical_bytes(m.public(result)); m.need(len(output)+1<=4096,'RESPONSE_LIMIT'); print(output.decode()); return 0
    except m.ReliabilityFailure as error: return m.cli_error(error)
    except Exception: return m.cli_error(m.ReliabilityFailure('PUBLICATION_FAILED'))




@dataclass(frozen=True)
class Staging:
    directory: Path
    files: tuple
    metadata: m.FileEvidence
    captures: tuple
    guards: tuple

    def __iter__(self): return iter((self.directory,self.files))

    def check(self):
        for path,identity in self.guards: m.info(path,True,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',identity)
        m.names(self.directory,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',tuple(sorted(['stage.json',*(f['file'] for f in self.files)])))
        m.observe(self.metadata.path,m.STAGE_MANIFEST_LIMIT,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',self.metadata,retain=True)
        obj=m.parse(self.metadata.raw,m.STAGE_MANIFEST_LIMIT,'PUBLICATION_INVALID')
        m.keys(obj,('staging_version','product_fingerprint','files'))
        m.integer(obj['staging_version'],2,2,'UNSUPPORTED_VERSION');m.match(obj['product_fingerprint'])
        m.need(obj['files']==[dict(f) for f in self.files])
        m.need(type(obj['files']) is list and 1<=len(obj['files'])<=m.MAX_INPUT_FILES)
        for item in obj['files']:
            m.keys(item,('file','byte_length','sha256','row_count','batch_fingerprint','artifact_id'))
            m.match(item['file'],re.compile(r'stage-[0-9]{2}-[0-9]{2}[.]jsonl'),'PUBLICATION_INVALID')
            m.integer(item['byte_length'],0,m.MAX_STAGE_BYTES);m.integer(item['row_count'],0,m.MAX_ROWS)
            m.match(item['sha256'],code='PUBLICATION_INVALID');m.match(item['batch_fingerprint'],code='PUBLICATION_INVALID')
            m.match(item['artifact_id'],m.ID,'PUBLICATION_INVALID')
        m.need(len({f['file'] for f in obj['files']})==len(obj['files']) and sum(f['byte_length'] for f in obj['files'])<=m.MAX_STAGE_BYTES)
        for ev,item in zip(self.captures,self.files,strict=True):
            m.need(ev.length==item['byte_length'] and ev.digest==item['sha256'],'DEPENDENCY_CHANGED')
            m.observe(ev.path,m.MAX_STAGE_BYTES,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',ev)
            count=0
            try:
                with ev.path.open('rb') as stream:
                    while raw:=stream.readline(m.STAGE_RECORD_LIMIT+1):
                        m.need(len(raw)<=m.STAGE_RECORD_LIMIT and raw.endswith(b'\n'))
                        row=validate_stage(raw[:-1]);count+=1
                        m.need(row['batch_fingerprint']==item['batch_fingerprint'] and row['artifact_id']==item['artifact_id'])
                m.need(count==item['row_count'])
            except OSError:
                m.info(ev.path,False,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',ev.stamp)
                raise m.ReliabilityFailure('PUBLICATION_FAILED') from None
            m.observe(ev.path,m.MAX_STAGE_BYTES,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',ev)


def dependency_failure(error):
    code={'BATCH_UNAVAILABLE':'DEPENDENCY_UNAVAILABLE','SNAPSHOT_UNAVAILABLE':'DEPENDENCY_UNAVAILABLE',
          'INPUT_UNAVAILABLE':'DEPENDENCY_UNAVAILABLE','BATCH_INVALID':'DEPENDENCY_INVALID',
          'PARQUET_INVALID':'DEPENDENCY_INVALID',
          'BATCH_INCOMPLETE':'DEPENDENCY_INVALID','SNAPSHOT_INVALID':'DEPENDENCY_INVALID',
          'SNAPSHOT_INCOMPLETE':'DEPENDENCY_INVALID','IDENTITY_MISMATCH':'DEPENDENCY_INVALID',
          'CSV_INVALID':'DEPENDENCY_INVALID','VOCABULARY_INVALID':'DEPENDENCY_INVALID',
          'SNAPSHOT_CHANGED':'DEPENDENCY_CHANGED','INPUT_CHANGED':'DEPENDENCY_CHANGED'}.get(error.code,error.code)
    return m.ReliabilityFailure(code)


@dataclass(frozen=True)
class Dependencies:
    batch_root: Path
    reference_root: Path
    batches: tuple
    reference: object
    reference_evidence: m.Evidence
    duplicate_batch_selections: int

    def check(self):
        import service_reference
        try:
            for batch in self.batches: batch.check()
            self.reference_evidence.check()
            current=service_reference.open_snapshot(self.reference_root,self.reference.reference_id,
                      self.reference.revision,self.reference.fingerprint)
            m.need(current==self.reference,'DEPENDENCY_CHANGED')
        except (m.ReliabilityFailure,legacy_metadata.ReliabilityFailure) as error: raise dependency_failure(error) from None

    def manifest(self):
        return dict(manifest_version=2,product_contract_version=2,processor_revision=2,input_format=self.batches[0].manifest['input_format'],product_id=m.PRODUCT,
            input_contract_versions=dict(registered_batch=2,request_event=1,service_reference=1),
            dataset_contract_versions={m.EVENTS:1,m.OCCURRENCES:2,m.METRICS:1},engine={'spark':'4.0.2'},origin='synthetic',
            batches=[dict(source_id=b.manifest['source_id'],batch_id=b.manifest['batch_id'],
                batch_fingerprint=b.fingerprint,completion=m.descriptor(b.evidence.raw('complete.json'))) for b in self.batches],
            reference=dict(reference_id=self.reference.reference_id,revision=self.reference.revision,
                snapshot_fingerprint=self.reference.fingerprint,completion=m.descriptor(self.reference_evidence.raw('complete.json'))),
            semantic_config=json.loads(canonical_bytes(m.semantics(self.batches[0].manifest['input_format']))))

    @property
    def manifest_bytes(self): return canonical_bytes(self.manifest())

    @property
    def fingerprint(self): return sha256(self.manifest_bytes).hexdigest()


def verify_dependencies(*,batch_root,input_format,batches,reference_root,reference_id,revision,reference_fingerprint,output_root=None,staging_root=None):
    import service_reference
    m.match(reference_id,m.ID);m.match(revision,m.REVISION);m.match(reference_fingerprint)
    root=m.root_path(batch_root);refs=m.root_path(reference_root)
    others=[m.root_path(r,'PUBLICATION_UNAVAILABLE') for r in (output_root,staging_root) if r is not None]
    m.separate(root,refs,*others)
    try:
        selected,duplicates=select(root,input_format,batches)
        m.need(all(canonical_bytes(m.public(b.manifest['profile']))==canonical_bytes(m.public(selected[0].manifest['profile'])) for b in selected),'BATCH_CONFLICT')
        directory=refs/('reference_id='+reference_id)/('revision='+revision)
        m.root_path(directory)
        evidence=m.capture(directory,{'reference.csv':10*1024**2,'vocabulary.json':32*1024,'manifest.json':16384,'complete.json':4096},(),
            'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',guards=(refs,directory.parent))
        reference=service_reference.open_snapshot(refs,reference_id,revision,reference_fingerprint)
        evidence.check()
        result=Dependencies(root,refs,selected,reference,evidence,duplicates)
        m.need(len(result.manifest_bytes)<=m.MANIFEST_LIMIT,'RESOURCE_LIMIT');result.check()
        return result
    except (m.ReliabilityFailure,legacy_metadata.ReliabilityFailure) as error: raise dependency_failure(error) from None


if __name__=='__main__': raise SystemExit(main())
