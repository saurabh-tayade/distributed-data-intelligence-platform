"""Bounded synthetic JSONL registration and exact physical indexing (no Spark)."""
from dataclasses import dataclass
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import sys
import uuid

from publication_metadata import canonical_bytes
import reliability_metadata as m

PROFILE_KEYS=('profile_id','schema_version','config_revision','origin','synthetic_source_id',
              'logical_dataset_id','allowed_service_ids','allowed_route_ids','policy_revision')
STAGE_KEYS=('batch_fingerprint','artifact_id','line_number','byte_offset','physical_byte_length',
            'state','recognized_service_id','recognized_event_id','semantic_sha256','reason_codes','event')
METHODS=('GET','HEAD','POST','PUT','PATCH','DELETE','OPTIONS')
ENVIRONMENTS=('dev','test','production')


def profile(value,source_id,code='INTAKE_NOT_APPROVED'):
    m.keys(value,PROFILE_KEYS,code)
    for key in ('schema_version','config_revision','policy_revision'):
        m.integer(value[key],1,1,code)
    m.need(value['profile_id']=='request-jsonl-v1' and value['origin']=='synthetic' and
           value['synthetic_source_id']==source_id,code)
    m.match(source_id,m.SOURCE,code)
    if value['logical_dataset_id'] is not None: m.match(value['logical_dataset_id'],m.ID,code)
    for name,cap,pattern in (('allowed_service_ids',128,m.SOURCE),('allowed_route_ids',256,m.ID)):
        values=value[name]
        m.need(type(values) is list and 1<=len(values)<=cap,code)
        for item in values: m.match(item,pattern,code)
        m.need(values==sorted(set(values)),code)
    m.need(len(canonical_bytes(value))<=m.PROFILE_LIMIT,'RESOURCE_LIMIT')
    return value


class Pairs(list): pass
class Fraction: pass
class Nonfinite: pass


def _lexical(text):
    """Allocation guard, not a substitute JSON parser or identity extractor."""
    quoted=False; escaped=False; depth=0; members=0; i=0
    while i<len(text):
        c=text[i]
        if quoted:
            if escaped: escaped=False
            elif c=='\\': escaped=True
            elif c=='"': quoted=False
        elif c=='"': quoted=True
        elif c in '{[':
            depth+=1; m.need(depth<=4,'BATCH_INVALID')
        elif c in '}]': depth-=1
        elif c==':':
            members+=1; m.need(members<=32,'BATCH_INVALID')
        elif c in '-0123456789':
            found=re.match(r'-?(?:0|[1-9][0-9]*)(?:[.][0-9]+)?(?:[eE][+-]?[0-9]+)?',text[i:])
            if found:
                m.need(len(found[0])<=64,'BATCH_INVALID'); i+=len(found[0])-1
        i+=1


def _privacy(value,policy):
    if isinstance(value,Pairs):
        for name,item in value:
            m.need(name in m.EVENT_FIELDS,'INTAKE_NOT_APPROVED')
            if name in ('service_id','route_id') and type(item) is str:
                key='allowed_service_ids' if name=='service_id' else 'allowed_route_ids'
                m.need(item in policy[key],'INTAKE_NOT_APPROVED')
            if name=='is_synthetic':
                m.need(item is not False and not (type(item) is str and item in ('false','operational','real','production')),'INTAKE_NOT_APPROVED')
            _privacy(item,policy)
    elif type(value) is list:
        for item in value: _privacy(item,policy)


def parse_event(text,policy):
    empty=dict(state='REJECTED',recognized_service_id=None,recognized_event_id=None,
               semantic_sha256=None,reason_codes=[],event=None)
    def rejected(code): return empty|{'reason_codes':[code]}
    if not text.strip(): return rejected('BLANK_LINE')
    _lexical(text)
    try:
        value=json.loads(text,object_pairs_hook=Pairs,parse_float=lambda _:Fraction(),parse_constant=lambda _:Nonfinite())
    except (ValueError,RecursionError): return rejected('JSON_SYNTAX')
    _privacy(value,policy)
    if not isinstance(value,Pairs): return rejected('JSON_NOT_OBJECT')
    items={}
    for name,item in value: items.setdefault(name,[]).append(item)
    m.need(all(len(items.get(n,[]))<=1 for n in ('service_id','event_id')),'BATCH_INVALID')
    for item in items.get('schema_version',[]):
        m.need(not(type(item) is int and item!=1),'BATCH_INVALID')
    reasons=set()
    if any(len(values)>1 for values in items.values()): reasons.add('JSON_DUPLICATE_KEY')
    data={name:values[0] for name,values in items.items() if len(values)==1}
    service,event=data.get('service_id'),data.get('event_id')
    if type(service) is str and m.SOURCE.fullmatch(service) and type(event) is str and m.UUID.fullmatch(event):
        empty.update(recognized_service_id=service,recognized_event_id=event)
    result={}
    for name in m.EVENT_FIELDS:
        if name not in items: reasons.add('FIELD_MISSING'); continue
        if len(items[name])!=1: continue
        item=data[name]
        if item is None: reasons.add('FIELD_NULL'); continue
        if isinstance(item,Nonfinite): reasons.add('JSON_NONFINITE'); continue
        expected=bool if name=='is_synthetic' else int if name in ('schema_version','status_code','duration_ms') else str
        if type(item) is not expected: reasons.add('FIELD_TYPE'); continue
        valid=True
        if name=='event_time':
            try: item=m.time_us(item,'BATCH_INVALID')
            except m.ReliabilityFailure: reasons.add('TIMESTAMP_INVALID'); continue
            result['event_time_epoch_us']=item; continue
        if name=='schema_version': valid=item==1
        elif name=='event_id': valid=m.UUID.fullmatch(item) is not None
        elif name=='service_id': valid=m.SOURCE.fullmatch(item) is not None
        elif name=='environment': valid=item in ENVIRONMENTS
        elif name=='method': valid=item in METHODS
        elif name=='route_id': valid=m.ID.fullmatch(item) is not None
        elif name=='status_code': valid=100<=item<=599
        elif name=='duration_ms': valid=0<=item<=3_600_000
        elif name=='is_synthetic': valid=item is True
        if not valid: reasons.add('FIELD_VALUE')
        result[name]=item
    if reasons:
        return empty|{'reason_codes':[c for c in m.REASONS if c in reasons]}
    payload=canonical_bytes([result[n] for n in m.TRANSPORT_FIELDS])
    return empty|dict(state='VALID',reason_codes=[],event=result,semantic_sha256=sha256(payload).hexdigest())


def scan_file(path,policy, *, previous=None, unavailable='INPUT_UNAVAILABLE',changed='INPUT_CHANGED',result=None):
    """Yield safe records only; exhausting the iterator is required for success."""
    before=m.info(path,False,unavailable,changed,previous.stamp if previous else None)
    m.need(before.st_size<=m.FILE_LIMIT,'RESOURCE_LIMIT')
    parent=m.info(path.parent,True,unavailable,changed)
    parent_id=(parent.st_dev,parent.st_ino,parent.st_mode)
    digest=sha256(); offset=number=0; complete=False
    try:
        fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW)
        try: handle=os.fdopen(fd,'rb')
        except BaseException:
            os.close(fd); raise
        with handle as stream:
            m.need(m.stamp(os.fstat(stream.fileno()))==m.stamp(before),changed)
            while raw:=stream.readline(m.LINE_LIMIT+3):
                number+=1; m.need(number<=m.MAX_ROWS,'RESOURCE_LIMIT')
                digest.update(raw); length=len(raw)
                m.need(offset+length<=m.FILE_LIMIT,'RESOURCE_LIMIT')
                content=raw[:-2] if raw.endswith(b'\r\n') else raw[:-1] if raw.endswith(b'\n') else raw
                m.need(len(content)<=m.LINE_LIMIT,'RESOURCE_LIMIT')
                m.need(b'\r' not in content and not(number==1 and content.startswith(b'\xef\xbb\xbf')),'BATCH_INVALID')
                try: text=content.decode('utf-8')
                except UnicodeError: raise m.ReliabilityFailure('BATCH_INVALID') from None
                yield dict(line_number=number,byte_offset=offset,physical_byte_length=length,**parse_event(text,policy))
                offset+=length
            complete=True
            m.need(m.stamp(os.fstat(stream.fileno()))==m.stamp(before),changed)
        m.info(path,False,unavailable,changed,m.stamp(before)); m.info(path.parent,True,unavailable,changed,parent_id)
        if previous: m.need((offset,digest.hexdigest())==(previous.length,previous.digest),changed)
    except OSError:
        if previous and complete: m.need((offset,digest.hexdigest())==(previous.length,previous.digest),changed)
        m.info(path,False,unavailable,changed,m.stamp(before)); m.info(path.parent,True,unavailable,changed,parent_id)
        raise m.ReliabilityFailure(unavailable) from None
    evidence=m.FileEvidence(path,m.stamp(before),offset,digest.hexdigest(),None)
    if result is not None: result.update(evidence=evidence,physical_line_count=number)


def ingestion_id(source_id,batch_id):
    return str(uuid.uuid5(uuid.NAMESPACE_URL,f'ddip:reliability:batch-v1:{source_id}:{batch_id}'))


def validate_manifest(raw):
    code='BATCH_INVALID'; obj=m.parse(raw,m.BATCH_MANIFEST_LIMIT,code)
    m.keys(obj,('manifest_version','batch_contract_version','request_schema_version','registrar_revision',
                'source_id','batch_id','ingestion_id','received_at','profile','files'),code)
    for name in ('manifest_version','batch_contract_version','request_schema_version','registrar_revision'):
        m.integer(obj[name],1,1,'UNSUPPORTED_VERSION')
    m.match(obj['source_id'],m.SOURCE,code); m.match(obj['batch_id'],m.ID,code)
    m.need(obj['ingestion_id']==ingestion_id(obj['source_id'],obj['batch_id']),code)
    m.time_us(obj['received_at'],code); profile(obj['profile'],obj['source_id'])
    m.need(type(obj['files']) is list and 1<=len(obj['files'])<=m.MAX_BATCH_FILES,code)
    ids=[]; bytes_total=lines=0
    for item in obj['files']:
        m.keys(item,('artifact_id','file','byte_length','sha256','physical_line_count'),code)
        m.match(item['artifact_id'],m.ID,code)
        m.need(item['file']=='data/'+item['artifact_id']+'.jsonl',code)
        m.integer(item['byte_length'],0,m.FILE_LIMIT,code); m.match(item['sha256'],code=code)
        m.integer(item['physical_line_count'],0,m.MAX_ROWS,code)
        ids.append(item['artifact_id']); bytes_total+=item['byte_length']; lines+=item['physical_line_count']
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
                    physical_line_count=sum(f['physical_line_count'] for f in obj['files']))


def _directory(root,source_id,batch_id): return root/('source_id='+source_id)/('batch_id='+batch_id)


def _open(root,source_id,batch_id,expected, *, pending=False,temporary=False):
    m.match(source_id,m.SOURCE); m.match(batch_id,m.ID); m.match(expected)
    root=m.root_path(root,'BATCH_UNAVAILABLE'); directory=_directory(root,source_id,batch_id)
    m.root_path(directory,'BATCH_UNAVAILABLE')
    marker='complete.tmp' if temporary else 'complete.json'
    metadata={'manifest.json':m.BATCH_MANIFEST_LIMIT}
    if not pending or temporary:
        try: (directory/marker).lstat()
        except FileNotFoundError: raise m.ReliabilityFailure('BATCH_INCOMPLETE') from None
        except OSError: raise m.ReliabilityFailure('BATCH_INVALID') from None
        metadata[marker]=m.BATCH_COMPLETION_LIMIT
    evidence=m.capture(directory,metadata,('data',),'BATCH_INVALID','DEPENDENCY_CHANGED',guards=(root,directory.parent))
    raw=evidence.raw('manifest.json'); obj=validate_manifest(raw)
    m.need(sha256(raw).hexdigest()==expected and obj['source_id']==source_id and obj['batch_id']==batch_id,'BATCH_INVALID')
    files=[dict(path=f['file'],byte_length=f['byte_length'],sha256=f['sha256']) for f in obj['files']]
    m.need(files==evidence.inventory()['data'],'BATCH_INVALID')
    if not pending or temporary:
        marker_obj=m.parse(evidence.raw(marker),m.BATCH_COMPLETION_LIMIT,'BATCH_INVALID')
        m.keys(marker_obj,('completion_version','batch_fingerprint','manifest_byte_length','completed_at'),'BATCH_INVALID')
        m.integer(marker_obj['completion_version'],1,1,'UNSUPPORTED_VERSION')
        m.need(marker_obj['batch_fingerprint']==expected and marker_obj['manifest_byte_length']==len(raw),'BATCH_INVALID')
        m.need(m.time_us(marker_obj['completed_at'],'BATCH_INVALID')>=m.time_us(obj['received_at'],'BATCH_INVALID'),'BATCH_INVALID')
    by_path={e.path:e for e in evidence.files}
    for item in obj['files']:
        result={}
        for _ in scan_file(directory/item['file'],obj['profile'],previous=by_path[directory/item['file']],
                           unavailable='BATCH_INVALID',changed='DEPENDENCY_CHANGED',result=result): pass
        m.need(result['physical_line_count']==item['physical_line_count'],'BATCH_INVALID')
    evidence.check()
    return Batch(m.freeze(obj),expected,evidence)


def open_batch(batch_root,source_id,batch_id,expected_fingerprint):
    return _open(batch_root,source_id,batch_id,expected_fingerprint)


def register(batch_root, *, input_root,source_id,batch_id,files,received_at,profile):
    m.match(source_id,m.SOURCE); m.match(batch_id,m.ID); m.time_us(received_at)
    policy=json.loads(canonical_bytes(globals()['profile'](profile,source_id)))
    root=m.root_path(batch_root,'BATCH_UNAVAILABLE'); incoming=m.root_path(input_root,'INPUT_UNAVAILABLE')
    m.separate(root,incoming)
    m.need(type(files) in (list,tuple) and 1<=len(files)<=m.MAX_BATCH_FILES,'ARGUMENT_INVALID')
    ids=[]; captures=[]; inventory=[]
    root_info=m.info(root,True,'BATCH_UNAVAILABLE','DEPENDENCY_CHANGED'); root_id=(root_info.st_dev,root_info.st_ino,root_info.st_mode)
    incoming_info=m.info(incoming,True,'INPUT_UNAVAILABLE','INPUT_CHANGED'); incoming_id=(incoming_info.st_dev,incoming_info.st_ino,incoming_info.st_mode)
    for item in files:
        m.keys(item,('artifact_id','input_file'),'ARGUMENT_INVALID'); m.match(item['artifact_id'],m.ID)
        m.need(item['input_file']==item['artifact_id']+'.jsonl','PATH_UNSAFE'); ids.append(item['artifact_id'])
    m.need(len(ids)==len(set(ids)),'ARGUMENT_INVALID')
    for item in sorted(files,key=lambda f:f['artifact_id']):
        result={}
        for _ in scan_file(incoming/item['input_file'],policy,result=result): pass
        ev=result['evidence']; captures.append(ev)
        inventory.append(dict(artifact_id=item['artifact_id'],file='data/'+item['input_file'],byte_length=ev.length,
                              sha256=ev.digest,physical_line_count=result['physical_line_count']))
        m.need(sum(f['byte_length'] for f in inventory)<=m.MAX_RAW and sum(f['physical_line_count'] for f in inventory)<=m.MAX_ROWS,'RESOURCE_LIMIT')
    obj=dict(manifest_version=1,batch_contract_version=1,request_schema_version=1,registrar_revision=1,
             source_id=source_id,batch_id=batch_id,ingestion_id=ingestion_id(source_id,batch_id),received_at=received_at,profile=policy,files=inventory)
    raw=canonical_bytes(obj); validate_manifest(raw); fingerprint=sha256(raw).hexdigest()
    directory=_directory(root,source_id,batch_id)
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
        prior=_open(root,source_id,batch_id,sha256(old.raw).hexdigest())
        m.need(prior.fingerprint==fingerprint,'BATCH_CONFLICT'); check_inputs()
        return m.freeze(prior.summary('replayed'))
    try:
        if not directory.parent.exists(): directory.parent.mkdir()
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
        batch=_open(root,source_id,batch_id,fingerprint,pending=True); guard(); batch.check()
        completion=canonical_bytes(dict(completion_version=1,batch_fingerprint=fingerprint,manifest_byte_length=len(raw),completed_at=m.now()))
        m.write(directory/'complete.tmp',completion)
        checked=_open(root,source_id,batch_id,fingerprint,pending=True,temporary=True)
        guard(); checked.check(); m.need(not (directory/'complete.json').exists(),'DEPENDENCY_CHANGED')
        (directory/'complete.tmp').rename(directory/'complete.json')
        return m.freeze(batch.summary('registered'))
    except m.ReliabilityFailure as error:
        if error.code in ('INPUT_CHANGED','DEPENDENCY_CHANGED','INPUT_UNAVAILABLE','PATH_UNSAFE','RESOURCE_LIMIT'): raise
        raise m.ReliabilityFailure('PUBLICATION_FAILED') from None
    except OSError: raise m.ReliabilityFailure('PUBLICATION_FAILED') from None


def select(batch_root,selections):
    m.need(type(selections) in (list,tuple) and 1<=len(selections)<=m.MAX_BATCHES,'ARGUMENT_INVALID')
    unique=set(); bindings={}
    for item in selections:
        m.need(type(item) in (list,tuple) and len(item)==3,'ARGUMENT_INVALID')
        source,batch,fingerprint=item; m.match(source,m.SOURCE); m.match(batch,m.ID); m.match(fingerprint)
        m.need((source,batch) not in bindings or bindings[source,batch]==fingerprint,'BATCH_CONFLICT')
        bindings[source,batch]=fingerprint; unique.add(tuple(item))
    batches=tuple(open_batch(batch_root,*item) for item in sorted(unique))
    m.need(sum(len(b.manifest['files']) for b in batches)<=m.MAX_INPUT_FILES,'RESOURCE_LIMIT')
    m.need(sum(f['byte_length'] for b in batches for f in b.manifest['files'])<=m.MAX_RAW,'RESOURCE_LIMIT')
    m.need(sum(f['physical_line_count'] for b in batches for f in b.manifest['files'])<=m.MAX_ROWS,'RESOURCE_LIMIT')
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
    raw=canonical_bytes(dict(staging_version=1,product_fingerprint=product_fingerprint,files=files))
    m.need(len(raw)<=m.STAGE_MANIFEST_LIMIT,'RESOURCE_LIMIT'); m.write(directory/'stage.json',raw)
    metadata=m.observe(directory/'stage.json',m.STAGE_MANIFEST_LIMIT,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',retain=True)
    result=Staging(directory,m.freeze(files),metadata,tuple(captures),guards)
    result.check()
    return result


def main(argv=None):
    try:
        argv=sys.argv[1:] if argv is None else argv
        if argv==['--help']:
            print('Reliability batch: register|verify [fixed batch/profile options]'); return 0
        m.need(bool(argv) and argv[0] in ('register','verify'),'ARGUMENT_INVALID')
        required={'root','source-id','batch-id','expected-fingerprint'} if argv[0]=='verify' else {'root','input-root','source-id','batch-id','file','received-at','profile'}
        options=m.arguments(argv[1:],required,repeat=('file',))
        if argv[0]=='verify':
            root=options.pop('root'); result=open_batch(root,**options).summary('verified')
        else:
            config_path=Path(options.pop('profile')).absolute()
            capture=m.observe(config_path,m.PROFILE_LIMIT,'INPUT_UNAVAILABLE','INPUT_CHANGED',retain=True)
            policy=m.parse(capture.raw,m.PROFILE_LIMIT,'INTAKE_NOT_APPROVED')
            options['profile']=policy
            options['files']=[dict(artifact_id=i,input_file=i+'.jsonl') for i in options.pop('file')]
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
        for ev,item in zip(self.captures,self.files,strict=True):
            m.need(ev.length==item['byte_length'] and ev.digest==item['sha256'],'DEPENDENCY_CHANGED')
            m.observe(ev.path,m.MAX_STAGE_BYTES,'PUBLICATION_FAILED','DEPENDENCY_CHANGED',ev)


def dependency_failure(error):
    code={'BATCH_UNAVAILABLE':'DEPENDENCY_UNAVAILABLE','SNAPSHOT_UNAVAILABLE':'DEPENDENCY_UNAVAILABLE',
          'INPUT_UNAVAILABLE':'DEPENDENCY_UNAVAILABLE','BATCH_INVALID':'DEPENDENCY_INVALID',
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
        except m.ReliabilityFailure as error: raise dependency_failure(error) from None

    def manifest(self):
        return dict(manifest_version=1,product_contract_version=1,processor_revision=1,product_id=m.PRODUCT,
            input_contract_versions=dict(registered_batch=1,request_event=1,service_reference=1),
            dataset_contract_versions=dict.fromkeys(m.TABLES,1),engine={'spark':'4.0.2'},origin='synthetic',
            batches=[dict(source_id=b.manifest['source_id'],batch_id=b.manifest['batch_id'],
                batch_fingerprint=b.fingerprint,completion=m.descriptor(b.evidence.raw('complete.json'))) for b in self.batches],
            reference=dict(reference_id=self.reference.reference_id,revision=self.reference.revision,
                snapshot_fingerprint=self.reference.fingerprint,completion=m.descriptor(self.reference_evidence.raw('complete.json'))),
            semantic_config=json.loads(canonical_bytes(m.SEMANTICS)))

    @property
    def manifest_bytes(self): return canonical_bytes(self.manifest())

    @property
    def fingerprint(self): return sha256(self.manifest_bytes).hexdigest()


def verify_dependencies(*,batch_root,batches,reference_root,reference_id,revision,reference_fingerprint,output_root=None,staging_root=None):
    import service_reference
    m.match(reference_id,m.ID);m.match(revision,m.REVISION);m.match(reference_fingerprint)
    root=m.root_path(batch_root);refs=m.root_path(reference_root)
    others=[m.root_path(r,'PUBLICATION_UNAVAILABLE') for r in (output_root,staging_root) if r is not None]
    m.separate(root,refs,*others)
    try:
        selected,duplicates=select(root,batches)
        directory=refs/('reference_id='+reference_id)/('revision='+revision)
        m.root_path(directory)
        evidence=m.capture(directory,{'reference.csv':10*1024**2,'vocabulary.json':32*1024,'manifest.json':16384,'complete.json':4096},(),
            'DEPENDENCY_INVALID','DEPENDENCY_CHANGED',guards=(refs,directory.parent))
        reference=service_reference.open_snapshot(refs,reference_id,revision,reference_fingerprint)
        evidence.check()
        result=Dependencies(root,refs,selected,reference,evidence,duplicates)
        m.need(len(result.manifest_bytes)<=m.MANIFEST_LIMIT,'RESOURCE_LIMIT');result.check()
        return result
    except m.ReliabilityFailure as error: raise dependency_failure(error) from None


if __name__=='__main__': raise SystemExit(main())
