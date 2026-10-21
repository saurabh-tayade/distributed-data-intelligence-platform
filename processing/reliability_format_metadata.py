"""Strict Spark-free Reliability metadata/evidence, ADR 0009, product contract 2. No network or producers."""
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
import stat
from types import MappingProxyType

from publication_metadata import canonical_bytes

PRODUCT = 'application-reliability'
LABEL = 'Synthetic request events — not production reliability.'
EVENTS, OCCURRENCES, METRICS = ('reliability_events_v1', 'reliability_occurrences_v2', 'reliability_requests_5m_v1')
TABLES = (EVENTS, OCCURRENCES, METRICS)
MAX_COUNT = 2**63-1
MAX_ROWS = 2_000_000
LINE_LIMIT = 16_384
FILE_LIMIT = 256*1024**2
MAX_RAW = 1024**3
MAX_BATCHES = 32
MAX_INPUT_FILES = 128
MAX_BATCH_FILES = 16
PROFILE_LIMIT = 64*1024
BATCH_MANIFEST_LIMIT = 128*1024
BATCH_COMPLETION_LIMIT = 4096
MANIFEST_LIMIT = 64*1024
COMPLETION_LIMIT = 1024**2
STAGE_MANIFEST_LIMIT = 64*1024
STAGE_RECORD_LIMIT = 2048
MAX_STAGE_BYTES = 4*1024**3
MAX_OUTPUT_BYTES = 4*1024**3
MAX_FILES = 1024
MAX_DATASET_FILES = 512
RESPONSE_LIMIT = 1024**2
PAGE_BYTES = 64*1024
CHUNK_ROWS = 1024
CHUNK_BYTES = 2*1024**2
WINDOW_US = 300_000_000
END_SENTINEL = '10000-01-01T00:00:00.000000Z'
END_US = 253402300800000000
MIN_US = -62135596800000000
HEX = re.compile(r'[0-9a-f]{64}')
SOURCE = re.compile(r'[a-z0-9][a-z0-9_-]{0,127}')
ID = re.compile(r'[a-z0-9][a-z0-9_-]{0,63}')
REVISION = re.compile(r'[a-z0-9][a-z0-9_-]{0,31}')
UUID = re.compile(r'[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}')
UTC = re.compile(r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}[.][0-9]{6}Z')
REASONS = ('BLANK_LINE','JSON_SYNTAX','JSON_NOT_OBJECT','JSON_DUPLICATE_KEY','JSON_NONFINITE','FIELD_MISSING','FIELD_NULL','FIELD_TYPE','FIELD_VALUE','TIMESTAMP_INVALID')
EVENT_FIELDS = ('schema_version','event_id','service_id','environment','event_time','method','route_id','status_code','duration_ms','is_synthetic')
TRANSPORT_FIELDS = tuple('event_time_epoch_us' if n=='event_time' else n for n in EVENT_FIELDS)
SEMANTICS = {'admission_policy_revision': 2, 'event_identity': 'service-event-v1', 'invalid_identity': 'repeat-recognized-abort-v1', 'occurrence': 'format-physical-v2', 'percentile': {'accuracy': 10000, 'algorithm': 'spark-percentile-approx', 'probability_denominator': 100, 'probability_numerator': 95, 'rank_policy': 'nearest-rank-interval-v1'}, 'reference_application': 'snapshot-service-v1', 'rejections': 'record-safe-rows-v1', 'semantic_equality': 'typed-ten-epoch-us-v1', 'window': 'utc-floor-300000000us-v1'}
SCHEMAS = {'reliability_events_v1': (('contract_version', 'integer'), ('schema_version', 'integer'), ('event_id', 'string'), ('service_id', 'string'), ('environment', 'string'), ('event_time', 'timestamp'), ('method', 'string'), ('route_id', 'string'), ('status_code', 'integer'), ('duration_ms', 'long'), ('is_synthetic', 'boolean'), ('valid_delivery_count', 'long'), ('first_received_at', 'timestamp'), ('last_received_at', 'timestamp'), ('service_name', 'string'), ('owner_team', 'string'), ('service_tier', 'string'), ('reference_status', 'string'), ('reference_id', 'string'), ('reference_revision', 'string'), ('reference_fingerprint', 'string')), 'reliability_occurrences_v2': (('contract_version', 'integer'), ('batch_fingerprint', 'string'), ('source_id', 'string'), ('batch_id', 'string'), ('ingestion_id', 'string'), ('artifact_id', 'string'), ('input_file_sha256', 'string'), ('input_format', 'string'), ('record_ordinal', 'long'), ('line_number', 'long'), ('byte_offset', 'long'), ('physical_byte_length', 'integer'), ('row_group_ordinal', 'integer'), ('row_ordinal', 'long'), ('received_at', 'timestamp'), ('disposition', 'string'), ('service_id', 'string'), ('event_id', 'string'), ('semantic_sha256', 'string'), ('reason_codes', 'array<string>')), 'reliability_requests_5m_v1': (('contract_version', 'integer'), ('service_id', 'string'), ('environment', 'string'), ('window_start', 'timestamp'), ('request_count', 'long'), ('server_error_count', 'long'), ('server_error_rate', 'double'), ('duration_sum_ms', 'decimal(38,0)'), ('mean_duration_ms', 'double'), ('p95_duration_ms_approx', 'long'), ('service_name', 'string'), ('owner_team', 'string'), ('service_tier', 'string'), ('reference_status', 'string'), ('reference_id', 'string'), ('reference_revision', 'string'), ('reference_fingerprint', 'string'))}
MESSAGES = {'ARGUMENT_INVALID': 'Arguments do not match the Reliability interface.', 'PATH_UNSAFE': 'A selected path violates the local path policy.', 'UNSUPPORTED_VERSION': 'A selected Reliability contract or processor is unsupported.', 'INTAKE_NOT_APPROVED': 'Input is not approved for synthetic Reliability intake.', 'INPUT_UNAVAILABLE': 'A selected intake file is unavailable.', 'INPUT_CHANGED': 'Intake evidence changed during the operation.', 'BATCH_UNAVAILABLE': 'The selected registered batch is unavailable.', 'BATCH_INCOMPLETE': 'Registered batch is incomplete; owner investigation is required.', 'BATCH_INVALID': 'Registered batch evidence is invalid.', 'BATCH_CONFLICT': 'Batch identity already binds different content.', 'CSV_INVALID': 'Service reference CSV is invalid.', 'VOCABULARY_INVALID': 'Service vocabulary is invalid or unsupported.', 'REVISION_CONFLICT': 'Service reference revision already binds different content.', 'SNAPSHOT_UNAVAILABLE': 'The selected service reference is unavailable.', 'SNAPSHOT_INVALID': 'Service reference evidence is invalid.', 'SNAPSHOT_CHANGED': 'Service reference evidence changed during the operation.', 'SNAPSHOT_INCOMPLETE': 'Service reference is incomplete; owner investigation is required.', 'IDENTITY_MISMATCH': 'Selected service reference identity does not match.', 'DEPENDENCY_UNAVAILABLE': 'A selected Reliability dependency is unavailable.', 'DEPENDENCY_INVALID': 'Reliability dependency evidence is invalid.', 'DEPENDENCY_CHANGED': 'Reliability dependency evidence changed during the operation.', 'EVENT_IDENTITY_CONFLICT': 'Selected request occurrences have an unresolved identity conflict.', 'PUBLICATION_UNAVAILABLE': 'The selected Reliability publication is unavailable.', 'PUBLICATION_INCOMPLETE': 'Reliability publication is incomplete; owner investigation is required.', 'PUBLICATION_INVALID': 'Reliability publication evidence is invalid.', 'PUBLICATION_CHANGED': 'Reliability publication evidence changed during the operation.', 'PUBLICATION_FAILED': 'Reliability publication failed; investigate retained evidence.', 'COUNT_OVERFLOW': 'A Reliability count or sum exceeds its supported range.', 'RESOURCE_LIMIT': 'A Reliability resource exceeds its configured limit.', 'RESPONSE_LIMIT': 'The Reliability report exceeds its response limit.', 'BUSY': 'The Reliability session already has an active request.', 'QUERY_FAILED': 'The Reliability report could not be completed.'}

MESSAGES.update(PARQUET_INVALID='Parquet input evidence is invalid.',
    PROVENANCE_UNSUPPORTED='Required physical provenance is unavailable.',
    BENCHMARK_INVALID='Benchmark evidence is invalid.',
    PREFLIGHT_BLOCKED='The benchmark plan did not pass resource admission.')
DECODERS = {'JSONL':{'implementation':'jsonl-physical-v1','revision':1},
    'PARQUET':{'implementation':'duckdb-file-row-number','revision':1,'duckdb':'1.4.5'}}


def semantics(input_format):
    need(type(input_format) is str and input_format in DECODERS,'ARGUMENT_INVALID')
    return SEMANTICS | {'raw_decoder':DECODERS[input_format]}


class ReliabilityFailure(Exception):
    def __init__(self, code, *, conflict_summary=None, **_):
        self.code = code
        self.diagnostic = dict(status='FAILED',product_id=PRODUCT,origin='synthetic',label=LABEL,code=code,message=MESSAGES[code],product_contract_version=2,processor_revision=2,input_format=None)
        if conflict_summary is not None:
            self.diagnostic['conflict_summary'] = public(conflict_summary)
        super().__init__(MESSAGES[code])


def need(condition, code='PUBLICATION_INVALID'):
    if not condition:
        raise ReliabilityFailure(code)


def freeze(value):
    if isinstance(value,dict): return MappingProxyType({k:freeze(v) for k,v in value.items()})
    if isinstance(value,list): return tuple(freeze(v) for v in value)
    return value


def keys(value, expected, code='PUBLICATION_INVALID'):
    need(type(value) is dict and set(value)==set(expected),code)


def match(value, pattern=HEX, code='ARGUMENT_INVALID'):
    need(type(value) is str and pattern.fullmatch(value) is not None,code)


def integer(value, low=0, high=MAX_COUNT, code='PUBLICATION_INVALID'):
    need(type(value) is int and low<=value<=high,code)
    return value


def parse(raw, limit, code):
    need(len(raw)<=limit,'RESOURCE_LIMIT')
    def pairs(items):
        result={}
        for k,v in items:
            need(k not in result,code)
            result[k]=v
        return result
    def number(token):
        need(re.fullmatch(r'0|[1-9][0-9]{0,18}',token) is not None,code)
        return integer(int(token),code=code)
    def no(_): raise ReliabilityFailure(code)
    try:
        obj=json.loads(raw.decode('utf-8'),object_pairs_hook=pairs,parse_int=number,parse_float=no,parse_constant=no)
        need(canonical_bytes(obj)==raw,code)
        return obj
    except (ValueError,UnicodeError,RecursionError,TypeError):
        raise ReliabilityFailure(code) from None


def leap(year): return year%4==0 and (year%100!=0 or year%400==0)


def days_before_year(year):
    y=year-1
    return 365*y+y//4-y//100+y//400


def time_us(value, code='ARGUMENT_INVALID', *, end=False):
    if end and value==END_SENTINEL:
        # Gregorian integer arithmetic only: never datetime(year=10000).
        return (days_before_year(10000)-days_before_year(1970))*86400*1_000_000
    match(value,UTC,code)
    y,mo,d,h,mi,se,us=map(int,re.findall(r'[0-9]+',value))
    need(1<=y<=9999 and 1<=mo<=12 and 0<=h<24 and 0<=mi<60 and 0<=se<60,code)
    lengths=(31,29 if leap(y) else 28,31,30,31,30,31,31,30,31,30,31)
    need(1<=d<=lengths[mo-1],code)
    days=days_before_year(y)-days_before_year(1970)+sum(lengths[:mo-1])+d-1
    return ((days*24+h)*60*60+mi*60+se)*1_000_000+us


def timestamp(us):
    need(type(us) is int and MIN_US<=us<END_US)
    date=datetime(1970,1,1,tzinfo=timezone.utc)+timedelta(microseconds=us)
    return date.isoformat(timespec='microseconds').replace('+00:00','Z')


def now(): return datetime.now(timezone.utc).isoformat(timespec='microseconds').replace('+00:00','Z')


def public(value):
    from decimal import Decimal
    from collections.abc import Mapping
    if type(value) is bool or value is None: return value
    if isinstance(value,(int,Decimal)): return str(value)
    if isinstance(value,Mapping): return {k:public(v) for k,v in value.items()}
    if isinstance(value,(list,tuple)): return [public(v) for v in value]
    return value


def descriptor(raw): return dict(byte_length=len(raw),sha256=sha256(raw).hexdigest())


def root_path(value, code='DEPENDENCY_UNAVAILABLE'):
    try:
        path=Path(value).absolute()
        need('..' not in path.parts and not any(c in str(path) for c in ('\x00','\\','*','?','[',']')), 'PATH_UNSAFE')
        info=path.lstat()
        need(stat.S_ISDIR(info.st_mode),'PATH_UNSAFE')
        # Ancestors of an operator-selected root are trusted. Canonicalize them
        # once so two spellings of one root cannot bypass overlap admission.
        # The root itself remains no-follow; descendants are guarded separately.
        path=path.parent.resolve(strict=True)/path.name
        return path
    except (OSError,ValueError,TypeError): raise ReliabilityFailure(code) from None


def separate(*roots):
    for i,a in enumerate(roots):
        for b in roots[i+1:]:
            need(a!=b and a not in b.parents and b not in a.parents,'PATH_UNSAFE')


def stamp(info): return (info.st_dev,info.st_ino,info.st_size,info.st_mtime_ns,info.st_mode,info.st_nlink)


def info(path, directory, unavailable, changed, previous=None):
    try: current=path.lstat()
    except FileNotFoundError: raise ReliabilityFailure(changed if previous is not None else unavailable) from None
    except OSError: raise ReliabilityFailure(unavailable) from None
    observed=(current.st_dev,current.st_ino,current.st_mode) if directory else stamp(current)
    if previous is not None: need(observed==previous,changed)
    need(stat.S_ISDIR(current.st_mode) if directory else stat.S_ISREG(current.st_mode) and current.st_nlink==1,'PATH_UNSAFE')
    return current


@dataclass(frozen=True)
class FileEvidence:
    path: Path
    stamp: tuple
    length: int
    digest: str
    raw: bytes | None


def observe(path, limit, unavailable, changed, previous=None, *, retain=False, sink=None):
    before=info(path,False,unavailable,changed,previous.stamp if previous else None)
    need(before.st_size<=limit,'RESOURCE_LIMIT')
    digest,size,data,complete=sha256(),0,bytearray() if retain else None,False
    try:
        fd=os.open(path,os.O_RDONLY|os.O_NONBLOCK|os.O_NOFOLLOW)
        try: handle=os.fdopen(fd,'rb')
        except BaseException:
            os.close(fd)
            raise
        with handle as stream:
            need(stamp(os.fstat(stream.fileno()))==stamp(before),changed)
            while chunk:=stream.read(min(1024**2,limit+1-size)):
                size+=len(chunk); need(size<=limit,'RESOURCE_LIMIT'); digest.update(chunk)
                if retain: data.extend(chunk)
                if sink is not None:
                    try: written=sink.write(chunk)
                    except OSError:
                        info(path,False,unavailable,changed,stamp(before))
                        raise ReliabilityFailure('PUBLICATION_FAILED') from None
                    need(written==len(chunk),'PUBLICATION_FAILED')
            complete=True
            if previous: need((size,digest.hexdigest())==(previous.length,previous.digest),changed)
            need(stamp(os.fstat(stream.fileno()))==stamp(before),changed)
        info(path,False,unavailable,changed,stamp(before))
    except OSError:
        if previous and complete: need((size,digest.hexdigest())==(previous.length,previous.digest),changed)
        if previous and retain and previous.raw is not None: need(previous.raw[:size]==data,changed)
        info(path,False,unavailable,changed,stamp(before))
        raise ReliabilityFailure(unavailable) from None
    return FileEvidence(path,stamp(before),size,digest.hexdigest(),bytes(data) if retain else None)


def names(path, unavailable, changed, old=None):
    result=[]; complete=False
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                need(len(result)<MAX_FILES*2+32,'RESOURCE_LIMIT')
                result.append(entry.name)
            complete=True
        result=tuple(sorted(result))
        if old is not None: need(result==old,changed)
        return result
    except FileNotFoundError: raise ReliabilityFailure(changed if old is not None else unavailable) from None
    except OSError:
        if old is not None:
            need(set(result)<=set(old),changed)
            if complete: need(tuple(sorted(result))==old,changed)
        raise ReliabilityFailure(unavailable) from None


@dataclass(frozen=True)
class Evidence:
    directory: Path
    metadata: tuple
    datasets: tuple
    files: tuple
    directories: tuple
    guards: tuple
    unavailable: str
    changed: str

    def check(self):
        return capture(self.directory,dict(self.metadata),self.datasets,self.unavailable,self.changed,
                       guards=tuple(p for p,_ in self.guards),previous=self)

    def raw(self,name): return next(f.raw for f in self.files if f.path==self.directory/name)

    def inventory(self):
        return {table:[dict(path=f.path.relative_to(self.directory).as_posix(),byte_length=f.length,sha256=f.digest)
                       for f in self.files if f.path.parent==self.directory/table and not f.path.name.startswith(('.','_'))]
                for table in self.datasets}

    def paths(self,table): return tuple(str(self.directory/i['path']) for i in self.inventory()[table])


def capture(directory, metadata, datasets, unavailable, changed, *, guards=(), previous=None):
    old_files={f.path:f for f in previous.files} if previous else {}
    old_dirs={p:(identity,entries) for p,identity,entries in previous.directories} if previous else {}
    old_guards=dict(previous.guards) if previous else {}
    guard_records=[]; directories=[]; files=[]
    try:
        for guard in guards:
            v=info(guard,True,unavailable,changed,old_guards.get(guard))
            guard_records.append((guard,(v.st_dev,v.st_ino,v.st_mode)))
        for path in (directory,*(directory/n for n in datasets)):
            v=info(path,True,unavailable,changed,old_dirs.get(path,(None,None))[0])
            entries=names(path,unavailable,changed,old_dirs.get(path,(None,None))[1])
            directories.append((path,(v.st_dev,v.st_ino,v.st_mode),entries))
            if path==directory:
                need(set(entries)==set(metadata)|set(datasets),unavailable)
            else:
                analytical=[n for n in entries if not n.startswith(('.','_'))]
                max_files=MAX_INPUT_FILES if path.name=='data' else MAX_DATASET_FILES
                need(len(analytical)<=max_files,'RESOURCE_LIMIT')
                extension='jsonl' if path.name=='data' else 'parquet'
                for name in analytical:
                    pattern=ID.pattern+r'[.](?:jsonl|parquet)' if extension=='jsonl' else r'[A-Za-z0-9][A-Za-z0-9_.-]*[.]parquet'
                    match(name,re.compile(pattern),'PATH_UNSAFE')
                    need(len((path/name).relative_to(directory).as_posix().encode())<=256,'RESOURCE_LIMIT')
                allowed=set(analytical)
                if extension=='parquet': allowed|={'_SUCCESS','._SUCCESS.crc',*('.'+n+'.crc' for n in analytical)}
                need(set(entries)<=allowed,unavailable)
            for name in entries:
                child=path/name
                if path==directory and name in datasets: continue
                retain=path==directory
                limit=metadata[name] if retain else FILE_LIMIT if path.name=='data' else MAX_OUTPUT_BYTES
                files.append(observe(child,limit,unavailable,changed,old_files.get(child),retain=retain))
        result=Evidence(directory,tuple(metadata.items()),tuple(datasets),tuple(files),tuple(directories),tuple(guard_records),unavailable,changed)
        for path,identity,entries in directories:
            info(path,True,unavailable,changed,identity); names(path,unavailable,changed,entries)
        analytical=[f for f in files if f.path.parent!=directory and not f.path.name.startswith(('.','_'))]
        need(len(analytical)<= (MAX_INPUT_FILES if datasets==('data',) else MAX_FILES),'RESOURCE_LIMIT')
        need(sum(f.length for f in analytical)<= (MAX_RAW if datasets==('data',) else MAX_OUTPUT_BYTES),'RESOURCE_LIMIT')
        for path,identity in guard_records: info(path,True,unavailable,changed,identity)
        return result
    except ReliabilityFailure as error:
        if error.code==unavailable:
            # Re-observe identities/membership already captured in this operation.
            # An I/O failure alone is not mutation; actual replacement still is.
            for path,identity in guard_records: info(path,True,unavailable,changed,identity)
            for path,identity,entries in directories:
                info(path,True,unavailable,changed,identity); names(path,unavailable,changed,entries)
            for ev in files: info(ev.path,False,unavailable,changed,ev.stamp)
        raise


def write(path,raw):
    try:
        with path.open('xb') as stream: need(stream.write(raw)==len(raw),'PUBLICATION_FAILED')
    except OSError: raise ReliabilityFailure('PUBLICATION_FAILED') from None


def namespace_guards(root,directory): return (root,root/'v2',directory.parent)


def publication_directory(root,fingerprint):
    match(fingerprint)
    return root/'v2'/'processor_revision=2'/('publication='+fingerprint)


def arguments(argv, required, optional=(), repeat=()):
    if argv==['--help']: return None
    need(len(argv)%2==0 and len(argv)<=512,'ARGUMENT_INVALID')
    allowed=set(required)|set(optional); values={}
    for i in range(0,len(argv),2):
        key=argv[i]
        need(key.startswith('--') and key[2:] in allowed and not argv[i+1].startswith('--'),'ARGUMENT_INVALID')
        key=key[2:]
        need(key not in values or key in repeat,'ARGUMENT_INVALID')
        if key in repeat: values.setdefault(key,[]).append(argv[i+1])
        else: values[key]=argv[i+1]
    need(set(required)<=set(values),'ARGUMENT_INVALID')
    return {k.replace('-','_'):v for k,v in values.items()}


def cli_error(error):
    import sys
    print(canonical_bytes(error.diagnostic).decode(),file=sys.stderr)
    return 2 if error.code=='ARGUMENT_INVALID' else 1


def metric_bounds(start,end):
    low=time_us(start); high=time_us(end,end=True)
    need(low%WINDOW_US==0 and high%WINDOW_US==0 and 0<high-low<=31*86400*1_000_000,'ARGUMENT_INVALID')
    return low,high


def validate_manifest(raw):
    code='PUBLICATION_INVALID'; obj=parse(raw,MANIFEST_LIMIT,code)
    keys(obj,('manifest_version','product_contract_version','processor_revision','product_id','input_contract_versions',
              'dataset_contract_versions','engine','origin','input_format','batches','reference','semantic_config'),code)
    for n in ('manifest_version','product_contract_version','processor_revision'): integer(obj[n],2,2,'UNSUPPORTED_VERSION')
    need(obj['product_id']==PRODUCT and obj['origin']=='synthetic',code)
    need(obj['input_contract_versions']==dict(registered_batch=2,request_event=1,service_reference=1),'UNSUPPORTED_VERSION')
    keys(obj['input_contract_versions'],('registered_batch','request_event','service_reference'),code)
    keys(obj['dataset_contract_versions'],TABLES,code)
    need(obj['dataset_contract_versions']=={EVENTS:1,OCCURRENCES:2,METRICS:1},'UNSUPPORTED_VERSION')
    for k,v in obj['input_contract_versions'].items():integer(v,2 if k=='registered_batch' else 1,2 if k=='registered_batch' else 1,'UNSUPPORTED_VERSION')
    for k,v in obj['dataset_contract_versions'].items():integer(v,2 if k==OCCURRENCES else 1,2 if k==OCCURRENCES else 1,'UNSUPPORTED_VERSION')
    need(type(obj['input_format']) is str and obj['input_format'] in DECODERS,code)
    keys(obj['engine'],('spark',),code);need(obj['engine']=={'spark':'4.0.2'},'UNSUPPORTED_VERSION')
    # Exact canonical bytes distinguish booleans/floats from required integer constants.
    need(canonical_bytes(obj['semantic_config'])==canonical_bytes(semantics(obj['input_format'])),'UNSUPPORTED_VERSION')
    need(type(obj['batches']) is list and 1<=len(obj['batches'])<=MAX_BATCHES,code)
    ordered=[];bindings=[]
    for b in obj['batches']:
        keys(b,('source_id','batch_id','batch_fingerprint','completion'),code)
        match(b['source_id'],SOURCE,code);match(b['batch_id'],ID,code);match(b['batch_fingerprint'],code=code)
        ordered.append((b['source_id'],b['batch_id'],b['batch_fingerprint']));bindings.append((b['source_id'],b['batch_id']))
        keys(b['completion'],('byte_length','sha256'),code)
        integer(b['completion']['byte_length'],1,BATCH_COMPLETION_LIMIT,code);match(b['completion']['sha256'],code=code)
    need(ordered==sorted(set(ordered)) and len(set(bindings))==len(bindings),code)
    r=obj['reference'];keys(r,('reference_id','revision','snapshot_fingerprint','completion'),code)
    match(r['reference_id'],ID,code);match(r['revision'],REVISION,code);match(r['snapshot_fingerprint'],code=code)
    keys(r['completion'],('byte_length','sha256'),code);integer(r['completion']['byte_length'],1,4096,code);match(r['completion']['sha256'],code=code)
    return obj


def validate_inventory(files):
    code='PUBLICATION_INVALID'; keys(files,TABLES,code);total=count=0
    for table,items in files.items():
        need(type(items) is list and 1<=len(items)<=MAX_DATASET_FILES,code)
        paths=[]
        for item in items:
            keys(item,('path','byte_length','sha256'),code)
            need(type(item['path']) is str and item['path'].startswith(table+'/') and len(item['path'].split('/'))==2,code)
            match(item['path'].split('/')[1],re.compile(r'[A-Za-z0-9][A-Za-z0-9_.-]*[.]parquet'),code)
            need(len(item['path'].encode())<=256,'RESOURCE_LIMIT')
            integer(item['byte_length'],1,MAX_OUTPUT_BYTES,code);match(item['sha256'],code=code)
            paths.append(item['path']);total+=item['byte_length'];count+=1
        need(paths==sorted(set(paths)),code)
    need(total<=MAX_OUTPUT_BYTES and count<=MAX_FILES,'RESOURCE_LIMIT')


def validate_completion(raw,fingerprint,manifest_length):
    code='PUBLICATION_INVALID';obj=parse(raw,COMPLETION_LIMIT,code)
    keys(obj,('completion_version','product_id','product_fingerprint','manifest_byte_length','occurrence_counts','output_counts','files','completed_at'),code)
    integer(obj['completion_version'],2,2,'UNSUPPORTED_VERSION')
    need(obj['product_id']==PRODUCT and obj['product_fingerprint']==fingerprint and type(obj['manifest_byte_length']) is int and obj['manifest_byte_length']==manifest_length,code)
    keys(obj['occurrence_counts'],('physical_occurrences','valid_occurrences','unique_valid_events','identical_duplicate_occurrences','rejected_occurrences'),code)
    for n in obj['occurrence_counts'].values(): integer(n,0,MAX_ROWS,code)
    c=obj['occurrence_counts'];need(c['physical_occurrences']==c['valid_occurrences']+c['rejected_occurrences'] and c['valid_occurrences']==c['unique_valid_events']+c['identical_duplicate_occurrences'],code)
    keys(obj['output_counts'],TABLES,code)
    for n in obj['output_counts'].values(): integer(n,0,MAX_ROWS,code)
    need(obj['output_counts'][EVENTS]==c['unique_valid_events'] and obj['output_counts'][OCCURRENCES]==c['physical_occurrences'] and obj['output_counts'][METRICS]<=c['unique_valid_events'],code)
    time_us(obj['completed_at'],code);validate_inventory(obj['files'])
    return obj
