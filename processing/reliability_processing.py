"""Spark-only Reliability normalization, deterministic lineage and five-minute metrics."""
from dataclasses import dataclass
from functools import reduce
from pyspark.sql import functions as F, types as T, Window
import reliability_metadata as m

TYPES={'integer':T.IntegerType(),'long':T.LongType(),'string':T.StringType(),'boolean':T.BooleanType(),
       'timestamp':T.TimestampType(),'double':T.DoubleType(),'decimal(38,0)':T.DecimalType(38,0),
       'array<string>':T.ArrayType(T.StringType())}
SCHEMAS={n:T.StructType([T.StructField(c,TYPES[t]) for c,t in fields]) for n,fields in m.SCHEMAS.items()}
EVENT_SCHEMA=T.StructType([T.StructField('event_time_epoch_us' if n=='event_time' else n,
                         T.LongType() if n=='event_time' else TYPES[t]) for n,t in m.SCHEMAS[m.EVENTS][1:11]])
STAGE_SCHEMA=T.StructType([T.StructField(n,t) for n,t in (
    ('batch_fingerprint',T.StringType()),('artifact_id',T.StringType()),('line_number',T.LongType()),
    ('byte_offset',T.LongType()),('physical_byte_length',T.IntegerType()),('state',T.StringType()),
    ('recognized_service_id',T.StringType()),('recognized_event_id',T.StringType()),('semantic_sha256',T.StringType()),
    ('reason_codes',T.ArrayType(T.StringType())),('event',EVENT_SCHEMA))])
ORDER=('source_id','batch_id','batch_fingerprint','artifact_id','line_number')


def session(spark):
    result=spark.newSession()
    for k,v in {'spark.sql.session.timeZone':'UTC','spark.sql.parquet.outputTimestampType':'TIMESTAMP_MICROS',
                'spark.sql.parquet.datetimeRebaseModeInRead':'CORRECTED','spark.sql.parquet.datetimeRebaseModeInWrite':'CORRECTED',
                'spark.sql.ansi.enabled':'true','spark.sql.shuffle.partitions':'2'}.items(): result.conf.set(k,v)
    return result


def require(frame,conditions,code='PUBLICATION_INVALID'):
    condition=reduce(lambda a,b:a&b,conditions,F.lit(True))
    m.need(not frame.filter(~F.coalesce(condition,F.lit(False))).limit(1).count(),code)


def same(a,b):
    m.need(not a.exceptAll(b).limit(1).count() and not b.exceptAll(a).limit(1).count())


def _provenance(spark,dependencies):
    rows=[]
    for b in dependencies.batches:
        obj=b.manifest
        for file in obj['files']:
            rows.append((b.fingerprint,file['artifact_id'],obj['source_id'],obj['batch_id'],obj['ingestion_id'],
                file['sha256'],m.time_us(obj['received_at']),file['physical_line_count'],file['byte_length'],
                list(obj['profile']['allowed_service_ids']),list(obj['profile']['allowed_route_ids'])))
    return spark.createDataFrame(rows,'batch_fingerprint string, artifact_id string, source_id string, batch_id string, ingestion_id string, input_file_sha256 string, received_us long, expected_lines long, expected_bytes long, allowed_services array<string>, allowed_routes array<string>')


@dataclass(frozen=True)
class Frames:
    events: object
    occurrences: object
    metrics: object = None

    def tables(self): return {m.EVENTS:self.events,m.OCCURRENCES:self.occurrences,m.METRICS:self.metrics}


def _normalize(spark,dependencies,staging,held):
    staging.check();dependencies.check()
    paths=[str(staging.directory/f['file']) for f in staging.files]
    raw=spark.read.schema(STAGE_SCHEMA).option('mode','FAILFAST').json(paths)
    joined=raw.join(_provenance(spark,dependencies),['batch_fingerprint','artifact_id'],'left').cache()
    held.append(joined)
    reason_positions=F.transform('reason_codes',lambda v:F.array_position(F.array(*map(F.lit,m.REASONS)),v))
    rules=[F.col(n).isNotNull() for n in ('batch_fingerprint','artifact_id','line_number','byte_offset','physical_byte_length','state','reason_codes','input_file_sha256')]
    rules += [F.col('line_number').between(1,F.col('expected_lines')),F.col('byte_offset')>=0,
              F.col('physical_byte_length').between(1,m.LINE_LIMIT+2),F.col('state').isin('VALID','REJECTED'),
              ~F.exists('reason_codes',lambda v:v.isNull()),F.array_size(F.array_distinct('reason_codes'))==F.array_size('reason_codes'),
              ~F.exists(reason_positions,lambda v:v<=0),reason_positions==F.array_sort(reason_positions),
              F.col('recognized_service_id').isNull()==F.col('recognized_event_id').isNull()]
    require(joined,rules)
    require(joined.filter(F.col('recognized_service_id').isNotNull()),[
        F.col('recognized_service_id').rlike('^'+m.SOURCE.pattern+'$'),F.col('recognized_event_id').rlike('^'+m.UUID.pattern+'$'),
        F.array_contains('allowed_services',F.col('recognized_service_id'))])
    rejected=joined.filter(F.col('state')=='REJECTED')
    require(rejected,[F.col('event').isNull(),F.col('semantic_sha256').isNull(),F.array_size('reason_codes')>0])
    valid=joined.filter(F.col('state')=='VALID')
    require(valid,[F.col('event.'+n).isNotNull() for n in m.TRANSPORT_FIELDS]+[
        F.array_size('reason_codes')==0,F.col('event.schema_version')==1,F.col('event.is_synthetic')==F.lit(True),
        F.col('event.event_id')==F.col('recognized_event_id'),F.col('event.service_id')==F.col('recognized_service_id'),
        F.col('event.environment').isin('dev','test','production'),F.col('event.method').isin('GET','HEAD','POST','PUT','PATCH','DELETE','OPTIONS'),
        F.array_contains('allowed_routes',F.col('event.route_id')),F.col('event.status_code').between(100,599),
        F.col('event.duration_ms').between(0,3600000),F.col('event.event_time_epoch_us').between(m.MIN_US,m.END_US-1)])
    # All string values are approved ASCII tokens/UUIDs; exact heterogeneous JSON tuple encoding.
    parts=[]
    for name in m.TRANSPORT_FIELDS:
        col=F.col('event.'+name).cast('string')
        if name in ('event_id','service_id','environment','method','route_id'): col=F.concat(F.lit('"'),col,F.lit('"'))
        parts.append(col)
    require(valid,[F.sha2(F.concat(F.lit('['),F.concat_ws(',',*parts),F.lit(']')),256)==F.col('semantic_sha256')])
    m.need(not joined.groupBy('batch_fingerprint','artifact_id','line_number').count().filter(F.col('count')>1).limit(1).count())
    perfile=joined.groupBy('batch_fingerprint','artifact_id').agg(F.count('*').alias('lines'),F.sum(F.col('physical_byte_length').cast('decimal(38,0)')).alias('bytes'))
    provenance=_provenance(spark,dependencies)
    require(provenance.join(perfile,['batch_fingerprint','artifact_id'],'left'),[
        F.coalesce(F.col('lines'),F.lit(0))==F.col('expected_lines'),F.coalesce(F.col('bytes'),F.lit(0))==F.col('expected_bytes')])
    ordered=Window.partitionBy('batch_fingerprint','artifact_id').orderBy('line_number')
    positions=joined.withColumn('_previous_end',F.lag(F.col('byte_offset')+F.col('physical_byte_length')).over(ordered))
    require(positions,[F.col('byte_offset')==F.coalesce(F.col('_previous_end'),F.lit(0))])
    p=joined.count();v=valid.count();r=p-v;m.need(p<=m.MAX_ROWS,'RESOURCE_LIMIT')
    conflicts=joined.filter(F.col('recognized_service_id').isNotNull()).groupBy('recognized_service_id','recognized_event_id').agg(
        F.count('*').alias('deliveries'),F.sum(F.when(F.col('state')=='REJECTED',1).otherwise(0)).alias('invalid'),
        F.count_distinct(F.when(F.col('state')=='VALID',F.col('event'))).alias('variants'))
    c=conflicts.filter((F.col('variants')>1)|((F.col('invalid')>0)&(F.col('deliveries')>1))).count()
    if c:
        raise m.ReliabilityFailure('EVENT_IDENTITY_CONFLICT',conflict_summary=dict(conflicting_identity_count=c,
            physical_occurrences=p,valid_occurrences=v,rejected_occurrences=r,complete_scan=True))
    valid=valid.withColumn('received_at',F.expr('timestamp_micros(received_us)'))
    # Rank known physical keys to select equal-delivery lineage; never derive physical line position.
    rank=Window.partitionBy('recognized_service_id','recognized_event_id').orderBy(*ORDER)
    marked=valid.withColumn('_representative',F.row_number().over(rank)).withColumn('disposition',F.when(F.col('_representative')==1,'ACCEPTED').otherwise('DUPLICATE'))
    def occurrences(frame):
        return frame.select(F.lit(1).alias('contract_version'),'batch_fingerprint','source_id','batch_id','ingestion_id','artifact_id','input_file_sha256',
            'line_number','byte_offset','physical_byte_length','received_at','disposition',
            F.col('recognized_service_id').alias('service_id'),F.col('recognized_event_id').alias('event_id'),'semantic_sha256','reason_codes')
    occurrence=occurrences(marked).unionByName(occurrences(rejected.withColumn('received_at',F.expr('timestamp_micros(received_us)')).withColumn('disposition',F.lit('REJECTED'))))
    business=valid.select(*[F.col('event.'+n).alias(n) for n in m.TRANSPORT_FIELDS],'received_at').withColumn('event_time',F.expr('timestamp_micros(event_time_epoch_us)')).drop('event_time_epoch_us')
    events=business.groupBy(*m.EVENT_FIELDS).agg(F.count('*').alias('valid_delivery_count'),F.min('received_at').alias('first_received_at'),F.max('received_at').alias('last_received_at'))
    reference=spark.createDataFrame([(row.service_id,row.service_name,row.owner_team,row.service_tier) for row in dependencies.reference.rows],
                                   'key string, service_name string, owner_team string, service_tier string')
    m.need(not reference.groupBy('key').count().filter(F.col('count')>1).limit(1).count())
    events=events.join(reference,events.service_id==reference.key,'left').withColumn('reference_status',F.when(F.col('key').isNotNull(),'MATCHED').otherwise('MISSING')).drop('key')
    events=events.withColumn('contract_version',F.lit(1))
    for name,value in (('reference_id',dependencies.reference.reference_id),('reference_revision',dependencies.reference.revision),('reference_fingerprint',dependencies.reference.fingerprint)):
        events=events.withColumn(name,F.lit(value))
    events=events.select(*SCHEMAS[m.EVENTS].fieldNames())
    u=events.count();m.need(u<=v)
    dependencies.check();staging.check()
    return Frames(events,occurrence),dict(physical_occurrences=p,valid_occurrences=v,unique_valid_events=u,identical_duplicate_occurrences=v-u,rejected_occurrences=r)


def aggregate(events):
    groups=events.withColumn('window_start',F.expr('timestamp_micros(unix_micros(event_time)-pmod(unix_micros(event_time),300000000L))')).groupBy('service_id','environment','window_start').agg(
        F.sum(F.lit(1).cast('decimal(38,0)')).alias('_count'),
        F.sum(F.when(F.col('status_code').between(500,599),F.lit(1)).otherwise(0).cast('decimal(38,0)')).alias('_errors'),
        F.sum(F.col('duration_ms').cast('decimal(38,0)')).alias('duration_sum_ms'),
        F.percentile_approx('duration_ms',0.95,10000).alias('p95_duration_ms_approx'),
        *[F.first(n,ignorenulls=False).alias(n) for n,t in m.SCHEMAS[m.METRICS][10:]])
    require(groups,[F.col('_count').between(1,m.MAX_COUNT),F.col('_errors').between(0,F.col('_count')),
                    F.col('duration_sum_ms').between(F.lit(0),F.lit(str(10**38-1)).cast('decimal(38,0)'))],'COUNT_OVERFLOW')
    return groups.select(F.lit(1).alias('contract_version'),'service_id','environment','window_start',
        F.col('_count').cast('long').alias('request_count'),F.col('_errors').cast('long').alias('server_error_count'),
        (F.col('_errors').cast('double')/F.col('_count').cast('double')).alias('server_error_rate'),'duration_sum_ms',
        (F.col('duration_sum_ms').cast('double')/F.col('_count').cast('double')).alias('mean_duration_ms'),'p95_duration_ms_approx',
        *[n for n,t in m.SCHEMAS[m.METRICS][10:]])


def verify_frames(expected,actual):
    for name,frame in actual.tables().items():
        m.need(frame.schema.simpleString()==SCHEMAS[name].simpleString())
        optional={'service_name','owner_team','service_tier'} if name!=m.OCCURRENCES else {'service_id','event_id','semantic_sha256'}
        require(frame,[F.col(n).isNotNull() for n,t in m.SCHEMAS[name] if n not in optional]+[F.col('contract_version')==1])
        keys=['service_id','event_id'] if name==m.EVENTS else ['batch_fingerprint','artifact_id','line_number'] if name==m.OCCURRENCES else ['service_id','environment','window_start']
        m.need(not frame.groupBy(*keys).count().filter(F.col('count')>1).limit(1).count())
        m.need(frame.count()<=m.MAX_ROWS,'RESOURCE_LIMIT')
        if name!=m.METRICS: same(frame,expected.tables()[name])
        else:
            # Approximation is independently qualified, not compared to another execution value.
            columns=[n for n,t in m.SCHEMAS[name] if n not in ('p95_duration_ms_approx','server_error_rate','mean_duration_ms')]
            same(frame.select(*columns),expected.metrics.select(*columns))


def normalize(spark,dependencies,staging):
    held=[]
    try: return _normalize(spark,dependencies,staging,held)
    finally:
        import sys
        pending=sys.exc_info()[0] is not None
        for frame in held:
            try:frame.unpersist()
            except Exception:
                if not pending:raise


def verify_ranks(events,metrics):
    e=events.withColumn('_window',F.expr('timestamp_micros(unix_micros(event_time)-pmod(unix_micros(event_time),300000000L))')).alias('e')
    a=metrics.alias('a')
    joined=e.join(a,(F.col('e.service_id')==F.col('a.service_id'))&(F.col('e.environment')==F.col('a.environment'))&(F.col('e._window')==F.col('a.window_start')))
    ranks=joined.groupBy(*[F.col('a.'+n) for n in SCHEMAS[m.METRICS].fieldNames()]).agg(
        F.count('*').alias('_n'),F.sum(F.when(F.col('e.duration_ms')<F.col('a.p95_duration_ms_approx'),1).otherwise(0)).alias('_below'),
        F.sum(F.when(F.col('e.duration_ms')<=F.col('a.p95_duration_ms_approx'),1).otherwise(0)).alias('_upto'),
        F.sum(F.when(F.col('e.duration_ms')==F.col('a.p95_duration_ms_approx'),1).otherwise(0)).alias('_present'))
    ranks=ranks.withColumn('_target',F.expr('(95*_n+99) DIV 100')).withColumn('_budget',F.greatest(F.lit(1),F.expr('(_n+9999) DIV 10000')))
    require(ranks,[F.col('_present')>0,F.col('_n')==F.col('request_count'),
        F.greatest(F.col('_below')+1-F.col('_target'),F.col('_target')-F.col('_upto'),F.lit(0))<=F.col('_budget')])
    m.need(ranks.count()==metrics.count())
