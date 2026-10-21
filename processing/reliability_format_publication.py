"""Immutable Reliability v2 publication/replay; one trusted writer, completion last."""
from dataclasses import dataclass
from contextlib import contextmanager
import time
import sys
from publication_metadata import canonical_bytes
import reliability_format_metadata as m
from reliability_format_inputs import verify_dependencies,stage
import reliability_format_analytics as analytics


@dataclass(frozen=True)
class Publication:
    status: str
    product_fingerprint: str
    occurrence_counts: object
    output_counts: object
    duplicate_batch_selections: int

    def summary(self):
        return dict(status=self.status,product_id=m.PRODUCT,product_fingerprint=self.product_fingerprint,
                    origin='synthetic',label=m.LABEL,product_contract_version=2,processor_revision=2,occurrence_counts=self.occurrence_counts,
                    output_counts=self.output_counts,duplicate_batch_selections=self.duplicate_batch_selections)


def _compare_phase(before,after):
    old={f.path:f for f in before.files};current={f.path:f for f in after.files}
    for path,ev in old.items():
        m.need(path in current and (ev.stamp,ev.length,ev.digest)==(current[path].stamp,current[path].length,current[path].digest),'PUBLICATION_CHANGED')
    for path,identity,names in before.directories:
        now=next((i,n) for p,i,n in after.directories if p==path)
        allowed=tuple(sorted((*names,'complete.tmp'))) if path==before.directory else names
        m.need(now==(identity,allowed),'PUBLICATION_CHANGED')


def _marker(deps,counts,frames,evidence):
    result=dict(completion_version=2,product_id=m.PRODUCT,product_fingerprint=deps.fingerprint,
        manifest_byte_length=len(deps.manifest_bytes),occurrence_counts=counts,
        output_counts={n:f.count() for n,f in frames.tables().items()},files=evidence.inventory(),completed_at=m.now())
    m.validate_completion(canonical_bytes(result),deps.fingerprint,len(deps.manifest_bytes))
    return result


def _publish(spark, *,batch_root,input_format,batches,reference_root,reference_id,revision,reference_fingerprint,staging_root,output_root,execution_config,ledger):
    root=m.root_path(output_root,'PUBLICATION_UNAVAILABLE')
    v=m.info(root,True,'PUBLICATION_UNAVAILABLE','PUBLICATION_CHANGED');root_id=(v.st_dev,v.st_ino,v.st_mode)
    with ledger.span('dependency_open'):
        deps=verify_dependencies(batch_root=batch_root,input_format=input_format,batches=batches,reference_root=reference_root,
        reference_id=reference_id,revision=revision,reference_fingerprint=reference_fingerprint,output_root=root,staging_root=staging_root)
    directory=m.publication_directory(root,deps.fingerprint)
    for parent in (root/'v2',directory.parent):
        try:parent.lstat()
        except FileNotFoundError:continue
        except OSError:raise m.ReliabilityFailure('PUBLICATION_INVALID') from None
        m.info(parent,True,'PUBLICATION_INVALID','PUBLICATION_CHANGED')
    try:directory.lstat()
    except FileNotFoundError:existing=False
    except OSError:raise m.ReliabilityFailure('PUBLICATION_INVALID') from None
    else:existing=True
    if existing:
        # Full independent verification suffices for replay; never calculate a new approximation.
        with analytics.open_publication(root,deps.fingerprint,batch_root=batch_root,reference_root=reference_root) as opened:
            marker=opened._marker
            deps.check();m.info(root,True,'PUBLICATION_INVALID','PUBLICATION_CHANGED',root_id)
            return Publication('replayed',deps.fingerprint,m.freeze(marker['occurrence_counts']),m.freeze(marker['output_counts']),deps.duplicate_batch_selections)
    from reliability_format_processing import session,normalize,aggregate,verify_frames,verify_ranks,Frames,SCHEMAS
    validate_execution(spark,execution_config)
    sql=session(spark)
    with ledger.span('staging'):staged=stage(deps.batches,staging_root,deps.fingerprint)
    staged.check();held=[]
    try:
        with ledger.span('normalization'):
            normalized,counts=normalize(sql,deps,staged)
            events=normalized.events.cache();held.append(events);events.count()
            occurrences=normalized.occurrences.cache();held.append(occurrences);occurrences.count()
        with ledger.span('aggregation'):
            metrics=aggregate(events).cache();held.append(metrics);metrics.count()
        expected=Frames(events,occurrences,metrics)
        verify_frames(expected,expected);verify_ranks(events,metrics)
        deps.check();staged.check();m.info(root,True,'PUBLICATION_FAILED','PUBLICATION_CHANGED',root_id)
        for parent in (root/'v2',directory.parent):
            try:parent.mkdir()
            except FileExistsError:pass
            m.info(parent,True,'PUBLICATION_FAILED','PUBLICATION_CHANGED')
        directory.mkdir()
        guard_paths=(root,root/'v2',directory.parent,directory)
        guards=tuple((p,(i.st_dev,i.st_ino,i.st_mode)) for p in guard_paths for i in (m.info(p,True,'PUBLICATION_FAILED','PUBLICATION_CHANGED'),))
        def guard():
            for path,identity in guards:m.info(path,True,'PUBLICATION_FAILED','PUBLICATION_CHANGED',identity)
        guard();m.write(directory/'inputs.json',deps.manifest_bytes)
        with ledger.span('output_write'):
            for name,frame in expected.tables().items():
                guard();m.need(frame.schema.simpleString()==SCHEMAS[name].simpleString())
                frame.repartition(execution_config['output_partitions']).write.mode('errorifexists').parquet(str(directory/name));guard()
        evidence=m.capture(directory,{'inputs.json':m.MANIFEST_LIMIT},m.TABLES,'PUBLICATION_FAILED','PUBLICATION_CHANGED',guards=m.namespace_guards(root,directory))
        m.need(evidence.raw('inputs.json')==deps.manifest_bytes,'PUBLICATION_CHANGED')
        marker=_marker(deps,counts,expected,evidence)
        with ledger.span('independent_pending_verify'):
            db,stats=analytics.verify_core(deps,evidence,marker);db.close()
        with ledger.span('spark_readback'):
            actual={n:sql.read.schema(SCHEMAS[n]).option('mergeSchema','false').option('ignoreCorruptFiles','false').option('ignoreMissingFiles','false').parquet(*evidence.paths(n)) for n in m.TABLES}
            verify_frames(expected,Frames(actual[m.EVENTS],actual[m.OCCURRENCES],actual[m.METRICS]));verify_ranks(actual[m.EVENTS],actual[m.METRICS])
        with ledger.span('final_rechecks'):
            deps.check();staged.check();evidence.check();guard()
        with ledger.span('finalize'):
            marker['completed_at']=m.now();raw=canonical_bytes(marker)
            m.write(directory/'complete.tmp',raw)
            temporary=m.capture(directory,{'inputs.json':m.MANIFEST_LIMIT,'complete.tmp':m.COMPLETION_LIMIT},m.TABLES,'PUBLICATION_FAILED','PUBLICATION_CHANGED',guards=m.namespace_guards(root,directory))
            _compare_phase(evidence,temporary)
            m.need(temporary.raw('complete.tmp')==raw,'PUBLICATION_FAILED');m.validate_completion(temporary.raw('complete.tmp'),deps.fingerprint,len(deps.manifest_bytes))
            deps.check();staged.check();temporary.check();guard()
            m.need(not (directory/'complete.json').exists(),'PUBLICATION_CHANGED')
            (directory/'complete.tmp').rename(directory/'complete.json')
        return Publication('published',deps.fingerprint,m.freeze(counts),m.freeze(marker['output_counts']),deps.duplicate_batch_selections)
    finally:
        pending=sys.exc_info()[0] is not None
        for frame in held:
            try:frame.unpersist()
            except Exception:
                if not pending:raise


PHASES=('dependency_open','staging','normalization','aggregation','output_write','spark_readback',
    'independent_pending_verify','final_rechecks','finalize','overhead')


class Ledger:
    def __init__(self):self.started=time.perf_counter_ns();self.phases=dict.fromkeys(PHASES,0);self.active=False

    @contextmanager
    def span(self,name):
        m.need(name in PHASES and name!='overhead' and not self.active,'BENCHMARK_INVALID')
        self.active=True;start=time.perf_counter_ns()
        try:yield
        finally:self.phases[name]+=time.perf_counter_ns()-start;self.active=False

    def finish(self):
        elapsed=time.perf_counter_ns()-self.started
        self.phases['overhead']=elapsed-sum(self.phases.values())
        m.need(self.phases['overhead']>=0,'BENCHMARK_INVALID')
        return dict(pipeline_ns=elapsed,exclusive_phases_ns=self.phases)


def validate_execution_config(value):
    m.keys(value,('master','shuffle_partitions','default_parallelism','driver_memory_mib',
        'executor_memory_mib','output_partitions','event_log_enabled'),'ARGUMENT_INVALID')
    m.need(value['master'] in ('local[1]','local[2]'),'ARGUMENT_INVALID')
    for k,v in dict(shuffle_partitions=8,default_parallelism=8,driver_memory_mib=1024,
                   executor_memory_mib=1024,output_partitions=4).items():m.integer(value[k],v,v,'ARGUMENT_INVALID')
    m.need(type(value['event_log_enabled']) is bool,'ARGUMENT_INVALID')


def validate_execution(spark,value):
    validate_execution_config(value)
    m.need(spark.version=='4.0.2' and spark.sparkContext.master==value['master'] and
        spark.sparkContext.defaultParallelism==8,'UNSUPPORTED_VERSION')
    conf=spark.sparkContext.getConf()
    m.need(conf.get('spark.eventLog.enabled','false')==str(value['event_log_enabled']).lower() and
        conf.get('spark.driver.memory')=='1g' and conf.get('spark.executor.memory')=='1g','ARGUMENT_INVALID')


def publish(spark, *,batch_root,input_format,batches,reference_root,reference_id,revision,
            reference_fingerprint,staging_root,output_root,execution_config,observer=None):
    ledger=Ledger()
    try:
        result=_publish(spark,batch_root=batch_root,input_format=input_format,batches=batches,
            reference_root=reference_root,reference_id=reference_id,revision=revision,
            reference_fingerprint=reference_fingerprint,staging_root=staging_root,output_root=output_root,
            execution_config=execution_config,ledger=ledger)
        timing=ledger.finish()
        if observer is not None:
            try:observer(timing)
            except Exception:raise m.ReliabilityFailure('BENCHMARK_INVALID') from None
        return result
    except m.ReliabilityFailure as e:
        e.diagnostic['input_format']=input_format if type(input_format) is str and input_format in m.DECODERS else None
        raise
    except Exception:raise m.ReliabilityFailure('PUBLICATION_FAILED') from None


def main(argv=None):
    spark=None
    try:
        options=m.arguments(sys.argv[1:] if argv is None else argv,{'batch-root','batch','reference-root','reference-id','revision','reference-fingerprint','staging-root','output-root','input-format','execution-config'},{'event-log-root'},repeat=('batch',))
        if options is None:
            print('Reliability v2 publication: --batch-root --input-format JSONL|PARQUET --batch SOURCE/BATCH/HEX --reference-root --reference-id --revision --reference-fingerprint --staging-root --output-root --execution-config [--event-log-root when event logging is enabled]');return 0
        selections=[tuple(v.split('/')) for v in options.pop('batch')]
        for item in selections:
            m.need(len(item)==3,'ARGUMENT_INVALID');m.match(item[0],m.SOURCE);m.match(item[1],m.ID);m.match(item[2])
        for key,pattern in (('reference_id',m.ID),('revision',m.REVISION),('reference_fingerprint',m.HEX)):m.match(options[key],pattern)
        from pathlib import Path
        config=m.observe(Path(options.pop('execution_config')),m.PROFILE_LIMIT,'INPUT_UNAVAILABLE','INPUT_CHANGED',retain=True)
        execution=m.parse(config.raw,m.PROFILE_LIMIT,'ARGUMENT_INVALID')
        validate_execution_config(execution)
        m.need(options['input_format'] in m.DECODERS,'ARGUMENT_INVALID')
        logroot=m.root_path(options.pop('event_log_root')) if 'event_log_root' in options else None
        m.need(not execution['event_log_enabled'] or logroot is not None,'ARGUMENT_INVALID')
        from pyspark.sql import SparkSession
        builder=SparkSession.builder.master(execution['master']).appName('ddip-reliability-format-publication').config('spark.ui.enabled','false').config('spark.default.parallelism','8').config('spark.driver.memory','1g').config('spark.executor.memory','1g').config('spark.eventLog.enabled',str(execution['event_log_enabled']).lower())
        if logroot is not None:builder=builder.config('spark.eventLog.dir',str(logroot))
        spark=builder.getOrCreate()
        options['execution_config']=execution
        result=publish(spark,batches=selections,**options)
        raw=canonical_bytes(m.public(result.summary()));m.need(len(raw)+1<=4096,'RESPONSE_LIMIT')
        # Stop before success serialization so shutdown failure cannot emit partial success.
        spark.stop();spark=None
        print(raw.decode());return 0
    except m.ReliabilityFailure as error:return m.cli_error(error)
    except Exception:return m.cli_error(m.ReliabilityFailure('PUBLICATION_FAILED'))
    finally:
        if spark is not None:
            try:spark.stop()
            except Exception:pass


if __name__=='__main__':raise SystemExit(main())
