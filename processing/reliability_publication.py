"""Immutable E2 publication/replay; one trusted local writer, completion last."""
from dataclasses import dataclass
import sys
from publication_metadata import canonical_bytes
import reliability_metadata as m
from reliability_inputs import verify_dependencies,stage
import reliability_analytics as analytics


@dataclass(frozen=True)
class Publication:
    status: str
    product_fingerprint: str
    occurrence_counts: object
    output_counts: object
    duplicate_batch_selections: int

    def summary(self):
        return dict(status=self.status,product_id=m.PRODUCT,product_fingerprint=self.product_fingerprint,
                    origin='synthetic',label=m.LABEL,occurrence_counts=self.occurrence_counts,
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
    result=dict(completion_version=1,product_id=m.PRODUCT,product_fingerprint=deps.fingerprint,
        manifest_byte_length=len(deps.manifest_bytes),occurrence_counts=counts,
        output_counts={n:f.count() for n,f in frames.tables().items()},files=evidence.inventory(),completed_at=m.now())
    m.validate_completion(canonical_bytes(result),deps.fingerprint,len(deps.manifest_bytes))
    return result


def _publish(spark, *,batch_root,batches,reference_root,reference_id,revision,reference_fingerprint,staging_root,output_root):
    root=m.root_path(output_root,'PUBLICATION_UNAVAILABLE')
    v=m.info(root,True,'PUBLICATION_UNAVAILABLE','PUBLICATION_CHANGED');root_id=(v.st_dev,v.st_ino,v.st_mode)
    deps=verify_dependencies(batch_root=batch_root,batches=batches,reference_root=reference_root,
        reference_id=reference_id,revision=revision,reference_fingerprint=reference_fingerprint,output_root=root,staging_root=staging_root)
    directory=m.publication_directory(root,deps.fingerprint)
    for parent in (root/'v1',directory.parent):
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
    from reliability_processing import session,normalize,aggregate,verify_frames,verify_ranks,Frames,SCHEMAS
    m.need(spark.version=='4.0.2','UNSUPPORTED_VERSION');sql=session(spark)
    staged=stage(deps.batches,staging_root,deps.fingerprint)
    staged.check();held=[]
    try:
        normalized,counts=normalize(sql,deps,staged)
        events=normalized.events.cache();held.append(events);events.count()
        occurrences=normalized.occurrences.cache();held.append(occurrences);occurrences.count()
        metrics=aggregate(events).cache();held.append(metrics);metrics.count()
        expected=Frames(events,occurrences,metrics)
        verify_frames(expected,expected);verify_ranks(events,metrics)
        deps.check();staged.check();m.info(root,True,'PUBLICATION_FAILED','PUBLICATION_CHANGED',root_id)
        for parent in (root/'v1',directory.parent):
            try:parent.mkdir()
            except FileExistsError:pass
            m.info(parent,True,'PUBLICATION_FAILED','PUBLICATION_CHANGED')
        directory.mkdir()
        guard_paths=(root,root/'v1',directory.parent,directory)
        guards=tuple((p,(i.st_dev,i.st_ino,i.st_mode)) for p in guard_paths for i in (m.info(p,True,'PUBLICATION_FAILED','PUBLICATION_CHANGED'),))
        def guard():
            for path,identity in guards:m.info(path,True,'PUBLICATION_FAILED','PUBLICATION_CHANGED',identity)
        guard();m.write(directory/'inputs.json',deps.manifest_bytes)
        for name,frame in expected.tables().items():
            guard();m.need(frame.schema.simpleString()==SCHEMAS[name].simpleString())
            frame.write.mode('errorifexists').parquet(str(directory/name));guard()
        evidence=m.capture(directory,{'inputs.json':m.MANIFEST_LIMIT},m.TABLES,'PUBLICATION_FAILED','PUBLICATION_CHANGED',guards=m.namespace_guards(root,directory))
        m.need(evidence.raw('inputs.json')==deps.manifest_bytes,'PUBLICATION_CHANGED')
        marker=_marker(deps,counts,expected,evidence)
        db,stats=analytics.verify_core(deps,evidence,marker);db.close()
        actual={n:sql.read.schema(SCHEMAS[n]).option('mergeSchema','false').option('ignoreCorruptFiles','false').option('ignoreMissingFiles','false').parquet(*evidence.paths(n)) for n in m.TABLES}
        verify_frames(expected,Frames(actual[m.EVENTS],actual[m.OCCURRENCES],actual[m.METRICS]));verify_ranks(actual[m.EVENTS],actual[m.METRICS])
        deps.check();staged.check();evidence.check();guard()
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


def publish(spark, *, batch_root,batches,reference_root,reference_id,revision,reference_fingerprint,staging_root,output_root):
    try:return _publish(spark,batch_root=batch_root,batches=batches,reference_root=reference_root,reference_id=reference_id,
                        revision=revision,reference_fingerprint=reference_fingerprint,staging_root=staging_root,output_root=output_root)
    except m.ReliabilityFailure:raise
    except Exception:raise m.ReliabilityFailure('PUBLICATION_FAILED') from None


def main(argv=None):
    spark=None
    try:
        options=m.arguments(sys.argv[1:] if argv is None else argv,{'batch-root','batch','reference-root','reference-id','revision','reference-fingerprint','staging-root','output-root'},repeat=('batch',))
        if options is None:
            print('Reliability publication: --batch-root --batch SOURCE/BATCH/HEX --reference-root --reference-id --revision --reference-fingerprint --staging-root --output-root');return 0
        selections=[tuple(v.split('/')) for v in options.pop('batch')]
        for item in selections:
            m.need(len(item)==3,'ARGUMENT_INVALID');m.match(item[0],m.SOURCE);m.match(item[1],m.ID);m.match(item[2])
        for key,pattern in (('reference_id',m.ID),('revision',m.REVISION),('reference_fingerprint',m.HEX)):m.match(options[key],pattern)
        from pyspark.sql import SparkSession
        spark=SparkSession.builder.master('local[2]').appName('ddip-reliability-publication').config('spark.ui.enabled','false').getOrCreate()
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
