"""Synthetic D5a reference registration and standalone equivalence; never an operator command."""
from pathlib import Path
import json
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
import artifact_import_verifier as p
from publication_metadata import canonical_bytes


def reference(root,binding):
    import publisher_reference
    root=Path(root);work=root.parent
    csv=work/(root.name+'-reference.csv');vocab=work/(root.name+'-vocab.json')
    csv.write_bytes(b'source_id,publisher_name,publisher_category,declared_language,country_code\ndemo-feed,Demo Publisher,science,en,CA\nunused-feed,Unused Publisher,technology,en,US\n')
    vocab.write_bytes(b'{"country_code":["CA","GB","US"],"declared_language":["en","es","fr"],"publisher_category":["business","culture","general-news","science","technology"],"schema_version":1,"version":1}')
    result=publisher_reference.publish(root,csv,vocab,'fixture-publishers','r1','test-team')
    a=dict(kind='PUBLISHER_REFERENCE',productId='content-intelligence',storageBindingId=binding,relativeAddress='reference_id=fixture-publishers/revision=r1',contractVersion=1,processorRevision=1,identity=dict(referenceId='fixture-publishers',revision='r1'),fingerprint=result['snapshot_fingerprint'])
    st=root.stat();b=dict(root=str(root),rootDevice=str(st.st_dev),rootInode=str(st.st_ino))
    before=p.capture(a,b);native=p.qualify(a,b);p.capture(a,b,before)
    return dict(qualificationVersion=1,selection=a,verification=dict(evidenceVersion=1,native=native,capture=before))


def equivalent(command,output):
    import managed_content as adapter
    import content_publication as native
    import content_analytics
    from silver import session
    import os
    os.environ["PYSPARK_PYTHON"]=sys.executable
    c=json.loads(Path(command).read_bytes());args,prepared=adapter.dependencies(c)
    spark=session()
    try:
        replay=native.publish(spark,output_root=c["content"]["root"],**args)
        assert replay["status"]=="replayed"
        result=native.publish(spark,output_root=output,**args)
        assert result['product_fingerprint']==prepared['fingerprint']
    finally:spark.stop()
    with content_analytics.open_publication(output,prepared['fingerprint'],gold_root=args['gold_root'],reference_root=args['reference_root']) as stand,content_analytics.open_publication(c['content']['root'],prepared['fingerprint'],gold_root=args['gold_root'],reference_root=args['reference_root']) as managed:
        for name in ('catalog','reference_coverage','source_discovery_daily'):assert stand.report(name)==managed.report(name)
    return 'EQUIVALENT'


if __name__=='__main__':
    if sys.argv[1]=='reference':sys.stdout.buffer.write(canonical_bytes(reference(*sys.argv[2:])))
    else:print(equivalent(*sys.argv[2:]))
