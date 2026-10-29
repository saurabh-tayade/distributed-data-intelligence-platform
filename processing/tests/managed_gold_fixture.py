"""Synthetic D4 equivalence fixture: standalone native Gold, never a production adapter."""
import json
from pathlib import Path
import sys
sys.path.insert(0,str(Path(__file__).resolve().parents[1]))
from silver import session
from silver_import_qualification import quiet_stdout
from pyspark import SparkContext
from gold_publication import run
from gold_analytics import open_publication

def rows(root,identity):
    with open_publication(root,identity) as opened:
        return {t:sorted([json.dumps(row,default=str,sort_keys=True) for row in opened._connection.execute('SELECT * FROM '+t).fetchall()]) for t in ('gold_articles','gold_source_daily')}

c=json.loads(Path(sys.argv[1]).read_bytes());output=Path(sys.argv[2]);spark=None
with quiet_stdout():
    try:
        spark=session();spark.sparkContext.setLogLevel('ERROR')
        result=run(spark,c['silver']['root'],output,markers=list(reversed([i['qualification']['selection']['relativeAddress'] for i in c['inputs']])))
        identity=c['prepared']['inputSetFingerprint']
        assert result['input_set_fingerprint']==identity and rows(output,identity)==rows(c['gold']['root'],identity)
    finally:
        if spark is not None:spark.stop()
        gateway=SparkContext._gateway;gateway.shutdown();gateway.proc.stdin.close();gateway.proc.wait(timeout=5)
print('EQUIVALENT')
