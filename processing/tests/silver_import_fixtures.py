"""Synthetic native M1 setup and a read-only loopback S3 protocol fixture, never production code."""
from contextlib import contextmanager
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
from pathlib import Path
import threading
from urllib.parse import unquote
from bronze_to_silver import encode, process_one


class Store:
    def __init__(self):self.objects={};self.reads=[]
    def get_object(self, Bucket, Key):self.reads.append(Key);return dict(Body=io.BytesIO(self.objects[(Bucket,Key)]))


def create(spark, base):
    base=Path(base);base.mkdir(exist_ok=True);root=base/'silver';root.mkdir(exist_ok=True)
    objects=Store();selections=[]
    payloads=[b'<rss version="2.0"><channel><item><guid>one</guid><title>One</title></item><item><title>Missing ID</title></item></channel></rss>',
              b'<feed xmlns="http://www.w3.org/2005/Atom"><entry><id>atom-one</id><title>Atom One</title></entry></feed>',
              b'<rss version="2.0"><channel><item><guid>missing-title</guid></item></channel></rss>']
    for index,payload in enumerate(payloads):
        source='synthetic-rss' if index!=1 else 'synthetic-atom';ingestion=f'50000000-0000-4000-8000-{index+1:012d}'
        key=f'bronze/rss/source_id={encode(source)}/ingestion_date=2026-10-12/ingestion_id={ingestion}/metadata.json'
        metadata=dict(schemaVersion=1,ingestionId=ingestion,sourceId=source,sourceUri='https://example.invalid/synthetic',ingestedAt='2026-10-12T13:00:00.123456789Z',httpStatus=200,contentType='application/xml',declaredContentLength=None,payloadByteLength=len(payload),payloadSha256=sha256(payload).hexdigest(),payloadKey=key.removesuffix('metadata.json')+'payload.xml')
        objects.objects[('local',key)]=json.dumps(metadata).encode();objects.objects[('local',metadata['payloadKey'])]=payload
        process_one(spark,objects,'local',key,root)
        address='v1/'+sha256(b'local').hexdigest()+'/'+key[len('bronze/rss/'):].removesuffix('metadata.json')+'complete.json'
        selections.append(dict(kind='SILVER',productId='content-intelligence',storageBindingId='silver-test',relativeAddress=address,contractVersion=1,processorRevision=1,
                               identity=dict(sourceId=source,ingestionId=ingestion,bronzeStorageBindingId='bronze-test',bronzeBucket='local',bronzeMetadataKey=key,bronzePayloadKey=metadata['payloadKey'])))
    return root,objects,selections


@contextmanager
def serve(store):
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            store.requests.append((self.command,self.path,self.headers.get('Authorization','')))
            try:bucket,key=unquote(self.path).lstrip('/').split('/',1)
            except ValueError:bucket,key='',''
            data=store.objects.get((bucket,key));status=store.status.get((bucket,key),200 if data is not None else 404)
            if status!=200:data=b'<Error><Code>AccessDenied</Code></Error>'
            self.send_response(status);self.send_header('Content-Length',str(len(data)));self.end_headers();self.wfile.write(data)
        def do_PUT(self):store.writes.append(self.path);self.send_error(405)
        def log_message(self,*args):pass
    store.requests=[];store.status={};store.writes=[]
    server=ThreadingHTTPServer(('127.0.0.1',0),Handler);thread=threading.Thread(target=server.serve_forever,daemon=True);thread.start()
    try:yield dict(endpoint=f'http://127.0.0.1:{server.server_port}',region='us-east-1',bucket='local',accessKey='synthetic-access',secretKey='synthetic-secret')
    finally:server.shutdown();server.server_close();thread.join(2)


if __name__=='__main__':
    import sys
    from publication_metadata import canonical_bytes
    from silver import session
    from pyspark import SparkContext
    from silver_import_qualification import quiet_stdout
    base=Path(sys.argv[1]).resolve()
    with quiet_stdout():
        spark=session();spark.sparkContext.setLogLevel('ERROR')
        try:root,store,selections=create(spark,base)
        finally:
            spark.stop();gateway=SparkContext._gateway;gateway.shutdown();gateway.proc.stdin.close();gateway.proc.wait(timeout=5)
    objects=[]
    for (bucket,key),data in store.objects.items():
        file=base/'objects'/bucket/key;file.parent.mkdir(parents=True,exist_ok=True);file.write_bytes(data)
        objects.append(dict(bucket=bucket,key=key,file=str(file.relative_to(base))))
    sys.stdout.buffer.write(canonical_bytes(dict(artifacts=selections,root=str(root),objects=objects)))
