"""Disposable real Bronze/Silver/Gold/Content fixtures, never production authority.

Receipts have all historical fields, but are synthetic test inputs, not journal
or database delivery evidence. Native captures and qualification are real.
"""
from contextlib import contextmanager
from copy import deepcopy
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import io
import json
import os
import subprocess
import sys
from pathlib import Path
import threading
from urllib.parse import unquote

import artifact_import_verifier as p
import managed_silver
import narrow_projection_transport as transport
from publication_metadata import canonical_bytes
from projection_transport_fixtures import RUN, ATTEMPT, CHECKPOINT, ARTIFACT, REFERENCE, SHA, digest


def binding(root):
    root = Path(root).resolve()
    value = root.stat()
    return dict(root=str(root), rootDevice=str(value.st_dev), rootInode=str(value.st_ino))


class Store:
    def __init__(self, objects):
        self.objects = objects
        self.reads = []
        self.writes = []

    def get_object(self, Bucket, Key):
        self.reads.append((Bucket, Key))
        data = self.objects[Bucket, Key]
        return dict(Body=io.BytesIO(data), ContentLength=len(data),
                    ContentType='application/json' if Key.endswith('metadata.json') else 'application/xml')


@contextmanager
def serve(store):
    """Full receipt reader checks both S3 metadata and bytes. GET only."""
    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            bucket, key = unquote(self.path).lstrip('/').split('/', 1)
            try:
                response = store.get_object(Bucket=bucket, Key=key)
            except KeyError:
                self.send_error(404)
                return
            self.send_response(200)
            self.send_header('Content-Type', response['ContentType'])
            self.send_header('Content-Length', str(response['ContentLength']))
            self.end_headers()
            self.wfile.write(response['Body'].read())

        def do_PUT(self):
            store.writes.append(self.path)
            self.send_error(405)

        def log_message(self, *args):
            pass

    server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield dict(endpoint=f'http://127.0.0.1:{server.server_port}', region='us-east-1', bucket='local')
    finally:
        server.shutdown()
        server.server_close()
        thread.join(2)


@contextmanager
def source(spark, base, *, empty=False):
    import bronze_to_silver as native
    import silver_import_qualification as q
    from silver_inputs import parse_marker, load_inputs
    from managed_content_fixture import reference

    base = Path(base).resolve()
    base.mkdir()
    roots = {role: base / role.lower() for role in ('GOLD', 'CONTENT', 'REFERENCE', 'SILVER', 'TEMP')}
    for root in roots.values():
        root.mkdir(mode=0o700)
    bindings = {role: binding(root) for role, root in roots.items()}
    ingestion = '50000000-0000-4000-8000-000000000001'
    key = f"bronze/rss/source_id={native.encode('demo-feed')}/ingestion_date=2026-11-04/ingestion_id={ingestion}/metadata.json"
    payload = (b'<rss version="2.0"><channel/></rss>' if empty else
               '<rss version="2.0"><channel><item><guid>one</guid><title>One é🙂</title></item></channel></rss>'.encode())
    metadata = dict(schemaVersion=1, ingestionId=ingestion, sourceId='demo-feed',
        sourceUri='https://example.invalid/native-v2', ingestedAt='2026-11-04T13:00:00Z',
        httpStatus=200, contentType='application/xml', declaredContentLength=None,
        payloadByteLength=len(payload), payloadSha256=sha256(payload).hexdigest(),
        payloadKey=key.removesuffix('metadata.json') + 'payload.xml')
    raw = canonical_bytes(metadata)
    receipt = dict(receiptVersion=1, runId=RUN, attemptId=ATTEMPT, sourceId='demo-feed', revision=1,
        capturedConfigSha256=SHA, ingestionId=ingestion, ingestedAt=metadata['ingestedAt'],
        payloadByteLength=len(payload), payloadSha256=metadata['payloadSha256'], journalSha256=SHA,
        storageBindingId='bronze', storageBindingSha256=SHA, bucket='local',
        expected=dict(metadataKey=key, payloadKey=metadata['payloadKey'], metadataByteLength=len(raw),
                      metadataSha256=sha256(raw).hexdigest()),
        payloadContentType='application/xml', metadataContentType='application/json')
    store = Store({('local', key): raw, ('local', metadata['payloadKey']): payload})
    exact = managed_silver.bronze(store, receipt)
    native.process_one(spark, exact, 'local', key, roots['SILVER'])
    artifact = managed_silver.selection(receipt, 'silver')
    local = q.capture(artifact, bindings['SILVER'])
    marker, _, _ = parse_marker(roots['SILVER'], artifact['relativeAddress'])
    bronze, expected_rows = q.bronze_read(artifact, None, marker, store)
    # The registered Silver shape requires confirmed shutdown. A separate
    # owned qualifier process establishes that fact without stopping the shared
    # producer session or inventing a cleanup outcome.
    runtime = dict(javaHome=str(Path(os.environ['JAVA_HOME']).resolve()),
        scratchRoot=bindings['TEMP']['root'], scratchDevice=bindings['TEMP']['rootDevice'],
        scratchInode=bindings['TEMP']['rootInode'])
    code = ('import json,sys;sys.path.insert(0,sys.argv[1]);'
            'import silver_import_qualification as q;'
            'from publication_metadata import canonical_bytes;'
            'a,b,l,m,e,r=json.load(sys.stdin);'
            'from datetime import datetime;e["ingested_at"]=datetime.fromisoformat(e["ingested_at"]);'
            'sys.stdout.buffer.write(canonical_bytes(q.spark_qualify(a,b,l,m,e,r)))')
    qualified = subprocess.run([sys.executable, '-I', '-B', '-c', code, str(Path(q.__file__).parent)],
        input=canonical_bytes([artifact, bindings['SILVER'], local, marker,
            expected_rows | dict(ingested_at=expected_rows['ingested_at'].isoformat()), runtime]),
        capture_output=True, timeout=180)
    if qualified.returncode:
        raise AssertionError(qualified.stderr.decode()[-6000:])
    qualification_native = json.loads(qualified.stdout)
    inventory, identity_sha = q.identity(artifact, local, bronze)
    qualification = dict(qualificationVersion=1, selection=artifact, verification=dict(evidenceVersion=2,
        native=qualification_native, capture=dict(local=local, bronze=bronze),
        inventoryIdentity=dict(kind='E5_SILVER_INVENTORY_V1', sha256=identity_sha, value=inventory)))
    q.capture(artifact, bindings['SILVER'], local)
    refq = reference(roots['REFERENCE'], 'reference')
    selected_reference = refq['selection']
    ref = {k: selected_reference[k] for k in ('kind', 'productId', 'storageBindingId', 'relativeAddress', 'contractVersion', 'processorRevision')}
    ref.update(artifactId=REFERENCE, nativeIdentity=canonical_bytes(selected_reference['identity']).decode('utf-8'), evidenceSha256=digest(refq),
        verifiedAt='2026-11-04T13:00:00Z', qualification=refq, role='PUBLISHER_REFERENCE', ordinal=0)
    credentials = base / 'credentials.json'
    credentials.write_bytes(canonical_bytes(dict(accessKey='synthetic-access', secretKey='synthetic-secret')))
    with serve(store) as configuration:
        selected = dict(artifactId=ARTIFACT, originRunId=RUN, sourceId=receipt['sourceId'],
            evidenceSha256=digest(qualification), qualification=qualification, receipt=receipt,
            bronze=configuration | dict(credentialsFile=str(credentials)), credentialsSha256=sha256(credentials.read_bytes()).hexdigest())
        silver = dict(outputBindingId='silver', tempBindingId='temp', javaHome=str(Path(os.environ['JAVA_HOME']).resolve()))
        gold = dict(goldBindingId='gold', silverRuntimeId='silver-runtime', silverRuntimeSha256=digest(silver))
        content = dict(contentBindingId='content', referenceBindingId='reference', goldRuntimeId='gold-runtime', goldRuntimeSha256=digest(gold))
        expectations = dict(policy=transport.POLICY, readyContentRuntimeSha256=digest(content),
            contentRuntime=dict(id='content-runtime', configuration=content, sha256=digest(content)),
            goldRuntime=dict(id='gold-runtime', configuration=gold, sha256=digest(gold)),
            silverRuntime=dict(id='silver-runtime', configuration=silver, sha256=digest(silver)),
            bindings={role: dict(bindingId=role.lower(), role=role, configuration=value) for role, value in bindings.items()},
            original=dict(runId=RUN, attemptId=ATTEMPT, sourceId=receipt['sourceId'], silverInput=selected),
            reference=ref, goldProof=None)
        native_runtime = dict(javaHome=silver['javaHome'], scratchRoot=bindings['TEMP']['root'],
            scratchDevice=bindings['TEMP']['rootDevice'], scratchInode=bindings['TEMP']['rootInode'])
        common = dict(mode='PUBLISH', runId=RUN, attemptId=ATTEMPT, ownerEpoch=3, checkpointId=CHECKPOINT,
                      runtime=native_runtime, verified=None)
        inputs = load_inputs(spark, roots['SILVER'], markers=[artifact['relativeAddress']])
        prepared = dict(manifest=inputs.manifest, inputSetFingerprint=inputs.input_set_fingerprint, counts=inputs.counts)
        command = transport.create_command_v2('GOLD', common | dict(runtimeSha256=digest(gold),
            inputSha256=digest([selected]), silverBindingId='silver', inputs=[selected], silver=bindings['SILVER'],
            gold=bindings['GOLD'], prepared=prepared), expectations=expectations)
        yield dict(expected=expectations, gold=command, store=store, base=base, common=common)


def content_command(case, gold_verification):
    from content_dependencies import verify_dependencies
    expected = case['expected']
    proof = transport.gold_proof_v2(case['gold'], gold_verification, artifact_id=ARTIFACT)
    expected['goldProof'] = deepcopy(proof)
    ref = expected['reference']['qualification']['selection']
    roots = {role: value['configuration'] for role, value in expected['bindings'].items()}
    args = dict(gold_root=roots['GOLD']['root'], gold_fingerprint=gold_verification['prepared']['inputSetFingerprint'],
        reference_root=roots['REFERENCE']['root'], reference_id=ref['identity']['referenceId'],
        revision=ref['identity']['revision'], reference_fingerprint=ref['fingerprint'])
    dependencies = verify_dependencies(**args, output_root=roots['CONTENT']['root'])
    prepared = dict(manifest=p.thaw(dependencies.manifest()), fingerprint=dependencies.fingerprint)
    fields = case['common'] | dict(runtimeSha256=expected['contentRuntime']['sha256'], goldProof=proof,
        reference=expected['reference'], referenceBindingId='reference', referenceBinding=roots['REFERENCE'],
        content=roots['CONTENT'], prepared=prepared)
    fields['inputSha256'] = digest(transport._dependencies(fields))
    return transport.create_command_v2('CONTENT', fields, expectations=expected), args


def envelopes(gold, gv, content, cv):
    """All actual complete bodies, including the three exact resume boundaries."""
    result = []
    for kind, command, verification in [('GOLD', gold, gv), ('CONTENT', content, cv)]:
        names = ['publish', 'prepare', 'verify', 'recheck', 'prepared', 'verification', 'artifact',
                 'recovery_prepare', 'recovery_verify', 'recovery_evidence']
        destinations = [kind + '_COMMAND'] * 4 + [kind + '_EVIDENCE'] * 2 + [kind + '_ARTIFACT',
            'RECOVERY_COMMAND', 'RECOVERY_COMMAND', 'RECOVERY_EVIDENCE']
        if kind == 'CONTENT':
            names.append('inspect'); destinations.append('CONTENT_COMMAND')
        names.extend(['observation_prepared', 'observation_verified'])
        destinations.extend(['RECOVERY_OBSERVATION'] * 2)
        result.extend((kind.lower() + '_' + name, destination, raw) for name, destination, raw in
                      zip(names, destinations, transport.qualify_publication_v2(command, verification), strict=True))
    for boundary in ('BEFORE_FETCH', 'GOLD', 'CONTENT_INPUTS'):
        args = {} if boundary == 'BEFORE_FETCH' else dict(gold_verification=gv, prepared=cv['prepared'])
        request, evidence = transport.resume_v2(content, boundary, **args)
        result.extend([(f'resume_{boundary}_command', 'RESUME_COMMAND', request),
                       (f'resume_{boundary}_evidence', 'RESUME_EVIDENCE', evidence)])
    return result
