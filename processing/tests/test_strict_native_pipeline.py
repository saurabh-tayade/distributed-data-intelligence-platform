"""Real strict subprocess chain, with GET-only loopback Bronze test storage.

Every Spark/Parquet/native verifier and every evidence sink is real. The storage
fixture serves fixed receipt-bound bytes; it is not an S3 writer qualification or
Java/SQL lifecycle test. Those remain separate integration obligations.
"""
from copy import deepcopy
from dataclasses import replace
from hashlib import sha256
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import importlib.util
import json
import os
from pathlib import Path
import signal
import subprocess
import sys
import tempfile
import threading
import unittest
from urllib.parse import unquote
from uuid import uuid4

sys.path[:0] = [str(Path(__file__).resolve().parent), str(Path(__file__).resolve().parents[1])]
import narrow_evidence_sink as ledger
import strict_native_content as child
from narrow_transport_json import canonical_envelope


def digest(value):
    return sha256(canonical_envelope(value, 1024**2)).hexdigest()


def binding(path):
    info = path.stat()
    return dict(root=str(path), rootDevice=str(info.st_dev), rootInode=str(info.st_ino))


@unittest.skipUnless(importlib.util.find_spec('pyspark'), 'requires pinned native processing runtime')
class StrictNativePipelineTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix='strict-native-pipeline-')
        self.addCleanup(self.temp.cleanup)
        self.base = Path(self.temp.name).resolve()
        self.paths = {role: self.base / role.lower() for role in ('CONTROL', 'SILVER', 'GOLD', 'CONTENT', 'REFERENCE', 'TEMP')}
        for path in self.paths.values():
            path.mkdir(mode=0o700)
        self.run, self.attempt, self.installation, self.worker = (str(uuid4()) for _ in range(4))
        self.source = 'source-one'
        self.ingestion = str(uuid4())
        self.ingested = '2026-11-04T13:00:00.123456789Z'
        self.credentials = self.base / 'credentials.json'
        self.credentials.write_bytes(canonical_envelope(dict(accessKey='local-test', secretKey='local-test-secret'), 8192))
        self.credentials.chmod(0o600)
        import bronze_to_silver as native
        key = 'bronze/rss/source_id=' + native.encode(self.source) + '/ingestion_date=2026-11-04/ingestion_id=' + self.ingestion + '/metadata.json'
        payload = (b'<rss version="2.0"><channel><item><guid>one</guid><title>One</title>'
                   b'<link>https://example.invalid/one</link></item><item><title>No ID</title></item></channel></rss>')
        metadata = dict(schemaVersion=1, ingestionId=self.ingestion, sourceId=self.source,
            sourceUri='https://example.invalid/feed', ingestedAt=self.ingested, httpStatus=200,
            contentType='application/xml', declaredContentLength=None, payloadByteLength=len(payload),
            payloadSha256=sha256(payload).hexdigest(), payloadKey=key.removesuffix('metadata.json') + 'payload.xml')
        raw = canonical_envelope(metadata, 65536)
        self.objects = {'/local-bucket/' + key: (raw, 'application/json'),
                        '/local-bucket/' + metadata['payloadKey']: (payload, 'application/xml')}
        self.requests = []
        owner = self
        class Handler(BaseHTTPRequestHandler):
            def do_GET(self):
                owner.requests.append(('GET', unquote(self.path)))
                item = owner.objects.get(unquote(self.path))
                if item is None:
                    self.send_error(404)
                    return
                data, content_type = item
                self.send_response(200)
                self.send_header('Content-Length', str(len(data)))
                self.send_header('Content-Type', content_type)
                self.end_headers()
                self.wfile.write(data)
            def do_PUT(self):
                owner.requests.append(('PUT', unquote(self.path)))
                self.send_error(403)
            def log_message(self, *_):
                pass
        self.server = ThreadingHTTPServer(('127.0.0.1', 0), Handler)
        thread = threading.Thread(target=self.server.serve_forever, daemon=True)
        thread.start()
        self.addCleanup(self.server.server_close)
        self.addCleanup(self.server.shutdown)
        self.bronze = dict(importBronzeVersion=1, endpoint='http://127.0.0.1:' + str(self.server.server_port), region='us-east-1',
                           bucket='local-bucket', credentialsFile=str(self.credentials))
        self.receipt = dict(receiptVersion=1, runId=self.run, attemptId=self.attempt, sourceId=self.source,
            revision=1, capturedConfigSha256='a' * 64, ingestionId=self.ingestion, ingestedAt=self.ingested,
            payloadByteLength=len(payload), payloadSha256=sha256(payload).hexdigest(), journalSha256='b' * 64,
            storageBindingId='bronze', storageBindingSha256='c' * 64, bucket='local-bucket',
            expected=dict(metadataKey=key, payloadKey=metadata['payloadKey'], metadataByteLength=len(raw), metadataSha256=sha256(raw).hexdigest()),
            payloadContentType='application/xml', metadataContentType='application/json')
        self.runtime = dict(javaHome=str(Path(os.environ['JAVA_HOME']).resolve()), scratchRoot=str(self.paths['TEMP']),
            scratchDevice=binding(self.paths['TEMP'])['rootDevice'], scratchInode=binding(self.paths['TEMP'])['rootInode'])
        silver = dict(outputBindingId='silver', tempBindingId='temp', javaHome=self.runtime['javaHome'])
        gold = dict(goldBindingId='gold', silverRuntimeId='silver-runtime', silverRuntimeSha256=digest(silver))
        content = dict(contentBindingId='content', referenceBindingId='reference', goldRuntimeId='gold-runtime', goldRuntimeSha256=digest(gold))
        self.records = {key: dict(id=key.removesuffix('Runtime') + '-runtime', configuration=value, sha256=digest(value))
                        for key, value in (('silverRuntime', silver), ('goldRuntime', gold), ('contentRuntime', content))}
        self.bindings = {role: dict(bindingId=role.lower(), role=role, configuration=binding(self.paths[role]))
                         for role in ('SILVER', 'GOLD', 'CONTENT', 'REFERENCE', 'TEMP')}
        self.root = self.paths['CONTROL'] / 'runs' / self.run / 'attempts' / self.attempt
        self.root.mkdir(parents=True, mode=0o700)
        for path in (self.root.parent, self.root.parent.parent, self.root.parent.parent.parent):
            path.chmod(0o700)
        (self.root / ledger.LOCK).touch(mode=0o600)
        def identity(path):
            info = path.stat()
            return ledger.Identity(info.st_dev, info.st_ino)
        writer = ledger.Writer(self.installation, self.worker, 1, self.run, self.attempt, 'SILVER', 0,
                               ledger.checkpoint_id(self.run, self.attempt, 'SILVER', 0), 'd' * 64)
        self.expected = ledger.Expected(self.installation, ledger.POLICY, 'ORIGINAL', self.run, self.attempt, self.source,
            self.paths['CONTROL'], identity(self.paths['CONTROL']), identity(self.root), identity(self.root / ledger.LOCK),
            '0' * 64, ledger.Frozen('a' * 64, 'b' * 64, digest(content)), writer)
        raw_ledger = canonical_envelope(ledger._plan(self.expected), ledger.LEDGER_LIMIT)
        (self.root / ledger.MANIFEST).write_bytes(raw_ledger)
        (self.root / ledger.MANIFEST).chmod(0o600)
        self.expected = replace(self.expected, ledger_sha256=sha256(raw_ledger).hexdigest())

    def common(self, family, mode):
        if family == 'BRONZE':
            origin, ordinal = 'INSPECT_BRONZE', 0
        else:
            _, _, origin, ordinal = child._OPERATIONS[family, mode]
        return dict(protocolVersion=2, mode=mode, runId=self.run, attemptId=self.attempt, ownerEpoch=1,
                    checkpointId=ledger.checkpoint_id(self.run, self.attempt, origin, ordinal),
                    runtimeSha256='e' * 64 if family == 'BRONZE' else self.records[family.lower() + 'Runtime']['sha256'])

    def launch(self, family, mode, command, expectations):
        stage, ordinal = ('INSPECT_BRONZE', 0) if family == 'BRONZE' else child._OPERATIONS[family, mode][:2]
        command = command | self.common(family, mode)
        raw = canonical_envelope(command, ledger._slots(stage)['command.json'])
        writer = replace(self.expected.writer, stage=stage, ordinal=ordinal,
            checkpoint_id=ledger.checkpoint_id(self.run, self.attempt, stage, ordinal), command_sha256=sha256(raw).hexdigest())
        expected = replace(self.expected, writer=writer)
        directory = self.root / ledger._address(expected, stage, ordinal)
        directory.mkdir(parents=True, mode=0o700)
        directory.parent.chmod(0o700)
        (directory / 'command.json').write_bytes(raw)
        (directory / 'command.json').chmod(0o600)
        packet = dict(bootstrapVersion=2, mode='STRICT_CONTENT', operationId=str(uuid4()),
            installationId=self.installation, policy=ledger.POLICY, planKind='ORIGINAL', runId=self.run,
            attemptId=self.attempt, sourceId=self.source, controlRoot=str(self.paths['CONTROL']),
            ledgerSha256=expected.ledger_sha256, selectionSha256=expected.frozen.selection_sha256,
            profileSha256=expected.frozen.profile_sha256, runtimeSha256=expected.frozen.runtime_sha256,
            workerInstanceId=self.worker, ownerEpoch='1', stage=stage, ordinal=ordinal,
            checkpointId=writer.checkpoint_id, commandSha256=writer.command_sha256, childPid='1',
            family=family, nativeMode=mode, nativeRuntimeSha256=command['runtimeSha256'],
            strictRuntimeSha256=child.STRICT_RUNTIME_SHA256, expectations=expectations)
        for prefix, identity in (('control', expected.control_identity), ('root', expected.root_identity), ('lock', expected.lock_identity)):
            packet[prefix + 'Device'], packet[prefix + 'Inode'] = str(identity.device), str(identity.inode)
        script = 'import sys;sys.path.insert(0,sys.argv.pop());from strict_native_content import main;sys.exit(main())'
        process = subprocess.Popen([sys.executable, '-I', '-B', '-X', 'pycache_prefix=/dev/null',
                                    '-c', script, str(Path(child.__file__).parent)],
                                   stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   start_new_session=True)
        packet['childPid'] = str(process.pid)
        try:
            stdout, stderr = process.communicate(canonical_envelope(packet, 65535), timeout=240)
        except subprocess.TimeoutExpired:
            os.killpg(process.pid, signal.SIGKILL)
            process.communicate()
            raise
        result_path = directory / 'result.json'
        result = json.loads(result_path.read_bytes()) if result_path.exists() else None
        self.assertEqual(process.returncode, 0, (family, mode, result, stderr[-5000:].decode(errors='replace')))
        self.assertEqual(stdout, b'')
        self.assertTrue(result['candidateOnly'])
        self.assertEqual(result['checkpointId'], packet['checkpointId'])
        self.assertEqual(result['nativeCheckpointId'], command['checkpointId'])
        self.assertEqual(result['commandSha256'], sha256(raw).hexdigest())
        self.assertLess(result_path.stat().st_size, 65536)
        self.assertFalse((directory / 'result.json.preparing').exists())
        needs_spark = family == 'SILVER' and mode != 'RECHECK' or family == 'GOLD' and mode != 'RECHECK' or family == 'CONTENT' and mode != 'PREPARE'
        self.assertEqual(result['cleanup']['status'], 'CONFIRMED' if needs_spark else 'NOT_STARTED')
        outputs = {}
        for name in ('evidence', 'selection'):
            path = directory / (name + '.json')
            outputs[name] = json.loads(path.read_bytes()) if path.exists() else None
            self.assertEqual(result[name + 'Sha256'], sha256(path.read_bytes()).hexdigest() if path.exists() else None)
        self.assertEqual(ledger.StrictSink(expected, ledger.Current(expected.policy, expected.frozen, writer)).inspect().reserved_entries, 221)
        return result, outputs, command

    def projection_expectations(self, selected, reference=None, proof=None):
        return self.records | dict(bindings=self.bindings, silverInputSha256=digest(selected),
            referenceSha256=digest(reference) if reference else None, goldProofSha256=digest(proof) if proof else None)

    def test_real_bronze_silver_gold_content_chain_and_readonly_checks(self):
        import artifact_import_verifier as p
        import managed_silver
        import narrow_projection_transport as transport
        import publisher_reference
        credential_digest = sha256(self.credentials.read_bytes()).hexdigest()
        self.launch('BRONZE', 'INSPECT', dict(receipt=self.receipt, bronze=self.bronze),
            dict(receiptSha256=digest(self.receipt), bronzeSha256=digest(self.bronze), credentialsSha256=credential_digest))
        silver = dict(receipt=self.receipt, bronze=self.bronze, artifact=managed_silver.selection(self.receipt, 'silver'),
                      binding=self.bindings['SILVER']['configuration'], runtime=self.runtime, expectedCapture=None)
        expectations = {key + 'Sha256': digest(value) for key, value in silver.items()}
        expectations['credentialsSha256'] = credential_digest
        self.launch('SILVER', 'PUBLISH', silver, expectations)
        _, outputs, _ = self.launch('SILVER', 'VERIFY', silver, expectations)
        verification = outputs['evidence']
        silver['expectedCapture'] = verification['capture']
        expectations['expectedCaptureSha256'] = digest(silver['expectedCapture'])
        self.launch('SILVER', 'RECHECK', silver, expectations)
        qualification = dict(qualificationVersion=1, selection=silver['artifact'], verification=verification)
        selected = dict(artifactId=str(uuid4()), originRunId=self.run, sourceId=self.source,
            evidenceSha256=digest(qualification), qualification=qualification, receipt=self.receipt,
            bronze=self.bronze, credentialsSha256=credential_digest)
        gold = dict(projectionContext=dict(projectionContextVersion=1, policy=ledger.POLICY, kind='GOLD', outputBindingId='gold'),
            inputSha256=digest([selected]), silverBindingId='silver', inputs=[selected], silver=self.bindings['SILVER']['configuration'],
            gold=self.bindings['GOLD']['configuration'], runtime=self.runtime, prepared=None, verified=None)
        compact = self.projection_expectations(selected)
        self.assertEqual(list(self.paths['GOLD'].iterdir()), [])
        _, output, _ = self.launch('GOLD', 'PREPARE', gold, compact)
        self.assertEqual(list(self.paths['GOLD'].iterdir()), [])
        gold['prepared'] = output['evidence']['prepared']
        _, published, gold_command = self.launch('GOLD', 'PUBLISH', gold, compact)
        _, verified, _ = self.launch('GOLD', 'VERIFY', gold, compact)
        self.assertEqual(published, verified)
        gold['verified'] = verified['evidence']
        self.launch('GOLD', 'RECHECK', gold, compact)
        full = self.records | dict(policy=ledger.POLICY, readyContentRuntimeSha256=self.records['contentRuntime']['sha256'],
            bindings=self.bindings, original=dict(runId=self.run, attemptId=self.attempt, sourceId=self.source, silverInput=selected),
            reference=None, goldProof=None)
        bound = transport.read_command_v2(canonical_envelope(gold_command, 1024**2), expectations=full)
        proof = transport.gold_proof_v2(bound, verified['evidence'], artifact_id=str(uuid4()))
        csv, vocabulary = self.base / 'reference.csv', self.base / 'vocabulary.json'
        csv.write_bytes(b'source_id,publisher_name,publisher_category,declared_language,country_code\nsource-one,One Press,science,en,CA\nunused,Unused Press,culture,fr,GB\n')
        vocabulary.write_bytes(canonical_envelope(publisher_reference.VOCABULARY, 65536))
        published_reference = publisher_reference.publish(self.paths['REFERENCE'], csv, vocabulary, 'fixture-publishers', 'r1', 'test')
        selection = dict(kind='PUBLISHER_REFERENCE', productId='content-intelligence', storageBindingId='reference',
            relativeAddress='reference_id=fixture-publishers/revision=r1', contractVersion=1, processorRevision=1,
            identity=dict(referenceId='fixture-publishers', revision='r1'), fingerprint=published_reference['snapshot_fingerprint'])
        ref_binding = self.bindings['REFERENCE']['configuration']
        qualification = dict(qualificationVersion=1, selection=selection,
            verification=dict(evidenceVersion=1, native=p.qualify(selection, ref_binding), capture=p.capture(selection, ref_binding)))
        reference = {key: selection[key] for key in ('productId', 'kind', 'contractVersion', 'processorRevision', 'storageBindingId', 'relativeAddress')}
        reference.update(artifactId=str(uuid4()), nativeIdentity=canonical_envelope(selection['identity'], 65536).decode('utf-8'), evidenceSha256=digest(qualification),
            verifiedAt='2026-11-04T13:01:00Z', qualification=qualification, role='PUBLISHER_REFERENCE', ordinal=0)
        content = dict(projectionContext=dict(projectionContextVersion=1, policy=ledger.POLICY, kind='CONTENT', outputBindingId='content'),
            goldProof=proof, reference=reference, referenceBindingId='reference', referenceBinding=ref_binding,
            content=self.bindings['CONTENT']['configuration'], runtime=self.runtime, prepared=None, verified=None)
        content['inputSha256'] = digest(transport._dependencies(content))
        compact = self.projection_expectations(selected, reference, proof)
        _, output, _ = self.launch('CONTENT', 'PREPARE', content, compact)
        self.assertEqual(list(self.paths['CONTENT'].iterdir()), [])
        content['prepared'] = output['evidence']['prepared']
        _, published, _ = self.launch('CONTENT', 'PUBLISH', content, compact)
        content['verified'] = published['evidence']
        _, verified, _ = self.launch('CONTENT', 'VERIFY', content, compact)
        _, rechecked, _ = self.launch('CONTENT', 'RECHECK', content, compact)
        self.assertEqual(published, verified)
        self.assertEqual(verified, rechecked)
        self.assertEqual({method for method, _ in self.requests}, {'GET'})
        self.assertEqual({address for _, address in self.requests}, set(self.objects))
        self.assertEqual({entry['name'] for entry in verified['evidence']['smoke']},
                         {'catalog', 'reference_coverage', 'source_discovery_daily'})


if __name__ == '__main__':
    unittest.main()
