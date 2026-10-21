"""Small actual registrations/snapshots and private deployment, never production discovery."""
from copy import deepcopy
from hashlib import sha256
import importlib.util
import json
import os
from pathlib import Path
import shutil
import sys
from unittest.mock import patch
import uuid
from publication_metadata import canonical_bytes
import reliability_execution_preflight as preflight
from artifact_import_fixtures import create as create_imports
import artifact_import_verifier as verifier

INSTALL = '10000000-0000-4000-8000-000000000001'
PROFILE = '10000000-0000-4000-8000-000000000002'
COMPLETED = '2026-10-03T00:00:00.000000Z'


def create(base, fmt='JSONL', multi=False, publication=False):
    base = Path(base).resolve()
    base.mkdir(exist_ok=True)
    inputs = base/'inputs'
    inputs.mkdir()
    with patch('reliability_format_metadata.now', return_value=COMPLETED), patch('service_reference._now', return_value=COMPLETED):
        fixture = create_imports(inputs)
    batches = [deepcopy(fixture['artifacts'][0 if fmt == 'JSONL' else 1])]
    if multi:
        import reliability_format_inputs as native
        original = batches[0]
        root = Path(fixture['roots'][original['storageBindingId']])
        manifest = json.loads((root/original['relativeAddress']/'manifest.json').read_bytes())
        if publication:
            from reliability_format_fixtures import g1
            incoming = root.parent/'incoming'/('events.'+fmt.lower())
            if fmt == 'JSONL':incoming.write_bytes(b''.join(canonical_bytes(r)+b'\n' for r in g1()[:5]))
            else:
                from reliability_benchmark_generator import write_parquet
                incoming.unlink()
                def transport():
                    for row in g1()[:5]:
                        row=dict(row);value=row.pop('event_time');row['event_time_epoch_us']=native.m.time_us(value);yield row
                write_parquet(incoming,transport())
        with patch('reliability_format_metadata.now', return_value=COMPLETED):
            b = native.register(root, input_root=root.parent/'incoming', source_id=original['identity']['sourceId'], batch_id='b01',
                                input_format=fmt, files=[dict(artifact_id='events', input_file='events.'+fmt.lower())],
                                received_at='2026-10-02T00:05:00.000000Z', profile=manifest['profile'])
        a = deepcopy(original)
        a['identity']['batchId'] = 'b01'
        a['relativeAddress'] = a['relativeAddress'].replace('batch_id=b00', 'batch_id=b01')
        a['fingerprint'] = b['batch_fingerprint']
        batches.append(a)
    reference = deepcopy(fixture['artifacts'][2])
    deployment = base/'deployment'
    deployment.mkdir()
    origin = Path(preflight.__file__).parent
    for name in preflight.MODULES:
        shutil.copyfile(origin/name, deployment/name)
    manifest = preflight.deployment_manifest('1'*40, deployment)
    raw = canonical_bytes(manifest)
    (deployment/'e5-reliability-deployment.json').write_bytes(raw)
    profile = dict(runProfileVersion=1, pipeline='RELIABILITY_V2', pipelineVersion=1,
                   sourceProfile='request-jsonl-v2' if fmt == 'JSONL' else 'request-parquet-v1',
                   runtime=dict(javaVersion='21', pythonVersion='3.12.15', pysparkVersion='4.0.2', deploymentSha256=sha256(raw).hexdigest()))
    bindings = dict(batch=batches[0]['storageBindingId'], reference=reference['storageBindingId'], output='reliability-output',
                    staging='reliability-staging', temp='reliability-temp', control='test-control')
    storage, resolved = [], {}
    for role, name in bindings.items():
        root = Path(fixture['roots'][name]) if role in ('batch', 'reference') else base/role
        root.mkdir(exist_ok=True)
        root.chmod(0o700)
        s = root.stat()
        config = dict(importStorageVersion=1, root=str(root), rootDevice=str(s.st_dev), rootInode=str(s.st_ino))
        resolved[role] = {k:config[k] for k in ('root', 'rootDevice', 'rootInode')}
        if role == 'control':
            (root/'host.guard').write_bytes(b'')
            (root/'installation.id').write_text(INSTALL+'\n')
            config = dict(bindingVersion=1, bindingId=name, installationId=INSTALL, **resolved[role])
            for file, prefix in [('host.guard','guard'), ('installation.id','installation')]:
                p = root/file
                p.chmod(0o600)
                st = p.stat()
                config[prefix+'Device'], config[prefix+'Inode'] = str(st.st_dev), str(st.st_ino)
        storage.append(dict(bindingId=name, productId='application-reliability', role=dict(batch='BATCH',reference='REFERENCE',output='RELIABILITY',staging='STAGING',temp='TEMP',control='CONTROL')[role],
                            configuration=config, configurationSha256=sha256(canonical_bytes(config)).hexdigest()))
    runtime_config = dict(executionRuntimeVersion=1, pipeline='RELIABILITY_V2', runProfileId=PROFILE,
                          runProfileSha256=sha256(canonical_bytes(profile)).hexdigest(), python=sys.executable,
                          javaHome=os.environ.get('JAVA_HOME',''), processingRoot=str(deployment), deploymentSha256=profile['runtime']['deploymentSha256'],
                          deploymentRevision='1'*40, bindings=bindings, verificationSeconds=30)
    runtime = dict(profileId='reliability-preflight-v1', profileSha256=sha256(canonical_bytes(runtime_config)).hexdigest(), configuration=runtime_config)
    selected = []
    for i, a in enumerate(batches+[reference]):
        binding = resolved['batch' if i < len(batches) else 'reference']
        _, e = verifier.execute(dict(protocolVersion=1, operationId=str(uuid.uuid4()), selectionSha256=sha256(canonical_bytes(a)).hexdigest(),
                                    mode='VERIFY', binding=binding, artifact=a, expectedCapture=None))
        q = dict(qualificationVersion=1, selection=a, verification=e)
        native_id = canonical_bytes(dict(sourceId=a['identity']['sourceId'],batchId=a['identity']['batchId'])).decode() if i < len(batches) else canonical_bytes(a['identity']).decode()
        selected.append(dict(artifactId=str(uuid.uuid4()), productId='application-reliability', kind=a['kind'], contractVersion=a['contractVersion'],
                             processorRevision=a['processorRevision'], storageBindingId=a['storageBindingId'], relativeAddress=a['relativeAddress'],
                             nativeIdentity=native_id, evidenceSha256=sha256(canonical_bytes(q)).hexdigest(), verifiedAt=COMPLETED,
                             qualification=q, role='RELIABILITY_BATCH' if i < len(batches) else 'SERVICE_REFERENCE', ordinal=i if i < len(batches) else 0))
    selected[:-1] = sorted(selected[:-1], key=lambda n:n['artifactId'])
    for i, a in enumerate(selected[:-1]):
        a['ordinal'] = i
    identity = batches[0]['identity']
    config = dict(configVersion=1,profile=profile['sourceProfile'],dataOrigin='synthetic',feedEndpointId=None,
                  registeredProfileSha256=identity['registeredProfileSha256'],inputStorageProfileId=bindings['batch'])
    sources = [dict(sourceId=identity['sourceId'],revision=1,capturedGateVersion='0',capturedEnabled=True,configuration=config,
                    configurationSha256=sha256(canonical_bytes(config)).hexdigest(),endpoint=None,endpointSha256=None)]
    frozen = dict(selectionVersion=1,pipeline='RELIABILITY_V2',pipelineVersion=1,productId='application-reliability',principalScope='local-operator',triggerType='OPERATOR',
                  executionProfile=dict(profileId=PROFILE,configurationSha256=runtime_config['runProfileSha256'],configuration=profile),
                  storageBindings=sorted(storage,key=lambda n:n['bindingId']),sources=sources,inputs=selected)
    bundle = dict(capturedSelection=frozen, bindings=resolved)
    command = dict(protocolVersion=1,runId=str(uuid.uuid4()),attemptId=str(uuid.uuid4()),ownerEpoch='1',stage='CHECK_INPUTS',itemOrdinal=0,
                   capturedConfigSha256=sha256(canonical_bytes(frozen)).hexdigest(),checkpointId=str(uuid.uuid4()),runtime=runtime,selection='selection.json',
                   bundleSha256=sha256(canonical_bytes(bundle)).hexdigest())
    return dict(command=command,bundle=bundle)


def load(runtime):
    path=Path(runtime['configuration']['processingRoot'])/'reliability_execution_preflight.py'
    spec=importlib.util.spec_from_file_location('private_preflight',path)
    module=importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


if __name__ == '__main__':
    sys.stdout.buffer.write(canonical_bytes(create(sys.argv[1],sys.argv[2] if len(sys.argv)>2 else 'JSONL',multi=len(sys.argv)>3)))
