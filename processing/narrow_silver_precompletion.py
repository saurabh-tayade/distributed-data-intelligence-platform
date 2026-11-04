"""Opt-in, managed-only Silver precompletion for ddip-content-narrow-v1.

This versioned publisher owns orchestration; all transformation, provenance and
semantic qualification helpers remain native and unchanged. Prospective metadata
is only a conservative size calculation, never verification or admission evidence.
No dispatcher, runtime manifest, retry, repair or cleanup is enabled here.
"""
import base64
from copy import deepcopy
from dataclasses import dataclass
from functools import wraps
from hashlib import sha256
import json
import os
from pathlib import Path
import stat

import artifact_import_verifier as p
import narrow_content_profile as n
from publication_metadata import canonical_bytes, fingerprint
import silver_import_qualification as q

PROFILE = n.PROFILE
# The existing stamp grammar permits a minus sign PLUS twenty digits.
FUTURE_STAMP = '-' + '9' * 20


def _fixed(function):
    """Public failures are bounded classifications, never source/JVM/I/O text."""
    @wraps(function)
    def checked(*args, **kwargs):
        try:
            return function(*args, **kwargs)
        except (p.Failure, n.ProfileError):
            raise
        except OSError:
            raise p.Failure('EVIDENCE_UNAVAILABLE') from None
        except ImportError:
            raise p.Failure('RUNTIME_INCOMPATIBLE') from None
        except Exception as error:
            code = getattr(error, 'code', None)
            if code in q.CODES | p.CODES:
                raise p.Failure(code) from None
            if str(error) in {'INCOMPLETE_OUTPUT', 'OUTPUT_CONFLICT', 'OUTPUT_COUNT_MISMATCH',
                    'OUTPUT_PROVENANCE_MISMATCH', 'PAYLOAD_LENGTH_MISMATCH', 'PAYLOAD_SHA256_MISMATCH',
                    'INVALID_METADATA', 'OBJECT_READ_FAILED', 'OBJECT_TOO_LARGE', 'UNSUPPORTED_ROOT',
                    'DTD_FORBIDDEN', 'UNSUPPORTED_ENCODING', 'MALFORMED_XML'}:
                raise p.Failure(str(error)) from None
            raise p.Failure('SILVER_INVALID') from None
    return checked


@dataclass(frozen=True)
class ProspectiveBounds:
    """Byte counts only, explicitly not a qualification or verification result."""
    completion: int
    identity: int
    capture: int
    selection: int
    verification: int
    qualification: int


@dataclass(frozen=True)
class CompletionGuard:
    """Actual unfinished snapshot and planned bytes, not publication authority."""
    snapshot_bytes: bytes
    marker_bytes: bytes
    bounds: ProspectiveBounds


@_fixed
def marker_bytes_v1(marker):
    """Exact bronze_to_silver.process_one json.dump(sort_keys=True) encoding."""
    raw = json.dumps(marker, sort_keys=True).encode('utf-8')
    n._need(0 < len(raw) <= n.COMPLETION_LIMIT)
    return raw


def _root(artifact, binding):
    q.selection(artifact)
    p.fields(binding, ('root', 'rootDevice', 'rootInode'))
    root = Path(binding['root'])
    p.need(root.is_absolute() and root == root.resolve(strict=True), 'PATH_UNSAFE')
    value = p.info(root, True)
    p.need([str(value.st_dev), str(value.st_ino)] ==
           [binding['rootDevice'], binding['rootInode']], 'EVIDENCE_CHANGED')
    namespace = root / Path(artifact['relativeAddress']).parent
    _ancestors(root, namespace, missing=True)
    return root, namespace


def _ancestors(root, namespace, *, missing=False):
    result, current = [], root
    for part in ('', *namespace.relative_to(root).parts):
        if part:
            current /= part
        try:
            value = current.lstat()
        except FileNotFoundError:
            if missing:
                break
            raise
        p.need(stat.S_ISDIR(value.st_mode), 'PATH_UNSAFE')
        result.append(dict(path=current.relative_to(root).as_posix(),
                           identity=[str(value.st_dev), str(value.st_ino), str(value.st_mode)]))
    return result


def _capture_unfinished(artifact, binding, *, temporary=None):
    """Read actual unfinished files without exposing a native completion marker.

    Mirrors the native capture namespace policy with tighter count bounds. A
    complete.tmp is allowed only during this publisher's own final rechecks, with
    its exact bytes checked separately. I/O errors are never absence or retry.
    """
    root, namespace = _root(artifact, binding)
    guards, current = [], root
    for part in ('', *namespace.relative_to(root).parts):
        if part:
            current /= part
        value = p.info(current, True)
        guards.append(dict(path=current.relative_to(root).as_posix(),
                           identity=[str(value.st_dev), str(value.st_ino), str(value.st_mode)]))
    files, directories = [], []
    analytical = dict(accepted=0, rejected=0)
    total, temp_record = 0, None

    def visit(directory, dataset=None, depth=0, bookkeeping=False):
        nonlocal total, temp_record
        n._need(depth <= 6 and len(directories) < 4)
        relative = directory.relative_to(namespace).as_posix()
        before = p.info(directory, True)
        with os.scandir(directory) as stream:
            names = []
            for entry in stream:
                # A fresh publication has at most 23 files + 3 child directories.
                n._need(len(names) < 27)
                names.append(entry.name)
        names.sort(key=lambda name: name.encode('utf-8'))
        if directory == namespace:
            p.need('complete.json' not in names, 'EVIDENCE_CHANGED')
            p.need(('complete.tmp' in names) == (temporary is not None), 'EVIDENCE_CHANGED')
        directories.append(dict(path=relative, stamp=p.stamp(before),
                                names=[name for name in names if not
                                       (directory == namespace and name == 'complete.tmp')]))
        for name in names:
            path = directory / name
            rel = path.relative_to(namespace).as_posix()
            is_temp = directory == namespace and name == 'complete.tmp'
            hidden = name.startswith(('.', '_'))
            if directory == namespace and name in analytical:
                visit(path, name, depth + 1)
                continue
            if not hidden and not bookkeeping and not is_temp:
                p.need(relative in analytical and name.endswith('.parquet'), 'SILVER_INVALID')
            value = path.lstat()
            if stat.S_ISDIR(value.st_mode) and (hidden or bookkeeping):
                visit(path, None, depth + 1, True)
                continue
            value = p.info(path, False)
            association = dataset if not hidden and not bookkeeping and relative in analytical else 'bookkeeping'
            if association in analytical:
                p.need(not any(c in name for c in '*?[]{}\\'), 'PATH_UNSAFE')
                analytical[association] += 1
                n._need(analytical[association] <= 4)
            if not is_temp:
                n._need(len(files) < 23)
            limit = n.COMPLETION_LIMIT if is_temp else 16 * 1024**2 if association == 'bookkeeping' else q.MAX_RAW
            p.need(value.st_size <= limit and total + value.st_size <= q.MAX_RAW, 'RESOURCE_LIMIT')
            digest, length, raw = sha256(), 0, bytearray()
            fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
            with os.fdopen(fd, 'rb') as stream:
                p.need(p.stamp(os.fstat(stream.fileno())) == p.stamp(value), 'EVIDENCE_CHANGED')
                while block := stream.read(min(1024**2, limit + 1 - length)):
                    length += len(block)
                    p.need(length <= limit, 'RESOURCE_LIMIT')
                    digest.update(block)
                    if is_temp:
                        raw.extend(block)
                p.need(p.stamp(os.fstat(stream.fileno())) == p.stamp(value), 'EVIDENCE_CHANGED')
            p.need(p.stamp(p.info(path, False, True)) == p.stamp(value) and length == value.st_size,
                   'EVIDENCE_CHANGED')
            total += length
            record = dict(path=rel, dataset=association, byteLength=length,
                          sha256=digest.hexdigest(), stamp=p.stamp(value))
            if is_temp:
                p.need(bytes(raw) == temporary, 'EVIDENCE_CHANGED')
                temp_record = record
            else:
                files.append(record)
        p.need(p.stamp(p.info(directory, True, True)) == p.stamp(before), 'EVIDENCE_CHANGED')

    visit(namespace)
    n._need(all(analytical.values()))
    for guard in guards:
        value = p.info(root / guard['path'], True, True)
        p.need([str(value.st_dev), str(value.st_ino), str(value.st_mode)] == guard['identity'],
               'EVIDENCE_CHANGED')
    return dict(files=sorted(files, key=lambda item: item['path'].encode('utf-8')),
                directories=directories, guards=guards,
                analyticalFileCounts=analytical, temporary=temp_record)


def _native_selection(artifact, marker, local):
    """Exact single-publication inventory wrapper; actual bytes are checked later."""
    address = artifact['relativeAddress']
    prefix = str(Path(address).parent) + '/'
    completion = next(item for item in local['files'] if item['path'] == 'complete.json')
    item = dict(marker_path=address, marker_sha256=completion['sha256'],
                identity=marker['identity'], counts=marker['counts'],
                files={name: [dict(path=prefix + entry['path'], byte_length=entry['byteLength'],
                                  sha256=entry['sha256']) for entry in local['files']
                              if entry['dataset'] == name] for name in ('accepted', 'rejected')},
                representative_path=address)
    manifest = dict(manifest_version=1, gold_contract_versions=dict(gold_articles=1, gold_source_daily=1),
                    gold_processor_revision=1, silver_contract_version=1, silver_processor_revision=1,
                    inputs=[item])
    return dict(manifest=manifest, inputSetFingerprint=fingerprint(manifest))


def _measure(artifact, marker, bronze, local):
    """Discard all prospective envelopes; only their exact byte counts escape."""
    selected = _native_selection(artifact, marker, local)
    value, digest = q.identity(artifact, local, bronze)
    # Fixed native success fields are a budget template, NOT observations. In
    # particular this does not assert Spark has run or shutdown has occurred.
    template = dict(evidenceVersion=2, native=dict(verifier='silver_inputs.load_inputs',
        counts=marker['counts'], schemaPolicy='native-nullable-schema-and-required-row-values-v1',
        bronzeProvenance='all-rows-exact-v1', semanticReconstruction=False, sparkShutdown='CONFIRMED'),
        capture=dict(local=local, bronze=bronze),
        inventoryIdentity=dict(kind='E5_SILVER_INVENTORY_V1', sha256=digest, value=value))
    qualification = dict(qualificationVersion=1, selection=artifact, verification=template)
    completion = base64.b64decode(local['completionBase64'], validate=True)
    return ProspectiveBounds(len(completion), len(n.canonical_envelope(value, n.IDENTITY_LIMIT)),
        len(n.validate_silver_capture(local)), len(n.validate_native_selection(selected)),
        len(n.validate_silver_verification(template, artifact)),
        len(n.validate_silver_qualification(qualification)))


@_fixed
def precompletion_guard_v1(artifact, binding, marker, bronze):
    """Reject oversized projected metadata before complete.tmp or complete.json.

    Stamps reserve the entire signed grammar width, including all directory and
    guard identities. File size stamps stay equal to their actual byte lengths.
    No fake marker, verification, selection or projection is written to disk.
    """
    raw = marker_bytes_v1(marker)
    snapshot = _capture_unfinished(artifact, binding)
    p.need(marker['identity']['bucket'] == artifact['identity']['bronzeBucket'] and
           marker['identity']['metadata_key'] == artifact['identity']['bronzeMetadataKey'] and
           marker['identity']['metadata_sha256'] == bronze['metadataSha256'] and
           marker['identity']['payload_sha256'] == bronze['payloadSha256'], 'IDENTITY_MISMATCH')
    p.need(sum(item['byteLength'] for item in snapshot['files']) + len(raw) <= q.MAX_RAW, 'RESOURCE_LIMIT')
    local = deepcopy(snapshot)
    del local['temporary']
    local['completionBase64'] = base64.b64encode(raw).decode('ascii')
    local['files'].append(dict(path='complete.json', dataset='bookkeeping', byteLength=len(raw),
                              sha256=sha256(raw).hexdigest(), stamp=[]))
    local['files'].sort(key=lambda item: item['path'].encode('utf-8'))
    for entry in local['files']:
        entry['stamp'] = [FUTURE_STAMP] * 7
        entry['stamp'][2] = str(entry['byteLength'])
    for directory in local['directories']:
        directory['stamp'] = [FUTURE_STAMP] * 7
        if directory['path'] == '.':
            directory['names'] = sorted([*directory['names'], 'complete.json'], key=lambda name: name.encode('utf-8'))
    for guard in local['guards']:
        guard['identity'] = [FUTURE_STAMP] * 3
    bounds = _measure(artifact, marker, bronze, local)
    return CompletionGuard(canonical_bytes(snapshot), raw, bounds)


def _recheck(artifact, binding, guard, *, temporary=False):
    baseline = json.loads(guard.snapshot_bytes)
    current = _capture_unfinished(artifact, binding, temporary=guard.marker_bytes if temporary else None)
    compared = deepcopy(current)
    if temporary:
        # Creating our temporary changes only root size/times. Keep its inode,
        # device, mode, link count, exact listing and all other stamps bound.
        compared['temporary'] = None
        for snapshot in (baseline, compared):
            root = next(item for item in snapshot['directories'] if item['path'] == '.')
            for index in (2, 3, 4):
                root['stamp'][index] = '0'
    p.need(compared == baseline, 'EVIDENCE_CHANGED')
    return current


def _publish_marker(artifact, binding, marker, bronze, client, guard):
    """Retain every incomplete prefix on faults; never retry or remove files."""
    _recheck(artifact, binding, guard)
    q.bronze_read(artifact, None, marker, client, baseline=bronze)
    _recheck(artifact, binding, guard)
    namespace = Path(binding['root']) / Path(artifact['relativeAddress']).parent
    with (namespace / 'complete.tmp').open('xb') as stream:
        stream.write(guard.marker_bytes)
    after_write = _recheck(artifact, binding, guard, temporary=True)
    q.bronze_read(artifact, None, marker, client, baseline=bronze)
    # Recheck output, temporary and the entire ancestor chain after the last
    # Bronze read and immediately before native rename. This is the existing
    # cooperative single-writer lifecycle, not protection from a hostile writer.
    p.need(_capture_unfinished(artifact, binding, temporary=guard.marker_bytes) == after_write,
           'EVIDENCE_CHANGED')
    (namespace / 'complete.tmp').rename(namespace / 'complete.json')


def _qualify_in_session(spark, client, artifact, binding, marker):
    """Actual native semantics; the caller still owns Spark shutdown."""
    import silver_inputs as native
    local = q.capture(artifact, binding)
    n.validate_silver_capture(local)
    parsed, _, _ = native.parse_marker(binding['root'], artifact['relativeAddress'])
    p.need(parsed == marker, 'EVIDENCE_CHANGED')
    bronze, expected = q.bronze_read(artifact, None, marker, client)
    _measure(artifact, marker, bronze, local)
    q.qualify(spark, artifact, binding, local, marker, expected)
    selection = native.discover(binding['root'], markers=[artifact['relativeAddress']])
    manifest = native.inventory(selection)
    selected = dict(manifest=manifest, inputSetFingerprint=fingerprint(manifest))
    n.validate_native_selection(selected)
    p.need(selected == _native_selection(artifact, marker, local), 'EVIDENCE_CHANGED')
    q.capture(artifact, binding, local)
    q.bronze_read(artifact, None, marker, client, baseline=bronze)
    q.capture(artifact, binding, local)


@_fixed
def process_one_v1(spark, client, bucket, key, output, *, artifact, binding):
    """Explicit managed entry point; never call the standalone native publisher.

    Success retains the native PROCESSED/SKIPPED counts contract. It is not a
    shutdown-confirmed verification receipt. Use reconcile_v1 for actual evidence.
    """
    import bronze_to_silver as native
    root, directory = _root(artifact, binding)
    original_ancestors = _ancestors(root, directory, missing=True)
    p.need(Path(output).is_absolute() and Path(output) == root and
           bucket == artifact['identity']['bronzeBucket'] and
           key == artifact['identity']['bronzeMetadataKey'], 'IDENTITY_MISMATCH')
    raw = native.read_object(client, bucket, key, native.METADATA_LIMIT)
    metadata, instant = native.metadata(raw, key)
    p.need(metadata['sourceId'] == artifact['identity']['sourceId'] and
           metadata['ingestionId'] == artifact['identity']['ingestionId'] and
           metadata['payloadKey'] == artifact['identity']['bronzePayloadKey'], 'IDENTITY_MISMATCH')
    payload = native.read_object(client, bucket, metadata['payloadKey'], native.MAX_BYTES)
    if len(payload) != metadata['payloadByteLength']:
        raise native.Failure('PAYLOAD_LENGTH_MISMATCH')
    if sha256(payload).hexdigest() != metadata['payloadSha256']:
        raise native.Failure('PAYLOAD_SHA256_MISMATCH')
    feed_format = None
    for candidate in ('rss', 'atom'):
        error = native.preflight(payload, candidate)
        if error is None:
            feed_format = candidate
            break
        if error != 'UNSUPPORTED_ROOT':
            raise native.Failure(error)
    if feed_format is None:
        raise native.Failure('UNSUPPORTED_ROOT')
    provenance = dict(bronze_bucket=bucket, bronze_payload_key=metadata['payloadKey'], bronze_metadata_key=key)
    expected = dict(provenance, source_id=metadata['sourceId'], ingestion_id=metadata['ingestionId'],
        source_uri=metadata['sourceUri'], ingested_at=instant, payload_sha256=metadata['payloadSha256'],
        payload_byte_length=len(payload), feed_format=feed_format, contract_version=1,
        input_uri=f"s3://{bucket}/{metadata['payloadKey']}")
    identity = dict(metadata_sha256=sha256(raw).hexdigest(), payload_sha256=metadata['payloadSha256'],
                    contract_version=1, processor_revision=1, bucket=bucket, metadata_key=key)
    try:
        directory.lstat()
    except FileNotFoundError:
        existing = False
    else:
        existing = True
    if existing:
        p.info(directory, True)
        try:
            (directory / 'complete.json').lstat()
        except FileNotFoundError:
            raise native.Failure('INCOMPLETE_OUTPUT') from None
        local = q.capture(artifact, binding)
        n.validate_silver_capture(local)
        completed = json.loads(base64.b64decode(local['completionBase64'], validate=True))
        if completed.get('identity') != identity:
            raise native.Failure('OUTPUT_CONFLICT')
        _qualify_in_session(spark, client, artifact, binding, completed)
        return dict(status='SKIPPED', **completed['counts'])
    observation = native.Observation(metadata['sourceId'], metadata['sourceUri'], metadata['ingestionId'], instant, feed_format)
    accepted, rejected = native.normalize_bytes(spark, payload, observation, expected['input_uri'], provenance)
    p.need(_ancestors(root, directory, missing=True) == original_ancestors, 'EVIDENCE_CHANGED')
    directory.mkdir(parents=True, exist_ok=False)
    created_ancestors = _ancestors(root, directory)
    p.need(created_ancestors[:len(original_ancestors)] == original_ancestors, 'EVIDENCE_CHANGED')
    p.need(_ancestors(root, directory) == created_ancestors, 'EVIDENCE_CHANGED')
    accepted.write.mode('errorifexists').parquet(str(directory / 'accepted'))
    p.need(_ancestors(root, directory) == created_ancestors, 'EVIDENCE_CHANGED')
    rejected.write.mode('errorifexists').parquet(str(directory / 'rejected'))
    p.need(_ancestors(root, directory) == created_ancestors, 'EVIDENCE_CHANGED')
    counts = native.verify_dataset(spark, directory, expected)
    p.need(_ancestors(root, directory) == created_ancestors, 'EVIDENCE_CHANGED')
    if counts != dict(accepted=accepted.count(), rejected=rejected.count()):
        raise native.Failure('OUTPUT_COUNT_MISMATCH')
    marker = dict(identity=identity, counts=counts)
    bronze, _ = q.bronze_read(artifact, None, marker, client)
    guard = precompletion_guard_v1(artifact, binding, marker, bronze)
    p.need(json.loads(guard.snapshot_bytes)['guards'] == created_ancestors, 'EVIDENCE_CHANGED')
    _publish_marker(artifact, binding, marker, bronze, client, guard)
    _qualify_in_session(spark, client, artifact, binding, marker)
    return dict(status='PROCESSED', **counts)


@_fixed
def reconcile_v1(request):
    """Read-only actual native qualification/recheck; never invokes a publisher.

    Incomplete, unavailable and too-wide publications remain untouched. Returned
    verification is exclusively q.execute's real shutdown-confirmed evidence.
    """
    p.validate(request)
    p.need(request['protocolVersion'] == 2, 'ADAPTER_INVALID')
    artifact, binding = request['artifact'], request['binding']
    before = q.capture(artifact, binding)
    n.validate_silver_capture(before)
    status, verification = q.execute(request)
    after = q.capture(artifact, binding, before)
    import silver_inputs as native
    selection = native.discover(binding['root'], markers=[artifact['relativeAddress']])
    manifest = native.inventory(selection)
    n.validate_native_selection(dict(manifest=manifest, inputSetFingerprint=fingerprint(manifest)))
    q.capture(artifact, binding, after)
    if status == 'VERIFIED':
        n.validate_silver_qualification(dict(qualificationVersion=1, selection=artifact, verification=verification))
        p.need(verification['capture']['local'] == after, 'EVIDENCE_CHANGED')
    else:
        p.need(status == 'UNCHANGED' and verification is None, 'ADAPTER_INVALID')
        marker, _, _ = native.parse_marker(binding['root'], artifact['relativeAddress'])
        _measure(artifact, marker, request['expectedCapture']['bronze'], after)
        q.capture(artifact, binding, after)
    return status, verification
