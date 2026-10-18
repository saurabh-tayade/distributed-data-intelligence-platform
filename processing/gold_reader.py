"""Standalone metadata/inventory checks for one completed Gold v1 publication.

No Spark imports or Silver reads. The analytics opener also verifies Parquet
schemas and SQL invariants before returning a usable query session.
"""
from dataclasses import dataclass
from hashlib import sha256
import json
from pathlib import Path, PurePosixPath
import re

from publication_metadata import canonical_bytes

TABLES = ('gold_articles', 'gold_source_daily')
MAX_COUNT = 2**63 - 1
HEX = re.compile(r'[0-9a-f]{64}')
NAMESPACE = re.compile(r'v1/processor_revision=1/input_set=([0-9a-f]{64})')


class AnalyticsFailure(Exception):
    def __init__(self, code, path=''):
        self.code, self.path = code, str(path)
        super().__init__(json.dumps(dict(code=code, path=self.path), sort_keys=True))


def _need(condition, code='INVALID_METADATA'):
    if not condition:
        raise AnalyticsFailure(code)


def _keys(value, names):
    _need(type(value) is dict and set(value) == set(names))


def _count(value):
    _need(type(value) is int and 0 <= value <= MAX_COUNT)


def _version(value):
    _need(type(value) is int and value == 1, 'UNSUPPORTED_VERSION')


def _digest(value):
    _need(type(value) is str and HEX.fullmatch(value) is not None)


def _relative(value):
    _need(type(value) is str and bool(value) and '\\' not in value)
    path = PurePosixPath(value)
    _need(bool(path.parts) and not path.is_absolute() and '..' not in path.parts and path.as_posix() == value)
    return path


def _files(values, dataset=None):
    _need(type(values) is list and bool(values))
    paths = []
    for item in values:
        _keys(item, ('path', 'byte_length', 'sha256'))
        path = _relative(item['path'])
        _count(item['byte_length'])
        _digest(item['sha256'])
        if dataset:
            _need(len(path.parts) == 2 and path.parts[0] == dataset
                  and path.suffix == '.parquet' and not path.name.startswith(('.', '_'))
                  and not any(c in path.name for c in '*?[]'), 'INVALID_INVENTORY')
        paths.append(item['path'])
    _need(paths == sorted(set(paths), key=lambda s: s.encode('utf-8')), 'INVALID_INVENTORY')


def _json(raw):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError()
            result[key] = value
        return result
    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=unique)
        canonical_bytes(value)
        return value
    except (ValueError, UnicodeError, RecursionError):
        raise AnalyticsFailure('INVALID_METADATA') from None


def _validate_marker(marker, identity):
    _keys(marker, ('publication_version', 'gold_contract_versions', 'processor_revision',
                   'input_set_fingerprint', 'input_manifest_sha256', 'input_counts',
                   'duplicates', 'output_counts', 'files'))
    _version(marker['publication_version'])
    _version(marker['processor_revision'])
    _keys(marker['gold_contract_versions'], TABLES)
    for version in marker['gold_contract_versions'].values():
        _version(version)
    for name in ('input_set_fingerprint', 'input_manifest_sha256'):
        _digest(marker[name])
        _need(marker[name] == identity, 'IDENTITY_MISMATCH')
    _keys(marker['input_counts'], ('accepted', 'accepted_before_replay_collapse', 'rejected'))
    _keys(marker['duplicates'], ('duplicate_publications', 'duplicate_accepted_rows'))
    _keys(marker['output_counts'], TABLES)
    for group in ('input_counts', 'duplicates', 'output_counts'):
        for value in marker[group].values():
            _count(value)
    counts = marker['input_counts']
    _need(counts['accepted_before_replay_collapse'] - counts['accepted']
          == marker['duplicates']['duplicate_accepted_rows'])
    _keys(marker['files'], TABLES)
    for table in TABLES:
        _files(marker['files'][table], table)


def _validate_manifest(manifest, marker):
    _keys(manifest, ('manifest_version', 'gold_contract_versions', 'gold_processor_revision',
                     'silver_contract_version', 'silver_processor_revision', 'inputs'))
    for name in ('manifest_version', 'gold_processor_revision', 'silver_contract_version', 'silver_processor_revision'):
        _version(manifest[name])
    _keys(manifest['gold_contract_versions'], TABLES)
    for value in manifest['gold_contract_versions'].values():
        _version(value)
    _need(type(manifest['inputs']) is list and bool(manifest['inputs']))
    representatives, paths = {}, []
    accepted = rejected = duplicates = 0
    for item in manifest['inputs']:
        _keys(item, ('marker_path', 'marker_sha256', 'identity', 'counts', 'files', 'representative_path'))
        _relative(item['marker_path'])
        _relative(item['representative_path'])
        _digest(item['marker_sha256'])
        paths.append(item['marker_path'])
        identity = item['identity']
        _keys(identity, ('metadata_sha256', 'payload_sha256', 'contract_version', 'processor_revision', 'bucket', 'metadata_key'))
        for key in ('contract_version', 'processor_revision'):
            _version(identity[key])
        for key in ('metadata_sha256', 'payload_sha256'):
            _digest(identity[key])
        _need(type(identity['bucket']) is str and bool(identity['bucket'].strip()))
        _relative(identity['metadata_key'])
        _keys(item['counts'], ('accepted', 'rejected'))
        for value in item['counts'].values():
            _count(value)
        _keys(item['files'], ('accepted', 'rejected'))
        for files in item['files'].values():
            _files(files)
        key = (identity['bucket'], identity['metadata_key'])
        if key in representatives:
            previous = representatives[key]
            _need(item['representative_path'] == previous['marker_path']
                  and item['identity'] == previous['identity'] and item['counts'] == previous['counts'])
            duplicates += 1
        else:
            _need(item['representative_path'] == item['marker_path'])
            representatives[key] = item
            accepted += item['counts']['accepted']
            rejected += item['counts']['rejected']
    _need(paths == sorted(set(paths), key=lambda s: s.encode('utf-8')))
    _need(accepted == marker['input_counts']['accepted_before_replay_collapse']
          and rejected == marker['input_counts']['rejected']
          and duplicates == marker['duplicates']['duplicate_publications'], 'COUNT_MISMATCH')


def _regular(path):
    _need(not path.is_symlink() and path.is_file(), 'INVALID_LAYOUT')


def _hash(path):
    digest, length = sha256(), 0
    with path.open('rb') as stream:
        while block := stream.read(1024 * 1024):
            digest.update(block)
            length += len(block)
    return length, digest.hexdigest()


@dataclass(frozen=True)
class VerifiedMetadata:
    root: Path
    identity: str
    directory: Path
    marker: dict
    marker_bytes: bytes
    manifest_bytes: bytes

    def paths(self, table):
        return [str(self.directory / f['path']) for f in self.marker['files'][table]]

    def verify_unchanged(self):
        current = verify_metadata(self.root, self.identity)
        _need(current.marker_bytes == self.marker_bytes
              and current.manifest_bytes == self.manifest_bytes, 'PUBLICATION_CHANGED')


def verify_metadata(gold_root, publication):
    """Validate recorded metadata and every listed output byte, without Silver.

    Identity is a 64-character fingerprint or the exact root-relative v1 path.
    Schema/value checks belong to gold_analytics.open_publication().
    """
    try:
        text = str(publication)
        if HEX.fullmatch(text):
            relative, identity = f'v1/processor_revision=1/input_set={text}', text
        else:
            match = NAMESPACE.fullmatch(text)
            _need(match is not None, 'INVALID_SELECTION')
            relative, identity = text, match.group(1)
        root = Path(gold_root).resolve(strict=True)
        _need(root.is_dir(), 'INVALID_SELECTION')
        directory = root
        for part in PurePosixPath(relative).parts:
            directory /= part
            _need(not directory.is_symlink() and directory.is_dir(), 'INVALID_SELECTION')
        _need(not any(c in str(directory) for c in '*?[]'), 'INVALID_SELECTION')
        _need((directory / 'complete.json').exists(), 'INCOMPLETE_OUTPUT')
        _need({p.name for p in directory.iterdir()} == {'complete.json', 'inputs.json', *TABLES}, 'INVALID_LAYOUT')
        for name in ('complete.json', 'inputs.json'):
            _regular(directory / name)
        marker_bytes = (directory / 'complete.json').read_bytes()
        marker = _json(marker_bytes)
        _validate_marker(marker, identity)
        manifest_bytes = (directory / 'inputs.json').read_bytes()
        _need(sha256(manifest_bytes).hexdigest() == marker['input_manifest_sha256'], 'MANIFEST_MISMATCH')
        manifest = _json(manifest_bytes)
        _need(canonical_bytes(manifest) == manifest_bytes, 'MANIFEST_MISMATCH')
        _validate_manifest(manifest, marker)
        for table in TABLES:
            dataset = directory / table
            _need(not dataset.is_symlink() and dataset.is_dir(), 'INVALID_LAYOUT')
            expected = {PurePosixPath(f['path']).name for f in marker['files'][table]}
            housekeeping = {'_SUCCESS', '._SUCCESS.crc'} | {f'.{name}.crc' for name in expected}
            present = {p.name for p in dataset.iterdir()}
            _need(expected <= present <= expected | housekeeping, 'INVENTORY_MISMATCH')
            for path in dataset.iterdir():
                _regular(path)
            for item in marker['files'][table]:
                _need(_hash(directory / item['path']) == (item['byte_length'], item['sha256']), 'FILE_MISMATCH')
        return VerifiedMetadata(root, identity, directory, marker, marker_bytes, manifest_bytes)
    except AnalyticsFailure:
        raise
    except (OSError, ValueError, RuntimeError, TypeError):
        raise AnalyticsFailure('PUBLICATION_READ_FAILED') from None
