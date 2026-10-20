"""Strict, Spark-free Content v1 metadata and filesystem evidence (ADR 0007)."""
from dataclasses import dataclass
from datetime import datetime
from hashlib import sha256
import json
import os
from pathlib import Path, PurePosixPath
import re
import stat
from types import MappingProxyType

from publication_metadata import canonical_bytes

PRODUCT = 'content-intelligence'
CATALOG = 'content_catalog_v1'
DAILY = 'content_source_discovery_daily_v1'
TABLES = (CATALOG, DAILY)
VERSIONS = dict.fromkeys(TABLES, 1)
SEMANTICS = dict(discovery='first-observed-utc-v1', gold_preservation='all-fields-v1',
                 reference_application='snapshot-v1', reference_join='exact-source-left-v1')
MAX_COUNT = 2**63 - 1
MAX_ROWS = 10_000_000
MAX_FILES = 8192
MAX_DATASET_FILES = 4096
MAX_BYTES = 8 * 1024**3
MANIFEST_LIMIT = 16 * 1024
GOLD_MANIFEST_LIMIT = 16 * 1024**2
COMPLETION_LIMIT = 8 * 1024**2
RESPONSE_LIMIT = 1024**2
PAGE_BYTES = 128 * 1024
HEX = re.compile(r'[0-9a-f]{64}')
SOURCE = re.compile(r'[a-z0-9][a-z0-9_-]{0,127}')
IDENTIFIER = re.compile(r'[a-z0-9][a-z0-9_-]{0,63}')
REVISION = re.compile(r'[a-z0-9][a-z0-9_-]{0,31}')
GOLD_SCHEMA = (
    ('contract_version', 'INTEGER'),
    *((name, 'VARCHAR') for name in ('article_id', 'source_id', 'identifier_kind',
       'source_article_id', 'source_uri', 'feed_format', 'title', 'url', 'summary')),
    ('published_at', 'TIMESTAMP'), ('updated_at', 'TIMESTAMP'), ('quality_warnings', 'VARCHAR[]'),
    ('first_observed_at', 'TIMESTAMP'), ('last_observed_at', 'TIMESTAMP'),
    ('observation_count', 'BIGINT'), ('distinct_ingestion_count', 'BIGINT'),
    ('selected_silver_contract_version', 'INTEGER'), ('selected_ingestion_id', 'VARCHAR'),
    ('selected_ingested_at', 'TIMESTAMP'), ('selected_entry_index', 'INTEGER'),
    ('selected_input_uri', 'VARCHAR'), ('selected_payload_sha256', 'VARCHAR'),
    ('selected_payload_byte_length', 'BIGINT'), ('selected_bronze_bucket', 'VARCHAR'),
    ('selected_bronze_payload_key', 'VARCHAR'), ('selected_bronze_metadata_key', 'VARCHAR'),
)
ADDITIONS = (('content_contract_version', 'INTEGER'), ('discovered_at', 'TIMESTAMP'),
             ('discovery_date', 'DATE'), *((name, 'VARCHAR') for name in (
                 'publisher_name', 'publisher_category', 'declared_language', 'country_code',
                 'reference_status', 'reference_id', 'reference_revision', 'reference_fingerprint')))
SCHEMAS = {CATALOG: GOLD_SCHEMA + ADDITIONS, DAILY: (
    ('contract_version', 'INTEGER'), ('source_id', 'VARCHAR'), ('discovery_date', 'DATE'),
    ('discovered_article_count', 'BIGINT'), ('articles_missing_reference_count', 'BIGINT'))}
ATTRIBUTES = ('publisher_name', 'publisher_category', 'declared_language', 'country_code')
OPTIONAL = {'url', 'summary', 'published_at', 'updated_at', *ATTRIBUTES}
MESSAGES = {
    'ARGUMENT_INVALID': 'Arguments do not match the Content interface.',
    'PATH_UNSAFE': 'A selected path violates the local path policy.',
    'UNSUPPORTED_VERSION': 'A selected contract or processor version is unsupported.',
    'DEPENDENCY_UNAVAILABLE': 'A selected dependency is unavailable.',
    'DEPENDENCY_INVALID': 'Dependency evidence is incomplete or invalid.',
    'DEPENDENCY_CHANGED': 'A selected dependency changed during the operation.',
    'PUBLICATION_UNAVAILABLE': 'The selected Content publication is unavailable.',
    'PUBLICATION_INCOMPLETE': 'Content publication is incomplete; owner investigation is required.',
    'PUBLICATION_INVALID': 'Content publication evidence is invalid.',
    'PUBLICATION_CHANGED': 'Content publication evidence changed during the operation.',
    'PUBLICATION_FAILED': 'Content publication failed; investigate retained evidence.',
    'COUNT_OVERFLOW': 'A Content count exceeds its supported range.',
    'RESOURCE_LIMIT': 'A Content resource exceeds its configured limit.',
    'RESPONSE_LIMIT': 'The requested report exceeds its response limit.',
    'BUSY': 'The Content session already has an active request.',
    'QUERY_FAILED': 'The Content report could not be completed.',
}


class ContentFailure(Exception):
    def __init__(self, code):
        self.code = code
        self.diagnostic = dict(code=code, message=MESSAGES[code])
        super().__init__(MESSAGES[code])


def need(condition, code='PUBLICATION_INVALID'):
    if not condition:
        raise ContentFailure(code)


def freeze(value):
    if isinstance(value, dict):
        return MappingProxyType({k: freeze(v) for k, v in value.items()})
    if isinstance(value, list):
        return tuple(freeze(v) for v in value)
    return value


def keys(value, expected, code='PUBLICATION_INVALID'):
    need(type(value) is dict and set(value) == set(expected), code)


def count(value, *, positive=False, code='PUBLICATION_INVALID'):
    need(type(value) is int and int(positive) <= value <= MAX_COUNT, code)


def match(value, pattern=HEX, code='ARGUMENT_INVALID'):
    need(type(value) is str and pattern.fullmatch(value) is not None, code)


def parse(raw, *, code='PUBLICATION_INVALID', canonical=True):
    def unique(items):
        result = {}
        for k, v in items:
            need(k not in result, code)
            result[k] = v
        return result

    def integer(token):
        need(re.fullmatch(r'0|[1-9][0-9]{0,18}', token) is not None, code)
        value = int(token)
        count(value, code=code)
        return value

    def forbidden(_):
        raise ContentFailure(code)

    try:
        value = json.loads(raw.decode('utf-8'), object_pairs_hook=unique,
                           parse_int=integer, parse_float=forbidden, parse_constant=forbidden)
        encoded = canonical_bytes(value)
        need(not canonical or encoded == raw, code)
        return value
    except (ValueError, UnicodeError, RecursionError, TypeError):
        raise ContentFailure(code) from None


def validate_manifest(raw):
    m = parse(raw)
    keys(m, ('manifest_version', 'product_id', 'product_contract_version', 'processor_revision',
             'dataset_contract_versions', 'gold', 'reference', 'semantic_config'))
    for name in ('manifest_version', 'product_contract_version', 'processor_revision'):
        need(type(m[name]) is int and m[name] == 1, 'UNSUPPORTED_VERSION')
    need(m['product_id'] == PRODUCT)
    keys(m['dataset_contract_versions'], TABLES)
    for version in m['dataset_contract_versions'].values():
        need(type(version) is int and version == 1, 'UNSUPPORTED_VERSION')
    need(m['semantic_config'] == SEMANTICS)
    keys(m['gold'], ('publication_fingerprint', 'input_manifest', 'completion'))
    match(m['gold']['publication_fingerprint'], code='PUBLICATION_INVALID')
    for name, filename, limit in (('input_manifest', 'gold-inputs.json', GOLD_MANIFEST_LIMIT),
                                  ('completion', 'gold-complete.json', COMPLETION_LIMIT)):
        item = m['gold'][name]
        keys(item, ('file', 'byte_length', 'sha256'))
        need(item['file'] == filename)
        count(item['byte_length'], positive=True)
        need(item['byte_length'] <= limit, 'RESOURCE_LIMIT')
        match(item['sha256'], code='PUBLICATION_INVALID')
    need(m['gold']['input_manifest']['sha256'] == m['gold']['publication_fingerprint'])
    keys(m['reference'], ('reference_id', 'revision', 'snapshot_fingerprint'))
    for name, pattern in (('reference_id', IDENTIFIER), ('revision', REVISION),
                          ('snapshot_fingerprint', HEX)):
        match(m['reference'][name], pattern, 'PUBLICATION_INVALID')
    return m


def validate_completion(raw, identity, manifest_length):
    m = parse(raw)
    keys(m, ('completion_version', 'product_id', 'product_fingerprint', 'manifest_byte_length',
             'output_counts', 'files', 'completed_at'))
    need(type(m['completion_version']) is int and m['completion_version'] == 1,
         'UNSUPPORTED_VERSION')
    need(m['product_id'] == PRODUCT and m['product_fingerprint'] == identity)
    count(m['manifest_byte_length'], positive=True)
    need(m['manifest_byte_length'] == manifest_length)
    keys(m['output_counts'], TABLES)
    for value in m['output_counts'].values():
        count(value)
        need(value <= MAX_ROWS, 'RESOURCE_LIMIT')
    validate_inventory(m['files'])
    timestamp = m['completed_at']
    need(type(timestamp) is str and re.fullmatch(
        r'[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{6}Z', timestamp) is not None)
    try:
        datetime.strptime(timestamp, '%Y-%m-%dT%H:%M:%S.%fZ')
    except ValueError:
        raise ContentFailure('PUBLICATION_INVALID') from None
    return m


def validate_inventory(files, tables=TABLES, code='PUBLICATION_INVALID'):
    keys(files, tables, code)
    for table in tables:
        items = files[table]
        need(type(items) is list and bool(items), code)
        need(len(items) <= MAX_DATASET_FILES, 'RESOURCE_LIMIT')
        paths = []
        for item in items:
            keys(item, ('path', 'byte_length', 'sha256'), code)
            value = item['path']
            need(type(value) is str, code)
            p = PurePosixPath(value)
            need(len(value.encode('utf-8')) <= 512, 'RESOURCE_LIMIT')
            need(len(p.parts) == 2 and p.parts[0] == table and p.as_posix() == value and
                 p.suffix == '.parquet' and not p.name.startswith(('.', '_')) and
                 not any(c in value for c in '\\*?[]'), code)
            count(item['byte_length'], positive=True, code=code)
            match(item['sha256'], code=code)
            paths.append(value)
        need(paths == sorted(set(paths), key=lambda x: x.encode('utf-8')), code)
    admit(files)


def admit(*inventories):
    items = [f for inventory in inventories for values in inventory.values() for f in values]
    need(len(items) <= MAX_FILES and sum(f['byte_length'] for f in items) <= MAX_BYTES,
         'RESOURCE_LIMIT')


def root_path(value, unavailable='DEPENDENCY_UNAVAILABLE'):
    try:
        p = Path(value).absolute()
        need('..' not in p.parts and not any(c in str(p) for c in '\0*?[]'), 'PATH_UNSAFE')
        info = p.lstat()
        need(stat.S_ISDIR(info.st_mode), 'PATH_UNSAFE')
        return p.resolve(strict=True)
    except (OSError, ValueError, TypeError):
        raise ContentFailure(unavailable) from None


def separate(*roots):
    for i, a in enumerate(roots):
        for b in roots[i + 1:]:
            need(not a.is_relative_to(b) and not b.is_relative_to(a), 'PATH_UNSAFE')


def stamp(info):
    return info.st_dev, info.st_ino, info.st_size, info.st_mtime_ns, info.st_mode, info.st_nlink


def _info(path, directory, unavailable, changed, previous=None):
    try:
        info = path.lstat()
    except FileNotFoundError:
        raise ContentFailure(changed if previous is not None else unavailable) from None
    except OSError:
        raise ContentFailure(unavailable) from None
    if previous is not None:
        observed = (info.st_dev, info.st_ino, info.st_mode) if directory else stamp(info)
        need(observed == previous, changed)
    need(stat.S_ISDIR(info.st_mode) if directory else
         stat.S_ISREG(info.st_mode) and info.st_nlink == 1, 'PATH_UNSAFE')
    return info


@dataclass(frozen=True)
class FileEvidence:
    path: Path
    stamp: tuple
    length: int
    digest: str
    data: bytes | None


def observe_file(path, limit, unavailable, changed, previous=None, *, retain=False):
    before = _info(path, False, unavailable, changed, previous.stamp if previous else None)
    need(before.st_size <= limit, 'RESOURCE_LIMIT')
    digest, size, data = sha256(), 0, bytearray() if retain else None
    complete = False
    try:
        fd = os.open(path, os.O_RDONLY | os.O_NONBLOCK | os.O_NOFOLLOW)
        try:
            handle = os.fdopen(fd, 'rb')
        except Exception:
            os.close(fd)
            raise
        with handle as stream:
            need(stamp(os.fstat(stream.fileno())) == stamp(before), changed)
            while chunk := stream.read(min(1024 * 1024, limit + 1 - size)):
                size += len(chunk)
                need(size <= limit, 'RESOURCE_LIMIT')
                digest.update(chunk)
                if retain:
                    data.extend(chunk)
            complete = True
            if previous:
                need((size, digest.hexdigest()) == (previous.length, previous.digest), changed)
            need(stamp(os.fstat(stream.fileno())) == stamp(before), changed)
        _info(path, False, unavailable, changed, stamp(before))
    except OSError:
        need(size <= limit, 'RESOURCE_LIMIT')
        if previous and retain and previous.data is not None:
            need(previous.data[:size] == data, changed)
        if previous and complete:
            need((size, digest.hexdigest()) == (previous.length, previous.digest), changed)
        _info(path, False, unavailable, changed, stamp(before))
        raise ContentFailure(unavailable) from None
    return FileEvidence(path, stamp(before), size, digest.hexdigest(), bytes(data) if retain else None)


@dataclass(frozen=True)
class Evidence:
    directory: Path
    tables: tuple
    top: frozenset
    files: tuple[FileEvidence, ...]
    directories: tuple
    guards: tuple
    unavailable: str
    changed: str

    def check(self):
        return capture(self.directory, self.tables, self.top, self.unavailable, self.changed,
                       guards=tuple(path for path, _ in self.guards), previous=self)

    def raw(self, name):
        return next(item.data for item in self.files if item.path == self.directory / name)

    def inventory(self):
        return {table: [dict(path=item.path.relative_to(self.directory).as_posix(),
                            byte_length=item.length, sha256=item.digest)
                        for item in self.files if item.path.parent == self.directory / table
                        and item.path.suffix == '.parquet' and not item.path.name.startswith(('.', '_'))]
                for table in self.tables}


def _names(path, unavailable, changed, old=None):
    names, complete = [], False
    try:
        with os.scandir(path) as entries:
            for entry in entries:
                need(len(names) < MAX_FILES * 2 + 16, 'RESOURCE_LIMIT')
                names.append(entry.name)
            complete = True
        result = tuple(sorted(names, key=lambda s: s.encode('utf-8')))
        if old is not None:
            need(result == old, changed)
        return result
    except FileNotFoundError:
        raise ContentFailure(changed if old is not None else unavailable) from None
    except OSError:
        need(len(names) < MAX_FILES * 2 + 16, 'RESOURCE_LIMIT')
        if old is not None:
            need(set(names) <= set(old), changed)
        if complete and old is not None:
            need(tuple(sorted(names, key=lambda s: s.encode('utf-8'))) == old, changed)
        raise ContentFailure(unavailable) from None


def capture(directory, tables, top, unavailable, changed, *, guards=(), previous=None):
    """Capture only a product/dependency's fixed flat layout; never a directory glob."""
    old_files = {f.path: f for f in previous.files} if previous else {}
    old_dirs = dict(previous.directories) if previous else {}
    old_guards = dict(previous.guards) if previous else {}
    guard_values = []
    for path in guards:
        info = _info(path, True, unavailable, changed, old_guards.get(path))
        guard_values.append((path, (info.st_dev, info.st_ino, info.st_mode)))
    directories, files = [], []
    analytical_count = analytical_bytes = 0
    for path in (directory, *(directory / t for t in tables)):
        old = old_dirs.get(path)
        info = _info(path, True, unavailable, changed, old[0] if old else None)
        identity = info.st_dev, info.st_ino, info.st_mode
        names = _names(path, unavailable, changed, old[1] if old else None)
        if path == directory:
            need(set(names) == set(top), unavailable)
        else:
            parquet = {name for name in names if name.endswith('.parquet') and not name.startswith(('.', '_'))}
            need(bool(parquet), unavailable)
            need(len(parquet) <= MAX_DATASET_FILES, 'RESOURCE_LIMIT')
            allowed = parquet | {'_SUCCESS', '._SUCCESS.crc'} | {f'.{n}.crc' for n in parquet}
            need(set(names) <= allowed, unavailable)
        directories.append((path, (identity, names)))
        for name in names:
            if path == directory and name in tables:
                continue
            need(not any(c in name for c in '\\*?[]'), 'PATH_UNSAFE')
            need(len((path / name).relative_to(directory).as_posix().encode()) <= 512, 'RESOURCE_LIMIT')
            if path != directory and name.endswith('.parquet') and not name.startswith(('.', '_')):
                analytical_count += 1
                analytical_bytes += _info(path / name, False, unavailable, changed).st_size
                need(analytical_count <= MAX_FILES and analytical_bytes <= MAX_BYTES, 'RESOURCE_LIMIT')
            limit = MAX_BYTES
            retain = path == directory
            if retain:
                limit = {'inputs.json': MANIFEST_LIMIT if tables == TABLES else GOLD_MANIFEST_LIMIT,
                         'gold-inputs.json': GOLD_MANIFEST_LIMIT,
                         'gold-complete.json': COMPLETION_LIMIT,
                         'manifest.json': 16384, 'vocabulary.json': 32768,
                         'reference.csv': 10_485_760}.get(name, COMPLETION_LIMIT)
            files.append(observe_file(path / name, limit, unavailable, changed,
                                      old_files.get(path / name), retain=retain))
        _info(path, True, unavailable, changed, identity)
        _names(path, unavailable, changed, names)
    result = Evidence(directory, tuple(tables), frozenset(top), tuple(files), tuple(directories),
                      tuple(guard_values), unavailable, changed)
    admit(result.inventory())
    return result


def publication_directory(root, fingerprint):
    return root / 'v1' / 'processor_revision=1' / f'publication={fingerprint}'


def namespace_guards(root, directory):
    return (root, directory.parent.parent, directory.parent)


def descriptor(filename, raw):
    return dict(file=filename, byte_length=len(raw), sha256=sha256(raw).hexdigest())


def arguments(argv, required, optional=()):
    if argv == ['--help']:
        return None
    need(len(argv) % 2 == 0 and len(argv) <= 40, 'ARGUMENT_INVALID')
    result = {}
    for name,value in zip(argv[::2], argv[1::2]):
        need(name.startswith('--') and name[2:] in set(required) | set(optional) and
             name[2:] not in result and not value.startswith('--'), 'ARGUMENT_INVALID')
        result[name[2:]] = value
    need(set(required) <= result.keys(), 'ARGUMENT_INVALID')
    return {k.replace('-', '_'): v for k,v in result.items()}
