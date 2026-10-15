"""Read-only completed M1 Silver boundary. No Bronze client or Gold writer."""
import base64
from dataclasses import dataclass
from datetime import date
from functools import reduce
from hashlib import sha256
import json
from pathlib import Path
import re
from uuid import UUID

from pyspark.sql import DataFrame, functions as F
from silver import MAX_BYTES, REJECTION_SCHEMA, SILVER_SCHEMA

REPLAY_KEY = ['source_id', 'ingestion_id', 'entry_index', 'contract_version']
SCHEMAS = {'accepted': SILVER_SCHEMA, 'rejected': REJECTION_SCHEMA}
WARNINGS = ['INVALID_PUBLISHED_AT', 'INVALID_UPDATED_AT']
REASONS = ['XML_MAPPING_ERROR', 'NO_SUPPORTED_ENTRIES', 'MISSING_IDENTIFIER',
           'MISSING_TITLE', 'MALFORMED_XML', 'UNSUPPORTED_ROOT', 'DTD_FORBIDDEN',
           'UNSUPPORTED_ENCODING']
PROVENANCE = ['source_uri', 'ingested_at', 'payload_byte_length', 'feed_format',
              'payload_sha256', 'bronze_bucket', 'bronze_metadata_key',
              'bronze_payload_key', 'input_uri', 'contract_version']
PATTERN = 'v1/*/source_id=*/ingestion_date=*/ingestion_id=*'


class InputFailure(Exception):
    """Safe structured diagnostics: never include source values or JVM errors."""
    def __init__(self, code, path='', **details):
        self.code, self.path, self.details = code, str(path), details
        super().__init__(json.dumps(dict(code=code, path=self.path, **details), sort_keys=True))


def canonical_bytes(manifest):
    return json.dumps(manifest, sort_keys=True, separators=(',', ':'),
                      ensure_ascii=False, allow_nan=False).encode('utf-8')


def fingerprint(manifest):
    return sha256(canonical_bytes(manifest)).hexdigest()


def ordered(values):
    return sorted(values, key=lambda x: x.encode('utf-8'))


def safe_path(root, path):
    root = Path(root).resolve()
    path = Path(path)
    if '..' in path.parts:
        raise InputFailure('INVALID_SELECTION')
    candidate = path if path.is_absolute() else root / path
    try:
        resolved = candidate.resolve(strict=True)
        resolved.relative_to(root)
        try:
            relative = candidate.relative_to(root).as_posix()
        except ValueError:
            candidate = resolved
            relative = resolved.relative_to(root).as_posix()
    except (OSError, ValueError, RuntimeError):
        raise InputFailure('INVALID_SELECTION') from None
    return candidate, relative


def path_identity(root, marker):
    """Validate the resolved M1 namespace; in-root aliases can denote copies."""
    try:
        parts = marker.resolve(strict=True).relative_to(root).parts
        if len(parts) != 6 or parts[0] != 'v1' or parts[-1] != 'complete.json':
            raise ValueError()
        bucket_hash, source_part, day_part, ingestion_part = parts[1:5]
        if not re.fullmatch('[0-9a-f]{64}', bucket_hash):
            raise ValueError()
        if not source_part.startswith('source_id=') or not day_part.startswith('ingestion_date=') or not ingestion_part.startswith('ingestion_id='):
            raise ValueError()
        encoded = source_part[len('source_id='):]
        source = base64.b64decode(encoded + '=' * (-len(encoded) % 4), altchars=b'-_', validate=True).decode('utf-8')
        if not source.strip() or base64.urlsafe_b64encode(source.encode()).decode().rstrip('=') != encoded:
            raise ValueError()
        day = day_part[len('ingestion_date='):]
        if date.fromisoformat(day).isoformat() != day:
            raise ValueError()
        ingestion = ingestion_part[len('ingestion_id='):]
        if str(UUID(ingestion)) != ingestion:
            raise ValueError()
        key = 'bronze/rss/' + '/'.join(parts[2:5]) + '/metadata.json'
        if len(key.encode()) > 1024:
            raise ValueError()
        return dict(source_id=source, ingestion_id=ingestion, day=day,
                    bucket_hash=bucket_hash, metadata_key=key)
    except (ValueError, OSError, UnicodeError):
        raise InputFailure('INVALID_PUBLICATION_PATH', marker.relative_to(root).as_posix()) from None


@dataclass(frozen=True)
class Selection:
    root: Path
    markers: tuple[str, ...]
    incomplete: tuple[str, ...]
    duplicate_selections: int


def discover(root, *, sources=(), markers=()):
    """Select exact M1 publications only. Paths are relative to root or absolute."""
    try:
        root = Path(root).resolve(strict=True)
    except (OSError, RuntimeError):
        raise InputFailure('INVALID_ROOT') from None
    if not root.is_dir() or (sources and markers):
        raise InputFailure('INVALID_SELECTION')
    if any(not isinstance(s, str) or not s.strip() for s in sources):
        raise InputFailure('INVALID_SELECTION')
    selected, incomplete, seen_sources = [], [], set()
    try:
        candidates = list(markers) if markers else [p / 'complete.json' for p in root.glob(PATTERN)]
    except OSError:
        raise InputFailure('INPUT_READ_FAILED') from None
    for candidate in candidates:
        p = Path(candidate)
        p = p if p.is_absolute() else root / p
        # Filter by the encoded path before opening markers outside the requested scope.
        if sources:
            wanted = {'source_id=' + base64.urlsafe_b64encode(s.encode()).decode().rstrip('=') for s in sources}
            if p.parent.parent.parent.name not in wanted:
                continue
        if not markers and not p.exists() and not p.is_symlink():
            safe_path(root, p.parent)
            incomplete.append(p.relative_to(root).as_posix())
            continue
        p, relative = safe_path(root, p)
        if not p.is_file():
            raise InputFailure('INVALID_SELECTION', relative)
        identity = path_identity(root, p)
        if sources and identity['source_id'] not in sources:
            raise InputFailure('INVALID_PUBLICATION_PATH', relative)
        selected.append(relative)
        seen_sources.add(identity['source_id'])
    if set(sources) - seen_sources:
        raise InputFailure('SOURCE_NOT_FOUND')
    if not selected:
        raise InputFailure('NO_COMPLETED_INPUTS', incomplete_count=len(incomplete))
    return Selection(root, tuple(ordered(set(selected))), tuple(ordered(incomplete)),
                     len(selected) - len(set(selected)))


def parse_marker(root, relative):
    root = Path(root).resolve()
    p, relative = safe_path(root, relative)
    namespace = path_identity(root, p)

    def unique(pairs):
        result = {}
        for k, v in pairs:
            if k in result:
                raise ValueError()
            result[k] = v
        return result

    try:
        with p.open('rb') as stream:
            raw = stream.read(65537)
        if len(raw) > 65536:
            raise ValueError()
        m = json.loads(raw.decode('utf-8'), object_pairs_hook=unique)
        if type(m) is not dict or set(m) != {'identity', 'counts'}:
            raise ValueError()
        i, counts = m['identity'], m['counts']
        if type(i) is not dict or set(i) != {'metadata_sha256', 'payload_sha256', 'contract_version', 'processor_revision', 'bucket', 'metadata_key'}:
            raise ValueError()
        if type(counts) is not dict or set(counts) != {'accepted', 'rejected'}:
            raise ValueError()
        if any(type(n) is not int or n < 0 or n > 2**63-1 for n in counts.values()):
            raise ValueError()
        for field in ('contract_version', 'processor_revision'):
            if type(i[field]) is not int:
                raise ValueError()
            if i[field] != 1:
                raise InputFailure('UNSUPPORTED_VERSION', relative)
        for field in ('metadata_sha256', 'payload_sha256'):
            if type(i[field]) is not str or not re.fullmatch('[0-9a-f]{64}', i[field]):
                raise ValueError()
        if type(i['bucket']) is not str or not i['bucket'].strip():
            raise ValueError()
        if sha256(i['bucket'].encode()).hexdigest() != namespace['bucket_hash'] or i['metadata_key'] != namespace['metadata_key']:
            raise InputFailure('MARKER_PATH_MISMATCH', relative)
    except (ValueError, TypeError, OSError, UnicodeError):
        raise InputFailure('INVALID_MARKER', relative) from None
    return m, sha256(raw).hexdigest(), namespace


def file_record(root, path):
    path, relative = safe_path(root, path)
    if not path.is_file():
        raise InputFailure('INVALID_DATASET', relative)
    digest, length = sha256(), 0
    try:
        with path.open('rb') as stream:
            while block := stream.read(1024 * 1024):
                digest.update(block)
                length += len(block)
    except OSError:
        raise InputFailure('INPUT_READ_FAILED', relative) from None
    return dict(path=relative, byte_length=length, sha256=digest.hexdigest())


def inventory(selection):
    inputs = []
    for relative in selection.markers:
        m, digest, _ = parse_marker(selection.root, relative)
        files = {}
        for name in SCHEMAS:
            directory = (selection.root / relative).parent / name
            if not directory.is_dir():
                raise InputFailure('MISSING_DATASET', relative, dataset=name)
            safe_path(selection.root, directory)
            paths = []
            try:
                entries = list(directory.iterdir())
            except OSError:
                raise InputFailure('INPUT_READ_FAILED', relative, dataset=name) from None
            for p in entries:
                if p.name.startswith(('.', '_')):
                    continue  # Spark CRC and success metadata are not data.
                if p.suffix != '.parquet' or not p.is_file():
                    raise InputFailure('INVALID_DATASET', relative, dataset=name)
                paths.append(p)
            if not paths:
                raise InputFailure('MISSING_DATASET', relative, dataset=name)
            files[name] = [file_record(selection.root, p) for p in sorted(paths, key=lambda p: p.name.encode())]
        inputs.append(dict(marker_path=relative, marker_sha256=digest,
                           identity=m['identity'], counts=m['counts'], files=files))
    representatives = {}
    for item in inputs:
        i = item['identity']
        key = (i['bucket'], i['metadata_key'])
        item['representative_path'] = representatives.setdefault(key, item['marker_path'])
    return dict(manifest_version=1, gold_contract_versions={'gold_articles': 1, 'gold_source_daily': 1},
                gold_processor_revision=1, silver_contract_version=1,
                silver_processor_revision=1, inputs=inputs)


def assert_unchanged(selection, manifest):
    try:
        same = canonical_bytes(inventory(selection)) == canonical_bytes(manifest)
    except InputFailure:
        same = False
    if not same:
        raise InputFailure('INPUT_CHANGED')


def require(frame, condition, code, path):
    # Null predicate results must fail too, rather than disappear in SQL filters.
    if frame.filter(~F.coalesce(condition, F.lit(False))).limit(1).count():
        raise InputFailure(code, path)


def conjunction(conditions):
    return reduce(lambda a, b: a & b, conditions, F.lit(True))


def validate_rows(frame, item, namespace, name):
    i, path = item['identity'], item['marker_path']
    payload_key = i['metadata_key'].removesuffix('metadata.json') + 'payload.xml'
    expected = dict(contract_version=1, source_id=namespace['source_id'],
                    ingestion_id=namespace['ingestion_id'], bronze_bucket=i['bucket'],
                    bronze_metadata_key=i['metadata_key'], bronze_payload_key=payload_key,
                    input_uri=f"s3://{i['bucket']}/{payload_key}", payload_sha256=i['payload_sha256'])
    provenance = [F.col(k).eqNullSafe(F.lit(v)) for k, v in expected.items()]
    provenance += [F.to_date('ingested_at') == F.lit(namespace['day']).cast('date'),
                   F.length(F.trim('source_uri')) > 0,
                   F.col('payload_byte_length').between(0, MAX_BYTES),
                   F.col('feed_format').isin('rss', 'atom')]
    require(frame, conjunction(provenance), 'PROVENANCE_MISMATCH', path)
    warnings = F.col('quality_warnings')
    canonical_warnings = F.filter(F.array(*[F.lit(x) for x in WARNINGS]),
                                  lambda x: F.array_contains(warnings, x))
    rules = [warnings.isNotNull(), warnings.eqNullSafe(canonical_warnings),
             ~F.array_contains(warnings, WARNINGS[0]) | F.col('published_at').isNull(),
             ~F.array_contains(warnings, WARNINGS[1]) | F.col('updated_at').isNull(),
             (F.col('feed_format') != 'rss') | (F.col('updated_at').isNull()
                 & ~F.array_contains(warnings, WARNINGS[1]))]
    article_hash = F.sha2(F.to_json(F.array(F.lit('v1'), 'source_id', 'identifier_kind', 'source_article_id')), 256)
    has_identifier = F.col('source_article_id').isNotNull()
    identity_rules = conjunction([
        F.length('source_article_id') > 0,
        F.col('source_article_id') == F.trim('source_article_id'),
        F.when(F.col('feed_format') == 'rss', F.col('identifier_kind').isin('rss_guid', 'rss_link'))
         .otherwise(F.col('identifier_kind').isin('atom_id', 'atom_link')),
        F.col('article_id').eqNullSafe(article_hash),
        ~F.col('identifier_kind').isin('rss_link', 'atom_link')
        | F.col('source_article_id').eqNullSafe(F.col('url'))])
    rules += [F.when(has_identifier, identity_rules).otherwise(
        F.col('article_id').isNull() & F.col('identifier_kind').isNull()),
        F.col('entry_index').isNull() | (F.col('entry_index') >= 0),
        F.col('title').isNull() | ((F.length('title') > 0) & (F.col('title') == F.trim('title')))]
    if name == 'accepted':
        rules += [has_identifier, F.col('entry_index').isNotNull(), F.col('title').isNotNull()]
    else:
        reasons = F.col('rejection_reasons')
        rules += [reasons.isNotNull(), F.size(reasons) > 0,
                  F.size(F.array_distinct(reasons)) == F.size(reasons),
                  F.forall(reasons, lambda x: x.isNotNull() & x.isin(*REASONS)),
                  reasons.eqNullSafe(F.filter(F.array(*[F.lit(x) for x in REASONS]),
                                             lambda x: F.array_contains(reasons, x)))]
        document = F.exists(reasons, lambda x: x.isin('MALFORMED_XML', 'UNSUPPORTED_ROOT',
                                                     'DTD_FORBIDDEN', 'UNSUPPORTED_ENCODING'))
        rules += [F.when(document,
                         (F.size(reasons) == 1) & F.col('entry_index').isNull()
                         & ~has_identifier & F.col('title').isNull() & (F.size(warnings) == 0))
                  .otherwise(conjunction([
                      F.array_contains(reasons, 'MISSING_IDENTIFIER') == ~has_identifier,
                      F.array_contains(reasons, 'MISSING_TITLE') == F.col('title').isNull(),
                      F.array_contains(reasons, 'NO_SUPPORTED_ENTRIES') == F.col('entry_index').isNull()]))]
    require(frame, conjunction(rules), 'INVALID_ROW', path)


def read_dataset(spark, selection, item, name):
    schema = SCHEMAS[name]
    paths = [str(selection.root / f['path']) for f in item['files'][name]]
    # Inspect every file separately: Spark's default inference can sample one footer.
    for path in paths:
        physical = (spark.read.option('mergeSchema', 'false')
                    .option('ignoreCorruptFiles', 'false').option('ignoreMissingFiles', 'false')
                    .parquet(path).schema)
        if physical.simpleString() != schema.simpleString():
            raise InputFailure('SCHEMA_MISMATCH', item['marker_path'], dataset=name)
    frame = (spark.read.schema(schema).option('mergeSchema', 'false')
             .option('ignoreCorruptFiles', 'false').option('ignoreMissingFiles', 'false')
             .parquet(*paths))
    actual = frame.count()
    if actual != item['counts'][name]:
        raise InputFailure('COUNT_MISMATCH', item['marker_path'], dataset=name,
                           expected=item['counts'][name], actual=actual)
    validate_rows(frame, item, path_identity(selection.root, selection.root / item['marker_path']), name)
    return frame


def conflicts(frame, key, code):
    if frame.distinct().groupBy(*key).count().filter(F.col('count') > 1).limit(1).count():
        raise InputFailure(code)


@dataclass
class ValidatedInputs:
    accepted: DataFrame
    manifest: dict
    input_set_fingerprint: str
    counts: dict
    diagnostics: dict
    selection: Selection

    def verify_unchanged(self):
        """Call again after downstream actions; DataFrames retain lazy file lineage."""
        assert_unchanged(self.selection, self.manifest)


def prepare(spark, selection):
    """All-or-fail validation; no frames escape until the whole set passes."""
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    manifest = inventory(selection)
    frames, representatives = [], {}
    try:
        for item in manifest['inputs']:
            pair = {name: read_dataset(spark, selection, item, name) for name in SCHEMAS}
            representative = item['representative_path']
            if representative in representatives:
                previous_item, previous = representatives[representative]
                if item['identity'] != previous_item['identity'] or item['counts'] != previous_item['counts']:
                    raise InputFailure('PUBLICATION_CONFLICT', item['marker_path'])
                for name in SCHEMAS:
                    if pair[name].exceptAll(previous[name]).limit(1).count() or previous[name].exceptAll(pair[name]).limit(1).count():
                        raise InputFailure('PUBLICATION_CONFLICT', item['marker_path'])
            else:
                representatives[representative] = (item, pair)
                frames.append(pair)
        accepted = reduce(lambda a, b: a.unionByName(b), [p['accepted'] for p in frames])
        rejected = reduce(lambda a, b: a.unionByName(b), [p['rejected'] for p in frames])
        all_rows = accepted.unionByName(rejected.select(SILVER_SCHEMA.fieldNames()))
        conflicts(all_rows.select('source_id', 'ingestion_id', *PROVENANCE),
                  ['source_id', 'ingestion_id'], 'INGESTION_CONFLICT')
        conflicts(accepted, REPLAY_KEY, 'REPLAY_CONFLICT')
        conflicts(rejected, REPLAY_KEY, 'REPLAY_CONFLICT')
        if (accepted.join(rejected, REPLAY_KEY, 'inner').limit(1).count()
                or accepted.join(rejected.filter(F.col('entry_index').isNull()),
                                 ['source_id', 'ingestion_id'], 'inner').limit(1).count()):
            raise InputFailure('ACCEPTED_REJECTED_CONFLICT')
        conflicts(accepted.select('article_id', 'source_id', 'identifier_kind', 'source_article_id'),
                  ['article_id'], 'ARTICLE_IDENTITY_CONFLICT')
        unique = accepted.distinct()
        accepted_count = sum(i['counts']['accepted'] for i, _ in representatives.values())
        rejected_count = sum(i['counts']['rejected'] for i, _ in representatives.values())
        unique_count = unique.count()
        assert_unchanged(selection, manifest)
        return ValidatedInputs(unique, manifest, fingerprint(manifest),
                               dict(accepted=unique_count, accepted_before_replay_collapse=accepted_count,
                                    rejected=rejected_count),
                               dict(duplicate_selections=selection.duplicate_selections,
                                    duplicate_publications=len(manifest['inputs']) - len(representatives),
                                    duplicate_accepted_rows=accepted_count - unique_count,
                                    incomplete=list(selection.incomplete)), selection)
    except InputFailure:
        raise
    except Exception:
        raise InputFailure('PARQUET_VALIDATION_FAILED') from None


def load_inputs(spark, root='outputs/bronze-to-silver', *, sources=(), markers=()):
    return prepare(spark, discover(root, sources=sources, markers=markers))
