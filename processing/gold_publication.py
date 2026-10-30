"""Immutable local Gold Parquet publication and verified replay (single writer)."""
import argparse
from functools import reduce
from hashlib import sha256
import json
from pathlib import Path

from pyspark.sql import functions as F, types as T

from gold import GOLD_ARTICLES_SCHEMA, GOLD_SOURCE_DAILY_SCHEMA, GoldFrames, build_gold
from silver import SILVER_SCHEMA, session
from silver_inputs import InputFailure, WARNINGS, canonical_bytes, fingerprint, load_inputs

SCHEMAS = {'gold_articles': GOLD_ARTICLES_SCHEMA, 'gold_source_daily': GOLD_SOURCE_DAILY_SCHEMA}
VERSIONS = dict.fromkeys(SCHEMAS, 1)
MAX_COUNT = 2**63 - 1
OPTIONAL_ARTICLE = {'url', 'summary', 'published_at', 'updated_at'}
SELECTED = {'contract_version': 'selected_silver_contract_version', **{
    name: 'selected_' + name for name in (
        'ingestion_id', 'ingested_at', 'entry_index', 'input_uri', 'payload_sha256',
        'payload_byte_length', 'bronze_bucket', 'bronze_payload_key', 'bronze_metadata_key')}}


class PublicationFailure(Exception):
    """Fixed classifications and relative paths; no source content/JVM messages."""
    def __init__(self, code, path='', **details):
        self.code, self.path, self.details = code, str(path), details
        super().__init__(json.dumps(dict(code=code, path=self.path, **details), sort_keys=True))


def _require(frame, conditions, code, name):
    condition = reduce(lambda a, b: a & b, conditions, F.lit(True))
    if frame.filter(~F.coalesce(condition, F.lit(False))).limit(1).count():
        raise PublicationFailure(code, name)


def _same_rows(actual, expected, code, name):
    # EXCEPT ALL is null-safe and preserves multiplicity; compare both directions.
    if (actual.exceptAll(expected).limit(1).count()
            or expected.exceptAll(actual).limit(1).count()):
        raise PublicationFailure(code, name)


def _total(column):
    # Decimal prevents silent 64-bit overflow while reconciling count columns.
    return F.coalesce(F.sum(column.cast('decimal(38,0)')), F.lit(0))


def verify_frames(inputs, expected: GoldFrames, actual: GoldFrames):
    """Verify read-back rows with Spark; return only bounded scalar row counts.

    Caller must inspect every physical file schema before constructing actual.
    Both comparisons and provenance checks preserve nulls and array ordering.
    """
    frames = dict(zip(SCHEMAS, (actual.articles, actual.source_daily), strict=True))
    for name, frame in frames.items():
        schema = SCHEMAS[name]
        if frame.schema.simpleString() != schema.simpleString():
            raise PublicationFailure('OUTPUT_SCHEMA_MISMATCH', name)
        optional = OPTIONAL_ARTICLE if name == 'gold_articles' else set()
        required = [f for f in schema if f.name not in optional]
        rules = [F.col(f.name).isNotNull() for f in required]
        rules += [F.length(F.trim(f.name)) > 0 for f in required if isinstance(f.dataType, T.StringType)]
        rules += [F.col('contract_version') == 1, F.col('observation_count') > 0,
                  F.col('distinct_ingestion_count').between(1, F.col('observation_count'))]
        if name == 'gold_articles':
            warnings = F.col('quality_warnings')
            canonical = F.filter(F.array(*[F.lit(w) for w in WARNINGS]),
                                 lambda w: F.array_contains(warnings, w))
            rules += [warnings.eqNullSafe(canonical),
                      F.col('selected_silver_contract_version') == 1,
                      F.col('selected_entry_index') >= 0,
                      F.col('selected_payload_byte_length') >= 0,
                      F.col('first_observed_at') <= F.col('last_observed_at'),
                      F.col('selected_ingested_at') == F.col('last_observed_at')]
            keys = ['article_id']
        else:
            rules += [F.col('distinct_article_count').between(1, F.col('observation_count')),
                      F.col('quality_warning_count') >= 0,
                      F.to_date('latest_ingested_at') == F.col('ingestion_date')]
            keys = ['source_id', 'ingestion_date']
        _require(frame, rules, 'OUTPUT_INVALID_VALUES', name)
        if frame.groupBy(*keys).count().filter(F.col('count') > 1).limit(1).count():
            raise PublicationFailure('OUTPUT_DUPLICATE_KEY', name)

    selected = actual.articles.select(*[
        F.col(SELECTED.get(name, name)).alias(name) for name in SILVER_SCHEMA.fieldNames()])
    if selected.exceptAll(inputs.accepted.select(SILVER_SCHEMA.fieldNames())).limit(1).count():
        raise PublicationFailure('OUTPUT_PROVENANCE_MISMATCH', 'gold_articles')

    source = inputs.accepted.agg(F.count('*').alias('observations'),
                                F.count_distinct('article_id').alias('articles'),
                                _total(F.size('quality_warnings')).alias('warnings')).first()
    article = actual.articles.agg(F.count('*').alias('rows'),
                                 _total(F.col('observation_count')).alias('observations')).first()
    daily = actual.source_daily.agg(F.count('*').alias('rows'),
                                   _total(F.col('observation_count')).alias('observations'),
                                   _total(F.col('quality_warning_count')).alias('warnings')).first()
    for name, wanted, found in (
        ('accepted', inputs.counts['accepted'], source.observations),
        ('gold_articles', source.observations, article.observations),
        ('gold_source_daily', source.observations, daily.observations),
        ('article_id', source.articles, article.rows),
        ('quality_warnings', source.warnings, daily.warnings),
    ):
        if not 0 <= wanted <= MAX_COUNT or not 0 <= found <= MAX_COUNT:
            raise PublicationFailure('COUNT_OVERFLOW', name)
        if wanted != found:
            raise PublicationFailure('OUTPUT_RECONCILIATION_FAILED', name,
                                     expected=int(wanted), actual=int(found))
    _same_rows(actual.articles, expected.articles, 'OUTPUT_ROWS_MISMATCH', 'gold_articles')
    _same_rows(actual.source_daily, expected.source_daily, 'OUTPUT_ROWS_MISMATCH', 'gold_source_daily')
    return dict(gold_articles=article.rows, gold_source_daily=daily.rows)


def _regular(path, relative):
    if path.is_symlink() or not path.is_file():
        raise PublicationFailure('OUTPUT_LAYOUT_INVALID', relative)


def output_inventory(directory):
    """Inventory exact flat datasets; reject links and unexpected visible entries."""
    result = {}
    for name in SCHEMAS:
        dataset = directory / name
        if dataset.is_symlink() or not dataset.is_dir():
            raise PublicationFailure('OUTPUT_LAYOUT_INVALID', name)
        files = []
        for path in sorted(dataset.iterdir(), key=lambda p: p.name.encode('utf-8')):
            if path.name.startswith(('.', '_')):
                continue  # Spark bookkeeping is not analytical data.
            relative = path.relative_to(directory).as_posix()
            _regular(path, relative)
            if path.suffix != '.parquet':
                raise PublicationFailure('OUTPUT_LAYOUT_INVALID', relative)
            digest, length = sha256(), 0
            with path.open('rb') as stream:
                while block := stream.read(1024 * 1024):
                    digest.update(block)
                    length += len(block)
            files.append(dict(path=relative, byte_length=length, sha256=digest.hexdigest()))
        if not files:
            raise PublicationFailure('OUTPUT_LAYOUT_INVALID', name)
        result[name] = files
    return result


def _read_frames(spark, directory, inventory):
    frames = []
    for name, schema in SCHEMAS.items():
        paths = [str(directory / item['path']) for item in inventory[name]]
        reader = (spark.read.option('mergeSchema', 'false')
                  .option('ignoreCorruptFiles', 'false').option('ignoreMissingFiles', 'false'))
        for path in paths:
            if reader.parquet(path).schema.simpleString() != schema.simpleString():
                raise PublicationFailure('OUTPUT_SCHEMA_MISMATCH', name)
        frames.append(reader.schema(schema).parquet(*paths))
    return GoldFrames(*frames)


def _marker(inputs, counts, files):
    # Selection spelling/incomplete discoveries are run diagnostics, not identity.
    return dict(publication_version=1, gold_contract_versions=VERSIONS,
                processor_revision=1, input_set_fingerprint=inputs.input_set_fingerprint,
                input_manifest_sha256=fingerprint(inputs.manifest), input_counts=inputs.counts,
                duplicates={k: inputs.diagnostics[k] for k in
                            ('duplicate_publications', 'duplicate_accepted_rows')},
                output_counts=counts, files=files)


def _read_marker(directory):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError()
            result[key] = value
        return result
    _regular(directory / 'complete.json', 'complete.json')
    raw = (directory / 'complete.json').read_bytes()
    try:
        parsed = json.loads(raw.decode('utf-8'), object_pairs_hook=unique)
        canonical_bytes(parsed)  # rejects non-finite JSON values
    except (ValueError, UnicodeError, RecursionError):
        raise PublicationFailure('INVALID_OUTPUT_MARKER', 'complete.json') from None
    return raw, parsed


def _check_manifest(directory, manifest_bytes):
    _regular(directory / 'inputs.json', 'inputs.json')
    if (directory / 'inputs.json').read_bytes() != manifest_bytes:
        raise PublicationFailure('OUTPUT_MANIFEST_MISMATCH', 'inputs.json')


def _verify(inputs, directory, expected, manifest_bytes, existing):
    names = {'inputs.json', *SCHEMAS} | ({'complete.json'} if existing else set())
    if {p.name for p in directory.iterdir()} != names:
        raise PublicationFailure('OUTPUT_LAYOUT_INVALID')
    _check_manifest(directory, manifest_bytes)
    files = output_inventory(directory)
    marker_raw, marker = _read_marker(directory) if existing else (None, None)
    if existing:
        # Compare canonical bytes, not Python equality (True == 1, 1.0 == 1).
        # Counts are independently checked against read-back below.
        candidate = _marker(inputs, marker.get('output_counts') if isinstance(marker, dict) else None, files)
        if canonical_bytes(marker) != canonical_bytes(candidate):
            raise PublicationFailure('OUTPUT_METADATA_MISMATCH', 'complete.json')
    actual = _read_frames(inputs.accepted.sparkSession, directory, files)
    counts = verify_frames(inputs, expected, actual)
    completed = _marker(inputs, counts, files)
    if existing and canonical_bytes(marker) != canonical_bytes(completed):
        raise PublicationFailure('OUTPUT_METADATA_MISMATCH', 'complete.json')
    # Check again after all Spark actions. Concurrent mutation remains unsupported.
    if output_inventory(directory) != files:
        raise PublicationFailure('OUTPUT_CHANGED')
    _check_manifest(directory, manifest_bytes)
    if existing and (directory / 'complete.json').read_bytes() != marker_raw:
        raise PublicationFailure('OUTPUT_CHANGED', 'complete.json')
    inputs.verify_unchanged()
    return completed


def publish(inputs, output_root='outputs/gold'):
    """Publish ValidatedInputs or fully verify an existing immutable publication.

    Returns PROCESSED/SKIPPED with a root-relative publication path and counts.
    Failures retain incomplete artifacts for operator inspection; never resume,
    overwrite or delete. The caller must enforce one writer per output root.
    """
    manifest_bytes = canonical_bytes(inputs.manifest)
    if (sha256(manifest_bytes).hexdigest() != inputs.input_set_fingerprint
            or inputs.manifest.get('gold_contract_versions') != VERSIONS
            or inputs.manifest.get('gold_processor_revision') != 1):
        raise PublicationFailure('INPUT_IDENTITY_MISMATCH')
    for count in inputs.counts.values():
        if type(count) is not int or not 0 <= count <= MAX_COUNT:
            raise PublicationFailure('COUNT_OVERFLOW')
    relative = Path('v1') / 'processor_revision=1' / ('input_set=' + inputs.input_set_fingerprint)
    phase = 'OUTPUT_WRITE_FAILED'
    try:
        root = Path(output_root).resolve()
        directory = root / relative
        # Do not follow aliases beneath the configured output root.
        for path in (root / 'v1', directory.parent, directory):
            if path.is_symlink() or (path.exists() and not path.is_dir()):
                raise PublicationFailure('OUTPUT_LAYOUT_INVALID', relative)
        directory.parent.mkdir(parents=True, exist_ok=True)
        try:
            directory.mkdir(exist_ok=False)
            existing = False
        except FileExistsError:
            existing = True
        if existing and not (directory / 'complete.json').exists():
            raise PublicationFailure('INCOMPLETE_OUTPUT', relative)
        phase = 'GOLD_PROCESSING_FAILED'
        expected = build_gold(inputs)
        if not existing:
            phase = 'OUTPUT_WRITE_FAILED'
            with (directory / 'inputs.json').open('xb') as stream:
                stream.write(manifest_bytes)
            for name, frame in zip(SCHEMAS, (expected.articles, expected.source_daily), strict=True):
                if frame.schema.simpleString() != SCHEMAS[name].simpleString():
                    raise PublicationFailure('OUTPUT_SCHEMA_MISMATCH', name)
                frame.select(SCHEMAS[name].fieldNames()).write.mode('errorifexists').parquet(str(directory / name))
        phase = 'OUTPUT_READBACK_FAILED'
        completed = _verify(inputs, directory, expected, manifest_bytes, existing)
        if not existing:
            phase = 'OUTPUT_WRITE_FAILED'
            temporary = directory / 'complete.tmp'
            with temporary.open('xb') as stream:
                stream.write(canonical_bytes(completed))
            # Single-writer contract: refuse any observed marker before rename.
            if (directory / 'complete.json').exists() or (directory / 'complete.json').is_symlink():
                raise PublicationFailure('OUTPUT_CHANGED', 'complete.json')
            temporary.rename(directory / 'complete.json')
        return dict(status='SKIPPED' if existing else 'PROCESSED',
                    publication=relative.as_posix(), input_set_fingerprint=inputs.input_set_fingerprint,
                    input_counts=inputs.counts, output_counts=completed['output_counts'],
                    diagnostics=inputs.diagnostics)
    except (PublicationFailure, InputFailure):
        raise
    except Exception:
        raise PublicationFailure(phase, relative) from None


def verify_publication(inputs, output_root='outputs/gold'):
    """Read-only full native verification against exact validated Silver inputs.

    Unlike publish(), this never creates a namespace, even when output is absent.
    Existing standalone publication/CLI and fingerprints are unchanged.
    """
    from gold_reader import verify_metadata
    metadata = verify_metadata(output_root, inputs.input_set_fingerprint)
    return _verify(inputs, metadata.directory, build_gold(inputs), canonical_bytes(inputs.manifest), True)


def run(spark, silver_root='outputs/bronze-to-silver', output_root='outputs/gold', *, sources=(), markers=()):
    """Load the M2.2 boundary, build Gold, publish or verify replay."""
    return publish(load_inputs(spark, silver_root, sources=sources, markers=markers), output_root)


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument('--silver-root', type=Path, default=Path('outputs/bronze-to-silver'))
    parser.add_argument('--gold-root', type=Path, default=Path('outputs/gold'))
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument('--source', action='append', default=[])
    selection.add_argument('--marker', action='append', default=[])
    args = parser.parse_args(argv)
    spark = None
    exit_code = 0
    try:
        spark = session()
        result = run(spark, args.silver_root, args.gold_root,
                     sources=args.source, markers=args.marker)
    except (InputFailure, PublicationFailure) as error:
        result = dict(status='FAILED', code=error.code, path=error.path, details=error.details)
        exit_code = 1
    except Exception:
        result = dict(status='FAILED', code='INITIALIZATION_FAILED')
        exit_code = 1
    finally:
        if spark:
            try:
                spark.stop()
            except Exception:
                if exit_code == 0:
                    result = dict(status='FAILED', code='SPARK_SHUTDOWN_FAILED')
                    exit_code = 1
    print(json.dumps(result, sort_keys=True))
    return exit_code


if __name__ == '__main__':
    raise SystemExit(main())
