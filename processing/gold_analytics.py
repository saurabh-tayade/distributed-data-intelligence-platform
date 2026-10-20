"""Bounded named DuckDB reports over one verified, immutable Gold publication."""
import argparse
from datetime import date, datetime, timezone
import json
import re

import duckdb

from gold_reader import AnalyticsFailure, TABLES, verify_metadata


# DuckDB logical mapping of Gold v1, kept independent of PySpark imports.
ARTICLE_SCHEMA = [
    ('contract_version', 'INTEGER'),
    *[(name, 'VARCHAR') for name in ('article_id', 'source_id', 'identifier_kind',
       'source_article_id', 'source_uri', 'feed_format', 'title', 'url', 'summary')],
    ('published_at', 'TIMESTAMP'), ('updated_at', 'TIMESTAMP'), ('quality_warnings', 'VARCHAR[]'),
    ('first_observed_at', 'TIMESTAMP'), ('last_observed_at', 'TIMESTAMP'),
    ('observation_count', 'BIGINT'), ('distinct_ingestion_count', 'BIGINT'),
    ('selected_silver_contract_version', 'INTEGER'), ('selected_ingestion_id', 'VARCHAR'),
    ('selected_ingested_at', 'TIMESTAMP'), ('selected_entry_index', 'INTEGER'),
    ('selected_input_uri', 'VARCHAR'), ('selected_payload_sha256', 'VARCHAR'),
    ('selected_payload_byte_length', 'BIGINT'), ('selected_bronze_bucket', 'VARCHAR'),
    ('selected_bronze_payload_key', 'VARCHAR'), ('selected_bronze_metadata_key', 'VARCHAR'),
]
DAILY_SCHEMA = [('contract_version', 'INTEGER'), ('source_id', 'VARCHAR'), ('ingestion_date', 'DATE'),
                ('observation_count', 'BIGINT'), ('distinct_article_count', 'BIGINT'),
                ('distinct_ingestion_count', 'BIGINT'), ('quality_warning_count', 'BIGINT'),
                ('latest_ingested_at', 'TIMESTAMP')]
SCHEMAS = dict(zip(TABLES, (ARTICLE_SCHEMA, DAILY_SCHEMA), strict=True))
MAX_LIMIT = 1000
MAX_OFFSET = 1_000_000

REPORTS = {
    'publishers': """
        SELECT source_id, count(*) AS article_count, sum(observation_count) AS observation_count,
               min(first_observed_at) AS first_observed_at, max(last_observed_at) AS last_observed_at,
               count(*) FILTER (WHERE published_at IS NULL) AS articles_without_published_at
        FROM gold_articles WHERE ($source IS NULL OR source_id = $source)
        GROUP BY source_id ORDER BY source_id LIMIT $limit
    """,
    'daily': """
        SELECT source_id, ingestion_date, observation_count, distinct_article_count,
               distinct_ingestion_count, quality_warning_count, latest_ingested_at
        FROM gold_source_daily
        WHERE ($source IS NULL OR source_id = $source)
          AND (CAST($start AS DATE) IS NULL OR ingestion_date >= CAST($start AS DATE))
          AND (CAST($end AS DATE) IS NULL OR ingestion_date <= CAST($end AS DATE))
        ORDER BY source_id, ingestion_date LIMIT $limit
    """,
    'articles': """
        SELECT * FROM gold_articles WHERE ($source IS NULL OR source_id = $source)
        ORDER BY last_observed_at DESC, article_id LIMIT $limit
    """,
    'quality': """
        SELECT source_id, sum(observation_count) AS observation_count,
               sum(quality_warning_count) AS quality_warning_count,
               sum(quality_warning_count)::DOUBLE / sum(observation_count) AS warning_codes_per_observation
        FROM gold_source_daily
        WHERE ($source IS NULL OR source_id = $source)
          AND (CAST($start AS DATE) IS NULL OR ingestion_date >= CAST($start AS DATE))
          AND (CAST($end AS DATE) IS NULL OR ingestion_date <= CAST($end AS DATE))
        GROUP BY source_id ORDER BY source_id LIMIT $limit
    """,
}


def _reject_rows(connection, query, code):
    if connection.execute(query).fetchone() is not None:
        raise AnalyticsFailure(code)


def _validate_tables(connection, metadata):
    for table, schema in SCHEMAS.items():
        optional = {'url', 'summary', 'published_at', 'updated_at'} if table == 'gold_articles' else set()
        nulls = ' OR '.join(f'{name} IS NULL' for name, _ in schema if name not in optional)
        strings = ' OR '.join(f"length(trim({name})) = 0" for name, kind in schema
                              if name not in optional and kind == 'VARCHAR')
        _reject_rows(connection, f"SELECT 1 FROM {table} WHERE {nulls} OR {strings} OR contract_version <> 1 "
                     'OR observation_count < 1 OR distinct_ingestion_count NOT BETWEEN 1 AND observation_count LIMIT 1',
                     'INVALID_VALUES')
        keys = 'article_id' if table == 'gold_articles' else 'source_id, ingestion_date'
        _reject_rows(connection, f'SELECT 1 FROM {table} GROUP BY {keys} HAVING count(*) > 1 LIMIT 1', 'DUPLICATE_KEY')
        rows, observations = connection.execute(f'SELECT count(*), coalesce(sum(observation_count), 0) FROM {table}').fetchone()
        if rows != metadata.marker['output_counts'][table] or observations != metadata.marker['input_counts']['accepted']:
            raise AnalyticsFailure('COUNT_MISMATCH', table)
    _reject_rows(connection, """
        SELECT 1 FROM gold_articles WHERE selected_silver_contract_version <> 1
          OR selected_entry_index < 0 OR selected_payload_byte_length < 0
          OR first_observed_at > last_observed_at OR selected_ingested_at <> last_observed_at
          OR NOT isfinite(first_observed_at) OR NOT isfinite(last_observed_at)
          OR quality_warnings NOT IN ([]::VARCHAR[], ['INVALID_PUBLISHED_AT'],
                                     ['INVALID_UPDATED_AT'], ['INVALID_PUBLISHED_AT', 'INVALID_UPDATED_AT'])
        LIMIT 1
    """, 'INVALID_VALUES')
    _reject_rows(connection, """
        SELECT 1 FROM gold_source_daily WHERE distinct_article_count NOT BETWEEN 1 AND observation_count
          OR quality_warning_count < 0 OR quality_warning_count::HUGEINT > 2 * observation_count::HUGEINT
          OR ingestion_date <> CAST(latest_ingested_at AS DATE) OR NOT isfinite(latest_ingested_at)
        LIMIT 1
    """, 'INVALID_VALUES')
    # Match sources and totals without summing non-additive distinct counts.
    _reject_rows(connection, """
        WITH a AS (
          SELECT source_id, count(*) AS articles, sum(observation_count) AS observations,
                 min(CAST(first_observed_at AS DATE)) AS first_day, max(last_observed_at) AS latest,
                 sum(len(quality_warnings)) AS selected_warnings
          FROM gold_articles GROUP BY source_id
        ), d AS (
          SELECT source_id, sum(observation_count) AS observations, min(ingestion_date) AS first_day,
                 max(latest_ingested_at) AS latest, sum(quality_warning_count) AS warnings,
                 max(distinct_article_count) AS max_daily_articles
          FROM gold_source_daily GROUP BY source_id
        )
        SELECT 1 FROM a FULL JOIN d USING (source_id)
        WHERE a.observations IS DISTINCT FROM d.observations
           OR a.first_day IS DISTINCT FROM d.first_day OR a.latest IS DISTINCT FROM d.latest
           OR a.selected_warnings > d.warnings OR d.max_daily_articles > a.articles LIMIT 1
    """, 'RECONCILIATION_FAILED')


def _filters(report, source, start, end, limit):
    if report not in REPORTS or type(limit) is not int or not 1 <= limit <= MAX_LIMIT:
        raise AnalyticsFailure('INVALID_ARGUMENT')
    if source is not None and (type(source) is not str or not source.strip()):
        raise AnalyticsFailure('INVALID_ARGUMENT')
    values = {'source': source, 'limit': limit}
    if report in ('daily', 'quality'):
        try:
            for key, value in (('start', start), ('end', end)):
                values[key] = date.fromisoformat(value) if value is not None else None
                if value is not None and values[key].isoformat() != value:
                    raise ValueError()
            if start and end and values['start'] > values['end']:
                raise ValueError()
        except (ValueError, TypeError):
            raise AnalyticsFailure('INVALID_ARGUMENT') from None
    elif start is not None or end is not None:
        raise AnalyticsFailure('INVALID_ARGUMENT')
    return values


def _offset(value):
    if type(value) is not int or not 0 <= value <= MAX_OFFSET:
        raise AnalyticsFailure('INVALID_ARGUMENT')
    return value


def _cli_offset(raw):
    if not re.fullmatch(r'[0-9]{1,7}', raw):
        raise AnalyticsFailure('INVALID_ARGUMENT')
    return _offset(int(raw))


class GoldAnalytics:
    """Context-managed named reports. No unrestricted SQL interface is promised."""
    def __init__(self, metadata, connection):
        self.publication = metadata.identity
        self._metadata, self._connection = metadata, connection

    def report(self, name, *, source=None, start=None, end=None, limit=100):
        values = _filters(name, source, start, end, limit)
        return self._query(REPORTS[name], values)

    def article_page(self, *, source=None, limit=100, offset=0):
        """Opt-in page contract; legacy report() output and limits remain unchanged."""
        values = _filters('articles', source, None, None, limit)
        values['offset'] = _offset(offset)
        values['limit'] = limit + 1  # One bounded lookahead row, including at MAX_LIMIT.
        rows = self._query(REPORTS['articles'].rstrip() + ' OFFSET $offset', values)
        return dict(rows=rows[:limit], offset=offset, has_more=len(rows) > limit)

    def _query(self, sql, values):
        try:
            self._metadata.verify_unchanged()
            cursor = self._connection.execute(sql, values)
            columns = [d[0] for d in cursor.description]
            rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
            self._metadata.verify_unchanged()
            return rows
        except AnalyticsFailure:
            raise
        except Exception:
            raise AnalyticsFailure('QUERY_FAILED') from None

    def close(self):
        self._connection.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def open_publication(gold_root, publication, *, memory_limit=None):
    """Verify one exact publication and expose both views in an in-memory session."""
    if memory_limit is not None and (type(memory_limit) is not str or
                                    re.fullmatch(r'[1-9][0-9]{0,5}(?:KiB|MiB|GiB)', memory_limit) is None):
        raise AnalyticsFailure('INVALID_ARGUMENT')
    metadata = verify_metadata(gold_root, publication)
    connection = None
    try:
        config = {
            'threads': 2, 'autoinstall_known_extensions': False,
            'autoload_known_extensions': False, 'temp_directory': '',
        }
        if memory_limit is not None:
            config['memory_limit'] = memory_limit
        connection = duckdb.connect(':memory:', config=config)
        connection.execute("SET TimeZone = 'UTC'")
        for table, schema in SCHEMAS.items():
            paths = metadata.paths(table)
            for path in paths:
                relation = connection.read_parquet(path, hive_partitioning=False, union_by_name=False)
                if list(zip(relation.columns, map(str, relation.types))) != schema:
                    raise AnalyticsFailure('UNSUPPORTED_SCHEMA', table)
            # Relational API binds paths as values, including quotes in local paths.
            connection.read_parquet(paths, hive_partitioning=False, union_by_name=False).create_view(table)
        _validate_tables(connection, metadata)
        metadata.verify_unchanged()
        return GoldAnalytics(metadata, connection)
    except Exception as error:
        if connection is not None:
            connection.close()
        if isinstance(error, AnalyticsFailure):
            raise
        if memory_limit is not None and isinstance(error, duckdb.OutOfMemoryException):
            raise AnalyticsFailure('RESOURCE_LIMIT') from None
        raise AnalyticsFailure('PARQUET_VALIDATION_FAILED') from None


def _json_value(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
    if isinstance(value, date):
        return value.isoformat()
    raise TypeError()


class _Parser(argparse.ArgumentParser):
    def error(self, message):
        raise AnalyticsFailure('INVALID_ARGUMENT')


def main(argv=None):
    parser = _Parser(description=__doc__)
    parser.add_argument('--gold-root', default='outputs/gold')
    parser.add_argument('--publication', required=True, help='fingerprint or exact root-relative v1 publication path')
    parser.add_argument('--report', required=True, choices=REPORTS)
    parser.add_argument('--source')
    parser.add_argument('--start-date')
    parser.add_argument('--end-date')
    parser.add_argument('--limit', type=int, default=100)
    parser.add_argument('--offset', type=_cli_offset, help='articles only: opt into page metadata; range 0..1000000')
    try:
        args = parser.parse_args(argv)
        _filters(args.report, args.source, args.start_date, args.end_date, args.limit)
        if args.offset is not None:
            _offset(args.offset)
            if args.report != 'articles':
                raise AnalyticsFailure('INVALID_ARGUMENT')
        with open_publication(args.gold_root, args.publication) as analytics:
            result = dict(status='OK', publication=analytics.publication, report=args.report, limit=args.limit)
            if args.offset is None:
                result['rows'] = analytics.report(args.report, source=args.source, start=args.start_date,
                                                  end=args.end_date, limit=args.limit)
            else:
                result.update(analytics.article_page(source=args.source, limit=args.limit, offset=args.offset))
        print(json.dumps(result, default=_json_value, sort_keys=True, allow_nan=False))
        return 0
    except AnalyticsFailure as error:
        print(json.dumps(dict(status='FAILED', code=error.code, path=error.path), sort_keys=True))
        return 2 if error.code == 'INVALID_ARGUMENT' else 1


if __name__ == '__main__':
    raise SystemExit(main())
