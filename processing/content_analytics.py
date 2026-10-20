"""Synchronous, verified Content v1 named reports; no Spark/Silver imports."""
from collections.abc import Mapping
from datetime import date, datetime, timezone
from decimal import Decimal
from hashlib import sha256
import sys
from threading import Lock

import duckdb

import content_metadata as m
from content_dependencies import verify_dependencies
from publication_metadata import canonical_bytes

TOP = frozenset(('inputs.json', 'gold-inputs.json', 'gold-complete.json', 'complete.json', *m.TABLES))
CATALOG_COLUMNS = ('article_id', 'source_id', 'identifier_kind', 'source_article_id', 'feed_format',
    'title', 'url', 'summary', 'published_at', 'updated_at', 'quality_warnings', 'first_observed_at',
    'last_observed_at', 'observation_count', 'distinct_ingestion_count', 'discovered_at', 'discovery_date',
    *m.ATTRIBUTES, 'reference_status', 'reference_id', 'reference_revision', 'reference_fingerprint')
COVERAGE_COLUMNS = ('source_id', 'reference_status', 'reference_usage', *m.ATTRIBUTES,
                    'article_count', 'articles_missing_reference_count', 'missing_optional_attributes')
DAILY_COLUMNS = ('source_id', 'discovery_date', 'discovered_article_count', 'articles_missing_reference_count')
REPORTS = {
    'catalog': (m.CATALOG, CATALOG_COLUMNS, 'last_observed_at DESC, article_id ASC', 10),
    'reference_coverage': ('coverage', COVERAGE_COLUMNS, 'source_id ASC', 100),
    'source_discovery_daily': (m.DAILY, DAILY_COLUMNS, 'source_id ASC, discovery_date ASC', 100),
}
TOTAL_NAMES = ('article_count', 'matched_article_count', 'missing_article_count', 'catalog_source_count',
               'matched_source_count', 'missing_source_count', 'matched_sources_missing_optional_attributes_count',
               'reference_source_count', 'unused_reference_source_count')
TOTAL_SQL = """
SELECT coalesce(sum(article_count),0),
       coalesce(sum(article_count) FILTER (WHERE reference_status='MATCHED'),0),
       coalesce(sum(articles_missing_reference_count),0),
       count(*) FILTER (WHERE article_count>0),
       count(*) FILTER (WHERE reference_usage='USED'),
       count(*) FILTER (WHERE reference_usage='ABSENT'),
       count(*) FILTER (WHERE reference_usage='USED' AND len(missing_optional_attributes)>0),
       count(*) FILTER (WHERE reference_status='MATCHED'),
       count(*) FILTER (WHERE reference_usage='UNUSED')
FROM coverage WHERE ($source IS NULL OR source_id=$source)
"""


def connection():
    conn = duckdb.connect(':memory:', config=dict(threads=2, memory_limit='512MiB',
        temp_directory='', autoinstall_known_extensions=False, autoload_known_extensions=False))
    conn.execute("SET TimeZone='UTC'")
    return conn


def _reject(conn, sql):
    m.need(conn.execute(sql).fetchone() is None)


def _same(conn, actual, expected):
    _reject(conn, f'SELECT 1 FROM (({actual}) EXCEPT ALL ({expected})) LIMIT 1')
    _reject(conn, f'SELECT 1 FROM (({expected}) EXCEPT ALL ({actual})) LIMIT 1')


def register(conn, table, paths, schema):
    for path in paths:
        relation = conn.read_parquet(path, hive_partitioning=False, union_by_name=False)
        m.need(tuple(zip(relation.columns, map(str, relation.types))) == tuple(schema))
        physical = conn.execute('SELECT name,logical_type FROM parquet_schema(?)', [path]).fetchall()
        timestamps = {n for n,t in schema if t == 'TIMESTAMP'}
        for name,logical in physical:
            if name in timestamps and logical is not None:
                m.need('isAdjustedToUTC=0' not in logical)
    conn.read_parquet(list(paths), hive_partitioning=False, union_by_name=False).create_view(table)


def validate_tables(conn, dependency, counts):
    """Independent SQL oracle over complete relations; only scalar checks reach Python."""
    conn.execute('CREATE TABLE reference_rows (source_id VARCHAR, publisher_name VARCHAR, '
                 'publisher_category VARCHAR, declared_language VARCHAR, country_code VARCHAR)')
    rows = [tuple(getattr(row, n) for n in ('source_id', *m.ATTRIBUTES)) for row in dependency.reference.rows]
    if rows:
        conn.executemany('INSERT INTO reference_rows VALUES (?,?,?,?,?)', rows)
    conn.execute('CREATE TABLE selection (reference_id VARCHAR, reference_revision VARCHAR, reference_fingerprint VARCHAR)')
    conn.execute('INSERT INTO selection VALUES (?,?,?)', [dependency.reference.reference_id,
                 dependency.reference.revision, dependency.reference.fingerprint])
    attributes = ','.join('r.' + n for n in m.ATTRIBUTES)
    conn.execute(f"""CREATE VIEW expected_catalog AS SELECT g.*, 1::INTEGER AS content_contract_version,
        g.first_observed_at AS discovered_at, CAST(g.first_observed_at AS DATE) AS discovery_date,
        {attributes}, CASE WHEN r.source_id IS NULL THEN 'MISSING' ELSE 'MATCHED' END AS reference_status,
        s.* FROM base_articles g LEFT JOIN reference_rows r ON g.source_id=r.source_id CROSS JOIN selection s""")
    group_sql = """SELECT 1::INTEGER AS contract_version, source_id, discovery_date,
        count(*)::BIGINT AS discovered_article_count,
        count(*) FILTER (WHERE reference_status='MISSING')::BIGINT AS articles_missing_reference_count
        FROM {table} GROUP BY source_id, discovery_date"""
    conn.execute('CREATE VIEW expected_daily AS ' + group_sql.format(table='expected_catalog'))
    for table, schema in m.SCHEMAS.items():
        optional = m.OPTIONAL if table == m.CATALOG else set()
        nulls = ' OR '.join(n + ' IS NULL' for n,t in schema if n not in optional)
        _reject(conn, f'SELECT 1 FROM {table} WHERE {nulls} OR contract_version<>1 LIMIT 1')
        for n,t in schema:
            if t in ('TIMESTAMP', 'DATE'):
                _reject(conn, f'SELECT 1 FROM {table} WHERE {n} IS NOT NULL AND '
                             f'(NOT isfinite({n}) OR year({n}) NOT BETWEEN 1 AND 9999) LIMIT 1')
        keys = 'article_id' if table == m.CATALOG else 'source_id,discovery_date'
        _reject(conn, f'SELECT 1 FROM {table} GROUP BY {keys} HAVING count(*)>1 LIMIT 1')
        actual_count = conn.execute(f'SELECT count(*) FROM {table}').fetchone()[0]
        m.need(actual_count <= m.MAX_ROWS, 'RESOURCE_LIMIT')
        m.need(actual_count == counts[table])
    _same(conn, 'SELECT ' + ','.join(n for n,t in m.GOLD_SCHEMA) + ' FROM ' + m.CATALOG,
          'SELECT * FROM base_articles')
    _same(conn, 'SELECT article_id FROM ' + m.CATALOG, 'SELECT article_id FROM base_articles')
    _same(conn, 'SELECT * FROM ' + m.CATALOG, 'SELECT * FROM expected_catalog')
    _same(conn, 'SELECT * FROM ' + m.DAILY, 'SELECT * FROM expected_daily')
    _same(conn, 'SELECT * FROM ' + m.DAILY, group_sql.format(table=m.CATALOG))
    discovered, missing = conn.execute(f'SELECT coalesce(sum(discovered_article_count),0), '
        f'coalesce(sum(articles_missing_reference_count),0) FROM {m.DAILY}').fetchone()
    for value in (discovered, missing):
        m.need(0 <= value <= m.MAX_COUNT, 'COUNT_OVERFLOW')
    m.need(discovered == counts[m.CATALOG])
    optional_list = ','.join(f"CASE WHEN r.{name} IS NULL THEN '{name}' END" for name in m.ATTRIBUTES[1:])
    conn.execute(f"""CREATE VIEW coverage AS WITH a AS (
        SELECT source_id,count(*)::BIGINT AS articles FROM {m.CATALOG} GROUP BY source_id)
        SELECT coalesce(a.source_id,r.source_id) AS source_id,
        CASE WHEN r.source_id IS NULL THEN 'MISSING' ELSE 'MATCHED' END AS reference_status,
        CASE WHEN r.source_id IS NULL THEN 'ABSENT' WHEN a.source_id IS NULL THEN 'UNUSED' ELSE 'USED' END AS reference_usage,
        {attributes}, coalesce(a.articles,0)::BIGINT AS article_count,
        CASE WHEN r.source_id IS NULL THEN a.articles ELSE 0 END::BIGINT AS articles_missing_reference_count,
        CASE WHEN r.source_id IS NULL THEN []::VARCHAR[] ELSE list_filter([{optional_list}], x -> x IS NOT NULL) END AS missing_optional_attributes
        FROM a FULL JOIN reference_rows r ON a.source_id=r.source_id""")
    totals = coverage_totals(conn)
    m.need(totals['article_count'] == counts[m.CATALOG] and totals['missing_article_count'] == missing and
           totals['reference_source_count'] == len(rows))


def coverage_totals(conn, source=None):
    values = conn.execute(TOTAL_SQL, dict(source=source)).fetchone()
    for value in values:
        m.need(type(value) is int and 0 <= value <= m.MAX_COUNT, 'COUNT_OVERFLOW')
    result = dict(zip(TOTAL_NAMES, values, strict=True))
    m.need(values[0] == values[1] + values[2] and values[3] == values[4] + values[5] and
           values[7] == values[4] + values[8])
    return result


def filters(name, *, source=None, start_date=None, end_date=None, reference_status=None,
            publisher_category=None, declared_language=None, country_code=None, limit=None, offset=0):
    m.need(type(name) is str and name in REPORTS, 'ARGUMENT_INVALID')
    if limit is None:
        limit = REPORTS[name][3]
    m.need(type(limit) is int and 1 <= limit <= 1000 and type(offset) is int and 0 <= offset <= 1_000_000,
           'ARGUMENT_INVALID')
    if source is not None:
        m.match(source, m.SOURCE)
    values = dict(source=source)
    clauses = ['($source IS NULL OR source_id=$source)']
    supplied = dict(reference_status=reference_status, publisher_category=publisher_category,
                    declared_language=declared_language, country_code=country_code)
    if name != 'catalog':
        m.need(all(v is None for v in supplied.values()), 'ARGUMENT_INVALID')
    else:
        import publisher_reference
        for key,value in supplied.items():
            allowed = ('MATCHED', 'MISSING') if key == 'reference_status' else publisher_reference.VOCABULARY[key]
            m.need(value is None or type(value) is str and value in allowed, 'ARGUMENT_INVALID')
            values[key] = value
            clauses.append(f'(${key} IS NULL OR {key}=${key})')
    if name == 'reference_coverage':
        m.need(start_date is None and end_date is None, 'ARGUMENT_INVALID')
    else:
        try:
            for key,value in (('start_date', start_date), ('end_date', end_date)):
                parsed = date.fromisoformat(value) if type(value) is str else None
                m.need(value is None or parsed is not None and parsed.isoformat() == value, 'ARGUMENT_INVALID')
                values[key] = value
            m.need(not start_date or not end_date or start_date <= end_date, 'ARGUMENT_INVALID')
        except (ValueError, TypeError):
            raise m.ContentFailure('ARGUMENT_INVALID') from None
        clauses += ['(CAST($start_date AS DATE) IS NULL OR discovery_date>=CAST($start_date AS DATE))',
                    '(CAST($end_date AS DATE) IS NULL OR discovery_date<=CAST($end_date AS DATE))']
    return values, ' AND '.join(clauses), limit, offset


def public(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=timezone.utc).isoformat(timespec='microseconds').replace('+00:00', 'Z')
    if isinstance(value, date):
        return value.isoformat()
    if type(value) in (int, Decimal):
        m.need(0 <= value <= m.MAX_COUNT and int(value) == value, 'COUNT_OVERFLOW')
        return str(int(value))
    if isinstance(value, Mapping):
        return {k: public(v) for k,v in value.items()}
    if isinstance(value, (list, tuple)):
        return [public(v) for v in value]
    return value


class ContentAnalytics:
    def __init__(self, identity, manifest, dependency, evidence, conn):
        self._identity, self._manifest = identity, m.freeze(manifest)
        self._dependency, self._evidence, self._conn = dependency, evidence, conn
        self._lock = Lock()

    @property
    def product_fingerprint(self):
        return self._identity

    def _check(self):
        self._evidence.check()
        self._dependency.check()

    def report(self, name, **kwargs):
        try:
            values, where, limit, offset = filters(name, **kwargs)
        except TypeError:
            raise m.ContentFailure('ARGUMENT_INVALID') from None
        m.need(self._lock.acquire(blocking=False), 'BUSY')
        try:
            m.need(self._conn is not None, 'QUERY_FAILED')
            self._check()
            table, columns, order, _ = REPORTS[name]
            sql = f'SELECT {",".join(columns)} FROM {table} WHERE {where} ORDER BY {order} LIMIT $limit OFFSET $offset'
            parameters = values | dict(limit=limit+1, offset=offset)
            # The preflight materializes no strings in Python, including the lookahead row.
            string_columns = ('source_id', 'reference_status', 'reference_usage', *m.ATTRIBUTES) if name == 'reference_coverage' else tuple(
                n for n,t in m.SCHEMAS[table] if t == 'VARCHAR' and n in columns)
            size_sql = ' + '.join(f'octet_length(encode(coalesce({n},\'\')))' for n in string_columns)
            size = self._conn.execute(f'SELECT coalesce(sum({size_sql}),0) FROM ({sql}) page', parameters).fetchone()[0]
            m.need(size <= m.PAGE_BYTES, 'RESPONSE_LIMIT')
            cursor = self._conn.execute(sql, parameters)
            rows = [dict(zip(columns, row, strict=True)) for row in cursor.fetchall()]
            result = dict(status='OK', product_id=m.PRODUCT, product_fingerprint=self._identity,
                          gold_fingerprint=self._dependency.gold_fingerprint,
                          reference=dict(self._manifest['reference']), report=name, filters=values,
                          limit=limit, offset=offset, has_more=len(rows)>limit, rows=rows[:limit])
            if name == 'reference_coverage':
                result['totals'] = coverage_totals(self._conn, values['source'])
            result = public(result)
            m.need(len(canonical_bytes(result))+1 <= m.RESPONSE_LIMIT, 'RESPONSE_LIMIT')
            self._check()
            return result
        except m.ContentFailure:
            try:
                self.close()
            except m.ContentFailure:
                pass
            raise
        except Exception as error:
            try:
                self._check()
            finally:
                try:
                    self.close()
                except m.ContentFailure:
                    pass
            raise m.ContentFailure('RESOURCE_LIMIT' if isinstance(error, duckdb.OutOfMemoryException)
                                   else 'QUERY_FAILED') from None
        finally:
            self._lock.release()

    def close(self):
        if self._conn is not None:
            conn, self._conn = self._conn, None
            try:
                conn.close()
            except Exception:
                raise m.ContentFailure('QUERY_FAILED') from None

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()


def _open_publication(content_root, product_fingerprint, *, gold_root, reference_root):
    m.match(product_fingerprint)
    root = m.root_path(content_root, 'PUBLICATION_UNAVAILABLE')
    directory = m.publication_directory(root, product_fingerprint)
    m.root_path(directory, 'PUBLICATION_UNAVAILABLE')
    try:
        (directory / 'complete.json').lstat()
    except FileNotFoundError:
        raise m.ContentFailure('PUBLICATION_INCOMPLETE') from None
    except OSError:
        raise m.ContentFailure('PUBLICATION_INVALID') from None
    evidence = m.capture(directory, m.TABLES, TOP, 'PUBLICATION_INVALID', 'PUBLICATION_CHANGED',
                         guards=m.namespace_guards(root, directory))
    raw = evidence.raw('inputs.json')
    m.need(sha256(raw).hexdigest() == product_fingerprint)
    manifest = m.validate_manifest(raw)
    marker = m.validate_completion(evidence.raw('complete.json'), product_fingerprint, len(raw))
    m.need(marker['files'] == evidence.inventory())
    reference = manifest['reference']
    dependency = verify_dependencies(gold_root=gold_root, gold_fingerprint=manifest['gold']['publication_fingerprint'],
        reference_root=reference_root, reference_id=reference['reference_id'], revision=reference['revision'],
        reference_fingerprint=reference['snapshot_fingerprint'], output_root=root)
    m.admit(dependency.gold.inventory(), marker['files'])
    for name, original in (('gold-inputs.json', dependency.gold.raw('inputs.json')),
                           ('gold-complete.json', dependency.gold.raw('complete.json'))):
        m.need(evidence.raw(name) == original)
    m.need(dependency.manifest_bytes == raw)
    conn = None
    try:
        conn = connection()
        register(conn, 'base_articles', dependency.paths('gold_articles'), m.GOLD_SCHEMA)
        for table in m.TABLES:
            register(conn, table, [str(directory / item['path']) for item in marker['files'][table]], m.SCHEMAS[table])
        validate_tables(conn, dependency, marker['output_counts'])
        evidence.check()
        dependency.check()
        return ContentAnalytics(product_fingerprint, manifest, dependency, evidence, conn)
    except Exception as error:
        if conn is not None:
            try:
                conn.close()
            except Exception:
                pass
        evidence.check()
        dependency.check()
        if isinstance(error, m.ContentFailure):
            raise
        raise m.ContentFailure('RESOURCE_LIMIT' if isinstance(error, duckdb.OutOfMemoryException)
                               else 'PUBLICATION_INVALID') from None


def open_publication(content_root, product_fingerprint, *, gold_root, reference_root):
    try:
        return _open_publication(content_root, product_fingerprint, gold_root=gold_root,
                                 reference_root=reference_root)
    except m.ContentFailure:
        raise
    except Exception:
        raise m.ContentFailure('PUBLICATION_INVALID') from None


def main(argv=None):
    from content_metadata import arguments
    try:
        options = arguments(sys.argv[1:] if argv is None else argv,
            {'root', 'publication', 'gold-root', 'reference-root', 'report'},
            {'source', 'start-date', 'end-date', 'reference-status', 'publisher-category',
             'declared-language', 'country-code', 'limit', 'offset'})
        if options is None:
            print('Content reports: --root --publication --gold-root --reference-root --report [approved filters]')
            return 0
        kwargs = {k: v for k,v in options.items() if k not in ('root', 'publication', 'gold_root', 'reference_root', 'report')}
        for key in ('limit', 'offset'):
            if key in kwargs:
                import re
                m.need(re.fullmatch(r'0|[1-9][0-9]{0,6}', kwargs[key]) is not None, 'ARGUMENT_INVALID')
                kwargs[key] = int(kwargs[key])
        filters(options['report'], **kwargs)
        with open_publication(options['root'], options['publication'], gold_root=options['gold_root'],
                              reference_root=options['reference_root']) as analytics:
            result = analytics.report(options['report'], **kwargs)
        print(canonical_bytes(result).decode())
        return 0
    except m.ContentFailure as error:
        print(canonical_bytes(error.diagnostic).decode(), file=sys.stderr)
        return 2 if error.code == 'ARGUMENT_INVALID' else 1


if __name__ == '__main__':
    raise SystemExit(main())
