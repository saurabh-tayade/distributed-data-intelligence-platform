"""Inactive, versioned full-report bounds for narrow managed Content.

Native metadata, schema, provenance and relational validation remain authoritative.
This module neither publishes nor repairs evidence and has no Spark dependency.
The SQL lower bound protects Python materialization; only the existing public
conversion and complete canonical row validator establish the actual byte bound.
"""
import duckdb

import content_analytics as native
import content_metadata as m
import narrow_content_profile as profile
import publisher_reference


def _fields(report):
    table, columns, _, _ = native.REPORTS[report]
    if report == 'reference_coverage':
        kinds = dict.fromkeys(('source_id', 'reference_status', 'reference_usage', *m.ATTRIBUTES),
                              'VARCHAR')
        kinds['missing_optional_attributes'] = 'VARCHAR[]'
    else:
        kinds = dict(m.SCHEMAS[table])
    return table, columns, kinds


def _lower_bound(columns, kinds):
    terms = []
    for name in columns:
        if kinds.get(name) == 'VARCHAR':
            terms.append(f"octet_length(encode(coalesce({name},'')))::HUGEINT")
        elif kinds.get(name) == 'VARCHAR[]':
            # Include minimal quotes/separators, not just raw element bytes: an
            # array of empty strings must not escape the materialization bound.
            array = f"coalesce({name},[]::VARCHAR[])"
            terms.append(f"(coalesce(list_sum(list_transform({array}, x -> "
                         "octet_length(encode(coalesce(x,'')))::HUGEINT)),0::HUGEINT) "
                         f"+ 2 * len({array})::HUGEINT + greatest(len({array})::HUGEINT - 1,0) + 2)")
    return ' + '.join(terms) or '0::HUGEINT'


def _scan_reports(conn):
    """Visit every unfiltered public row without a sort or unbounded Python fetch."""
    specifications = []
    for report in native.REPORTS:
        table, columns, kinds = _fields(report)
        lower = _lower_bound(columns, kinds)
        maximum = m.MAX_ROWS + (publisher_reference.ROW_LIMIT if report == 'reference_coverage' else 0)
        count, size = conn.execute(f'SELECT count(*), coalesce(max({lower}),0) FROM {table}').fetchone()
        m.need(type(count) is int and 0 <= count <= maximum and
               type(size) is int and 0 <= size <= profile.REPORT_ROW_LIMIT, 'RESOURCE_LIMIT')
        specifications.append((report, table, columns, lower, count, maximum))
    counts = {}
    for report, table, columns, lower, expected, maximum in specifications:
        # Reapply the necessary bound in the projection itself. If a file changes
        # between the scalar preflight and this query, oversized strings/arrays
        # still cannot reach Python. Retained native evidence is checked below.
        guarded = ','.join(f'CASE WHEN _row_bytes <= {profile.REPORT_ROW_LIMIT} THEN {name} END'
                           for name in columns)
        cursor = conn.execute(f'SELECT _row_bytes,{guarded} FROM '
                              f'(SELECT {",".join(columns)},({lower}) AS _row_bytes FROM {table}) bounded')
        count = 0
        while (values := cursor.fetchone()) is not None:
            count += 1
            m.need(count <= maximum and count <= expected and
                   0 <= values[0] <= profile.REPORT_ROW_LIMIT, 'RESOURCE_LIMIT')
            row = dict(zip(columns, values[1:], strict=True))
            try:
                profile.validate_report_row(report, row)
            except profile.ProfileError:
                raise m.ContentFailure('RESOURCE_LIMIT') from None
        m.need(count == expected, 'PUBLICATION_CHANGED')
        counts[report] = count
    return m.freeze(counts)


def validate_precompletion_v1(dependency, evidence, counts):
    """Check read-back evidence before either completion marker is created.

    Call only after the native producer's full read-back verification. Reuse its
    exact captured paths and the independent native DuckDB validation, including
    the reference-only coverage relation. No completion marker is required here.
    """
    conn, failed = None, False
    try:
        evidence.check()
        dependency.check()
        conn = native.connection()
        native.register(conn, 'base_articles', dependency.paths('gold_articles'), m.GOLD_SCHEMA)
        for table, files in evidence.inventory().items():
            native.register(conn, table, tuple(str(evidence.directory / item['path']) for item in files),
                            m.SCHEMAS[table])
        native.validate_tables(conn, dependency, counts)
        return _scan_reports(conn)
    except m.ContentFailure:
        failed = True
        raise
    except Exception as error:
        failed = True
        raise m.ContentFailure('RESOURCE_LIMIT' if isinstance(error, duckdb.OutOfMemoryException)
                               else 'PUBLICATION_FAILED') from None
    finally:
        try:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    if not failed:
                        raise m.ContentFailure('PUBLICATION_FAILED') from None
        finally:
            # A failure does not excuse checking retained mutation evidence.
            evidence.check()
            dependency.check()


def verify_publication_v1(content_root, product_fingerprint, *, gold_root, reference_root):
    """Read-only full qualification/reconciliation of an already completed output.

    Missing, incomplete or stale-temporary publications remain blocked and are
    never created, adopted or repaired. An acknowledgement lost after rename can
    be resolved here without calling any producer or importing Spark.
    """
    opened = None
    try:
        with native.open_publication(content_root, product_fingerprint,
                                     gold_root=gold_root, reference_root=reference_root) as opened:
            try:
                report_counts = _scan_reports(opened._conn)
                marker = m.validate_completion(opened._evidence.raw('complete.json'), product_fingerprint,
                                               len(opened._evidence.raw('inputs.json')))
                return m.freeze(dict(status='VERIFIED', product_id=m.PRODUCT,
                    product_fingerprint=product_fingerprint, output_counts=marker['output_counts'],
                    report_counts=dict(report_counts)))
            finally:
                opened._check()
    except m.ContentFailure:
        raise
    except Exception as error:
        if opened is not None:
            opened._check()
        raise m.ContentFailure('RESOURCE_LIMIT' if isinstance(error, duckdb.OutOfMemoryException)
                               else 'PUBLICATION_INVALID') from None
