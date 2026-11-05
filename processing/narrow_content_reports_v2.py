"""Inactive v2 precompletion scan with actual coverage from the same relation.

The immutable result is a sizing input, not completed native verification.
"""
from dataclasses import dataclass
from collections.abc import Mapping

import duckdb
import content_analytics as native
import content_metadata as m
from narrow_content_reports import _scan_reports


@dataclass(frozen=True)
class PrecompletionScanV2:
    report_counts: Mapping
    coverage: Mapping


def validate_precompletion_v2(dependency, evidence, counts):
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
        report_counts = _scan_reports(conn)
        coverage = native.public(native.coverage_totals(conn))
        return PrecompletionScanV2(report_counts, m.freeze(coverage))
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
            try:
                evidence.check()
            finally:
                dependency.check()
