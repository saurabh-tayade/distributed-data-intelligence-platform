"""Deterministic Spark Content v1 enrichment; Gold winners remain untouched."""
from dataclasses import dataclass
from functools import reduce

from pyspark.sql import functions as F, types as T

import content_metadata as m

TYPES = {'INTEGER': T.IntegerType(), 'BIGINT': T.LongType(), 'VARCHAR': T.StringType(),
         'TIMESTAMP': T.TimestampType(), 'DATE': T.DateType(),
         'VARCHAR[]': T.ArrayType(T.StringType())}
SCHEMAS = {name: T.StructType([T.StructField(column, TYPES[kind]) for column, kind in fields])
           for name, fields in m.SCHEMAS.items()}
GOLD_SCHEMA = T.StructType([T.StructField(n, TYPES[t]) for n,t in m.GOLD_SCHEMA])
REFERENCE_SCHEMA = T.StructType([T.StructField(n, T.StringType(), i >= 2)
    for i,n in enumerate(('source_id', *m.ATTRIBUTES))])


@dataclass(frozen=True)
class ContentFrames:
    catalog: object
    daily: object


def _require(frame, conditions, code='PUBLICATION_INVALID'):
    condition = reduce(lambda a,b: a & b, conditions, F.lit(True))
    m.need(not frame.filter(~F.coalesce(condition, F.lit(False))).limit(1).count(), code)


def _same(actual, expected):
    m.need(not actual.exceptAll(expected).limit(1).count() and
           not expected.exceptAll(actual).limit(1).count())


def read_frames(spark, paths, schemas):
    result = []
    for name, schema in schemas.items():
        reader = (spark.read.option('mergeSchema', 'false').option('ignoreCorruptFiles', 'false')
                  .option('ignoreMissingFiles', 'false'))
        files = paths[name]
        for path in files:
            m.need(reader.parquet(path).schema.simpleString() == schema.simpleString())
        result.append(reader.schema(schema).parquet(*files))
    return result


def read_gold(spark, dependency):
    return read_frames(spark, {'gold_articles': dependency.paths('gold_articles')},
                       {'gold_articles': GOLD_SCHEMA})[0]


def discovery(catalog):
    # Admission bounds catalog rows; decimal sums additionally guard stored counters.
    groups = catalog.groupBy('source_id', 'discovery_date').agg(
        F.count('*').alias('discovered_article_count'),
        F.sum(F.when(F.col('reference_status') == 'MISSING', F.lit(1).cast('decimal(38,0)'))
              .otherwise(F.lit(0).cast('decimal(38,0)'))).alias('articles_missing_reference_count'))
    _require(groups, [F.col('articles_missing_reference_count').between(0, m.MAX_COUNT)], 'COUNT_OVERFLOW')
    return groups.select(F.lit(1).alias('contract_version'), 'source_id', 'discovery_date',
                         'discovered_article_count', F.col('articles_missing_reference_count').cast('long'))


def build_content(spark, dependency):
    spark.conf.set('spark.sql.session.timeZone', 'UTC')
    gold = read_gold(spark, dependency)
    rows = [tuple(getattr(row, name) for name in REFERENCE_SCHEMA.fieldNames())
            for row in dependency.reference.rows]
    reference = spark.createDataFrame(rows, REFERENCE_SCHEMA)
    m.need(not reference.groupBy('source_id').count().filter(F.col('count') > 1).limit(1).count())
    joined = gold.alias('g').join(reference.alias('r'), F.col('g.source_id') == F.col('r.source_id'), 'left')
    catalog = joined.select(*[F.col('g.' + n).alias(n) for n,t in m.GOLD_SCHEMA],
        F.lit(1).alias('content_contract_version'), F.col('g.first_observed_at').alias('discovered_at'),
        F.to_date('g.first_observed_at').alias('discovery_date'),
        *[F.col('r.' + n).alias(n) for n in m.ATTRIBUTES],
        F.when(F.col('r.source_id').isNotNull(), 'MATCHED').otherwise('MISSING').alias('reference_status'),
        F.lit(dependency.reference.reference_id).alias('reference_id'),
        F.lit(dependency.reference.revision).alias('reference_revision'),
        F.lit(dependency.reference.fingerprint).alias('reference_fingerprint'))
    return gold, ContentFrames(catalog, discovery(catalog))


def verify_frames(gold, expected, actual):
    counts = {}
    for name, frame, schema in ((m.CATALOG, actual.catalog, SCHEMAS[m.CATALOG]),
                                 (m.DAILY, actual.daily, SCHEMAS[m.DAILY])):
        m.need(frame.schema.simpleString() == schema.simpleString())
        optional = m.OPTIONAL if name == m.CATALOG else set()
        rules = [F.col(f.name).isNotNull() for f in schema if f.name not in optional]
        rules.append(F.col('contract_version') == 1)
        for field in schema:
            if isinstance(field.dataType, T.TimestampType):
                rules.append(F.col(field.name).isNull() | F.col(field.name).between(
                    F.lit('0001-01-01 00:00:00').cast('timestamp'),
                    F.lit('9999-12-31 23:59:59.999999').cast('timestamp')))
        if name == m.CATALOG:
            rules += [F.col('content_contract_version') == 1,
                      F.col('discovered_at') == F.col('first_observed_at'),
                      F.col('discovery_date') == F.to_date('discovered_at'),
                      F.col('reference_status').isin('MATCHED', 'MISSING'),
                      ~F.exists('quality_warnings', lambda x: x.isNull())]
            keys = ['article_id']
        else:
            rules += [F.col('discovered_article_count') > 0,
                      F.col('articles_missing_reference_count').between(0, F.col('discovered_article_count'))]
            keys = ['source_id', 'discovery_date']
        _require(frame, rules)
        m.need(not frame.groupBy(*keys).count().filter(F.col('count') > 1).limit(1).count())
        counts[name] = frame.count()
        m.need(counts[name] <= m.MAX_ROWS, 'RESOURCE_LIMIT')
    _same(actual.catalog.select(GOLD_SCHEMA.fieldNames()), gold)
    _same(actual.catalog.select('article_id'), gold.select('article_id'))
    _same(actual.catalog, expected.catalog)
    _same(actual.daily, expected.daily)
    _same(actual.daily, discovery(actual.catalog))
    totals = actual.daily.agg(*[F.coalesce(F.sum(F.col(n).cast('decimal(38,0)')), F.lit(0)).alias(n)
        for n in ('discovered_article_count', 'articles_missing_reference_count')]).first()
    for value in totals:
        m.need(0 <= value <= m.MAX_COUNT, 'COUNT_OVERFLOW')
    m.need(totals.discovered_article_count == counts[m.CATALOG])
    m.need(totals.articles_missing_reference_count ==
           actual.catalog.filter(F.col('reference_status') == 'MISSING').count())
    return counts
