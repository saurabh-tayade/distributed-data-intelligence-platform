"""Gold v1 Spark transformations over M2.2 validated Silver observations.

The returned DataFrames are lazy and retain references to Silver Parquet files.
Call ``inputs.verify_unchanged()`` after processing actions and before a future
publication is completed. This is not a transactional snapshot guarantee.
"""
from dataclasses import dataclass

from pyspark.sql import DataFrame, functions as F, types as T, Window

from silver_inputs import ValidatedInputs


def _fields(data_type, *names):
    return [T.StructField(name, data_type) for name in names]


GOLD_ARTICLES_SCHEMA = T.StructType([
    T.StructField('contract_version', T.IntegerType()),
    *_fields(T.StringType(), 'article_id', 'source_id', 'identifier_kind',
             'source_article_id', 'source_uri', 'feed_format', 'title', 'url', 'summary'),
    *_fields(T.TimestampType(), 'published_at', 'updated_at'),
    T.StructField('quality_warnings', T.ArrayType(T.StringType())),
    *_fields(T.TimestampType(), 'first_observed_at', 'last_observed_at'),
    *_fields(T.LongType(), 'observation_count', 'distinct_ingestion_count'),
    T.StructField('selected_silver_contract_version', T.IntegerType()),
    T.StructField('selected_ingestion_id', T.StringType()),
    T.StructField('selected_ingested_at', T.TimestampType()),
    T.StructField('selected_entry_index', T.IntegerType()),
    *_fields(T.StringType(), 'selected_input_uri', 'selected_payload_sha256'),
    T.StructField('selected_payload_byte_length', T.LongType()),
    *_fields(T.StringType(), 'selected_bronze_bucket', 'selected_bronze_payload_key',
             'selected_bronze_metadata_key'),
])

GOLD_SOURCE_DAILY_SCHEMA = T.StructType([
    T.StructField('contract_version', T.IntegerType()),
    T.StructField('source_id', T.StringType()),
    T.StructField('ingestion_date', T.DateType()),
    *_fields(T.LongType(), 'observation_count', 'distinct_article_count',
             'distinct_ingestion_count', 'quality_warning_count'),
    T.StructField('latest_ingested_at', T.TimestampType()),
])


@dataclass(frozen=True)
class GoldFrames:
    articles: DataFrame
    source_daily: DataFrame


def _project(frame, schema, expressions):
    """Use contract order and types, including for schema-bearing empty results."""
    return frame.select(*(expression.cast(field.dataType).alias(field.name)
                          for field, expression in zip(schema.fields, expressions, strict=True)))


def _articles(observations):
    metrics = observations.groupBy('article_id').agg(
        F.min('ingested_at').alias('first_observed_at'),
        F.max('ingested_at').alias('last_observed_at'),
        F.count('*').alias('observation_count'),
        F.count_distinct('ingestion_id').alias('distinct_ingestion_count'))
    order = Window.partitionBy('article_id').orderBy(
        F.col('ingested_at').desc(), F.col('ingestion_id').desc(),
        F.col('entry_index').desc())
    winners = (observations.withColumn('_gold_rank', F.row_number().over(order))
               .filter(F.col('_gold_rank') == 1).drop('_gold_rank'))
    selected = winners.join(metrics, 'article_id')
    return _project(selected, GOLD_ARTICLES_SCHEMA, [
        F.lit(1), F.col('article_id'), F.col('source_id'), F.col('identifier_kind'),
        F.col('source_article_id'), F.col('source_uri'), F.col('feed_format'),
        F.col('title'), F.col('url'), F.col('summary'), F.col('published_at'),
        F.col('updated_at'), F.col('quality_warnings'), F.col('first_observed_at'),
        F.col('last_observed_at'), F.col('observation_count'),
        F.col('distinct_ingestion_count'), F.col('contract_version'),
        F.col('ingestion_id'), F.col('ingested_at'), F.col('entry_index'),
        F.col('input_uri'), F.col('payload_sha256'), F.col('payload_byte_length'),
        F.col('bronze_bucket'), F.col('bronze_payload_key'),
        F.col('bronze_metadata_key'),
    ])


def _source_daily(observations):
    daily = (observations.withColumn('ingestion_date', F.to_date('ingested_at'))
             .groupBy('source_id', 'ingestion_date').agg(
                 F.count('*').alias('observation_count'),
                 F.count_distinct('article_id').alias('distinct_article_count'),
                 F.count_distinct('ingestion_id').alias('distinct_ingestion_count'),
                 F.sum(F.size('quality_warnings')).alias('quality_warning_count'),
                 F.max('ingested_at').alias('latest_ingested_at')))
    return _project(daily, GOLD_SOURCE_DAILY_SCHEMA, [
        F.lit(1), F.col('source_id'), F.col('ingestion_date'),
        F.col('observation_count'), F.col('distinct_article_count'),
        F.col('distinct_ingestion_count'), F.col('quality_warning_count'),
        F.col('latest_ingested_at'),
    ])


def build_gold(inputs: ValidatedInputs) -> GoldFrames:
    """Build both Gold v1 DataFrames from an M2.2 validated input set.

    This only constructs lazy Spark plans. Keep the session in UTC during all
    downstream actions; the caller owns final input re-verification.
    """
    if not isinstance(inputs, ValidatedInputs):
        raise TypeError('build_gold requires M2.2 ValidatedInputs')
    inputs.accepted.sparkSession.conf.set('spark.sql.session.timeZone', 'UTC')
    return GoldFrames(_articles(inputs.accepted), _source_daily(inputs.accepted))
