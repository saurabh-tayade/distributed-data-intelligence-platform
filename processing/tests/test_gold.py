"""Gold v1 contract examples and completed-Silver boundary integration."""
from datetime import datetime, timezone, date
from hashlib import sha256
import json
from pathlib import Path
import tempfile
import unittest

from pyspark.sql import functions as F

from bronze_to_silver import process_one
from gold import (GOLD_ARTICLES_SCHEMA, GOLD_SOURCE_DAILY_SCHEMA, build_gold)
from silver import SILVER_SCHEMA, session
from silver_inputs import ValidatedInputs, load_inputs
from test_bronze import Store, snapshot


def observation(source='alpha', article='a', ingestion=1, entry=0,
                instant='2026-10-12T23:59:59.123456+00:00', **changes):
    source_article_id = article
    article_id = sha256(json.dumps(['v1', source, 'rss_guid', article],
                                   ensure_ascii=False, separators=(',', ':')).encode()).hexdigest()
    row = dict(contract_version=1, article_id=article_id, source_id=source,
               source_uri=f'https://example.com/{source}/feed',
               ingestion_id=f'00000000-0000-0000-0000-{ingestion:012d}',
               ingested_at=datetime.fromisoformat(instant),
               input_uri=f's3://local/{source}/{ingestion}',
               payload_sha256=f'{ingestion:064x}', payload_byte_length=10,
               bronze_bucket='local', bronze_payload_key=f'{source}/{ingestion}/payload.xml',
               bronze_metadata_key=f'{source}/{ingestion}/metadata.json',
               feed_format='rss', entry_index=entry, identifier_kind='rss_guid',
               source_article_id=source_article_id, title=f'{article} title',
               url=f'https://example.com/{article}', summary='old summary',
               published_at=datetime(2026, 10, 11, tzinfo=timezone.utc),
               updated_at=None, quality_warnings=[])
    row.update(changes)
    return row


class GoldTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session()
        cls.spark.sparkContext.setLogLevel('ERROR')

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def build(self, rows):
        frame = self.spark.createDataFrame(rows, SILVER_SCHEMA)
        # Synthetic rows exercise transformations; completed-publication test below
        # exercises actual M2.2 validation and replay collapse.
        inputs = ValidatedInputs(frame, {}, '', {}, {}, None)
        return build_gold(inputs)

    def assert_schemas(self, gold):
        self.assertEqual(gold.articles.schema.simpleString(), GOLD_ARTICLES_SCHEMA.simpleString())
        self.assertEqual(gold.source_daily.schema.simpleString(), GOLD_SOURCE_DAILY_SCHEMA.simpleString())
        self.assertEqual(gold.articles.columns, GOLD_ARTICLES_SCHEMA.fieldNames())
        self.assertEqual(gold.source_daily.columns, GOLD_SOURCE_DAILY_SCHEMA.fieldNames())

    def test_single_observation_and_empty_input(self):
        row = observation()
        gold = self.build([row])
        self.assert_schemas(gold)
        article, = gold.articles.collect()
        daily, = gold.source_daily.collect()
        self.assertEqual(article.contract_version, 1)
        self.assertEqual(article.article_id, row['article_id'])
        self.assertEqual((article.observation_count, article.distinct_ingestion_count), (1, 1))
        self.assertEqual(article.first_observed_at, row['ingested_at'].replace(tzinfo=None))
        self.assertEqual(article.selected_input_uri, row['input_uri'])
        self.assertEqual((daily.source_id, daily.ingestion_date), ('alpha', date(2026, 10, 12)))
        self.assertEqual((daily.observation_count, daily.distinct_article_count,
                          daily.distinct_ingestion_count, daily.quality_warning_count), (1, 1, 1, 0))
        self.assertEqual(daily.latest_ingested_at, article.selected_ingested_at)

        empty = self.build([])
        self.assert_schemas(empty)
        self.assertEqual(empty.articles.count(), 0)
        self.assertEqual(empty.source_daily.count(), 0)

    def test_reconciliation_selection_and_order_independence(self):
        rows = [
            observation(entry=0, title='old', summary='old summary'),
            observation(entry=1, title='same retrieval later', summary='same retrieval'),
            observation(ingestion=2, entry=0, instant='2026-10-13T00:00:00.000001+00:00',
                        title='new', summary=None, url=None, published_at=None,
                        quality_warnings=['INVALID_PUBLISHED_AT', 'INVALID_UPDATED_AT']),
            # This later-retrieved observation has an older ingestion instant.
            observation(ingestion=3, entry=0, instant='2026-10-12T22:00:00+00:00',
                        title='late older'),
            observation(article='b', ingestion=2, entry=1,
                        instant='2026-10-12T20:00:00-04:00',
                        quality_warnings=['INVALID_PUBLISHED_AT'], published_at=None),
            observation(source='beta', article='a', ingestion=4, entry=0,
                        instant='2026-10-12T04:00:00+04:00'),
        ]
        expected = None
        for ordered in (rows, list(reversed(rows))):
            self.spark.conf.set('spark.sql.session.timeZone', 'America/New_York')
            gold = self.build(ordered)
            self.assertEqual(self.spark.conf.get('spark.sql.session.timeZone'), 'UTC')
            self.assert_schemas(gold)
            articles = {r.article_id: r for r in gold.articles.collect()}
            daily = {(r.source_id, r.ingestion_date): r for r in gold.source_daily.collect()}
            self.assertEqual(len(articles), 3)  # unique article IDs
            self.assertEqual(len(daily), 3)  # unique source/date keys
            a = articles[rows[0]['article_id']]
            self.assertEqual((a.title, a.summary, a.url, a.published_at),
                             ('new', None, None, None))
            self.assertEqual((a.observation_count, a.distinct_ingestion_count), (4, 3))
            self.assertEqual((a.selected_ingestion_id, a.selected_entry_index),
                             (rows[2]['ingestion_id'], 0))
            self.assertEqual((a.selected_input_uri, a.selected_payload_sha256,
                              a.selected_payload_byte_length, a.selected_bronze_bucket,
                              a.selected_bronze_payload_key, a.selected_bronze_metadata_key),
                             (rows[2]['input_uri'], rows[2]['payload_sha256'], 10, 'local',
                              rows[2]['bronze_payload_key'], rows[2]['bronze_metadata_key']))
            self.assertEqual(a.quality_warnings, rows[2]['quality_warnings'])
            self.assertEqual(a.first_observed_at, datetime(2026, 10, 12, 22))
            self.assertEqual(a.last_observed_at, datetime(2026, 10, 13, 0, 0, 0, 1))
            self.assertEqual(a.selected_ingested_at, a.last_observed_at)
            self.assertEqual((daily['alpha', date(2026, 10, 12)].observation_count,
                              daily['alpha', date(2026, 10, 12)].distinct_article_count,
                              daily['alpha', date(2026, 10, 12)].distinct_ingestion_count,
                              daily['alpha', date(2026, 10, 12)].quality_warning_count),
                             (3, 1, 2, 0))
            self.assertEqual(daily['alpha', date(2026, 10, 12)].latest_ingested_at,
                             datetime(2026, 10, 12, 23, 59, 59, 123456))
            self.assertEqual((daily['alpha', date(2026, 10, 13)].observation_count,
                              daily['alpha', date(2026, 10, 13)].distinct_article_count,
                              daily['alpha', date(2026, 10, 13)].distinct_ingestion_count,
                              daily['alpha', date(2026, 10, 13)].quality_warning_count),
                             (2, 2, 1, 3))
            self.assertEqual(daily['beta', date(2026, 10, 12)].observation_count, 1)
            self.assertEqual(sum(r.observation_count for r in articles.values()), len(rows))
            self.assertEqual(sum(r.observation_count for r in daily.values()), len(rows))
            self.assertEqual(sum(r.quality_warning_count for r in daily.values()),
                             sum(len(r['quality_warnings']) for r in rows))
            outcome = (sorted((k, v.asDict(recursive=True)) for k, v in articles.items()),
                       sorted((k, v.asDict(recursive=True)) for k, v in daily.items()))
            if expected is None:
                expected = outcome
            else:
                self.assertEqual(outcome, expected)

    def test_latest_tie_breaking(self):
        instant = '2026-10-12T12:00:00+00:00'
        rows = [observation(ingestion=1, entry=9, instant=instant, title='lower UUID'),
                observation(ingestion=2, entry=0, instant=instant, title='lower entry'),
                observation(ingestion=2, entry=3, instant=instant, title='winner')]
        article, = self.build(rows).articles.collect()
        self.assertEqual((article.title, article.selected_ingestion_id,
                          article.selected_entry_index), ('winner', rows[2]['ingestion_id'], 3))
        self.assertEqual((article.observation_count, article.distinct_ingestion_count), (3, 2))

    def test_completed_silver_boundary_and_replay(self):
        store = Store()
        key, _ = snapshot(store)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            process_one(self.spark, store, 'local', key, root)
            marker = next(root.rglob('complete.json'))
            accepted_dir = marker.parent / 'accepted'
            row = self.spark.read.schema(SILVER_SCHEMA).parquet(str(accepted_dir)).first().asDict()
            # Write an identical replay and a distinct occurrence; update marker
            # count so M2.2 validates the completed publication as usual.
            for file in accepted_dir.iterdir():
                file.unlink()
            self.spark.createDataFrame([row, row, dict(row, entry_index=3)],
                                       SILVER_SCHEMA).write.mode('overwrite').parquet(str(accepted_dir))
            value = json.loads(marker.read_text())
            value['counts']['accepted'] = 3
            marker.write_text(json.dumps(value))
            inputs = load_inputs(self.spark, root)
            self.assertEqual(inputs.counts['accepted'], 2)
            self.assertEqual(inputs.diagnostics['duplicate_accepted_rows'], 1)
            gold = build_gold(inputs)
            self.assert_schemas(gold)
            article, = gold.articles.collect()
            daily, = gold.source_daily.collect()
            self.assertEqual((article.observation_count, article.distinct_ingestion_count,
                              article.selected_entry_index), (2, 1, 3))
            self.assertEqual((daily.observation_count, daily.distinct_article_count,
                              daily.distinct_ingestion_count), (2, 1, 1))
            inputs.verify_unchanged()

    def test_completed_silver_with_no_accepted_rows(self):
        store = Store()
        key, _ = snapshot(store)
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            process_one(self.spark, store, 'local', key, root)
            marker = next(root.rglob('complete.json'))
            accepted_dir = marker.parent / 'accepted'
            self.spark.createDataFrame([], SILVER_SCHEMA).write.mode('overwrite').parquet(str(accepted_dir))
            value = json.loads(marker.read_text())
            value['counts']['accepted'] = 0
            marker.write_text(json.dumps(value))
            inputs = load_inputs(self.spark, root)
            self.assertEqual((inputs.counts['accepted'], inputs.counts['rejected']), (0, 1))
            gold = build_gold(inputs)
            self.assert_schemas(gold)
            self.assertEqual((gold.articles.count(), gold.source_daily.count()), (0, 0))
            inputs.verify_unchanged()


if __name__ == '__main__':
    unittest.main()
