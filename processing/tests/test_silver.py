import os
from dataclasses import replace
from datetime import datetime, timezone
from hashlib import sha256
import json
from pathlib import Path
import tempfile
import unittest

from silver import (Observation, ROOT, REJECTION_SCHEMA, SILVER_SCHEMA,
                    normalize, normalize_bytes, session)


class SilverTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session()
        cls.spark.sparkContext.setLogLevel("ERROR")
        cls.observation = Observation("fictional", "https://example.com/feed",
                                      "00000000-0000-0000-0000-000000000001",
                                      datetime(2026, 10, 12, 13, tzinfo=timezone.utc), "rss")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def fixture(self, name, observation=None):
        return normalize(self.spark, ROOT / "ingestion/src/test/resources/feeds" / name,
                         observation or self.observation)

    def xml(self, value, observation=None):
        with tempfile.TemporaryDirectory() as tmp:
            path = Path(tmp) / "synthetic.xml"
            path.write_text(value, encoding="utf-8")
            return normalize(self.spark, path, observation or self.observation)

    def test_rss_mapping_and_rejection(self):
        accepted, rejected = self.fixture("rss-technology.xml")
        row, = accepted.collect()
        self.assertEqual(row.source_article_id, "technology-001")
        self.assertEqual(row.identifier_kind, "rss_guid")
        self.assertEqual(row.title, "Café workshop opens")
        self.assertEqual(row.url, "https://example.com/technology/1")
        self.assertIn("ideas & prototypes", row.summary)
        self.assertEqual(row.published_at, datetime(2026, 10, 12, 10))
        self.assertEqual(row.ingested_at, datetime(2026, 10, 12, 13))
        self.assertEqual(row.source_id, self.observation.source_id)
        self.assertEqual(row.source_uri, self.observation.source_uri)
        self.assertEqual(row.ingestion_id, self.observation.ingestion_id)
        self.assertTrue(row.input_uri.endswith("rss-technology.xml"))
        payload = (ROOT / "ingestion/src/test/resources/feeds/rss-technology.xml").read_bytes()
        self.assertEqual(row.payload_sha256, sha256(payload).hexdigest())
        self.assertEqual(row.payload_byte_length, len(payload))
        self.assertIsNone(row.bronze_payload_key)
        self.assertIsNone(row.updated_at)
        bad, = rejected.collect()
        self.assertEqual(bad.entry_index, 1)
        self.assertEqual(bad.rejection_reasons, ["MISSING_IDENTIFIER"])

    def test_atom_mapping_optional_fields(self):
        good, bad = self.fixture("atom-science.xml", replace(self.observation, feed_format="atom"))
        rows = sorted(good.collect(), key=lambda r: r.entry_index)
        self.assertEqual(bad.count(), 0)
        self.assertEqual(rows[0].source_article_id, "https://example.com/science/1")
        self.assertEqual(rows[0].title, "Étoile experiment")
        self.assertEqual(rows[0].url, "https://example.com/science/1")
        self.assertEqual(rows[0].published_at, datetime(2026, 10, 11, 9))
        self.assertEqual(rows[0].updated_at, datetime(2026, 10, 12, 12))
        self.assertEqual(rows[1].title, "水 and light")
        self.assertIsNone(rows[1].published_at)
        self.assertIsNone(rows[1].url)

    def test_validation_and_untrusted_text(self):
        good, bad = self.xml('''<rss><channel>
          <item><guid> a </guid><title> title </title><description><![CDATA[<script>alert(1)</script>]]></description></item>
          <item><guid>b</guid><title> </title></item>
          <item><title>no id</title></item>
          <item><guid>c</guid><title>bad date</title><pubDate>yesterday</pubDate></item>
          <item><link>https://example.com/fallback</link><title>fallback</title></item>
        </channel></rss>''')
        rows = sorted(good.collect(), key=lambda r: r.entry_index)
        self.assertEqual(len(rows), 3)
        self.assertEqual(rows[0].source_article_id, "a")
        self.assertEqual(rows[0].summary, "<script>alert(1)</script>")
        self.assertIsNone(rows[0].published_at)
        self.assertEqual(rows[2].identifier_kind, "rss_link")
        self.assertEqual(rows[0].quality_warnings, [])
        self.assertIsNone(rows[1].published_at)
        self.assertEqual(rows[1].quality_warnings, ["INVALID_PUBLISHED_AT"])
        reasons = [r.rejection_reasons for r in bad.collect()]
        self.assertCountEqual(reasons, [["MISSING_TITLE"], ["MISSING_IDENTIFIER"]])

    def test_identity_and_repeated_observations(self):
        first, _ = self.fixture("rss-technology.xml")
        again, _ = self.fixture("rss-technology.xml")
        later, _ = self.fixture("rss-technology.xml", replace(self.observation,
                               ingestion_id="00000000-0000-0000-0000-000000000002"))
        other, _ = self.fixture("rss-technology.xml", replace(self.observation, source_id="other"))
        a, b, c, d = [f.first() for f in (first, again, later, other)]
        expected = sha256(json.dumps(["v1", "fictional", "rss_guid", "technology-001"],
                                    separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(a.article_id, expected)
        self.assertEqual(a, b)
        self.assertEqual(a.article_id, c.article_id)
        self.assertNotEqual(a.ingestion_id, c.ingestion_id)
        self.assertNotEqual(a.article_id, d.article_id)
        self.assertEqual(first.unionByName(later).count(), 2)
        duplicate, _ = self.xml('<rss><channel>' + '<item><guid>x</guid><title>X</title></item>' * 2 + '</channel></rss>')
        rows = duplicate.collect()
        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0].article_id, rows[1].article_id)
        self.assertNotEqual(rows[0].entry_index, rows[1].entry_index)

    def test_offsets_and_invalid_atom(self):
        good, bad = self.xml('''<feed xmlns="http://www.w3.org/2005/Atom">
          <entry><id>x</id><title>X</title><published>2026-10-12T12:30:00+02:30</published>
          <updated>2026-10-12T05:00:00-05:00</updated>
          <link rel="self" href="https://example.com/self"/><link rel="alternate" href="https://example.com/article"/></entry>
          <entry><title>no id</title></entry>
          <entry><id>y</id><title>Y</title><updated>2026-10-12T10:00:00</updated></entry>
        </feed>''', replace(self.observation, feed_format="atom"))
        row, warned = sorted(good.collect(), key=lambda r: r.entry_index)
        self.assertIsNone(warned.updated_at)
        self.assertEqual(warned.quality_warnings, ["INVALID_UPDATED_AT"])
        self.assertEqual(row.published_at, datetime(2026, 10, 12, 10))
        self.assertEqual(row.updated_at, row.published_at)
        self.assertEqual(row.url, "https://example.com/article")
        self.assertCountEqual([r.rejection_reasons for r in bad.collect()],
                              [["MISSING_IDENTIFIER"]])
        rss, _ = self.xml('<rss><channel><item><guid>x</guid><title>X</title><pubDate>12 Oct 2026 12:30:00 +0230</pubDate></item></channel></rss>')
        self.assertEqual(rss.first().published_at, row.published_at)

    def test_malformed_documents(self):
        for xml, reason in [
            ('<rss><channel><item><guid>x</guid><title>X</title></item></channel>', "MALFORMED_XML"),
            ('<rss><channel></rss>', "MALFORMED_XML"),
            ('', "MALFORMED_XML"),
            ('<html/>', "UNSUPPORTED_ROOT"),
            ('<!DOCTYPE rss [<!ENTITY x SYSTEM "file:///etc/passwd">]><rss/>', "DTD_FORBIDDEN"),
        ]:
            with self.subTest(reason=reason, xml=xml):
                good, bad = self.xml(xml)
                self.assertEqual(good.count(), 0)
                row, = bad.collect()
                self.assertEqual(row.rejection_reasons, [reason])
                self.assertIsNone(row.entry_index)
                self.assertEqual(row.ingestion_id, self.observation.ingestion_id)
        good, bad = self.xml('<rss><channel/></rss>')
        self.assertEqual(good.count(), 0)
        self.assertIn("NO_SUPPORTED_ENTRIES", bad.first().rejection_reasons)

    def test_schema_parquet_and_sql(self):
        good, bad = self.fixture("rss-business.xml")
        self.assertEqual([(f.name, f.dataType) for f in good.schema],
                         [(f.name, f.dataType) for f in SILVER_SCHEMA])
        with tempfile.TemporaryDirectory() as tmp:
            good.write.parquet(tmp + "/articles")
            bad.write.parquet(tmp + "/rejections")
            loaded = self.spark.read.schema(SILVER_SCHEMA).parquet(tmp + "/articles")
            rejected = self.spark.read.schema(REJECTION_SCHEMA).parquet(tmp + "/rejections")
            self.assertEqual(loaded.schema, SILVER_SCHEMA)
            self.assertEqual(loaded.collect(), good.collect())
            self.assertEqual(rejected.collect(), bad.collect())
            loaded.createOrReplaceTempView("test_articles")
            self.assertEqual(self.spark.sql("SELECT count(*) AS n FROM test_articles").first().n, 1)

    def test_atom_identity_fallback(self):
        xml = '<feed xmlns="http://www.w3.org/2005/Atom">' +             '<entry><id>native</id><title>A</title></entry>' +             '<entry><title>B</title><link rel="self" href="https://example.com/self"/><link rel="alternate" href="https://example.com/b"/></entry>' +             '<entry><id>preferred</id><title>C</title><link href="https://example.com/c"/></entry>' +             '<entry><title>D</title><link rel="self" href="https://example.com/d"/></entry></feed>'
        obs = replace(self.observation, feed_format="atom")
        good, bad = self.xml(xml, obs)
        rows = sorted(good.collect(), key=lambda r: r.entry_index)
        self.assertEqual([(r.identifier_kind, r.source_article_id) for r in rows],
                         [("atom_id", "native"), ("atom_link", "https://example.com/b"),
                          ("atom_id", "preferred")])
        self.assertEqual(bad.first().rejection_reasons, ["MISSING_IDENTIFIER"])
        later, _ = self.xml(xml, replace(obs, ingestion_id="00000000-0000-0000-0000-000000000002"))
        repeated = sorted(later.collect(), key=lambda r: r.entry_index)
        self.assertEqual([r.article_id for r in rows], [r.article_id for r in repeated])
        self.assertEqual(good.unionByName(later).count(), 6)
        expected = sha256(json.dumps(["v1", "fictional", "atom_id", "native"], separators=(",", ":")).encode()).hexdigest()
        self.assertEqual(rows[0].article_id, expected)

    def test_atom_content_fallback(self):
        bodies = [
            '<summary>Summary only</summary>',
            '<content>Text only</content>',
            '<summary>Preferred</summary><content>Other</content>',
            '',
            '<summary> </summary><content type="html">&lt;b&gt;Untrusted&lt;/b&gt;</content>',
            '<content type="text">Café &amp; 水</content>',
            '<content type="xhtml"><div xmlns="http://www.w3.org/1999/xhtml">Rich</div></content>',
            '<content src="https://example.com/remote" type="text"/>',
            '<content type="application/octet-stream">AAAA</content>',
        ]
        xml = '<feed xmlns="http://www.w3.org/2005/Atom">' + ''.join(
            f'<entry><id>{i}</id><title>T</title>{body}</entry>' for i, body in enumerate(bodies)) + '</feed>'
        good, bad = self.xml(xml, replace(self.observation, feed_format="atom"))
        rows = sorted(good.collect(), key=lambda r: r.entry_index)
        self.assertEqual(bad.count(), 0)
        self.assertEqual([r.summary for r in rows],
                         ["Summary only", "Text only", "Preferred", None,
                          "<b>Untrusted</b>", "Café & 水", None, None, None])

    def test_warnings_parquet_and_sql(self):
        good, bad = self.xml('''<feed xmlns="http://www.w3.org/2005/Atom">
          <entry><id>a</id><title>A</title><published>bad</published><updated>bad too</updated></entry>
          <entry><id>b</id><title>B</title></entry>
          <entry><id>c</id><published>bad</published></entry>
        </feed>''', replace(self.observation, feed_format="atom"))
        rows = sorted(good.collect(), key=lambda r: r.entry_index)
        self.assertEqual(len(rows), 2)
        self.assertIsNone(rows[0].published_at)
        self.assertIsNone(rows[0].updated_at)
        self.assertEqual(rows[0].quality_warnings, ["INVALID_PUBLISHED_AT", "INVALID_UPDATED_AT"])
        self.assertEqual(rows[1].quality_warnings, [])
        rejected, = bad.collect()
        self.assertEqual(rejected.rejection_reasons, ["MISSING_TITLE"])
        self.assertEqual(rejected.quality_warnings, ["INVALID_PUBLISHED_AT"])
        with tempfile.TemporaryDirectory() as tmp:
            good.write.parquet(tmp + "/accepted")
            bad.write.parquet(tmp + "/rejected")
            loaded = self.spark.read.schema(SILVER_SCHEMA).parquet(tmp + "/accepted")
            self.assertEqual(loaded.schema, SILVER_SCHEMA)
            self.assertEqual(sorted(loaded.collect(), key=lambda r: r.entry_index), rows)
            self.assertEqual(self.spark.read.schema(REJECTION_SCHEMA).parquet(tmp + "/rejected").collect(), [rejected])
            loaded.createOrReplaceTempView("warned_articles")
            self.assertEqual(self.spark.sql("SELECT count(*) AS n FROM warned_articles WHERE array_contains(quality_warnings, 'INVALID_PUBLISHED_AT')").first().n, 1)
            self.assertEqual(self.spark.sql("SELECT explode(quality_warnings) AS warning FROM warned_articles").count(), 2)

    def test_bronze_references_preserve_validated_provenance(self):
        payload = (ROOT / "ingestion/src/test/resources/feeds/rss-business.xml").read_bytes()
        references = {"bronze_bucket": "local", "bronze_payload_key": "bronze/payload.xml",
                      "bronze_metadata_key": "bronze/metadata.json"}
        good, bad = normalize_bytes(self.spark, payload, self.observation,
                                    "s3://local/bronze/payload.xml", references)
        for row in good.collect() + bad.collect():
            for field, value in references.items():
                self.assertEqual(row[field], value)
            self.assertEqual(row.source_id, self.observation.source_id)
            self.assertEqual(row.ingestion_id, self.observation.ingestion_id)
            self.assertEqual(row.payload_sha256, sha256(payload).hexdigest())
            self.assertEqual(row.payload_byte_length, len(payload))
            self.assertEqual(row.ingested_at, datetime(2026, 10, 12, 13))
        baseline, _ = self.fixture("rss-business.xml")
        self.assertEqual(good.first().article_id, baseline.first().article_id)

    def test_bronze_references_reject_overrides(self):
        # Reject before parsing or projecting, including fields used for identity.
        for field in ("unexpected", "source_id", "source_uri", "ingestion_id",
                      "payload_sha256", "payload_byte_length", "ingested_at", "article_id",
                      "identifier_kind", "source_article_id", "contract_version", "feed_format"):
            with self.subTest(field=field), self.assertRaisesRegex(
                    ValueError, "UNEXPECTED_BRONZE_PROVENANCE_FIELDS"):
                normalize_bytes(self.spark, b"<rss/>", self.observation, "fixture://test",
                                {"bronze_bucket": "local", field: "override"})

    def test_invalid_provenance(self):
        for observation in [replace(self.observation, source_id=" "),
                            replace(self.observation, ingestion_id="invalid"),
                            replace(self.observation, ingested_at=datetime(2026, 10, 12))]:
            with self.assertRaises(ValueError):
                self.fixture("rss-business.xml", observation)


if __name__ == "__main__":
    os.environ["TZ"] = "UTC"
    unittest.main()
