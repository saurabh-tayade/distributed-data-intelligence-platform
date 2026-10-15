import io
import json
from hashlib import sha256
from pathlib import Path
import tempfile
import unittest
from unittest.mock import Mock, patch

from bronze_to_silver import (Failure, discover, encode, local_client, metadata,
                             main, process_one, read_object, run)
from silver import ROOT, session


class Store:
    def __init__(self):
        self.objects = {}
        self.reads = []

    def get_object(self, Bucket, Key):
        self.reads.append(Key)
        return {"Body": io.BytesIO(self.objects[Key])}

    def get_paginator(self, name):
        assert name == "list_objects_v2"
        return self

    def paginate(self, Bucket, Prefix):
        # Small pages exercise discovery across pagination boundaries.
        for key in sorted(self.objects):
            if key.startswith(Prefix):
                yield {"Contents": [{"Key": key}]}


def snapshot(store, name="rss-business.xml", number=1):
    source = "fictional"
    uuid = f"00000000-0000-0000-0000-{number:012d}"
    prefix = f"bronze/rss/source_id={encode(source)}/ingestion_date=2026-10-12/ingestion_id={uuid}/"
    payload = (ROOT / "ingestion/src/test/resources/feeds" / name).read_bytes()
    m = dict(schemaVersion=1, ingestionId=uuid, sourceId=source,
             sourceUri="https://example.com/feed", ingestedAt="2026-10-12T13:00:00.123456789Z",
             httpStatus=200, contentType="application/xml", declaredContentLength=None,
             payloadByteLength=len(payload), payloadSha256=sha256(payload).hexdigest(),
             payloadKey=prefix + "payload.xml")
    key = prefix + "metadata.json"
    store.objects[key] = json.dumps(m).encode()
    store.objects[m["payloadKey"]] = payload
    return key, m


class CleanupTest(unittest.TestCase):
    def test_s3_closed_after_spark_shutdown_failure(self):
        spark, client = Mock(), Mock()
        shutdown_error = RuntimeError("synthetic shutdown failure")
        spark.stop.side_effect = shutdown_error
        with patch("sys.argv", ["bronze_to_silver.py"]), \
                patch("bronze_to_silver.local_client", return_value=(client, "local")), \
                patch("bronze_to_silver.discover", return_value=["synthetic"]), \
                patch("bronze_to_silver.session", return_value=spark), \
                patch("bronze_to_silver.run", return_value=[]):
            with self.assertRaises(RuntimeError) as caught:
                main()
        self.assertIs(caught.exception, shutdown_error)
        client.close.assert_called_once_with()

    def test_double_cleanup_failure_preserves_shutdown_context(self):
        spark, client = Mock(), Mock()
        shutdown_error = RuntimeError("synthetic shutdown failure")
        close_error = OSError("synthetic close failure")
        spark.stop.side_effect = shutdown_error
        client.close.side_effect = close_error
        with patch("sys.argv", ["bronze_to_silver.py"]), \
                patch("bronze_to_silver.local_client", return_value=(client, "local")), \
                patch("bronze_to_silver.discover", return_value=["synthetic"]), \
                patch("bronze_to_silver.session", return_value=spark), \
                patch("bronze_to_silver.run", return_value=[]):
            with self.assertRaises(OSError) as caught:
                main()
        self.assertIs(caught.exception, close_error)
        self.assertIs(caught.exception.__context__, shutdown_error)
        client.close.assert_called_once_with()


class BronzeValidationTest(unittest.TestCase):
    def test_discovery_orphans_and_selection(self):
        store = Store()
        key, m = snapshot(store)
        store.objects["bronze/rss/orphan/payload.xml"] = b"orphan"
        self.assertEqual(discover(store, "local"), [key])
        self.assertEqual(discover(store, "local", ["fictional"]), [key])
        self.assertEqual(discover(store, "local", ["absent"]), [])
        self.assertEqual(discover(store, "local", keys=[key, key]), [key])
        self.assertEqual(store.reads, [])

    def test_metadata_validation(self):
        store = Store()
        key, m = snapshot(store)
        self.assertEqual(metadata(store.objects[key], key)[0], m)
        variants = {"schemaVersion": True, "sourceId": "wrong", "ingestionId": "bad",
                    "payloadSha256": "xyz", "payloadByteLength": -1, "httpStatus": 404,
                    "declaredContentLength": "123", "contentType": 123,
                    "ingestedAt": "2026-10-12T13:00:00", "payloadKey": "outside"}
        for field, value in variants.items():
            with self.subTest(field=field), self.assertRaises(Failure):
                metadata(json.dumps(dict(m, **{field: value})).encode(), key)
        for raw in (b'{}', b'not json', b'{"schemaVersion":1,"schemaVersion":1}'):
            with self.assertRaises(Failure):
                metadata(raw, key)

    def test_bounds_and_local_endpoint(self):
        store = Store()
        store.objects["x"] = b"12345"
        with self.assertRaisesRegex(Failure, "OBJECT_TOO_LARGE"):
            read_object(store, "local", "x", 4)
        env = {"BRONZE_S3_" + k: v for k, v in dict(ENDPOINT="https://s3.amazonaws.com",
               REGION="us-east-1", BUCKET="local", ACCESS_KEY="local", SECRET_KEY="local").items()}
        for endpoint in ("https://s3.amazonaws.com", "http://127.0.0.1", "http://127.0.0.1:8333/path"):
            with self.assertRaises(Failure):
                local_client(dict(env, BRONZE_S3_ENDPOINT=endpoint))
        client, bucket = local_client(dict(env, BRONZE_S3_ENDPOINT="http://localhost:8333"))
        self.assertEqual(client.meta.endpoint_url, "http://127.0.0.1:8333")
        self.assertEqual(client.meta.config.proxies, {})
        client.close()


class BronzeSparkTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = session()
        cls.spark.sparkContext.setLogLevel("ERROR")

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_publication_replay_provenance_and_conflicts(self):
        store = Store()
        rss, _ = snapshot(store)
        atom, _ = snapshot(store, "atom-science.xml", 2)
        with tempfile.TemporaryDirectory() as tmp:
            results = run(self.spark, store, "local", [rss, atom], tmp)
            self.assertEqual([(r["status"], r["accepted"], r["rejected"]) for r in results],
                             [("PROCESSED", 1, 1), ("PROCESSED", 2, 0)])
            again = run(self.spark, store, "local", [rss, atom], tmp)
            self.assertEqual([r["status"] for r in again], ["SKIPPED", "SKIPPED"])
            marker = next(Path(tmp).rglob("complete.json"))
            saved = marker.read_text()
            marker.unlink()
            result = run(self.spark, store, "local", [rss, atom], tmp)
            self.assertIn("INCOMPLETE_OUTPUT", [r.get("reason") for r in result])
            marker.write_text(saved)
            changed = json.loads(store.objects[rss])
            changed["contentType"] = "text/xml"
            store.objects[rss] = json.dumps(changed).encode()
            self.assertEqual(run(self.spark, store, "local", [rss], tmp)[0]["reason"], "OUTPUT_CONFLICT")

    def test_integrity_failures_isolated(self):
        store = Store()
        keys = []
        for n in range(1, 6):
            key, m = snapshot(store, number=n)
            keys.append(key)
            if n == 1:
                del store.objects[m["payloadKey"]]
            elif n == 2:
                m["payloadSha256"] = "0" * 64
            elif n == 3:
                m["payloadByteLength"] += 1
            elif n == 4:
                m["schemaVersion"] = 2
            store.objects[key] = json.dumps(m).encode()
        with tempfile.TemporaryDirectory() as tmp:
            results = run(self.spark, store, "local", keys, tmp)
            self.assertEqual([r.get("reason") for r in results[:4]],
                             ["OBJECT_READ_FAILED", "PAYLOAD_SHA256_MISMATCH", "PAYLOAD_LENGTH_MISMATCH", "INVALID_METADATA"])
            self.assertEqual(results[-1]["status"], "PROCESSED")
            self.assertEqual(len(list(Path(tmp).rglob("complete.json"))), 1)

    def test_unsupported_and_malformed_are_explicit(self):
        for payload, reason in ((b'<html/>', "UNSUPPORTED_ROOT"), (b'<rss>', "MALFORMED_XML"),
                                (b'<!DOCTYPE rss><rss/>', "DTD_FORBIDDEN")):
            store = Store()
            key, m = snapshot(store)
            m.update(payloadByteLength=len(payload), payloadSha256=sha256(payload).hexdigest())
            store.objects[m["payloadKey"]] = payload
            store.objects[key] = json.dumps(m).encode()
            with tempfile.TemporaryDirectory() as tmp:
                self.assertEqual(run(self.spark, store, "local", [key], tmp)[0]["reason"], reason)
