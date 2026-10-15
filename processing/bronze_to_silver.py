"""Read-only local S3 Bronze consumer with single-writer local publication."""
import argparse
import base64
from datetime import datetime, timezone
from hashlib import sha256
import json
import os
from pathlib import Path
import re
from urllib.parse import urlsplit
from uuid import UUID

from pyspark.sql import functions as F
from silver import (MAX_BYTES, Observation, REJECTION_SCHEMA, SILVER_SCHEMA,
                    normalize_bytes, preflight, session)

METADATA_LIMIT = 64 * 1024
PREFIX = "bronze/rss/"


class Failure(Exception):
    """Messages are fixed classifications, never remote exception text."""


def local_client(env):
    import boto3
    from botocore.config import Config
    names = ("ENDPOINT", "REGION", "BUCKET", "ACCESS_KEY", "SECRET_KEY")
    values = {n: env.get("BRONZE_S3_" + n, "") for n in names}
    if any(not v.strip() for v in values.values()):
        raise Failure("MISSING_LOCAL_CONFIGURATION")
    try:
        u = urlsplit(values["ENDPOINT"])
        if (u.scheme != "http" or u.hostname not in ("127.0.0.1", "localhost", "::1")
                or not u.port or u.username or u.password or u.query or u.fragment
                or u.path not in ("", "/")):
            raise ValueError()
    except ValueError:
        raise Failure("INVALID_LOCAL_ENDPOINT") from None
    # Canonicalize localhost to a literal loopback address; disable proxy discovery.
    host = "[::1]" if u.hostname == "::1" else "127.0.0.1"
    client = boto3.session.Session().client(
        "s3", endpoint_url=f"http://{host}:{u.port}", region_name=values["REGION"],
        aws_access_key_id=values["ACCESS_KEY"], aws_secret_access_key=values["SECRET_KEY"],
        config=Config(signature_version="s3v4", proxies={}, connect_timeout=5, read_timeout=10,
                      retries={"total_max_attempts": 1},
                      s3={"addressing_style": "path"},
                      request_checksum_calculation="when_required",
                      response_checksum_validation="when_required"))
    # Disallow SDK redirects to any other host (including S3 region redirects).
    def guard(request, **kwargs):
        target = urlsplit(request.url)
        if target.hostname != urlsplit(f"http://{host}").hostname or target.port != u.port:
            raise Failure("NONLOCAL_REDIRECT")
    client.meta.events.register("before-send.s3", guard)
    return client, values["BUCKET"]


def read_object(client, bucket, key, limit):
    try:
        response = client.get_object(Bucket=bucket, Key=key)
        body = response["Body"]
        try:
            data = body.read(limit + 1)
        finally:
            body.close()
        if len(data) > limit:
            raise Failure("OBJECT_TOO_LARGE")
        return data
    except Failure:
        raise
    except Exception:
        raise Failure("OBJECT_READ_FAILED") from None


def discover(client, bucket, sources=(), keys=()):
    if keys:
        if any(not k.startswith(PREFIX) or not k.endswith("/metadata.json") for k in keys):
            raise Failure("INVALID_SELECTION")
        return sorted(set(keys))
    prefixes = [PREFIX + "source_id=" + encode(s) + "/" for s in sources] or [PREFIX]
    found = set()
    for prefix in prefixes:
        for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix):
            found.update(o["Key"] for o in page.get("Contents", [])
                         if o["Key"].endswith("/metadata.json"))
    return sorted(found)


def encode(value):
    return base64.urlsafe_b64encode(value.encode("utf-8")).decode("ascii").rstrip("=")


def metadata(raw, key):
    def unique(pairs):
        result = {}
        for k, v in pairs:
            if k in result:
                raise ValueError()
            result[k] = v
        return result
    try:
        m = json.loads(raw.decode("utf-8"), object_pairs_hook=unique)
        required = {"schemaVersion", "ingestionId", "sourceId", "sourceUri", "ingestedAt",
                    "httpStatus", "contentType", "declaredContentLength", "payloadByteLength",
                    "payloadSha256", "payloadKey"}
        if type(m) is not dict or set(m) != required:
            raise ValueError()
        for field in ("ingestionId", "sourceId", "sourceUri", "ingestedAt", "payloadSha256", "payloadKey"):
            if type(m[field]) is not str or not m[field].strip():
                raise ValueError()
        if type(m["schemaVersion"]) is not int or m["schemaVersion"] != 1:
            raise ValueError()
        if str(UUID(m["ingestionId"])) != m["ingestionId"]:
            raise ValueError()
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{1,9})?Z", m["ingestedAt"]):
            raise ValueError()
        instant = datetime.fromisoformat(m["ingestedAt"].replace("Z", "+00:00"))
        uri = urlsplit(m["sourceUri"])
        if uri.scheme not in ("http", "https") or not uri.hostname or uri.username or uri.password:
            raise ValueError()
        if type(m["httpStatus"]) is not int or not 200 <= m["httpStatus"] < 300:
            raise ValueError()
        if m["contentType"] is not None and type(m["contentType"]) is not str:
            raise ValueError()
        if m["declaredContentLength"] is not None and (type(m["declaredContentLength"]) is not int or m["declaredContentLength"] < 0):
            raise ValueError()
        if type(m["payloadByteLength"]) is not int or not 0 <= m["payloadByteLength"] <= MAX_BYTES:
            raise ValueError()
        if not re.fullmatch("[0-9a-f]{64}", m["payloadSha256"]):
            raise ValueError()
        prefix = (PREFIX + "source_id=" + encode(m["sourceId"]) + "/ingestion_date="
                  + instant.date().isoformat() + "/ingestion_id=" + m["ingestionId"] + "/")
        if key != prefix + "metadata.json" or m["payloadKey"] != prefix + "payload.xml" or len(key.encode()) > 1024:
            raise ValueError()
        return m, instant
    except (ValueError, TypeError, KeyError, UnicodeError):
        raise Failure("INVALID_METADATA") from None


def verify_dataset(spark, directory, expected):
    counts = {}
    for name, schema in (("accepted", SILVER_SCHEMA), ("rejected", REJECTION_SCHEMA)):
        frame = spark.read.schema(schema).parquet(str(directory / name))
        frame.createOrReplaceTempView("verify_" + name)
        counts[name] = spark.sql(f"SELECT count(*) AS n FROM verify_{name}").first().n
        for field, value in expected.items():
            if frame.filter(~F.col(field).eqNullSafe(F.lit(value))).limit(1).count():
                raise Failure("OUTPUT_PROVENANCE_MISMATCH")
    return counts


def process_one(spark, client, bucket, key, output):
    raw = read_object(client, bucket, key, METADATA_LIMIT)
    m, instant = metadata(raw, key)
    payload = read_object(client, bucket, m["payloadKey"], MAX_BYTES)
    if len(payload) != m["payloadByteLength"]:
        raise Failure("PAYLOAD_LENGTH_MISMATCH")
    if sha256(payload).hexdigest() != m["payloadSha256"]:
        raise Failure("PAYLOAD_SHA256_MISMATCH")
    feed_format = None
    for candidate in ("rss", "atom"):
        error = preflight(payload, candidate)
        if error is None:
            feed_format = candidate
            break
        if error != "UNSUPPORTED_ROOT":
            raise Failure(error)
    if feed_format is None:
        raise Failure("UNSUPPORTED_ROOT")
    provenance = {"bronze_bucket": bucket, "bronze_payload_key": m["payloadKey"],
                  "bronze_metadata_key": key}
    expected = dict(provenance, source_id=m["sourceId"], ingestion_id=m["ingestionId"],
                    source_uri=m["sourceUri"], ingested_at=instant, payload_sha256=m["payloadSha256"],
                    payload_byte_length=len(payload), feed_format=feed_format, contract_version=1,
                    input_uri=f"s3://{bucket}/{m['payloadKey']}")
    # Bucket hash and original encoded source/date/UUID namespace avoid unsafe local paths.
    directory = Path(output) / "v1" / sha256(bucket.encode()).hexdigest() / key[len(PREFIX):].removesuffix("metadata.json")
    identity = {"metadata_sha256": sha256(raw).hexdigest(), "payload_sha256": m["payloadSha256"],
                "contract_version": 1, "processor_revision": 1, "bucket": bucket, "metadata_key": key}
    marker = directory / "complete.json"
    if directory.exists():
        if not marker.is_file():
            raise Failure("INCOMPLETE_OUTPUT")
        try:
            completed = json.loads(marker.read_text())
        except (ValueError, OSError):
            raise Failure("INVALID_OUTPUT_MARKER") from None
        if completed.get("identity") != identity:
            raise Failure("OUTPUT_CONFLICT")
        counts = verify_dataset(spark, directory, expected)
        if completed.get("counts") != counts:
            raise Failure("OUTPUT_COUNT_MISMATCH")
        return dict(status="SKIPPED", **counts)
    obs = Observation(m["sourceId"], m["sourceUri"], m["ingestionId"], instant, feed_format)
    accepted, rejected = normalize_bytes(spark, payload, obs, expected["input_uri"], provenance)
    directory.mkdir(parents=True, exist_ok=False)
    accepted.write.mode("errorifexists").parquet(str(directory / "accepted"))
    rejected.write.mode("errorifexists").parquet(str(directory / "rejected"))
    counts = verify_dataset(spark, directory, expected)
    if counts != {"accepted": accepted.count(), "rejected": rejected.count()}:
        raise Failure("OUTPUT_COUNT_MISMATCH")
    with (directory / "complete.tmp").open("x") as stream:
        json.dump({"identity": identity, "counts": counts}, stream, sort_keys=True)
    (directory / "complete.tmp").rename(marker)
    return dict(status="PROCESSED", **counts)


def run(spark, client, bucket, keys, output):
    results = []
    for key in keys:
        try:
            result = process_one(spark, client, bucket, key, output)
        except Failure as error:
            result = {"status": "FAILED", "reason": str(error)}
        except Exception:
            result = {"status": "FAILED", "reason": "PROCESSING_FAILED"}
        # Keys contain encoded source/date/UUID; JSON escapes control characters.
        results.append(dict(metadata_key=key, **result))
    return results


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    selection = parser.add_mutually_exclusive_group()
    selection.add_argument("--source", action="append", default=[])
    selection.add_argument("--metadata-key", action="append", default=[])
    parser.add_argument("--output", type=Path, default=Path("outputs/bronze-to-silver"))
    args = parser.parse_args()
    spark = None
    client = None
    try:
        client, bucket = local_client(os.environ)
        keys = discover(client, bucket, args.source, args.metadata_key)
        if not keys:
            print(json.dumps({"status": "NO_SNAPSHOTS", "selected": 0}))
            return 1
        spark = session()
        results = run(spark, client, bucket, keys, args.output)
        for result in results:
            print(json.dumps(result, sort_keys=True))
        return int(any(r["status"] == "FAILED" for r in results))
    except Exception as error:
        print(json.dumps({"status": "FAILED", "reason": str(error) if isinstance(error, Failure) else "INITIALIZATION_FAILED"}))
        return 1
    finally:
        try:
            if spark:
                spark.stop()
        finally:
            # If both cleanups fail, Python retains the shutdown error as context.
            if client:
                client.close()


if __name__ == "__main__":
    raise SystemExit(main())
