# Local Bronze-to-Silver integration

## Implemented boundary and acceptance

The local consumer reads completed SeaweedFS Bronze snapshots and writes accepted/rejected Silver Parquet on the local filesystem. Java retrieval/storage contracts and accepted ADRs 0001–0003 are unchanged. It does not fetch publishers, write Bronze objects, persist Silver to SeaweedFS, or introduce a cluster.

Acceptance criteria: discover metadata markers, validate their contract and key namespace, bound and verify exact payload bytes, reuse the accepted normalization rules with actual provenance, isolate failed snapshots, verify Parquet and SQL, and prevent duplicate publication on repeated execution. Offline tests use small synthetic fixtures and an in-memory S3 double; local integration is explicit and read-only against retained snapshots.

```text
SeaweedFS S3 list -> sorted metadata.json markers -> bounded metadata GET
  -> schema/key validation -> bounded payload GET -> length + SHA-256
  -> safe XML format detection -> existing Spark normalize_bytes
  -> local accepted/ + rejected/ -> read-back and SQL -> complete.json
```

[bronze_to_silver.py](../../processing/bronze_to_silver.py) owns discovery, integrity and output publication. [silver.py](../../processing/silver.py) adds a byte-input boundary; existing file input delegates to it. Field mappings, identity hashing, warnings, rejection rules and the explicit version-1 schemas remain unchanged. XML is validated with the existing DTD-prohibiting preflight before Spark extraction. RSS and Atom are determined from the validated root, not metadata MIME type or source name. Malformed XML, forbidden DTDs, unsupported roots and unsupported encodings are explicit per-snapshot processing failures; they never reach normalization. Record-level rejections are stored in rejection Parquet.

## Discovery and metadata contract

Only keys ending in `/metadata.json` under `bronze/rss/` are candidates. Listing is paginated, deduplicated and sorted. Payload-only orphans are ignored. By default all candidates are selected; repeated `--source` arguments restrict encoded source prefixes. Alternatively repeated `--metadata-key` arguments select exact markers; both modes cannot be combined. An explicitly selected missing marker is a failure. An empty discovery reports `NO_SNAPSHOTS` and exits nonzero, not a successful integration claim. Listing errors abort discovery rather than processing a partial listing.

The adapter accepts exactly the eleven version-1 fields written by the Java adapter, with no duplicate JSON properties or unknown fields. It checks nonblank source/URI/UUID/time/key/checksum strings, integer schema version 1 (not a boolean), canonical UUID, HTTP(S) source URI, a UTC ISO timestamp ending in Z, 2xx HTTP status, nullable string content type, nullable nonnegative integer declared length, bounded nonnegative actual length, and a lowercase 64-digit hex SHA-256. The successful-status policy belongs to this consumer; Bronze itself can archive non-2xx responses.

Expected keys are reconstructed using the Java contract: unpadded Base64URL of UTF-8 source ID, UTC ingestion date, original UUID, and `payload.xml`/`metadata.json`. Both keys must match that exact namespace; keys over 1024 UTF-8 bytes fail. No path is taken from unchecked metadata. Metadata is limited to **64 KiB**, payloads to **5 MiB**. Reads request at most the limit plus one sentinel byte and always close response bodies; these are object bounds, not total process-memory limits. A larger or unreadable object fails explicitly.

Exact payload bytes remain unchanged until actual length and SHA-256 both match metadata. Declared HTTP Content-Length is audit information, not the integrity length. Missing payload, invalid metadata and checksum/length mismatches fail the snapshot and do not publish output. Subsequent snapshots continue. The CLI reports JSON status, fixed failure classification and the metadata key containing encoded source/date/UUID; it does not print payloads, credentials, SDK exception messages or source URI query strings. Exit is nonzero if any snapshot fails.

## Local S3 client

[requirements-bronze.txt](../../processing/requirements-bronze.txt) pins Boto3 **1.43.114**, Botocore **1.43.114**, and the installed transitive dependencies, in addition to the existing Spark requirements. This SDK supplies signing, pagination and bounded streaming responses without implementing an S3 protocol client. The low-level list/GET API is used; no upload API or transfer service is needed. A custom HTTP signer was rejected as unnecessary infrastructure code; Spark S3A would add JVM/Hadoop connector configuration before a distributed read path is needed.

The existing `BRONZE_S3_ENDPOINT`, `BRONZE_S3_REGION`, `BRONZE_S3_BUCKET`, `BRONZE_S3_ACCESS_KEY`, and `BRONZE_S3_SECRET_KEY` are all mandatory. Only explicit HTTP loopback endpoints with a port and no userinfo/path/query/fragment are allowed. `localhost` is canonicalized to `127.0.0.1`. Credentials are passed explicitly; proxy discovery is disabled; path-style addressing, one attempt, five-second connection and ten-second socket timeouts are configured. A before-send guard rejects redirects to another host/port. There is no fallback to AWS or ambient credentials. Configuration is exported by the operator, not automatically loaded by Python.

## Provenance and output layout

Every accepted/rejected candidate carries original source ID/URI, ingestion UUID, ingestion instant, checksum and actual byte length, plus actual `bronze_bucket`, `bronze_payload_key`, and `bronze_metadata_key`. `input_uri` is the S3 URI, never a temporary local payload path. The normalizer's optional `bronze` argument accepts only `bronze_bucket`, `bronze_payload_key`, and `bronze_metadata_key`; unexpected keys raise `UNEXPECTED_BRONZE_PROVENANCE_FIELDS` before processing and cannot override validated identities or computed integrity fields. As in the accepted Spark contract, timestamp precision is microseconds: Bronze nanoseconds beyond six fractional digits are truncated, with the exact original retained in Bronze metadata.

Default output:

```text
outputs/bronze-to-silver/v1/<sha256-of-bucket>/
  source_id=<base64url-source>/ingestion_date=<UTC-date>/ingestion_id=<UUID>/
    accepted/       # explicit SILVER_SCHEMA Parquet, including warnings
    rejected/       # explicit REJECTION_SCHEMA Parquet, including reasons
    complete.json  # marker written only after read-back verification
```

The bucket digest avoids unsafe bucket path components; source/date/UUID match the validated Bronze namespace. Both empty and nonempty datasets are written. Read-back uses explicit schemas, SQL counts, and per-row checks of all provenance fields against validated metadata. Counts must match the source frames before publication. Repeated article observations from distinct ingestion UUIDs remain separate; no article deduplication is performed.

A completed marker stores metadata/payload fingerprints, bucket/key, contract version, processor revision and counts. Re-execution re-reads and validates Bronze, then compares marker identity and verifies Parquet provenance/counts. Matching output reports `SKIPPED` and writes nothing. Changed metadata, counts or provenance fail. A directory without a valid completion marker is incomplete and is never overwritten automatically. Inspect/quarantine incomplete local output manually or select a fresh output root. Do not delete Bronze to recover processing.

This is a **single-writer local publication convention**, not a transactional table or exactly-once system. Directory creation refuses existing paths; the marker is renamed into place after data verification. There is no fsync/power-loss guarantee, distributed lock, atomic commit across all ingestions, automated recovery, or content-level integrity check of all Silver fields on replay. Readers must enumerate `complete.json` markers and read only their datasets, not glob incomplete Parquet directories. Different output roots are separate publications and must not be unioned blindly. A new transformation revision needs a reviewed output-version/reprocessing policy; it must not silently reuse old completion markers. Existing accepted ADRs already establish observation identity and explicitly exclude a transactional sink; no new durable platform decision is made here.

## Reproducible execution

Use Python **3.12.15**, Java **21.0.7** (Java 21), PySpark **4.0.2**, and Spark `local[2]`. See [Python setup](silver-processing.md#contract-correction-acceptance-and-python-setup). Install the additional pinned dependencies into that isolated environment; bootstrap needs package access, default tests do not.

```sh
uv pip install --python outputs/silver-python312/.venv/bin/python \
  -r processing/requirements-bronze.txt
export PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python"
export SPARK_LOCAL_IP=127.0.0.1 TZ=UTC
PYTHONPATH=processing "$PYSPARK_PYTHON" -m unittest discover -s processing/tests -v
```

Opt-in retained-data integration, with the existing SeaweedFS instance already running:

```sh
set -a
. ./.env
set +a
"$PYSPARK_PYTHON" processing/bronze_to_silver.py \
  --source nasa-technology --source guardian-world --source xkcd
# Repeat exactly the same command to verify read-back and SKIPPED replay.
```

No batch ingestion command is required. Do not run the Java live-feeds profile for this workflow. All output stays in ignored `outputs/`; alternate output roots must also be kept outside Git. Processing never changes existing Bronze objects.

For aggregate SQL, load only completed datasets:

```python
from pathlib import Path
from silver import session, SILVER_SCHEMA

spark = session()
try:
    markers = sorted(Path("outputs/bronze-to-silver").rglob("complete.json"))
    if not markers:
        raise RuntimeError("No completed datasets")
    observations = spark.read.schema(SILVER_SCHEMA).parquet(
        *[str(marker.parent / "accepted") for marker in markers])
    observations.createOrReplaceTempView("observations")
    spark.sql("""
        SELECT source_id, ingestion_id, count(*) AS observations,
               sum(size(quality_warnings)) AS warnings
        FROM observations GROUP BY source_id, ingestion_id
        ORDER BY source_id, ingestion_id
    """).show(truncate=False)
finally:
    spark.stop()
```

## Actual local integration results

Verified October 14, 2026 (America/New_York; October 15 UTC), macOS ARM64, Python 3.12.15, Java 21.0.7, Spark 4.0.2 `local[2]`, existing SeaweedFS endpoint and `content-lake` bucket. All six retained metadata markers were discovered, validated and read without publisher requests. Length and SHA-256 matched for every payload. Both Parquet schemas, provenance and SQL counts passed.

| Source | Original ingestion UUID | Format | Accepted | Rejected | Warnings |
| --- | --- | --- | --- | --- | --- |
| guardian-world | `0a171a1a-6149-4693-b5f2-c15282dd6b21` | RSS | 45 | 0 | 0 |
| guardian-world | `9574943a-da7c-441f-8e1b-b94dfd348868` | RSS | 45 | 0 | 0 |
| nasa-technology | `1251f3be-2d17-4147-b09d-dd3c96c795de` | RSS | 10 | 0 | 0 |
| nasa-technology | `f11cfda7-3047-468e-9255-b0d632b5d5ce` | RSS | 10 | 0 | 0 |
| xkcd | `78e1a5a5-1734-4f8d-b342-37fbc2ae645c` | Atom | 4 | 0 | 0 |
| xkcd | `bf801dba-84d7-43bf-8314-1d3d969c5288` | Atom | 4 | 0 | 0 |

Aggregate SQL returned **118 observations, 59 distinct article IDs, and 6 ingestion UUIDs**. Thus repeated article observations were preserved. A second execution reported six `SKIPPED` results with identical counts after integrity/provenance/read-back checks; no duplicate output was published. No raw publisher content is included in this document or tests.

## Validation and limits

The offline suite covers paginated discovery, orphan exclusion, exact selection, metadata types/version/keys, bounded reads, local endpoint safeguards, missing payloads, checksum and length mismatches, RSS/Atom provenance, rejected rows, failure isolation, incomplete/conflicting outputs, replay and explicit-schema Parquet/SQL. Existing Silver tests cover quality warnings and identity semantics.

No distributed performance, AWS compatibility, S3A, Silver-on-SeaweedFS storage, Iceberg, scheduling or crash/power-loss recovery was executed or claimed. Source dialect limitations from the [Silver contract](silver-processing.md) still apply. Output path limits of the local filesystem can be narrower than S3 key limits; such failures are explicit processing failures. Single-process buffering and collection of marker keys are suitable for this bounded local workload, not a scalability claim.

| Validation | Result |
| --- | --- |
| Python 3.12.15 offline suite | PASSED: 17 tests (11 existing Silver, 6 Bronze integration tests) |
| Existing Java offline Maven verify | PASSED: 29 tests, zero failures/errors/skips |
| Existing shell argument regressions | PASSED: 5 cases |
| npm run check | PASSED: Markdown lint and repository hygiene |
| git diff --check and output ignore check | PASSED |
| Retained SeaweedFS snapshots | PASSED: six processed; six skipped on replay |
| Aggregate Parquet read-back and Spark SQL | PASSED: 118 accepted, 0 rejected, 0 warnings |
| Failed checks in this integration work | None |
| AWS, distributed execution, publisher retrieval, crash recovery | NOT EXECUTED |

Spark emitted the existing native-Hadoop fallback warning and Python socket ResourceWarnings during tests; no assertions failed. No native Hadoop installation or unrelated dependency upgrade was introduced.
