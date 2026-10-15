# M1 Day 5: Silver contract and local Spark prototype

## Scope and acceptance criteria

This is an implemented **fixture-only prototype**, not the operational Bronze-to-Silver pipeline or a persisted SeaweedFS Silver lake. The proposed contract below is exercised locally; durable identity choices await review in [ADR 0003](../adr/0003-silver-observation-contract.md). Accepted ADRs and Java ingestion contracts are unchanged.

Acceptance criteria: map the existing fictional RSS/Atom fixtures using explicit schemas; retain provenance and repeated observations; normalize supported dates to UTC; report missing required fields and malformed documents; write and read local Parquet; verify field mapping, identity, rejection, SQL and schema behavior through offline tests. No publishers, Bronze objects, cloud credentials, clusters or new Docker services are involved.

```text
Existing synthetic XML fixtures + explicit synthetic observation envelope
  -> bounded local read + complete-document XML validation
  -> Spark from_xml (explicit RSS or Atom schema)
  -> posexplode entries (zero-based document ordinal)
  -> Spark SQL field mapping, UTC timestamps, identity, validation
     -> accepted article observations -> local Parquet -> Spark SQL
     -> rejected observations/documents -> local Parquet
```

The [implementation](../../processing/silver.py) is isolated from Maven. Standard-library Expat validates document completeness and the expected root, and forbids DTD/entity declarations before Spark parses it. This prevents a row-oriented XML scan from overlooking a broken feed wrapper or trailing malformed input. It is a validation guard, not an article extractor or custom general-purpose XML parser. Native Spark `from_xml` performs field extraction with a corrupt-record field and permissive mode; a mapping failure rejects affected rows rather than silently dropping them. All per-article transformations use built-in Spark SQL functions, with no Python UDFs.

## Proposed schema version 1

Grain: **one occurrence of an article in one ingestion payload**. Schema definitions are explicit `StructType` constants: `SILVER_SCHEMA`, `REJECTION_SCHEMA`, `RSS`, and `ATOM`. No production schema inference is used. All physical fields are nullable to support rejected candidates and consistent Parquet read-back; requiredness below is enforced before acceptance. Spark/Parquet nullability is not a substitute for validation.

| Field | Spark type | Accepted-row rule / meaning |
| --- | --- | --- |
| `contract_version` | integer | Required; `1` |
| `article_id` | string | Required; lowercase SHA-256 described below |
| `source_id` | string | Required, nonblank; stable configured source, never feed title |
| `source_uri` | string | Required, nonblank; configured source URI, not article URL |
| `ingestion_id` | string | Required; canonical original ingestion UUID |
| `ingested_at` | timestamp | Required; timezone-aware envelope instant, UTC in Spark |
| `input_uri` | string | Required; exact local fixture URI in this prototype |
| `payload_sha256` | string | Required; lowercase SHA-256 of exact input bytes |
| `payload_byte_length` | long | Required; actual input byte count |
| `bronze_bucket` | string | Null for fixtures; required for future Bronze-backed inputs |
| `bronze_payload_key` | string | Null for fixtures; required for future Bronze-backed inputs |
| `bronze_metadata_key` | string | Null for fixtures; required for future Bronze-backed inputs |
| `feed_format` | string | Required; `rss` or `atom` |
| `entry_index` | integer | Required; zero-based ordinal within payload |
| `identifier_kind` | string | Required; `rss_guid`, `rss_link`, `atom_id`, or `atom_link` |
| `source_article_id` | string | Required; trimmed publisher identifier or fallback article link |
| `title` | string | Required; nonblank after trimming |
| `url` | string | Optional; trimmed link, never fetched or trusted |
| `summary` | string | Optional; description/summary content as parsed, not sanitized |
| `published_at` | timestamp | Optional; valid supplied publication timestamp |
| `updated_at` | timestamp | Optional; valid supplied Atom update timestamp |
| `quality_warnings` | array of string | Always populated; empty array or deterministic warning codes, publication before update |

`bronze_payload_key` corresponds to the initially proposed `payload_key` in Issue #9; the more explicit name is retained. This updates the still-Proposed version-1 prototype contract before acceptance; it is not a production schema migration. Existing prototype Parquet should be regenerated into a fresh directory to obtain the added warning field.

Rejections carry the same nullable candidate/provenance columns plus `rejection_reasons: array<string>`. Warnings and rejection reasons are distinct: warnings alone never reject an observation. Rejected candidates can carry warnings as well as required-field rejection reasons. Whole-document rejections have null article fields and ordinal and an empty warning array. Record rejections retain their candidate values and ordinal. Original unsupported date strings and exact XML remain recoverable from the referenced immutable input; they are not duplicated into the output schema. No raw document content is printed in diagnostics.

## Field mappings and dates

| Silver field | RSS `channel/item` | Atom `feed/entry` |
| --- | --- | --- |
| `source_article_id` | `guid`; otherwise nonblank `link` | `id`; otherwise the selected alternate article link |
| `identifier_kind` | `rss_guid` or `rss_link` | `atom_id` or `atom_link` |
| `title` | `title` | `title` |
| `url` | `link` | First `link/@href` with absent `rel` or `rel="alternate"` |
| `summary` | `description` | Nonblank `summary`, otherwise supported nonblank `content` |
| `published_at` | `pubDate` | `published` |
| `updated_at` | Null | `updated` |

RSS `guid/@isPermaLink` does not change identity or imply a usable URL. XML entity escapes are decoded. Identity and title/link boundary whitespace is trimmed; blank optional links/dates become null. Summary text is retained without HTML sanitization. Atom content fallback supports inline text (absent `type` or `type="text"`) and escaped HTML (`type="html"`), preserving the parsed value as untrusted text. Blank summary allows fallback; a nonblank summary takes precedence. Content with `src`, rich XHTML and other content types are not mapped or fetched; absent/unsupported content yields null. Category, author, feed-level defaults and other extensions are outside this initial schema. Unknown extension fields are not projected; their original bytes remain in the input. Atom links preserve document-order selection, not canonical URL resolution.

RSS dates support an English `d MMM yyyy HH:mm:ss` timestamp with a numeric `+HHMM`/`-HHMM` offset or `GMT`, optionally preceded by an English weekday and comma. The weekday is informational and removed before parsing. Other named zones are unsupported. Atom dates require ISO-8601 `yyyy-MM-dd'T'HH:mm:ss[.fraction]` followed by `Z` or `+HH:MM`/`-HH:MM`. Up to nine fractional digits are parsed; Spark timestamps retain microsecond precision. Timezone-free timestamps become null with a warning, never interpreted in the machine timezone. Spark session timezone is explicitly UTC; run Python tests with `TZ=UTC` for consistent collected Python datetime display.

Missing dates remain null; no ingestion-time substitution or `updated`-to-`published` substitution is performed. Nonblank invalid/unsupported publication or update dates become null and add `INVALID_PUBLISHED_AT` or `INVALID_UPDATED_AT` to `quality_warnings`. Missing dates produce no warning. Warning codes contain no raw source text and are emitted in publication-then-update order. Otherwise-valid articles remain accepted. Future-dated and late-arriving records are retained; there is no watermark, chronology gate, latest-version selection or event-time ordering guarantee. SQL queries must request an explicit order if needed.

## Article and ingestion identity

`article_id` is SHA-256 over the UTF-8 bytes of Spark's compact JSON array:

```text
["v1", source_id, identifier_kind, source_article_id]
```

Array framing avoids delimiter ambiguity. The version and identifier kind avoid namespace collisions. IDs are case-sensitive; no URL canonicalization, Unicode normalization, title hashing or cross-publisher deduplication occurs. A publisher changing its identifier (or adding a native identifier after previously supplying only a link) produces a new article ID. Atom URL fallback deliberately tolerates incomplete/nonconformant entries lacking an Atom ID. A suitable link here means the first nonblank href with absent rel or rel="alternate"; URI resolution and URL canonicalization remain out of scope. SHA-256 collision risk is accepted for the proposed prototype contract, not a mathematical uniqueness guarantee.

`ingestion_id` identifies a retrieval, not an article. It is copied unchanged from the envelope. The demo uses fixed **synthetic** UUIDs and timestamps rather than pretending these fixtures were fetched or stored in Bronze. Different ingestion UUIDs for the same article retain the same article ID and produce separate observations. Duplicate occurrences within one payload are also retained, distinguished by `entry_index`.

The proposed replay key is `(source_id, ingestion_id, entry_index, contract_version)`, under the Bronze immutable-input precondition. Reprocessing the same envelope and bytes yields identical records. There is no append deduplication, upsert, transaction or exactly-once sink. The demo refuses an existing output directory, so reruns cannot silently append duplicates or overwrite a prior dataset. An interrupted run may leave a partial directory; inspect it and choose a fresh output path. Future replay publication needs its own durable design.

## Quality and untrusted input

| Condition | Outcome |
| --- | --- |
| Missing/blank native identifier and suitable fallback link | `MISSING_IDENTIFIER` rejection |
| Missing/blank title | `MISSING_TITLE` rejection |
| Unsupported/invalid nonblank optional date | Null timestamp plus `INVALID_PUBLISHED_AT` / `INVALID_UPDATED_AT` warning; retain otherwise-valid record |
| Native XML mapping corruption | `XML_MAPPING_ERROR` rejection |
| No supported entry collection, including an empty feed | `NO_SUPPORTED_ENTRIES` diagnostic rejection; may also include missing identifier/title |
| Broken document, including wrapper/trailing errors or empty bytes | One `MALFORMED_XML` document rejection; no partial acceptance |
| Wrong RSS/Atom root | One `UNSUPPORTED_ROOT` document rejection |
| DTD or entity declaration | One `DTD_FORBIDDEN` document rejection before expansion |
| Invalid UTF-8 or declared non-UTF-8 encoding | One `UNSUPPORTED_ENCODING` document rejection |
| File exceeds 5 MiB | Explicit `PAYLOAD_TOO_LARGE` exception before Spark output; no truncated checksum |
| Missing/unreadable file or invalid envelope | Explicit exception; abort run, do not invent provenance |

All applicable record reasons are retained. Schema/version/source/envelope failures are caller errors, not fabricated article rows. The prototype accepts UTF-8 RSS 2-style documents and Atom with the standard feed namespace; it is not a complete RSS/Atom standards validator. Expat checks root namespaces; Spark's child mapping ignores namespace prefixes. Arbitrary namespace collisions, repeated scalar fields, rich XHTML constructs, unusual mixed content, alternate encodings and broader publisher dialects require additional contract work before real feeds are processed.

All source text, HTML and URLs remain **untrusted data**. Nothing is executed, rendered, fetched, or followed. Escaped HTML/CDATA is preserved as text. Any future UI must escape or appropriately sanitize it; this prototype makes no safe-HTML claim. Input bytes are bounded and DTDs prohibited, but this is not a hardened parser service for hostile workloads.

## Runtime choice and reproducible execution

Pinned dependencies in [requirements.txt](../../processing/requirements.txt): PySpark **4.0.2**, Py4J **0.10.9.9**. PySpark supplies Spark and its native XML support; Py4J is its required JVM bridge. No Pandas, Arrow, XML connector package, Python testing framework or Maven Spark dependency is needed.

[Spark 4.2.0](https://spark.apache.org/docs/4.2.0/) supports Java 21 but requires Python 3.10+. The available Python is 3.9.6, so the preferred version is not compatible with this environment. [Spark 4.0.2](https://spark.apache.org/docs/4.0.2/) supports Python 3.9+ and Java 17/21 and was installed and executed with Java 21.0.7 here. Upgrading the machine Python merely for this milestone was avoided. Reassess the pinned Spark version when upgrading Python; this is a local compatibility choice, not a production runtime commitment. Native [Spark XML functionality](https://spark.apache.org/docs/4.0.2/sql-data-sources-xml.html) avoids a third-party XML connector. A pure Python transformation would be lighter for six entries but would not validate the requested Spark/Parquet path.

Prefer Python **3.12.15** for ongoing development; Python 3.9 is end-of-life. Keep PySpark 4.0.2, Py4J 0.10.9.9 and JDK 21 unchanged. Create a fresh environment with an explicitly selected Python 3.12 interpreter; do not reuse a Python 3.9 venv or replace system Python. With Python 3.12.15 installed separately and JDK 21 selected (`JAVA_HOME`/`PATH`), from the repository root:

```sh
python3.12 -m venv outputs/silver-python312/.venv
outputs/silver-python312/.venv/bin/python -m pip install -r processing/requirements.txt
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python" PYTHONPATH=processing \
  outputs/silver-python312/.venv/bin/python -m unittest discover -s processing/tests -v
TZ=UTC SPARK_LOCAL_IP=127.0.0.1 PYSPARK_PYTHON="$PWD/outputs/silver-python312/.venv/bin/python" \
  outputs/silver-python312/.venv/bin/python processing/silver.py --output outputs/silver-prototype-corrected
```

Only dependency bootstrap requires package-index access. Tests and execution are offline after installation. Spark uses `local[2]`, two local worker threads in one JVM; its gateway/driver bind loopback and its web UI is disabled. A sandbox must permit local sockets. No Spark cluster or Docker service is created. The repository's existing `.venv/`, `/outputs/`, `/artifacts/` and Spark/cache ignore rules cover generated files.

Default outputs are `outputs/silver-prototype/articles/` and `outputs/silver-prototype/rejections/`. For another run use `--output outputs/silver-prototype-run2`. Keep alternate output paths under ignored `outputs/`. The demo writes both datasets, reads articles using `SILVER_SCHEMA`, calls `printSchema()` and `count()`, registers `article_observations`, then runs:

```sql
SELECT source_id, count(*) AS observations
FROM article_observations
GROUP BY source_id
ORDER BY source_id;
```

Expected: **4 accepted records and 2 rejected records**, with `atom-science=2`, `rss-business=1`, `rss-technology=1`. The second item in each RSS fixture lacks both GUID and link; those fixtures are reused unchanged. Tests additionally read back rejection Parquet and compare rows. To tear down, stop the process and remove only the generated virtual environment/output directories when no longer wanted; no persistent service or Bronze data needs teardown.

## Bronze provenance and future SeaweedFS integration

A future input adapter must require both metadata and the referenced payload under the [accepted Bronze contract](bronze-storage.md). It must validate metadata `schemaVersion=1`, source identity, canonical UUID, ingestion instant, successful HTTP status policy, payload key, actual byte length and SHA-256 before parsing. Carry `sourceId`, `sourceUri`, `ingestionId`, `ingestedAt`, receipt bucket and payload/metadata keys unchanged. Preserve the original metadata object for HTTP status, content type and declared-length audit; these are not article attributes. Do not trust content type as XML validation or generate a new UUID during processing.

Fixture paths and computed checksums are real local provenance, but null Bronze fields explicitly mean **not stored in Bronze**. The current module has no metadata reader, S3 client, SeaweedFS connection or Bronze mutation path. Future integration needs authenticated local object reads, verified publication completeness, rejection storage, replay coordination, schema evolution, partitioning, retention and crash recovery. Parquet does not supply these guarantees. Table formats, distributed execution and streaming are deferred; no performance/scalability claims are made from this tiny workload.

## Original validation evidence (before contract corrections)

Validated on macOS ARM64, Python 3.9.6, Java 21.0.7, PySpark 4.0.2 in `local[2]` mode. The test suite covers RSS/Atom mappings, optional fields, required identifiers/titles, offset timestamps, invalid timestamps, deterministic source-scoped IDs, distinct retrieval UUIDs, repeated observations, malformed documents, DTD rejection, invalid provenance, explicit schemas, SQL and Parquet read-back.

Initial dependency bootstrap failed inside the network-restricted sandbox; the authorized package download succeeded. Initial Spark and Java test attempts failed because the sandbox denied loopback socket binding; reruns with loopback permission passed. PySpark emitted Python socket `ResourceWarning` messages during collection; assertions passed. No live publishers or SeaweedFS integration checks were run.

| Check | Result |
| --- | --- |
| PySpark unittest suite | PASSED: 8 tests, including subcases |
| Existing offline Maven `verify` | PASSED: 29 tests, zero failures/errors/skips |
| Batch launch script regressions | PASSED: all 5 argument cases |
| Demo Parquet read-back, schema inspection and Spark SQL | PASSED: 4 accepted; 2 rejected; source counts 2/1/1 |
| Markdown lint and repository hygiene (`npm run check`) | PASSED |
| `git diff --check` and generated-output ignore verification | PASSED |
| Initial sandbox bootstrap / socket-dependent test attempts | FAILED due to sandbox restrictions; authorized reruns passed |
| Spark 4.2.0 execution | NOT EXECUTED: installed Python 3.9.6 is below its minimum |
| Live publishers, SeaweedFS Silver, AWS, distributed execution | NOT EXECUTED; outside this milestone |

## Contract correction acceptance and Python setup

Acceptance criteria for this focused correction: optional invalid dates remain accepted with deterministic warnings; required-field failures still reject; Atom native IDs take precedence over alternate-link identity; supported content fills only missing/blank summary; unchanged identities and repeated observations remain stable; warnings survive explicit-schema Parquet read-back and Spark SQL. ADR 0003 stays Proposed.

For a non-system Python installation, the validation used an isolated `uv` provisioning tool and standalone Python under ignored `outputs/`. No runtime dependency was added to requirements.txt. With `uv` available, the reproducible provisioning equivalent is:

```sh
UV_PYTHON_INSTALL_DIR="$PWD/outputs/python-runtimes" \
  uv python install 3.12.15 --no-bin
UV_PYTHON_INSTALL_DIR="$PWD/outputs/python-runtimes" \
  uv venv --python 3.12.15 outputs/silver-python312/.venv
uv pip install --python outputs/silver-python312/.venv/bin/python \
  -r processing/requirements.txt
```

The original Python 3.9.6 virtual environment remains functional. Python 3.12 makes newer Spark versions eligible for a separate evaluation; this correction intentionally keeps Spark 4.0.2 pinned. No system interpreter or Java dependencies are changed.

## Corrected validation results

Locally verified with Python **3.12.15**, PySpark **4.0.2**, Py4J **0.10.9.9**, Java **21.0.7**, macOS ARM64, Spark `local[2]`. The Python 3.12 interpreter is a standalone installation under ignored outputs; system Python was not replaced. Tests remain offline after environment bootstrap.

| Check | Result |
| --- | --- |
| Updated PySpark suite on Python 3.12.15 | PASSED: 11 tests |
| Updated PySpark suite on existing Python 3.9.6 | PASSED: 11 tests |
| Existing Java offline Maven verify | PASSED: 29 tests, zero failures/errors/skips |
| Shell launcher regressions | PASSED: 5 cases |
| Corrected fixture demo and explicit-schema Parquet/SQL | PASSED: 4 accepted, 2 rejected; source counts 2/1/1 |
| Warning edge cases and Parquet/SQL | PASSED: two accepted (one with both warnings), one rejected for missing title with a separate publication warning |
| Markdown/hygiene and git diff --check | PASSED after removing one extra blank line detected by the initial lint run |
| SeaweedFS, real publisher feeds, AWS, Spark upgrade | NOT EXECUTED; outside scope |

The original three fixtures are unchanged and contain no invalid timestamps, so their accepted/rejected counts remain unchanged. New edge cases are fictional inline XML in tests. The fresh dataset is `outputs/silver-prototype-corrected/`; earlier prototype outputs were preserved. No remaining known discrepancy from the requested corrections was observed. Rich XHTML, namespace collisions, URL resolution and transactional replay remain documented prototype limitations.
