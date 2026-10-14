# M1 Day 4: RSS/Atom batch ingestion

## Acceptance criteria and design

Validate the entire catalog before network activity; fetch enabled sources sequentially in catalog order; preserve raw bytes and retrieval identities through the existing storage boundary; isolate individual failures; report truthful outcomes and process exit codes. Default tests use synthetic fixtures and loopback servers, with no Docker or AWS credentials. Explicit live verification must read payloads and metadata back from local SeaweedFS and confirm SHA-256 and new identities on repeated retrieval.

```text
JSON catalog -> SourceCatalog -> List<RssSource>
                                      |
                              BatchIngestionRunner
                                      |
                          FeedFetcher -> RawFeed
                                      |
                           BronzeStorage -> SeaweedFS
                                      |
                           SourceResult / BatchResult
```

The runner accepts validated definitions and also validates direct callers before processing. Retrieval, storage, and orchestration remain separate. It does not parse XML, inspect article fields, provision buckets, schedule work, introduce concurrency, or retry failed operations. Existing production contracts and accepted ADRs 0001/0002 are unchanged. These are wiring choices within those decisions, so no new ADR is proposed.

Jackson already exists in the build and reads a strict JSON tree. A configuration framework or automatic record binding would add complexity or risk implicit defaults/coercions. A plain Java CLI avoids an application framework. One pinned Maven dependency plugin copies existing runtime dependencies into ignored `target/dependency/` for a normal `java` launch; it adds no application dependency. Sequential execution makes ordering and failure ownership clear at the cost of throughput; no scalability claim is made.

## Source catalog

[config/rss-sources.json](../../config/rss-sources.json) is an ordered JSON array:

```json
[
  {"sourceId": "nasa-technology", "feedUri": "https://www.nasa.gov/technology/feed/", "enabled": true}
]
```

All three fields are mandatory with exact string/string/boolean types. Unknown fields, duplicate JSON properties, trailing JSON values, duplicate IDs (including disabled sources), null values, and invalid URIs fail before client initialization. Empty arrays and all-disabled catalogs are valid. IDs match `[a-z0-9][a-z0-9_-]{0,127}` for safe console diagnostics. Catalog URIs must satisfy `RssSource` and have no query string; this deliberately excludes token-bearing URL configurations. This catalog policy narrows configuration without changing the existing domain contract. Credentials and private URLs must never be placed here.

| sourceId | feedUri | enabled | Expected format |
| --- | --- | --- | --- |
| nasa-technology | `https://www.nasa.gov/technology/feed/` | true | RSS |
| guardian-world | `https://www.theguardian.com/world/rss` | true | RSS |
| xkcd | `https://xkcd.com/atom.xml` | true | Atom |

These are public endpoints, not availability guarantees. No fallback silently replaces them. The CLI has a 10-second connection timeout, 30-second retrieval timeout, existing 5 MiB payload limit, and no automatic redirects. A redirect is reported as an HTTP failure. TLS verification and access controls remain intact. The JDK can internally retry connection failures; there are no application retries.

## Results and failure handling

Each immutable `SourceResult` contains `sourceId`, `status`, nullable `ingestionId`, nullable `payloadByteLength`, measured `durationMs`, nullable `failureClassification`, and nullable `BronzeReceipt`. Unavailable fields are null rather than invented. No raw feed is retained in the aggregate result. `BatchResult` copies its result list and derives success/failure/skipped counts. Durations use monotonic `System.nanoTime` and include fetch/storage work; the CLI batch duration excludes initialization.

| Status | Meaning and context |
| --- | --- |
| SUCCESS | Retrieval and storage returned successfully; original ingestion ID, actual length and complete receipt available |
| FETCH_FAILED | Retrieval failed; classification is fetch kind (with status for HTTP errors); completed error responses supply identity/length but are never stored |
| STORAGE_FAILED | Retrieval succeeded; original identity/length retained; classification includes storage kind and stage |
| SKIPPED | Source disabled; neither fetch nor storage called |

Unexpected runtime exceptions from either component become `UNEXPECTED` failures at that boundary, without exposing potentially sensitive exception messages. JVM errors are not swallowed. Checked interruption retains the fetcher's restored interrupt flag; subsequent enabled sources are still attempted and may fail immediately under that flag. There is no cancellation/resume protocol. Catalog errors abort the whole batch. Individual fetch/storage failures allow later sources to proceed.

Exit code **0** means all enabled sources succeeded (also empty/all-disabled catalogs after successful initialization). Exit **1** means one or more source failures. Exit **2** means invalid arguments/catalog or initialization/application failure. Initialization diagnostics identify the phase and exception type without printing URI values, payloads, SDK messages, or credentials. The CLI checks authenticated bucket access even for an empty/all-disabled catalog. Source results include object keys for inspection; they contain no payload content.

A storage failure can leave a payload without metadata or an uncertain remote outcome. Existing metadata-last publication is not a transaction. Preserve the original immutable `RawFeed` to retry storage with its UUID; the batch does not automatically retry or retain failed payloads for replay. See [Bronze storage](bronze-storage.md).

## Identity and data policy

Every successful HTTP retrieval creates a fresh UUID. Storage uses that UUID unchanged. Two runs are two observations even if payload checksums match. Retrying storage of the same immutable `RawFeed` is a different operation and preserves identity. No article deduplication or exactly-once ingestion is claimed.

GitHub contains source code, public configuration, small self-authored synthetic fixtures, tests, and documentation. SeaweedFS contains actual ingested publisher snapshots, raw payloads, and ingestion metadata in its existing persistent Docker volume. Publisher responses must never be copied into test resources or committed, regardless of public accessibility. Large generated workloads also stay outside Git. Existing ignore rules protect `.env`, credentials directories, local datasets, outputs, Docker volume paths, and Maven targets. Do not force-add them.

The three original fictional fixtures in [test resources](../../ingestion/src/test/resources/feeds/) are `rss-technology.xml`, `rss-business.xml`, and `atom-science.xml`. Each has two entries, reserved `example.com` URLs, Unicode, timestamps and categories; some optional fields are absent. These are deterministic byte fixtures, not licensed publisher snapshots. No fixture generator or malformed fixture is needed for today's raw-byte tests.

Bronze preserves raw source payload bytes, including any unexpected non-XML successful response. HTTP success does not establish XML validity. RSS/Atom article parsing, schema validation, normalization, and invalid-record handling belong to future Silver processing. The runner downloads no linked articles or images.

## Publisher usage

Reviewed on 2026-10-13 for this personal, non-commercial local demonstration:

- NASA publishes [official RSS feeds](https://www.nasa.gov/rss-feeds/). Its [media guidelines](https://www.nasa.gov/nasa-brand-center/images-and-media/) distinguish NASA material from third-party rights and protect branding; public access is not a blanket redistribution license.
- Guardian [RSS guidance](https://www.theguardian.com/help/feeds) permits personal non-commercial use subject to its [terms](https://www.theguardian.com/help/terms-of-service). Keep snapshots private; do not bypass access restrictions or repurpose this workflow for redistribution or model training.
- xkcd's [license](https://xkcd.com/license.html) is CC BY-NC 2.5, requiring attribution and non-commercial use. This workflow retains source provenance and does not publish the content.

Recheck terms before materially changing use. The live check makes two one-shot observations to test identity, not a polling service. Do not run it automatically in CI or repeatedly for routine development. If blocked, record status/transport diagnosis and stop rather than disguise requests or bypass controls.

## Run and inspect

Follow [local SeaweedFS setup](../local-seaweedfs.md) first. Use JDK 21 and Maven 3.9+. From the repository root, export the existing local-only configuration and run:

```sh
set -a
. ./.env
set +a
bash scripts/run-batch.sh config/rss-sources.json
```

The script accepts zero or one argument. No argument selects `config/rss-sources.json`; one argument is used unchanged and must contain a non-whitespace character. Empty, whitespace-only, or multiple arguments print usage to stderr and exit 2 before Maven or ingestion starts. Run `bash scripts/test-run-batch.sh` for five deterministic argument regression cases using stubbed Maven and Java commands; CI also runs this check without ingestion.

The script packages the module, then replaces itself with the Java CLI so its exit code is preserved. After packaging, the equivalent command is:

```sh
java -cp 'ingestion/target/classes:ingestion/target/dependency/*' \
  platform.ingestion.batch.BatchIngestionCli config/rss-sources.json
```

`LocalS3` requires an explicit HTTP loopback endpoint, local credentials, region and bucket. No ambient AWS credential lookup or AWS endpoint is allowed. The CLI owns and closes both clients, and never creates a bucket. It does not automatically read `.env`.

Default verification and two independent opt-in workflows:

```sh
mvn -B -ntp -f ingestion/pom.xml verify
# Synthetic SeaweedFS storage verification only; no publishers:
mvn -B -ntp -f ingestion/pom.xml -Pseaweedfs-it verify
# Exactly two live batches plus object read-back; requires exported local settings:
mvn -B -ntp -f ingestion/pom.xml -Plive-feeds verify
```

For the live test, `RSS_SOURCE_CATALOG` optionally selects an absolute catalog path; the default is `../config/rss-sources.json` relative to the Maven module. Profiles have explicit test includes so the synthetic profile never runs live retrieval. The live test fails if a source fails or no successful observations occur; successful objects remain available. It verifies byte equality, metadata source/URI/UUID/timestamp/status/length/key, SHA-256, and distinct repeated-success identities. Assertion failures are never converted into successful results.

Receipts print the exact `payload.xml` and `metadata.json` keys. Use the loopback-only `local_s3` helper in [local setup](../local-seaweedfs.md#start-and-access-the-bucket) to inspect them. Read-back is also built into `LiveBatchIT`, which needs no AWS CLI or filesystem payload copies. Verify payload length and SHA-256 against metadata and require both objects before consumption. Never delete previous snapshots or remove the named volume as part of this workflow.

## Validation evidence: 2026-10-13

Local environment: macOS ARM64, Java 21.0.7, IntelliJ bundled Maven 3.9.16, existing SeaweedFS 4.48 container at loopback port 8333. Dependency cache was explicitly placed under ignored `ingestion/target/m2`. No AWS access was used. Source fetches below returned successful 2xx responses; the runner did not validate their XML structure.

The synthetic SeaweedFS profile passed authenticated bucket access, exact payload/metadata read-back, checksum and identical-write retry for existing test identity `d1c45ec7-d52f-407b-bd92-3c50bc7dfb11`. Existing objects were retained. Container restart was not repeated on Day 4; Day 3 restart evidence remains separate.

Both live batches passed, each with 3 successes, 0 failures and 0 skipped sources. All six payload/metadata pairs were read back and verified. First batch measured 1005 ms; second measured 233 ms, including verification in the test's storage wrapper. These are single local observations, not throughput benchmarks or production performance claims.

| Source | Run 1 UUID / UTC ingestion time | Run 2 UUID / UTC ingestion time | Bytes each run |
| --- | --- | --- | --- |
| nasa-technology | `f11cfda7-3047-468e-9255-b0d632b5d5ce` / `2026-10-13T23:54:12.294026Z` | `1251f3be-2d17-4147-b09d-dd3c96c795de` / `2026-10-13T23:54:12.965161Z` | 463456 |
| guardian-world | `9574943a-da7c-441f-8e1b-b94dfd348868` / `2026-10-13T23:54:12.595541Z` | `0a171a1a-6149-4693-b5f2-c15282dd6b21` / `2026-10-13T23:54:13.051439Z` | 154008 |
| xkcd | `78e1a5a5-1734-4f8d-b342-37fbc2ae645c` / `2026-10-13T23:54:12.846422Z` | `bf801dba-84d7-43bf-8314-1d3d969c5288` / `2026-10-13T23:54:13.098211Z` | 2522 |

xkcd had identical SHA-256 `44874e4d09dfa11af92bacb82d178458577dc6519844cd720a1a2417daf42039` across distinct UUIDs. NASA and Guardian checksums differed between observations despite matching lengths. Both versions remain in Bronze. No content-level interpretation was attempted.

The live profile and synthetic storage profile passed separately. Final offline Maven verification passed 29 JUnit tests (including the original Day 2 regression wrapper), with zero failures, errors or skips. CLI subprocess tests verified exits 0/1/2, disabled behavior, repeated storage identities, exact Atom bytes, failed fetch/write handling and rejection of an AWS endpoint. `npm run check` and `git diff --check` passed. The initial sandboxed dependency bootstrap failed DNS resolution; the authorized download and subsequent offline build passed. No failed publisher or storage outcome was hidden. AWS compatibility, crash recovery, distributed processing, scheduling, retention, and scalability are not tested or implemented here.
