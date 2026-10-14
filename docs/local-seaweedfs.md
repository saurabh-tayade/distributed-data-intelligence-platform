# Local SeaweedFS development

## Prerequisites and verified configuration

Use JDK 21, Maven 3.9.16 (or compatible Maven 3.9+), and Docker Engine/Desktop with Docker Compose v2. Java dependencies and Maven plugins are pinned in [ingestion/pom.xml](../ingestion/pom.xml). No AWS account is required. The AWS CLI below is optional for inspection; it is not an application dependency.

The [Compose file](../infrastructure/local/compose.yaml) pins `chrislusf/seaweedfs:4.48` and manifest digest `sha256:4e61d15fd35994cb1e43e1e553dff106794841fd9a99ade2fc8c8bfce4d7872d`. The tag/digest were verified from the publisher's Docker Hub registry metadata on 2026-10-12; release and flags were checked against the [official 4.48 release](https://github.com/seaweedfs/seaweedfs/releases/tag/4.48) and [versioned mini source](https://github.com/seaweedfs/seaweedfs/blob/4.48/weed/command/mini.go). The [official quick start](https://github.com/seaweedfs/seaweedfs/wiki/Quick-Start-with-weed-mini) documents environment credential seeding and bucket initialization. This verifies configuration provenance, not runtime compatibility.

One `weed mini` process runs the necessary master/volume/filer/S3 components. Iceberg and Lance ports are disabled, as are WebDAV and the admin UI. Only the S3 port is published, at `127.0.0.1:8333`. No additional services or cluster are deployed. Internal components remain on the container network. A named volume stores `/data`. Missing credentials cause Compose interpolation to fail rather than starting in unauthenticated mode. Automatic bucket creation on application upload and recursive nonempty-bucket deletion are disabled.

## Start and access the bucket

From the repository root:

```sh
cp .env.example .env
chmod 600 .env
```

Edit `.env` to set `BRONZE_S3_ACCESS_KEY` and `BRONZE_S3_SECRET_KEY` to locally generated, nonblank credentials. For example, generate values with `openssl rand -hex 16` and `openssl rand -hex 32`; paste them into the ignored file. Do not reuse AWS credentials. Values must be shell-safe if sourcing the file below. Never commit `.env`, render resolved Compose configuration into Git, or share credential-bearing output.

```sh
docker compose --env-file .env -f infrastructure/local/compose.yaml up -d
# Wait for startup; check diagnostics if the integration test cannot connect.
docker compose --env-file .env -f infrastructure/local/compose.yaml ps
docker compose --env-file .env -f infrastructure/local/compose.yaml logs --tail=40 seaweedfs
```

`S3_BUCKET` initializes `content-lake` during explicit local deployment; the Java adapter never provisions buckets. The named volume retains initialized state across restarts. Changing credential environment values on an existing volume should not be assumed to rotate persisted credentials; validate that separately without deleting user data.

Export configuration for Java (Compose's env-file option does not export variables to your shell):

```sh
set -a
. ./.env
set +a
mvn -B -ntp -f ingestion/pom.xml verify
mvn -B -ntp -f ingestion/pom.xml -Pseaweedfs-it verify
```

The integration test first checks bucket access, then writes and reads synthetic feed objects under an isolated UUID/source prefix. Missing configuration or a non-loopback endpoint fails before an SDK connection is opened. Default unit tests ignore `.env` and use synthetic local settings. Dependencies need an initial download; tests themselves do not use external services.

For optional AWS CLI endpoint/bucket verification and object inspection, this function explicitly targets the local endpoint with local credentials:

```sh
local_s3() {
  AWS_ACCESS_KEY_ID="$BRONZE_S3_ACCESS_KEY" \
  AWS_SECRET_ACCESS_KEY="$BRONZE_S3_SECRET_KEY" \
  AWS_SESSION_TOKEN= AWS_EC2_METADATA_DISABLED=true \
    aws --endpoint-url http://127.0.0.1:8333 --region "$BRONZE_S3_REGION" "$@"
}
local_s3 s3api head-bucket --bucket "$BRONZE_S3_BUCKET"
local_s3 s3api list-objects-v2 --bucket "$BRONZE_S3_BUCKET" --prefix bronze/rss/
# Substitute keys from the listing; downloaded data stays in ignored outputs/.
mkdir -p outputs
local_s3 s3api get-object --bucket "$BRONZE_S3_BUCKET" --key '<metadata-key>' outputs/metadata.json
local_s3 s3api get-object --bucket "$BRONZE_S3_BUCKET" --key '<payload-key>' outputs/payload.xml
```

If explicitly using another **local** bucket, initialize it through deployment configuration or `local_s3 s3api create-bucket --bucket '<local-bucket>'`. Do not change the helper to point at AWS during this milestone.

## Restart persistence check

Choose a fresh run ID; keep it for the read-only phase. A fixed synthetic ingestion timestamp makes the test keys reproducible.

```sh
export BRONZE_IT_ID="$(uuidgen | tr '[:upper:]' '[:lower:]')"
unset BRONZE_IT_READ_ONLY
mvn -B -ntp -f ingestion/pom.xml -Pseaweedfs-it verify
docker compose --env-file .env -f infrastructure/local/compose.yaml restart seaweedfs
# Wait until the local bucket is accessible again; head-bucket above can verify readiness.
BRONZE_IT_READ_ONLY=true mvn -B -ntp -f ingestion/pom.xml -Pseaweedfs-it verify
```

The second invocation performs no uploads, so a pass establishes read-back across that restart instead of silently recreating objects. Tests retain their two objects and print the ingestion ID. They do not delete unrelated data or automatically clean up orphan payloads.

## Stop, resume, and optional cleanup

```sh
docker compose --env-file .env -f infrastructure/local/compose.yaml stop
# Resume the same container and volume:
docker compose --env-file .env -f infrastructure/local/compose.yaml start
# Or remove containers/network while retaining named-volume data:
docker compose --env-file .env -f infrastructure/local/compose.yaml down
# Recreate containers against the retained volume:
docker compose --env-file .env -f infrastructure/local/compose.yaml up -d
```

`docker compose down` retains the named volume. **`docker compose down -v` deletes the named volume and can destroy all local Bronze data and credentials stored there.** It is not part of setup, tests, or normal shutdown. Only the owner should deliberately run `docker compose --env-file .env -f infrastructure/local/compose.yaml down -v` after confirming that all data is disposable. No volume removal was performed during implementation.

For selective cleanup, inspect the test keys first and remove only the two objects belonging to a known test ingestion ID using your local S3 client. No broad prefix/bucket deletion is provided.

## Execution status and portability

Local integration validation **PASSED on 2026-10-12** on macOS ARM64 using Docker Desktop 4.95.0, Docker Engine 29.8.2, Compose 5.6.0, Java 21, and Maven 3.9.16. The pinned SeaweedFS 4.48 image above was used without application changes.

- Compose configuration validation and container startup passed. Only `127.0.0.1:8333` was published; unauthenticated requests returned HTTP 403 and authenticated access to `content-lake` succeeded.
- `mvn -B -ntp -f ingestion/pom.xml -Pseaweedfs-it verify` passed: 23 unit tests and one integration test, with no failures, errors, or skips. The run used an explicit local Maven dependency cache.
- The integration test uploaded and retrieved `payload.xml` and `metadata.json`, verified exact payload bytes, all metadata fields and SHA-256, and retried identical input successfully.
- After `docker compose --env-file .env -f infrastructure/local/compose.yaml restart seaweedfs`, the same Maven command with `BRONZE_IT_READ_ONLY=true` passed all 23 unit tests and the integration test again. This phase performed no uploads: both original objects remained accessible with matching bytes and metadata.
- Both phases used ingestion ID `d1c45ec7-d52f-407b-bd92-3c50bc7dfb11`. The named volume and test objects were retained; no data was deleted. Local credentials were generated in ignored `.env` with permissions `0600` and were not committed.

The existing SLF4J no-provider warning did not affect test results. These checks validate the exercised operations on this local release and configuration, not every S3 operation, crash recovery, or production durability. **Amazon S3 compatibility has not been tested.**

Single-node storage has no high availability or off-host backup. Local HTTP/static credentials are development settings. Optional SDK checksums and AWS chunked encoding are disabled for this local client; the metadata SHA-256 is the application integrity check. Amazon S3 requires separately approved configuration and compatibility validation, not SeaweedFS-specific application code. See [Bronze architecture](architecture/bronze-storage.md) for publication and failure semantics.

## Day 4 batch workflow

The [batch ingestion guide](architecture/batch-ingestion.md) documents the executable CLI, public catalog, separate opt-in live profile, object inspection and verified two-run results. It uses this same bucket and persistent volume; no additional container or data store is introduced.
