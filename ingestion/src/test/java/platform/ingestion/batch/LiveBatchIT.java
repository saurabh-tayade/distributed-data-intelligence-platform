package platform.ingestion.batch;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import platform.ingestion.bronze.LocalS3;
import platform.ingestion.bronze.S3BronzeStorage;
import platform.ingestion.rss.HttpFeedFetcher;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import static org.junit.jupiter.api.Assertions.*;

/** Explicit live-feeds profile only. Two observations; read-back checks; no deletion or payload logging. */
class LiveBatchIT {
    @Test void twoLiveBatchesPreserveBytesMetadataAndDistinctIdentities() throws Exception {
        var sources = SourceCatalog.load(Path.of(System.getenv().getOrDefault("RSS_SOURCE_CATALOG", "../config/rss-sources.json")));
        var local = new LocalS3(System.getenv());
        var previous = new HashMap<String, UUID>();
        long failures = 0;
        long successes = 0;
        try (var s3 = local.client(); var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NEVER).build()) {
            s3.headBucket(HeadBucketRequest.builder().bucket(local.bucket()).build());
            System.out.println("Local SeaweedFS bucket connectivity and authenticated access verified");
            var storage = new S3BronzeStorage(s3, local.bucket());
            var fetcher = new HttpFeedFetcher(http, Duration.ofSeconds(30), Clock.systemUTC());
            var runner = new BatchIngestionRunner(fetcher, (source, raw) -> {
                var receipt = storage.write(source, raw);
                byte[] payload = s3.getObjectAsBytes(GetObjectRequest.builder().bucket(receipt.bucket())
                        .key(receipt.payloadKey()).build()).asByteArray();
                assertArrayEquals(raw.body(), payload);
                String sha;
                try {
                    sha = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(payload));
                } catch (java.security.NoSuchAlgorithmException impossible) {
                    throw new AssertionError(impossible);
                }
                com.fasterxml.jackson.databind.JsonNode metadata;
                try {
                    metadata = new ObjectMapper().readTree(s3.getObjectAsBytes(GetObjectRequest.builder()
                            .bucket(receipt.bucket()).key(receipt.metadataKey()).build()).asByteArray());
                } catch (java.io.IOException malformed) {
                    throw new AssertionError("Invalid stored metadata", malformed);
                }
                assertEquals(1, metadata.path("schemaVersion").asInt());
                assertEquals(raw.sourceId(), metadata.path("sourceId").asText());
                assertEquals(source.feedUri().toString(), metadata.path("sourceUri").asText());
                assertEquals(raw.ingestedAt().toString(), metadata.path("ingestedAt").asText());
                assertEquals(raw.ingestionId().toString(), metadata.path("ingestionId").asText());
                assertEquals(raw.httpStatus(), metadata.path("httpStatus").asInt());
                assertEquals(payload.length, metadata.path("payloadByteLength").asInt());
                assertEquals(receipt.payloadKey(), metadata.path("payloadKey").asText());
                assertEquals(sha, metadata.path("payloadSha256").asText());
                assertEquals(sha, receipt.payloadSha256());
                System.out.println("Verified payload + metadata + SHA-256: " + raw.sourceId() + " "
                        + raw.ingestionId() + " ingestedAt=" + raw.ingestedAt());
                return receipt;
            });
            for (int run = 1; run <= 2; run++) {
                System.out.println("Live batch " + run);
                var result = runner.run(sources);
                BatchIngestionCli.print(result, System.out);
                failures += result.failed(); successes += result.succeeded();
                for (var source : result.sources()) {
                    if (source.status() == SourceResult.Status.SUCCESS) {
                        assertNotEquals(previous.put(source.sourceId(), source.ingestionId()), source.ingestionId());
                    }
                }
            }
        }
        assertTrue(successes > 0, "No successful live publisher observations");
        assertEquals(0, failures, "Live sources failed; inspect per-source summaries (successful snapshots retained)");
    }
}
