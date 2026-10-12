package platform.ingestion.bronze;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import platform.ingestion.rss.RawFeed;
import platform.ingestion.rss.RssSource;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;
import static org.junit.jupiter.api.Assertions.*;

/** Opt-in Failsafe test. Does not delete objects or buckets; each run has an isolated identity. */
class SeaweedFsIT {
    @Test void persistsAndReadsBackLocalBronze() throws Exception {
        var environment = System.getenv();
        var local = new LocalS3(environment); // Fails closed; never uses ambient AWS configuration.
        boolean readOnly = Boolean.parseBoolean(environment.getOrDefault("BRONZE_IT_READ_ONLY", "false"));
        String configuredId = environment.get("BRONZE_IT_ID");
        if (readOnly && (configuredId == null || configuredId.isBlank())) {
            throw new IllegalArgumentException("Restart verification requires BRONZE_IT_ID");
        }
        UUID id = configuredId == null || configuredId.isBlank() ? UUID.randomUUID() : UUID.fromString(configuredId);
        var source = new RssSource("integration-" + id, URI.create("https://example.test/synthetic-feed"), true);
        byte[] payload = "<?xml version=\"1.0\"?><rss><channel><title>café</title></channel></rss>\r\n"
                .getBytes(StandardCharsets.UTF_8);
        var feed = new RawFeed(id, source.sourceId(), Instant.parse("2026-10-12T00:00:00Z"), 200,
                Optional.of("application/rss+xml"), OptionalLong.empty(), payload);
        var keys = BronzeKeys.forFeed(feed);
        try (var client = local.client()) {
            client.headBucket(HeadBucketRequest.builder().bucket(local.bucket()).build());
            if (!readOnly) {
                var storage = new S3BronzeStorage(client, local.bucket());
                var receipt = storage.write(source, feed);
                assertEquals(receipt, storage.write(source, feed));
                assertEquals(keys.payload(), receipt.payloadKey());
                assertEquals(keys.metadata(), receipt.metadataKey());
            }
            byte[] stored = client.getObjectAsBytes(GetObjectRequest.builder()
                    .bucket(local.bucket()).key(keys.payload()).build()).asByteArray();
            assertArrayEquals(payload, stored);
            byte[] metadata = client.getObjectAsBytes(GetObjectRequest.builder()
                    .bucket(local.bucket()).key(keys.metadata()).build()).asByteArray();
            var json = new ObjectMapper().readTree(metadata);
            assertEquals(1, json.get("schemaVersion").asInt());
            assertEquals(id.toString(), json.get("ingestionId").asText());
            assertEquals(source.sourceId(), json.get("sourceId").asText());
            assertEquals(source.feedUri().toString(), json.get("sourceUri").asText());
            assertEquals(feed.ingestedAt().toString(), json.get("ingestedAt").asText());
            assertEquals(200, json.get("httpStatus").asInt());
            assertEquals("application/rss+xml", json.get("contentType").asText());
            assertTrue(json.get("declaredContentLength").isNull());
            assertEquals(payload.length, json.get("payloadByteLength").asInt());
            assertEquals(S3BronzeStorage.sha256(stored), json.get("payloadSha256").asText());
            assertEquals(keys.payload(), json.get("payloadKey").asText());
            System.out.println("SeaweedFS verified ingestion " + id + " (readOnly=" + readOnly + ")");
        }
    }
}
