package platform.ingestion.bronze;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import platform.ingestion.rss.RawFeed;
import platform.ingestion.rss.RssSource;
import software.amazon.awssdk.awscore.exception.AwsErrorDetails;
import software.amazon.awssdk.core.ResponseInputStream;
import software.amazon.awssdk.core.exception.SdkClientException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.http.AbortableInputStream;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.GetObjectResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static org.junit.jupiter.api.Assertions.*;

class S3BronzeStorageTest {
    static final UUID ID = UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee");
    static final Instant TIME = Instant.parse("2026-10-12T00:15:00Z");
    static final RssSource SOURCE = new RssSource("publisher", URI.create("https://example.test/feed"), true);
    static final byte[] PAYLOAD = "<?xml version=\"1.0\"?><rss>café\r\n</rss>".getBytes(StandardCharsets.UTF_8);

    static RawFeed feed(byte[] payload) {
        return new RawFeed(ID, SOURCE.sourceId(), TIME, 200, Optional.of("application/rss+xml"),
                OptionalLong.of(999), payload);
    }

    @Test void writesPayloadBeforeMetadataAndReturnsReceipt() throws Exception {
        var fake = new Fake();
        var receipt = new S3BronzeStorage(fake.client(), "content-lake").write(SOURCE, feed(PAYLOAD));
        assertEquals(List.of(receipt.payloadKey(), receipt.metadataKey()), fake.puts);
        assertArrayEquals(PAYLOAD, fake.objects.get(receipt.payloadKey()));
        assertEquals("content-lake", receipt.bucket());
        assertEquals(ID, receipt.ingestionId());
        assertEquals(PAYLOAD.length, receipt.payloadByteLength());
        assertEquals(S3BronzeStorage.sha256(PAYLOAD), receipt.payloadSha256());
        assertEquals("application/octet-stream", fake.contentTypes.get(receipt.payloadKey()));
        assertEquals("application/json", fake.contentTypes.get(receipt.metadataKey()));
        var json = new ObjectMapper().readTree(fake.objects.get(receipt.metadataKey()));
        assertEquals(11, json.size());
        assertEquals(1, json.get("schemaVersion").asInt());
        assertEquals(ID.toString(), json.get("ingestionId").asText());
        assertEquals("publisher", json.get("sourceId").asText());
        assertEquals(SOURCE.feedUri().toString(), json.get("sourceUri").asText());
        assertEquals(TIME.toString(), json.get("ingestedAt").asText());
        assertEquals(200, json.get("httpStatus").asInt());
        assertEquals("application/rss+xml", json.get("contentType").asText());
        assertEquals(999, json.get("declaredContentLength").asLong());
        assertEquals(PAYLOAD.length, json.get("payloadByteLength").asLong());
        assertEquals(receipt.payloadSha256(), json.get("payloadSha256").asText());
        assertEquals(receipt.payloadKey(), json.get("payloadKey").asText());
    }

    @Test void missingHeadersAndEmptyPayloadArePreserved() throws Exception {
        var fake = new Fake();
        var raw = new RawFeed(ID, SOURCE.sourceId(), TIME, 204, Optional.empty(), OptionalLong.empty(), new byte[0]);
        var receipt = new S3BronzeStorage(fake.client(), "content-lake").write(SOURCE, raw);
        var json = new ObjectMapper().readTree(fake.objects.get(receipt.metadataKey()));
        assertTrue(json.get("contentType").isNull());
        assertTrue(json.get("declaredContentLength").isNull());
        assertEquals(0, json.get("payloadByteLength").asInt());
        assertEquals(0, fake.objects.get(receipt.payloadKey()).length);
        assertEquals("e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855", receipt.payloadSha256());
    }

    @Test void arbitraryBinaryBytesRemainUnchanged() throws Exception {
        byte[] bytes = new byte[] {0, (byte) 0xff, (byte) 0xfe, 13, 10, 32};
        var fake = new Fake();
        var receipt = new S3BronzeStorage(fake.client(), "bucket").write(SOURCE, feed(bytes));
        assertArrayEquals(bytes, fake.objects.get(receipt.payloadKey()));
    }

    @Test void metadataSerializationIsDeterministicAcrossInstances() throws Exception {
        var first = new Fake();
        var second = new Fake();
        var receipt = new S3BronzeStorage(first.client(), "bucket").write(SOURCE, feed(PAYLOAD));
        new S3BronzeStorage(second.client(), "bucket").write(SOURCE, feed(PAYLOAD));
        assertArrayEquals(first.objects.get(receipt.metadataKey()), second.objects.get(receipt.metadataKey()));
    }

    @Test void streamReadFailureRetainsCauseAndAbortsWithoutWrites() {
        var fake = new Fake();
        fake.readFailure = new IOException("synthetic read failure");
        var failure = assertThrows(BronzeStorageException.class,
                () -> new S3BronzeStorage(fake.client(), "bucket").write(SOURCE, feed(PAYLOAD)));
        assertEquals(BronzeStorageException.Kind.PERSISTENCE, failure.kind());
        assertEquals(BronzeStorageException.Stage.CHECK_EXISTING, failure.stage());
        assertSame(fake.readFailure, failure.getCause());
        assertTrue(fake.aborted);
        assertTrue(fake.puts.isEmpty());
    }

    @Test void checksumMatchesKnownVector() {
        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad",
                S3BronzeStorage.sha256("abc".getBytes(StandardCharsets.UTF_8)));
    }

    @Test void identityMismatchAndNullsFailBeforeIo() {
        var fake = new Fake();
        var storage = new S3BronzeStorage(fake.client(), "content-lake");
        var wrong = new RssSource("different", SOURCE.feedUri(), true);
        for (var source : new RssSource[] {null, wrong}) {
            assertEquals(BronzeStorageException.Kind.VALIDATION,
                    assertThrows(BronzeStorageException.class, () -> storage.write(source, feed(PAYLOAD))).kind());
        }
        assertThrows(BronzeStorageException.class, () -> storage.write(SOURCE, null));
        assertEquals(0, fake.calls);
    }

    @Test void deterministicKeysUseUtcDateAndSafeEncoding() {
        var raw = new RawFeed(ID, "../雪/=?%\\\"", Instant.parse("2026-10-11T20:15:00-04:00"), 200,
                Optional.empty(), OptionalLong.empty(), PAYLOAD);
        var keys = BronzeKeys.forFeed(raw);
        assertEquals(keys, BronzeKeys.forFeed(raw));
        assertTrue(keys.payload().contains("/ingestion_date=2026-10-12/ingestion_id=" + ID));
        assertTrue(keys.payload().matches("bronze/rss/source_id=[A-Za-z0-9_-]+/ingestion_date=2026-10-12/ingestion_id=" + ID + "/payload.xml"));
        assertFalse(keys.payload().contains(".."));
        var other = new RawFeed(UUID.fromString("aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeef"), raw.sourceId(), TIME,
                200, Optional.empty(), OptionalLong.empty(), PAYLOAD);
        assertNotEquals(keys, BronzeKeys.forFeed(other));
    }

    @Test void invalidUnicodeAndLongKeysFailBeforeIo() {
        for (String id : new String[] {"\uD800", "x".repeat(1000)}) {
            var fake = new Fake();
            var raw = new RawFeed(ID, id, TIME, 200, Optional.empty(), OptionalLong.empty(), PAYLOAD);
            assertEquals(BronzeStorageException.Kind.VALIDATION, assertThrows(BronzeStorageException.class,
                    () -> new S3BronzeStorage(fake.client(), "bucket").write(new RssSource(id, SOURCE.feedUri(), true), raw)).kind());
            assertEquals(0, fake.calls);
        }
    }

    @Test void jsonEscapesUnusualSourceValues() throws Exception {
        String id = "雪\n\"\\/../";
        var fake = new Fake();
        var source = new RssSource(id, SOURCE.feedUri(), false);
        var raw = new RawFeed(ID, id, TIME, 503, Optional.empty(), OptionalLong.empty(), PAYLOAD);
        var receipt = new S3BronzeStorage(fake.client(), "bucket").write(source, raw);
        var json = new ObjectMapper().readTree(fake.objects.get(receipt.metadataKey()));
        assertEquals(id, json.get("sourceId").asText());
        assertEquals(503, json.get("httpStatus").asInt()); // Raw error responses may be archived explicitly.
    }

    @Test void identicalRetryDoesNotRewriteOrCreateAnotherRecord() throws Exception {
        var fake = new Fake();
        var storage = new S3BronzeStorage(fake.client(), "bucket");
        var receipt = storage.write(SOURCE, feed(PAYLOAD));
        assertEquals(receipt, storage.write(SOURCE, feed(PAYLOAD)));
        assertEquals(2, fake.puts.size());
        assertEquals(2, fake.objects.size());
    }

    @Test void changedContentAndProvenanceAreConflicts() throws Exception {
        var fake = new Fake();
        var storage = new S3BronzeStorage(fake.client(), "bucket");
        storage.write(SOURCE, feed(PAYLOAD));
        assertEquals(BronzeStorageException.Kind.CONFLICT,
                assertThrows(BronzeStorageException.class, () -> storage.write(SOURCE, feed(new byte[] {9}))).kind());
        var relocated = new RssSource(SOURCE.sourceId(), URI.create("https://example.test/other"), true);
        assertEquals(BronzeStorageException.Kind.CONFLICT,
                assertThrows(BronzeStorageException.class, () -> storage.write(relocated, feed(PAYLOAD))).kind());
        assertEquals(2, fake.puts.size());
    }

    @Test void corruptOrMissingPublishedPayloadIsConflict() throws Exception {
        for (boolean missing : new boolean[] {false, true}) {
            var fake = new Fake();
            var storage = new S3BronzeStorage(fake.client(), "bucket");
            var receipt = storage.write(SOURCE, feed(PAYLOAD));
            if (missing) fake.objects.remove(receipt.payloadKey());
            else fake.objects.put(receipt.payloadKey(), new byte[] {42});
            assertEquals(BronzeStorageException.Kind.CONFLICT,
                    assertThrows(BronzeStorageException.class, () -> storage.write(SOURCE, feed(PAYLOAD))).kind());
            assertEquals(2, fake.puts.size());
        }
    }

    @Test void payloadFailureDoesNotPublishMetadata() {
        var fake = new Fake();
        fake.failPutSuffix = "payload.xml";
        var failure = assertThrows(BronzeStorageException.class,
                () -> new S3BronzeStorage(fake.client(), "bucket").write(SOURCE, feed(PAYLOAD)));
        assertEquals(BronzeStorageException.Kind.PERSISTENCE, failure.kind());
        assertEquals(BronzeStorageException.Stage.PAYLOAD, failure.stage());
        assertSame(fake.failure, failure.getCause());
        assertEquals(1, fake.puts.size());
        assertTrue(fake.objects.isEmpty());
    }

    @Test void metadataFailureLeavesOrphanAndIdenticalRetryCompletesIt() throws Exception {
        var fake = new Fake();
        fake.failPutSuffix = "metadata.json";
        var storage = new S3BronzeStorage(fake.client(), "bucket");
        var failure = assertThrows(BronzeStorageException.class, () -> storage.write(SOURCE, feed(PAYLOAD)));
        assertEquals(BronzeStorageException.Stage.METADATA, failure.stage());
        assertSame(fake.failure, failure.getCause());
        assertEquals(1, fake.objects.size());
        fake.failPutSuffix = null;
        storage.write(SOURCE, feed(PAYLOAD));
        assertEquals(3, fake.puts.size());
        assertEquals(2, fake.objects.size());
    }

    @Test void differingOrphanIsNotOverwritten() {
        var fake = new Fake();
        fake.objects.put(BronzeKeys.forFeed(feed(PAYLOAD)).payload(), new byte[] {7});
        assertEquals(BronzeStorageException.Kind.CONFLICT, assertThrows(BronzeStorageException.class,
                () -> new S3BronzeStorage(fake.client(), "bucket").write(SOURCE, feed(PAYLOAD))).kind());
        assertTrue(fake.puts.isEmpty());
        assertTrue(fake.aborted);
    }

    @Test void uncertainMetadataSuccessCanBeRetried() throws Exception {
        var fake = new Fake();
        fake.failPutSuffix = "metadata.json";
        fake.persistBeforeFailure = true;
        var storage = new S3BronzeStorage(fake.client(), "bucket");
        assertThrows(BronzeStorageException.class, () -> storage.write(SOURCE, feed(PAYLOAD)));
        fake.failPutSuffix = null;
        storage.write(SOURCE, feed(PAYLOAD));
        assertEquals(2, fake.puts.size());
    }

    @Test void missingBucketAndPermissionErrorsAreNotTreatedAsMissingObjects() {
        for (String code : List.of("NoSuchBucket", "AccessDenied")) {
            var fake = new Fake();
            fake.getFailure = s3Error(code, code.equals("NoSuchBucket") ? 404 : 403);
            var failure = assertThrows(BronzeStorageException.class,
                    () -> new S3BronzeStorage(fake.client(), "bucket").write(SOURCE, feed(PAYLOAD)));
            assertEquals(BronzeStorageException.Stage.CHECK_EXISTING, failure.stage());
            assertSame(fake.getFailure, failure.getCause());
            assertTrue(fake.puts.isEmpty());
        }
    }

    static S3Exception s3Error(String code, int status) {
        var builder = S3Exception.builder();
        builder.statusCode(status);
        builder.awsErrorDetails(AwsErrorDetails.builder().errorCode(code).build());
        return (S3Exception) builder.build();
    }

    /** Fake only the SDK boundary; no extra production gateway abstraction is introduced. */
    static final class Fake {
        final Map<String, byte[]> objects = new HashMap<>();
        final Map<String, String> contentTypes = new HashMap<>();
        final List<String> puts = new ArrayList<>();
        final SdkClientException failure = SdkClientException.create("synthetic upload failure");
        S3Exception getFailure;
        IOException readFailure;
        String failPutSuffix;
        boolean persistBeforeFailure;
        boolean aborted;
        int calls;

        S3Client client() {
            return (S3Client) Proxy.newProxyInstance(S3Client.class.getClassLoader(), new Class<?>[] {S3Client.class},
                    (proxy, method, args) -> {
                        calls++;
                        if (method.getName().equals("getObject")) {
                            if (getFailure != null) throw getFailure;
                            if (readFailure != null) {
                                return new ResponseInputStream<>(GetObjectResponse.builder().build(),
                                        AbortableInputStream.create(new java.io.InputStream() {
                                            @Override public int read() throws IOException { throw readFailure; }
                                        }, () -> aborted = true));
                            }
                            String key = ((GetObjectRequest) args[0]).key();
                            byte[] bytes = objects.get(key);
                            if (bytes == null) throw s3Error("NoSuchKey", 404);
                            return new ResponseInputStream<>(GetObjectResponse.builder().contentLength((long) bytes.length).build(),
                                    AbortableInputStream.create(new ByteArrayInputStream(bytes), () -> aborted = true));
                        }
                        if (method.getName().equals("putObject")) {
                            var request = (PutObjectRequest) args[0];
                            puts.add(request.key());
                            boolean fail = failPutSuffix != null && request.key().endsWith(failPutSuffix);
                            if (!fail || persistBeforeFailure) {
                                try (var input = ((RequestBody) args[1]).contentStreamProvider().newStream()) {
                                    objects.put(request.key(), input.readAllBytes());
                                }
                                contentTypes.put(request.key(), request.contentType());
                            }
                            if (fail) throw failure;
                            return PutObjectResponse.builder().build();
                        }
                        throw new UnsupportedOperationException(method.getName());
                    });
        }
    }
}
