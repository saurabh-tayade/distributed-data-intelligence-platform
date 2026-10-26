package platform.ingestion.bronze;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.Objects;
import platform.ingestion.rss.RawFeed;
import platform.ingestion.rss.RssSource;
import software.amazon.awssdk.core.exception.SdkException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.GetObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.S3Exception;

import static platform.ingestion.bronze.BronzeStorageException.Kind.*;
import static platform.ingestion.bronze.BronzeStorageException.Stage.*;

/** Uses GET and PUT only; bucket provisioning and client lifecycle belong to the caller. */
public final class S3BronzeStorage implements BronzeStorage {
    private final S3Client client;
    private final String bucket;
    private static final ObjectMapper JSON = new ObjectMapper();

    /** Exact existing wire metadata for a private journal; performs no object-store I/O. */
    public static byte[] preparedMetadata(RssSource source, RawFeed feed) throws BronzeStorageException {
        if (source == null || feed == null || !source.sourceId().equals(feed.sourceId())) {
            throw new BronzeStorageException(VALIDATION, VALIDATE, null);
        }
        byte[] payload = feed.body();
        return metadata(source, feed, BronzeKeys.forFeed(feed).payload(), payload.length, sha256(payload));
    }

    public S3BronzeStorage(S3Client client, String bucket) {
        this.client = Objects.requireNonNull(client, "client");
        if (bucket == null || bucket.isBlank()) {
            throw new IllegalArgumentException("bucket is required");
        }
        this.bucket = bucket;
    }

    @Override
    public BronzeReceipt write(RssSource source, RawFeed feed) throws BronzeStorageException {
        final BronzeKeys keys;
        try {
            if (source == null || feed == null || !source.sourceId().equals(feed.sourceId())) {
                throw new IllegalArgumentException("Source and feed identities must match");
            }
            keys = BronzeKeys.forFeed(feed);
        } catch (IllegalArgumentException | java.time.DateTimeException failure) {
            throw new BronzeStorageException(VALIDATION, VALIDATE, failure);
        }
        byte[] payload = feed.body(); // One owned snapshot; do not ask RawFeed for additional copies.
        String sha = sha256(payload);
        byte[] metadata = metadata(source, feed, keys.payload(), payload.length, sha);
        BronzeReceipt receipt = new BronzeReceipt(bucket, keys.payload(), keys.metadata(),
                feed.ingestionId(), payload.length, sha);
        var stage = CHECK_EXISTING;
        try {
            boolean published = matchesExisting(keys.metadata(), metadata);
            boolean payloadExists = matchesExisting(keys.payload(), payload);
            if (published) {
                if (!payloadExists) {
                    throw new BronzeStorageException(CONFLICT, CHECK_EXISTING, null);
                }
                return receipt;
            }
            if (!payloadExists) {
                stage = PAYLOAD;
                put(keys.payload(), "application/octet-stream", payload);
            }
            stage = METADATA;
            put(keys.metadata(), "application/json", metadata);
            return receipt;
        } catch (SdkException | IOException failure) {
            throw new BronzeStorageException(PERSISTENCE, stage, failure);
        }
    }

    private static byte[] metadata(RssSource source, RawFeed feed, String key, int length, String sha)
            throws BronzeStorageException {
        var fields = new LinkedHashMap<String, Object>();
        fields.put("schemaVersion", 1);
        fields.put("ingestionId", feed.ingestionId().toString());
        fields.put("sourceId", feed.sourceId());
        fields.put("sourceUri", source.feedUri().toString());
        fields.put("ingestedAt", feed.ingestedAt().toString());
        fields.put("httpStatus", feed.httpStatus());
        fields.put("contentType", feed.contentType().orElse(null));
        fields.put("declaredContentLength", feed.contentLength().isPresent() ? feed.contentLength().getAsLong() : null);
        fields.put("payloadByteLength", length);
        fields.put("payloadSha256", sha);
        fields.put("payloadKey", key);
        try {
            return JSON.writeValueAsBytes(fields);
        } catch (JsonProcessingException failure) {
            throw new BronzeStorageException(PERSISTENCE, SERIALIZE, failure);
        }
    }

    private boolean matchesExisting(String key, byte[] expected) throws IOException, BronzeStorageException {
        try (var input = client.getObject(GetObjectRequest.builder().bucket(bucket).key(key).build())) {
            try {
                byte[] chunk = new byte[8192];
                int offset = 0;
                int count;
                while ((count = input.read(chunk)) != -1) {
                    if (count > expected.length - offset) {
                        throw new BronzeStorageException(CONFLICT, CHECK_EXISTING, null);
                    }
                    for (int i = 0; i < count; i++) {
                        if (chunk[i] != expected[offset + i]) {
                            throw new BronzeStorageException(CONFLICT, CHECK_EXISTING, null);
                        }
                    }
                    offset += count;
                }
                if (offset != expected.length) {
                    throw new BronzeStorageException(CONFLICT, CHECK_EXISTING, null);
                }
                return true;
            } finally {
                // Abort on mismatch instead of draining an arbitrarily large remote object.
                input.abort();
            }
        } catch (S3Exception failure) {
            if (failure.statusCode() == 404 && failure.awsErrorDetails() != null
                    && "NoSuchKey".equals(failure.awsErrorDetails().errorCode())) {
                return false;
            }
            throw failure; // NoSuchBucket, access denied and other failures are not absence.
        }
    }

    private void put(String key, String contentType, byte[] bytes) {
        // Repeatable stream over our snapshot avoids RequestBody.fromBytes making another copy.
        client.putObject(PutObjectRequest.builder().bucket(bucket).key(key).contentType(contentType).build(),
                RequestBody.fromContentProvider(() -> new ByteArrayInputStream(bytes), bytes.length, contentType));
    }

    static String sha256(byte[] bytes) {
        try {
            return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));
        } catch (NoSuchAlgorithmException impossible) {
            throw new IllegalStateException("Java runtime must provide SHA-256", impossible);
        }
    }
}
