package platform.ingestion.bronze;

import java.util.Objects;
import java.util.UUID;

/** Returned only after both objects exist with the expected contents. */
public record BronzeReceipt(String bucket, String payloadKey, String metadataKey,
                            UUID ingestionId, long payloadByteLength, String payloadSha256) {
    public BronzeReceipt {
        Objects.requireNonNull(ingestionId, "ingestionId");
        if (bucket == null || bucket.isBlank() || payloadKey == null || payloadKey.isBlank()
                || metadataKey == null || metadataKey.isBlank() || payloadByteLength < 0
                || payloadSha256 == null || !payloadSha256.matches("[0-9a-f]{64}")) {
            throw new IllegalArgumentException("Invalid Bronze receipt");
        }
    }
}
