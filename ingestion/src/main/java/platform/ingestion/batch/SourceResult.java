package platform.ingestion.batch;

import java.util.UUID;
import platform.ingestion.bronze.BronzeReceipt;

/** Nullable attempt fields mean unavailable, never invented. Contains no payload or exception messages. */
public record SourceResult(String sourceId, Status status, UUID ingestionId, Long payloadByteLength,
                           long durationMs, String failureClassification, BronzeReceipt receipt) {
    public enum Status { SUCCESS, FETCH_FAILED, STORAGE_FAILED, SKIPPED }
}
