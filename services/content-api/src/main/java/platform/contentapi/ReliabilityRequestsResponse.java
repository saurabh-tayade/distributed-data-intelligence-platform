package platform.contentapi;

import java.util.List;

/** Native exact integers remain strings; floating-point analytical values are not recomputed. */
public record ReliabilityRequestsResponse(String publication, String productId, int productContractVersion,
        int processorRevision, String inputFormat, String origin, String label, Reference reference,
        Filters filters, List<Row> data, Meta meta) {
    public record Reference(String referenceId, String revision, String snapshotFingerprint) { }
    public record Filters(String serviceId, String environment, String start, String end) { }
    public record Row(String windowStart, String serviceId, String environment, String requestCount,
            String serverErrorCount, double serverErrorRate, String durationSumMs, double meanDurationMs,
            String p95DurationMsApprox, String serviceName, String ownerTeam, String serviceTier, String referenceStatus) { }
    public record Meta(int returned, int limit, int offset, boolean hasMore) { }
}
