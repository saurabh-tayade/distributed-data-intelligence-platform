package platform.contentapi;

import java.util.List;

/** Exact native counters, UTC microseconds and nulls are retained without reinterpretation. */
public record ContentCatalogResponse(String publication, String productId, String goldPublication, Reference reference,
        Filters filters, List<Row> data, Meta meta) {
    public record Reference(String referenceId, String revision, String snapshotFingerprint) { }
    public record Filters(String sourceId, String from, String to, String referenceStatus, String publisherCategory,
            String declaredLanguage, String countryCode) { }
    public record Row(String articleId, String sourceId, String identifierKind, String sourceArticleId, String feedFormat,
            String title, String url, String summary, String publishedAt, String updatedAt, List<String> qualityWarnings,
            String firstObservedAt, String lastObservedAt, String observationCount, String distinctIngestionCount,
            String discoveredAt, String discoveryDate, String publisherName, String publisherCategory,
            String declaredLanguage, String countryCode, String referenceStatus, String referenceId,
            String referenceRevision, String referenceFingerprint) { }
    public record Meta(int returned, int limit, int offset, boolean hasMore) { }
}
