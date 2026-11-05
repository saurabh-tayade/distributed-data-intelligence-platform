package platform.contentapi;

import java.util.List;
import platform.contentapi.ContentCatalogResponse.Meta;
import platform.contentapi.ContentCatalogResponse.Reference;

/** Coverage totals describe the entire source-filtered union domain, never only this page. */
public record ContentCoverageResponse(String publication, String productId, String goldPublication, Reference reference,
        Filters filters, List<Row> data, Totals totals, Meta meta) {
    public record Filters(String sourceId) { }
    public record Row(String sourceId, String referenceStatus, String referenceUsage, String publisherName,
            String publisherCategory, String declaredLanguage, String countryCode, String articleCount,
            String articlesMissingReferenceCount, List<String> missingOptionalAttributes) { }
    public record Totals(String articleCount, String matchedArticleCount, String missingArticleCount,
            String catalogSourceCount, String matchedSourceCount, String missingSourceCount,
            String matchedSourcesMissingOptionalAttributesCount, String referenceSourceCount, String unusedReferenceSourceCount) { }
}
