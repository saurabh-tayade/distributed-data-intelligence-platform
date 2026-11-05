package platform.contentapi;

import java.util.List;
import platform.contentapi.ContentCatalogResponse.Meta;
import platform.contentapi.ContentCatalogResponse.Reference;

/** One native source/day page, with inclusive UTC first-discovery bounds and no fabricated total. */
public record ContentDiscoveryResponse(String publication, String productId, String goldPublication, Reference reference,
        Filters filters, List<Row> data, Meta meta) {
    public record Filters(String sourceId, String from, String to) { }
    public record Row(String sourceId, String discoveryDate, String discoveredArticleCount, String articlesMissingReferenceCount) { }
}
