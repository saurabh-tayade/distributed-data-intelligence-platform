package platform.contentapi;

import java.util.List;

public record PublishersResponse(String publication, List<Publisher> data, Meta meta) {
    public record Publisher(String sourceId, String articleCount, String observationCount,
                            String firstObservedAt, String lastObservedAt, String articlesWithoutPublishedAt) { }
    public record Meta(int returned, int limit) { }
}
