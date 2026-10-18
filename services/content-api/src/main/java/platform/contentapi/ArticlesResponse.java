package platform.contentapi;

import java.util.List;

public record ArticlesResponse(String publication, List<Article> data, Meta meta) {
    public record Article(String articleId, String sourceId, String title, String url, String summary,
                          String publishedAt, String updatedAt, String firstObservedAt, String lastObservedAt,
                          String observationCount, String distinctIngestionCount, List<String> qualityWarnings) { }
    public record Meta(int returned, int limit, int offset, boolean hasMore) { }
}
