package platform.contentapi;

import java.util.List;

public record DailyResponse(String publication, List<Day> data, Meta meta) {
    public record Day(String sourceId, String ingestionDate, String observationCount, String distinctArticleCount,
                      String distinctIngestionCount, String qualityWarningCount, String latestIngestedAt) { }
    public record Meta(int returned, int limit) { }
}
