package platform.contentapi;

import org.springframework.stereotype.Service;

@Service
public class AnalyticsService {
    private final GoldAnalyticsAdapter adapter;
    public AnalyticsService(GoldAnalyticsAdapter adapter) { this.adapter = adapter; }

    public ArticlesResponse articles(ArticleQuery query) {
        var result = adapter.articles(query);
        var data = result.rows().stream().map(row -> new ArticlesResponse.Article(row.articleId(), row.sourceId(),
                row.title(), row.url(), row.summary(), row.publishedAt(), row.updatedAt(), row.firstObservedAt(),
                row.lastObservedAt(), Long.toString(row.observationCount()), Long.toString(row.distinctIngestionCount()),
                row.qualityWarnings())).toList();
        return new ArticlesResponse(result.publication(), data,
                new ArticlesResponse.Meta(data.size(), query.limit(), query.offset(), result.hasMore()));
    }

    public DailyResponse daily(DateQuery query) {
        var result = adapter.daily(query);
        var data = result.rows().stream().map(row -> new DailyResponse.Day(row.sourceId(), row.ingestionDate(),
                Long.toString(row.observationCount()), Long.toString(row.distinctArticleCount()),
                Long.toString(row.distinctIngestionCount()), Long.toString(row.qualityWarningCount()), row.latestIngestedAt())).toList();
        return new DailyResponse(result.publication(), data, new DailyResponse.Meta(data.size(), query.limit()));
    }

    public QualityResponse quality(DateQuery query) {
        var result = adapter.quality(query);
        var data = result.rows().stream().map(row -> new QualityResponse.Quality(row.sourceId(),
                Long.toString(row.observationCount()), Long.toString(row.qualityWarningCount()), row.warningCodesPerObservation())).toList();
        return new QualityResponse(result.publication(), data, new QualityResponse.Meta(data.size(), query.limit()));
    }
}
