package platform.contentapi;

import org.springframework.stereotype.Service;

@Service
public class PublisherService {
    private final GoldAnalyticsAdapter adapter;
    public PublisherService(GoldAnalyticsAdapter adapter) { this.adapter = adapter; }

    public PublishersResponse publishers(PublisherQuery query) {
        var result = adapter.execute(GoldAnalyticsAdapter.Report.PUBLISHERS, query);
        var data = result.rows().stream().map(row -> new PublishersResponse.Publisher(
                row.sourceId(), Long.toString(row.articleCount()), Long.toString(row.observationCount()),
                row.firstObservedAt(), row.lastObservedAt(), Long.toString(row.articlesWithoutPublishedAt()))).toList();
        return new PublishersResponse(result.publication(), data, new PublishersResponse.Meta(data.size(), query.limit()));
    }
}
