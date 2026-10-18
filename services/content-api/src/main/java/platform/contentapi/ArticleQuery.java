package platform.contentapi;

public record ArticleQuery(String sourceId, int limit, int offset) {
    public static final int MAX_OFFSET = 1_000_000;
    public ArticleQuery {
        new PublisherQuery(sourceId, limit);
        if (offset < 0 || offset > MAX_OFFSET) throw new ApiFailure(ApiFailure.Code.INVALID_ARGUMENT);
    }
}
