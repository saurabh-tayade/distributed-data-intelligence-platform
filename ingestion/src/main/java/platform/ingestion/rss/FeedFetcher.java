package platform.ingestion.rss;

/** Retrieval only: no parsing, persistence, scheduling, or application retries. */
public interface FeedFetcher {
    RawFeed fetch(RssSource source) throws FeedFetchException;
}
