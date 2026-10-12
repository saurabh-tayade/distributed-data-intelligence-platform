package platform.ingestion.bronze;

import platform.ingestion.rss.RawFeed;
import platform.ingestion.rss.RssSource;

/** Raw persistence only. Caller guarantees a single writer and immutable provenance per ingestion ID. */
public interface BronzeStorage {
    BronzeReceipt write(RssSource source, RawFeed feed) throws BronzeStorageException;
}
