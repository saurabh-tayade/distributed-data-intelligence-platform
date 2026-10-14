package platform.ingestion.batch;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import platform.ingestion.rss.*;
import platform.ingestion.bronze.*;
import static platform.ingestion.batch.SourceResult.Status.*;

/** One fetch and one write per enabled source, in catalog order. No application retries. */
public final class BatchIngestionRunner {
    private final FeedFetcher fetcher;
    private final BronzeStorage storage;

    public BatchIngestionRunner(FeedFetcher fetcher, BronzeStorage storage) {
        this.fetcher = Objects.requireNonNull(fetcher);
        this.storage = Objects.requireNonNull(storage);
    }

    public BatchResult run(List<RssSource> sources) {
        var validated = SourceCatalog.validate(sources);
        long start = System.nanoTime();
        var results = new ArrayList<SourceResult>();
        for (var source : validated) {
            long attempt = System.nanoTime();
            if (!source.enabled()) {
                results.add(new SourceResult(source.sourceId(), SKIPPED, null, null, elapsed(attempt), null, null));
                continue;
            }
            RawFeed raw;
            try {
                raw = fetcher.fetch(source);
            } catch (FeedFetchException failure) {
                var response = failure.response().orElse(null);
                results.add(new SourceResult(source.sourceId(), FETCH_FAILED,
                        response == null ? null : response.ingestionId(),
                        response == null ? null : (long) response.body().length, elapsed(attempt),
                        failure.kind().name() + (response == null ? "" : "_" + response.httpStatus()), null));
                continue;
            } catch (RuntimeException failure) {
                results.add(new SourceResult(source.sourceId(), FETCH_FAILED, null, null,
                        elapsed(attempt), "UNEXPECTED", null));
                continue;
            }
            try {
                var receipt = storage.write(source, raw);
                results.add(new SourceResult(source.sourceId(), SUCCESS, raw.ingestionId(),
                        (long) raw.body().length, elapsed(attempt), null, receipt));
            } catch (BronzeStorageException failure) {
                results.add(new SourceResult(source.sourceId(), STORAGE_FAILED, raw.ingestionId(),
                        (long) raw.body().length, elapsed(attempt), failure.kind() + "_" + failure.stage(), null));
            } catch (RuntimeException failure) {
                results.add(new SourceResult(source.sourceId(), STORAGE_FAILED, raw.ingestionId(),
                        (long) raw.body().length, elapsed(attempt), "UNEXPECTED", null));
            }
        }
        return new BatchResult(results, elapsed(start));
    }

    private static long elapsed(long start) { return (System.nanoTime() - start) / 1_000_000; }
}
