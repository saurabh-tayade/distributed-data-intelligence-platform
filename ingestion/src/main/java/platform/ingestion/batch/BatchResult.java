package platform.ingestion.batch;

import java.util.List;
import static platform.ingestion.batch.SourceResult.Status.*;

public record BatchResult(List<SourceResult> sources, long durationMs) {
    public BatchResult { sources = List.copyOf(sources); }
    public long succeeded() { return sources.stream().filter(s -> s.status() == SUCCESS).count(); }
    public long skipped() { return sources.stream().filter(s -> s.status() == SKIPPED).count(); }
    public long failed() { return sources.size() - succeeded() - skipped(); }
    public int exitCode() { return failed() == 0 ? 0 : 1; }
}
