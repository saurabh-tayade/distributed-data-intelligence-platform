package platform.contentapi;

import java.util.Set;
import org.springframework.util.MultiValueMap;
import static platform.contentapi.ApiFailure.Code.INVALID_ARGUMENT;

/** Only the two fixed discovery/coverage projections; the report is never client input. */
public record ContentReportQuery(Report report, String sourceId, String from, String to, int limit, int offset) {
    public enum Report {
        DISCOVERY("source_discovery_daily"), COVERAGE("reference_coverage");
        private final String nativeName;
        Report(String nativeName) { this.nativeName = nativeName; }
        String nativeName() { return nativeName; }
    }
    public ContentReportQuery {
        if (report == null || report == Report.COVERAGE && (from != null || to != null)) throw new ApiFailure(INVALID_ARGUMENT);
        // Keep the accepted source/date/pagination domain identical across all Content reports.
        new ContentCatalogQuery(sourceId, from, to, null, null, null, null, limit, offset);
    }
    static ContentReportQuery from(MultiValueMap<String, String> parameters, Report report) {
        Set<String> allowed = report == Report.DISCOVERY ? Set.of("sourceId", "from", "to", "limit", "offset")
                : Set.of("sourceId", "limit", "offset");
        if (!allowed.containsAll(parameters.keySet()) || parameters.values().stream().anyMatch(values -> values.size() != 1))
            throw new ApiFailure(INVALID_ARGUMENT);
        var common = ContentCatalogQuery.from(parameters);
        return new ContentReportQuery(report, common.sourceId(), common.from(), common.to(), common.limit(), common.offset());
    }
}
