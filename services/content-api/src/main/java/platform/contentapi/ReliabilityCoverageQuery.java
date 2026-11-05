package platform.contentapi;

import java.util.Set;
import org.springframework.util.MultiValueMap;
import static platform.contentapi.ApiFailure.Code.INVALID_ARGUMENT;

/** Coverage is publication-wide; request-window filters are deliberately unsupported. */
public record ReliabilityCoverageQuery(String report, String sourceId, String batchId, String serviceId, int limit, int offset) {
    public ReliabilityCoverageQuery {
        if (!Set.of("input_coverage", "reference_coverage").contains(report)
                || sourceId != null && !sourceId.matches("[a-z0-9][a-z0-9_-]{0,127}")
                || serviceId != null && !serviceId.matches("[a-z0-9][a-z0-9_-]{0,127}")
                || batchId != null && !batchId.matches("[a-z0-9][a-z0-9_-]{0,63}")
                || report.equals("input_coverage") && serviceId != null
                || report.equals("reference_coverage") && (sourceId != null || batchId != null)
                || limit < 1 || limit > 1000 || offset < 0 || offset > 1_000_000) throw new ApiFailure(INVALID_ARGUMENT);
    }
    static ReliabilityCoverageQuery from(String report, MultiValueMap<String, String> parameters) {
        var allowed = report.equals("input_coverage") ? Set.of("sourceId", "batchId", "limit", "offset")
                : Set.of("serviceId", "limit", "offset");
        if (!allowed.containsAll(parameters.keySet()) || parameters.values().stream().anyMatch(v -> v.size() != 1))
            throw new ApiFailure(INVALID_ARGUMENT);
        return new ReliabilityCoverageQuery(report, parameters.getFirst("sourceId"), parameters.getFirst("batchId"),
                parameters.getFirst("serviceId"), number(parameters, "limit", 100), number(parameters, "offset", 0));
    }
    private static int number(MultiValueMap<String, String> parameters, String name, int fallback) {
        if (!parameters.containsKey(name)) return fallback;
        String value = parameters.getFirst(name);
        if (value == null || !value.matches("0|[1-9][0-9]{0,6}")) throw new ApiFailure(INVALID_ARGUMENT);
        return Integer.parseInt(value);
    }
}
