package platform.contentapi;

import java.util.List;
import java.util.Map;

/** Exact native coverage counters are strings. Totals describe the full filtered scope, not the page. */
public record ReliabilityCoverageResponse(String publication, String productId, int productContractVersion,
        int processorRevision, String inputFormat, String origin, String label, ReliabilityRequestsResponse.Reference reference,
        Map<String, String> filters, List<Map<String, Object>> data, Map<String, String> totals, ReliabilityRequestsResponse.Meta meta) { }
