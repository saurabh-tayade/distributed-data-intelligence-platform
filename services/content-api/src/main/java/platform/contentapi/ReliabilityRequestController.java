package platform.contentapi;

import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import platform.contentapi.execution.AnalyticsRole;

@AnalyticsRole
@RestController
@RequestMapping("/api/v1/reliability")
public class ReliabilityRequestController {
    private final ReliabilityAnalyticsAdapter adapter;
    public ReliabilityRequestController(ReliabilityAnalyticsAdapter adapter) { this.adapter = adapter; }

    @GetMapping("/input-coverage")
    public ResponseEntity<ReliabilityCoverageResponse> inputCoverage(@RequestParam MultiValueMap<String, String> parameters) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
                .body(adapter.coverage(ReliabilityCoverageQuery.from("input_coverage", parameters)));
    }

    @GetMapping("/reference-coverage")
    public ResponseEntity<ReliabilityCoverageResponse> referenceCoverage(@RequestParam MultiValueMap<String, String> parameters) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore())
                .body(adapter.coverage(ReliabilityCoverageQuery.from("reference_coverage", parameters)));
    }

    @GetMapping("/request-windows")
    public ResponseEntity<ReliabilityRequestsResponse> windows(@RequestParam MultiValueMap<String, String> parameters) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(adapter.requests(ReliabilityRequestQuery.from(parameters)));
    }
}
