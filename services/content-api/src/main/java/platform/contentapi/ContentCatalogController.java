package platform.contentapi;

import org.springframework.http.CacheControl;
import org.springframework.http.ResponseEntity;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import platform.contentapi.execution.AnalyticsRole;

@AnalyticsRole
@RestController
@RequestMapping("/api/v1/content")
public class ContentCatalogController {
    private final ContentAnalyticsAdapter adapter;
    public ContentCatalogController(ContentAnalyticsAdapter adapter) { this.adapter = adapter; }
    @GetMapping("/catalog")
    public ResponseEntity<ContentCatalogResponse> catalog(@RequestParam MultiValueMap<String, String> parameters) {
        return ResponseEntity.ok().cacheControl(CacheControl.noStore()).body(adapter.catalog(ContentCatalogQuery.from(parameters)));
    }
}
