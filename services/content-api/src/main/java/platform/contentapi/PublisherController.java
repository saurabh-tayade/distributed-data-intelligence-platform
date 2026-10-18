package platform.contentapi;

import java.util.Set;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import static platform.contentapi.ApiFailure.Code.INVALID_ARGUMENT;

@RestController
@RequestMapping("/api/v1/publishers")
public class PublisherController {
    private final PublisherService service;
    public PublisherController(PublisherService service) { this.service = service; }

    @GetMapping
    public PublishersResponse publishers(@RequestParam MultiValueMap<String, String> parameters) {
        // Reject unknown/duplicate parameters, including attempted publication/root/SQL overrides.
        if (!Set.of("sourceId", "limit").containsAll(parameters.keySet())
                || parameters.values().stream().anyMatch(values -> values.size() != 1)) {
            throw new ApiFailure(INVALID_ARGUMENT);
        }
        int limit = 100;
        if (parameters.containsKey("limit")) {
            String raw = parameters.getFirst("limit");
            if (raw == null || !raw.matches("[0-9]{1,4}")) throw new ApiFailure(INVALID_ARGUMENT);
            limit = Integer.parseInt(raw);
        }
        return service.publishers(new PublisherQuery(parameters.getFirst("sourceId"), limit));
    }
}
