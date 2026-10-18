package platform.contentapi;

import java.time.LocalDate;
import java.time.format.DateTimeParseException;
import java.util.Set;
import org.springframework.util.MultiValueMap;
import org.springframework.web.bind.annotation.*;
import static platform.contentapi.ApiFailure.Code.INVALID_ARGUMENT;

@RestController
@RequestMapping("/api/v1")
public class AnalyticsController {
    private final AnalyticsService service;
    public AnalyticsController(AnalyticsService service) { this.service = service; }

    @GetMapping("/articles")
    public ArticlesResponse articles(@RequestParam MultiValueMap<String, String> parameters) {
        validate(parameters, Set.of("sourceId", "limit", "offset"));
        return service.articles(new ArticleQuery(parameters.getFirst("sourceId"), number(parameters, "limit", 100, 4),
                number(parameters, "offset", 0, 7)));
    }

    @GetMapping("/daily")
    public DailyResponse daily(@RequestParam MultiValueMap<String, String> parameters) {
        return service.daily(dates(parameters));
    }

    @GetMapping("/quality")
    public QualityResponse quality(@RequestParam MultiValueMap<String, String> parameters) {
        return service.quality(dates(parameters));
    }

    private static DateQuery dates(MultiValueMap<String, String> parameters) {
        validate(parameters, Set.of("sourceId", "limit", "from", "to"));
        return new DateQuery(parameters.getFirst("sourceId"), number(parameters, "limit", 100, 4),
                date(parameters.getFirst("from")), date(parameters.getFirst("to")));
    }

    private static void validate(MultiValueMap<String, String> parameters, Set<String> allowed) {
        if (!allowed.containsAll(parameters.keySet()) || parameters.values().stream().anyMatch(values -> values.size() != 1)) {
            throw new ApiFailure(INVALID_ARGUMENT);
        }
    }

    private static int number(MultiValueMap<String, String> parameters, String name, int fallback, int digits) {
        if (!parameters.containsKey(name)) return fallback;
        String raw = parameters.getFirst(name);
        if (raw == null || !raw.matches("[0-9]{1," + digits + "}")) throw new ApiFailure(INVALID_ARGUMENT);
        return Integer.parseInt(raw); // Length bound is below Integer.MAX_VALUE.
    }

    private static LocalDate date(String raw) {
        if (raw == null) return null;
        if (!raw.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) throw new ApiFailure(INVALID_ARGUMENT);
        try { return LocalDate.parse(raw); }
        catch (DateTimeParseException ignored) { throw new ApiFailure(INVALID_ARGUMENT); }
    }
}
