package platform.contentapi;

import java.time.DateTimeException;
import java.time.LocalDate;
import java.util.Set;
import org.springframework.util.MultiValueMap;
import static platform.contentapi.ApiFailure.Code.INVALID_ARGUMENT;

/** The fixed Content v1 catalog filter vocabulary; no free-form expressions or paths. */
public record ContentCatalogQuery(String sourceId, String from, String to, String referenceStatus,
        String publisherCategory, String declaredLanguage, String countryCode, int limit, int offset) {
    static final Set<String> CATEGORIES = Set.of("business", "culture", "general-news", "science", "technology");
    static final Set<String> LANGUAGES = Set.of("en", "es", "fr");
    static final Set<String> COUNTRIES = Set.of("CA", "GB", "US");
    static final Set<String> STATUSES = Set.of("MATCHED", "MISSING");
    public ContentCatalogQuery {
        if (sourceId != null && !sourceId.matches("[a-z0-9][a-z0-9_-]{0,127}")
                || !optional(referenceStatus, STATUSES) || !optional(publisherCategory, CATEGORIES)
                || !optional(declaredLanguage, LANGUAGES) || !optional(countryCode, COUNTRIES)
                || limit < 1 || limit > 1000 || offset < 0 || offset > 1_000_000) invalid();
        if (from != null) date(from);
        if (to != null) date(to);
        if (from != null && to != null && from.compareTo(to) > 0) invalid();
    }
    static boolean optional(String value, Set<String> allowed) { return value == null || allowed.contains(value); }
    static LocalDate date(String value) {
        if (value == null || !value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}")) throw new ApiFailure(INVALID_ARGUMENT);
        try {
            var date = LocalDate.parse(value);
            if (date.getYear() < 1) invalid();
            return date;
        } catch (DateTimeException ignored) { throw new ApiFailure(INVALID_ARGUMENT); }
    }
    static ContentCatalogQuery from(MultiValueMap<String, String> parameters) {
        if (!Set.of("sourceId", "from", "to", "referenceStatus", "publisherCategory", "declaredLanguage", "countryCode", "limit", "offset")
                .containsAll(parameters.keySet()) || parameters.values().stream().anyMatch(values -> values.size() != 1)) invalid();
        return new ContentCatalogQuery(parameters.getFirst("sourceId"), parameters.getFirst("from"), parameters.getFirst("to"),
                parameters.getFirst("referenceStatus"), parameters.getFirst("publisherCategory"), parameters.getFirst("declaredLanguage"),
                parameters.getFirst("countryCode"), number(parameters, "limit", 100), number(parameters, "offset", 0));
    }
    private static int number(MultiValueMap<String, String> parameters, String name, int fallback) {
        if (!parameters.containsKey(name)) return fallback;
        String raw = parameters.getFirst(name);
        if (raw == null || !raw.matches("0|[1-9][0-9]{0,6}")) throw new ApiFailure(INVALID_ARGUMENT);
        return Integer.parseInt(raw);
    }
    private static void invalid() { throw new ApiFailure(INVALID_ARGUMENT); }
}
