package platform.contentapi;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Set;
import org.springframework.util.MultiValueMap;
import static platform.contentapi.ApiFailure.Code.INVALID_ARGUMENT;

/** Exactly the native requests_5m filter domain, including its exclusive upper sentinel. */
public record ReliabilityRequestQuery(String start, String end, String serviceId, String environment, int limit, int offset) {
    static final String END_SENTINEL = "10000-01-01T00:00:00.000000Z";
    public ReliabilityRequestQuery {
        if (serviceId != null && !serviceId.matches("[a-z0-9][a-z0-9_-]{0,127}")
                || environment != null && !Set.of("dev", "test", "production").contains(environment)
                || limit < 1 || limit > 1000 || offset < 0 || offset > 1_000_000) invalid();
        Instant low = instant(start, false), high = instant(end, true);
        if (low.getNano() != 0 || high.getNano() != 0 || low.getEpochSecond() % 300 != 0
                || high.getEpochSecond() % 300 != 0 || !high.isAfter(low)
                || Duration.between(low, high).compareTo(Duration.ofDays(31)) > 0) invalid();
    }

    static Instant instant(String value, boolean end) {
        if (end && END_SENTINEL.equals(value)) return LocalDateTime.of(10000, 1, 1, 0, 0).toInstant(ZoneOffset.UTC);
        if (value == null || !value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z")) {
            throw new ApiFailure(INVALID_ARGUMENT);
        }
        try {
            var time = LocalDateTime.parse(value.substring(0, value.length() - 1));
            if (time.getYear() < 1) invalid();
            return time.toInstant(ZoneOffset.UTC);
        } catch (java.time.DateTimeException ignored) { throw new ApiFailure(INVALID_ARGUMENT); }
    }

    static ReliabilityRequestQuery from(MultiValueMap<String, String> parameters) {
        if (!Set.of("start", "end", "serviceId", "environment", "limit", "offset").containsAll(parameters.keySet())
                || parameters.values().stream().anyMatch(values -> values.size() != 1)) invalid();
        return new ReliabilityRequestQuery(parameters.getFirst("start"), parameters.getFirst("end"),
                parameters.getFirst("serviceId"), parameters.getFirst("environment"),
                number(parameters, "limit", 100), number(parameters, "offset", 0));
    }

    private static int number(MultiValueMap<String, String> parameters, String name, int fallback) {
        if (!parameters.containsKey(name)) return fallback;
        String raw = parameters.getFirst(name);
        if (raw == null || !raw.matches("0|[1-9][0-9]{0,6}")) throw new ApiFailure(INVALID_ARGUMENT);
        return Integer.parseInt(raw);
    }
    private static void invalid() { throw new ApiFailure(INVALID_ARGUMENT); }
}
