package platform.contentapi;

import java.time.LocalDate;

public record DateQuery(String sourceId, int limit, LocalDate from, LocalDate to) {
    public DateQuery {
        new PublisherQuery(sourceId, limit);
        if (from != null && (from.getYear() < 1 || from.getYear() > 9999)
                || to != null && (to.getYear() < 1 || to.getYear() > 9999)
                || from != null && to != null && from.isAfter(to)) {
            throw new ApiFailure(ApiFailure.Code.INVALID_ARGUMENT);
        }
    }
}
