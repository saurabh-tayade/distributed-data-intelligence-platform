package platform.contentapi;

import static platform.contentapi.ApiFailure.Code.INVALID_ARGUMENT;

public record PublisherQuery(String sourceId, int limit) {
    public PublisherQuery {
        // Same source ID syntax as the ingestion source catalog. No trimming or case folding.
        if (limit < 1 || limit > 1000 || sourceId != null && !sourceId.matches("[a-z0-9][a-z0-9_-]{0,127}")) {
            throw new ApiFailure(INVALID_ARGUMENT);
        }
    }
}
