package platform.ingestion.rss;

import java.time.Instant;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;

/** Unparsed response bytes; contentLength is the declared header, not a computed byte count. */
public record RawFeed(UUID ingestionId, String sourceId, Instant ingestedAt, int httpStatus,
                      Optional<String> contentType, OptionalLong contentLength, byte[] body) {
    public RawFeed {
        Objects.requireNonNull(ingestionId, "ingestionId");
        Objects.requireNonNull(ingestedAt, "ingestedAt");
        Objects.requireNonNull(contentType, "contentType");
        Objects.requireNonNull(contentLength, "contentLength");
        if (sourceId == null || sourceId.isBlank() || httpStatus < 100 || httpStatus > 599
                || (contentLength.isPresent() && contentLength.getAsLong() < 0)) {
            throw new IllegalArgumentException("Invalid response metadata");
        }
        body = Objects.requireNonNull(body, "body").clone();
    }

    @Override
    public byte[] body() {
        return body.clone();
    }
}
