package platform.ingestion.rss;

import java.util.Optional;

/** HTTP failures retain the raw response; transport failures retain their cause. */
public final class FeedFetchException extends Exception {
    private static final long serialVersionUID = 1L;

    public enum Kind { HTTP, TRANSPORT, INTERRUPTED, DISABLED, PAYLOAD_TOO_LARGE }

    private final Kind kind;
    private final String sourceId;
    @SuppressWarnings("serial") // Exception serialization is not part of this in-process contract.
    private final RawFeed response;

    FeedFetchException(Kind kind, String sourceId, RawFeed response, Throwable cause) {
        super("Feed fetch failed: " + kind
                + (response == null ? "" : " (HTTP " + response.httpStatus() + ")"), cause);
        this.kind = kind;
        this.sourceId = sourceId;
        this.response = response;
    }

    public Kind kind() { return kind; }
    public String sourceId() { return sourceId; }
    public Optional<RawFeed> response() { return Optional.ofNullable(response); }
}
