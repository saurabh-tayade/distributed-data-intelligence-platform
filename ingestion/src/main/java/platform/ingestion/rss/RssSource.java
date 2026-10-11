package platform.ingestion.rss;

import java.net.URI;

/** Stable identity is assigned by configuration, never derived from a mutable feed URI. */
public record RssSource(String sourceId, URI feedUri, boolean enabled) {
    public RssSource {
        if (sourceId == null || sourceId.isBlank()) {
            throw new IllegalArgumentException("sourceId must not be blank");
        }
        if (feedUri == null || feedUri.getHost() == null
                || !("http".equalsIgnoreCase(feedUri.getScheme())
                || "https".equalsIgnoreCase(feedUri.getScheme()))
                || feedUri.getUserInfo() != null || feedUri.getFragment() != null
                || feedUri.getPort() > 65535 || feedUri.getPort() == 0) {
            throw new IllegalArgumentException("feedUri must be an HTTP(S) URI with a host, valid port, no credentials or fragment");
        }
    }
}
