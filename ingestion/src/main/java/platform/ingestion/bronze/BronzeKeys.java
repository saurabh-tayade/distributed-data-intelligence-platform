package platform.ingestion.bronze;

import java.nio.charset.StandardCharsets;
import java.time.ZoneOffset;
import java.util.Base64;
import platform.ingestion.rss.RawFeed;

/** Vendor-independent naming in a flat object namespace; Base64URL is case-sensitive. */
public record BronzeKeys(String payload, String metadata) {
    public static BronzeKeys forFeed(RawFeed feed) {
        String id = feed.sourceId();
        // UTF-8 replaces unpaired surrogates: reject these rather than create encoding collisions.
        if (!id.equals(new String(id.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8))) {
            throw new IllegalArgumentException("Source ID must contain valid Unicode");
        }
        String encoded = Base64.getUrlEncoder().withoutPadding().encodeToString(id.getBytes(StandardCharsets.UTF_8));
        String prefix = "bronze/rss/source_id=" + encoded + "/ingestion_date="
                + feed.ingestedAt().atOffset(ZoneOffset.UTC).toLocalDate()
                + "/ingestion_id=" + feed.ingestionId() + "/";
        if ((prefix + "metadata.json").getBytes(StandardCharsets.UTF_8).length > 1024) {
            throw new IllegalArgumentException("Encoded object key exceeds S3's 1024-byte limit");
        }
        return new BronzeKeys(prefix + "payload.xml", prefix + "metadata.json");
    }
}
