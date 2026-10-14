package platform.ingestion.batch;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import platform.ingestion.rss.RssSource;

/** Strict public configuration; no implicit defaults or coercion. */
public final class SourceCatalog {
    private SourceCatalog() { }

    public static List<RssSource> load(Path path) throws IOException {
        var mapper = new ObjectMapper().enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
                .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
        var root = mapper.readTree(path.toFile());
        if (root == null || !root.isArray()) throw new IllegalArgumentException("Catalog must be a JSON array");
        var sources = new ArrayList<RssSource>();
        for (var entry : root) {
            if (!entry.isObject() || entry.size() != 3
                    || !entry.path("sourceId").isTextual() || !entry.path("feedUri").isTextual()
                    || !entry.path("enabled").isBoolean()) {
                throw new IllegalArgumentException("Each source requires only sourceId, feedUri and boolean enabled");
            }
            try {
                sources.add(new RssSource(entry.get("sourceId").textValue(),
                        URI.create(entry.get("feedUri").textValue()), entry.get("enabled").booleanValue()));
            } catch (IllegalArgumentException failure) {
                throw new IllegalArgumentException("Invalid source definition at index " + sources.size());
            }
        }
        return validate(sources);
    }

    public static List<RssSource> validate(List<RssSource> sources) {
        var copy = List.copyOf(sources);
        Set<String> ids = new HashSet<>();
        for (var source : copy) {
            // IDs are printed in console diagnostics; exclude control characters and ambiguous whitespace.
            if (!source.sourceId().matches("[a-z0-9][a-z0-9_-]{0,127}")
                    || source.feedUri().getQuery() != null) {
                throw new IllegalArgumentException("Catalog requires simple source IDs and public URIs without queries");
            }
            if (!ids.add(source.sourceId())) throw new IllegalArgumentException("Duplicate source identifier");
        }
        return copy;
    }
}
