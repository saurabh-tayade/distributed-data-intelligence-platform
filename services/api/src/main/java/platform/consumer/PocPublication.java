package platform.consumer;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/** Selection guard only: deliberately NOT the full Gold publication verifier. */
record PocPublication(String fingerprint, Map<String, List<String>> files) {
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION);
    private static final List<String> TABLES = List.of("gold_articles", "gold_source_daily");
    private static final Pattern GLOB = Pattern.compile("[*?\\[\\]]");
    private static final Pattern UNSAFE_FILENAME = Pattern.compile("[/\\\\:*?\\[\\]\\p{Cntrl}]");

    static PocPublication select(Path root, String fingerprint) {
        if (root == null || fingerprint == null || !fingerprint.matches("[0-9a-f]{64}")) {
            throw new PocFailure("INVALID_SELECTION");
        }
        try {
            // The configured root may resolve an operator-controlled alias, as in Python.
            Path directory = root.toRealPath();
            if (GLOB.matcher(directory.toString()).find()) {
                throw new PocFailure("INVALID_SELECTION");
            }
            for (String part : List.of("v1", "processor_revision=1", "input_set=" + fingerprint)) {
                directory = directory.resolve(part);
                if (Files.isSymbolicLink(directory) || !Files.isDirectory(directory)) {
                    throw new PocFailure("INVALID_SELECTION");
                }
            }
            Path markerPath = directory.resolve("complete.json");
            if (!Files.exists(markerPath, LinkOption.NOFOLLOW_LINKS)) {
                throw new PocFailure("INCOMPLETE_OUTPUT");
            }
            regular(markerPath);
            // A POC resource bound, not a new publication-contract limit.
            byte[] markerBytes;
            try (var stream = Files.newInputStream(markerPath)) {
                markerBytes = stream.readNBytes(1024 * 1024 + 1);
            }
            if (markerBytes.length > 1024 * 1024) {
                throw new PocFailure("INVALID_METADATA");
            }
            JsonNode marker = JSON.readTree(markerBytes);
            if (marker == null || !marker.isObject()) {
                throw new PocFailure("INVALID_METADATA");
            }
            version(marker.path("publication_version"));
            version(marker.path("processor_revision"));
            if (!marker.path("input_set_fingerprint").isTextual()
                    || !fingerprint.equals(marker.path("input_set_fingerprint").textValue())) {
                throw new PocFailure("IDENTITY_MISMATCH");
            }
            Map<String, List<String>> files = new HashMap<>();
            for (String table : TABLES) {
                version(marker.path("gold_contract_versions").path(table));
                Path dataset = directory.resolve(table);
                if (Files.isSymbolicLink(dataset) || !Files.isDirectory(dataset)) {
                    throw new PocFailure("INVALID_LAYOUT");
                }
                JsonNode inventory = marker.path("files").path(table);
                if (!inventory.isArray() || inventory.isEmpty()) {
                    throw new PocFailure("INVALID_METADATA");
                }
                var paths = new ArrayList<String>();
                var seen = new HashSet<String>();
                for (JsonNode item : inventory) {
                    JsonNode field = item.path("path");
                    if (!field.isTextual()) {
                        throw new PocFailure("INVALID_METADATA");
                    }
                    String relative = field.textValue();
                    // Flat exact filenames only: reject traversal, aliases and DuckDB globs.
                    String prefix = table + "/";
                    if (!relative.startsWith(prefix)) {
                        throw new PocFailure("INVALID_LAYOUT");
                    }
                    String filename = relative.substring(prefix.length());
                    if (!filename.endsWith(".parquet") || filename.startsWith(".") || filename.startsWith("_")
                            || UNSAFE_FILENAME.matcher(filename).find() || !seen.add(filename)) {
                        throw new PocFailure("INVALID_LAYOUT");
                    }
                    Path path = dataset.resolve(filename);
                    regular(path);
                    paths.add(path.toString());
                }
                files.put(table, List.copyOf(paths));
            }
            return new PocPublication(fingerprint, Map.copyOf(files));
        } catch (IOException | IllegalArgumentException e) {
            throw new PocFailure("PUBLICATION_READ_FAILED");
        }
    }

    private static void regular(Path path) {
        if (Files.isSymbolicLink(path) || !Files.isRegularFile(path, LinkOption.NOFOLLOW_LINKS)) {
            throw new PocFailure("INVALID_LAYOUT");
        }
    }

    private static void version(JsonNode value) {
        if (!value.isIntegralNumber() || !value.canConvertToInt() || value.intValue() != 1) {
            throw new PocFailure("UNSUPPORTED_VERSION");
        }
    }
}
