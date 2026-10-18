package platform.contentapi;

import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.nio.charset.CodingErrorAction;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static platform.contentapi.ApiFailure.Code.*;

/** Delegates all publication verification and SQL to the existing Python consumer. */
@Component
public class GoldAnalyticsAdapter {
    public enum Report {
        PUBLISHERS("publishers");
        private final String cliName;
        Report(String cliName) { this.cliName = cliName; }
        public String cliName() { return cliName; }
    }
    public record PublisherRow(String sourceId, long articleCount, long observationCount,
                               String firstObservedAt, String lastObservedAt, long articlesWithoutPublishedAt) { }
    public record Result(String publication, List<PublisherRow> rows) { }
    private static final Logger LOG = LoggerFactory.getLogger(GoldAnalyticsAdapter.class);
    private static final JsonMapper JSON = JsonMapper.builder()
            .enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    private static final Set<String> PUBLICATION_FAILURES = Set.of(
            "INVALID_SELECTION", "INCOMPLETE_OUTPUT", "INVALID_LAYOUT", "INVALID_METADATA", "UNSUPPORTED_VERSION",
            "IDENTITY_MISMATCH", "MANIFEST_MISMATCH", "INVALID_INVENTORY", "INVENTORY_MISMATCH", "FILE_MISMATCH",
            "PUBLICATION_READ_FAILED", "UNSUPPORTED_SCHEMA", "INVALID_VALUES", "DUPLICATE_KEY", "COUNT_MISMATCH",
            "RECONCILIATION_FAILED", "PUBLICATION_CHANGED", "PARQUET_VALIDATION_FAILED");
    private final AnalyticsProperties properties;
    private final BoundedProcessRunner runner;
    private final Path script;
    private final Path root;
    private final Path executable;

    public GoldAnalyticsAdapter(AnalyticsProperties properties, BoundedProcessRunner runner) {
        this.properties = properties;
        this.runner = runner;
        script = properties.script().toAbsolutePath().normalize();
        root = properties.goldRoot().toAbsolutePath().normalize();
        executable = resolveExecutable(properties.pythonExecutable());
    }

    private static Path resolveExecutable(String configured) {
        Path path = Path.of(configured);
        if (path.isAbsolute() || path.getNameCount() > 1) return path.toAbsolutePath().normalize();
        // Resolve once from the operator's startup PATH. Never inherit that environment into Python.
        for (String directory : System.getenv().getOrDefault("PATH", "").split(java.io.File.pathSeparator)) {
            if (directory.isBlank()) continue;
            Path candidate = Path.of(directory).resolve(path).toAbsolutePath().normalize();
            if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) return candidate;
        }
        return null;
    }

    public Result execute(Report report, PublisherQuery query) {
        long start = System.nanoTime();
        String outcome = "OK";
        String correlation = UUID.randomUUID().toString();
        try {
            if (properties.publication().isEmpty()) throw new ApiFailure(PUBLICATION_UNAVAILABLE);
            if (executable == null || !Files.isExecutable(executable) || !Files.isRegularFile(script)
                    || !Files.isReadable(script)) throw new ApiFailure(ANALYTICS_RUNTIME_UNAVAILABLE);
            return decode(runner.run(command(report, query), script.getParent()), report, query);
        } catch (ApiFailure failure) {
            outcome = failure.code().name();
            throw failure;
        } catch (RuntimeException ignored) {
            outcome = ANALYTICS_EXECUTION_FAILED.name();
            throw new ApiFailure(ANALYTICS_EXECUTION_FAILED);
        } finally {
            LOG.info("report={} executionId={} durationMs={} outcome={}", report.cliName(), correlation,
                    (System.nanoTime() - start) / 1_000_000, outcome);
        }
    }

    List<String> command(Report report, PublisherQuery query) {
        var args = new ArrayList<>(List.of(executable.toString(), "-E", "-s", "-B", "-X", "utf8", script.toString(),
                "--gold-root", root.toString(), "--publication", properties.publication(),
                "--report", report.cliName(), "--limit", Integer.toString(query.limit())));
        if (query.sourceId() != null) args.addAll(List.of("--source", query.sourceId()));
        return List.copyOf(args);
    }

    Result decode(BoundedProcessRunner.Result result, Report report, PublisherQuery query) {
        JsonNode envelope;
        try {
            String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(result.stdout())).toString();
            envelope = JSON.readTree(text);
        } catch (Exception ignored) {
            throw new ApiFailure(result.exitCode() == 0 ? ANALYTICS_RESPONSE_INVALID : ANALYTICS_EXECUTION_FAILED);
        }
        if (result.exitCode() != 0) {
            if (result.exitCode() == 1 && envelope != null && envelope.isObject()
                    && envelope.properties().stream().map(java.util.Map.Entry::getKey).collect(java.util.stream.Collectors.toSet())
                    .equals(Set.of("status", "code", "path"))
                    && envelope.path("status").isString() && "FAILED".equals(envelope.path("status").asString())
                    && envelope.path("code").isString() && envelope.path("path").isString()
                    && PUBLICATION_FAILURES.contains(envelope.path("code").asString())) {
                throw new ApiFailure(PUBLICATION_UNAVAILABLE);
            }
            throw new ApiFailure(ANALYTICS_EXECUTION_FAILED);
        }
        try {
            fields(envelope, Set.of("status", "publication", "report", "limit", "rows"));
            require("OK".equals(string(envelope.get("status"))));
            require(properties.publication().equals(string(envelope.get("publication"))));
            require(report.cliName().equals(string(envelope.get("report"))));
            require(count(envelope.get("limit")) == query.limit());
            JsonNode rows = envelope.get("rows");
            require(rows.isArray() && rows.size() <= query.limit());
            List<PublisherRow> mapped = new ArrayList<>();
            String previous = null;
            for (JsonNode row : rows) {
                fields(row, Set.of("source_id", "article_count", "observation_count", "first_observed_at",
                        "last_observed_at", "articles_without_published_at"));
                String source = string(row.get("source_id"));
                require(!source.isBlank());
                // DuckDB orders strings by UTF-8 bytes, not Java UTF-16 code units.
                require(previous == null || java.util.Arrays.compareUnsigned(previous.getBytes(StandardCharsets.UTF_8),
                        source.getBytes(StandardCharsets.UTF_8)) < 0);
                require(query.sourceId() == null || query.sourceId().equals(source));
                mapped.add(new PublisherRow(source, count(row.get("article_count")), count(row.get("observation_count")),
                        timestamp(row.get("first_observed_at")), timestamp(row.get("last_observed_at")),
                        count(row.get("articles_without_published_at"))));
                previous = source;
            }
            return new Result(properties.publication(), List.copyOf(mapped));
        } catch (Exception ignored) {
            throw new ApiFailure(ANALYTICS_RESPONSE_INVALID);
        }
    }

    private static void fields(JsonNode node, Set<String> expected) {
        require(node != null && node.isObject() && node.size() == expected.size()
                && expected.stream().allMatch(node::has));
    }
    private static void require(boolean valid) {
        if (!valid) throw new ApiFailure(ANALYTICS_RESPONSE_INVALID);
    }
    private static String string(JsonNode node) {
        require(node != null && node.isString());
        return node.asString();
    }
    private static long count(JsonNode node) {
        require(node != null && node.isIntegralNumber() && node.canConvertToLong() && node.longValue() >= 0);
        return node.longValue();
    }
    private static String timestamp(JsonNode node) {
        if (node.isNull()) return null;
        String value = string(node);
        require(value.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\\.[0-9]{6}Z"));
        // Strict calendar parsing rejects impossible dates, leap-second coercion and 24:00 normalization.
        var parsed = java.time.LocalDateTime.parse(value.substring(0, value.length() - 1),
                java.time.format.DateTimeFormatter.ISO_LOCAL_DATE_TIME);
        require(parsed.getYear() >= 1);
        Instant.parse(value);
        return value;
    }
}
