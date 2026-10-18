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
        PUBLISHERS("publishers"), ARTICLES("articles"), DAILY("daily"), QUALITY("quality");
        private final String cliName;
        Report(String cliName) { this.cliName = cliName; }
        public String cliName() { return cliName; }
    }
    public record PublisherRow(String sourceId, long articleCount, long observationCount,
                               String firstObservedAt, String lastObservedAt, long articlesWithoutPublishedAt) { }
    public record Result(String publication, List<PublisherRow> rows) { }
    public record ArticleRow(String articleId, String sourceId, String title, String url, String summary,
                             String publishedAt, String updatedAt, String firstObservedAt, String lastObservedAt,
                             long observationCount, long distinctIngestionCount, List<String> qualityWarnings) { }
    public record ArticleResult(String publication, List<ArticleRow> rows, boolean hasMore) { }
    public record DailyRow(String sourceId, String ingestionDate, long observationCount, long distinctArticleCount,
                           long distinctIngestionCount, long qualityWarningCount, String latestIngestedAt) { }
    public record DailyResult(String publication, List<DailyRow> rows) { }
    public record QualityRow(String sourceId, long observationCount, long qualityWarningCount,
                             double warningCodesPerObservation) { }
    public record QualityResult(String publication, List<QualityRow> rows) { }
    private static final Set<String> ARTICLE_FIELDS = Set.of("contract_version", "article_id", "source_id",
            "identifier_kind", "source_article_id", "source_uri", "feed_format", "title", "url", "summary",
            "published_at", "updated_at", "quality_warnings", "first_observed_at", "last_observed_at",
            "observation_count", "distinct_ingestion_count", "selected_silver_contract_version", "selected_ingestion_id",
            "selected_ingested_at", "selected_entry_index", "selected_input_uri", "selected_payload_sha256",
            "selected_payload_byte_length", "selected_bronze_bucket", "selected_bronze_payload_key", "selected_bronze_metadata_key");
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
        if (report != Report.PUBLISHERS) throw new ApiFailure(INVALID_ARGUMENT);
        return perform(report, () -> decode(runner.run(command(report, query), script.getParent()), report, query));
    }

    public ArticleResult articles(ArticleQuery query) {
        return perform(Report.ARTICLES, () -> decodeArticles(
                runner.run(articleCommand(query), script.getParent()), query));
    }

    public DailyResult daily(DateQuery query) {
        return perform(Report.DAILY, () -> decodeDaily(
                runner.run(dateCommand(Report.DAILY, query), script.getParent()), query));
    }

    public QualityResult quality(DateQuery query) {
        return perform(Report.QUALITY, () -> decodeQuality(
                runner.run(dateCommand(Report.QUALITY, query), script.getParent()), query));
    }

    private <T> T perform(Report report, java.util.function.Supplier<T> execution) {
        long start = System.nanoTime();
        String outcome = "OK";
        String correlation = UUID.randomUUID().toString();
        try {
            if (properties.publication().isEmpty()) throw new ApiFailure(PUBLICATION_UNAVAILABLE);
            if (executable == null || !Files.isExecutable(executable) || !Files.isRegularFile(script)
                    || !Files.isReadable(script)) throw new ApiFailure(ANALYTICS_RUNTIME_UNAVAILABLE);
            return execution.get();
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

    private JsonNode envelope(BoundedProcessRunner.Result result, Report report, PublisherQuery query, Integer offset) {
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
            fields(envelope, offset == null ? Set.of("status", "publication", "report", "limit", "rows")
                    : Set.of("status", "publication", "report", "limit", "rows", "offset", "has_more"));
            require("OK".equals(string(envelope.get("status"))));
            require(properties.publication().equals(string(envelope.get("publication"))));
            require(report.cliName().equals(string(envelope.get("report"))));
            require(count(envelope.get("limit")) == query.limit());
            JsonNode rows = envelope.get("rows");
            require(rows.isArray() && rows.size() <= query.limit());
            if (offset != null) {
                require(count(envelope.get("offset")) == offset);
                require(envelope.get("has_more").isBoolean());
                require(!envelope.get("has_more").booleanValue() || rows.size() == query.limit());
            }
            return envelope;
        } catch (Exception ignored) {
            throw new ApiFailure(ANALYTICS_RESPONSE_INVALID);
        }
    }

    Result decode(BoundedProcessRunner.Result result, Report report, PublisherQuery query) {
        JsonNode rows = envelope(result, report, query, null).get("rows");
        try {
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

    List<String> articleCommand(ArticleQuery query) {
        var args = new ArrayList<>(command(Report.ARTICLES, new PublisherQuery(query.sourceId(), query.limit())));
        args.addAll(List.of("--offset", Integer.toString(query.offset())));
        return List.copyOf(args);
    }

    List<String> dateCommand(Report report, DateQuery query) {
        if (report != Report.DAILY && report != Report.QUALITY) throw new ApiFailure(INVALID_ARGUMENT);
        var args = new ArrayList<>(command(report, new PublisherQuery(query.sourceId(), query.limit())));
        if (query.from() != null) args.addAll(List.of("--start-date", query.from().toString()));
        if (query.to() != null) args.addAll(List.of("--end-date", query.to().toString()));
        return List.copyOf(args);
    }

    ArticleResult decodeArticles(BoundedProcessRunner.Result result, ArticleQuery query) {
        JsonNode body = envelope(result, Report.ARTICLES, new PublisherQuery(query.sourceId(), query.limit()), query.offset());
        return validated(() -> {
            List<ArticleRow> mapped = new ArrayList<>();
            var ids = new java.util.HashSet<String>();
            ArticleRow previous = null;
            for (JsonNode row : body.get("rows")) {
                fields(row, ARTICLE_FIELDS);
                String id = string(row.get("article_id"));
                require(id.matches("[0-9a-f]{64}") && ids.add(id));
                String source = source(row, query.sourceId());
                String title = nonblank(row.get("title"));
                String first = requiredTimestamp(row.get("first_observed_at"));
                String last = requiredTimestamp(row.get("last_observed_at"));
                require(first.compareTo(last) <= 0);
                require(previous == null || previous.lastObservedAt().compareTo(last) > 0
                        || previous.lastObservedAt().equals(last) && previous.articleId().compareTo(id) < 0);
                long observations = positive(row.get("observation_count"));
                long ingestions = positive(row.get("distinct_ingestion_count"));
                require(ingestions <= observations);
                JsonNode warnings = row.get("quality_warnings");
                require(warnings.isArray());
                List<String> codes = new ArrayList<>();
                for (JsonNode code : warnings) codes.add(string(code));
                require(List.of(List.of(), List.of("INVALID_PUBLISHED_AT"), List.of("INVALID_UPDATED_AT"),
                        List.of("INVALID_PUBLISHED_AT", "INVALID_UPDATED_AT")).contains(codes));
                // Check CLI primitive types, then deliberately discard internal provenance.
                for (String field : List.of("identifier_kind", "source_article_id", "source_uri", "feed_format",
                        "selected_ingestion_id", "selected_input_uri", "selected_payload_sha256",
                        "selected_bronze_bucket", "selected_bronze_payload_key", "selected_bronze_metadata_key")) {
                    nonblank(row.get(field));
                }
                for (String field : List.of("contract_version", "selected_silver_contract_version", "selected_entry_index",
                        "selected_payload_byte_length")) count(row.get(field));
                requiredTimestamp(row.get("selected_ingested_at"));
                var value = new ArticleRow(id, source, title, nullableString(row.get("url")), nullableString(row.get("summary")),
                        timestamp(row.get("published_at")), timestamp(row.get("updated_at")), first, last,
                        observations, ingestions, List.copyOf(codes));
                mapped.add(value);
                previous = value;
            }
            return new ArticleResult(properties.publication(), List.copyOf(mapped), body.get("has_more").booleanValue());
        });
    }

    DailyResult decodeDaily(BoundedProcessRunner.Result result, DateQuery query) {
        JsonNode rows = envelope(result, Report.DAILY, new PublisherQuery(query.sourceId(), query.limit()), null).get("rows");
        return validated(() -> {
            List<DailyRow> mapped = new ArrayList<>();
            DailyRow previous = null;
            for (JsonNode row : rows) {
                fields(row, Set.of("source_id", "ingestion_date", "observation_count", "distinct_article_count",
                        "distinct_ingestion_count", "quality_warning_count", "latest_ingested_at"));
                String source = source(row, query.sourceId());
                String day = string(row.get("ingestion_date"));
                require(day.matches("[0-9]{4}-[0-9]{2}-[0-9]{2}"));
                var date = java.time.LocalDate.parse(day);
                require(date.getYear() >= 1 && (query.from() == null || !date.isBefore(query.from()))
                        && (query.to() == null || !date.isAfter(query.to())));
                require(previous == null || compareSources(previous.sourceId(), source) < 0
                        || previous.sourceId().equals(source) && previous.ingestionDate().compareTo(day) < 0);
                String latest = requiredTimestamp(row.get("latest_ingested_at"));
                require(latest.startsWith(day + "T"));
                long observations = positive(row.get("observation_count"));
                long articles = positive(row.get("distinct_article_count"));
                long ingestions = positive(row.get("distinct_ingestion_count"));
                require(articles <= observations && ingestions <= observations);
                var value = new DailyRow(source, day, observations, articles, ingestions,
                        count(row.get("quality_warning_count")), latest);
                mapped.add(value);
                previous = value;
            }
            return new DailyResult(properties.publication(), List.copyOf(mapped));
        });
    }

    QualityResult decodeQuality(BoundedProcessRunner.Result result, DateQuery query) {
        JsonNode rows = envelope(result, Report.QUALITY, new PublisherQuery(query.sourceId(), query.limit()), null).get("rows");
        return validated(() -> {
            List<QualityRow> mapped = new ArrayList<>();
            String previous = null;
            for (JsonNode row : rows) {
                fields(row, Set.of("source_id", "observation_count", "quality_warning_count", "warning_codes_per_observation"));
                String source = source(row, query.sourceId());
                require(previous == null || compareSources(previous, source) < 0);
                JsonNode value = row.get("warning_codes_per_observation");
                require(value.isNumber());
                double rate = value.doubleValue();
                require(Double.isFinite(rate) && rate >= 0);
                mapped.add(new QualityRow(source, positive(row.get("observation_count")),
                        count(row.get("quality_warning_count")), rate));
                previous = source;
            }
            return new QualityResult(properties.publication(), List.copyOf(mapped));
        });
    }

    private static <T> T validated(java.util.function.Supplier<T> mapping) {
        try { return mapping.get(); }
        catch (Exception ignored) { throw new ApiFailure(ANALYTICS_RESPONSE_INVALID); }
    }
    private static int compareSources(String left, String right) {
        return java.util.Arrays.compareUnsigned(left.getBytes(StandardCharsets.UTF_8), right.getBytes(StandardCharsets.UTF_8));
    }
    private static String source(JsonNode row, String filter) {
        String value = nonblank(row.get("source_id"));
        require(filter == null || filter.equals(value));
        return value;
    }
    private static String nonblank(JsonNode node) {
        String value = string(node);
        require(!value.isBlank());
        return value;
    }
    private static String nullableString(JsonNode node) { return node.isNull() ? null : string(node); }
    private static String requiredTimestamp(JsonNode node) {
        String value = timestamp(node);
        require(value != null);
        return value;
    }
    private static long positive(JsonNode node) {
        long value = count(node);
        require(value > 0);
        return value;
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
