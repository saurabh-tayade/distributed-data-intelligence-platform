package platform.contentapi;

import java.nio.ByteBuffer;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;
import platform.contentapi.execution.AnalyticsRole;
import tools.jackson.core.StreamReadFeature;
import tools.jackson.databind.DeserializationFeature;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static platform.contentapi.ApiFailure.Code.*;
import static platform.contentapi.ContentCatalogResponse.*;

/** A fixed projection of the independently verified native Content catalog. No SQL or cache. */
@AnalyticsRole
@Component
public class ContentAnalyticsAdapter {
    private static final Logger LOG = LoggerFactory.getLogger(ContentAnalyticsAdapter.class);
    private static final JsonMapper JSON = JsonMapper.builder().enable(StreamReadFeature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS).build();
    private static final Set<String> ENVELOPE = Set.of("status", "product_id", "product_fingerprint", "gold_fingerprint",
            "reference", "report", "filters", "limit", "offset", "has_more", "rows");
    private static final Set<String> ROW = Set.of("article_id", "source_id", "identifier_kind", "source_article_id", "feed_format",
            "title", "url", "summary", "published_at", "updated_at", "quality_warnings", "first_observed_at", "last_observed_at",
            "observation_count", "distinct_ingestion_count", "discovered_at", "discovery_date", "publisher_name", "publisher_category",
            "declared_language", "country_code", "reference_status", "reference_id", "reference_revision", "reference_fingerprint");
    private final ContentAnalyticsProperties properties;
    private final BoundedProcessRunner runner;
    private final Path script;
    private final Path executable;

    public ContentAnalyticsAdapter(ContentAnalyticsProperties properties, BoundedProcessRunner runner) {
        this.properties = properties;
        this.runner = runner;
        script = properties.processingRoot().toAbsolutePath().normalize().resolve("content_analytics.py");
        executable = resolveExecutable(properties.pythonExecutable());
    }

    private static Path resolveExecutable(String configured) {
        Path path = Path.of(configured);
        if (path.isAbsolute() || path.getNameCount() > 1) return path.toAbsolutePath().normalize();
        for (String directory : System.getenv().getOrDefault("PATH", "").split(java.io.File.pathSeparator)) {
            if (directory.isBlank()) continue;
            Path candidate = Path.of(directory).resolve(path).toAbsolutePath().normalize();
            if (Files.isRegularFile(candidate) && Files.isExecutable(candidate)) return candidate;
        }
        return null;
    }

    public ContentCatalogResponse catalog(ContentCatalogQuery query) {
        long start = System.nanoTime();
        String outcome = "OK", correlation = UUID.randomUUID().toString();
        try {
            if (properties.publication().isEmpty()) throw new ApiFailure(PUBLICATION_UNAVAILABLE);
            if (executable == null || !Files.isExecutable(executable) || !Files.isRegularFile(script)
                    || !Files.isReadable(script)) throw new ApiFailure(ANALYTICS_RUNTIME_UNAVAILABLE);
            return decode(runner.run(command(query), script.getParent()), query);
        } catch (ApiFailure failure) {
            outcome = failure.code().name(); throw failure;
        } catch (RuntimeException ignored) {
            outcome = ANALYTICS_EXECUTION_FAILED.name(); throw new ApiFailure(ANALYTICS_EXECUTION_FAILED);
        } finally {
            LOG.info("report=content_catalog executionId={} durationMs={} outcome={}", correlation,
                    (System.nanoTime() - start) / 1_000_000, outcome);
        }
    }

    List<String> command(ContentCatalogQuery query) {
        var args = new ArrayList<>(List.of(executable.toString(), "-E", "-s", "-B", "-X", "utf8", script.toString(),
                "--root", properties.root().toAbsolutePath().normalize().toString(), "--publication", properties.publication(),
                "--gold-root", properties.goldRoot().toAbsolutePath().normalize().toString(),
                "--reference-root", properties.referenceRoot().toAbsolutePath().normalize().toString(),
                "--report", "catalog", "--limit", Integer.toString(query.limit()), "--offset", Integer.toString(query.offset())));
        option(args, "source", query.sourceId()); option(args, "start-date", query.from()); option(args, "end-date", query.to());
        option(args, "reference-status", query.referenceStatus()); option(args, "publisher-category", query.publisherCategory());
        option(args, "declared-language", query.declaredLanguage()); option(args, "country-code", query.countryCode());
        return List.copyOf(args);
    }
    private static void option(List<String> args, String name, String value) {
        if (value != null) args.addAll(List.of("--" + name, value));
    }

    ContentCatalogResponse decode(BoundedProcessRunner.Result result, ContentCatalogQuery query) {
        // Native diagnostics are counted and discarded on stderr by the shared runner.
        // A nonzero exit never becomes a partial result or a guessed path-bearing failure.
        if (result.exitCode() != 0) throw new ApiFailure(ANALYTICS_EXECUTION_FAILED);
        try {
            String text = StandardCharsets.UTF_8.newDecoder().onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT).decode(ByteBuffer.wrap(result.stdout())).toString();
            JsonNode body = JSON.readTree(text);
            fields(body, ENVELOPE);
            require("OK".equals(string(body.get("status"))) && "catalog".equals(string(body.get("report")))
                    && "content-intelligence".equals(string(body.get("product_id")))
                    && properties.publication().equals(fingerprint(body.get("product_fingerprint"))));
            String gold = fingerprint(body.get("gold_fingerprint"));
            require(Integer.toString(query.limit()).equals(decimal(body.get("limit")))
                    && Integer.toString(query.offset()).equals(decimal(body.get("offset"))));
            JsonNode filter = body.get("filters");
            fields(filter, Set.of("source", "start_date", "end_date", "reference_status", "publisher_category", "declared_language", "country_code"));
            require(Objects.equals(query.sourceId(), nullable(filter.get("source")))
                    && Objects.equals(query.from(), nullable(filter.get("start_date")))
                    && Objects.equals(query.to(), nullable(filter.get("end_date")))
                    && Objects.equals(query.referenceStatus(), nullable(filter.get("reference_status")))
                    && Objects.equals(query.publisherCategory(), nullable(filter.get("publisher_category")))
                    && Objects.equals(query.declaredLanguage(), nullable(filter.get("declared_language")))
                    && Objects.equals(query.countryCode(), nullable(filter.get("country_code"))));
            JsonNode ref = body.get("reference");
            fields(ref, Set.of("reference_id", "revision", "snapshot_fingerprint"));
            var reference = new Reference(identifier(ref.get("reference_id"), 64), identifier(ref.get("revision"), 32),
                    fingerprint(ref.get("snapshot_fingerprint")));
            JsonNode rows = body.get("rows"), more = body.get("has_more");
            require(rows.isArray() && rows.size() <= query.limit() && more.isBoolean()
                    && (!more.booleanValue() || rows.size() == query.limit()));
            var mapped = new ArrayList<Row>();
            var seen = new HashSet<String>();
            Row previous = null;
            for (JsonNode row : rows) {
                fields(row, ROW);
                String article = fingerprint(row.get("article_id")), source = identifier(row.get("source_id"), 128);
                require(seen.add(article) && (query.sourceId() == null || query.sourceId().equals(source)));
                String kind = string(row.get("identifier_kind")), format = string(row.get("feed_format"));
                require(format.equals("rss") && Set.of("rss_guid", "rss_link").contains(kind)
                        || format.equals("atom") && Set.of("atom_id", "atom_link").contains(kind));
                String sourceArticle = nonblank(row.get("source_article_id")), title = nonblank(row.get("title"));
                String first = timestamp(row.get("first_observed_at")), last = timestamp(row.get("last_observed_at"));
                require(first.compareTo(last) <= 0 && (previous == null || previous.lastObservedAt().compareTo(last) > 0
                        || previous.lastObservedAt().equals(last) && previous.articleId().compareTo(article) < 0));
                String discovered = timestamp(row.get("discovered_at")), date = string(row.get("discovery_date"));
                ContentCatalogQuery.date(date);
                require(discovered.equals(first) && date.equals(discovered.substring(0, 10))
                        && (query.from() == null || date.compareTo(query.from()) >= 0)
                        && (query.to() == null || date.compareTo(query.to()) <= 0));
                String count = decimal(row.get("observation_count")), distinct = decimal(row.get("distinct_ingestion_count"));
                require(Long.parseLong(count) > 0 && Long.parseLong(distinct) > 0 && Long.parseLong(distinct) <= Long.parseLong(count));
                JsonNode warnings = row.get("quality_warnings");
                require(warnings.isArray() && warnings.size() <= 2);
                var warningList = new ArrayList<String>();
                for (JsonNode warning : warnings) warningList.add(string(warning));
                require(List.of(List.of(), List.of("INVALID_PUBLISHED_AT"), List.of("INVALID_UPDATED_AT"),
                        List.of("INVALID_PUBLISHED_AT", "INVALID_UPDATED_AT")).contains(warningList));
                String name = nullable(row.get("publisher_name")), category = nullable(row.get("publisher_category")),
                        language = nullable(row.get("declared_language")), country = nullable(row.get("country_code")), status = string(row.get("reference_status"));
                require(status.equals("MISSING") && name == null && category == null && language == null && country == null
                        || status.equals("MATCHED") && scalarName(name));
                require(ContentCatalogQuery.optional(category, ContentCatalogQuery.CATEGORIES)
                        && ContentCatalogQuery.optional(language, ContentCatalogQuery.LANGUAGES)
                        && ContentCatalogQuery.optional(country, ContentCatalogQuery.COUNTRIES)
                        && (query.referenceStatus() == null || query.referenceStatus().equals(status))
                        && (query.publisherCategory() == null || query.publisherCategory().equals(category))
                        && (query.declaredLanguage() == null || query.declaredLanguage().equals(language))
                        && (query.countryCode() == null || query.countryCode().equals(country)));
                require(reference.referenceId().equals(string(row.get("reference_id")))
                        && reference.revision().equals(string(row.get("reference_revision")))
                        && reference.snapshotFingerprint().equals(string(row.get("reference_fingerprint"))));
                var value = new Row(article, source, kind, sourceArticle, format, title, nullable(row.get("url")), nullable(row.get("summary")),
                        optionalTimestamp(row.get("published_at")), optionalTimestamp(row.get("updated_at")), List.copyOf(warningList),
                        first, last, count, distinct, discovered, date, name, category, language, country, status,
                        reference.referenceId(), reference.revision(), reference.snapshotFingerprint());
                mapped.add(value); previous = value;
            }
            return new ContentCatalogResponse(properties.publication(), "content-intelligence", gold, reference,
                    new Filters(query.sourceId(), query.from(), query.to(), query.referenceStatus(), query.publisherCategory(),
                            query.declaredLanguage(), query.countryCode()), List.copyOf(mapped),
                    new Meta(mapped.size(), query.limit(), query.offset(), more.booleanValue()));
        } catch (Exception ignored) { throw new ApiFailure(ANALYTICS_RESPONSE_INVALID); }
    }
    private static boolean scalarName(String text) {
        if (text == null || text.isEmpty() || text.codePointCount(0, text.length()) > 200) return false;
        var whitespace = Set.of(9, 10, 11, 12, 13, 32, 133, 160, 5760, 8192, 8193, 8194, 8195, 8196,
                8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 8287, 12288);
        if (whitespace.contains(text.codePointAt(0)) || whitespace.contains(text.codePointBefore(text.length()))) return false;
        return text.codePoints().noneMatch(c -> c < 32 || c >= 127 && c <= 159 || c == 8232 || c == 8233 || c == 65279);
    }
    private static String timestamp(JsonNode value) {
        String text = string(value);
        ReliabilityRequestQuery.instant(text, false);
        return text;
    }
    private static String optionalTimestamp(JsonNode value) { require(value != null); return value.isNull() ? null : timestamp(value); }
    private static String identifier(JsonNode value, int maximum) {
        String text = string(value); require(text.matches("[a-z0-9][a-z0-9_-]{0," + (maximum - 1) + "}")); return text;
    }
    private static String fingerprint(JsonNode value) { String text = string(value); require(text.matches("[0-9a-f]{64}")); return text; }
    private static String decimal(JsonNode value) {
        String text = string(value); require(text.matches("0|[1-9][0-9]{0,18}")); Long.parseLong(text); return text;
    }
    private static String nonblank(JsonNode value) { String text = string(value); require(!text.isBlank()); return text; }
    private static String nullable(JsonNode value) { require(value != null); return value.isNull() ? null : string(value); }
    private static String string(JsonNode value) {
        require(value != null && value.isString());
        String text = value.asString();
        // Reject lone surrogate escapes: native UTF-8 strings are Unicode scalar text.
        for (int i = 0; i < text.length(); i++) {
            char c = text.charAt(i);
            if (Character.isHighSurrogate(c)) require(++i < text.length() && Character.isLowSurrogate(text.charAt(i)));
            else require(!Character.isLowSurrogate(c));
        }
        return text;
    }
    private static void fields(JsonNode value, Set<String> expected) {
        require(value != null && value.isObject() && value.size() == expected.size() && expected.stream().allMatch(value::has));
    }
    private static void require(boolean valid) { if (!valid) throw new ApiFailure(ANALYTICS_RESPONSE_INVALID); }
}
