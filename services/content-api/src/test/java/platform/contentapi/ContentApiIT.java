package platform.contentapi;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static org.junit.jupiter.api.Assertions.*;

/** Actual loopback HTTP -> Spring -> bounded child -> unchanged native Content CLI. */
class ContentApiIT {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final List<String> ROW_FIELDS = List.of("article_id", "source_id", "identifier_kind", "source_article_id",
            "feed_format", "title", "url", "summary", "published_at", "updated_at", "quality_warnings",
            "first_observed_at", "last_observed_at", "observation_count", "distinct_ingestion_count", "discovered_at",
            "discovery_date", "publisher_name", "publisher_category", "declared_language", "country_code",
            "reference_status", "reference_id", "reference_revision", "reference_fingerprint");
    private static Path fixtures;
    private static Path processing;
    private static String python;
    private static JsonNode reference;
    private static Map<String, String> baseline;

    @BeforeAll
    static void load() throws Exception {
        fixtures = Path.of(System.getProperty("content.fixture")).toAbsolutePath().normalize();
        assertTrue(Files.isRegularFile(fixtures.resolve("reference.json")),
                "Generate Content fixtures with services/content-api/tests/generate_content_fixtures.py first");
        reference = JSON.readTree(Files.readString(fixtures.resolve("reference.json")));
        processing = Path.of("../../processing").toAbsolutePath().normalize();
        python = System.getProperty("analytics.python");
        assertNotNull(python, "Set -Danalytics.python to the pinned analytics Python executable");
        baseline = inventory();
    }

    private static Map<String, String> inventory() throws Exception {
        Map<String, String> result = new TreeMap<>();
        try (var files = Files.walk(fixtures)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                // Python may create bytecode only in the deliberately incomplete script directory.
                if (file.toString().contains("__pycache__")) continue;
                String hash = HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
                result.put(fixtures.relativize(file).toString(), hash + ":" + Files.size(file) + ":" + Files.getLastModifiedTime(file));
            }
        }
        return result;
    }

    @AfterAll
    static void inputsAndPublicationsAreReadOnly() throws Exception {
        if (baseline != null) assertEquals(baseline, inventory());
    }

    private static Path root(String format) {
        return fixtures.resolve(reference.path("publications").path(format).path("root").asString());
    }

    private static String publication(String format) {
        return reference.path("publications").path(format).path("publication").asString();
    }

    private static ConfigurableApplicationContext start(Path root, String publication, Path code) {
        return new SpringApplicationBuilder(ContentApiApplication.class).run(
                "--server.port=0", "--server.address=127.0.0.1", "--spring.main.banner-mode=off",
                "--logging.level.root=WARN", "--ddip.role=analytics", "--analytics.publication=",
                "--content.analytics.python-executable=" + python,
                "--content.analytics.processing-root=" + code,
                "--content.analytics.root=" + root.resolve("output"),
                "--content.analytics.gold-root=" + root.resolve("gold"),
                "--content.analytics.reference-root=" + root.resolve("references"),
                "--content.analytics.publication=" + publication);
    }

    private static HttpResponse<String> get(ConfigurableApplicationContext context, String query) throws Exception {
        return get(context, "catalog", query);
    }

    private static HttpResponse<String> get(ConfigurableApplicationContext context, String report, String query) throws Exception {
        String endpoint = switch (report) { case "source_discovery_daily" -> "source-discovery-daily";
            case "reference_coverage" -> "reference-coverage"; default -> "catalog"; };
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        try (var client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                            + "/api/v1/content/" + endpoint + query)).timeout(Duration.ofSeconds(40))
                    .GET().build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    private static String camel(String snake) {
        StringBuilder value = new StringBuilder();
        boolean upper = false;
        for (char character : snake.toCharArray()) {
            if (character == '_') upper = true;
            else { value.append(upper ? Character.toUpperCase(character) : character); upper = false; }
        }
        return value.toString();
    }

    private static String query(JsonNode options) {
        List<String> fields = new ArrayList<>();
        for (var field : options.properties()) {
            fields.add(httpName(field.getKey()) + "=" + URLEncoder.encode(field.getValue().asString(), StandardCharsets.UTF_8));
        }
        return "?" + String.join("&", fields);
    }

    private static String httpName(String name) {
        return switch (name) { case "source" -> "sourceId"; case "start_date" -> "from";
            case "end_date" -> "to"; default -> camel(name); };
    }

    private static JsonNode restEnvelope(JsonNode nativeEnvelope) {
        var rest = JSON.createObjectNode();
        rest.set("publication", nativeEnvelope.get("product_fingerprint"));
        rest.set("productId", nativeEnvelope.get("product_id"));
        rest.set("goldPublication", nativeEnvelope.get("gold_fingerprint"));
        var ref = rest.putObject("reference");
        for (String name : List.of("reference_id", "revision", "snapshot_fingerprint"))
            ref.set(camel(name), nativeEnvelope.path("reference").get(name));
        var filters = rest.putObject("filters");
        for (var field : nativeEnvelope.path("filters").properties())
            filters.set(httpName(field.getKey()), field.getValue());
        Set<String> expectedFields = switch (nativeEnvelope.path("report").asString()) {
            case "source_discovery_daily" -> Set.of("source_id", "discovery_date", "discovered_article_count", "articles_missing_reference_count");
            case "reference_coverage" -> Set.of("source_id", "reference_status", "reference_usage", "publisher_name", "publisher_category", "declared_language", "country_code", "article_count", "articles_missing_reference_count", "missing_optional_attributes");
            default -> Set.copyOf(ROW_FIELDS);
        };
        var rows = rest.putArray("data");
        for (JsonNode nativeRow : nativeEnvelope.path("rows")) {
            assertEquals(expectedFields, nativeRow.properties().stream().map(Map.Entry::getKey).collect(java.util.stream.Collectors.toSet()));
            var row = rows.addObject();
            for (var field : nativeRow.properties()) row.set(camel(field.getKey()), field.getValue());
        }
        if (nativeEnvelope.has("totals")) {
            var totals = rest.putObject("totals");
            for (var field : nativeEnvelope.path("totals").properties()) totals.set(camel(field.getKey()), field.getValue());
        }
        rest.putObject("meta").put("returned", rows.size())
                .put("limit", Integer.parseInt(nativeEnvelope.path("limit").asString()))
                .put("offset", Integer.parseInt(nativeEnvelope.path("offset").asString()))
                .set("hasMore", nativeEnvelope.get("has_more"));
        return rest;
    }

    @TestFactory
    List<DynamicTest> everyFieldAndTypeMatchesNativeCli() {
        List<DynamicTest> tests = new ArrayList<>();
        for (String kind : List.of("standard", "empty", "large-counts")) {
            tests.add(DynamicTest.dynamicTest("Native HTTP parity: " + kind, () -> {
                try (var context = start(root(kind), publication(kind), processing)) {
                    int seen = 0;
                    for (JsonNode scenario : reference.path("scenarios")) {
                        if (!kind.equals(scenario.path("kind").asString())) continue;
                        String report = scenario.path("report").asString();
                        var response = get(context, report, query(scenario.path("options")));
                        assertEquals(200, response.statusCode(), scenario.path("name").asString() + ": " + response.body());
                        JsonNode actual = JSON.readTree(response.body());
                        assertEquals(restEnvelope(scenario.path("envelope")), actual, scenario.path("name").asString());
                        for (JsonNode row : actual.path("data")) {
                            if (report.equals("catalog")) {
                                assertTrue(row.path("observationCount").isString());
                                assertTrue(row.path("distinctIngestionCount").isString());
                                assertTrue(row.path("qualityWarnings").isArray());
                            } else {
                                assertTrue(row.path(report.equals("reference_coverage") ? "articleCount" : "discoveredArticleCount").isString());
                                assertTrue(row.path("articlesMissingReferenceCount").isString());
                            }
                        }
                        for (String number : List.of("returned", "limit", "offset"))
                            assertTrue(actual.path("meta").path(number).isIntegralNumber());
                        assertTrue(actual.path("meta").path("hasMore").isBoolean());
                        seen++;
                    }
                    assertEquals(50, seen, "Do not silently omit a parity scenario");
                    assertEquals(100, JSON.readTree(get(context, "").body()).path("meta").path("limit").asInt());
                }
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> completePublicationAndDependenciesAreVerifiedBeforeEveryPage() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode failure : reference.path("failures")) {
            tests.add(DynamicTest.dynamicTest("Safe native failure: " + failure.path("case").asString(), () -> {
                try (var context = start(fixtures.resolve(failure.path("root").asString()), failure.path("publication").asString(), processing)) {
                    for (String report : List.of("catalog", "source_discovery_daily", "reference_coverage")) {
                        assertError(get(context, report, "?limit=1"), 500, "ANALYTICS_EXECUTION_FAILED");
                        assertError(get(context, report, "?sourceId=absent"), 500, "ANALYTICS_EXECUTION_FAILED");
                        assertError(get(context, report, "?offset=1000000"), 500, "ANALYTICS_EXECUTION_FAILED");
                    }
                }
            }));
        }
        return tests;
    }

    @Test
    void strictHttpParameterValidation() throws Exception {
        try (var context = start(root("standard"), publication("standard"), processing)) {
            for (String query : List.of("?from=2026-02-30", "?from=2026-10-14&to=2026-10-13", "?from=20261013",
                    "?from=2026-10-13T00:00:00Z", "?sourceId=UPPER", "?sourceId=", "?referenceStatus=matched",
                    "?publisherCategory=unknown", "?declaredLanguage=EN", "?countryCode=ca", "?limit=0",
                    "?limit=1001", "?limit=01", "?offset=-1", "?offset=1000001", "?limit=1&limit=2",
                    "?sourceId=alpha&sourceId=beta", "?from=2026-10-12&from=2026-10-13",
                    "?publication=" + publication("standard"), "?report=reference_coverage", "?sql=SELECT")) {
                assertError(get(context, query), 400, "INVALID_ARGUMENT");
            }
        }
    }

    @Test
    void newReportsRejectUnsupportedHttpFiltersWithoutFallback() throws Exception {
        try (var context = start(root("standard"), publication("standard"), processing)) {
            for (String report : List.of("source_discovery_daily", "reference_coverage"))
                for (String query : List.of("?sourceId=", "?sourceId=alpha&sourceId=beta", "?limit=1&limit=2", "?offset=1000001",
                        "?referenceStatus=MATCHED", "?publisherCategory=science", "?declaredLanguage=en", "?countryCode=CA",
                        "?publication=" + publication("standard"), "?root=/private", "?report=catalog", "?sql=SELECT"))
                    assertError(get(context, report, query), 400, "INVALID_ARGUMENT");
            for (String query : List.of("?from=2026-10-12", "?to=2026-10-13"))
                assertError(get(context, "reference_coverage", query), 400, "INVALID_ARGUMENT");
            for (String query : List.of("?from=2026-02-30", "?from=2026-10-13&to=2026-10-12", "?from=0000-01-01"))
                assertError(get(context, "source_discovery_daily", query), 400, "INVALID_ARGUMENT");
        }
    }

    @Test
    void missingConfiguredPublicationAndRuntimeFailuresNeverReturnRows() throws Exception {
        try (var context = start(root("standard"), "", processing)) {
            for (String report : List.of("catalog", "source_discovery_daily", "reference_coverage"))
                assertError(get(context, report, ""), 503, "PUBLICATION_UNAVAILABLE");
        }
        try (var context = start(root("standard"), "0".repeat(64), processing)) {
            for (String report : List.of("catalog", "source_discovery_daily", "reference_coverage"))
                assertError(get(context, report, ""), 500, "ANALYTICS_EXECUTION_FAILED");
        }
        try (var context = start(root("standard"), publication("standard"), fixtures.resolve("missing native dependency"))) {
            for (String report : List.of("catalog", "source_discovery_daily", "reference_coverage"))
                assertError(get(context, report, ""), 500, "ANALYTICS_EXECUTION_FAILED");
        }
        try (var context = start(root("standard"), publication("standard"), fixtures.resolve("missing processing directory"))) {
            for (String report : List.of("catalog", "source_discovery_daily", "reference_coverage"))
                assertError(get(context, report, ""), 503, "ANALYTICS_RUNTIME_UNAVAILABLE");
        }
    }

    private static void assertError(HttpResponse<String> response, int status, String code) {
        assertEquals(status, response.statusCode(), response.body());
        assertEquals(JSON.createObjectNode().put("code", code), JSON.readTree(response.body()));
    }
}
