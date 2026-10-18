package platform.contentapi;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.boot.web.server.context.WebServerApplicationContext;
import org.springframework.context.ConfigurableApplicationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.json.JsonMapper;
import static org.junit.jupiter.api.Assertions.*;

/** Real HTTP -> Spring -> ProcessBuilder -> Python/DuckDB, using M6.1 fixtures without Spark/Silver. */
class GoldApiIT {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static Path fixtures;
    private static JsonNode reference;
    private static Path script;
    private static String python;
    private static Map<String, String> baseline;

    @BeforeAll
    static void load() throws Exception {
        fixtures = Path.of(System.getProperty("gold.fixture")).toAbsolutePath();
        assertTrue(Files.isRegularFile(fixtures.resolve("reference.json")), "Generate M6.1 synthetic fixtures first");
        reference = JSON.readTree(Files.readString(fixtures.resolve("reference.json")));
        script = Path.of("../../processing/gold_analytics.py").toAbsolutePath().normalize();
        python = System.getProperty("analytics.python");
        assertNotNull(python, "Set -Danalytics.python to the analytics Python executable");
        baseline = inventory();
    }

    private static Map<String, String> inventory() throws Exception {
        var values = new java.util.TreeMap<String, String>();
        Path root = fixtures.resolve("gold's snapshots");
        try (var files = Files.walk(root)) {
            for (Path file : files.filter(Files::isRegularFile).toList()) {
                String hash = java.util.HexFormat.of().formatHex(java.security.MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(file)));
                values.put(root.relativize(file).toString(), hash + ":" + Files.size(file) + ":" + Files.getLastModifiedTime(file));
            }
        }
        return values;
    }

    @AfterAll
    static void publicationsRemainUnchanged() throws Exception { assertEquals(baseline, inventory()); }

    private static String identity(String name) {
        return reference.path("publications").path(name).asString().split("input_set=")[1];
    }

    private static ConfigurableApplicationContext start(Path root, String identity) {
        return new SpringApplicationBuilder(ContentApiApplication.class).run(
                "--server.port=0", "--server.address=127.0.0.1", "--spring.main.banner-mode=off",
                "--logging.level.root=WARN", "--analytics.python-executable=" + python,
                "--analytics.script=" + script, "--analytics.gold-root=" + root,
                "--analytics.publication=" + identity);
    }

    private static HttpResponse<String> get(ConfigurableApplicationContext context, String query) throws Exception {
        return get(context, "publishers", query);
    }

    private static HttpResponse<String> get(ConfigurableApplicationContext context, String report, String query) throws Exception {
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        try (var client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/v1/" + report + query))
                    .timeout(Duration.ofSeconds(40)).GET().build(), HttpResponse.BodyHandlers.ofString());
        }
    }

    @TestFactory
    List<DynamicTest> syntheticReferenceParity() {
        List<DynamicTest> tests = new ArrayList<>();
        for (String snapshot : List.of("full", "first", "empty")) {
            tests.add(DynamicTest.dynamicTest("HTTP publisher parity: " + snapshot, () -> {
                try (var context = start(fixtures.resolve("gold's snapshots"), identity(snapshot))) {
                    for (JsonNode scenario : reference.path("scenarios")) {
                        if (!snapshot.equals(scenario.path("snapshot").asString())
                                || !"publishers".equals(scenario.path("report").asString())) continue;
                        JsonNode options = scenario.path("options");
                        int limit = options.path("limit").asInt(100);
                        String source = options.has("source") ? options.get("source").asString() : null;
                        String query = "?limit=" + limit + (source == null ? "" : "&sourceId=" + URLEncoder.encode(source, StandardCharsets.UTF_8));
                        var response = get(context, query);
                        if (source != null && !source.matches("[a-z0-9][a-z0-9_-]{0,127}")) {
                            assertEquals(400, response.statusCode());
                            continue;
                        }
                        assertEquals(200, response.statusCode(), response.body());
                        JsonNode actual = JSON.readTree(response.body());
                        assertEquals(identity(snapshot), actual.path("publication").asString());
                        assertEquals(limit, actual.path("meta").path("limit").asInt());
                        assertEquals(scenario.path("rows").size(), actual.path("meta").path("returned").asInt());
                        assertEquals(scenario.path("rows").size(), actual.path("data").size());
                        int index = 0;
                        for (JsonNode expected : scenario.path("rows")) {
                            JsonNode row = actual.path("data").get(index++);
                            for (var field : Map.of("source_id", "sourceId", "article_count", "articleCount",
                                    "observation_count", "observationCount", "first_observed_at", "firstObservedAt",
                                    "last_observed_at", "lastObservedAt", "articles_without_published_at", "articlesWithoutPublishedAt").entrySet()) {
                                if (expected.get(field.getKey()).isNull()) assertTrue(row.get(field.getValue()).isNull());
                                else assertEquals(expected.get(field.getKey()).asString(), row.get(field.getValue()).asString());
                                if (field.getKey().contains("count") || field.getKey().equals("articles_without_published_at")) {
                                    assertTrue(row.get(field.getValue()).isString());
                                }
                            }
                        }
                    }
                    var missingSource = get(context, "?sourceId=unknown-source");
                    assertEquals(200, missingSource.statusCode());
                    assertEquals(0, JSON.readTree(missingSource.body()).path("data").size());
                }
            }));
        }
        return tests;
    }

    private static final Map<String, List<String>> FIELDS = Map.of(
            "articles", List.of("article_id", "source_id", "title", "url", "summary", "published_at", "updated_at",
                    "first_observed_at", "last_observed_at", "observation_count", "distinct_ingestion_count", "quality_warnings"),
            "daily", List.of("source_id", "ingestion_date", "observation_count", "distinct_article_count",
                    "distinct_ingestion_count", "quality_warning_count", "latest_ingested_at"),
            "quality", List.of("source_id", "observation_count", "quality_warning_count", "warning_codes_per_observation"));

    private static String camel(String snake) {
        StringBuilder value = new StringBuilder();
        boolean upper = false;
        for (char c : snake.toCharArray()) {
            if (c == '_') upper = true;
            else { value.append(upper ? Character.toUpperCase(c) : c); upper = false; }
        }
        return value.toString();
    }

    private static JsonNode restRows(String report, JsonNode pythonRows) {
        var result = JSON.createArrayNode();
        for (JsonNode row : pythonRows) {
            var mapped = result.addObject();
            for (String field : FIELDS.get(report)) {
                JsonNode value = row.get(field);
                if (field.endsWith("_count")) mapped.put(camel(field), value.asString());
                else mapped.set(camel(field), value);
            }
        }
        return result;
    }

    @TestFactory
    List<DynamicTest> newReportParityAndArticlePages() {
        List<DynamicTest> tests = new ArrayList<>();
        for (String snapshot : List.of("full", "first", "empty")) {
            tests.add(DynamicTest.dynamicTest("New HTTP reports and pagination: " + snapshot, () -> {
                try (var context = start(fixtures.resolve("gold's snapshots"), identity(snapshot))) {
                    for (JsonNode scenario : reference.path("scenarios")) {
                        String report = scenario.path("report").asString();
                        if (!snapshot.equals(scenario.path("snapshot").asString()) || report.equals("publishers")) continue;
                        JsonNode options = scenario.path("options");
                        int limit = options.path("limit").asInt(100);
                        String source = options.has("source") ? options.get("source").asString() : null;
                        String query = "?limit=" + limit + (source == null ? "" : "&sourceId=" + URLEncoder.encode(source, StandardCharsets.UTF_8));
                        if (options.has("start")) query += "&from=" + options.get("start").asString();
                        if (options.has("end")) query += "&to=" + options.get("end").asString();
                        var response = get(context, report, query);
                        if (source != null && !source.matches("[a-z0-9][a-z0-9_-]{0,127}")) {
                            assertEquals(400, response.statusCode());
                            continue;
                        }
                        assertEquals(200, response.statusCode(), response.body());
                        JsonNode actual = JSON.readTree(response.body());
                        assertEquals(identity(snapshot), actual.path("publication").asString());
                        assertEquals(limit, actual.path("meta").path("limit").asInt());
                        assertEquals(scenario.path("rows").size(), actual.path("meta").path("returned").asInt());
                        assertEquals(restRows(report, scenario.path("rows")), actual.path("data"));
                    }
                    // Compare every visible page to the original unpaged Python reference, including ties.
                    for (String source : List.of("", "alpha", "absent-source")) {
                        var expected = JSON.createArrayNode();
                        for (JsonNode scenario : reference.path("scenarios")) {
                            if (snapshot.equals(scenario.path("snapshot").asString()) && "articles".equals(scenario.path("report").asString())
                                    && scenario.path("options").isEmpty()) {
                                for (JsonNode row : restRows("articles", scenario.path("rows"))) {
                                    if (source.isEmpty() || source.equals(row.path("sourceId").asString())) expected.add(row);
                                }
                            }
                        }
                        var collected = JSON.createArrayNode();
                        for (int offset = 0; offset <= expected.size() + 1; offset++) {
                            var response = get(context, "articles", "?limit=1&offset=" + offset + (source.isEmpty() ? "" : "&sourceId=" + source));
                            assertEquals(200, response.statusCode(), response.body());
                            JsonNode page = JSON.readTree(response.body());
                            assertEquals(identity(snapshot), page.path("publication").asString());
                            assertEquals(offset, page.path("meta").path("offset").asInt());
                            assertEquals(1, page.path("meta").path("limit").asInt());
                            assertEquals(offset + 1 < expected.size(), page.path("meta").path("hasMore").booleanValue());
                            assertEquals(offset < expected.size() ? 1 : 0, page.path("meta").path("returned").asInt());
                            for (JsonNode row : page.path("data")) { assertEquals(expected.get(offset), row); collected.add(row); }
                        }
                        assertEquals(expected, collected);
                    }
                    for (String report : List.of("articles", "daily", "quality")) {
                        var response = get(context, report, "?sourceId=absent-source");
                        assertEquals(200, response.statusCode());
                        assertTrue(JSON.readTree(response.body()).path("data").isEmpty());
                    }
                    var outside = get(context, "articles", "?offset=1000000");
                    assertEquals(200, outside.statusCode());
                    assertFalse(JSON.readTree(outside.body()).path("meta").path("hasMore").booleanValue());
                }
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> everyDamagedFixtureIsRejectedIncludingJdbcGaps() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode failure : reference.path("failures")) {
            String name = failure.path("case").asString();
            tests.add(DynamicTest.dynamicTest("HTTP rejection: " + name, () -> {
                try (var context = start(fixtures.resolve("cases").resolve(name), identity("full"))) {
                    for (String report : List.of("publishers", "articles", "daily", "quality")) {
                        var response = get(context, report, "");
                        assertEquals(503, response.statusCode());
                        assertEquals(JSON.readTree("{\"code\":\"PUBLICATION_UNAVAILABLE\"}"), JSON.readTree(response.body()));
                    }
                }
            }));
        }
        return tests;
    }

    @Test
    void missingPublicationNeverReturnsSuccess() throws Exception {
        try (var context = start(fixtures.resolve("gold's snapshots"), "0".repeat(64))) {
            for (String report : List.of("publishers", "articles", "daily", "quality")) {
                var response = get(context, report, "");
                assertEquals(503, response.statusCode());
                assertEquals("PUBLICATION_UNAVAILABLE", JSON.readTree(response.body()).path("code").asString());
            }
        }
    }
}
