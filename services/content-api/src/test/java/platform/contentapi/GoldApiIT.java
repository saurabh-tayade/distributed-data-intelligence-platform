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

    @BeforeAll
    static void load() throws Exception {
        fixtures = Path.of(System.getProperty("gold.fixture")).toAbsolutePath();
        assertTrue(Files.isRegularFile(fixtures.resolve("reference.json")), "Generate M6.1 synthetic fixtures first");
        reference = JSON.readTree(Files.readString(fixtures.resolve("reference.json")));
        script = Path.of("../../processing/gold_analytics.py").toAbsolutePath().normalize();
        python = System.getProperty("analytics.python");
        assertNotNull(python, "Set -Danalytics.python to the analytics Python executable");
    }

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
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        try (var client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port + "/api/v1/publishers" + query))
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

    @TestFactory
    List<DynamicTest> everyDamagedFixtureIsRejectedIncludingJdbcGaps() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode failure : reference.path("failures")) {
            String name = failure.path("case").asString();
            tests.add(DynamicTest.dynamicTest("HTTP rejection: " + name, () -> {
                try (var context = start(fixtures.resolve("cases").resolve(name), identity("full"))) {
                    var response = get(context, "");
                    assertEquals(503, response.statusCode());
                    assertEquals(JSON.readTree("{\"code\":\"PUBLICATION_UNAVAILABLE\"}"), JSON.readTree(response.body()));
                }
            }));
        }
        return tests;
    }

    @Test
    void missingPublicationNeverReturnsSuccess() throws Exception {
        try (var context = start(fixtures.resolve("gold's snapshots"), "0".repeat(64))) {
            var response = get(context, "");
            assertEquals(503, response.statusCode());
            assertEquals("PUBLICATION_UNAVAILABLE", JSON.readTree(response.body()).path("code").asString());
        }
    }
}
