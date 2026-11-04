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

/** Actual loopback HTTP -> Spring -> bounded child -> unchanged native Reliability CLI. */
class ReliabilityApiIT {
    private static final JsonMapper JSON = JsonMapper.builder().build();
    private static final String START = "2026-10-01T00:00:00.000000Z";
    private static final String END = "2026-10-01T00:10:00.000000Z";
    private static final String WINDOW = "?start=" + START + "&end=" + END;
    private static final List<String> ROW_FIELDS = List.of("window_start", "service_id", "environment",
            "request_count", "server_error_count", "server_error_rate", "duration_sum_ms", "mean_duration_ms",
            "p95_duration_ms_approx", "service_name", "owner_team", "service_tier", "reference_status");
    private static Path fixtures;
    private static Path processing;
    private static String python;
    private static JsonNode reference;
    private static Map<String, String> baseline;

    @BeforeAll
    static void load() throws Exception {
        fixtures = Path.of(System.getProperty("reliability.fixture")).toAbsolutePath().normalize();
        assertTrue(Files.isRegularFile(fixtures.resolve("reference.json")),
                "Generate Spark-free fixtures with services/content-api/tests/generate_reliability_fixtures.py first");
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
                "--reliability.analytics.python-executable=" + python,
                "--reliability.analytics.processing-root=" + code,
                "--reliability.analytics.root=" + root.resolve("output"),
                "--reliability.analytics.batch-root=" + root.resolve("batches"),
                "--reliability.analytics.reference-root=" + root.resolve("references"),
                "--reliability.analytics.publication=" + publication);
    }

    private static HttpResponse<String> get(ConfigurableApplicationContext context, String query) throws Exception {
        int port = ((WebServerApplicationContext) context).getWebServer().getPort();
        try (var client = HttpClient.newHttpClient()) {
            return client.send(HttpRequest.newBuilder(URI.create("http://127.0.0.1:" + port
                            + "/api/v1/reliability/request-windows" + query)).timeout(Duration.ofSeconds(40))
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
            fields.add(camel(field.getKey()) + "=" + URLEncoder.encode(field.getValue().asString(), StandardCharsets.UTF_8));
        }
        return "?" + String.join("&", fields);
    }

    private static JsonNode restEnvelope(JsonNode nativeEnvelope) {
        var rest = JSON.createObjectNode();
        rest.set("publication", nativeEnvelope.get("product_fingerprint"));
        for (String name : List.of("product_id", "product_contract_version", "processor_revision", "input_format", "origin", "label")) {
            rest.set(camel(name), nativeEnvelope.get(name));
        }
        var ref = rest.putObject("reference");
        for (String name : List.of("reference_id", "revision", "snapshot_fingerprint")) {
            ref.set(camel(name), nativeEnvelope.path("reference").get(name));
        }
        var filters = rest.putObject("filters");
        for (String name : List.of("service_id", "environment", "start", "end")) {
            filters.set(camel(name), nativeEnvelope.path("filters").get(name));
        }
        var rows = rest.putArray("data");
        for (JsonNode nativeRow : nativeEnvelope.path("rows")) {
            var row = rows.addObject();
            for (String name : ROW_FIELDS) row.set(camel(name), nativeRow.get(name));
        }
        rest.putObject("meta").put("returned", rows.size())
                .put("limit", Integer.parseInt(nativeEnvelope.path("limit").asString()))
                .put("offset", Integer.parseInt(nativeEnvelope.path("offset").asString()))
                .set("hasMore", nativeEnvelope.get("has_more"));
        return rest;
    }

    @TestFactory
    List<DynamicTest> everyFieldAndTypeMatchesNativeCliForBothFormats() {
        List<DynamicTest> tests = new ArrayList<>();
        for (String format : List.of("JSONL", "PARQUET")) {
            tests.add(DynamicTest.dynamicTest("Native HTTP parity, filters, pages and pre-epoch UTC: " + format, () -> {
                try (var context = start(root(format), publication(format), processing)) {
                    int seen = 0;
                    for (JsonNode scenario : reference.path("scenarios")) {
                        if (!format.equals(scenario.path("input_format").asString())) continue;
                        var response = get(context, query(scenario.path("options")));
                        assertEquals(200, response.statusCode(), scenario.path("name").asString() + ": " + response.body());
                        JsonNode actual = JSON.readTree(response.body());
                        assertEquals(restEnvelope(scenario.path("envelope")), actual, scenario.path("name").asString());
                        for (JsonNode row : actual.path("data")) {
                            for (String metric : List.of("requestCount", "serverErrorCount", "durationSumMs", "p95DurationMsApprox")) {
                                assertTrue(row.get(metric).isString(), metric);
                            }
                            assertTrue(row.get("serverErrorRate").isNumber());
                            assertTrue(row.get("meanDurationMs").isNumber());
                        }
                        assertTrue(actual.path("productContractVersion").isIntegralNumber());
                        assertTrue(actual.path("processorRevision").isIntegralNumber());
                        for (String number : List.of("returned", "limit", "offset")) assertTrue(actual.path("meta").path(number).isIntegralNumber());
                        assertTrue(actual.path("meta").path("hasMore").isBoolean());
                        seen++;
                    }
                    assertEquals(17, seen, "Do not silently omit a parity scenario");
                }
            }));
        }
        return tests;
    }

    @TestFactory
    List<DynamicTest> corruptedPublicationsAndMissingDependenciesFailClosed() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode failure : reference.path("failures")) {
            String name = failure.path("input_format").asString() + "/" + failure.path("case").asString();
            tests.add(DynamicTest.dynamicTest("Safe native failure: " + name, () -> {
                try (var context = start(fixtures.resolve(failure.path("root").asString()), failure.path("publication").asString(), processing)) {
                    assertError(get(context, WINDOW), 500, "ANALYTICS_EXECUTION_FAILED");
                    // An empty filter cannot bypass the full native publication/dependency verifier.
                    assertError(get(context, WINDOW + "&serviceId=absent-service"), 500, "ANALYTICS_EXECUTION_FAILED");
                }
            }));
        }
        return tests;
    }

    @Test
    void requiredAlignedBoundedWindowAndHttpParameterValidation() throws Exception {
        try (var context = start(root("JSONL"), publication("JSONL"), processing)) {
            for (String query : List.of("", "?start=" + START, "?end=" + END,
                    "?start=" + START + "&end=" + START,
                    "?start=" + END + "&end=" + START,
                    "?start=" + START + "&end=2026-11-01T00:05:00.000000Z",
                    "?start=2026-10-01T00:00:00.000001Z&end=" + END,
                    "?start=2026-10-01T00:01:00.000000Z&end=" + END,
                    "?start=" + START + "&end=2026-10-01T00:10:00Z",
                    WINDOW + "&environment=staging", WINDOW + "&serviceId=UPPER",
                    WINDOW + "&start=" + START, WINDOW + "&end=" + END,
                    WINDOW + "&limit=0", WINDOW + "&offset=1000001",
                    WINDOW + "&publication=" + publication("JSONL"), WINDOW + "&report=input_coverage")) {
                assertError(get(context, query), 400, "INVALID_ARGUMENT");
            }
        }
    }

    @Test
    void missingConfiguredPublicationAndRuntimeFailuresNeverReturnRows() throws Exception {
        try (var context = start(root("JSONL"), "", processing)) {
            assertError(get(context, WINDOW), 503, "PUBLICATION_UNAVAILABLE");
        }
        try (var context = start(root("JSONL"), "0".repeat(64), processing)) {
            assertError(get(context, WINDOW), 500, "ANALYTICS_EXECUTION_FAILED");
        }
        try (var context = start(root("JSONL"), publication("JSONL"), fixtures.resolve("missing native dependency"))) {
            assertError(get(context, WINDOW), 500, "ANALYTICS_EXECUTION_FAILED");
        }
        try (var context = start(root("JSONL"), publication("JSONL"), fixtures.resolve("missing processing directory"))) {
            assertError(get(context, WINDOW), 503, "ANALYTICS_RUNTIME_UNAVAILABLE");
        }
    }

    private static void assertError(HttpResponse<String> response, int status, String code) {
        assertEquals(status, response.statusCode(), response.body());
        assertEquals(JSON.createObjectNode().put("code", code), JSON.readTree(response.body()));
    }
}
