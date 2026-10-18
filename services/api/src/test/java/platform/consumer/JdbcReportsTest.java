package platform.consumer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class JdbcReportsTest {
    private static final ObjectMapper JSON = new ObjectMapper();
    private static Path fixtures;
    private static Path root;
    private static JsonNode reference;
    private static String full;

    @BeforeAll
    static void load() throws Exception {
        fixtures = Path.of(System.getProperty("gold.fixture"));
        assertTrue(Files.isRegularFile(fixtures.resolve("reference.json")),
                "Generate synthetic fixtures first; see docs/architecture/consumer-integration.md");
        root = fixtures.resolve("gold's snapshots");
        reference = JSON.readTree(fixtures.resolve("reference.json").toFile());
        full = identity("full");
    }

    static String identity(String name) { return reference.path("publications").path(name).asText().split("input_set=")[1]; }
    static String optional(JsonNode node, String key) { return node.has(key) ? node.get(key).asText() : null; }
    static JsonNode normalized(Object rows) throws Exception {
        // Round-trip JSON compares numerical values independently of Jackson's Long/BigInteger node choices.
        return JSON.readTree(JSON.writeValueAsBytes(rows));
    }

    @TestFactory
    Stream<DynamicTest> pythonReportParity() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode scenario : reference.path("scenarios")) {
            tests.add(DynamicTest.dynamicTest(scenario.path("snapshot") + " " + scenario.path("report")
                    + " " + scenario.path("options"), () -> {
                JsonNode options = scenario.path("options");
                try (var reports = JdbcReports.open(root, identity(scenario.path("snapshot").asText()))) {
                    assertEquals(scenario.path("rows"), normalized(reports.report(scenario.path("report").asText(),
                            optional(options, "source"), optional(options, "start"), optional(options, "end"),
                            options.path("limit").asInt(100))));
                }
            }));
        }
        return tests.stream();
    }

    @TestFactory
    Stream<DynamicTest> protectionAndExplicitGaps() {
        List<DynamicTest> tests = new ArrayList<>();
        for (JsonNode failure : reference.path("failures")) {
            String name = failure.path("case").asText();
            String code = optional(failure, "java_code");
            boolean gap = failure.path("java_code").isNull();
            tests.add(DynamicTest.dynamicTest((gap ? "KNOWN JAVA GAP: " : "Java rejection: ") + name, () -> {
                assertFalse(failure.path("python_code").asText().isEmpty(), "Python must reject every damaged copy");
                if (gap) {
                    // Passing this assertion documents missing protection, NOT verified publication acceptance.
                    try (var reports = JdbcReports.open(fixtures.resolve("cases").resolve(name), full)) {
                        assertFalse(reports.report("articles", null, null, null, 100).isEmpty());
                    }
                } else {
                    PocFailure error = assertThrows(PocFailure.class, () -> {
                        try (var reports = JdbcReports.open(fixtures.resolve("cases").resolve(name), full)) {
                            reports.report("articles", null, null, null, 100);
                        }
                    });
                    assertEquals(code, error.getMessage());
                }
            }));
        }
        return tests.stream();
    }

    @Test
    void utcMicrosecondsNullsAndArraysUnderNonUtcJvm() throws Exception {
        TimeZone previous = TimeZone.getDefault();
        try {
            TimeZone.setDefault(TimeZone.getTimeZone("Pacific/Honolulu"));
            try (var reports = JdbcReports.open(root, full)) {
                var articles = reports.report("articles", "alpha", null, null, 100);
                var selected = articles.getFirst();
                assertEquals("2026-10-13T00:00:00.000001Z", selected.get("selected_ingested_at"));
                assertNull(selected.get("published_at"));
                assertNull(selected.get("updated_at"));
                assertEquals(List.of("INVALID_PUBLISHED_AT", "INVALID_UPDATED_AT"), selected.get("quality_warnings"));
                assertEquals(List.of(), articles.get(1).get("quality_warnings"));
                assertInstanceOf(Long.class, selected.get("observation_count"));
                assertInstanceOf(Integer.class, selected.get("selected_entry_index"));
                assertEquals(2.0, reports.report("quality", "alpha", "2026-10-13", null, 100)
                        .getFirst().get("warning_codes_per_observation"));
                assertEquals("2026-10-12", reports.report("daily", "alpha", null, null, 100)
                        .getFirst().get("ingestion_date"));
            }
            try (var reports = JdbcReports.open(root, identity("first"))) {
                var rows = reports.report("articles", "alpha", null, null, 100);
                assertTrue(rows.stream().anyMatch(r -> "2026-10-10T00:00:00.000000Z".equals(r.get("published_at"))));
            }
        } finally { TimeZone.setDefault(previous); }
    }

    @Test
    void jdbcCanInspectEveryPhysicalSchemaButPocDoesNotEnforceIt() throws Exception {
        PocPublication publication = PocPublication.select(root, full);
        try (var connection = DriverManager.getConnection("jdbc:duckdb:")) {
            assertEquals("v1.4.5", connection.getMetaData().getDatabaseProductVersion());
            try (var statement = connection.createStatement();
                 var result = statement.executeQuery("SELECT 9223372036854775808::HUGEINT AS exact_count")) {
                assertTrue(result.next());
                assertEquals(new java.math.BigInteger("9223372036854775808"), result.getObject(1));
            }
            for (var table : publication.files().entrySet()) {
                for (String path : table.getValue()) {
                    try (var statement = connection.prepareStatement(
                            "DESCRIBE SELECT * FROM read_parquet(?, hive_partitioning=false, union_by_name=false)")) {
                        statement.setString(1, path);
                        try (var result = statement.executeQuery()) {
                            List<List<String>> schema = new ArrayList<>();
                            while (result.next()) { schema.add(List.of(result.getString(1), result.getString(2))); }
                            assertEquals(reference.path("schemas").path(table.getKey()), normalized(schema));
                        }
                    }
                }
            }
        }
    }

    @Test
    void resourcesCloseAfterSuccessAndQueryFailure() throws Exception {
        var reports = JdbcReports.open(root, full);
        try (reports) {
            for (int i = 0; i < 20; i++) {
                assertEquals(2, reports.report("publishers", null, null, null, 100).size());
            }
            assertFalse(reports.isClosed());
        }
        assertTrue(reports.isClosed());
        reports.close();
        assertEquals("QUERY_FAILED", assertThrows(PocFailure.class,
                () -> reports.report("publishers", null, null, null, 100)).getMessage());
        var bad = JdbcReports.open(fixtures.resolve("cases/corrupt-parquet"), full);
        try (bad) {
            assertEquals("QUERY_FAILED", assertThrows(PocFailure.class,
                    () -> bad.report("articles", null, null, null, 100)).getMessage());
        }
        assertTrue(bad.isClosed());
    }

    @Test
    void invalidArgumentsAndSelections(@TempDir Path temporary) throws Exception {
        for (String invalid : List.of("../outside", "*", "a".repeat(63), "A".repeat(64),
                "v2/processor_revision=1/input_set=" + full)) {
            assertEquals("INVALID_SELECTION", assertThrows(PocFailure.class,
                    () -> JdbcReports.open(root, invalid)).getMessage());
        }
        try (var reports = JdbcReports.open(root, full)) {
            for (int limit : List.of(-1, 0, 1001)) {
                assertEquals("INVALID_ARGUMENT", assertThrows(PocFailure.class,
                        () -> reports.report("articles", null, null, null, limit)).getMessage());
            }
            assertThrows(PocFailure.class, () -> reports.report("DROP TABLE data", null, null, null, 100));
            assertThrows(PocFailure.class, () -> reports.report("articles", " ", null, null, 100));
            assertThrows(PocFailure.class, () -> reports.report("articles", null, "2026-10-13", null, 100));
            assertThrows(PocFailure.class, () -> reports.report("daily", null, "2026-02-30", null, 100));
            assertThrows(PocFailure.class, () -> reports.report("daily", null, "2026-10-14", "2026-10-13", 100));
        }
        Path copied = copyPublication(temporary);
        Path markerPath = copied.resolve("complete.json");
        JsonNode marker = JSON.readTree(markerPath.toFile());
        var item = (com.fasterxml.jackson.databind.node.ObjectNode) marker.path("files").path("gold_articles").get(0);
        String unsafe = "gold_articles/part*\n\n.parquet";
        Files.copy(copied.resolve(item.path("path").asText()), copied.resolve(unsafe));
        item.put("path", unsafe);
        JSON.writeValue(markerPath.toFile(), marker);
        assertEquals("INVALID_LAYOUT", assertThrows(PocFailure.class,
                () -> JdbcReports.open(temporary, full)).getMessage());
    }

    @Test
    void cliLabelsSuccessUnverifiedAndUsesSafeErrors() throws Exception {
        var bytes = new ByteArrayOutputStream();
        try (var output = new PrintStream(bytes, true, StandardCharsets.UTF_8)) {
            assertEquals(0, Main.run(new String[]{"--gold-root", root.toString(), "--publication", full,
                    "--report", "publishers"}, output));
            assertEquals("UNVERIFIED_POC", JSON.readTree(bytes.toByteArray()).path("status").asText());
            for (String[] arguments : List.of(new String[]{"--unknown", "secret"},
                    new String[]{"--gold-root"}, new String[]{"--gold-root", root.toString(),
                    "--publication", full, "--report", "daily", "--limit", "secret"})) {
                bytes.reset();
                assertEquals(2, Main.run(arguments, output));
                assertEquals("{\"status\":\"FAILED\",\"code\":\"INVALID_ARGUMENT\"}", bytes.toString(StandardCharsets.UTF_8).trim());
            }
            bytes.reset();
            assertEquals(1, Main.run(new String[]{"--gold-root", fixtures.resolve("cases/corrupt-parquet").toString(),
                    "--publication", full, "--report", "articles"}, output));
            assertEquals("{\"status\":\"FAILED\",\"code\":\"QUERY_FAILED\"}", bytes.toString(StandardCharsets.UTF_8).trim());
        }
    }

    @Test
    void queriesLeavePublicationFilesUnchanged() throws Exception {
        Map<String, String> before = inventory(root);
        try (var reports = JdbcReports.open(root, full)) {
            for (String report : List.of("publishers", "articles", "daily", "quality")) {
                reports.report(report, null, null, null, 100);
            }
        }
        assertEquals(before, inventory(root));
    }

    @Test
    void knownGapChangesAfterOpenAreNotDetected(@TempDir Path temporary) throws Exception {
        Path copied = copyPublication(temporary);
        try (var reports = JdbcReports.open(temporary, full)) {
            var before = reports.report("publishers", null, null, null, 100);
            Files.writeString(copied.resolve("inputs.json"), "tampered");
            assertEquals(before, reports.report("publishers", null, null, null, 100));
        }
    }

    private static Path copyPublication(Path temporary) throws Exception {
        Path original = root.resolve(reference.path("publications").path("full").asText());
        Path copied = temporary.resolve(reference.path("publications").path("full").asText());
        try (var paths = Files.walk(original)) {
            for (Path path : paths.toList()) {
                Path destination = copied.resolve(original.relativize(path));
                if (Files.isDirectory(path)) { Files.createDirectories(destination); }
                else { Files.copy(path, destination); }
            }
        }
        return copied;
    }

    private static Map<String, String> inventory(Path directory) throws Exception {
        Map<String, String> inventory = new HashMap<>();
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.filter(Files::isRegularFile).toList()) {
                inventory.put(directory.relativize(path).toString(),
                        HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(Files.readAllBytes(path)))
                        + ":" + Files.getLastModifiedTime(path));
            }
        }
        return inventory;
    }
}
