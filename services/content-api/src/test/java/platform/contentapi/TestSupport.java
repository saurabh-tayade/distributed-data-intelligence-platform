package platform.contentapi;

import java.nio.file.Path;
import java.time.Duration;
import java.nio.charset.StandardCharsets;

final class TestSupport {
    static final String PUBLICATION = "a".repeat(64);
    static final String TIMESTAMP = "2026-10-12T10:20:30.123456Z";
    static AnalyticsProperties properties() { return properties(Duration.ofSeconds(5), 1048576, 65536); }
    static AnalyticsProperties properties(Duration timeout, int stdout, int stderr) {
        return new AnalyticsProperties(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                Path.of("pom.xml"), Path.of("target/gold's snapshots"), PUBLICATION, timeout, 1, stdout, stderr);
    }
    static String row() {
        return """
                {"source_id":"alpha","article_count":9007199254740993,"observation_count":9223372036854775807,
                 "first_observed_at":"2026-10-12T10:20:30.123456Z","last_observed_at":null,
                 "articles_without_published_at":0}
                """;
    }
    static String envelope(String rows) {
        return "{\"status\":\"OK\",\"publication\":\"" + PUBLICATION
                + "\",\"report\":\"publishers\",\"limit\":100,\"rows\":" + rows + "}";
    }
    static BoundedProcessRunner.Result output(int exit, String value) {
        return new BoundedProcessRunner.Result(exit, value.getBytes(StandardCharsets.UTF_8));
    }
    static GoldAnalyticsAdapter adapter() { return new GoldAnalyticsAdapter(properties(), new BoundedProcessRunner(properties())); }
}
