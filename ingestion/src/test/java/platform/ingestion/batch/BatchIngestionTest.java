package platform.ingestion.batch;

import com.sun.net.httpserver.HttpServer;
import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import platform.ingestion.rss.*;
import platform.ingestion.bronze.*;
import static org.junit.jupiter.api.Assertions.*;
import static platform.ingestion.batch.SourceResult.Status.*;

class BatchIngestionTest {
    @TempDir Path temp;

    private List<RssSource> catalog(String json) throws Exception {
        Path path = temp.resolve("sources.json");
        Files.writeString(path, json);
        return SourceCatalog.load(path);
    }

    @Test void validCatalogPreservesOrderAndDisabledState() throws Exception {
        var sources = catalog("""
                [{"sourceId":"a","feedUri":"https://example.com/a","enabled":true},
                 {"sourceId":"b","feedUri":"http://example.com/b","enabled":false}]
                """);
        assertEquals(List.of("a", "b"), sources.stream().map(RssSource::sourceId).toList());
        assertFalse(sources.get(1).enabled());
        assertThrows(UnsupportedOperationException.class, () -> sources.clear());
    }

    @Test void invalidCatalogsFailCompletely() {
        for (String json : List.of("", "{}", "[null]", "[] []",
                "[{\"sourceId\":\"a\",\"feedUri\":\"https://example.com\"}]",
                "[{\"sourceId\":\"a\",\"enabled\":true}]",
                "[{\"feedUri\":\"https://example.com\",\"enabled\":true}]")) {
            assertThrows(Exception.class, () -> catalog(json), json);
        }
        String valid = "{\"sourceId\":\"a\",\"feedUri\":\"https://example.com\",\"enabled\":true}";
        assertThrows(IllegalArgumentException.class, () -> catalog("[" + valid + "," + valid + "]"));
        for (String uri : List.of("relative", "ftp://example.com", "https://u:p@example.com", "https://example.com/?token=x", "https://example.com/#fragment")) {
            assertThrows(IllegalArgumentException.class, () -> catalog("[" + valid.replace("https://example.com", uri) + "]"));
        }
        for (String bad : List.of(valid.replace("true", "\"true\""), valid.replace("true", "null"),
                valid.replace("\"a\"", "\"\""), valid.replace("\"a\"", "\"a\\nforged\""),
                valid.replace("true}", "true,\"extra\":1}"), valid.replace("true}", "true,\"enabled\":false}"))) {
            assertThrows(Exception.class, () -> catalog("[" + bad + "]"));
        }
    }

    @Test void orderedHttpExecutionIsolatesFailuresAndPreservesFixturesAndIdentities() throws Exception {
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        var calls = new ArrayList<String>();
        var bodies = new java.util.HashMap<String, byte[]>();
        for (String fixture : List.of("rss-technology", "rss-business", "atom-science")) {
            try (var stream = getClass().getResourceAsStream("/feeds/" + fixture + ".xml")) {
                bodies.put(fixture, java.util.Objects.requireNonNull(stream).readAllBytes());
            }
        }
        server.createContext("/", exchange -> {
            String id = exchange.getRequestURI().getPath().substring(1);
            calls.add(id);
            byte[] bytes = bodies.getOrDefault(id, new byte[]{1, 2});
            exchange.sendResponseHeaders(id.equals("bad") ? 503 : 200, bytes.length);
            try (var out = exchange.getResponseBody()) { out.write(bytes); }
        });
        server.start();
        try (var client = HttpClient.newHttpClient()) {
            var stored = new ArrayList<RawFeed>();
            var receipts = new ArrayList<BronzeReceipt>();
            BronzeStorage storage = (source, raw) -> {
                stored.add(raw);
                if (source.sourceId().equals("rss-business")) {
                    // Exercise an actual classified storage validation error without mocking constructors.
                    try (var s3 = software.amazon.awssdk.services.s3.S3Client.builder()
                            .region(software.amazon.awssdk.regions.Region.US_EAST_1)
                            .credentialsProvider(software.amazon.awssdk.auth.credentials.AnonymousCredentialsProvider.create())
                            .build()) {
                        return new S3BronzeStorage(s3, "test").write(new RssSource("mismatch", source.feedUri(), true), raw);
                    }
                }
                var keys = BronzeKeys.forFeed(raw);
                var receipt = new BronzeReceipt("test", keys.payload(), keys.metadata(), raw.ingestionId(),
                        raw.body().length, "0".repeat(64));
                receipts.add(receipt);
                return receipt;
            };
            var sources = new ArrayList<RssSource>();
            for (String id : List.of("rss-technology", "disabled", "bad", "rss-business", "atom-science")) {
                sources.add(new RssSource(id, URI.create("http://127.0.0.1:" + server.getAddress().getPort() + "/" + id), !id.equals("disabled")));
            }
            var runner = new BatchIngestionRunner(new HttpFeedFetcher(client, Duration.ofSeconds(2), Clock.systemUTC()), storage);
            var first = runner.run(sources);
            assertEquals(List.of(SUCCESS, SKIPPED, FETCH_FAILED, STORAGE_FAILED, SUCCESS), first.sources().stream().map(SourceResult::status).toList());
            assertEquals(List.of("rss-technology", "bad", "rss-business", "atom-science"), calls);
            assertEquals(3, stored.size());
            assertEquals(2, first.succeeded()); assertEquals(2, first.failed()); assertEquals(1, first.skipped());
            assertEquals(1, first.exitCode()); assertTrue(first.durationMs() >= 0);
            assertEquals("HTTP_503", first.sources().get(2).failureClassification());
            assertEquals("VALIDATION_VALIDATE", first.sources().get(3).failureClassification());
            assertEquals(stored.get(1).ingestionId(), first.sources().get(3).ingestionId());
            for (var raw : stored) assertArrayEquals(bodies.get(raw.sourceId()), raw.body());
            assertSame(receipts.getFirst(), first.sources().getFirst().receipt());
            assertEquals(stored.getFirst().ingestionId(), first.sources().getFirst().ingestionId());
            assertEquals((long) bodies.get("rss-technology").length, first.sources().getFirst().payloadByteLength());
            var second = runner.run(List.of(sources.getFirst(), sources.getLast()));
            assertEquals(0, second.exitCode()); assertEquals(2, second.succeeded());
            assertNotEquals(first.sources().getFirst().ingestionId(), second.sources().getFirst().ingestionId());
            var output = new ByteArrayOutputStream();
            BatchIngestionCli.print(first, new PrintStream(output));
            assertTrue(output.toString().contains("Succeeded: 2"));
            assertFalse(output.toString().contains("Café"));
        } finally { server.stop(0); }
    }

    @Test void emptyDisabledAndUnexpectedFailuresHaveCorrectExitSemantics() {
        var runner = new BatchIngestionRunner(source -> { throw new IllegalStateException("secret"); },
                (source, raw) -> { fail("Must not write"); return null; });
        var disabled = new RssSource("disabled", URI.create("https://example.com"), false);
        assertEquals(0, runner.run(List.of()).exitCode());
        assertEquals(0, runner.run(List.of(disabled)).exitCode());
        var result = runner.run(List.of(new RssSource("a", disabled.feedUri(), true), disabled));
        assertEquals(1, result.exitCode()); assertEquals("UNEXPECTED", result.sources().getFirst().failureClassification());
        assertNull(result.sources().getFirst().ingestionId());
        assertThrows(IllegalArgumentException.class, () -> runner.run(List.of(disabled, disabled)));
    }

    @Test void cliRejectsInvalidCatalogBeforeInitializingStorageAndReturnsTwo() throws Exception {
        Path file = temp.resolve("bad.json"); Files.writeString(file, "{}");
        var output = new ByteArrayOutputStream();
        assertEquals(2, BatchIngestionCli.execute(new String[]{file.toString()}, Map.of(), new PrintStream(output)));
        assertTrue(output.toString().contains("at catalog"));
        assertEquals(2, BatchIngestionCli.execute(new String[0], Map.of(), new PrintStream(output)));
    }
}
