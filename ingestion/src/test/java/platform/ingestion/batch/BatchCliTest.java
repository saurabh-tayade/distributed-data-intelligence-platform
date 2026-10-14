package platform.ingestion.batch;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real CLI subprocess plus loopback feed/S3 stub. No Docker, ambient credentials or publishers. */
class BatchCliTest {
    @TempDir Path temp;

    @Test void processExitCodesAndStoredBytesMatchBatchOutcomes() throws Exception {
        byte[] payload;
        try (var input = getClass().getResourceAsStream("/feeds/atom-science.xml")) {
            payload = java.util.Objects.requireNonNull(input).readAllBytes();
        }
        var objects = new ConcurrentHashMap<String, byte[]>();
        var fetches = new AtomicInteger();
        var failWrites = new java.util.concurrent.atomic.AtomicBoolean();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath();
                if (path.equals("/feed") || path.equals("/failed-feed")) {
                    fetches.incrementAndGet();
                    exchange.sendResponseHeaders(path.equals("/feed") ? 200 : 503, payload.length);
                    exchange.getResponseBody().write(payload);
                } else if (exchange.getRequestMethod().equals("HEAD")) {
                    exchange.sendResponseHeaders(200, -1);
                } else if (exchange.getRequestMethod().equals("PUT")) {
                    if (failWrites.get()) {
                        byte[] error = "<Error><Code>AccessDenied</Code></Error>".getBytes(StandardCharsets.UTF_8);
                        exchange.sendResponseHeaders(403, error.length);
                        exchange.getResponseBody().write(error);
                    } else {
                        objects.put(path, exchange.getRequestBody().readAllBytes());
                        exchange.sendResponseHeaders(200, -1);
                    }
                } else {
                    byte[] bytes = objects.get(path);
                    boolean absent = bytes == null;
                    if (absent) bytes = "<Error><Code>NoSuchKey</Code></Error>".getBytes(StandardCharsets.UTF_8);
                    exchange.sendResponseHeaders(absent ? 404 : 200, bytes.length);
                    exchange.getResponseBody().write(bytes);
                }
            } finally { exchange.close(); }
        });
        server.start();
        try {
            String endpoint = "http://127.0.0.1:" + server.getAddress().getPort();
            String valid = "[{\"sourceId\":\"atom\",\"feedUri\":\"" + endpoint + "/feed\",\"enabled\":true}]";
            assertEquals(0, run(valid, endpoint));
            assertEquals(2, objects.size());
            var stored = objects.entrySet().stream().filter(e -> e.getKey().endsWith("payload.xml")).findFirst().orElseThrow();
            assertArrayEquals(payload, stored.getValue());
            assertEquals(0, run(valid, endpoint));
            assertEquals(4, objects.size(), "Repeated retrieval uses new keys");
            assertEquals(0, run(valid.replace("true", "false"), endpoint));
            assertEquals(2, fetches.get(), "Disabled source must not fetch");
            assertEquals(1, run(valid.replace("/feed", "/failed-feed"), endpoint));
            assertEquals(4, objects.size(), "No storage after fetch failure");
            failWrites.set(true);
            assertEquals(1, run(valid, endpoint));
            assertEquals(4, objects.size());
            assertEquals(2, run("{}", endpoint));
            assertEquals(4, fetches.get(), "Invalid catalog must not fetch");
            assertEquals(2, run(valid, "https://s3.amazonaws.com"));
        } finally { server.stop(0); }
    }

    private int run(String catalog, String endpoint) throws Exception {
        Path file = temp.resolve("catalog.json"); Files.writeString(file, catalog);
        Path log = temp.resolve("cli.log");
        var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "-cp", System.getProperty("java.class.path"), BatchIngestionCli.class.getName(), file.toString());
        process.environment().keySet().removeIf(key -> key.startsWith("AWS_") || key.startsWith("BRONZE_"));
        process.environment().putAll(Map.of("BRONZE_S3_ENDPOINT", endpoint, "BRONZE_S3_REGION", "us-east-1",
                "BRONZE_S3_BUCKET", "test", "BRONZE_S3_ACCESS_KEY", "synthetic", "BRONZE_S3_SECRET_KEY", "synthetic"));
        var child = process.redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try {
            assertTrue(child.waitFor(20, TimeUnit.SECONDS), "CLI timed out");
            String output = Files.readString(log);
            assertFalse(output.contains("Imaginary Science Notes"));
            assertFalse(output.contains("synthetic"));
            return child.exitValue();
        } finally { if (child.isAlive()) child.destroyForcibly(); }
    }
}
