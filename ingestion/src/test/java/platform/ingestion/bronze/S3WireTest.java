package platform.ingestion.bronze;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Exercises real AWS SDK serialization against a loopback stub, not SeaweedFS compatibility. */
class S3WireTest {
    @Test void sdkUploadsExactBytesAndRecognizesNoSuchKey() throws Exception {
        var objects = new ConcurrentHashMap<String, byte[]>();
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/", exchange -> {
            try {
                String path = exchange.getRequestURI().getPath();
                if (exchange.getRequestMethod().equals("PUT")) {
                    objects.put(path, exchange.getRequestBody().readAllBytes());
                    exchange.sendResponseHeaders(200, -1);
                } else {
                    byte[] bytes = objects.get(path);
                    if (bytes == null) {
                        bytes = "<Error><Code>NoSuchKey</Code><Message>Absent</Message></Error>".getBytes(StandardCharsets.UTF_8);
                        exchange.getResponseHeaders().set("Content-Type", "application/xml");
                        exchange.sendResponseHeaders(404, bytes.length);
                    } else {
                        exchange.sendResponseHeaders(200, bytes.length);
                    }
                    exchange.getResponseBody().write(bytes);
                }
            } finally {
                exchange.close();
            }
        });
        server.start();
        try {
            var config = new HashMap<>(Map.of("BRONZE_S3_ENDPOINT", "http://127.0.0.1:" + server.getAddress().getPort(),
                    "BRONZE_S3_REGION", "us-east-1", "BRONZE_S3_BUCKET", "wire-test",
                    "BRONZE_S3_ACCESS_KEY", "unit-test-only", "BRONZE_S3_SECRET_KEY", "unit-test-only"));
            var local = new LocalS3(config);
            try (var client = local.client()) {
                var storage = new S3BronzeStorage(client, local.bucket());
                var receipt = storage.write(S3BronzeStorageTest.SOURCE, S3BronzeStorageTest.feed(S3BronzeStorageTest.PAYLOAD));
                assertArrayEquals(S3BronzeStorageTest.PAYLOAD, objects.get("/wire-test/" + receipt.payloadKey()));
                assertEquals(receipt, storage.write(S3BronzeStorageTest.SOURCE, S3BronzeStorageTest.feed(S3BronzeStorageTest.PAYLOAD)));
                assertEquals(2, objects.size());
            }
        } finally {
            server.stop(0);
        }
    }
}
