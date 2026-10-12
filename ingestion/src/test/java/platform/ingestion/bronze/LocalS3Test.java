package platform.ingestion.bronze;

import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class LocalS3Test {
    private Map<String, String> config() {
        return new HashMap<>(Map.of("BRONZE_S3_ENDPOINT", "http://127.0.0.1:8333", "BRONZE_S3_REGION", "us-east-1",
                "BRONZE_S3_BUCKET", "content-lake", "BRONZE_S3_ACCESS_KEY", "unit-test-only",
                "BRONZE_S3_SECRET_KEY", "synthetic-unit-test-only"));
    }

    @Test void requiresEverySettingWithoutAmbientAwsFallback() {
        assertThrows(IllegalArgumentException.class, () -> new LocalS3(Map.of()));
        for (String key : config().keySet()) {
            var settings = config();
            settings.remove(key);
            assertThrows(IllegalArgumentException.class, () -> new LocalS3(settings));
        }
    }

    @Test void refusesCloudAndAmbiguousEndpoints() {
        for (String endpoint : new String[] {"https://s3.amazonaws.com", "http://example.com:8333", "http://127.0.0.1",
                "http://127.0.0.1:8333/path", "http://user:password@localhost:8333", "http://localhost:8333/?token=x"}) {
            var settings = config();
            settings.put("BRONZE_S3_ENDPOINT", endpoint);
            assertThrows(IllegalArgumentException.class, () -> new LocalS3(settings));
        }
    }

    @Test void buildsLocalClientWithoutNetworkCalls() {
        var local = new LocalS3(config());
        assertEquals("content-lake", local.bucket());
        try (var client = local.client()) {
            assertEquals("s3", client.serviceName());
        }
    }
}
