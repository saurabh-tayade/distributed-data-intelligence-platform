package platform.ingestion.bronze;

import java.net.URI;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.core.checksums.RequestChecksumCalculation;
import software.amazon.awssdk.core.checksums.ResponseChecksumValidation;
import software.amazon.awssdk.http.urlconnection.UrlConnectionHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.retries.StandardRetryStrategy;
import software.amazon.awssdk.services.s3.S3Client;

/** Local development wiring only. No ambient AWS credentials, region, endpoint or proxy lookup. */
public final class LocalS3 {
    private final URI endpoint;
    private final String region;
    private final String bucket;
    private final String accessKey;
    private final String secretKey;

    public LocalS3(Map<String, String> environment) {
        endpoint = URI.create(required(environment, "BRONZE_S3_ENDPOINT"));
        if (!"http".equals(endpoint.getScheme()) || endpoint.getHost() == null
                || !Set.of("127.0.0.1", "localhost", "[::1]").contains(endpoint.getHost())
                || endpoint.getPort() < 1 || endpoint.getPort() > 65535
                || endpoint.getUserInfo() != null || endpoint.getQuery() != null || endpoint.getFragment() != null
                || !(endpoint.getPath().isEmpty() || endpoint.getPath().equals("/"))) {
            throw new IllegalArgumentException("BRONZE_S3_ENDPOINT must be an explicit HTTP loopback endpoint with port");
        }
        region = required(environment, "BRONZE_S3_REGION");
        bucket = required(environment, "BRONZE_S3_BUCKET");
        accessKey = required(environment, "BRONZE_S3_ACCESS_KEY");
        secretKey = required(environment, "BRONZE_S3_SECRET_KEY");
    }

    public String bucket() { return bucket; }

    public S3Client client() {
        return S3Client.builder().endpointOverride(endpoint).region(Region.of(region))
                .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create(accessKey, secretKey)))
                .forcePathStyle(true)
                .serviceConfiguration(software.amazon.awssdk.services.s3.S3Configuration.builder()
                        .chunkedEncodingEnabled(false).build())
                .requestChecksumCalculation(RequestChecksumCalculation.WHEN_REQUIRED)
                .responseChecksumValidation(ResponseChecksumValidation.WHEN_REQUIRED)
                .httpClientBuilder(UrlConnectionHttpClient.builder()
                        .connectionTimeout(Duration.ofSeconds(5)).socketTimeout(Duration.ofSeconds(10))
                        .proxyConfiguration(software.amazon.awssdk.http.urlconnection.ProxyConfiguration.builder()
                                .useSystemPropertyValues(false).useEnvironmentVariablesValues(false).build()))
                .overrideConfiguration(c -> c.apiCallTimeout(Duration.ofSeconds(30))
                        .apiCallAttemptTimeout(Duration.ofSeconds(20))
                        .retryStrategy(StandardRetryStrategy.builder().maxAttempts(1).build()))
                .build();
    }

    private static String required(Map<String, String> environment, String key) {
        String value = environment.get(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Set " + key + " explicitly for local storage");
        }
        return value;
    }
}
