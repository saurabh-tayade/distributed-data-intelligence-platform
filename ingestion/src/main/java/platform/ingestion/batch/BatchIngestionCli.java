package platform.ingestion.batch;

import java.io.PrintStream;
import java.net.http.HttpClient;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Map;
import platform.ingestion.bronze.LocalS3;
import platform.ingestion.bronze.S3BronzeStorage;
import platform.ingestion.rss.HttpFeedFetcher;
import software.amazon.awssdk.services.s3.model.HeadBucketRequest;

public final class BatchIngestionCli {
    private BatchIngestionCli() { }
    public static void main(String[] args) { System.exit(execute(args, System.getenv(), System.out)); }

    public static int execute(String[] args, Map<String, String> environment, PrintStream out) {
        String stage = "catalog (usage: BatchIngestionCli <catalog.json>)";
        try {
            if (args.length != 1) throw new IllegalArgumentException();
            var sources = SourceCatalog.load(Path.of(args[0]));
            stage = "local S3 configuration; check BRONZE_S3_* variables";
            var local = new LocalS3(environment);
            stage = "local S3 connectivity/authentication; check container, bucket and local credentials";
            try (var s3 = local.client(); var http = HttpClient.newBuilder()
                    .connectTimeout(Duration.ofSeconds(10)).followRedirects(HttpClient.Redirect.NEVER).build()) {
                s3.headBucket(HeadBucketRequest.builder().bucket(local.bucket()).build());
                stage = "batch execution";
                var result = new BatchIngestionRunner(new HttpFeedFetcher(http, Duration.ofSeconds(30),
                        Clock.systemUTC()), new S3BronzeStorage(s3, local.bucket())).run(sources);
                print(result, out);
                return result.exitCode();
            }
        } catch (Exception failure) {
            // Do not print exception messages: SDK/JSON/URI diagnostics can contain secrets or payloads.
            out.println("Initialization/application failure at " + stage + " [" + failure.getClass().getSimpleName() + "]");
            return 2;
        }
    }

    public static void print(BatchResult result, PrintStream out) {
        for (var source : result.sources()) out.println(source);
        out.printf("Batch Ingestion Summary%nTotal Sources: %d%nSucceeded: %d%nFailed: %d%nSkipped: %d%nDuration: %d ms%n",
                result.sources().size(), result.succeeded(), result.failed(), result.skipped(), result.durationMs());
    }
}
