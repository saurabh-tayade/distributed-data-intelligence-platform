package platform.ingestion.managed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.io.InputStream;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.SocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import platform.ingestion.rss.FeedFetchException;
import platform.ingestion.rss.HttpFeedFetcher;
import platform.ingestion.rss.RawFeed;
import static platform.ingestion.managed.StrictFetchJson.*;

/** Separately identified launch-v2 child. This is not an alternate entry to the legacy launch. */
public final class StrictPublicManagedRssFetch {
    static final String ERROR = "STRICT_PUBLIC_FETCH_REJECTED";
    record Bootstrap(StrictManagedRssFetch.Bootstrap common, JsonNode publicFetch) {
        Bootstrap { publicFetch = publicFetch.deepCopy(); }
        @Override public JsonNode publicFetch() { return publicFetch.deepCopy(); }
        @Override public String toString() { return "PublicBootstrap[redacted]"; }
    }
    public static void main(String[] args) {
        int status = 2;
        try {
            PublicFetchResolverProvider.initialize();
            need(args.length == 0);
            int mode = ((Number) Files.getAttribute(Path.of("/proc/self/fd/0"), "unix:mode")).intValue();
            need((mode & 0170000) == 0010000);
            status = execute(System.in);
        } catch (Exception | LinkageError ignored) { System.err.println(ERROR); }
        System.exit(status);
    }

    static Bootstrap parse(byte[] packet) throws IOException {
        try {
            JsonNode value = StrictFetchJson.parse(packet, StrictManagedRssFetch.BOOTSTRAP_LIMIT - 1);
            fields(value, "bootstrapVersion", "mode", "childPid", "expected", "current", "fetch", "publicFetch");
            need(value.get("bootstrapVersion").isInt() && value.get("bootstrapVersion").intValue() == 2
                    && text(value, "mode").equals("PUBLIC_FETCH_SOURCE_CANDIDATE"));
            // Reuse every existing expected/current/native-command check without widening v1 parsing.
            ObjectNode common = (ObjectNode) value.deepCopy();
            common.remove("publicFetch");
            common.put("bootstrapVersion", 1).put("mode", "FETCH_SOURCE_CANDIDATE");
            var parsed = StrictManagedRssFetch.parse(canonical(common));
            need(!parsed.fetch().get("testEndpoint").booleanValue());
            JsonNode pub = value.get("publicFetch");
            fields(pub, "launchProfileId", "launchProfileSha256", "networkPolicySha256", "selectedEndpoint", "networkPolicy");
            need(text(pub, "launchProfileId").matches("[A-Za-z0-9_-]{1,128}"));
            need(text(pub, "launchProfileSha256").matches("[0-9a-f]{64}"));
            byte[] policy = canonical(pub.get("networkPolicy"));
            need(Arrays.equals(policy, canonical(PublicFetchNetworkPolicy.policy()))
                    && sha(policy).equals(text(pub, "networkPolicySha256")));
            JsonNode endpoint = pub.get("selectedEndpoint");
            fields(endpoint, "endpointId", "sourceId", "uri");
            uuid(text(endpoint, "endpointId"));
            need(text(endpoint, "sourceId").equals(parsed.expected().sourceId())
                    && text(endpoint, "uri").equals(text(parsed.fetch(), "feedUri"))
                    && sha(canonical(endpoint)).equals(text(parsed.fetch(), "endpointSha256")));
            return new Bootstrap(parsed, pub);
        } catch (RuntimeException error) { throw invalid(); }
    }
    private static void uuid(String value) throws IOException {
        need(value.matches("[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}")
                && UUID.fromString(value).toString().equals(value));
    }
    static int execute(InputStream input) throws Exception {
        Bootstrap bootstrap = parse(StrictManagedRssFetch.receive(input));
        var common = bootstrap.common();
        var sink = new StrictFetchEvidence(common.expected(), common.current());
        byte[] command = sink.readCommand();
        JsonNode c = StrictFetchJson.parse(command, 65536);
        need(Arrays.equals(command, canonical(StrictManagedRssFetch.command(common))));
        boolean[] restoreInterrupt = {false};
        try {
            return sink.runFresh(() -> {
                // Only the consumed, forced result prefix can precede SPI initialization or DNS.
                var source = StrictManagedRssFetch.source(common.expected(), common.fetch());
                PublicFetchResolverProvider.attest(source.feedUri().getHost());
                PublicFetchResolverProvider.resolveAndSeal(source.feedUri().getHost());
                need(!Thread.currentThread().isInterrupted());
                sink.inspect(); // A late resolver result cannot bypass changed ownership/evidence.
                try (var http = client()) {
                    RawFeed feed;
                    try {
                        feed = new HttpFeedFetcher(http, Duration.ofMillis(common.fetch().get("httpTimeoutMillis").intValue()),
                                Clock.systemUTC()).fetch(source);
                    } catch (FeedFetchException failure) {
                        boolean interrupted = Thread.interrupted();
                        try { sink.write("result.json", StrictManagedRssFetch.result(c, "FETCH_" + failure.kind().name())); }
                        finally { if (interrupted) restoreInterrupt[0] = true; }
                        return 1;
                    }
                    StrictManagedRssFetch.candidate(sink, command, c, source, feed);
                    RawFeed candidate = StrictFetchCandidate.reopen(sink, command);
                    need(feed.ingestionId().equals(candidate.ingestionId()) && feed.ingestedAt().equals(candidate.ingestedAt())
                            && Arrays.equals(feed.body(), candidate.body()));
                    sink.write("result.json", StrictManagedRssFetch.result(c, null));
                    RawFeed reopened = ManagedRssFetch.reopen(sink.directory(), command);
                    need(feed.ingestionId().equals(reopened.ingestionId()) && feed.ingestedAt().equals(reopened.ingestedAt())
                            && Arrays.equals(feed.body(), reopened.body()));
                    sink.inspect();
                    return 0;
                }
            });
        } finally { if (restoreInterrupt[0]) Thread.currentThread().interrupt(); }
    }
    static HttpClient client() {
        return HttpClient.newBuilder().version(HttpClient.Version.HTTP_1_1).connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NEVER).proxy(new ProxySelector() {
                    @Override public List<Proxy> select(URI uri) { return List.of(Proxy.NO_PROXY); }
                    @Override public void connectFailed(URI uri, SocketAddress address, IOException failure) { }
                }).build();
    }
    private StrictPublicManagedRssFetch() { }
}
