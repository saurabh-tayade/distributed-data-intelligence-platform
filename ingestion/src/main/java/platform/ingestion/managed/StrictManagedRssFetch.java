package platform.ingestion.managed;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.ByteArrayOutputStream;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import platform.ingestion.batch.SourceCatalog;
import platform.ingestion.bronze.BronzeKeys;
import platform.ingestion.bronze.S3BronzeStorage;
import platform.ingestion.rss.FeedFetchException;
import platform.ingestion.rss.HttpFeedFetcher;
import platform.ingestion.rss.RawFeed;
import platform.ingestion.rss.RssSource;
import static platform.ingestion.managed.StrictFetchJson.*;

/**
 * Inactive child-only FETCH_SOURCE candidate. No production launcher/registration,
 * SQL operation authority, completion publication, retries or recovery adoption.
 * Independent bootstrap correspondence is not durable authorization.
 */
public final class StrictManagedRssFetch {
    static final int BOOTSTRAP_LIMIT = 65536;
    static final int BOOTSTRAP_TIMEOUT_SECONDS = 5;
    static final String ERROR = "STRICT_FETCH_REJECTED";

    record Bootstrap(StrictFetchEvidence.Expected expected, StrictFetchEvidence.Current current,
                     JsonNode fetch) {
        Bootstrap { fetch = fetch.deepCopy(); }
        @Override public JsonNode fetch() { return fetch.deepCopy(); }
        @Override public String toString() { return "Bootstrap[redacted]"; }
    }

    public static void main(String[] args) {
        int status = 2;
        try {
            need(args.length == 0);
            // This fixed Linux entry point accepts only its startup pipe,
            // never a command-path argument, environment packet or regular-file stdin.
            int mode = ((Number) Files.getAttribute(Path.of("/proc/self/fd/0"), "unix:mode")).intValue();
            need((mode & 0170000) == 0010000);
            status = execute(System.in);
        } catch (Exception ignored) { System.err.println(ERROR); }
        System.exit(status);
    }

    /** Bounded memory and total deadline; a complete JSON prefix is not an EOF gate. */
    static byte[] receive(InputStream input) throws IOException {
        need(input != null);
        var received = new CompletableFuture<byte[]>();
        Thread reader = Thread.ofVirtual().name("strict-fetch-bootstrap").start(() -> {
            try {
                var out = new ByteArrayOutputStream();
                byte[] buffer = new byte[4096];
                while (out.size() < BOOTSTRAP_LIMIT) {
                    int count = input.read(buffer, 0, Math.min(buffer.length, BOOTSTRAP_LIMIT - out.size()));
                    if (count == -1) {
                        need(out.size() > 0); received.complete(out.toByteArray()); return;
                    }
                    if (count > 0) out.write(buffer, 0, count);
                }
                received.completeExceptionally(invalid());
            } catch (Exception error) { received.completeExceptionally(invalid()); }
        });
        try { return received.get(BOOTSTRAP_TIMEOUT_SECONDS, TimeUnit.SECONDS); }
        catch (InterruptedException error) { Thread.currentThread().interrupt(); throw invalid(); }
        catch (java.util.concurrent.ExecutionException | java.util.concurrent.TimeoutException error) { throw invalid(); }
        finally {
            // Do not close a blocking stream from this thread: close can itself block.
            // The daemon receiver owns no path/sink/callback and cannot launch work.
            reader.interrupt();
        }
    }

    static Bootstrap parse(byte[] packet) throws IOException {
        try {
            JsonNode value = StrictFetchJson.parse(packet, BOOTSTRAP_LIMIT - 1);
            fields(value, "bootstrapVersion", "mode", "childPid", "expected", "current", "fetch");
            need(value.get("bootstrapVersion").isInt() && value.get("bootstrapVersion").intValue() == 1
                    && text(value, "mode").equals("FETCH_SOURCE_CANDIDATE")
                    && decimal(value, "childPid", true) == ProcessHandle.current().pid());
            JsonNode e = value.get("expected"), c = value.get("current");
            fields(e, "installationId", "policy", "planKind", "runId", "originalAttemptId", "sourceId", "controlRoot",
                    "controlIdentity", "rootIdentity", "lockIdentity", "ledgerSha256", "frozen", "writer");
            fields(c, "policy", "frozen", "writer");
            String root = text(e, "controlRoot");
            Path path = Path.of(root);
            need(path.isAbsolute() && path.normalize().equals(path) && path.toString().equals(root));
            var expected = new StrictFetchEvidence.Expected(text(e, "installationId"), text(e, "policy"), text(e, "planKind"),
                    text(e, "runId"), text(e, "originalAttemptId"), text(e, "sourceId"), path,
                    identity(e.get("controlIdentity")), identity(e.get("rootIdentity")), identity(e.get("lockIdentity")),
                    text(e, "ledgerSha256"), frozen(e.get("frozen")), writer(e.get("writer")));
            var current = new StrictFetchEvidence.Current(text(c, "policy"), frozen(c.get("frozen")), writer(c.get("writer")));
            StrictFetchEvidence.validateExpectations(expected, current);
            JsonNode fetch = value.get("fetch");
            fields(fetch, "revision", "capturedConfigSha256", "endpointSha256", "feedUri", "runtimeSha256",
                    "javaRuntime", "testEndpoint", "httpTimeoutMillis");
            need(fetch.get("revision").isInt() && fetch.get("revision").intValue() > 0
                    && fetch.get("testEndpoint").isBoolean() && fetch.get("httpTimeoutMillis").isInt()
                    && fetch.get("httpTimeoutMillis").intValue() > 0 && fetch.get("httpTimeoutMillis").intValue() <= 30000
                    && expected.writer().epoch() > 0 && Runtime.version().feature() == 21
                    && text(fetch, "javaRuntime").equals(Runtime.version().toString()));
            for (String key : List.of("capturedConfigSha256", "endpointSha256", "runtimeSha256"))
                need(text(fetch, key).matches("[0-9a-f]{64}"));
            source(expected, fetch); // Pure endpoint validation, never DNS or HTTP.
            return new Bootstrap(expected, current, fetch);
        } catch (RuntimeException error) { throw invalid(); }
    }

    private static StrictFetchEvidence.Identity identity(JsonNode node) throws IOException {
        fields(node, "device", "inode");
        return new StrictFetchEvidence.Identity(decimal(node, "device", false), decimal(node, "inode", true));
    }
    private static StrictFetchEvidence.Frozen frozen(JsonNode node) throws IOException {
        fields(node, "selectionSha256", "profileSha256", "runtimeSha256");
        return new StrictFetchEvidence.Frozen(text(node, "selectionSha256"), text(node, "profileSha256"), text(node, "runtimeSha256"));
    }
    private static StrictFetchEvidence.Writer writer(JsonNode node) throws IOException {
        fields(node, "installationId", "workerInstanceId", "epoch", "runId", "attemptId", "stage", "ordinal", "checkpointId", "commandSha256");
        need(node.get("ordinal").isInt());
        return new StrictFetchEvidence.Writer(text(node, "installationId"), text(node, "workerInstanceId"), decimal(node, "epoch", true),
                text(node, "runId"), text(node, "attemptId"), text(node, "stage"), node.get("ordinal").intValue(),
                text(node, "checkpointId"), text(node, "commandSha256"));
    }
    static RssSource source(StrictFetchEvidence.Expected expected, JsonNode fetch) throws IOException {
        var source = SourceCatalog.validate(List.of(new RssSource(expected.sourceId(), URI.create(text(fetch, "feedUri")), true))).getFirst();
        ManagedRssFetch.validateEndpoint(source.feedUri(), fetch.get("testEndpoint").booleanValue());
        return source;
    }

    /** Only test-only launchers call this new entry point in the current slice. */
    static int execute(InputStream input) throws Exception {
        Bootstrap bootstrap = parse(receive(input));
        var sink = new StrictFetchEvidence(bootstrap.expected(), bootstrap.current());
        byte[] command = sink.readCommand();
        JsonNode c = StrictFetchJson.parse(command, 65536);
        need(Arrays.equals(command, canonical(command(bootstrap))));
        var source = source(bootstrap.expected(), bootstrap.fetch());
        boolean[] restoreInterrupt = {false};
        try { return sink.runFresh(() -> fetch(sink, command, c, source, bootstrap.fetch(), restoreInterrupt)); }
        finally { if (restoreInterrupt[0]) Thread.currentThread().interrupt(); }
    }

    static ObjectNode command(Bootstrap bootstrap) throws IOException {
        var w = bootstrap.expected().writer();
        var c = object().put("protocolVersion", 1).put("runId", w.runId()).put("attemptId", w.attemptId())
                .put("ownerEpoch", w.epoch()).put("stage", "FETCH_SOURCE").put("itemOrdinal", 0)
                .put("checkpointId", w.checkpointId()).put("sourceId", bootstrap.expected().sourceId());
        var nativeFacts = bootstrap.fetch();
        for (String key : List.of("revision", "capturedConfigSha256", "endpointSha256", "feedUri", "runtimeSha256",
                "javaRuntime", "testEndpoint", "httpTimeoutMillis")) c.set(key, nativeFacts.get(key));
        return c;
    }

    private static int fetch(StrictFetchEvidence sink, byte[] command, JsonNode c, RssSource source, JsonNode fetch, boolean[] restoreInterrupt) throws Exception {
        // Result prefix has been forced before this point. Failure/interruption never
        // permits another invocation, even if no candidate bytes were produced.
        try (var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NEVER).proxy(new ProxySelector() {
                    @Override public List<Proxy> select(URI uri) { return List.of(Proxy.NO_PROXY); }
                    @Override public void connectFailed(URI uri, SocketAddress address, IOException failure) { }
                }).build()) {
            RawFeed feed;
            try {
                feed = new HttpFeedFetcher(http, Duration.ofMillis(fetch.get("httpTimeoutMillis").intValue()), Clock.systemUTC()).fetch(source);
            } catch (FeedFetchException failure) {
                // The request may already have reached the server. Preserve the flag,
                // but allow a bounded failure write without interrupting its file lock.
                boolean interrupted = Thread.interrupted();
                try { sink.write("result.json", result(c, "FETCH_" + failure.kind().name())); }
                finally { if (interrupted) restoreInterrupt[0] = true; }
                return 1;
            }
            candidate(sink, command, c, source, feed);
            RawFeed candidate = StrictFetchCandidate.reopen(sink, command);
            need(feed.ingestionId().equals(candidate.ingestionId()) && feed.ingestedAt().equals(candidate.ingestedAt())
                    && Arrays.equals(feed.body(), candidate.body()));
            sink.write("result.json", result(c, null));
            // The unchanged validator is read-only. Its check cannot publish complete.json.
            RawFeed reopened = ManagedRssFetch.reopen(sink.directory(), command);
            need(feed.ingestionId().equals(reopened.ingestionId()) && feed.ingestedAt().equals(reopened.ingestedAt())
                    && Arrays.equals(feed.body(), reopened.body()));
            sink.inspect();
            return 0;
        }
    }

    static void candidate(StrictFetchEvidence sink, byte[] command, JsonNode c, RssSource source, RawFeed feed) throws Exception {
        byte[] payload = feed.body(), metadata = S3BronzeStorage.preparedMetadata(source, feed);
        need(payload.length <= HttpFeedFetcher.DEFAULT_MAX_PAYLOAD_BYTES && metadata.length <= 65536);
        var keys = BronzeKeys.forFeed(feed);
        var d = object().put("journalVersion", 1).put("commandSha256", sha(command));
        for (String key : List.of("runId", "attemptId", "ownerEpoch", "stage", "itemOrdinal", "checkpointId", "sourceId",
                "revision", "capturedConfigSha256", "endpointSha256", "runtimeSha256")) d.set(key, c.get(key));
        d.put("ingestionId", feed.ingestionId().toString()).put("ingestedAt", feed.ingestedAt().toString()).put("httpStatus", feed.httpStatus());
        if (feed.contentType().isPresent()) d.put("contentType", feed.contentType().get()); else d.putNull("contentType");
        if (feed.contentLength().isPresent()) d.put("declaredContentLength", feed.contentLength().getAsLong()); else d.putNull("declaredContentLength");
        d.put("payloadByteLength", payload.length).put("payloadSha256", sha(payload));
        d.putObject("expected").put("payloadKey", keys.payload()).put("metadataKey", keys.metadata())
                .put("metadataByteLength", metadata.length).put("metadataSha256", sha(metadata));
        byte[] descriptor = canonical(d);
        need(descriptor.length <= 65536); // Qualify all native bytes before the first candidate write.
        sink.write("payload.xml", payload);
        sink.write("metadata.json", metadata);
        sink.write("descriptor.json", descriptor);
        need(Arrays.equals(command, sink.readCommand()));
    }

    static byte[] result(JsonNode c, String code) throws IOException {
        var r = object();
        for (String key : List.of("protocolVersion", "runId", "attemptId", "stage", "itemOrdinal")) r.set(key, c.get(key));
        r.put("status", code == null ? "OK" : "FAILED");
        if (code == null) { r.putNull("code"); r.set("evidenceId", c.get("checkpointId")); }
        else { r.put("code", code); r.putNull("evidenceId"); }
        r.putObject("counts");
        return canonical(r);
    }
    private StrictManagedRssFetch() { }
}
