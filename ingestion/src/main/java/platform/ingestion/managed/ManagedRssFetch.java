package platform.ingestion.managed;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.net.URI;
import java.net.ProxySelector;
import java.net.Proxy;
import java.net.SocketAddress;
import java.net.http.HttpClient;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.security.MessageDigest;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.util.*;
import platform.ingestion.batch.SourceCatalog;
import platform.ingestion.bronze.BronzeKeys;
import platform.ingestion.bronze.S3BronzeStorage;
import platform.ingestion.rss.*;

/** Fixed prebuilt child: one fetch and private candidate only. Parent alone publishes completion.
 * No storage client, retries, stdout payloads, XML parsing or automatic dispatch. */
public final class ManagedRssFetch {
    public static final int DESCRIPTOR_LIMIT = 65536;
    private static final ObjectMapper JSON = new ObjectMapper()
            .enable(JsonParser.Feature.STRICT_DUPLICATE_DETECTION)
            .enable(DeserializationFeature.FAIL_ON_TRAILING_TOKENS);
    private ManagedRssFetch() { }

    public static void main(String[] args) {
        int status = 2;
        try { if (args.length == 1) status = execute(Path.of(args[0])); }
        catch (Exception ignored) { /* No paths, endpoint, response or exception text leaves the child. */ }
        System.exit(status);
    }

    public static int execute(Path command) throws Exception {
        privateEntry(command.getParent(), true);
        byte[] raw = read(command, DESCRIPTOR_LIMIT);
        JsonNode c = JSON.readTree(raw);
        fields(c, "protocolVersion", "runId", "attemptId", "ownerEpoch", "stage", "itemOrdinal", "checkpointId",
                "sourceId", "revision", "capturedConfigSha256", "endpointSha256", "feedUri", "runtimeSha256",
                "javaRuntime", "testEndpoint", "httpTimeoutMillis");
        for (String key : List.of("runId", "attemptId", "checkpointId")) check(UUID.fromString(text(c,key)).toString().equals(text(c,key)));
        for (String key : List.of("capturedConfigSha256", "endpointSha256", "runtimeSha256")) check(text(c,key).matches("[0-9a-f]{64}"));
        check(c.path("protocolVersion").isInt() && c.path("protocolVersion").intValue() == 1 && text(c,"stage").equals("FETCH_SOURCE")
                && c.path("revision").isInt() && c.path("revision").intValue() > 0
                && c.path("ownerEpoch").isIntegralNumber() && c.path("ownerEpoch").canConvertToLong() && c.path("ownerEpoch").longValue() > 0
                && c.path("itemOrdinal").isInt() && c.path("itemOrdinal").intValue() >= 0 && c.path("itemOrdinal").intValue() < 32
                && c.path("testEndpoint").isBoolean() && c.path("httpTimeoutMillis").isInt()
                && c.path("httpTimeoutMillis").intValue() > 0 && c.path("httpTimeoutMillis").intValue() <= 30000
                && Runtime.version().feature() == 21 && text(c,"javaRuntime").equals(Runtime.version().toString()));
        var source = SourceCatalog.validate(List.of(new RssSource(text(c,"sourceId"), URI.create(text(c,"feedUri")), true))).getFirst();
        validateEndpoint(source.feedUri(), c.get("testEndpoint").booleanValue());
        try (var http = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(10))
                .followRedirects(HttpClient.Redirect.NEVER).proxy(new ProxySelector() {
                    public List<Proxy> select(URI uri) { return List.of(Proxy.NO_PROXY); }
                    public void connectFailed(URI uri, SocketAddress address, IOException failure) { }
                }).build()) {
            RawFeed feed = new HttpFeedFetcher(http, Duration.ofMillis(c.get("httpTimeoutMillis").intValue()), Clock.systemUTC()).fetch(source);
            writeCandidate(command.getParent(), raw, c, source, feed);
            result(command.getParent(), c, null);
            return 0;
        } catch (FeedFetchException failure) {
            result(command.getParent(), c, "FETCH_" + failure.kind().name());
            return 1;
        }
    }

    public static void validateEndpoint(URI uri, boolean isolatedTest) {
        String host = uri.getHost() == null ? "" : uri.getHost().toLowerCase(Locale.ROOT);
        boolean loopback = host.equals("127.0.0.1") || host.equals("[::1]");
        check(uri.getQuery() == null && uri.getUserInfo() == null && uri.getFragment() == null);
        check(isolatedTest ? loopback : host.matches("[a-z0-9][a-z0-9.-]*\\.[a-z][a-z0-9.-]*")
                && !host.endsWith(".localhost") && !host.endsWith(".local") && !host.endsWith(".internal"));
    }

    /** Package-private fault boundary used by deterministic journal vector tests. */
    static void writeCandidate(Path directory, byte[] command, JsonNode c, RssSource source, RawFeed feed) throws Exception {
        byte[] payload = feed.body(), metadata = S3BronzeStorage.preparedMetadata(source,feed);
        check(payload.length <= HttpFeedFetcher.DEFAULT_MAX_PAYLOAD_BYTES && metadata.length <= DESCRIPTOR_LIMIT);
        var keys = BronzeKeys.forFeed(feed);
        ObjectNode d = JSON.createObjectNode().put("journalVersion",1).put("commandSha256",sha(command));
        for (String key : List.of("runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId","revision","capturedConfigSha256","endpointSha256","runtimeSha256")) d.set(key,c.get(key));
        d.put("ingestionId",feed.ingestionId().toString()).put("ingestedAt",feed.ingestedAt().toString()).put("httpStatus",feed.httpStatus());
        if(feed.contentType().isPresent()) d.put("contentType",feed.contentType().get()); else d.putNull("contentType");
        if(feed.contentLength().isPresent()) d.put("declaredContentLength",feed.contentLength().getAsLong()); else d.putNull("declaredContentLength");
        d.put("payloadByteLength",payload.length).put("payloadSha256",sha(payload));
        d.putObject("expected").put("payloadKey",keys.payload()).put("metadataKey",keys.metadata())
                .put("metadataByteLength",metadata.length).put("metadataSha256",sha(metadata));
        byte[] descriptor = JSON.writeValueAsBytes(d);
        check(descriptor.length <= DESCRIPTOR_LIMIT);
        write(directory.resolve("payload.xml"),payload);
        write(directory.resolve("metadata.json"),metadata);
        write(directory.resolve("descriptor.json"),descriptor);
        check(Arrays.equals(command,read(directory.resolve("command.json"),DESCRIPTOR_LIMIT)));
        reopen(directory,command); // Exact RawFeed and shared serializer read-back, before parent completion.
    }

    public static RawFeed reopen(Path directory, byte[] command) throws Exception {
        privateEntry(directory,true);
        Set<String> names=new HashSet<>();try(var entries=Files.list(directory)){entries.forEach(p->names.add(p.getFileName().toString()));}
        check(names.containsAll(Set.of("command.json","payload.xml","metadata.json","descriptor.json")) && Set.of("command.json","payload.xml","metadata.json","descriptor.json","result.json","complete.json").containsAll(names));
        check(Arrays.equals(command,read(directory.resolve("command.json"),DESCRIPTOR_LIMIT)));
        JsonNode c=JSON.readTree(command),d=JSON.readTree(read(directory.resolve("descriptor.json"),DESCRIPTOR_LIMIT));
        fields(d,"journalVersion","commandSha256","runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId","revision","capturedConfigSha256","endpointSha256","runtimeSha256","ingestionId","ingestedAt","httpStatus","contentType","declaredContentLength","payloadByteLength","payloadSha256","expected");
        check(d.path("journalVersion").isInt() && d.path("journalVersion").intValue()==1 && text(d,"commandSha256").equals(sha(command)));
        for(String key:List.of("runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId","revision","capturedConfigSha256","endpointSha256","runtimeSha256"))check(d.get(key).equals(c.get(key)));
        byte[] payload=read(directory.resolve("payload.xml"),HttpFeedFetcher.DEFAULT_MAX_PAYLOAD_BYTES);
        check(d.path("payloadByteLength").isInt() && d.get("payloadByteLength").intValue()==payload.length && text(d,"payloadSha256").equals(sha(payload)));
        check(d.path("httpStatus").isInt() && (d.get("declaredContentLength").isNull() || d.get("declaredContentLength").isIntegralNumber() && d.get("declaredContentLength").canConvertToLong()));
        var feed=new RawFeed(UUID.fromString(text(d,"ingestionId")),text(d,"sourceId"),Instant.parse(text(d,"ingestedAt")),d.get("httpStatus").intValue(),
                d.get("contentType").isNull()?Optional.empty():Optional.of(text(d,"contentType")),
                d.get("declaredContentLength").isNull()?OptionalLong.empty():OptionalLong.of(d.get("declaredContentLength").longValue()),payload);
        check(feed.httpStatus()>=200 && feed.httpStatus()<300 && feed.ingestedAt().toString().equals(text(d,"ingestedAt")));
        byte[] metadata=read(directory.resolve("metadata.json"),DESCRIPTOR_LIMIT);
        check(Arrays.equals(metadata,S3BronzeStorage.preparedMetadata(new RssSource(feed.sourceId(),URI.create(text(c,"feedUri")),true),feed)));
        var e=d.get("expected");fields(e,"payloadKey","metadataKey","metadataByteLength","metadataSha256");var keys=BronzeKeys.forFeed(feed);
        check(text(e,"payloadKey").equals(keys.payload()) && text(e,"metadataKey").equals(keys.metadata()) && e.path("metadataByteLength").isInt() && e.get("metadataByteLength").intValue()==metadata.length && text(e,"metadataSha256").equals(sha(metadata)));
        return feed;
    }

    private static void result(Path directory,JsonNode c,String code) throws IOException {
        var r=JSON.createObjectNode();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))r.set(key,c.get(key));
        r.put("status",code==null?"OK":"FAILED");if(code==null)r.putNull("code");else r.put("code",code);
        if(code==null)r.set("evidenceId",c.get("checkpointId"));else r.putNull("evidenceId");r.putObject("counts");
        write(directory.resolve("result.json"),JSON.writeValueAsBytes(r));
    }
    static void privateEntry(Path p,boolean directory) throws IOException {
        check(p.isAbsolute() && p.equals(p.toRealPath()) && (directory?Files.isDirectory(p,LinkOption.NOFOLLOW_LINKS):Files.isRegularFile(p,LinkOption.NOFOLLOW_LINKS)));
        check(Files.getOwner(p).equals(Files.getOwner(Path.of(System.getProperty("user.home")))) && Files.getPosixFilePermissions(p).equals(PosixFilePermissions.fromString(directory?"rwx------":"rw-------")));
        if(!directory)check(((Number)Files.getAttribute(p,"unix:nlink",LinkOption.NOFOLLOW_LINKS)).longValue()==1);
    }
    static byte[] read(Path p,int maximum) throws IOException {
        privateEntry(p,false);check(Files.size(p)<=maximum);
        byte[] bytes;try(var stream=Files.newInputStream(p,LinkOption.NOFOLLOW_LINKS)){bytes=stream.readNBytes(maximum+1);}check(bytes.length<=maximum);return bytes;
    }
    static void write(Path p,byte[] bytes) throws IOException {
        if(ReservedEvidence.active()){ReservedEvidence.write(p,bytes);return;}
        privateEntry(p.getParent(),true);
        try(var ch=FileChannel.open(p,Set.of(StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))){
            var buffer=ByteBuffer.wrap(bytes);while(buffer.hasRemaining())ch.write(buffer);ch.force(true);
        }
        check(Arrays.equals(bytes,read(p,Math.max(bytes.length,1))));
    }
    private static String text(JsonNode n,String key){check(n.has(key) && n.get(key).isTextual());return n.get(key).textValue();}
    private static void fields(JsonNode n,String...keys){check(n!=null && n.isObject() && n.size()==keys.length);for(String key:keys)check(n.has(key));}
    private static String sha(byte[] bytes){try{return HexFormat.of().formatHex(MessageDigest.getInstance("SHA-256").digest(bytes));}catch(java.security.NoSuchAlgorithmException ex){throw new IllegalStateException(ex);}}
    private static void check(boolean valid){if(!valid)throw new IllegalArgumentException("RSS_EVIDENCE_INVALID");}
}
