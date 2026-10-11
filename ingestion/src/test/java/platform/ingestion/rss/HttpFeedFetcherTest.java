package platform.ingestion.rss;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.concurrent.atomic.AtomicInteger;

/** Dependency-free contract tests; assertions are always active. */
public final class HttpFeedFetcherTest {
    public static void main(String[] args) throws Exception {
        invalidSources();
        subscriberBounds();
        subscriberOwnership();
        subscriberGrowth();
        subscriberLifecycle();
        var requests = new AtomicInteger();
        byte[] payload = "<?xml version=\"1.0\"?><feed>café\r\n</feed>".getBytes(StandardCharsets.UTF_8);
        var server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
        server.createContext("/feed", exchange -> {
            requests.incrementAndGet();
            exchange.getResponseHeaders().set("Content-Type", "application/atom+xml; charset=utf-8");
            exchange.sendResponseHeaders(200, payload.length);
            try (var output = exchange.getResponseBody()) { output.write(payload); }
            exchange.close();
        });
        server.createContext("/error", exchange -> {
            exchange.sendResponseHeaders(503, payload.length);
            try (var output = exchange.getResponseBody()) { output.write(payload); }
            exchange.close();
        });
        server.createContext("/chunked", exchange -> {
            exchange.sendResponseHeaders(200, 0);
            try (var output = exchange.getResponseBody()) { output.write(payload); }
            exchange.close();
        });
        server.createContext("/redirect", exchange -> {
            exchange.getResponseHeaders().set("Location", "/feed");
            exchange.sendResponseHeaders(302, -1);
            exchange.close();
        });
        server.start();
        try (var client = HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(2)).build()) {
            var time = Instant.parse("2026-10-11T12:00:00Z");
            var fetcher = new HttpFeedFetcher(client, Duration.ofSeconds(2), Clock.fixed(time, ZoneOffset.UTC));
            String base = "http://127.0.0.1:" + server.getAddress().getPort();
            var source = new RssSource("publisher-1", URI.create(base + "/feed"), true);
            payloadLimits(client, time, base, payload);
            var raw = fetcher.fetch(source);
            check(Arrays.equals(payload, raw.body()), "Exact payload bytes");
            check(raw.sourceId().equals(source.sourceId()) && raw.ingestedAt().equals(time), "Identity and timestamp");
            check(raw.httpStatus() == 200 && raw.contentLength().orElseThrow() == payload.length, "Status and length");
            check(raw.contentType().orElseThrow().equals("application/atom+xml; charset=utf-8"), "Content type");
            check(!raw.ingestionId().equals(fetcher.fetch(source).ingestionId()), "Unique retrieval identities");
            raw.body()[0] = 0;
            check(Arrays.equals(payload, raw.body()), "Body accessor is defensive");
            byte[] copy = payload.clone();
            var constructed = new RawFeed(raw.ingestionId(), raw.sourceId(), time, 200,
                    raw.contentType(), raw.contentLength(), copy);
            copy[0] = 0;
            check(Arrays.equals(payload, constructed.body()), "Body constructor is defensive");
            var chunked = fetcher.fetch(new RssSource("chunked", URI.create(base + "/chunked"), true));
            check(chunked.contentType().isEmpty() && chunked.contentLength().isEmpty(), "Absent optional headers");
            check(Arrays.equals(payload, chunked.body()), "Chunked payload");
            var http = failure(() -> fetcher.fetch(new RssSource("error", URI.create(base + "/error"), true)),
                    FeedFetchException.Kind.HTTP);
            check(http.response().orElseThrow().httpStatus() == 503
                    && Arrays.equals(payload, http.response().orElseThrow().body()), "HTTP error retains response");
            failure(() -> fetcher.fetch(new RssSource("redirect", URI.create(base + "/redirect"), true)),
                    FeedFetchException.Kind.HTTP);
            int before = requests.get();
            failure(() -> fetcher.fetch(new RssSource("disabled", source.feedUri(), false)), FeedFetchException.Kind.DISABLED);
            check(requests.get() == before, "Disabled source makes no request");
            Thread.currentThread().interrupt();
            try {
                failure(() -> fetcher.fetch(source), FeedFetchException.Kind.INTERRUPTED);
                check(Thread.currentThread().isInterrupted(), "Interrupt restored");
            } finally {
                Thread.interrupted();
            }
            // Close before sending HTTP headers to deterministically exercise IOException.
            var broken = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
            broken.createContext("/", exchange -> exchange.close());
            broken.start();
            try {
                var transport = failure(() -> fetcher.fetch(new RssSource("broken",
                        URI.create("http://127.0.0.1:" + broken.getAddress().getPort() + "/"), true)),
                        FeedFetchException.Kind.TRANSPORT);
                check(transport.getCause() instanceof java.io.IOException && transport.response().isEmpty(),
                        "Transport cause retained, no fabricated response");
            } finally {
                broken.stop(0);
            }
        } finally {
            server.stop(0);
        }
        System.out.println("All RSS ingestion contract tests passed.");
    }

    private static void payloadLimits(HttpClient client, Instant time, String base, byte[] payload) throws Exception {
        Clock clock = Clock.fixed(time, ZoneOffset.UTC);
        invalid(() -> new HttpFeedFetcher(client, Duration.ofSeconds(2), clock, 0));
        invalid(() -> new HttpFeedFetcher(client, Duration.ofSeconds(2), clock, -1));
        check(HttpFeedFetcher.DEFAULT_MAX_PAYLOAD_BYTES == 5 * 1024 * 1024, "Default is 5 MiB");
        for (String path : new String[] {"/feed", "/chunked"}) {
            var source = new RssSource("bounded", URI.create(base + path), true);
            for (int limit : new int[] {payload.length, payload.length + 1}) {
                var accepted = new HttpFeedFetcher(client, Duration.ofSeconds(2), clock, limit).fetch(source);
                check(Arrays.equals(payload, accepted.body()), "Exact bytes at/below limit: " + path);
            }
            var small = new HttpFeedFetcher(client, Duration.ofSeconds(2), clock, payload.length - 1);
            assertOversized(() -> small.fetch(source));
        }
        var error = new RssSource("error", URI.create(base + "/error"), true);
        var exact = new HttpFeedFetcher(client, Duration.ofSeconds(2), clock, payload.length);
        var http = failure(() -> exact.fetch(error), FeedFetchException.Kind.HTTP);
        check(Arrays.equals(payload, http.response().orElseThrow().body()), "HTTP error at limit retains bytes");
        var small = new HttpFeedFetcher(client, Duration.ofSeconds(2), clock, payload.length - 1);
        assertOversized(() -> small.fetch(error));
        // Conflicting framing is rejected before the JDK can hide bytes behind a false small length.
        rawResponseFailure(small, FeedFetchException.Kind.TRANSPORT, "Transfer-Encoding: chunked\r\nContent-Length: 1\r\n",
                (Integer.toHexString(payload.length) + "\r\n").getBytes(StandardCharsets.US_ASCII),
                payload, "\r\n0\r\n\r\n".getBytes(StandardCharsets.US_ASCII));
        // Overstated length must not cause preallocation; received bytes still trip the limit.
        rawResponseFailure(small, FeedFetchException.Kind.PAYLOAD_TOO_LARGE, "Content-Length: 2147483647\r\n", new byte[0], payload, new byte[0]);
        // EOF-delimited body with no Content-Length or Transfer-Encoding.
        rawResponseFailure(small, FeedFetchException.Kind.PAYLOAD_TOO_LARGE, "", new byte[0], payload, new byte[0]);
        check(Arrays.equals(payload, exact.fetch(new RssSource("after", URI.create(base + "/feed"), true)).body()),
                "Client remains usable after cancellation");
    }

    private static void assertOversized(FetchAction action) throws Exception {
        var failure = failure(action, FeedFetchException.Kind.PAYLOAD_TOO_LARGE);
        check(failure.response().isEmpty(), "Oversize does not expose a truncated response");
        check(failure.getCause() instanceof java.io.IOException, "Oversize retains IOException cause chain");
    }

    /** Raw loopback server allows deliberately inaccurate headers that HttpServer controls itself. */
    private static void rawResponseFailure(HttpFeedFetcher fetcher, FeedFetchException.Kind kind,
                                          String headers, byte[] prefix,
                                     byte[] payload, byte[] suffix) throws Exception {
        try (var server = new java.net.ServerSocket();
             var executor = java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()) {
            server.bind(new InetSocketAddress("127.0.0.1", 0));
            server.setSoTimeout(5000);
            var serving = executor.submit(() -> {
                try (var socket = server.accept()) {
                    socket.setSoTimeout(5000);
                    var reader = new java.io.BufferedReader(new java.io.InputStreamReader(
                            socket.getInputStream(), StandardCharsets.US_ASCII));
                    String line;
                    while ((line = reader.readLine()) != null && !line.isEmpty()) {
                        // Consume request headers before delivering the crafted response.
                    }
                    var response = new java.io.ByteArrayOutputStream();
                    response.write(("HTTP/1.1 200 OK\r\nConnection: close\r\n" + headers + "\r\n")
                            .getBytes(StandardCharsets.US_ASCII));
                    response.write(prefix);
                    response.write(payload);
                    response.write(suffix);
                    socket.getOutputStream().write(response.toByteArray());
                }
                return null;
            });
            var failure = failure(() -> fetcher.fetch(new RssSource("misleading",
                    URI.create("http://127.0.0.1:" + server.getLocalPort() + "/"), true)), kind);
            check(failure.response().isEmpty() && failure.getCause() instanceof java.io.IOException,
                    "Raw response failure preserves cause without partial response");
            serving.get(5, java.util.concurrent.TimeUnit.SECONDS);
        }
    }

    private static void subscriberBounds() throws Exception {
        var cancelled = new java.util.concurrent.atomic.AtomicBoolean();
        var subscriber = new HttpFeedFetcher.BoundedBodySubscriber(4);
        subscriber.onSubscribe(new java.util.concurrent.Flow.Subscription() {
            @Override
            public void request(long count) { }
            @Override
            public void cancel() { cancelled.set(true); }
        });
        subscriber.onNext(java.util.List.of(java.nio.ByteBuffer.wrap(new byte[] {1, 2})));
        subscriber.onNext(java.util.List.of(java.nio.ByteBuffer.wrap(new byte[] {3}),
                java.nio.ByteBuffer.wrap(new byte[] {4, 5})));
        check(cancelled.get(), "Oversize cancels subscription across batches and buffers");
        // Late terminal/data callbacks must not replace the oversize failure or retain more bytes.
        subscriber.onComplete();
        subscriber.onError(new java.io.IOException("late transport error"));
        subscriber.onNext(java.util.List.of(java.nio.ByteBuffer.wrap(new byte[] {6})));
        try {
            subscriber.getBody().toCompletableFuture().get(1, java.util.concurrent.TimeUnit.SECONDS);
            throw new AssertionError("Expected bounded subscriber failure");
        } catch (java.util.concurrent.ExecutionException expected) {
            check(expected.getCause() instanceof java.io.IOException
                    && expected.getCause().getMessage().equals("Response payload exceeds 4 bytes"),
                    "Oversize failure survives late callbacks");
        }
    }

    private static void subscriberOwnership() throws Exception {
        var subscriber = new HttpFeedFetcher.BoundedBodySubscriber(1024 * 1024);
        var subscription = new RecordingSubscription();
        subscriber.onSubscribe(subscription);
        check(ownedStorage(subscriber).length == 0, "Storage is allocated lazily");
        byte[] backing = new byte[1024 * 1024];
        backing[101] = 10;
        backing[102] = 20;
        backing[103] = 30;
        var buffer = java.nio.ByteBuffer.wrap(backing);
        buffer.position(100).limit(105);
        var slice = buffer.slice().asReadOnlyBuffer();
        slice.position(1).limit(4);
        var fragments = java.util.List.of(slice);
        subscriber.onNext(fragments);
        check(slice.position() == 4, "Only readable slice bytes consumed");
        check(ownedStorage(subscriber).length < 1024 * 1024, "Small payload does not allocate full limit");
        check(ownedStorage(subscriber) != backing, "Storage does not alias upstream backing array");
        // Deterministic structural ownership check: no hidden delegate, buffer, or collection field.
        // The only retained reference fields are our array, the body future, and the subscription.
        for (var field : HttpFeedFetcher.BoundedBodySubscriber.class.getDeclaredFields()) {
            if (java.lang.reflect.Modifier.isStatic(field.getModifiers()) || field.getType().isPrimitive()) {
                continue;
            }
            field.setAccessible(true);
            Object value = field.get(subscriber);
            check(value == ownedStorage(subscriber) || value == subscriber.getBody() || value == subscription,
                    "Subscriber retains only owned storage and lifecycle objects: " + field.getName());
        }
        // Reuse/mutate the backing array and input list's buffer before completion.
        Arrays.fill(backing, (byte) 99);
        slice.clear();
        subscriber.onNext(java.util.List.of(java.nio.ByteBuffer.wrap(new byte[] {40}),
                java.nio.ByteBuffer.wrap(new byte[] {50, 60}).asReadOnlyBuffer()));
        subscriber.onComplete();
        check(Arrays.equals(subscriber.getBody().toCompletableFuture().get(), new byte[] {10, 20, 30, 40, 50, 60}),
                "Copied bytes survive upstream mutation across multiple callbacks and buffers");
        check(ownedStorage(subscriber).length == 0, "Accumulator released on success");
        check(subscription.requests == 3 && subscription.cancellations == 0, "One batch demanded at a time");
    }

    private static void subscriberGrowth() throws Exception {
        int limit = 4097;
        var subscriber = new HttpFeedFetcher.BoundedBodySubscriber(limit);
        var subscription = new RecordingSubscription();
        subscriber.onSubscribe(subscription);
        byte[] previous = ownedStorage(subscriber);
        int allocations = 0;
        for (int i = 0; i < limit; i++) {
            subscriber.onNext(java.util.List.of(java.nio.ByteBuffer.wrap(new byte[] {(byte) i})));
            byte[] current = ownedStorage(subscriber);
            check(current.length >= i + 1 && current.length <= limit, "Capacity stays within payload limit");
            if (current != previous) {
                allocations++;
                previous = current;
            }
        }
        check(allocations <= 14, "Logarithmic growth, not one allocation per fragment");
        subscriber.onComplete();
        byte[] result = subscriber.getBody().toCompletableFuture().get();
        check(result.length == limit, "Exact limit succeeds across many callbacks");
        for (int i = 0; i < limit; i++) {
            check(result[i] == (byte) i, "Fragment ordering preserved");
        }
        check(subscription.requests == limit + 1, "Demand replenished for every consumed batch");
    }

    private static void subscriberLifecycle() throws Exception {
        var subscriber = new HttpFeedFetcher.BoundedBodySubscriber(3);
        var subscription = new RecordingSubscription();
        subscriber.onSubscribe(subscription);
        var duplicate = new RecordingSubscription();
        subscriber.onSubscribe(duplicate);
        check(duplicate.cancellations == 1 && duplicate.requests == 0, "Duplicate subscription cancelled");
        subscriber.onNext(java.util.List.of());
        check(subscription.requests == 2, "Empty callback replenishes demand without allocating");
        check(ownedStorage(subscriber).length == 0, "Empty callback does not allocate");
        subscriber.onNext(java.util.List.of(java.nio.ByteBuffer.wrap(new byte[] {1, 2, 3})));
        subscriber.onNext(java.util.List.of(java.nio.ByteBuffer.wrap(new byte[] {4})));
        check(subscription.cancellations == 1 && subscription.requests == 3, "Overflow cancels without more demand");
        check(ownedStorage(subscriber).length == 0, "Overflow releases accumulated storage");
        Throwable overflow = bodyFailure(subscriber);
        subscriber.onNext(java.util.List.of(java.nio.ByteBuffer.wrap(new byte[] {5})));
        subscriber.onComplete();
        subscriber.onError(new java.io.IOException("late"));
        check(bodyFailure(subscriber) == overflow && subscription.cancellations == 1,
                "Late callbacks preserve original overflow and do not repeat cancellation");

        var failed = new HttpFeedFetcher.BoundedBodySubscriber(8);
        failed.onSubscribe(new RecordingSubscription());
        failed.onNext(java.util.List.of(java.nio.ByteBuffer.wrap(new byte[] {1})));
        var cause = new java.io.IOException("transport failure");
        failed.onError(cause);
        failed.onComplete();
        check(bodyFailure(failed) == cause && ownedStorage(failed).length == 0, "Original transport cause and cleanup");

        var rejected = new HttpFeedFetcher.BoundedBodySubscriber(8);
        rejected.onError(cause); // Header rejection happens before subscription.
        var lateSubscription = new RecordingSubscription();
        rejected.onSubscribe(lateSubscription);
        check(lateSubscription.cancellations == 1 && lateSubscription.requests == 0,
                "Header failure cancels late subscription without requesting data");
        check(bodyFailure(rejected) == cause, "Header failure cause preserved");

        var empty = new HttpFeedFetcher.BoundedBodySubscriber(8);
        empty.onSubscribe(new RecordingSubscription());
        empty.onComplete();
        byte[] result = empty.getBody().toCompletableFuture().get();
        empty.onComplete();
        empty.onError(cause);
        check(result.length == 0 && empty.getBody().toCompletableFuture().get() == result,
                "Empty response succeeds and terminal success cannot be overwritten");
    }

    private static byte[] ownedStorage(HttpFeedFetcher.BoundedBodySubscriber subscriber) throws Exception {
        var field = HttpFeedFetcher.BoundedBodySubscriber.class.getDeclaredField("storage");
        field.setAccessible(true);
        return (byte[]) field.get(subscriber);
    }

    private static Throwable bodyFailure(HttpFeedFetcher.BoundedBodySubscriber subscriber) throws Exception {
        try {
            subscriber.getBody().toCompletableFuture().get(1, java.util.concurrent.TimeUnit.SECONDS);
            throw new AssertionError("Expected body failure");
        } catch (java.util.concurrent.ExecutionException expected) {
            return expected.getCause();
        }
    }

    private static final class RecordingSubscription implements java.util.concurrent.Flow.Subscription {
        private int requests;
        private int cancellations;

        @Override
        public void request(long count) {
            check(count == 1, "Demand must request one batch");
            requests++;
        }

        @Override
        public void cancel() {
            cancellations++;
        }
    }

    private static void invalidSources() {
        for (String id : new String[] {null, "", "  "}) {
            invalid(() -> new RssSource(id, URI.create("https://example.test/feed"), true));
        }
        invalid(() -> new RssSource("id", null, true));
        for (String uri : new String[] {"/feed", "file:///feed", "https:/feed", "https://user:secret@example.test/feed",
                "https://example.test/feed#part", "http://example.test:65536/feed", "http://example.test:0/feed"}) {
            invalid(() -> new RssSource("id", URI.create(uri), true));
        }
        check(!new RssSource("id", URI.create("https://example.test/feed"), false).enabled(), "Disabled model is valid");
    }

    private static void invalid(Runnable action) {
        try { action.run(); } catch (IllegalArgumentException expected) { return; }
        throw new AssertionError("Expected invalid source");
    }

    private static FeedFetchException failure(FetchAction action, FeedFetchException.Kind kind) throws Exception {
        try { action.run(); } catch (FeedFetchException expected) {
            check(expected.kind() == kind, "Expected " + kind + ", got " + expected.kind());
            return expected;
        }
        throw new AssertionError("Expected failure " + kind);
    }

    private static void check(boolean condition, String description) {
        if (!condition) { throw new AssertionError(description); }
    }

    @FunctionalInterface
    private interface FetchAction { void run() throws Exception; }
}
