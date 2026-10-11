package platform.ingestion.rss;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.ByteBuffer;
import java.time.Clock;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionStage;
import java.util.concurrent.Flow;

public final class HttpFeedFetcher implements FeedFetcher {
    public static final int DEFAULT_MAX_PAYLOAD_BYTES = 5 * 1024 * 1024;

    private final int maxPayloadBytes;
    private final HttpClient client;
    private final Duration requestTimeout;
    private final Clock clock;

    /** Caller owns client lifecycle, connect timeout, proxy, and redirect policy. */
    public HttpFeedFetcher(HttpClient client, Duration requestTimeout, Clock clock) {
        this(client, requestTimeout, clock, DEFAULT_MAX_PAYLOAD_BYTES);
    }

    /** Limit applies to received payload bytes, before any downstream decompression. */
    public HttpFeedFetcher(HttpClient client, Duration requestTimeout, Clock clock, int maxPayloadBytes) {
        if (maxPayloadBytes <= 0) {
            throw new IllegalArgumentException("maxPayloadBytes must be positive");
        }
        this.maxPayloadBytes = maxPayloadBytes;
        this.client = Objects.requireNonNull(client, "client");
        this.requestTimeout = Objects.requireNonNull(requestTimeout, "requestTimeout");
        this.clock = Objects.requireNonNull(clock, "clock");
        if (requestTimeout.isZero() || requestTimeout.isNegative()) {
            throw new IllegalArgumentException("requestTimeout must be positive");
        }
    }

    @Override
    public RawFeed fetch(RssSource source) throws FeedFetchException {
        Objects.requireNonNull(source, "source");
        if (!source.enabled()) {
            throw new FeedFetchException(FeedFetchException.Kind.DISABLED, source.sourceId(), null, null);
        }
        var request = HttpRequest.newBuilder(source.feedUri()).timeout(requestTimeout).GET().build();
        final HttpResponse<byte[]> response;
        try {
            response = client.send(request, info -> {
                var subscriber = new BoundedBodySubscriber(maxPayloadBytes);
                // Reject ambiguous framing: some clients otherwise trust the shorter length.
                if (info.headers().firstValue("Transfer-Encoding").isPresent()
                        && info.headers().firstValue("Content-Length").isPresent()) {
                    subscriber.onError(new IOException("Response has conflicting Transfer-Encoding and Content-Length"));
                }
                return subscriber;
            });
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            throw new FeedFetchException(FeedFetchException.Kind.INTERRUPTED, source.sourceId(), null, exception);
        } catch (IOException exception) {
            for (Throwable cause = exception; cause != null; cause = cause.getCause()) {
                if (cause instanceof PayloadTooLargeException) {
                    throw new FeedFetchException(FeedFetchException.Kind.PAYLOAD_TOO_LARGE,
                            source.sourceId(), null, exception);
                }
            }
            throw new FeedFetchException(FeedFetchException.Kind.TRANSPORT, source.sourceId(), null, exception);
        }
        var raw = new RawFeed(UUID.randomUUID(), source.sourceId(), clock.instant(), response.statusCode(),
                response.headers().firstValue("Content-Type"),
                response.headers().firstValueAsLong("Content-Length"), response.body());
        if (raw.httpStatus() < 200 || raw.httpStatus() >= 300) {
            throw new FeedFetchException(FeedFetchException.Kind.HTTP, source.sourceId(), raw, null);
        }
        return raw;
    }

    /** Flow serializes callbacks; every fetch gets its own subscriber and counter. */
    static final class BoundedBodySubscriber implements HttpResponse.BodySubscriber<byte[]> {
        private static final byte[] EMPTY = new byte[0];
        private final CompletableFuture<byte[]> result = new CompletableFuture<>();
        private byte[] storage = EMPTY;
        private final int limit;
        private int received;
        private Flow.Subscription subscription;
        private boolean done;

        BoundedBodySubscriber(int limit) {
            if (limit <= 0) {
                throw new IllegalArgumentException("limit must be positive");
            }
            this.limit = limit;
        }

        @Override
        public CompletionStage<byte[]> getBody() {
            return result;
        }

        @Override
        public void onSubscribe(Flow.Subscription incoming) {
            if (subscription != null || done) {
                incoming.cancel();
                return;
            }
            subscription = incoming;
            incoming.request(1);
        }

        @Override
        public void onNext(List<ByteBuffer> buffers) {
            if (done) {
                return;
            }
            int batchBytes = 0;
            for (ByteBuffer buffer : buffers) {
                // Subtraction avoids overflow, including across buffers in the same batch.
                if (buffer.remaining() > limit - received - batchBytes) {
                    var active = subscription;
                    try {
                        onError(new PayloadTooLargeException(limit));
                    } finally {
                        active.cancel();
                    }
                    return;
                }
                batchBytes += buffer.remaining();
            }
            int required = received + batchBytes;
            if (required > storage.length) {
                // Widen before doubling; neither capacity nor required can exceed limit.
                int capacity = (int) Math.min(limit,
                        Math.max(required, Math.max(1024L, 2L * storage.length)));
                storage = Arrays.copyOf(storage, capacity);
            }
            for (ByteBuffer buffer : buffers) {
                int length = buffer.remaining();
                buffer.get(storage, received, length);
                received += length;
            }
            // Request the next batch only after copying this one, including empty batches.
            subscription.request(1);
        }

        @Override
        public void onError(Throwable failure) {
            if (!done) {
                done = true;
                storage = EMPTY;
                subscription = null;
                result.completeExceptionally(failure);
            }
        }

        @Override
        public void onComplete() {
            if (!done) {
                done = true;
                byte[] body = received == storage.length ? storage : Arrays.copyOf(storage, received);
                storage = EMPTY;
                subscription = null;
                result.complete(body);
            }
        }
    }

    private static final class PayloadTooLargeException extends IOException {
        private static final long serialVersionUID = 1L;

        PayloadTooLargeException(int limit) {
            super("Response payload exceeds " + limit + " bytes");
        }
    }

}
