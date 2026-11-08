package platform.ingestion.managed;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.Proxy;
import java.net.ProxySelector;
import java.net.ServerSocket;
import java.net.SocketAddress;
import java.net.SocketTimeoutException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.time.Clock;
import java.time.Duration;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import javax.net.ssl.ExtendedSSLSession;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLHandshakeException;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.TrustManagerFactory;
import platform.ingestion.rss.FeedFetchException;
import platform.ingestion.rss.HttpFeedFetcher;
import platform.ingestion.rss.RssSource;
import static platform.ingestion.managed.PublicFetchResolverTestDriver.*;

/** Only this test launcher can install synthetic trust and loopback pins; no production switch/delegate does so. */
public final class PublicFetchTransportTestDriver {
    static KeyStore keys(Path path) throws Exception {
        KeyStore keys = KeyStore.getInstance("PKCS12");
        try (var in = Files.newInputStream(path)) { keys.load(in, "fixture-only".toCharArray()); }
        return keys;
    }
    static SSLContext serverContext(Path path) throws Exception {
        var km = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        km.init(keys(path), "fixture-only".toCharArray());
        var ssl = SSLContext.getInstance("TLS"); ssl.init(km.getKeyManagers(), null, null); return ssl;
    }
    static void installTestTrust(Path path) throws Exception {
        var tm = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm()); tm.init(keys(path));
        var ssl = SSLContext.getInstance("TLS"); ssl.init(null, tm.getTrustManagers(), null); SSLContext.setDefault(ssl);
    }
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        PublicFetchResolverProvider.initialize();
        PublicFetchResolverProvider.attest(HOST);
        var delegates = installDelegate("rebind", null, null);
        PublicFetchResolverProvider.resolveAndSeal(HOST);
        // This synthetic substitution is confined to the test launcher, after real policy tests/sealing.
        set("pin", InetAddress.getByAddress(HOST, new byte[]{127,0,0,1}));
        boolean tls = mode.equals("https") || mode.equals("wrongcert");
        if (tls) installTestTrust(Path.of(args[1]));
        ServerSocket server = tls ? serverContext(Path.of(args[1])).getServerSocketFactory().createServerSocket() : new ServerSocket();
        server.bind(new InetSocketAddress(InetAddress.getByAddress(new byte[]{127,0,0,1}), 0));
        server.setSoTimeout(500);
        int port = server.getLocalPort();
        var replacement = new ServerSocket();
        replacement.bind(new InetSocketAddress(InetAddress.getByAddress(new byte[]{127,0,0,2}), port)); replacement.setSoTimeout(500);
        var connections = new AtomicInteger(); var requests = new AtomicInteger(); var replacements = new AtomicInteger();
        var applicationInvocations = new AtomicInteger(); var done = new CountDownLatch(1); var requestSeen = new CountDownLatch(1);
        var hosts = new CopyOnWriteArrayList<String>(); var snis = new CopyOnWriteArrayList<String>();
        var serverFailure = new AtomicReference<Throwable>();
        Thread listener = Thread.ofPlatform().start(() -> {
            while (done.getCount() > 0) {
                try (var socket = server.accept()) {
                    connections.incrementAndGet(); check(socket.getLocalAddress().getHostAddress().equals("127.0.0.1"));
                    socket.setSoTimeout(2500);
                    if (mode.equals("acceptdrop")) continue;
                    if (socket instanceof SSLSocket ssl) {
                        try { ssl.startHandshake(); snis.add(((ExtendedSSLSession)ssl.getSession()).getRequestedServerNames().toString()); }
                        catch (IOException expected) { if (!mode.equals("wrongcert")) throw expected; continue; }
                    }
                    var reader = new BufferedReader(new InputStreamReader(socket.getInputStream(), StandardCharsets.ISO_8859_1));
                    String line = reader.readLine(); if (line == null) continue;
                    check(line.equals("GET /feed HTTP/1.1")); requests.incrementAndGet(); requestSeen.countDown();
                    while ((line = reader.readLine()) != null && !line.isEmpty())
                        if (line.toLowerCase(Locale.ROOT).startsWith("host:")) hosts.add(line.substring(5).trim());
                    if (mode.equals("responsedrop")) continue;
                    if (mode.equals("timeout") || mode.equals("interrupt")) { done.await(4, TimeUnit.SECONDS); continue; }
                    String response = switch (mode) {
                        case "redirect" -> "HTTP/1.1 302 Found\r\nLocation: http://other.test.invalid:" + port + "/second\r\nContent-Length: 0\r\nConnection: close\r\n\r\n";
                        case "malformed" -> "NOT HTTP\r\n\r\n";
                        default -> "HTTP/1.1 200 OK\r\nContent-Length: 2\r\nConnection: close\r\n\r\nOK";
                    };
                    socket.getOutputStream().write(response.getBytes(StandardCharsets.US_ASCII)); socket.getOutputStream().flush();
                } catch (SocketTimeoutException expected) { /* bounded check for fixture completion */ }
                catch (IOException error) { if (!server.isClosed()) serverFailure.compareAndSet(null, error); }
                catch (Throwable error) { serverFailure.compareAndSet(null, error); }
            }
        });
        Thread other = Thread.ofPlatform().start(() -> {
            while (done.getCount() > 0) try (var socket = replacement.accept()) {
                replacements.incrementAndGet(); check(socket.isConnected());
            } catch (SocketTimeoutException expected) { /* bounded fixture shutdown */ }
            catch (IOException error) { if (!replacement.isClosed()) serverFailure.compareAndSet(null, error); }
        });
        if (mode.equals("preconnect")) server.close();
        ProxySelector.setDefault(new ProxySelector() {
            @Override public List<Proxy> select(URI uri) { throw new AssertionError("hostile default proxy selected"); }
            @Override public void connectFailed(URI uri, SocketAddress address, IOException failure) { throw new AssertionError(failure); }
        });
        var outcome = new AtomicReference<FeedFetchException>(); var unexpected = new AtomicReference<Throwable>();
        var interrupted = new AtomicReference<Boolean>(false);
        long started = System.nanoTime();
        try (var client = StrictPublicManagedRssFetch.client()) {
            check(client.version() == java.net.http.HttpClient.Version.HTTP_1_1);
            check(client.followRedirects() == java.net.http.HttpClient.Redirect.NEVER);
            check(client.authenticator().isEmpty() && client.cookieHandler().isEmpty());
            Thread fetch = Thread.ofPlatform().start(() -> {
                try {
                    applicationInvocations.incrementAndGet();
                    var feed = new HttpFeedFetcher(client, Duration.ofMillis(1200), Clock.systemUTC())
                            .fetch(new RssSource("fixture", URI.create((tls ? "https" : "http") + "://" + HOST + ":" + port + "/feed"), true));
                    check(new String(feed.body(), StandardCharsets.US_ASCII).equals("OK"));
                } catch (FeedFetchException failure) { outcome.set(failure); }
                catch (Throwable failure) { unexpected.set(failure); }
                finally { interrupted.set(Thread.currentThread().isInterrupted()); done.countDown(); }
            });
            if (mode.equals("interrupt")) { check(requestSeen.await(4, TimeUnit.SECONDS)); fetch.interrupt(); }
            fetch.join(6000); check(!fetch.isAlive()); check(unexpected.get() == null);
        } finally {
            done.countDown(); server.close(); replacement.close(); listener.join(5000); other.join(5000);
        }
        check(serverFailure.get() == null); check(!listener.isAlive() && !other.isAlive());
        check(applicationInvocations.get() == 1 && delegates.get() == 1 && replacements.get() == 0);
        check(System.nanoTime() - started < TimeUnit.SECONDS.toNanos(8));
        check(hosts.stream().allMatch(h -> h.equals(HOST + ":" + port)));
        if (mode.equals("http") || mode.equals("https")) {
            check(outcome.get() == null && requests.get() >= 1);
            if (tls) check(snis.toString().contains(HOST));
        } else {
            check(outcome.get() != null);
            if (mode.equals("redirect")) check(outcome.get().kind() == FeedFetchException.Kind.HTTP && requests.get() == 1);
            if (mode.equals("interrupt")) check(outcome.get().kind() == FeedFetchException.Kind.INTERRUPTED && interrupted.get());
            if (mode.equals("wrongcert")) {
                check(outcome.get().getCause() instanceof SSLHandshakeException);
                check(outcome.get().getCause().getMessage().contains("No subject alternative DNS name matching"));
            }
        }
        // Keep wire ambiguity visible: internal JDK retries need not be exactly one request.
        System.out.println(mode + " application=" + applicationInvocations + " delegates=" + delegates
                + " connections=" + connections + " requests=" + requests + " otherDestination=" + replacements);
    }
    private PublicFetchTransportTestDriver() { }
}
