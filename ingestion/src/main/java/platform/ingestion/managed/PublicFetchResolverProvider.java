package platform.ingestion.managed;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.net.spi.InetAddressResolverProvider;
import java.nio.charset.StandardCharsets;
import java.security.Security;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.ServiceLoader;
import java.util.Set;
import java.util.stream.Stream;

/** Installed only by the separately hashed public-fetch-provider JAR. No pass-through mode. */
public final class PublicFetchResolverProvider extends InetAddressResolverProvider {
    static final String POLICY = "ddip-public-feed-pinned-v1";
    static final String PROPERTY = "ddip.publicFetch.policy";
    private static final String SERVICE = "META-INF/services/java.net.spi.InetAddressResolverProvider";
    private enum State { UNSEALED, RESOLVING, SEALED, FAILED }
    private static State state = State.UNSEALED;
    private static boolean configured;
    private static InetAddressResolver builtin;
    private static InetAddressResolver installed;
    private static Thread attestationThread;
    private static boolean attested;
    private static String hostname;
    private static InetAddress pin;

    public PublicFetchResolverProvider() { }
    @Override public String name() { return POLICY; }

    /** Must be the first network-related action in the fresh production main. */
    static synchronized void initialize() throws IOException {
        require(!configured && state == State.UNSEALED && builtin == null);
        require(POLICY.equals(System.getProperty(PROPERTY)) && Runtime.version().feature() == 21);
        Set<String> forbiddenEnvironment = Set.of("JAVA_TOOL_OPTIONS", "JDK_JAVA_OPTIONS", "_JAVA_OPTIONS", "CLASSPATH",
                "HTTP_PROXY", "HTTPS_PROXY", "ALL_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "all_proxy", "no_proxy");
        for (String key : forbiddenEnvironment) require(System.getenv(key) == null);
        for (String argument : ManagementFactory.getRuntimeMXBean().getInputArguments())
            require(!argument.startsWith("-javaagent") && !argument.startsWith("-agentlib") && !argument.startsWith("-agentpath")
                    && !argument.startsWith("-Xbootclasspath"));
        for (String key : System.getProperties().stringPropertyNames()) {
            String lower = key.toLowerCase(Locale.ROOT);
            require(!lower.contains("proxy") && !lower.equals("java.net.usesystemproxies") && !lower.startsWith("javax.net.ssl.")
                    && !lower.equals("jdk.internal.httpclient.disablehostnameverification")
                    && !lower.equals("jdk.net.hosts.file") && !lower.equals("java.security.properties")
                    && !lower.equals("com.sun.net.ssl.checkrevocation") && !lower.equals("com.sun.security.enableaiaissuers")
                    && !lower.equals("java.net.preferipv6addresses") && !lower.equals("java.net.preferipv4stack"));
        }
        for (String key : List.of("networkaddress.cache.ttl", "networkaddress.cache.negative.ttl", "networkaddress.cache.stale.ttl")) {
            Security.setProperty(key, "0");
            require("0".equals(Security.getProperty(key)));
        }
        configured = true;
    }

    @Override public InetAddressResolver get(Configuration configuration) {
        synchronized (PublicFetchResolverProvider.class) {
            if (!configured || state != State.UNSEALED || builtin != null || attestationThread != Thread.currentThread()) {
                state = State.FAILED;
                throw new IllegalStateException("PUBLIC_FETCH_RESOLVER_REJECTED");
            }
            builtin = configuration.builtinResolver(); // Capture only; do not resolve in provider initialization.
            installed = new InetAddressResolver() {
                @Override public Stream<InetAddress> lookupByName(String host, LookupPolicy policy) throws UnknownHostException {
                    return lookup(host, policy);
                }
                @Override public String lookupByAddress(byte[] address) throws UnknownHostException {
                    throw denied();
                }
            };
            return installed;
        }
    }

    /** Resource uniqueness and provider types are checked before forcing actual SPI selection. */
    static void attest(String host) throws IOException {
        synchronized (PublicFetchResolverProvider.class) {
            require(configured && state == State.UNSEALED && !attested && attestationThread == null);
            require(host != null && !host.isEmpty());
            hostname = host.toLowerCase(Locale.ROOT);
            ClassLoader loader = ClassLoader.getSystemClassLoader();
            var resources = Collections.list(loader.getResources(SERVICE));
            require(resources.size() == 1);
            try (var input = resources.getFirst().openStream()) {
                byte[] descriptor = input.readNBytes(4097);
                require(descriptor.length <= 4096 && new String(descriptor, StandardCharsets.UTF_8)
                        .equals(PublicFetchResolverProvider.class.getName() + "\n"));
            }
            var providers = ServiceLoader.load(InetAddressResolverProvider.class, loader).stream().limit(2).toList();
            require(providers.size() == 1 && providers.getFirst().type() == PublicFetchResolverProvider.class);
            attestationThread = Thread.currentThread();
        }
        try {
            InetAddress.getAllByName(hostname);
            synchronized (PublicFetchResolverProvider.class) { state = State.FAILED; }
            throw denied();
        } catch (UnknownHostException expected) {
            synchronized (PublicFetchResolverProvider.class) {
                require(attested && installed != null && builtin != null && state == State.UNSEALED);
            }
        } finally {
            synchronized (PublicFetchResolverProvider.class) { attestationThread = null; }
        }
    }

    private static synchronized Stream<InetAddress> lookup(String host, InetAddressResolver.LookupPolicy policy)
            throws UnknownHostException {
        if (state == State.UNSEALED && attestationThread == Thread.currentThread() && host != null && hostname.equals(host.toLowerCase(Locale.ROOT))) {
            attested = true;
            throw denied();
        }
        if (state != State.SEALED || host == null || !hostname.equals(host.toLowerCase(Locale.ROOT))
                || (policy.characteristics() & InetAddressResolver.LookupPolicy.IPV4) == 0) throw denied();
        return Stream.of(pin);
    }

    /** Exactly one built-in A lookup. All failures are terminal; no timeout grants a replacement lookup. */
    static void resolveAndSeal(String host) throws IOException {
        InetAddressResolver delegate;
        synchronized (PublicFetchResolverProvider.class) {
            require(state == State.UNSEALED && attested && attestationThread == null && host != null && hostname.equals(host.toLowerCase(Locale.ROOT)));
            require(!Thread.currentThread().isInterrupted());
            state = State.RESOLVING;
            delegate = builtin;
        }
        try {
            InetAddress selected = PublicFetchAddressPolicy.choose(hostname,
                    delegate.lookupByName(hostname, InetAddressResolver.LookupPolicy.of(InetAddressResolver.LookupPolicy.IPV4)));
            synchronized (PublicFetchResolverProvider.class) {
                require(state == State.RESOLVING && !Thread.currentThread().isInterrupted());
                pin = selected;
                state = State.SEALED;
            }
        } catch (IOException | RuntimeException | Error failure) {
            synchronized (PublicFetchResolverProvider.class) { state = State.FAILED; pin = null; }
            throw failure;
        }
    }
    private static void require(boolean condition) throws IOException {
        if (!condition) { state = State.FAILED; pin = null; throw new IOException("PUBLIC_FETCH_RESOLVER_REJECTED"); }
    }
    private static UnknownHostException denied() { return new UnknownHostException("PUBLIC_FETCH_RESOLVER_REJECTED"); }
}
