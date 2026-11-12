package platform.ingestion.managed;

import java.io.IOException;
import java.lang.reflect.Field;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.net.spi.InetAddressResolver;
import java.security.Security;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;

/** Test-only reflection/delegates. Never present in either production JAR or reachable from the production main. */
public final class PublicFetchResolverTestDriver {
    static final String HOST = "pinned.test.invalid";
    static void check(boolean condition) { if (!condition) throw new AssertionError("probe failed"); }
    static Object get(String name) throws Exception {
        Field field = PublicFetchResolverProvider.class.getDeclaredField(name); field.setAccessible(true); return field.get(null);
    }
    static void set(String name, Object value) throws Exception {
        Field field = PublicFetchResolverProvider.class.getDeclaredField(name); field.setAccessible(true); field.set(null, value);
    }
    static void reject(Checked action) throws Exception {
        try { action.run(); throw new AssertionError("unexpectedly accepted"); } catch (IOException | IllegalStateException expected) { }
    }
    @FunctionalInterface interface Checked { void run() throws Exception; }
    static AtomicInteger installDelegate(String mode, CountDownLatch entered, CountDownLatch release) throws Exception {
        var calls = new AtomicInteger();
        set("builtin", new InetAddressResolver() {
            @Override public Stream<InetAddress> lookupByName(String host, LookupPolicy policy) throws UnknownHostException {
                calls.incrementAndGet(); check(host.equals(HOST)); check(policy.characteristics() == LookupPolicy.IPV4);
                if (entered != null) entered.countDown();
                if (release != null) {
                    boolean interrupted = false;
                    while (true) try { release.await(); break; } catch (InterruptedException ignored) { interrupted = true; }
                    if (interrupted) Thread.currentThread().interrupt();
                }
                if (mode.equals("delegate-fail")) throw new UnknownHostException("fixture");
                if (mode.equals("rebind") && calls.get() > 1)
                    return Stream.of(InetAddress.getByAddress(new byte[]{127,0,0,2}));
                var ip = InetAddress.getByAddress(new byte[]{8,8,8,8});
                if (mode.equals("mixed")) return Stream.of(ip, InetAddress.getByAddress(new byte[]{127,0,0,1}));
                if (mode.equals("overflow")) return Stream.generate(() -> ip);
                return Stream.of(ip, InetAddress.getByAddress(new byte[]{1,1,1,1}), ip);
            }
            @Override public String lookupByAddress(byte[] address) { throw new AssertionError("reverse delegate called"); }
        });
        return calls;
    }
    public static void main(String[] args) throws Exception {
        String mode = args[0];
        if (mode.equals("forbidden-property")) {
            System.setProperty(args[1], args[2]); reject(PublicFetchResolverProvider::initialize); return;
        }
        if (mode.equals("caches")) {
            for (String key : new String[]{"networkaddress.cache.ttl", "networkaddress.cache.negative.ttl", "networkaddress.cache.stale.ttl"})
                Security.setProperty(key, "9999");
        }
        if (mode.equals("preinitialize")) {
            try { InetAddress.getAllByName(HOST); } catch (Exception expected) { }
            reject(PublicFetchResolverProvider::initialize); return;
        }
        if (mode.equals("bad-property")) { reject(PublicFetchResolverProvider::initialize); return; }
        PublicFetchResolverProvider.initialize();
        for (String key : new String[]{"networkaddress.cache.ttl", "networkaddress.cache.negative.ttl", "networkaddress.cache.stale.ttl"})
            check("0".equals(Security.getProperty(key)));
        if (mode.equals("class-presence")) {
            check(new PublicFetchResolverProvider().name().equals(PublicFetchNetworkPolicy.ID));
            reject(() -> PublicFetchResolverProvider.resolveAndSeal(HOST)); return;
        }
        if (mode.equals("premature")) {
            try { InetAddress.getAllByName(HOST); } catch (Exception expected) { }
            reject(() -> PublicFetchResolverProvider.attest(HOST)); return;
        }
        if (mode.equals("packaging")) { reject(() -> PublicFetchResolverProvider.attest(HOST)); return; }
        PublicFetchResolverProvider.attest(HOST);
        check(Boolean.TRUE.equals(get("attested")) && get("installed") != null && get("builtin") != null);
        if (mode.equals("concurrent") || mode.equals("interrupted") || mode.equals("delayed")) {
            var entered = new CountDownLatch(1); var release = new CountDownLatch(1);
            var calls = installDelegate(mode, entered, release); var outcome = new AtomicReference<Throwable>();
            Thread resolver = Thread.ofPlatform().start(() -> {
                try { PublicFetchResolverProvider.resolveAndSeal(HOST); } catch (Throwable failure) { outcome.set(failure); }
            });
            entered.await(); reject(() -> InetAddress.getAllByName(HOST));
            if (mode.equals("delayed")) Thread.sleep(5500);
            if (mode.equals("interrupted")) resolver.interrupt();
            release.countDown(); resolver.join(3000); check(!resolver.isAlive());
            check(calls.get() == 1);
            if (mode.equals("interrupted")) {
                check(outcome.get() instanceof IOException); reject(() -> InetAddress.getAllByName(HOST));
                reject(() -> PublicFetchResolverProvider.resolveAndSeal(HOST));
            } else check(outcome.get() == null);
            check(calls.get() == 1); return;
        }
        var calls = installDelegate(mode, null, null);
        if (mode.equals("mixed") || mode.equals("overflow") || mode.equals("delegate-fail")) {
            reject(() -> PublicFetchResolverProvider.resolveAndSeal(HOST));
            reject(() -> PublicFetchResolverProvider.resolveAndSeal(HOST));
            reject(() -> InetAddress.getAllByName(HOST)); check(calls.get() == 1); return;
        }
        PublicFetchResolverProvider.resolveAndSeal(HOST);
        check(calls.get() == 1);
        check(InetAddress.getByName(HOST).getHostAddress().equals("1.1.1.1")); // Prior negative cache cannot win.
        var installed = (InetAddressResolver)get("installed");
        reject(() -> installed.lookupByName("other.test.invalid", InetAddressResolver.LookupPolicy.of(InetAddressResolver.LookupPolicy.IPV4)));
        reject(() -> installed.lookupByAddress(new byte[]{1,1,1,1}));
        reject(() -> installed.lookupByName(HOST, InetAddressResolver.LookupPolicy.of(InetAddressResolver.LookupPolicy.IPV6)));
        // Test-only pin replacement demonstrates positive cache policy initialization was not premature.
        set("pin", InetAddress.getByAddress(HOST, new byte[]{8,8,8,8}));
        check(InetAddress.getByName(HOST).getHostAddress().equals("8.8.8.8"));
        check(InetAddress.getByName(HOST).getHostAddress().equals("8.8.8.8"));
        reject(() -> PublicFetchResolverProvider.resolveAndSeal(HOST));
        check(calls.get() == 1); reject(() -> InetAddress.getByName(HOST));
    }
    private PublicFetchResolverTestDriver() { }
}
