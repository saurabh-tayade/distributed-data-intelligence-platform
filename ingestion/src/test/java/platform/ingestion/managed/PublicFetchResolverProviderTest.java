package platform.ingestion.managed;

import java.nio.file.Path;
import java.nio.file.Files;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class PublicFetchResolverProviderTest {
    @TempDir Path temporary;
    static final String SERVICE = "META-INF/services/java.net.spi.InetAddressResolverProvider";
    static Path descriptor(Path path, String provider) throws Exception {
        try (var jar = new JarOutputStream(Files.newOutputStream(path))) {
            jar.putNextEntry(new JarEntry(SERVICE)); jar.write((provider + "\n").getBytes(StandardCharsets.UTF_8)); jar.closeEntry();
        }
        return path;
    }
    static Process start(String main, String property, String extraClasspath, String... args) throws Exception {
        var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(),
                "-Dddip.publicFetch.policy=" + property, "-cp",
                System.getProperty("surefire.test.class.path", System.getProperty("java.class.path")) + extraClasspath, main));
        command.addAll(List.of(args));
        var builder = new ProcessBuilder(command); builder.environment().clear(); return builder.start();
    }
    static void finish(Process child) throws Exception {
        try {
            assertTrue(child.waitFor(12, TimeUnit.SECONDS));
            String output = new String(child.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
            String error = new String(child.getErrorStream().readAllBytes(), StandardCharsets.UTF_8);
            assertEquals(0, child.exitValue(), output + error);
            if (!output.isEmpty()) System.out.print(output);
        } finally { child.destroyForcibly(); child.waitFor(5, TimeUnit.SECONDS); }
    }
    @ParameterizedTest @ValueSource(strings={"seal", "premature", "preinitialize", "class-presence", "caches", "mixed", "overflow", "delegate-fail", "concurrent", "interrupted", "delayed"})
    void actualSpiAttestationCachesSingleDelegateAndTerminalFailure(String mode) throws Exception {
        Path provider = descriptor(temporary.resolve("provider.jar"), PublicFetchNetworkPolicy.PROVIDER);
        finish(start(PublicFetchResolverTestDriver.class.getName(), PublicFetchNetworkPolicy.ID, ":" + provider, mode));
    }
    @ParameterizedTest @ValueSource(strings={"missing", "duplicate", "foreign", "twoLines"})
    void missingDuplicateAndForeignProvidersFailBeforeResolution(String mode) throws Exception {
        String classpath = "";
        if (!mode.equals("missing")) classpath += ":" + descriptor(temporary.resolve("provider.jar"), PublicFetchNetworkPolicy.PROVIDER);
        if (mode.equals("duplicate")) classpath += ":" + descriptor(temporary.resolve("duplicate.jar"), PublicFetchNetworkPolicy.PROVIDER);
        if (mode.equals("foreign")) classpath += ":" + descriptor(temporary.resolve("foreign.jar"), "nonexistent.ForeignProvider");
        if (mode.equals("twoLines")) classpath = ":" + descriptor(temporary.resolve("bad.jar"), PublicFetchNetworkPolicy.PROVIDER + "\nnonexistent.ForeignProvider");
        finish(start(PublicFetchResolverTestDriver.class.getName(), PublicFetchNetworkPolicy.ID, classpath, "packaging"));
    }
    @Test void fixedPropertyRequired() throws Exception {
        finish(start(PublicFetchResolverTestDriver.class.getName(), "wrong", "", "bad-property"));
    }
    @ParameterizedTest @ValueSource(strings={"jdk.internal.httpclient.disableHostnameVerification", "javax.net.ssl.trustStore", "jdk.net.hosts.file", "java.security.properties", "http.proxyHost", "java.net.useSystemProxies", "com.sun.security.enableAIAissuers"})
    void callerTlsProxyHostsAndSecurityOverridesReject(String key) throws Exception {
        finish(start(PublicFetchResolverTestDriver.class.getName(), PublicFetchNetworkPolicy.ID, "", "forbidden-property", key, "true"));
    }
    @Test void sharedClasspathContainsNoProviderServiceDescriptor() throws Exception {
        assertFalse(ClassLoader.getSystemClassLoader().getResources(SERVICE).hasMoreElements());
    }
}
