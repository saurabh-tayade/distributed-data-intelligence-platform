package platform.ingestion.managed;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class PublicFetchTransportTest {
    @TempDir Path temporary;
    @ParameterizedTest @ValueSource(strings={"http", "https", "wrongcert", "redirect", "preconnect", "acceptdrop", "responsedrop", "malformed", "timeout", "interrupt", "limit", "overflow"})
    void pinnedOriginalAuthorityDefaultTlsNoProxyRedirectOrApplicationRetry(String mode) throws Exception {
        Path provider = PublicFetchResolverProviderTest.descriptor(temporary.resolve("provider.jar"), PublicFetchNetworkPolicy.PROVIDER);
        Path keys = temporary.resolve("fixture.p12");
        if (mode.equals("https") || mode.equals("wrongcert")) {
            String hostname = mode.equals("https") ? PublicFetchResolverTestDriver.HOST : "wrong.test.invalid";
            var builder = new ProcessBuilder(List.of(Path.of(System.getProperty("java.home"), "bin/keytool").toString(),
                    "-genkeypair", "-alias", "fixture", "-keyalg", "RSA", "-keysize", "2048", "-validity", "2", "-dname", "CN=" + hostname,
                    "-ext", "SAN=dns:" + hostname, "-storetype", "PKCS12", "-keystore", keys.toString(), "-storepass", "fixture-only", "-keypass", "fixture-only"));
            builder.environment().clear(); var keytool = builder.start();
            assertTrue(keytool.waitFor(30, TimeUnit.SECONDS));
            assertEquals(0, keytool.exitValue(), new String(keytool.getErrorStream().readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        }
        PublicFetchResolverProviderTest.finish(PublicFetchResolverProviderTest.start(PublicFetchTransportTestDriver.class.getName(),
                PublicFetchNetworkPolicy.ID, ":" + provider, mode, keys.toString()));
    }
}
