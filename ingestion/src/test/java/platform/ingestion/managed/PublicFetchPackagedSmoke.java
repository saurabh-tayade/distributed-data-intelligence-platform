package platform.ingestion.managed;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;
import java.util.zip.ZipFile;

/** Explicit post-package, offline smoke. Launched children use production classes only, never test delegates. */
public final class PublicFetchPackagedSmoke {
    private static void check(boolean value) { if (!value) throw new AssertionError("packaged smoke rejected"); }
    private static Map<String,String> snapshot(Path root) throws Exception {
        var result = new TreeMap<String,String>();
        try (var files = Files.walk(root)) {
            for (Path path : files.filter(Files::isRegularFile).toList())
                result.put(root.relativize(path).toString(), StrictFetchJson.sha(Files.readAllBytes(path)));
        }
        return result;
    }
    public static void main(String[] args) throws Exception {
        Path target = Path.of(args[0]).toAbsolutePath();
        Path shared = target.resolve("ingestion-0.1.0-SNAPSHOT.jar"), provider = target.resolve("ingestion-0.1.0-SNAPSHOT-public-fetch-provider.jar");
        try (var jar = new ZipFile(shared.toFile())) {
            check(jar.stream().noneMatch(e -> e.getName().contains("PublicFetchResolverProvider")
                    || e.getName().contains("PublicFetchAddressPolicy") || e.getName().equals(PublicFetchResolverProviderTest.SERVICE)));
        }
        try (var jar = new ZipFile(provider.toFile())) {
            List<String> classes = jar.stream().map(java.util.zip.ZipEntry::getName).filter(n -> n.endsWith(".class")).sorted().toList();
            check(classes.equals(List.of("platform/ingestion/managed/PublicFetchAddressPolicy.class",
                    "platform/ingestion/managed/PublicFetchResolverProvider$1.class", "platform/ingestion/managed/PublicFetchResolverProvider$State.class",
                    "platform/ingestion/managed/PublicFetchResolverProvider.class")));
            try (var in = jar.getInputStream(jar.getEntry(PublicFetchResolverProviderTest.SERVICE))) {
                check(new String(in.readAllBytes(), StandardCharsets.UTF_8).equals(PublicFetchNetworkPolicy.PROVIDER + "\n"));
            }
        }
        String classpath = shared + ":" + provider + ":" + target.resolve("dependency/*");
        for (String mode : List.of("retained-prefix", "wrong-policy", "duplicate-service")) {
            Path temporary = Files.createTempDirectory(Path.of(args[1]), "packaged-public-");
            try (var f = new StrictFetchFixture(temporary)) {
                StrictPublicManagedRssFetchTest.publicPacket(f);
                if (mode.equals("retained-prefix")) StrictFetchFixture.privateWrite(f.directory.resolve("result.json.preparing"), new byte[0]);
                else if (mode.equals("wrong-policy")) ((ObjectNode)f.packet.get("publicFetch").get("networkPolicy")).put("redirects", true);
                String childClasspath = classpath;
                if (mode.equals("duplicate-service")) childClasspath += ":" + PublicFetchResolverProviderTest.descriptor(temporary.resolve("duplicate-service.jar"), PublicFetchNetworkPolicy.PROVIDER);
                var before = snapshot(f.root);
                var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin/java").toString(), "-Xmx128m",
                        "-Dddip.publicFetch.policy=" + PublicFetchNetworkPolicy.ID, "-cp", childClasspath, StrictPublicManagedRssFetch.class.getName()));
                var builder = new ProcessBuilder(command); builder.environment().clear(); Process child = builder.start();
                try {
                    child.getOutputStream().write(f.packet(child)); child.getOutputStream().close();
                    check(child.waitFor(10, TimeUnit.SECONDS)); check(child.exitValue() == 2);
                    check(child.getInputStream().readAllBytes().length == 0);
                    check(new String(child.getErrorStream().readAllBytes(), StandardCharsets.UTF_8).equals(StrictPublicManagedRssFetch.ERROR + "\n"));
                    var after = snapshot(f.root);
                    if (mode.equals("duplicate-service")) {
                        Path marker = f.directory.resolve("result.json.preparing");
                        check(Files.isRegularFile(marker) && Files.size(marker) == 0);
                        after.remove(f.root.relativize(marker).toString());
                    }
                    check(before.equals(after) && f.requests.get() == 0);
                } finally { child.destroyForcibly(); child.waitFor(5, TimeUnit.SECONDS); }
            }
            System.out.println("PACKAGED PUBLIC MAIN PASS " + mode + " (no DNS/HTTP capability reached)");
        }
        System.out.println("PACKAGING PASS production JAR isolation; positive production egress remains unqualified");
    }
    private PublicFetchPackagedSmoke() { }
}
