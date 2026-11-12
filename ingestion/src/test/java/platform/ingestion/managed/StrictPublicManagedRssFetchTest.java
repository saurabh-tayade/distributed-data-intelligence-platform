package platform.ingestion.managed;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.junit.jupiter.api.Assertions.*;

class StrictPublicManagedRssFetchTest {
    @TempDir Path temporary;
    static void publicPacket(StrictFetchFixture f) throws Exception {
        var endpoint = StrictFetchJson.object().put("endpointId", "12345678-1234-1234-1234-123456789abc")
                .put("sourceId", "demo-feed").put("uri", "https://pinned.test.invalid/feed");
        var policy = PublicFetchNetworkPolicy.policy();
        f.packet.put("bootstrapVersion", 2).put("mode", "PUBLIC_FETCH_SOURCE_CANDIDATE")
                .put("childPid", Long.toString(ProcessHandle.current().pid()));
        var fetch = (ObjectNode)f.packet.get("fetch");
        fetch.put("testEndpoint", false).put("feedUri", endpoint.get("uri").textValue())
                .put("endpointSha256", StrictFetchJson.sha(StrictFetchJson.canonical(endpoint)));
        var pub = f.packet.putObject("publicFetch").put("launchProfileId", "Public_fetch-profile-2")
                .put("launchProfileSha256", "1".repeat(64)).put("networkPolicySha256", StrictFetchJson.sha(StrictFetchJson.canonical(policy)));
        pub.set("selectedEndpoint", endpoint); pub.set("networkPolicy", policy);
        for (var entry : fetch.properties()) f.commandNode.set(entry.getKey(), entry.getValue());
        byte[] command = StrictFetchJson.canonical(f.commandNode);
        StrictFetchFixture.privateWrite(f.directory.resolve("command.json"), command);
        for (String key : List.of("expected", "current"))
            ((ObjectNode)f.packet.get(key).get("writer")).put("commandSha256", StrictFetchJson.sha(command));
    }
    @Test void canonicalPolicyMatchesIndependentApiMirror() throws Exception {
        String expected = "{\"addressFamily\":\"IPV4_ONLY\",\"addressPolicySha256\":\"85959d534b478bf1c8194e99f49a2c860f9fe9864978ed7118496913767cefcd\",\"addressPolicyVersion\":1,\"httpVersion\":\"HTTP_1_1\",\"maxApplicationInvocations\":1,\"maxResolvedAddresses\":64,\"policyId\":\"ddip-public-feed-pinned-v1\",\"policyVersion\":1,\"proxies\":false,\"redirects\":false,\"resolverProvider\":\"platform.ingestion.managed.PublicFetchResolverProvider\",\"transportRetryPolicy\":\"JDK_BOUNDED_PINNED_V1\"}";
        byte[] bytes = StrictFetchJson.canonical(PublicFetchNetworkPolicy.policy());
        assertEquals(expected, new String(bytes, StandardCharsets.UTF_8));
        assertEquals("86e5e8ef709e0b1a7472515ca2569914668565c4a39785b52b6c4c36e4f8b808", StrictFetchJson.sha(bytes));
    }
    @Test void exactPublicPacketParsesWithoutResolvingAndLegacyParserRejectsIt() throws Exception {
        try (var f = new StrictFetchFixture(temporary)) {
            publicPacket(f);
            byte[] packet = StrictFetchJson.canonical(f.packet);
            var parsed = StrictPublicManagedRssFetch.parse(packet);
            assertFalse(parsed.common().fetch().get("testEndpoint").booleanValue());
            assertEquals(0, f.requests.get());
            assertThrows(IOException.class, () -> StrictManagedRssFetch.parse(packet));
            ((ObjectNode)parsed.publicFetch()).put("unexpected", 1);
            assertFalse(parsed.publicFetch().has("unexpected"));
        }
    }
    @ParameterizedTest @ValueSource(strings={"version", "mode", "operationId", "unknown", "testEndpoint", "launchId", "launchIdLength", "launchHash", "policyHash", "policy", "endpointId", "endpointHash", "source", "uri", "endpointUnknown", "privateUri", "literalUri", "duplicate", "trailing", "legacy"})
    void exactSchemasDigestsAndPublicEndpointCorrespondence(String fault) throws Exception {
        try (var f = new StrictFetchFixture(temporary)) {
            publicPacket(f);
            var pub = (ObjectNode)f.packet.get("publicFetch");
            var endpoint = (ObjectNode)pub.get("selectedEndpoint");
            switch (fault) {
                case "version" -> f.packet.put("bootstrapVersion", 1);
                case "mode" -> f.packet.put("mode", "FETCH_SOURCE_CANDIDATE");
                case "operationId" -> f.packet.put("operationId", UUID.randomUUID().toString());
                case "unknown" -> pub.put("unexpected", true);
                case "testEndpoint" -> ((ObjectNode)f.packet.get("fetch")).put("testEndpoint", true);
                case "launchId" -> pub.put("launchProfileId", "invalid/profile");
                case "launchIdLength" -> pub.put("launchProfileId", "a".repeat(129));
                case "launchHash" -> pub.put("launchProfileSha256", "A".repeat(64));
                case "policyHash" -> pub.put("networkPolicySha256", "a".repeat(64));
                case "policy" -> ((ObjectNode)pub.get("networkPolicy")).put("redirects", true);
                case "endpointId" -> endpoint.put("endpointId", "not-a-uuid");
                case "endpointHash" -> ((ObjectNode)f.packet.get("fetch")).put("endpointSha256", "a".repeat(64));
                case "source" -> endpoint.put("sourceId", "other-source");
                case "uri" -> endpoint.put("uri", "https://other.test.invalid/feed");
                case "endpointUnknown" -> endpoint.put("unexpected", true);
                case "privateUri", "literalUri" -> {
                    endpoint.put("uri", fault.equals("privateUri") ? "http://localhost/feed" : "http://127.0.0.1/feed");
                    ((ObjectNode)f.packet.get("fetch")).put("feedUri", endpoint.get("uri").textValue())
                            .put("endpointSha256", StrictFetchJson.sha(StrictFetchJson.canonical(endpoint)));
                }
                case "legacy" -> { f.packet.remove("publicFetch"); f.packet.put("bootstrapVersion", 1).put("mode", "FETCH_SOURCE_CANDIDATE"); }
                default -> { }
            }
            String packet = new String(StrictFetchJson.canonical(f.packet), StandardCharsets.UTF_8);
            if (fault.equals("duplicate")) packet = packet.replace("\"bootstrapVersion\":2", "\"bootstrapVersion\":2,\"bootstrapVersion\":2");
            if (fault.equals("trailing")) packet += "{}";
            byte[] bytes = packet.getBytes(StandardCharsets.UTF_8);
            assertThrows(IOException.class, () -> StrictPublicManagedRssFetch.parse(bytes));
            assertEquals(0, f.requests.get()); assertFalse(Files.exists(f.directory.resolve("result.json.preparing")));
        }
    }
    @Test void validPublicPacketWaitsForEofThenConsumesResultPrefixBeforeProviderSelectionFailure() throws Exception {
        try (var f = new StrictFetchFixture(temporary)) {
            publicPacket(f);
            Process child = PublicFetchResolverProviderTest.start(StrictPublicManagedRssFetch.class.getName(), PublicFetchNetworkPolicy.ID, "");
            try {
                var before = StrictManagedRssFetchTest.snapshot(f.root);
                child.getOutputStream().write(f.packet(child)); child.getOutputStream().flush();
                assertFalse(child.waitFor(300, TimeUnit.MILLISECONDS));
                assertEquals(before, StrictManagedRssFetchTest.snapshot(f.root));
                child.getOutputStream().close(); assertTrue(child.waitFor(8, TimeUnit.SECONDS));
                assertEquals(2, child.exitValue());
                assertEquals(StrictPublicManagedRssFetch.ERROR + "\n", new String(child.getErrorStream().readAllBytes(), StandardCharsets.UTF_8));
                assertTrue(Files.isRegularFile(f.directory.resolve("result.json.preparing")));
                assertEquals(0, Files.size(f.directory.resolve("result.json.preparing")));
                assertFalse(Files.exists(f.directory.resolve("payload.xml"))); assertEquals(0, f.requests.get());
                var retained = StrictManagedRssFetchTest.snapshot(f.root);
                Process again = PublicFetchResolverProviderTest.start(StrictPublicManagedRssFetch.class.getName(), PublicFetchNetworkPolicy.ID, "");
                try {
                    again.getOutputStream().write(f.packet(again)); again.getOutputStream().close();
                    assertTrue(again.waitFor(8, TimeUnit.SECONDS)); assertEquals(2, again.exitValue());
                    assertEquals(retained, StrictManagedRssFetchTest.snapshot(f.root));
                } finally { StrictManagedRssFetchTest.kill(again); }
            } finally { StrictManagedRssFetchTest.kill(child); }
        }
    }
    @Test void commandMismatchFailsBeforeResultConsumptionOrProviderSelection() throws Exception {
        try (var f = new StrictFetchFixture(temporary)) {
            publicPacket(f);
            f.commandNode.put("revision", 8);
            byte[] changed = StrictFetchJson.canonical(f.commandNode);
            StrictFetchFixture.privateWrite(f.directory.resolve("command.json"), changed);
            for (String key : List.of("expected", "current"))
                ((ObjectNode)f.packet.get(key).get("writer")).put("commandSha256", StrictFetchJson.sha(changed));
            var before = StrictManagedRssFetchTest.snapshot(f.root);
            Process child = PublicFetchResolverProviderTest.start(StrictPublicManagedRssFetch.class.getName(), PublicFetchNetworkPolicy.ID, "");
            try {
                child.getOutputStream().write(f.packet(child)); child.getOutputStream().close();
                assertTrue(child.waitFor(8, TimeUnit.SECONDS)); assertEquals(2, child.exitValue());
                assertEquals(before, StrictManagedRssFetchTest.snapshot(f.root)); assertEquals(0, f.requests.get());
            } finally { StrictManagedRssFetchTest.kill(child); }
        }
    }
}
