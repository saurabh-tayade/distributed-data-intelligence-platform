package platform.ingestion.managed;

import com.fasterxml.jackson.databind.node.ObjectNode;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.ingestion.rss.RawFeed;
import platform.ingestion.rss.RssSource;
import static org.junit.jupiter.api.Assertions.*;

/** Native read-back must qualify a candidate before the result can claim success. */
class StrictFetchCandidateTest {
    @TempDir Path temporary;
    static final List<String> CANDIDATES=List.of("payload.xml","metadata.json","descriptor.json");

    static StrictManagedRssFetch.Bootstrap bootstrap(StrictFetchFixture fixture) throws Exception {
        fixture.packet.put("childPid",Long.toString(ProcessHandle.current().pid()));
        return StrictManagedRssFetch.parse(StrictFetchFixture.canonical(fixture.packet));
    }
    static StrictFetchEvidence sink(StrictManagedRssFetch.Bootstrap bootstrap) throws Exception {
        return new StrictFetchEvidence(bootstrap.expected(),bootstrap.current());
    }
    Path stage(StrictFetchFixture fixture,RawFeed feed) throws Exception {
        Path staging=Files.createDirectory(temporary.resolve("native-staging"));
        Files.setPosixFilePermissions(staging,PosixFilePermissions.fromString("rwx------"));
        StrictFetchFixture.privateWrite(staging.resolve("command.json"),fixture.command);
        var source=new RssSource("demo-feed",URI.create(fixture.commandNode.get("feedUri").textValue()),true);
        ManagedRssFetch.writeCandidate(staging,fixture.command,fixture.commandNode,source,feed);
        // The historical native shape is unchanged; the strict writer additionally sorts its JSON keys.
        StrictFetchFixture.privateWrite(staging.resolve("descriptor.json"),StrictFetchFixture.canonical(
                StrictFetchFixture.JSON.readTree(Files.readAllBytes(staging.resolve("descriptor.json")))));
        return staging;
    }
    static RawFeed feed(byte[] body,boolean headers) {
        return new RawFeed(UUID.fromString("12345678-1234-4234-8234-123456789abc"),"demo-feed",
                Instant.parse("2026-10-26T18:00:01.123456789Z"),200,
                headers?Optional.of("application/atom+xml; charset=UTF-8"):Optional.empty(),
                headers?OptionalLong.of(body.length):OptionalLong.empty(),body);
    }
    static void copy(StrictFetchEvidence sink,Path staging) throws Exception {
        for(String name:CANDIDATES)sink.write(name,Files.readAllBytes(staging.resolve(name)));
    }
    static byte[] success(StrictFetchFixture fixture) throws Exception {
        ObjectNode result=StrictFetchFixture.JSON.createObjectNode();
        for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,fixture.commandNode.get(key));
        result.put("status","OK").putNull("code").set("evidenceId",fixture.commandNode.get("checkpointId"));
        result.putObject("counts");return StrictFetchFixture.canonical(result);
    }

    @ParameterizedTest @ValueSource(booleans={true,false})
    void exactRawFeedIsReadOnlyQualifiedWhileEmptyPrefixStillExists(boolean headers) throws Exception {
        try(var fixture=new StrictFetchFixture(temporary)) {
            RawFeed original=feed(headers?fixture.body:new byte[0],headers);
            Path staging=stage(fixture,original);
            var sink=sink(bootstrap(fixture));
            sink.runFresh(()-> {
                copy(sink,staging);
                assertFalse(Files.exists(fixture.directory.resolve("result.json")));
                assertEquals(0,Files.size(fixture.directory.resolve("result.json.preparing")));
                var before=StrictManagedRssFetchTest.snapshot(fixture.root);
                RawFeed reopened=StrictFetchCandidate.reopen(sink,fixture.command);
                assertArrayEquals(original.body(),reopened.body());assertEquals(original.ingestionId(),reopened.ingestionId());
                assertEquals(original.ingestedAt(),reopened.ingestedAt());assertEquals(original.sourceId(),reopened.sourceId());
                assertEquals(original.httpStatus(),reopened.httpStatus());assertEquals(original.contentType(),reopened.contentType());
                assertEquals(original.contentLength(),reopened.contentLength());
                assertEquals(before,StrictManagedRssFetchTest.snapshot(fixture.root),"strict reader must not write or publish");
                assertFalse(Files.exists(fixture.directory.resolve("result.json")));
                assertFalse(Files.exists(fixture.directory.resolve("complete.json")));
                sink.write("result.json",success(fixture));return null;
            });
            assertEquals(0,fixture.requests.get());
            assertFalse(Files.exists(fixture.directory.resolve("result.json.preparing")));
            assertArrayEquals(original.body(),ManagedRssFetch.reopen(fixture.directory,fixture.command).body());
        }
    }

    @ParameterizedTest @ValueSource(strings={"payload","metadata","descriptor","unknown","commandSha256","revision","expectedUnknown","metadataDigest","overflow","wrongCommand"})
    void invalidNativeCandidateCannotPublishOkAndConsumedPrefixIsRetained(String fault) throws Exception {
        try(var fixture=new StrictFetchFixture(temporary)) {
            Path staging=stage(fixture,feed(fixture.body,true));
            Path descriptor=staging.resolve("descriptor.json");
            ObjectNode value=(ObjectNode)StrictFetchFixture.JSON.readTree(Files.readAllBytes(descriptor));
            switch(fault) {
                case "payload" -> StrictFetchFixture.privateWrite(staging.resolve("payload.xml"),new byte[]{42});
                case "metadata" -> StrictFetchFixture.privateWrite(staging.resolve("metadata.json"),"{}".getBytes(java.nio.charset.StandardCharsets.UTF_8));
                case "unknown" -> value.put("extra",true);
                case "commandSha256" -> value.put("commandSha256","f".repeat(64));
                case "revision" -> value.put("revision",8);
                case "expectedUnknown" -> ((ObjectNode)value.get("expected")).put("extra",true);
                case "metadataDigest" -> ((ObjectNode)value.get("expected")).put("metadataSha256","f".repeat(64));
                case "overflow" -> value.put("declaredContentLength",new java.math.BigInteger("18446744073709551616"));
                default -> { }
            }
            StrictFetchFixture.privateWrite(descriptor,fault.equals("descriptor")?new byte[]{'{'}:StrictFetchFixture.canonical(value));
            var bootstrap=bootstrap(fixture);var sink=sink(bootstrap);
            assertThrows(Exception.class,()->sink.runFresh(()-> {
                copy(sink,staging);
                byte[] supplied=fixture.command;
                if(fault.equals("wrongCommand")) {
                    ObjectNode other=fixture.commandNode.deepCopy();other.put("revision",8);
                    supplied=StrictFetchFixture.canonical(other);
                }
                StrictFetchCandidate.reopen(sink,supplied);
                fail("invalid candidate must not reach result publication");return null;
            }));
            assertEquals(0,fixture.requests.get());assertFalse(Files.exists(fixture.directory.resolve("result.json")));
            assertFalse(Files.exists(fixture.directory.resolve("complete.json")));
            assertEquals(0,Files.size(fixture.directory.resolve("result.json.preparing")));
            var before=StrictManagedRssFetchTest.snapshot(fixture.root);
            assertThrows(Exception.class,()->sink(bootstrap));
            assertEquals(before,StrictManagedRssFetchTest.snapshot(fixture.root),"a new reader cannot adopt or repair consumed evidence");
        }
    }
}
