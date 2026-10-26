package platform.ingestion.managed;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.ingestion.bronze.S3BronzeStorage;
import platform.ingestion.rss.*;
import static org.junit.jupiter.api.Assertions.*;

class ManagedRssFetchTest {
    @TempDir Path temporary;
    final ObjectMapper json=new ObjectMapper();
    final RssSource source=new RssSource("demo-feed",URI.create("https://feeds.example.org/feed"),true);
    final byte[] payload={'<','f','e','e','d','>',0,(byte)0xff,'\r','\n'};
    Path directory;byte[] command;RawFeed feed;
    @BeforeEach void setup() throws Exception {
        directory=Files.createDirectory(temporary.resolve("journal")).toRealPath();Files.setPosixFilePermissions(directory,PosixFilePermissions.fromString("rwx------"));
        var c=json.createObjectNode().put("protocolVersion",1).put("runId",UUID.randomUUID().toString()).put("attemptId",UUID.randomUUID().toString()).put("ownerEpoch",3).put("stage","FETCH_SOURCE").put("itemOrdinal",0).put("checkpointId",UUID.randomUUID().toString()).put("sourceId",source.sourceId()).put("revision",7).put("capturedConfigSha256","a".repeat(64)).put("endpointSha256","b".repeat(64)).put("feedUri",source.feedUri().toString()).put("runtimeSha256","c".repeat(64));
        c.put("javaRuntime",Runtime.version().toString()).put("testEndpoint",false).put("httpTimeoutMillis",1000);
        command=json.writeValueAsBytes(c);ManagedRssFetch.write(directory.resolve("command.json"),command);
        feed=new RawFeed(UUID.fromString("12345678-1234-4234-8234-123456789abc"),source.sourceId(),Instant.parse("2026-10-26T18:00:01.123456789Z"),200,Optional.of("application/atom+xml; charset=UTF-8"),OptionalLong.of(payload.length),payload);
    }
    void candidate() throws Exception {ManagedRssFetch.writeCandidate(directory,command,json.readTree(command),source,feed);}
    @Test void exactRawFeedAndSharedMetadataIncludingNanoseconds() throws Exception {
        candidate();var reopened=ManagedRssFetch.reopen(directory,command);
        assertArrayEquals(payload,reopened.body());assertEquals(feed.ingestionId(),reopened.ingestionId());assertEquals(feed.ingestedAt(),reopened.ingestedAt());
        assertEquals(feed.contentType(),reopened.contentType());assertEquals(feed.contentLength(),reopened.contentLength());assertEquals(feed.httpStatus(),reopened.httpStatus());
        assertArrayEquals(S3BronzeStorage.preparedMetadata(source,feed),Files.readAllBytes(directory.resolve("metadata.json")));
        assertFalse(Files.exists(directory.resolve("complete.json")));assertEquals(PosixFilePermissions.fromString("rw-------"),Files.getPosixFilePermissions(directory.resolve("payload.xml")));
    }
    @Test void absentHeadersAndEmptyBodyPreserved() throws Exception {feed=new RawFeed(feed.ingestionId(),feed.sourceId(),feed.ingestedAt(),204,Optional.empty(),OptionalLong.empty(),new byte[0]);candidate();var reopened=ManagedRssFetch.reopen(directory,command);assertTrue(reopened.contentType().isEmpty());assertTrue(reopened.contentLength().isEmpty());assertEquals(0,reopened.body().length);}
    @Test void repeatedCandidateCannotOverwrite() throws Exception {candidate();byte[] before=Files.readAllBytes(directory.resolve("descriptor.json"));assertThrows(FileAlreadyExistsException.class,this::candidate);assertArrayEquals(before,Files.readAllBytes(directory.resolve("descriptor.json")));}
    @ParameterizedTest @ValueSource(strings={"payload.xml","metadata.json","descriptor.json"}) void changedOrMissingEvidenceFails(String name) throws Exception {candidate();Files.write(directory.resolve(name),new byte[]{1});assertThrows(Exception.class,()->ManagedRssFetch.reopen(directory,command));Files.delete(directory.resolve(name));assertThrows(Exception.class,()->ManagedRssFetch.reopen(directory,command));}
    @Test void unknownDescriptorFieldsFail() throws Exception {candidate();var d=(com.fasterxml.jackson.databind.node.ObjectNode)json.readTree(Files.readAllBytes(directory.resolve("descriptor.json")));d.put("extra",true);Files.write(directory.resolve("descriptor.json"),json.writeValueAsBytes(d));assertThrows(IllegalArgumentException.class,()->ManagedRssFetch.reopen(directory,command));}
    @Test void descriptorNumericOverflowIsRejected() throws Exception {candidate();Path p=directory.resolve("descriptor.json");byte[] raw=Files.readAllBytes(p);for(String field:List.of("journalVersion","httpStatus","declaredContentLength")){var d=(com.fasterxml.jackson.databind.node.ObjectNode)json.readTree(raw);d.put(field,new java.math.BigInteger("18446744073709551816"));Files.write(p,json.writeValueAsBytes(d));assertThrows(IllegalArgumentException.class,()->ManagedRssFetch.reopen(directory,command));}}
    @Test void responseAndDescriptorBoundsFailWithoutCompletion() throws Exception {feed=new RawFeed(feed.ingestionId(),feed.sourceId(),feed.ingestedAt(),200,Optional.empty(),OptionalLong.empty(),new byte[5242881]);assertThrows(IllegalArgumentException.class,this::candidate);assertFalse(Files.exists(directory.resolve("descriptor.json")));}
    @Test void oversizedHeaderCannotCreateDescriptor() throws Exception {feed=new RawFeed(feed.ingestionId(),feed.sourceId(),feed.ingestedAt(),200,Optional.of("x".repeat(65536)),OptionalLong.empty(),payload);assertThrows(IllegalArgumentException.class,this::candidate);assertFalse(Files.exists(directory.resolve("descriptor.json")));}
    @ParameterizedTest @ValueSource(strings={"https://feeds.example.org/a?x=1","https://u:p@feeds.example.org/a","https://feeds.example.org/a#x","http://127.0.0.1/a","http://x.local/a"}) void productionEndpointRestrictions(String uri){assertThrows(IllegalArgumentException.class,()->ManagedRssFetch.validateEndpoint(URI.create(uri),false));}
    @Test void syntheticProfileOnlyAllowsLoopback(){ManagedRssFetch.validateEndpoint(URI.create("http://127.0.0.1:1234/feed"),true);assertThrows(IllegalArgumentException.class,()->ManagedRssFetch.validateEndpoint(source.feedUri(),true));}
    @Test void permissivePrivateFilesAndSymlinksFail() throws Exception {candidate();Path p=directory.resolve("payload.xml");Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rw-r--r--"));assertThrows(IllegalArgumentException.class,()->ManagedRssFetch.reopen(directory,command));Files.delete(p);Files.createSymbolicLink(p,directory.resolve("metadata.json"));assertThrows(IllegalArgumentException.class,()->ManagedRssFetch.reopen(directory,command));}
    @Test void changedCommandAndWriteIoFailureRetainEvidence() throws Exception {candidate();byte[] descriptor=Files.readAllBytes(directory.resolve("descriptor.json"));assertThrows(Exception.class,()->ManagedRssFetch.reopen(directory,"{}".getBytes()));assertThrows(Exception.class,()->ManagedRssFetch.write(directory.resolve("absent/file"),payload));assertArrayEquals(descriptor,Files.readAllBytes(directory.resolve("descriptor.json")));}
    @Test void exactFiveMiBLimitRemainsAvailable() throws Exception {feed=new RawFeed(feed.ingestionId(),feed.sourceId(),feed.ingestedAt(),200,Optional.empty(),OptionalLong.empty(),new byte[5242880]);candidate();assertEquals(5242880,ManagedRssFetch.reopen(directory,command).body().length);}
    @Test void unexpectedFileHardLinkAndOwnerExecutablePermissionsFail() throws Exception {candidate();Path p=directory.resolve("payload.xml");Files.writeString(directory.resolve("extra"),"x");assertThrows(Exception.class,()->ManagedRssFetch.reopen(directory,command));Files.delete(directory.resolve("extra"));Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rwx------"));assertThrows(Exception.class,()->ManagedRssFetch.reopen(directory,command));Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rw-------"));Files.createLink(temporary.resolve("alias"),p);assertThrows(Exception.class,()->ManagedRssFetch.reopen(directory,command));}
    @ParameterizedTest @ValueSource(strings={"protocolVersion","revision","ownerEpoch","itemOrdinal","testEndpoint","httpTimeoutMillis"}) void numericAndBooleanCoercionRejectedBeforeAnyNetwork(String field)throws Exception {var c=(com.fasterxml.jackson.databind.node.ObjectNode)json.readTree(command);c.put(field,"1");Files.write(directory.resolve("command.json"),json.writeValueAsBytes(c));assertThrows(IllegalArgumentException.class,()->ManagedRssFetch.execute(directory.resolve("command.json")));assertFalse(Files.exists(directory.resolve("payload.xml")));}
}
