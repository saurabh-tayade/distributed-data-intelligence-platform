package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;

class ContentRecoveryFilesTest {
    @TempDir Path temp;InstallationHostGuard guard;UUID run=UUID.randomUUID(),attempt=UUID.randomUUID();Path root;
    @BeforeEach void setup()throws Exception{var binding=OwnershipFixtures.host(temp.toRealPath());guard=InstallationHostGuard.acquire(binding);root=binding.root().resolve("runs/"+run+"/attempts/"+attempt);}
    @AfterEach void close(){guard.close();}
    Path file(String name,byte[] bytes)throws Exception{Files.createDirectories(root);for(Path p=root;p.startsWith(guard.binding().root()) && !p.equals(guard.binding().root());p=p.getParent())Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rwx------"));Path f=root.resolve(name);Files.write(f,bytes);Files.setPosixFilePermissions(f,PosixFilePermissions.fromString("rw-------"));return f;}
    List<Map<String,Object>> stage(String address,String sha){return List.of(Map.of("command_address",address,"command_sha256",sha));}
    RecoveryFiles.Capture capture(List<Map<String,Object>> stages){return ContentRecoveryFiles.capture(guard,run,attempt,stages);}
    @Test void crashBeforeAnyPrivateDirectoryDoesNotInventEvidence(){assertThat(capture(List.of()).inventory().get("files")).isEmpty();}
    @Test void childSidecarRetainsLaunchGapIdentityHint()throws Exception{file("child.json",SourceJson.canonical(SourceJson.object().put("pid","12345").put("startedAt","2026-10-30T12:00:00Z")));assertThat(capture(List.of()).descendantPids()).containsExactly(12345L);}
    @Test void nativeGatewayHintInSilverResultIsRetained()throws Exception{file("result.json",SourceJson.canonical(SourceJson.object().set("cleanup",SourceJson.object().put("kind","PYSPARK_GATEWAY_REAPED").put("pid","12346"))));assertThat(capture(List.of()).descendantPids()).containsExactly(12346L);}
    @Test void changedBytesAreCapturedIndependently()throws Exception{Path p=file("payload.xml",new byte[]{1});var first=capture(List.of());Files.write(p,new byte[]{2});assertThat(capture(List.of()).inventory()).isNotEqualTo(first.inventory());}
    @Test void recordedCommandHashMustMatch()throws Exception{Path p=file("command.json",new byte[]{1});String address=guard.binding().root().relativize(p).toString();assertThatThrownBy(()->capture(stage(address,"0".repeat(64)))).hasMessage("EVIDENCE_CHANGED");}
    @Test void missingRecordedCommandIsUnavailable()throws Exception{file("retained.json",new byte[]{1});String address=guard.binding().root().relativize(root.resolve("missing.json")).toString();assertThatThrownBy(()->capture(stage(address,"0".repeat(64)))).hasMessage("EVIDENCE_UNAVAILABLE");}
    @Test void symlinkIsRejectedWithoutFollowing()throws Exception{Path p=file("target",new byte[]{1});Files.createSymbolicLink(root.resolve("alias"),p);assertThatThrownBy(()->capture(List.of())).hasMessage("EVIDENCE_CHANGED");}
    @Test void outsideAttemptAddressCannotSupplyEvidence()throws Exception{file("retained.json",new byte[]{1});assertThatThrownBy(()->capture(stage("runs/other/command.json","0".repeat(64)))).hasMessage("EVIDENCE_CHANGED");}
}
