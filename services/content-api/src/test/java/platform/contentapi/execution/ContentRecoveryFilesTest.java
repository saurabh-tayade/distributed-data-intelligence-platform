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
    @Test void privateAttemptAbove64MiBIsUnavailableWithoutTruncation()throws Exception{
        for(int i=0;i<5;i++){Path p=file("retained-"+i+".bin",new byte[0]);try(var f=new java.io.RandomAccessFile(p.toFile(),"rw")){f.setLength(14L*1024*1024);}}
        assertThatThrownBy(()->capture(List.of())).hasMessage("EVIDENCE_UNAVAILABLE");assertThat(Files.size(root.resolve("retained-4.bin"))).isEqualTo(14L*1024*1024);
    }
    @Test void outsideAttemptAddressCannotSupplyEvidence()throws Exception{file("retained.json",new byte[]{1});assertThatThrownBy(()->capture(stage("runs/other/command.json","0".repeat(64)))).hasMessage("EVIDENCE_CHANGED");}
    Path sparse(String name,long length)throws Exception{Path p=file(name,new byte[0]);try(var f=new java.io.RandomAccessFile(p.toFile(),"rw")){f.setLength(length);}return p;}
    @Test void exactly64MiBIsHashedAndOneMoreByteIsRejectedWithoutTruncation()throws Exception{
        for(int i=0;i<4;i++)sparse("evidence-"+i+".bin",16L*1024*1024);
        var accepted=capture(List.of());long bytes=0;int count=0;
        for(var entry:accepted.inventory().get("files"))if(!entry.get("directory").booleanValue()){bytes+=Long.parseLong(entry.get("length").stringValue());assertThat(entry.get("sha256").stringValue()).hasSize(64);count++;}
        assertThat(bytes).isEqualTo(64L*1024*1024);assertThat(count).isEqualTo(4);
        Path extra=file("first-rejected.bin",new byte[]{1});assertThatThrownBy(()->capture(List.of())).hasMessage("EVIDENCE_UNAVAILABLE");
        assertThat(Files.size(extra)).isOne();for(int i=0;i<4;i++)assertThat(Files.size(root.resolve("evidence-"+i+".bin"))).isEqualTo(16L*1024*1024);
    }
    @Test void smallTreeCannotHideAnIndividuallyOversizedSidecar()throws Exception{
        Path p=sparse("evidence.bin",16L*1024*1024+1);assertThatThrownBy(()->capture(List.of())).hasMessage("EVIDENCE_CHANGED");assertThat(Files.size(p)).isEqualTo(16L*1024*1024+1);
    }
    @Test void eightMaximumPayloadsFitThePayloadPortionOnly()throws Exception{
        byte[] payload=new byte[5*1024*1024];for(int i=0;i<8;i++){
            Path directory=root.resolve("fetch-source/source"+i+"/"+UUID.randomUUID());Files.createDirectories(directory);
            for(Path p=directory;p.startsWith(guard.binding().root()) && !p.equals(guard.binding().root());p=p.getParent())Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rwx------"));
            Path p=directory.resolve("payload.xml");Files.write(p,payload);Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rw-------"));
        }
        var accepted=capture(List.of());long total=0;int count=0;
        for(var entry:accepted.inventory().get("files"))if(!entry.get("directory").booleanValue()){total+=Long.parseLong(entry.get("length").stringValue());count++;}
        assertThat(total).isEqualTo(40L*1024*1024);assertThat(count).isEqualTo(8);
    }
    @Test void directoryAtWalkBoundaryCannotHideUnhashedProcessSidecars()throws Exception{
        file("retained.json",new byte[]{1});Path deepest=Files.createDirectories(root.resolve("a/b/c/d"));
        for(Path p=deepest;p.startsWith(root);p=p.getParent())Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rwx------"));
        Path child=Files.write(deepest.resolve("child.json"),SourceJson.canonical(SourceJson.object().put("pid","12345")));Files.setPosixFilePermissions(child,PosixFilePermissions.fromString("rw-------"));
        assertThatThrownBy(()->capture(List.of())).hasMessage("EVIDENCE_UNAVAILABLE");assertThat(Files.exists(child)).isTrue();
    }
    @Test void evenAnEmptyDirectoryAtWalkBoundaryIsUnsupported()throws Exception{
        file("retained.json",new byte[]{1});Path deepest=Files.createDirectories(root.resolve("a/b/c/d"));for(Path p=deepest;p.startsWith(root);p=p.getParent())Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rwx------"));
        assertThatThrownBy(()->capture(List.of())).hasMessage("EVIDENCE_UNAVAILABLE");
    }
    @Test void entryCeilingIncludesTheAttemptRootAndRejectsTheFirstExtraEntry()throws Exception{
        Path marker=file("marker",new byte[0]);Files.delete(marker);
        for(int i=0;i<4095;i++)Files.createDirectory(root.resolve(Integer.toString(i,36)),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        assertThat(capture(List.of()).inventory().get("files")).hasSize(4096);
        Files.createDirectory(root.resolve("extra"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
        assertThatThrownBy(()->capture(List.of())).hasMessage("EVIDENCE_UNAVAILABLE");try(var children=Files.list(root)){assertThat(children.count()).isEqualTo(4096);}
    }
    @Test void inventorySerializationCanRejectZeroByteEvidenceAtTheEntryCeiling()throws Exception{
        for(int i=0;i<4095;i++)file("x".repeat(124)+String.format(java.util.Locale.ROOT,"%04d",i),new byte[0]);
        assertThatThrownBy(()->capture(List.of())).hasMessage("EVIDENCE_UNAVAILABLE");try(var children=Files.list(root)){assertThat(children.count()).isEqualTo(4095);}
    }
    @Test void recoveryJsonFileHasItsOwnOneMiBBoundary()throws Exception{
        var n=SourceJson.object().put("pad","");int overhead=SourceJson.canonical(n).length;n.put("pad","x".repeat(1024*1024-overhead));byte[] bytes=SourceJson.canonical(n);
        assertThat(bytes).hasSize(1024*1024);UUID control=UUID.randomUUID();String address=RecoveryFiles.write(guard,control,UUID.randomUUID(),bytes);assertThat(RecoveryFiles.read(guard,address)).isEqualTo(bytes);
        n.put("pad",n.get("pad").stringValue()+"x");UUID rejected=UUID.randomUUID();assertThatThrownBy(()->RecoveryFiles.write(guard,control,rejected,SourceJson.canonical(n))).hasMessage("EVIDENCE_UNAVAILABLE");assertThat(Files.exists(guard.binding().root().resolve("recovery/"+control+"/"+rejected))).isFalse();
    }
    @Test void twoFortyMiBTreesAreIndividuallyCapturableWithoutAnAncestryQuota()throws Exception{
        for(int i=0;i<4;i++)sparse("first-"+i+".bin",10L*1024*1024);
        var first=capture(List.of());attempt=UUID.randomUUID();root=guard.binding().root().resolve("runs/"+run+"/attempts/"+attempt);
        for(int i=0;i<4;i++)sparse("second-"+i+".bin",10L*1024*1024);
        var second=capture(List.of());for(var tree:List.of(first,second)){long total=0;for(var entry:tree.inventory().get("files"))if(!entry.get("directory").booleanValue())total+=Long.parseLong(entry.get("length").stringValue());assertThat(total).isEqualTo(40L*1024*1024);}
    }
    @Test void sqlSnapshotRepresentsLargeByteArraysByDigestAndLength(){
        byte[] selection=new byte[16*1024*1024];var snapshot=RecoveryHandoff.json(Map.of("captured_selection",selection));
        assertThat(SourceJson.canonical(snapshot).length).isLessThan(256);assertThat(snapshot.get("captured_selection").get("sha256").stringValue()).isEqualTo(SourceJson.sha(selection));assertThat(snapshot.get("captured_selection").get("length").stringValue()).isEqualTo("16777216");
    }
}
