package platform.contentapi.execution;

import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class NarrowContentInstalledRuntimeQualificationTest {
    @TempDir Path temp;
    @Test void observesRealFilesAndDistinctDeclaredInterpretersWithoutLoadingEither() throws Exception {
        try(var f=new NarrowContentInstalledRuntimeFixture(temp)){
            var result=f.observe();assertThat(result.artifacts()).hasSize(22);assertThat(result.roots()).hasSize(8);
            assertThat(result.executables()).extracting(NarrowContentInstalledRuntimeQualification.Executable::role).containsExactly("FETCH_JAVA","GOLD_PYTHON","SILVER_JAVA","SILVER_PYTHON");
            assertThat(result.executables().get(1).file().identity()).isNotEqualTo(result.executables().get(3).file().identity());
            assertThat(result.artifacts()).allSatisfy(a->assertThat(a.file().bytes()).isPositive());
            assertThat(f.base.resolve("never-read-credentials")).doesNotExist();
            assertThatThrownBy(()->result.artifacts().clear()).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(()->result.roots().clear()).isInstanceOf(UnsupportedOperationException.class);
            assertThatThrownBy(()->result.executables().clear()).isInstanceOf(UnsupportedOperationException.class);
            assertThat(f.correspondence().nativeLaunchCompatible()).isFalse();assertThat(f.correspondence().installedArtifactsVerified()).isFalse();
        }
    }
    @ParameterizedTest @ValueSource(strings={"jar","module"})
    void changedBytesFail(String kind)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Files.writeString(file(f,kind),"changed bytes");assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @ParameterizedTest @ValueSource(strings={"jar","module"})
    void missingBytesFail(String kind)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Files.delete(file(f,kind));assertThatThrownBy(f::observe).hasMessage("INPUT_UNAVAILABLE");}}
    @ParameterizedTest @ValueSource(strings={"jar","module"})
    void symlinkFilesFail(String kind)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Path p=file(f,kind),kept=Files.move(p,f.base.resolve("kept"));Files.createSymbolicLink(p,kept);assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @ParameterizedTest @ValueSource(strings={"jar","module"})
    void hardLinkedFilesFail(String kind)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Files.createLink(f.base.resolve("hard-link"),file(f,kind));assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @ParameterizedTest @ValueSource(strings={"jar","module"})
    void directoriesCannotStandInForArtifacts(String kind)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Path p=file(f,kind);Files.delete(p);Files.createDirectory(p);assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @ParameterizedTest @ValueSource(strings={"jar","module"})
    void unchangedPerFileCeilingsRejectBeforeUnboundedRead(String kind)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Path p=file(f,kind);long limit=kind.equals("jar")?64L*1024*1024:1024L*1024;try(var channel=FileChannel.open(p,StandardOpenOption.WRITE)){channel.position(limit);channel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));}assertThatThrownBy(f::observe).hasMessage("RESOURCE_LIMIT");}}
    @Test void aggregateClasspathRetains256MiBCeiling()throws Exception {
        try(var f=new NarrowContentInstalledRuntimeFixture(temp)){
            byte[] chunk=new byte[1024*1024];var digest=java.security.MessageDigest.getInstance("SHA-256");
            for(int i=0;i<64;i++)digest.update(chunk);String hash=HexFormat.of().formatHex(digest.digest());
            var entries=((ObjectNode)f.configs.get("fetch").get("manifest")).putArray("classpath");
            for(int i=0;i<5;i++){
                Path jar=f.base.resolve("bounded-"+i+".jar");
                try(var channel=FileChannel.open(jar,StandardOpenOption.CREATE_NEW,StandardOpenOption.WRITE)){channel.position(64L*1024*1024-1);channel.write(java.nio.ByteBuffer.wrap(new byte[]{0}));}
                entries.addObject().put("path",jar.toString()).put("sha256",hash);
            }
            assertThatThrownBy(f::observe).hasMessage("RESOURCE_LIMIT");
            entries.remove(4);assertThat(f.observe().artifacts().stream().filter(a->a.kind().equals("JAR"))).hasSize(4);
        }
    }
    @Test void sharedModuleRegistrationsMustAgree()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){((ObjectNode)f.configs.get("gold").get("modules")).put("bounded_evidence.py","a".repeat(64));assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @ParameterizedTest @ValueSource(strings={"SILVER","TEMP","GOLD","REFERENCE","CONTENT","STAGING"})
    void registeredRootReplacementFails(String role)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Path root=f.roots.get(role);Files.move(root,f.base.resolve("retained-root"));Files.createDirectory(root);NarrowContentInstalledRuntimeFixture.privateMode(root,true);assertThatThrownBy(f::observe).hasMessage("INPUT_CHANGED");}}
    @Test void processingMustBeCanonicalAndSeparate()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Path alias=f.base.resolve("processing-alias");Files.createSymbolicLink(alias,f.processing);f.configs.get("silver").put("processingRoot",alias.toString());assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @Test void canonicalStorageAliasFails()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Path root=f.roots.get("GOLD"),kept=Files.move(root,f.base.resolve("retained-root"));Files.createSymbolicLink(root,kept);assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @Test void missingLocalDescriptorVersionFails()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){var b=f.binding("REFERENCE");((ObjectNode)b.get("configuration")).remove("importStorageVersion");b.put("configurationSha256",NarrowContentInstalledRuntimeFixture.sha(b.get("configuration")));assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @ParameterizedTest @ValueSource(strings={"silver","gold"})
    void missingExecutableFails(String family)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Files.delete(family.equals("silver")?f.silverPython:f.goldPython);assertThatThrownBy(f::observe).hasMessage("INPUT_UNAVAILABLE");}}
    @ParameterizedTest @ValueSource(strings={"silver","gold"})
    void nonExecutableFails(String family)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Files.setPosixFilePermissions(family.equals("silver")?f.silverPython:f.goldPython,PosixFilePermissions.fromString("rw-------"));assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @Test void executableDirectoryFails()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Files.delete(f.goldPython);Files.createDirectory(f.goldPython);assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @Test void normalInterpreterSymlinkIsObservedWithoutChangingLegacySemantics()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Files.delete(f.goldPython);Files.createSymbolicLink(f.goldPython,f.silverPython);assertThat(f.observe().executables()).hasSize(4);}}
    @Test void currentJvmRegistrationStillMustMatch()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){((ObjectNode)f.configs.get("fetch").get("manifest")).put("javaRuntime","wrong-version");assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @Test void privateRootsCannotBecomePublic()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){Files.setPosixFilePermissions(f.roots.get("TEMP"),PosixFilePermissions.fromString("rwxr-xr-x"));assertThatThrownBy(f::observe).hasMessage("INPUT_INVALID");}}
    @Test void onlyExistingWritableVolumeFloorsAreCheckedAtExactBoundary()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){var visited=new HashSet<Path>();var r=NarrowContentInstalledRuntimeQualification.observe(f.correspondence(),f.guard,p->{visited.add(p);return 1024L*1024*1024;});assertThat(visited).containsExactlyInAnyOrderElementsOf(List.of("CONTROL","SILVER","TEMP","GOLD","CONTENT").stream().map(f.roots::get).toList());assertThat(r.roots().stream().filter(v->v.usableBytes()!=null)).hasSize(5);}}
    @ParameterizedTest @ValueSource(strings={"CONTROL","SILVER","TEMP","GOLD","CONTENT"})
    void existingFloorRejectsOneByteUnder(String role)throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){assertThatThrownBy(()->NarrowContentInstalledRuntimeQualification.observe(f.correspondence(),f.guard,p->1024L*1024*1024-(p.equals(f.roots.get(role))?1:0))).hasMessage("RESOURCE_LIMIT");}}
    @Test void declaredCredentialPathCannotBeReadAsAnArtifact()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){var b=f.binding("BRONZE");((ObjectNode)b.get("configuration")).put("credentialsFile",f.jar.toString());b.put("configurationSha256",NarrowContentInstalledRuntimeFixture.sha(b.get("configuration")));f.configs.get("bronze").put("storageBindingSha256",b.get("configurationSha256").stringValue());assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");}}
    @Test void extraBronzeBindingCannotMaskSelectedCredentialPath()throws Exception {
        try(var f=new NarrowContentInstalledRuntimeFixture(temp)){
            var selected=f.binding("BRONZE");var c=(ObjectNode)selected.get("configuration");c.put("credentialsFile",f.jar.toString());
            selected.put("configurationSha256",NarrowContentInstalledRuntimeFixture.sha(c));f.configs.get("bronze").put("storageBindingSha256",selected.get("configurationSha256").stringValue());
            f.binding("extra-bronze","BRONZE",c.deepCopy().put("credentialsFile",f.base.resolve("other-never-read-credentials").toString()));
            assertThatThrownBy(f::observe).hasMessage("RUNTIME_MISMATCH");
        }
    }
    @Test void closedGuardCannotInspectFiles()throws Exception{try(var f=new NarrowContentInstalledRuntimeFixture(temp)){f.guard.close();assertThatThrownBy(f::observe).hasMessage("HOST_BINDING_INVALID");}}
    private static Path file(NarrowContentInstalledRuntimeFixture f,String kind){return kind.equals("jar")?f.jar:f.processing.resolve("bounded_evidence.py");}
}
