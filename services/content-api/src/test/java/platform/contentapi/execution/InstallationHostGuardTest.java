package platform.contentapi.execution;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import static org.assertj.core.api.Assertions.*;

class InstallationHostGuardTest {
    @TempDir Path temp;
    InstallationHostGuard.Binding binding;
    @BeforeEach void setup() throws Exception {binding=OwnershipFixtures.host(temp);}
    @Test void holdsPersistentGuardWithoutChangingBytesOrMtimes() throws Exception {
        var files=List.of(binding.root(),binding.root().resolve("host.guard"),binding.root().resolve("installation.id"));
        var stamps=files.stream().map(p -> {try{return Files.getLastModifiedTime(p);}catch(IOException e){throw new UncheckedIOException(e);}}).toList();
        try(var guard=InstallationHostGuard.acquire(binding)) {
            guard.checkHeld(OwnershipFixtures.INSTALL);
            assertThat(guard.process().pid()).isEqualTo(ProcessHandle.current().pid());
            assertThatThrownBy(() -> InstallationHostGuard.acquire(binding)).hasMessage("HOST_GUARD_OCCUPIED").hasNoCause();
        }
        for(int i=0;i<files.size();i++)assertThat(Files.getLastModifiedTime(files.get(i))).isEqualTo(stamps.get(i));
        assertThat(Files.readAllBytes(files.get(1))).isEmpty();
        assertThat(Files.readString(files.get(2))).isEqualTo(OwnershipFixtures.INSTALL+"\n");
    }
    @Test void competingProcessCannotEnterUntilLifetimeGuardIsReleased() throws Exception {
        Path descriptor=Files.write(temp.resolve("binding.json"),binding.descriptor());
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var child=OwnershipFixtures.child(descriptor,"guard",UUID.randomUUID()).start();
            try {
                assertThat(child.waitFor(10,TimeUnit.SECONDS)).isTrue();
                assertThat(new String(child.getInputStream().readAllBytes())).isEqualTo("HOST_GUARD_OCCUPIED\n");
            } finally {child.destroyForcibly();child.waitFor(5,TimeUnit.SECONDS);}
        }
        var child=OwnershipFixtures.child(descriptor,"guard",UUID.randomUUID()).start();
        try {
            var line=CompletableFuture.supplyAsync(() -> {try{return new BufferedReader(new InputStreamReader(child.getInputStream())).readLine();}catch(IOException e){throw new UncheckedIOException(e);}});
            assertThat(line.get(10,TimeUnit.SECONDS)).isEqualTo("ACQUIRED");
            assertThatThrownBy(() -> InstallationHostGuard.acquire(binding)).hasMessage("HOST_GUARD_OCCUPIED");
        } finally {child.destroyForcibly();assertThat(child.waitFor(5,TimeUnit.SECONDS)).isTrue();}
        try(var guard=InstallationHostGuard.acquire(binding)) {guard.checkHeld(OwnershipFixtures.INSTALL);}
    }
    @Test void installationMismatchFailsWithFixedDiagnostic() throws Exception {
        Files.writeString(binding.root().resolve("installation.id"),UUID.randomUUID()+"\n");
        assertThatThrownBy(() -> InstallationHostGuard.acquire(binding)).hasMessage("HOST_BINDING_INVALID").hasNoCause();
    }
    @Test void permissiveRootFailsClosed() throws Exception {
        Files.setPosixFilePermissions(binding.root(),PosixFilePermissions.fromString("rwxr-x---"));
        assertThatThrownBy(() -> InstallationHostGuard.acquire(binding)).hasMessage("HOST_BINDING_INVALID");
    }
    @Test void symlinkGuardFailsWithoutFollowingIt() throws Exception {
        Path guard=binding.root().resolve("host.guard");Files.move(guard,temp.resolve("saved-guard"));Files.createSymbolicLink(guard,temp.resolve("saved-guard"));
        assertThatThrownBy(() -> InstallationHostGuard.acquire(binding)).hasMessage("HOST_BINDING_INVALID");
    }
    @Test void hardLinkedGuardFailsClosed() throws Exception {
        Files.createLink(temp.resolve("guard-alias"),binding.root().resolve("host.guard"));
        assertThatThrownBy(() -> InstallationHostGuard.acquire(binding)).hasMessage("HOST_BINDING_INVALID");
    }
    @Test void replacedGuardIsDetectedAndNeverAutomaticallyRecreated() throws Exception {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            Path file=binding.root().resolve("host.guard");Files.move(file,temp.resolve("old-guard"));
            Files.write(file,new byte[0]);Files.setPosixFilePermissions(file,PosixFilePermissions.fromString("rw-------"));
            assertThatThrownBy(() -> guard.checkHeld(OwnershipFixtures.INSTALL)).hasMessage("HOST_BINDING_INVALID");
        }
        assertThatThrownBy(() -> InstallationHostGuard.acquire(binding)).hasMessage("HOST_BINDING_INVALID");
        Files.delete(binding.root().resolve("host.guard"));
        assertThatThrownBy(() -> InstallationHostGuard.acquire(binding)).hasMessage("HOST_BINDING_INVALID");
        assertThat(binding.root().resolve("host.guard")).doesNotExist();
    }
    @Test void unsafeRootAndWrongInstallationAreRejected() {
        assertThatThrownBy(() -> new InstallationHostGuard.Binding("control",OwnershipFixtures.INSTALL,Path.of("relative"),binding.rootIdentity(),binding.guardIdentity(),binding.installationIdentity())).hasMessage("HOST_BINDING_INVALID");
        try(var guard=InstallationHostGuard.acquire(binding)) {
            assertThatThrownBy(() -> guard.checkHeld(UUID.randomUUID())).hasMessage("HOST_BINDING_INVALID");
            assertThat(binding.toString()).isEqualTo("HostBinding[redacted]");
            assertThat(guard.toString()).isEqualTo("InstallationHostGuard[redacted]");
        }
    }
    @Test void requestEvidenceIsCanonicalBoundedAndDefensivelyCopied() {
        byte[] input=OwnershipFixtures.REQUEST.clone();var r=new ImportOwnershipRepository.Reservation(UUID.randomUUID(),"p",OwnershipFixtures.HASH,input);
        input[0]=0;r.requestBytes()[0]=0;assertThat(r.requestBytes()).isEqualTo(OwnershipFixtures.REQUEST);
        for(byte[] bad:List.of("{ \"a\":1}".getBytes(),"{\"a\":1,\"a\":2}".getBytes(),new byte[65537],"{\"x\":1.5}".getBytes()))
            assertThatThrownBy(() -> new ImportOwnershipRepository.Reservation(UUID.randomUUID(),"p",OwnershipFixtures.HASH,bad)).hasMessage("INVALID_REQUEST").hasNoCause();
        assertThat(r.toString()).isEqualTo("ImportReservation[redacted]");
    }
}
