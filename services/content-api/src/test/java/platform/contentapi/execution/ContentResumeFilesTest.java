package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;

class ContentResumeFilesTest {
    @TempDir Path temp;InstallationHostGuard guard;UUID run=UUID.randomUUID(),control=UUID.randomUUID();PreflightFiles files;
    @BeforeEach void setup()throws Exception{guard=InstallationHostGuard.acquire(OwnershipFixtures.host(temp.toRealPath()));files=new PreflightFiles(guard,run,control,UUID.randomUUID(),"VERIFY_RESULT");files.write("command.json",SourceJson.canonical(SourceJson.object()),65536);}
    @AfterEach void close(){guard.close();}
    void seal(boolean live){RssGoldExecution.recordChild(files,live?guard.process():new InstallationHostGuard.ProcessIdentity(UUID.randomUUID(),guard.process().pid(),java.time.Instant.EPOCH));try{Files.writeString(files.directory.resolve("result.json"),"{}");Files.setPosixFilePermissions(files.directory.resolve("result.json"),java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));}catch(Exception ex){throw new AssertionError(ex);}ContentResumeEvidence.completed(files);}
    @Test void launchBeforePidRecordingIsNotSafeToRepeat(){assertThatThrownBy(()->ContentResumeEvidence.previous(guard,run,control)).hasMessage("INPUT_UNAVAILABLE");}
    @Test void sealedReusedPidCanBeQualifiedAgain(){seal(false);ContentResumeEvidence.previous(guard,run,control);}
    @Test void liveIdentityVetoesEvenACompletedSeal(){seal(true);assertThatThrownBy(()->ContentResumeEvidence.previous(guard,run,control)).hasMessage("RESUME_EVIDENCE_UNAVAILABLE");}
    @Test void malformedCompletionSealHasFixedRejection()throws Exception{seal(false);Files.writeString(files.directory.resolve("verified.json"),"{}");assertThatThrownBy(()->ContentResumeEvidence.previous(guard,run,control)).hasMessage("RESUME_EVIDENCE_INVALID");}
    @Test void existingNativeNamespaceCannotBeAdopted()throws Exception{Path root=Files.createDirectory(temp.toRealPath().resolve("native"));ContentResumeLineage.requireAbsent(root,"v1/processor_revision=1/publication=abc");Files.createDirectories(root.resolve("v1/processor_revision=1/publication=abc"));assertThatThrownBy(()->ContentResumeLineage.requireAbsent(root,"v1/processor_revision=1/publication=abc")).hasMessage("INPUT_INVALID");}
    @Test void namespaceAncestorSymlinkDoesNotAuthorizeLaunch()throws Exception{Path root=Files.createDirectory(temp.toRealPath().resolve("native")),outside=Files.createDirectory(temp.toRealPath().resolve("outside"));Files.createSymbolicLink(root.resolve("v1"),outside);assertThatThrownBy(()->ContentResumeLineage.requireAbsent(root,"v1/publication=abc")).hasMessage("INPUT_CHANGED");}
    @Test void changedRetainedResultCannotAuthorizeAnotherQualifier()throws Exception{seal(false);Files.writeString(files.directory.resolve("result.json"),"{\"changed\":true}");assertThatThrownBy(()->ContentResumeEvidence.previous(guard,run,control)).hasMessage("RESUME_EVIDENCE_INVALID");}
}
