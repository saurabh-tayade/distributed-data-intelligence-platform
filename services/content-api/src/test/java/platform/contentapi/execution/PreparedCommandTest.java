package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;

class PreparedCommandTest {
    @TempDir Path temp;InstallationHostGuard guard;WorkerRepository.Owner owner;PreflightFiles files;byte[] bytes;
    @BeforeEach void setup()throws Exception{
        guard=InstallationHostGuard.acquire(OwnershipFixtures.host(temp.toRealPath()));
        var session=new WorkerRepository.Session(OwnershipFixtures.INSTALL,UUID.randomUUID(),"test","a".repeat(64),"b".repeat(64),"c".repeat(64),"READY",1,0,0);
        owner=new WorkerRepository.Owner(session,UUID.randomUUID(),UUID.randomUUID(),0,0,"RUNNING");files=new PreflightFiles(guard,owner.runId(),owner.attemptId(),UUID.randomUUID(),"PUBLISH_CONTENT");bytes=SourceJson.canonical(SourceJson.object().put("protocolVersion",1));
    }
    @AfterEach void close(){guard.close();}
    @Test void preparationIsCompleteAndImmutableBeforeAnIntentCanReferToIt()throws Exception{
        var command=PreparedCommand.prepare(guard,files.directory,bytes,65536);command.verify(guard,owner,files.address,SourceJson.sha(bytes));
        assertThat(Files.readAllBytes(files.directory.resolve("command.json"))).isEqualTo(bytes);assertThat(Files.exists(files.directory.resolve("command.json.preparing"))).isFalse();
        assertThatThrownBy(()->PreparedCommand.prepare(guard,files.directory,bytes,65536)).hasMessage("INPUT_CHANGED");
    }
    @Test void interruptedPreparationIsRetainedWithoutBecomingACommand()throws Exception{
        Files.writeString(files.directory.resolve("command.json.preparing"),"{");Files.setPosixFilePermissions(files.directory.resolve("command.json.preparing"),java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        assertThatThrownBy(()->PreparedCommand.prepare(guard,files.directory,bytes,65536)).hasMessage("INPUT_CHANGED");assertThat(Files.exists(files.directory.resolve("command.json"))).isFalse();assertThat(Files.readString(files.directory.resolve("command.json.preparing"))).isEqualTo("{");
    }
    @Test void missingChangedOrSubstitutedCommandsAreRefused()throws Exception{
        var command=PreparedCommand.prepare(guard,files.directory,bytes,65536);Files.writeString(files.directory.resolve("command.json"),"{}");
        assertThatThrownBy(()->command.verify(guard,owner,files.address,SourceJson.sha(bytes))).hasMessage("INPUT_CHANGED");Files.delete(files.directory.resolve("command.json"));
        assertThatThrownBy(()->command.verify(guard,owner,files.address,SourceJson.sha(bytes))).hasMessage("INPUT_UNAVAILABLE");
    }
    @Test void aPreparedCommandCannotBeSubstitutedForAnotherRunAttemptOrNamespace(){
        var command=PreparedCommand.prepare(guard,files.directory,bytes,65536);
        assertThatThrownBy(()->command.verify(guard,owner,files.address+"-other",SourceJson.sha(bytes))).hasMessage("INPUT_CHANGED");
        var other=new WorkerRepository.Owner(owner.worker(),UUID.randomUUID(),owner.attemptId(),0,0,"RUNNING");assertThatThrownBy(()->command.verify(guard,other,files.address,SourceJson.sha(bytes))).hasMessage("INPUT_CHANGED");
    }
    @Test void invalidOrOversizedCommandsFailBeforeCreatingAFile(){
        assertThatThrownBy(()->PreparedCommand.prepare(guard,files.directory,new byte[0],65536)).isInstanceOf(PreflightFailure.class);
        assertThatThrownBy(()->PreparedCommand.prepare(guard,files.directory,bytes,1)).isInstanceOf(PreflightFailure.class);assertThat(Files.exists(files.directory.resolve("command.json"))).isFalse();
    }
}
