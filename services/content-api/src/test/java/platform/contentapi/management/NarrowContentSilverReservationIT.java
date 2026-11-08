package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.execution.*;
import static platform.contentapi.management.SourceFixtures.*;
import static org.assertj.core.api.Assertions.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentSilverReservationIT extends NarrowContentSilverReservationFixtures {
    @Test void originalChainReservesOnlySqlAndRetainsDistinctInspectionPrerequisite()throws Exception{
        original();var files=new AtomicReference<Map<String,String>>();var requestsAt=new AtomicInteger();var boundaries=new ArrayList<String>();
        var result=chain(b->{if(b.equals("CHECKPOINT_COMMITTED")){try{files.set(filesystem());requestsAt.set(objectRequests.get());}catch(Exception ex){throw new IllegalStateException(ex);}}},b->{boundaries.add(b);if(!b.equals("INSERTED"))assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();});
        assertThat(result).isEqualTo(silverFacts());assertThat(result.operation()).isEqualTo(silverOperation);assertThat(result.bronzeOperation()).isEqualTo(bronzeOperation);
        assertThat(result.nativeLaunchCompatible()).isFalse();assertThat(result.requiresBronzeInspection()).isTrue();assertThat(NarrowContentSilverReservationTestSupport.privateOneUseBridge()).isTrue();
        assertThat(result.checkpoint()).isNotEqualTo(result.requiredInspectionCheckpoint()).isNotEqualTo(UUID.fromString(bronzeDirectory().getFileName().toString()));
        assertThat(filesystem()).isEqualTo(files.get());assertThat(objectRequests).hasValue(requestsAt.get());assertThat(objectWrites).hasValue(2);assertThat(requests).hasValue(1);
        assertThat(boundaries).containsExactly("INSPECTED","BEFORE_RESERVATION","INSERTED","RESERVED");assertThat(count("content_silver_reservations")).isEqualTo(1);
        var c=SourceJson.parse(jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_silver_reservations",byte[].class));
        assertThat(c.get("requiresInspection").get("checkpointId").stringValue()).isEqualTo(result.requiredInspectionCheckpoint().toString());
        assertThat(c.get("runtime").get("registeredLedgerVersion").intValue()).isEqualTo(1);assertThat(c.has("protocolVersion")).isFalse();
        var d=SourceJson.parse(Files.readAllBytes(fetchDirectory().resolve("descriptor.json")));
        assertThat(c.get("fetch").get("ingestionId")).isEqualTo(d.get("ingestionId"));assertThat(c.get("fetch").get("ingestedAt")).isEqualTo(d.get("ingestedAt"));
        assertThat(SourceJson.canonical(c.get("bronze").get("verified"))).isEqualTo(Files.readAllBytes(bronzeDirectory().resolve("verified.json")));
        assertThat(new String(SourceJson.canonical(c),java.nio.charset.StandardCharsets.UTF_8)).doesNotContain("fixture-secret","fixture-access");retained();noReplay();
    }
    @Test void readableSilverOutputRootKeepsExistingCompatibility()throws Exception{
        original();Files.setPosixFilePermissions(roots.get(SILVER_BINDING),PosixFilePermissions.fromString("rwxr-xr-x"));
        var result=chain(b->{},b->{});assertThat(result).isEqualTo(silverFacts());retained();
    }
    @ParameterizedTest @ValueSource(strings={"MARKER_RENAMED","DIRECTORY_FORCED","ANCHOR_WRITTEN","BEFORE_CHECKPOINT","CHECKPOINT_COMMITTED"})
    void postPublicationDirectoryMetadataChangeCannotAuthorizeSilver(String boundary)throws Exception{
        original();var changed=new AtomicBoolean();
        assertThatThrownBy(()->chain(b->{if(b.equals(boundary))try{
            Files.setLastModifiedTime(bronzeDirectory(),java.nio.file.attribute.FileTime.fromMillis(1));changed.set(true);
        }catch(Exception ex){throw new IllegalStateException(ex);}},b->{})).isInstanceOf(RuntimeException.class);
        assertThat(changed).isTrue();assertThat(count("content_bronze_checkpoints")).isEqualTo(boundary.equals("CHECKPOINT_COMMITTED")?1:0);noSilver();noReplay();
    }
    @Test void postCommitCancellationCannotAuthorizeSilver()throws Exception{
        original();assertThatThrownBy(()->chain(b->{if(b.equals("CHECKPOINT_COMMITTED"))cancel();},b->{})).isInstanceOf(RuntimeException.class);
        assertThat(count("content_bronze_checkpoints")).isEqualTo(1);noSilver();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"CHECKPOINT_COMMITTED","INSPECTED","BEFORE_RESERVATION","INSERTED","RESERVED"})
    void interruptionNeverReusesHistoricalCheckpoint(String boundary)throws Exception{
        original();var hit=new AtomicBoolean();java.util.function.Consumer<String> fault=b->{if(b.equals(boundary)){hit.set(true);throw new IllegalStateException("STOP");}};
        assertThatThrownBy(()->chain(fault,fault)).hasMessage("STOP");assertThat(hit).isTrue();assertThat(count("content_bronze_checkpoints")).isEqualTo(1);
        assertThat(count("content_silver_reservations")).isEqualTo(boundary.equals("RESERVED")?1:0);retained();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"run","attempt","worker","coordinator","stop","cancel","deadline"})
    void changedOwnerOrControlsStopsReservation(String change)throws Exception{
        original();var changed=new AtomicBoolean();
        assertThatThrownBy(()->chain(b->{},b->{if(b.equals("INSPECTED")){changeOwner(change);changed.set(true);}})).isInstanceOf(RuntimeException.class);
        assertThat(changed).isTrue();noSilver();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"command.json","evidence.json","result.json","verified.json","fetch","credential","credential-mode","candidate-directory","silver","inspect","silver-root","temp-root","temp-mode","silver-symlink"})
    void dependencyOrTargetMutationStopsReservation(String change)throws Exception{
        original();var mutated=new AtomicBoolean();assertThatThrownBy(()->chain(b->{},b->{if(b.equals("INSPECTED"))try{
            if(change.equals("silver-root")||change.equals("temp-root")||change.equals("silver-symlink")){
                Path root=roots.get(change.equals("temp-root")?"content-TEMP":SILVER_BINDING);Path old=root.resolveSibling("old-root-"+UUID.randomUUID());Files.move(root,old);
                if(change.equals("silver-symlink"))Files.createSymbolicLink(root,old);else Files.createDirectory(root,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            }else if(change.equals("temp-mode"))Files.setPosixFilePermissions(roots.get("content-TEMP"),PosixFilePermissions.fromString("rwxr-xr-x"));
            else if(change.equals("fetch"))Files.writeString(fetchDirectory().resolve("payload.xml"),"changed");
            else if(change.equals("credential"))Files.writeString(credentials,"{}");
            else if(change.equals("credential-mode"))Files.setPosixFilePermissions(credentials,PosixFilePermissions.fromString("rw-r--r--"));
            else if(change.equals("candidate-directory"))Files.setLastModifiedTime(bronzeDirectory(),java.nio.file.attribute.FileTime.fromMillis(1));
            else if(change.equals("silver")||change.equals("inspect"))Files.createDirectory(attemptRoot().resolve(change.equals("silver")?"silver":"inspect-bronze"),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            else Files.writeString(bronzeDirectory().resolve(change),"changed");
            mutated.set(true);
        }catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);assertThat(mutated).isTrue();noSilver();
        if(change.equals("silver-symlink")){
            Path root=roots.get(SILVER_BINDING),target=Files.readSymbolicLink(root);int calls=objectRequests.get();
            assertThatThrownBy(()->chain(b->{},b->{})).isInstanceOf(RuntimeException.class);
            assertThat(Files.readSymbolicLink(root)).isEqualTo(target);assertThat(objectRequests).hasValue(calls);noSilver();
        }else noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"command.json","evidence.json","result.json","verified.json"})
    void sameBytesDifferentInodeNeverAdoptedAfterCheckpoint(String name)throws Exception{
        original();assertThatThrownBy(()->chain(b->{if(b.equals("CHECKPOINT_COMMITTED"))try{
            Path p=bronzeDirectory().resolve(name);byte[] raw=Files.readAllBytes(p);var time=Files.getLastModifiedTime(p);Files.move(p,temp.resolve("old-candidate"));
            Files.createFile(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));Files.write(p,raw);Files.setLastModifiedTime(p,time);
        }catch(Exception ex){throw new IllegalStateException(ex);}},b->{})).isInstanceOf(RuntimeException.class);noSilver();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"bronze-checkpoint","silver-reservation"})
    void lostAcknowledgmentNeverRestoresLiveSuccessor(String target)throws Exception{
        original();var armed=new AtomicBoolean();var wrapped=loseCommitAcknowledgment(armed);
        var j=new org.springframework.jdbc.core.JdbcTemplate(wrapped.getDataSource()){@Override public int update(String sql,Object... args){int n=super.update(sql,args);
            if(sql.startsWith("INSERT INTO ddip_management."+(target.equals("bronze-checkpoint")?"content_bronze_checkpoints(":"content_silver_reservations(")))armed.set(true);return n;}};
        var t=transactions(j);var w=WorkerTestSupport.enabled(j,t,INSTALL);var entered=new AtomicBoolean();
        assertThatThrownBy(()->chain(j,t,w,b->{},b->entered.set(true))).isInstanceOf(RuntimeException.class);
        assertThat(armed).isFalse();assertThat(entered.get()).isEqualTo(!target.equals("bronze-checkpoint"));assertThat(count("content_bronze_checkpoints")).isEqualTo(1);
        assertThat(count("content_silver_reservations")).isEqualTo(target.equals("bronze-checkpoint")?0:1);assertThatThrownBy(()->w.heartbeat(guard,owner)).hasMessage("OWNERSHIP_LOST");retained();noReplay();
    }
    @Test void verifierHeartbeatOwnerFlowsIntoReservation()throws Exception{
        original();var gate=new CountDownLatch(1);try(var scheduler=Executors.newSingleThreadScheduledExecutor()){
            var result=chain(b->{if(b.equals("BEFORE_DELIVERY")){objectGate=gate;scheduler.schedule(gate::countDown,7,TimeUnit.SECONDS);}},b->{});
            assertThat(result).isEqualTo(silverFacts());assertThat(jdbc.queryForObject("SELECT owner_coordinator_version FROM ddip_management.content_silver_reservations",Long.class)).isGreaterThan(owner.worker().coordinatorVersion());retained();
        }finally{gate.countDown();}
    }
    @Test void historicalFactsNeedNoFilesCredentialsOrOwner()throws Exception{
        original();var result=chain(b->{},b->{});guard.close();guard=null;Files.move(attemptRoot(),attemptRoot().resolveSibling("removed-attempt"));Files.delete(credentials);assertThat(silverFacts()).isEqualTo(result);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"null-silver","same-fetch","same-bronze","null-fault","null-completion","ambient"})
    void invalidChainFailsBeforeEffects(String invalid)throws Exception{
        original();var files=filesystem();if(invalid.equals("null-silver"))silverOperation=null;if(invalid.equals("same-fetch"))silverOperation=operation;if(invalid.equals("same-bronze"))silverOperation=bronzeOperation;
        assertThatThrownBy(()->{if(invalid.equals("ambient"))tx.required(()->chain(b->{},b->{}));else chain(invalid.equals("null-completion")?null:b->{},invalid.equals("null-fault")?null:b->{});}).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(requests).hasValue(0);assertThat(objectRequests).hasValue(0);assertThat(filesystem()).isEqualTo(files);assertThat(count("content_native_operations")).isZero();noExecution();
    }
}
