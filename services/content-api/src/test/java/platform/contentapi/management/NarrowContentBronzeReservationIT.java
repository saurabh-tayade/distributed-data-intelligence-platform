package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Real PostgreSQL + loopback FETCH; Bronze reservation never contacts an object store. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentBronzeReservationIT extends NarrowContentFetchLaunchFixtures {
    UUID bronzeOperation;Path credentials;
    @BeforeEach void bronzeIdentity(){bronzeOperation=UUID.randomUUID();}
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        try{
            Path secretFile=Files.createTempFile(temp,"bronze-credentials-",".json",PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            if(product.equals(CONTENT))credentials=secretFile;
            Files.write(secretFile,SourceJson.canonical(SourceJson.object().put("accessKey","fixture-access").put("secretKey","fixture-secret")));
            byte[] raw=SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:1").put("region","local").put("bucket","bronze").put("credentialsFile",secretFile.toString()));
            jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,raw,SourceJson.sha(raw),INSTALL);
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
    void checkpoint(){prepared();NarrowContentFetchCompletionTestSupport.launchAndCheckpoint(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,b->{});}
    NarrowContentBronzeReservationTestSupport.Facts reserve(){return reserve(b->{});}
    NarrowContentBronzeReservationTestSupport.Facts reserve(Consumer<String> fault){return NarrowContentBronzeReservationTestSupport.reserve(jdbc,tx,workers,guard,owner,operation,bronzeOperation,fault);}
    NarrowContentBronzeReservationTestSupport.Facts reservationFacts(){return NarrowContentBronzeReservationTestSupport.inspect(jdbc,tx,bronzeOperation);}
    void rejected()throws Exception{var files=filesystem();var before=metadata();assertThatThrownBy(this::reserve).isInstanceOf(RuntimeException.class);assertThat(filesystem()).isEqualTo(files);assertThat(metadata()).isEqualTo(before);assertThat(count("content_bronze_reservations")).isZero();retained();}
    @Test void reservesOnlyImmutableNativeEvidenceAndNeverLaunchesBronze()throws Exception{
        checkpoint();var files=filesystem();var before=metadata();var boundaries=new ArrayList<String>();var result=reserve(b->{boundaries.add(b);if(!b.equals("INSERTED"))assertThat(TransactionSynchronizationManager.isActualTransactionActive()).as(b).isFalse();});
        assertThat(result.operation()).isEqualTo(bronzeOperation);assertThat(result.fetchOperation()).isEqualTo(operation);assertThat(result.attempt()).isEqualTo(owner.attemptId());assertThat(result.nativeLaunchCompatible()).isFalse();
        assertThat(result.checkpoint()).isNotEqualTo(jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_native_operations",UUID.class));
        assertThat(reservationFacts()).isEqualTo(result);assertThat(filesystem()).isEqualTo(files);assertThat(metadata()).isEqualTo(before);assertThat(requests).hasValue(1);retained();
        assertThat(boundaries).containsSubsequence("JOURNAL_REOPENED","INSPECTED","BEFORE_RESERVATION","JOURNAL_REOPENED","INSERTED","RESERVED");
        String bytes=new String(jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_bronze_reservations",byte[].class),java.nio.charset.StandardCharsets.UTF_8);
        assertThat(bytes).doesNotContain("fixture-secret","fixture-access").contains("STORE_BRONZE","credentialsSha256");
    }
    @Test void sameLiveChainCarriesHeartbeatOwnerWithoutExposingContinuation()throws Exception{
        prepared();responseGate=new CountDownLatch(1);
        try(var pool=Executors.newSingleThreadScheduledExecutor()){
            pool.schedule(()->responseGate.countDown(),7,TimeUnit.SECONDS);
            var result=NarrowContentBronzeReservationTestSupport.launchCheckpointAndReserve(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,bronzeOperation,b->{});
            assertThat(reservationFacts()).isEqualTo(result);assertThat(jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class)).isGreaterThan(owner.worker().coordinatorVersion());
            assertThat(requests).hasValue(1);retained();
        }finally{responseGate.countDown();}
    }
    @ParameterizedTest @ValueSource(strings={"null-operation","same-operation","null-fault"})
    void invalidSuccessorArgumentsFailBeforeFetchEffects(String invalid)throws Exception{
        prepared();var files=filesystem();UUID target=invalid.equals("null-operation")?null:invalid.equals("same-operation")?operation:bronzeOperation;
        Consumer<String> fault=invalid.equals("null-fault")?null:b->{};
        assertThatThrownBy(()->NarrowContentBronzeReservationTestSupport.launchCheckpointAndReserve(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,target,fault)).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(requests).hasValue(0);assertThat(count("content_native_launch_phases")).isZero();assertThat(count("content_fetch_checkpoints")).isZero();assertThat(count("content_bronze_reservations")).isZero();assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void nativeCandidateWithoutCompletionCannotReserve()throws Exception{prepared();launch(b->{});rejected();}
    @Test void wrongFetchOperationCannotReserve()throws Exception{checkpoint();operation=UUID.randomUUID();rejected();}
    @ParameterizedTest @ValueSource(strings={"run","attempt","worker","coordinator","stop","cancel","deadline"})
    void changedOwnerOrControlsAfterInspectionCannotReserve(String field)throws Exception{
        checkpoint();var files=filesystem();assertThatThrownBy(()->reserve(b->{if(b.equals("INSPECTED"))switch(field){
            case "run"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());
            case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
            case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",owner.worker().instanceId());
            case "coordinator"->jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");
            case "stop"->jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
            case "cancel"->cancel();
            case "deadline"->jdbc.update("UPDATE ddip_management.attempts SET started_at=clock_timestamp()-interval '3 hours' WHERE attempt_id=?",owner.attemptId());
        }})).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_reservations")).isZero();assertThat(filesystem()).isEqualTo(files);retained();
    }
    @ParameterizedTest @ValueSource(strings={"command.json","descriptor.json","metadata.json","payload.xml","result.json","complete.json"})
    void changedDependencyAfterInspectionCannotReserve(String name)throws Exception{
        checkpoint();assertThatThrownBy(()->reserve(b->{if(b.equals("INSPECTED"))try{Files.writeString(fetchDirectory().resolve(name),"changed");}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_reservations")).isZero();retained();
    }
    @Test void identicalBytesReplacementCannotReserve()throws Exception{
        checkpoint();assertThatThrownBy(()->reserve(b->{if(b.equals("JOURNAL_REOPENED"))try{Path path=fetchDirectory().resolve("payload.xml");byte[] bytes=Files.readAllBytes(path);var time=Files.getLastModifiedTime(path);Files.move(path,temp.resolve("old-payload"));Files.createFile(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));Files.write(path,bytes);Files.setLastModifiedTime(path,time);}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_reservations")).isZero();retained();
    }
    @ParameterizedTest @ValueSource(strings={"changed","mode","missing"})
    void credentialChangesCannotReserve(String change)throws Exception{
        checkpoint();assertThatThrownBy(()->reserve(b->{if(b.equals("INSPECTED"))try{switch(change){case "changed"->Files.writeString(credentials,"{}");case "mode"->Files.setPosixFilePermissions(credentials,PosixFilePermissions.fromString("rw-r--r--"));case "missing"->Files.delete(credentials);}}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_reservations")).isZero();retained();
    }
    @ParameterizedTest @ValueSource(strings={"INSPECTED","BEFORE_RESERVATION","JOURNAL_REOPENED","INSERTED","RESERVED"})
    void boundaryFailuresNeverRepeatAConsumedReservation(String boundary)throws Exception{
        checkpoint();var files=filesystem();assertThatThrownBy(()->reserve(b->{if(b.equals(boundary))throw new IllegalStateException("TEST_BOUNDARY");})).hasMessage("TEST_BOUNDARY");
        assertThat(count("content_bronze_reservations")).isEqualTo(boundary.equals("RESERVED")?1:0);assertThat(filesystem()).isEqualTo(files);
        if(boundary.equals("RESERVED")){var history=reservationFacts();workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(this::reserve).isInstanceOf(RuntimeException.class);assertThat(reservationFacts()).isEqualTo(history);}retained();
    }
    @Test void duplicateNewOperationAndMutationAreRejected(){
        checkpoint();var first=reserve();assertThatThrownBy(this::reserve).isInstanceOf(RuntimeException.class);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);bronzeOperation=UUID.randomUUID();assertThatThrownBy(this::reserve).isInstanceOf(RuntimeException.class);bronzeOperation=first.operation();
        for(String sql:List.of("UPDATE ddip_management.content_bronze_reservations SET operation_id=operation_id","DELETE FROM ddip_management.content_bronze_reservations","TRUNCATE ddip_management.content_bronze_reservations CASCADE"))assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(RuntimeException.class);
        assertThat(reservationFacts()).isEqualTo(first);retained();
    }
    @Test void inspectionNeedsNoOwnerCredentialsOrAttemptFilesystem()throws Exception{
        checkpoint();var first=reserve();workers.interrupt(guard,owner);Files.move(attemptRoot(),attemptRoot().resolveSibling("removed-attempt"));Files.delete(credentials);guard.close();guard=null;
        assertThat(reservationFacts()).isEqualTo(first);assertThat(reservationFacts().nativeLaunchCompatible()).isFalse();noExecution();
    }
    @Test void lostCommitAcknowledgmentConsumesReservationAndStopsOwnerChannel(){
        checkpoint();var armed=new AtomicBoolean();var j=loseCommitAcknowledgment(armed);var t=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(j.getDataSource())));var w=WorkerTestSupport.enabled(j,t,INSTALL);
        assertThatThrownBy(()->NarrowContentBronzeReservationTestSupport.reserve(j,t,w,guard,owner,operation,bronzeOperation,b->{if(b.equals("INSERTED"))armed.set(true);})).isInstanceOf(RuntimeException.class);
        assertThat(count("content_bronze_reservations")).isEqualTo(1);assertThat(reservationFacts().nativeLaunchCompatible()).isFalse();
        assertThatThrownBy(()->NarrowContentBronzeReservationTestSupport.reserve(j,t,w,guard,owner,operation,UUID.randomUUID(),b->{})).hasMessage("OWNERSHIP_LOST");retained();
    }
    @Test void confirmedFetchCommitAcknowledgmentLossCannotAuthorizeSuccessor(){
        prepared();launch(b->{});var armed=new AtomicBoolean();var j=loseCommitAcknowledgment(armed);var t=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(j.getDataSource())));var w=WorkerTestSupport.enabled(j,t,INSTALL);
        var completed=NarrowContentFetchCompletionTestSupport.complete(j,t,w,guard,owner,operation,b->{if(b.equals("CHECKPOINT_INSERTED"))armed.set(true);});
        assertThat(completed.checkpointed()).isTrue();assertThat(count("content_fetch_checkpoints")).isEqualTo(1);
        assertThatThrownBy(()->NarrowContentBronzeReservationTestSupport.reserve(j,t,w,guard,owner,operation,bronzeOperation,b->{})).hasMessage("OWNERSHIP_LOST");
        assertThat(count("content_bronze_reservations")).isZero();retained();
    }
    Map<String,Object> pendingRow(){var captured=new AtomicReference<Map<String,Object>>();assertThatThrownBy(()->reserve(b->{if(b.equals("INSERTED")){captured.set(jdbc.queryForMap("SELECT * FROM ddip_management.content_bronze_reservations"));throw new IllegalStateException("CAPTURE");}})).hasMessage("CAPTURE");return captured.get();}
    void insert(Map<String,Object> r){jdbc.update("INSERT INTO ddip_management.content_bronze_reservations(operation_id,fetch_operation_id,attempt_id,checkpoint_id,command_bytes,command_sha256,reservation_bytes,reservation_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",r.get("operation_id"),r.get("fetch_operation_id"),r.get("attempt_id"),r.get("checkpoint_id"),r.get("command_bytes"),r.get("command_sha256"),r.get("reservation_bytes"),r.get("reservation_sha256"),r.get("owner_coordinator_version"),r.get("owner_worker_version"),r.get("owner_run_version"),r.get("owner_attempt_version"));}
    @ParameterizedTest @ValueSource(strings={"checkpoint","fetch","attempt","owner","runtime","journal","storage","credential","field","reservation"})
    void directSqlCannotForgeIndependentReservationIdentity(String change){
        checkpoint();var r=new HashMap<>(pendingRow());var command=(ObjectNode)SourceJson.parse((byte[])r.get("command_bytes"));var facts=(ObjectNode)SourceJson.parse((byte[])r.get("reservation_bytes"));
        switch(change){
            case "checkpoint"->r.put("checkpoint_id",UUID.randomUUID());case "fetch"->r.put("fetch_operation_id",UUID.randomUUID());case "attempt"->r.put("attempt_id",UUID.randomUUID());case "owner"->r.put("owner_coordinator_version",-1L);
            case "runtime"->command.put("runtimeSha256","0".repeat(64));case "journal"->((ObjectNode)command.get("journal")).put("directory","/foreign");case "storage"->((ObjectNode)command.get("storage")).put("bindingId","foreign");
            case "credential"->((ObjectNode)command.get("storage").get("credentialsStamp")).put("size",8193);case "field"->command.put("unexpected",true);case "reservation"->facts.put("ledgerSha256","0".repeat(64));
        }
        byte[] c=SourceJson.canonical(command);r.put("command_bytes",c);r.put("command_sha256",SourceJson.sha(c));facts.put("commandSha256",SourceJson.sha(c));byte[] f=SourceJson.canonical(facts);r.put("reservation_bytes",f);r.put("reservation_sha256",SourceJson.sha(f));
        assertThatThrownBy(()->insert(r)).isInstanceOf(org.springframework.dao.DataAccessException.class);assertThat(count("content_bronze_reservations")).isZero();retained();
    }
    @Test void emptyExistingBronzeDirectoryCannotBeAdopted()throws Exception{
        checkpoint();var row=pendingRow();Path directory=attemptRoot().resolve("store-bronze/"+row.get("checkpoint_id"));
        Files.createDirectories(directory,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));rejected();
    }
    @Test void directSqlValidReservationRetainsExactIdentity(){
        checkpoint();var row=pendingRow();insert(row);assertThat(reservationFacts().operation()).isEqualTo(bronzeOperation);assertThat(count("content_bronze_reservations")).isEqualTo(1);retained();
    }
    @Test void fetchOnlyAssessmentRejectsSuccessorReservation(){
        checkpoint();reserve();assertThatThrownBy(()->NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId())).hasMessage("WORKER_NOT_READY");retained();
    }
    @ParameterizedTest @ValueSource(strings={"duplicate","float","string","oversized","unknown"})
    void directSqlRejectsDuplicateScalarOversizedAndUnknownEnvelopes(String change){
        checkpoint();var row=new HashMap<>(pendingRow());String c=new String((byte[])row.get("command_bytes"),java.nio.charset.StandardCharsets.UTF_8);
        c=switch(change){case "duplicate"->c.replace("\"protocolVersion\":1","\"protocolVersion\":1,\"protocolVersion\":1");case "float"->c.replace("\"protocolVersion\":1","\"protocolVersion\":1.0");case "string"->c.replace("\"protocolVersion\":1","\"protocolVersion\":\"1\"");case "oversized"->" ".repeat(65536)+c;default->c.substring(0,c.length()-1)+",\"unknown\":true}";};
        byte[] bytes=c.getBytes(java.nio.charset.StandardCharsets.UTF_8);row.put("command_bytes",bytes);row.put("command_sha256",SourceJson.sha(bytes));
        assertThatThrownBy(()->insert(row)).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_reservations")).isZero();retained();
    }
    @Test void assessmentCannotReturnFetchOnlyAfterConcurrentSuccessorReservation(){
        checkpoint();var once=new AtomicBoolean();
        assertThatThrownBy(()->NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId(),"ABSENT",b->{if(b.equals("JOURNAL_REOPENED") && once.compareAndSet(false,true))reserve();})).hasMessage("WORKER_NOT_READY");
        assertThat(count("content_bronze_reservations")).isEqualTo(1);retained();
    }
    @Test void concurrentSqlReservationsSerializeToOneConsumedSlot()throws Exception{
        checkpoint();var first=pendingRow();var second=new HashMap<>(first);UUID other=UUID.randomUUID();second.put("operation_id",other);
        var facts=(ObjectNode)SourceJson.parse((byte[])second.get("reservation_bytes"));facts.put("operationId",other.toString());byte[] bytes=SourceJson.canonical(facts);second.put("reservation_bytes",bytes);second.put("reservation_sha256",SourceJson.sha(bytes));
        var inserted=new CountDownLatch(1);var release=new CountDownLatch(1);var started=new CountDownLatch(1);
        try(var pool=Executors.newFixedThreadPool(2)){
            var a=pool.submit(()->new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())).execute(status->{insert(first);inserted.countDown();try{if(!release.await(15,TimeUnit.SECONDS))throw new IllegalStateException("TIMEOUT");}catch(InterruptedException ex){throw new IllegalStateException(ex);}return true;}));
            assertThat(inserted.await(15,TimeUnit.SECONDS)).isTrue();
            var b=pool.submit(()->{started.countDown();try{insert(second);return true;}catch(org.springframework.dao.DataAccessException ex){return false;}});
            assertThat(started.await(15,TimeUnit.SECONDS)).isTrue();release.countDown();assertThat(a.get(15,TimeUnit.SECONDS)).isTrue();assertThat(b.get(15,TimeUnit.SECONDS)).isFalse();
        }finally{release.countDown();}
        assertThat(count("content_bronze_reservations")).isEqualTo(1);assertThat(reservationFacts().operation()).isEqualTo(bronzeOperation);retained();
    }
    @ParameterizedTest @ValueSource(strings={"ISOLATION_REPEATABLE_READ","ISOLATION_SERIALIZABLE"})
    void directSqlRejectsSnapshotIsolation(String level){checkpoint();var row=pendingRow();var transaction=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));transaction.setIsolationLevelName(level);assertThatThrownBy(()->transaction.execute(status->{insert(row);return null;})).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_reservations")).isZero();retained();}
}
