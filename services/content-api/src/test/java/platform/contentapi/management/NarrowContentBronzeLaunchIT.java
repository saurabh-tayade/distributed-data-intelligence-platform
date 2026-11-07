package platform.contentapi.management;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.NarrowContentFetchLaunchIT.assertChildStopped;

/** No AWS: real children, local 403 object responses, durable fail-closed launch boundaries. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentBronzeLaunchIT extends NarrowContentBronzeLaunchFixtures {
    static final List<String> PHASES=List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED");
    @Test void unavailableStorageStillRecordsCandidateWithoutCompletionOrLegacyDelivery()throws Exception{
        checkpoint();byte[] historic=jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id='bronze'",byte[].class);
        var boundaries=new ArrayList<String>();var result=bronzeLaunch(boundaries::add);
        assertThat(result.operation()).isEqualTo(bronzeOperation);assertThat(result.fetchOperation()).isEqualTo(operation);assertThat(result.attempt()).isEqualTo(owner.attemptId());
        assertThat(result.phases()).containsExactlyElementsOf(PHASES);assertThat(result.storageState()).isEqualTo("UNAVAILABLE");assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.candidateForVerification()).isTrue();assertThat(result.completed()).isFalse();assertThat(result.nativeLaunchCompatible()).isFalse();
        assertThat(result.child()).isNotNull();assertThat(result.child().pid()).isPositive();assertChildStopped(result.child());
        assertThat(result.candidateSha256()).matches("[a-f0-9]{64}");assertThat(bronzeFacts()).isEqualTo(result);
        assertThat(boundaries).containsSubsequence("RESERVED","LAUNCH_BOUND","DIRECTORY_CREATED","COMMAND_WRITTEN","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","BEFORE_RELEASE","RELEASE_AUTHORIZED","BEFORE_DELIVERY","BEFORE_CANDIDATE","CANDIDATE_EXITED");
        assertThat(objectRequests.get()).isPositive();assertThat(requests).hasValue(1);assertThat(Files.readAllBytes(fetchDirectory().resolve("payload.xml"))).isEqualTo(payload);
        var evidence=SourceJson.parse(Files.readAllBytes(bronzeDirectory().resolve("evidence.json")));assertThat(evidence.get("receipt").isNull()).isTrue();
        assertThat(Files.exists(bronzeDirectory().resolve("result.json.preparing"))).isFalse();
        assertThat(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id='bronze'",byte[].class)).isEqualTo(historic);
        assertThat(SourceJson.parse(historic).get("adapter").stringValue()).isEqualTo("platform.ingestion.managed.BoundedManagedBronze");
        assertThatThrownBy(()->result.phases().add("FORGED")).isInstanceOf(UnsupportedOperationException.class);bronzeRetained();
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void realStrictChildVerifiesExactPairAndOnlyWritesWhenAbsent(boolean preexisting)throws Exception{
        checkpoint();objectError=null;if(preexisting)seedPair();
        byte[] fetchComplete=Files.readAllBytes(fetchDirectory().resolve("complete.json"));
        var result=bronzeLaunch(b->{});
        assertThat(result.phases()).containsExactlyElementsOf(PHASES);assertThat(result.storageState()).isEqualTo("EXACT");assertThat(result.exitCode()).isZero();
        assertThat(result.candidateForVerification()).isTrue();assertThat(result.completed()).isFalse();assertThat(result.nativeLaunchCompatible()).isFalse();assertChildStopped(result.child());
        assertThat(objects).hasSize(2);assertThat(objects.get(objectPath("payload"))).isEqualTo(payload);
        assertThat(objects.get(objectPath("metadata"))).isEqualTo(Files.readAllBytes(fetchDirectory().resolve("metadata.json")));
        assertThat(objectTypes.get(objectPath("payload"))).isEqualTo("application/octet-stream");assertThat(objectTypes.get(objectPath("metadata"))).isEqualTo("application/json");
        var evidence=SourceJson.parse(Files.readAllBytes(bronzeDirectory().resolve("evidence.json")));
        assertThat(evidence.get("state").stringValue()).isEqualTo("EXACT");assertThat(evidence.get("receipt").isObject()).isTrue();
        assertThat(Files.readAllBytes(fetchDirectory().resolve("complete.json"))).isEqualTo(fetchComplete);
        assertThat(bronzeFacts()).isEqualTo(result);assertThat(requests).hasValue(1);bronzeRetained(preexisting?0:2);
        int calls=objectRequests.get();assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(calls);
    }
    @ParameterizedTest @ValueSource(strings={"PAYLOAD_ONLY","METADATA_ONLY","CONFLICT"})
    void partialOrConflictingObjectsProduceFailureCandidateWithoutRepair(String state)throws Exception{
        checkpoint();objectError=null;
        if(state.equals("PAYLOAD_ONLY"))seedObject("payload",payload);
        else if(state.equals("METADATA_ONLY"))seedObject("metadata",Files.readAllBytes(fetchDirectory().resolve("metadata.json")));
        else{seedPair();seedObject("payload",new byte[]{1,2,3});}
        var before=new HashMap<String,byte[]>();objects.forEach((k,v)->before.put(k,v.clone()));
        var result=bronzeLaunch(b->{});assertThat(result.storageState()).isEqualTo(state);assertThat(result.exitCode()).isEqualTo(1);
        assertThat(result.candidateForVerification()).isTrue();assertThat(result.completed()).isFalse();assertChildStopped(result.child());
        var evidence=SourceJson.parse(Files.readAllBytes(bronzeDirectory().resolve("evidence.json")));assertThat(evidence.get("receipt").isNull()).isTrue();
        assertThat(objects.keySet()).isEqualTo(before.keySet());before.forEach((k,v)->assertThat(objects.get(k)).isEqualTo(v));bronzeRetained();
    }
    @Test void restartInspectionCannotAdoptOrRepeatReservation()throws Exception{
        checkpoint();var history=bronzeLaunch(b->{});var files=filesystem();int calls=objectRequests.get();
        for(String sql:List.of("UPDATE ddip_management.content_bronze_launch_phases SET phase=phase","DELETE FROM ddip_management.content_bronze_launch_phases","TRUNCATE ddip_management.content_bronze_launch_phases CASCADE"))assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(RuntimeException.class);
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThat(bronzeFacts()).isEqualTo(history);assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);
        bronzeOperation=UUID.randomUUID();assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);bronzeOperation=history.operation();
        assertThat(bronzeFacts()).isEqualTo(history);assertThat(objectRequests).hasValue(calls);assertThat(filesystem()).isEqualTo(files);bronzeRetained();
    }
    @Test void preexistingReservationIsNeverAdopted()throws Exception{
        checkpoint();NarrowContentBronzeReservationTestSupport.reserve(jdbc,tx,workers,guard,owner,operation,bronzeOperation,b->{});var files=filesystem();
        assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_launch_phases")).isZero();assertThat(filesystem()).isEqualTo(files);assertThat(objectRequests).hasValue(0);bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"RESERVED","LAUNCH_BOUND","DIRECTORY_CREATED","COMMAND_WRITTEN","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","BEFORE_RELEASE","RELEASE_AUTHORIZED","BEFORE_DELIVERY"})
    void interruptedLaunchConsumesReservationWithoutAnyStorageRequest(String boundary){
        checkpoint();assertThatThrownBy(()->bronzeLaunch(b->{if(b.equals(boundary))throw new IllegalStateException("TEST_BOUNDARY");})).isInstanceOf(RuntimeException.class);
        var history=bronzeFacts();assertThat(history.candidateForVerification()).isFalse();assertThat(objectRequests).hasValue(0);assertChildStopped(history.child());
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);assertThat(bronzeFacts()).isEqualTo(history);bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"DIRECTORY_CREATED","COMMAND_WRITTEN","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","BEFORE_RELEASE","RELEASE_AUTHORIZED","BEFORE_DELIVERY"})
    void staleCoordinatorAtEveryExternalBoundaryPreventsStorageRequest(String boundary){
        checkpoint();assertThatThrownBy(()->bronzeLaunch(b->{if(b.equals(boundary))jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");})).isInstanceOf(RuntimeException.class);
        assertThat(objectRequests).hasValue(0);assertThat(bronzeFacts().candidateForVerification()).isFalse();assertChildStopped(bronzeFacts().child());bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"run","attempt","worker","stop","cancel","guard"})
    void deliveryThreadRechecksExactOwnerAndControls(String change){
        checkpoint();long caller=Thread.currentThread().threadId();var writer=new AtomicLong();
        assertThatThrownBy(()->bronzeLaunch(b->{if(b.equals("BEFORE_DELIVERY")){writer.set(Thread.currentThread().threadId());switch(change){
            case "run"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());
            case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
            case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",owner.worker().instanceId());
            case "stop"->jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");case "cancel"->cancel();case "guard"->guard.close();
        }}})).isInstanceOf(RuntimeException.class);
        assertThat(writer).doesNotHaveValue(0).doesNotHaveValue(caller);assertThat(objectRequests).hasValue(0);assertChildStopped(bronzeFacts().child());bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"RELEASE_AUTHORIZED","CANDIDATE_EXITED"})
    void lostCommitAcknowledgmentKeepsHistoryAndNeverRetries(String phase){
        checkpoint();var armed=new AtomicBoolean();var wrapped=loseCommitAcknowledgment(armed);
        var j=new org.springframework.jdbc.core.JdbcTemplate(wrapped.getDataSource()){
            @Override public int update(String sql,Object... args){
                int changed=super.update(sql,args);
                // Arm the actual phase transaction, not an intervening read-only fence.
                if(sql.startsWith("INSERT INTO ddip_management.content_bronze_launch_phases(") && phase.equals(args[1]))armed.set(true);
                return changed;
            }
        };
        var t=transactions(j);var w=WorkerTestSupport.enabled(j,t,INSTALL);
        assertThatThrownBy(()->NarrowContentBronzeLaunchTestSupport.launch(j,t,w,guard,owner,operation,bronzeOperation,BRONZE_PROFILE,b->{})).hasMessage("MANAGEMENT_UNAVAILABLE");
        var history=bronzeFacts();assertThat(history.phases()).contains(phase);
        if(phase.equals("RELEASE_AUTHORIZED"))assertThat(objectRequests).hasValue(0);else assertThat(objectRequests.get()).isPositive();
        assertThatThrownBy(()->w.heartbeat(guard,owner)).hasMessage("OWNERSHIP_LOST");int calls=objectRequests.get();
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);assertThat(bronzeFacts()).isEqualTo(history);assertThat(objectRequests).hasValue(calls);assertChildStopped(history.child());bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"result.json.preparing","evidence.json","foreign-stage"})
    void foreignFilesBeforeDeliveryPreventStorageRequest(String name){
        checkpoint();assertThatThrownBy(()->bronzeLaunch(b->{if(b.equals("BEFORE_DELIVERY")){
            if(name.equals("foreign-stage")){try{Files.createDirectory(attemptRoot().resolve("check-inputs"),java.nio.file.attribute.PosixFilePermissions.asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rwx------")));}catch(Exception ex){throw new IllegalStateException(ex);}}
            else privateFile(bronzeDirectory().resolve(name));
        }})).isInstanceOf(RuntimeException.class);
        assertThat(objectRequests).hasValue(0);assertThat(bronzeFacts().candidateForVerification()).isFalse();assertChildStopped(bronzeFacts().child());bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"result.json.preparing","result.json","evidence.json"})
    void tamperedCandidatesCannotBeRecordedOrRelaunched(String name){
        checkpoint();assertThatThrownBy(()->bronzeLaunch(b->{if(b.equals("BEFORE_CANDIDATE"))try{
            if(name.equals("result.json.preparing"))privateFile(bronzeDirectory().resolve(name));else Files.writeString(bronzeDirectory().resolve(name),"{}");
        }catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);
        assertThat(objectRequests.get()).isPositive();assertThat(bronzeFacts().candidateForVerification()).isFalse();int calls=objectRequests.get();assertChildStopped(bronzeFacts().child());
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(calls);bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"credentials","payload","jar"})
    void changedDependenciesBeforeDeliveryPreventStorageRequest(String dependency){
        checkpoint();assertThatThrownBy(()->bronzeLaunch(b->{if(b.equals("BEFORE_DELIVERY"))try{
            Files.writeString(switch(dependency){case "credentials"->credentials;case "payload"->fetchDirectory().resolve("payload.xml");default->installedFetchJar;},"changed");
        }catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);
        assertThat(objectRequests).hasValue(0);assertThat(bronzeFacts().candidateForVerification()).isFalse();assertChildStopped(bronzeFacts().child());bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"HEARTBEAT","BEFORE_CANDIDATE","BEFORE_CANDIDATE_PHASE"})
    void replacedInstalledRuntimeAfterDeliveryCannotRecordCandidateOrRelaunch(String boundary){
        checkpoint();if(boundary.equals("HEARTBEAT"))objectGate=new CountDownLatch(1);var replaced=new AtomicBoolean();
        assertThatThrownBy(()->bronzeLaunch(b->{
            if(b.equals(boundary)&&(!b.equals("HEARTBEAT")||objectRequests.get()>0)&&replaced.compareAndSet(false,true))try{
                // Preserve bytes, mode, length and timestamp: the installed file identity must still match.
                Path previous=installedFetchJar.resolveSibling("previous.jar");Files.move(installedFetchJar,previous);
                Files.copy(previous,installedFetchJar,StandardCopyOption.COPY_ATTRIBUTES);
                assertThat(Files.readAllBytes(installedFetchJar)).isEqualTo(Files.readAllBytes(previous));
            }catch(Exception ex){throw new IllegalStateException(ex);}
        })).isInstanceOf(RuntimeException.class);
        assertThat(replaced).isTrue();assertThat(objectRequests.get()).isPositive();
        var history=bronzeFacts();assertThat(history.candidateForVerification()).isFalse();assertThat(history.phases()).doesNotContain("CANDIDATE_EXITED");assertChildStopped(history.child());
        int calls=objectRequests.get();workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);assertThat(bronzeFacts()).isEqualTo(history);assertThat(objectRequests).hasValue(calls);bronzeRetained();
    }
    @Test void sameLiveFetchChainCarriesHeartbeatRefreshedOwner()throws Exception{
        prepared();responseGate=new CountDownLatch(1);
        try(var pool=Executors.newSingleThreadScheduledExecutor()){
            pool.schedule(()->responseGate.countDown(),7,TimeUnit.SECONDS);
            var result=NarrowContentBronzeLaunchTestSupport.chain(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,bronzeOperation,BRONZE_PROFILE,b->{});
            assertThat(result.candidateForVerification()).isTrue();assertThat(bronzeFacts()).isEqualTo(result);assertThat(jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class)).isGreaterThan(owner.worker().coordinatorVersion());
            assertThat(requests).hasValue(1);bronzeRetained();
        }finally{responseGate.countDown();}
    }
    @ParameterizedTest @ValueSource(strings={"null-operation","same-operation","null-profile","null-boundary"})
    void invalidSuccessorArgumentsFailBeforeFetchEffects(String invalid)throws Exception{
        prepared();var files=filesystem();UUID target=invalid.equals("null-operation")?null:invalid.equals("same-operation")?operation:bronzeOperation;
        String profile=invalid.equals("null-profile")?null:BRONZE_PROFILE;
        java.util.function.Consumer<String> boundary=invalid.equals("null-boundary")?null:b->{};
        assertThatThrownBy(()->NarrowContentBronzeLaunchTestSupport.chain(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,target,profile,boundary)).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(requests).hasValue(0);assertThat(objectRequests).hasValue(0);assertThat(count("content_native_launch_phases")).isZero();
        assertThat(count("content_fetch_checkpoints")).isZero();assertThat(count("content_bronze_reservations")).isZero();assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void cancellationDuringObjectReadStopsChildWithoutCompletion(){
        checkpoint();objectGate=new CountDownLatch(1);var cancelled=new AtomicBoolean();
        assertThatThrownBy(()->bronzeLaunch(b->{if(b.equals("HEARTBEAT")&&objectRequests.get()>0&&cancelled.compareAndSet(false,true))cancel();})).isInstanceOf(RuntimeException.class);
        assertThat(cancelled).isTrue();assertChildStopped(bronzeFacts().child());assertThat(bronzeFacts().candidateForVerification()).isFalse();bronzeRetained();
    }
    @Test void historyInspectionNeedsNoCredentialsOwnerOrFilesystem()throws Exception{
        checkpoint();var history=bronzeLaunch(b->{});guard.close();guard=null;Files.move(attemptRoot(),attemptRoot().resolveSibling("removed-attempt"));Files.delete(credentials);
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThat(bronzeFacts()).isEqualTo(history);noExecution();
    }
}
