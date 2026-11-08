package platform.contentapi.management;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Real fetch, Bronze publisher and independent verifier children against loopback storage. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentBronzeCompletionIT extends NarrowContentBronzeLaunchFixtures {
    static final String VERIFY_PROFILE="strict-bronze-verification-test";
    @BeforeEach void verificationProfile(){
        var c=(ObjectNode)SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.content_bronze_launch_profiles WHERE launch_profile_id=?",byte[].class,BRONZE_PROFILE));
        c.remove("strictBronzeLaunchVersion");c.put("strictBronzeVerificationVersion",1);
        var manifest=(ObjectNode)c.get("manifest");manifest.put("adapter","platform.ingestion.managed.StrictBronzeVerifier");
        c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)));
        c.put("strictRuntimeSha256",SourceJson.sha(SourceJson.canonical(SourceJson.object().put("strictBronzeVerifierRuntimeVersion",1).put("adapter","platform.ingestion.managed.StrictBronzeVerifier").put("javaRuntime",Runtime.version().toString()))));
        byte[] bytes=SourceJson.canonical(c);jdbc.update("INSERT INTO ddip_management.content_bronze_verification_profiles(verification_profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",VERIFY_PROFILE,bytes,SourceJson.sha(bytes));
    }
    NarrowContentBronzeCompletionTestSupport.Observation complete(Consumer<String> fault){return NarrowContentBronzeCompletionTestSupport.launch(jdbc,tx,workers,guard,owner,operation,bronzeOperation,BRONZE_PROFILE,VERIFY_PROFILE,fault);}
    Optional<NarrowContentBronzeCompletionTestSupport.Observation> observed(){return NarrowContentBronzeCompletionTestSupport.inspect(jdbc,tx,bronzeOperation);}
    List<String> verificationPhases(){return jdbc.queryForList("SELECT phase FROM ddip_management.content_bronze_verification_phases WHERE operation_id=? ORDER BY CASE phase WHEN 'VERIFY_BOUND' THEN 1 WHEN 'START_AUTHORIZED' THEN 2 WHEN 'CHILD_RECORDED' THEN 3 WHEN 'RELEASE_AUTHORIZED' THEN 4 WHEN 'OBSERVED' THEN 5 END",String.class,bronzeOperation);}
    void noCheckpoint(){assertThat(count("content_bronze_checkpoints")).isZero();assertThat(observed()).isEmpty();retained();}
    void noReplay()throws Exception{int calls=objectRequests.get();var files=filesystem();assertThatThrownBy(()->complete(b->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(calls);assertThat(filesystem()).isEqualTo(files);}
    @ParameterizedTest @ValueSource(booleans={false,true})
    void independentVerifierProducesOnlyNativeAnchorAndCheckpoint(boolean preexisting)throws Exception{
        checkpoint();objectError=null;if(preexisting)seedPair();var before=metadata();before.remove("coordinator");before.remove("worker_instances");var boundaries=new ArrayList<String>();var atVerification=new AtomicInteger();
        var result=complete(b->{boundaries.add(b);if(b.equals("INVENTORY_READ"))atVerification.set(objectWrites.get());});
        assertThat(result.operation()).isEqualTo(bronzeOperation);assertThat(result.fetchOperation()).isEqualTo(operation);
        assertThat(result.run()).isEqualTo(owner.runId());assertThat(result.attempt()).isEqualTo(owner.attemptId());
        assertThat(result.checkpointed()).isTrue();assertThat(result.nativeLaunchCompatible()).isFalse();
        assertThat(result.candidateSha256()).isEqualTo(bronzeFacts().candidateSha256());assertThat(result.receiptSha256()).matches("[a-f0-9]{64}");
        assertThat(result.verifiedSha256()).isEqualTo(SourceJson.sha(Files.readAllBytes(bronzeDirectory().resolve("verified.json"))));
        assertThat(objectWrites).hasValue(preexisting?0:2).hasValue(atVerification.get());assertThat(requests).hasValue(1);
        assertThat(verificationPhases()).containsExactly("VERIFY_BOUND","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","OBSERVED");
        assertThat(boundaries).containsSubsequence("INVENTORY_READ","VERIFY_BOUND","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","BEFORE_DELIVERY","REMOTE_VERIFIED","OBSERVED_COMMITTED","ANCHOR_WRITTEN","CHECKPOINT_COMMITTED");
        var after=metadata();after.remove("coordinator");after.remove("worker_instances");assertThat(after).isEqualTo(before);assertThat(observed()).contains(result);assertThat(count("content_bronze_checkpoints")).isEqualTo(1);retained();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"cancel","candidate","directory"})
    void ordinaryCheckpointObservationRemainsHistoricalAfterPostCommitChange(String change)throws Exception{
        checkpoint();objectError=null;var changed=new AtomicBoolean();
        var result=complete(b->{if(b.equals("CHECKPOINT_COMMITTED"))try{
            if(change.equals("cancel"))cancel();
            else if(change.equals("candidate"))Files.writeString(bronzeDirectory().resolve("result.json"),"changed");
            else Files.setLastModifiedTime(bronzeDirectory(),java.nio.file.attribute.FileTime.fromMillis(1));
            changed.set(true);
        }catch(Exception ex){throw new IllegalStateException(ex);}});
        assertThat(changed).isTrue();assertThat(result.checkpointed()).isTrue();assertThat(observed()).contains(result);
        assertThat(count("content_silver_reservations")).isZero();retained();
    }
    @Test void wholeChainPreservesSingleFetchAndCheckpointsBothStages(){
        prepared();objectError=null;
        var result=NarrowContentBronzeCompletionTestSupport.chain(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,bronzeOperation,BRONZE_PROFILE,VERIFY_PROFILE,b->{});
        assertThat(result.checkpointed()).isTrue();assertThat(observed()).contains(result);assertThat(count("content_fetch_checkpoints")).isEqualTo(1);assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);retained();
    }
    @Test void slowFetchCarriesRefreshedOwnerThroughBothSuccessors()throws Exception{
        prepared();objectError=null;responseGate=new java.util.concurrent.CountDownLatch(1);
        try(var pool=java.util.concurrent.Executors.newSingleThreadScheduledExecutor()){
            pool.schedule(()->responseGate.countDown(),7,java.util.concurrent.TimeUnit.SECONDS);
            var result=NarrowContentBronzeCompletionTestSupport.chain(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,bronzeOperation,BRONZE_PROFILE,VERIFY_PROFILE,b->{});
            assertThat(result.checkpointed()).isTrue();assertThat(observed()).contains(result);
            assertThat(jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class)).isGreaterThan(owner.worker().coordinatorVersion());
            assertThat(requests).hasValue(1);retained();
        }finally{responseGate.countDown();}
    }
    @ParameterizedTest @ValueSource(strings={"INVENTORY_READ","VERIFY_BOUND","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","BEFORE_DELIVERY","REMOTE_VERIFIED","BEFORE_OBSERVED","OBSERVED_INSERTED","OBSERVED_COMMITTED","BEFORE_ANCHOR","PREFIX_CREATED","PREFIX_WRITTEN","PREFIX_FORCED","MARKER_RENAMED","DIRECTORY_FORCED","ANCHOR_WRITTEN","BEFORE_CHECKPOINT","CHECKPOINT_INSERTED","CHECKPOINT_COMMITTED"})
    void interruptedCompletionCannotRepeatHistoricalCandidate(String boundary)throws Exception{
        checkpoint();objectError=null;var reached=new AtomicBoolean();
        assertThatThrownBy(()->complete(b->{if(b.equals(boundary)){reached.set(true);throw new IllegalStateException("TEST_BOUNDARY");}})).isInstanceOf(RuntimeException.class);
        assertThat(reached).isTrue();assertThat(count("content_bronze_checkpoints")).isEqualTo(boundary.equals("CHECKPOINT_COMMITTED")?1:0);assertThat(objectWrites).hasValue(2);retained();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"payload","metadata","missing","unavailable"})
    void remoteMutationAfterCandidateCannotCreateCheckpointOrRepair(String mutation)throws Exception{
        checkpoint();objectError=null;var reached=new AtomicBoolean();
        assertThatThrownBy(()->complete(b->{if(b.equals("BEFORE_DELIVERY")){reached.set(true);try{
            if(mutation.equals("missing"))objects.remove(objectPath("payload"));else if(mutation.equals("unavailable"))objectError="AccessDenied";else seedObject(mutation,new byte[]{0});
        }catch(Exception ex){throw new IllegalStateException(ex);}}})).isInstanceOf(RuntimeException.class);
        assertThat(reached).isTrue();assertThat(objectWrites).hasValue(2);assertThat(verificationPhases()).doesNotContain("OBSERVED");noCheckpoint();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"AccessDenied","PARTIAL","CONFLICT"})
    void nonExactLaunchCandidateNeverStartsIndependentVerification(String state)throws Exception{
        checkpoint();
        if(state.equals("PARTIAL")){objectError=null;seedObject("payload",payload);}
        else if(state.equals("CONFLICT")){objectError=null;seedPair();seedObject("payload",new byte[]{1});}
        assertThatThrownBy(()->complete(b->{})).isInstanceOf(RuntimeException.class);
        assertThat(count("content_bronze_verification_phases")).isZero();assertThat(objectWrites).hasValue(0);noCheckpoint();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"OBSERVED_COMMITTED","BEFORE_ANCHOR","ANCHOR_WRITTEN","BEFORE_CHECKPOINT"})
    void changedCandidateAfterObservationNeverBecomesCheckpoint(String boundary)throws Exception{
        checkpoint();objectError=null;var reached=new AtomicBoolean();
        assertThatThrownBy(()->complete(b->{if(b.equals(boundary))try{reached.set(true);Files.writeString(bronzeDirectory().resolve("evidence.json"),"{}");}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);
        assertThat(reached).isTrue();assertThat(verificationPhases()).contains("OBSERVED");noCheckpoint();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"command.json","result.json","evidence.json"})
    void changedOriginalCandidateBeforeDeliveryCannotCheckpoint(String file){
        checkpoint();objectError=null;assertThatThrownBy(()->complete(b->{if(b.equals("BEFORE_DELIVERY"))try{Files.writeString(bronzeDirectory().resolve(file),"{}");}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);noCheckpoint();
    }
    @ParameterizedTest @ValueSource(strings={"run","attempt","worker","stop","cancel","guard","deadline"})
    void deliveryRechecksOwnerControlsAndDeadline(String change){
        checkpoint();objectError=null;var delivery=new AtomicBoolean();
        assertThatThrownBy(()->complete(b->{if(b.equals("BEFORE_DELIVERY")){delivery.set(true);switch(change){
            case "run"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());
            case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
            case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",owner.worker().instanceId());
            case "stop"->jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
            case "cancel"->cancel();case "guard"->guard.close();
            case "deadline"->{jdbc.execute("ALTER TABLE ddip_management.attempts DISABLE TRIGGER USER");try{jdbc.update("UPDATE ddip_management.attempts SET started_at=clock_timestamp()-interval '2 hours 1 second' WHERE attempt_id=?",owner.attemptId());}finally{jdbc.execute("ALTER TABLE ddip_management.attempts ENABLE TRIGGER USER");}}
        }}})).isInstanceOf(RuntimeException.class);assertThat(delivery).isTrue();assertThat(verificationPhases()).doesNotContain("OBSERVED");noCheckpoint();
    }
    @ParameterizedTest @ValueSource(strings={"RELEASE_AUTHORIZED","OBSERVED"})
    void lostPhaseCommitAcknowledgmentNeverRepeatsVerifier(String phase)throws Exception{
        checkpoint();objectError=null;var armed=new AtomicBoolean();var wrapped=loseCommitAcknowledgment(armed);
        var uncertain=new org.springframework.jdbc.core.JdbcTemplate(wrapped.getDataSource()){
            @Override public int update(String sql,Object... args){int changed=super.update(sql,args);
                if(sql.startsWith("INSERT INTO ddip_management.content_bronze_verification_phases(") && phase.equals(args[1]))armed.set(true);
                return changed;
            }
        };
        var transaction=transactions(uncertain);var repository=WorkerTestSupport.enabled(uncertain,transaction,INSTALL);
        assertThatThrownBy(()->NarrowContentBronzeCompletionTestSupport.launch(uncertain,transaction,repository,guard,owner,operation,bronzeOperation,BRONZE_PROFILE,VERIFY_PROFILE,b->{})).isInstanceOf(RuntimeException.class);
        assertThat(armed).isFalse();assertThatThrownBy(()->repository.heartbeat(guard,owner)).hasMessage("OWNERSHIP_LOST");assertThat(verificationPhases()).contains(phase);noCheckpoint();noReplay();
    }
    @Test void lostCheckpointAcknowledgmentAllowsOnlyReadOnlyConfirmation()throws Exception{
        checkpoint();objectError=null;var armed=new AtomicBoolean();var wrapped=loseCommitAcknowledgment(armed);
        var uncertain=new org.springframework.jdbc.core.JdbcTemplate(wrapped.getDataSource()){
            @Override public int update(String sql,Object... args){int changed=super.update(sql,args);
                if(sql.startsWith("INSERT INTO ddip_management.content_bronze_checkpoints("))armed.set(true);
                return changed;
            }
        };
        var transaction=transactions(uncertain);var repository=WorkerTestSupport.enabled(uncertain,transaction,INSTALL);
        var result=NarrowContentBronzeCompletionTestSupport.launch(uncertain,transaction,repository,guard,owner,operation,bronzeOperation,BRONZE_PROFILE,VERIFY_PROFILE,b->{});
        assertThat(armed).isFalse();assertThatThrownBy(()->repository.heartbeat(guard,owner)).hasMessage("OWNERSHIP_LOST");assertThat(result.checkpointed()).isTrue();assertThat(observed()).contains(result);
        assertThat(count("content_bronze_checkpoints")).isEqualTo(1);retained();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"PREFIX_WRITTEN","PREFIX_FORCED","MARKER_RENAMED","ANCHOR_WRITTEN"})
    void sameBytesAtNewInodeCannotReplacePrefixOrAnchor(String boundary)throws Exception{
        checkpoint();objectError=null;var replaced=new AtomicBoolean();
        assertThatThrownBy(()->complete(b->{if(b.equals(boundary))try{
            Path file=bronzeDirectory().resolve(boundary.startsWith("PREFIX")?"verified.json.preparing":"verified.json");
            byte[] bytes=Files.readAllBytes(file);var modified=Files.getLastModifiedTime(file);var identity=InstallationHostGuard.identity(file);
            Files.move(file,temp.resolve("original-"+UUID.randomUUID()));
            Files.createFile(file,java.nio.file.attribute.PosixFilePermissions.asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
            Files.write(file,bytes);Files.setLastModifiedTime(file,modified);
            assertThat(InstallationHostGuard.identity(file)).isNotEqualTo(identity);assertThat(Files.readAllBytes(file)).isEqualTo(bytes);replaced.set(true);
        }catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);
        assertThat(replaced).isTrue();assertThat(verificationPhases()).contains("OBSERVED");noCheckpoint();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"MARKER_RENAMED","ANCHOR_WRITTEN","BEFORE_CHECKPOINT"})
    void reintroducedPreparingFileAfterPublicationCannotCheckpoint(String boundary)throws Exception{
        checkpoint();objectError=null;var introduced=new AtomicBoolean();
        assertThatThrownBy(()->complete(b->{if(b.equals(boundary)){privateFile(bronzeDirectory().resolve("verified.json.preparing"));introduced.set(true);}})).isInstanceOf(RuntimeException.class);
        assertThat(introduced).isTrue();assertThat(verificationPhases()).contains("OBSERVED");noCheckpoint();noReplay();
    }
    @ParameterizedTest @ValueSource(strings={"null-fault","null-operation","same-operation","null-launch-profile","bad-verification-profile","null-guard","null-owner"})
    void invalidWholeChainArgumentsFailBeforeFetchEffects(String invalid)throws Exception{
        prepared();objectError=null;var files=filesystem();
        UUID target=invalid.equals("null-operation")?null:invalid.equals("same-operation")?operation:bronzeOperation;
        assertThatThrownBy(()->NarrowContentBronzeCompletionTestSupport.chain(jdbc,tx,workers,invalid.equals("null-guard")?null:guard,invalid.equals("null-owner")?null:owner,operation,LAUNCH_PROFILE,target,
                invalid.equals("null-launch-profile")?null:BRONZE_PROFILE,invalid.equals("bad-verification-profile")?"../invalid":VERIFY_PROFILE,invalid.equals("null-fault")?null:b->{})).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(requests).hasValue(0);assertThat(objectRequests).hasValue(0);assertThat(count("content_native_launch_phases")).isZero();
        assertThat(count("content_fetch_checkpoints")).isZero();assertThat(count("content_bronze_reservations")).isZero();assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"publisher","verifier"})
    void slowBronzeStageCarriesRefreshedOwnerToCheckpoint(String stage)throws Exception{
        checkpoint();objectError=null;var gate=new java.util.concurrent.CountDownLatch(1);
        try(var pool=java.util.concurrent.Executors.newSingleThreadScheduledExecutor()){
            if(stage.equals("publisher")){objectGate=gate;pool.schedule(gate::countDown,7,java.util.concurrent.TimeUnit.SECONDS);}
            var result=complete(b->{if(stage.equals("verifier")&&b.equals("BEFORE_DELIVERY")){objectGate=gate;pool.schedule(gate::countDown,7,java.util.concurrent.TimeUnit.SECONDS);}});
            assertThat(result.checkpointed()).isTrue();assertThat(observed()).contains(result);
            assertThat(jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class)).isGreaterThan(owner.worker().coordinatorVersion());
            assertThat(requests).hasValue(1);retained();
        }finally{gate.countDown();}
    }
    @ParameterizedTest @ValueSource(strings={"BEFORE_DELIVERY","REMOTE_VERIFIED"})
    void sameByteInstalledVerifierReplacementFailsClosed(String boundary)throws Exception{
        checkpoint();objectError=null;var replaced=new AtomicBoolean();
        assertThatThrownBy(()->complete(b->{if(b.equals(boundary))try{
            Path previous=installedFetchJar.resolveSibling("original-installed.jar");Files.move(installedFetchJar,previous);Files.copy(previous,installedFetchJar,StandardCopyOption.COPY_ATTRIBUTES);
            assertThat(Files.readAllBytes(installedFetchJar)).isEqualTo(Files.readAllBytes(previous));replaced.set(true);
        }catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);
        assertThat(replaced).isTrue();noCheckpoint();noReplay();
    }
    @Test void nullDirectLaunchFaultFailsBeforeBronzeEffects()throws Exception{
        checkpoint();objectError=null;var files=filesystem();
        assertThatThrownBy(()->complete(null)).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(objectRequests).hasValue(0);assertThat(count("content_bronze_reservations")).isZero();
        assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();
    }
    @Test void directCompletionRejectsAmbientTransactionBeforeBronzeEffects()throws Exception{
        checkpoint();objectError=null;var files=filesystem();
        assertThatThrownBy(()->tx.required(()->complete(b->{}))).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(objectRequests).hasValue(0);assertThat(count("content_bronze_reservations")).isZero();
        assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();
    }
    @Test void wholeChainRejectsAmbientTransactionBeforeFetchEffects()throws Exception{
        prepared();objectError=null;var files=filesystem();
        assertThatThrownBy(()->tx.required(()->NarrowContentBronzeCompletionTestSupport.chain(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,bronzeOperation,BRONZE_PROFILE,VERIFY_PROFILE,b->{}))).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(requests).hasValue(0);assertThat(objectRequests).hasValue(0);assertThat(count("content_native_launch_phases")).isZero();
        assertThat(count("content_fetch_checkpoints")).isZero();assertThat(count("content_bronze_reservations")).isZero();assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void inspectionDoesNotNeedOwnerFilesystemOrCredentials()throws Exception{
        checkpoint();objectError=null;var result=complete(b->{});guard.close();guard=null;Files.move(attemptRoot(),attemptRoot().resolveSibling("removed-attempt"));Files.delete(credentials);
        assertThat(observed()).contains(result);noExecution();
    }
}
