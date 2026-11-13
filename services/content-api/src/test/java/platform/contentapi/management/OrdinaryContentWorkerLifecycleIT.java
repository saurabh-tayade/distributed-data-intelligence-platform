package platform.contentapi.management;

import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Offline actual dispatcher tests. JDBC fences guarantee no FETCH reservation or source DNS. */
class OrdinaryContentWorkerLifecycleIT extends OrdinaryContentWorkerFixtures {
    @Test void packagedWorkerStartsOnePublicProfileAndStopsIdleWithoutNativeEffects()throws Exception {
        var before=filesystem();Path log=temp.resolve("packaged-content-worker.log");Process child=packagedWorker(log);
        try {
            await(()->{
                try{if(!Files.isRegularFile(log))return false;String output=Files.readString(log);
                    return output.contains("Started ContentApiApplication") && output.contains("WORKER_QUEUE_IDLE");}
                catch(java.io.IOException ex){throw new java.io.UncheckedIOException(ex);}
            },30);
            assertThat(child.isAlive()).as(Files.readString(log)).isTrue();
            assertThat(jdbc.queryForObject("SELECT execution_profile_id FROM ddip_management.worker_instances",String.class)).isEqualTo(CONTENT_PROFILE);
            assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances",String.class)).isEqualTo("READY");
            assertThatThrownBy(()->InstallationHostGuard.acquire(binding)).hasMessage("HOST_GUARD_OCCUPIED");
            assertThat(count("runs")).isZero();assertThat(count("attempts")).isZero();noNativeWork();
            child.destroy();assertThat(child.waitFor(30,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances",String.class))
                    .as("exit=%s; %s",child.exitValue(),Files.readString(log)).isEqualTo("STOPPED");
            assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
            assertThat(filesystem()).isEqualTo(before);
            try(var available=InstallationHostGuard.acquire(binding)){available.checkHeld(INSTALL);}
        } finally {
            if(child.isAlive()){child.destroyForcibly();child.waitFor(10,java.util.concurrent.TimeUnit.SECONDS);}
        }
    }
    UUID preexistingRun;
    @BeforeEach @Override void registerComposition(TestInfo test)throws Exception {
        preexistingRun=null;
        if(test.getTestMethod().orElseThrow().getName().equals("preexistingIncompatibleSelectionCannotEnterNarrowAdmission"))
            preexistingRun=UUID.fromString(submit(content(),"preexisting-retained-silver").get("runId").stringValue());
        super.registerComposition(test);
    }
    final class BoundaryData {
        final AtomicBoolean armed=new AtomicBoolean(),observed=new AtomicBoolean();
        final AtomicInteger nativeAttempts=new AtomicInteger(),polls=new AtomicInteger();
        final AtomicReference<WorkerIdleLifecycle> worker=new AtomicReference<>();
        final String boundary;
        final JdbcTemplate data;
        BoundaryData(String boundary){
            this.boundary=boundary;
            var source=new AbstractDataSource(){
                @Override public Connection getConnection()throws SQLException {
                    Connection real=jdbc.getDataSource().getConnection();
                    return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args)->{
                        try {
                            Object result=method.invoke(real,args);
                            if(method.getName().equals("commit") && armed.compareAndSet(true,false)){
                                observed.set(true);
                                if(boundary.endsWith("_ACK"))throw new SQLException("TEST_LOST_ACK","08006");
                                if(boundary.startsWith("STOP_")){
                                    // Stop on the owner thread, then clear its interrupt to test the durable local veto.
                                    worker.get().stop();Thread.interrupted();
                                }
                            }
                            return result;
                        } catch(InvocationTargetException ex){throw ex.getCause();}
                    });
                }
                @Override public Connection getConnection(String u,String p)throws SQLException{return getConnection();}
            };
            data=new JdbcTemplate(source){
                @Override public List<Map<String,Object>> queryForList(String sql,Object... args){
                    var rows=super.queryForList(sql,args);
                    if(sql.startsWith("SELECT run_id FROM ddip_management.runs WHERE state='QUEUED'"))polls.incrementAndGet();
                    return rows;
                }
                @Override public int update(String sql,Object... args){
                    // This independent tripwire stays in place even if the intended fault never fires.
                    if(sql.startsWith("INSERT INTO ddip_management.content_native_operations") || sql.startsWith("INSERT INTO ddip_management.stage_invocations")){
                        nativeAttempts.incrementAndGet();throw new IllegalStateException("OFFLINE_NATIVE_BOUNDARY");
                    }
                    boolean enrollment=sql.startsWith("INSERT INTO ddip_management.content_attempt_admissions");
                    boolean milestone=sql.startsWith("INSERT INTO ddip_management.content_admission_milestones");
                    String phase=milestone?(String)args[1]:"";
                    if((boundary.equals("CLAIM_FAILURE") && enrollment) || (boundary.equals("PREPARING_FAILURE") && phase.equals("PREPARING"))){
                        observed.set(true);throw new IllegalStateException("TEST_DEFINITIVE_SQL_FAILURE");
                    }
                    int changed=super.update(sql,args);
                    if((boundary.equals("REGISTER_ACK") && sql.startsWith("INSERT INTO ddip_management.worker_instances"))
                            || (boundary.equals("STOP_IDLE_ACK") && sql.startsWith("UPDATE ddip_management.worker_instances SET status='STOPPED'"))
                            || (enrollment && Set.of("CLAIM_ACK","STOP_CLAIM").contains(boundary))
                            || (phase.equals("PREPARING") && Set.of("PREPARING_ACK","STOP_PREPARING").contains(boundary))
                            || (phase.equals("READY") && Set.of("READY_ACK","STOP_READY").contains(boundary)))armed.set(true);
                    return changed;
                }
            };
        }
        WorkerIdleLifecycle start(){var value=lifecycle(data,workerEnvironment());worker.set(value);value.start();return value;}
    }
    void occupied(UUID run){
        UUID attempt=jdbc.queryForObject("SELECT current_attempt_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run);
        assertThat(attempt).isNotNull();assertThat(state(run)).isEqualTo("RUNNING");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,attempt)).isEqualTo("CLAIMED");
        assertThat(jdbc.queryForObject("SELECT content_admission_policy FROM ddip_management.attempts WHERE attempt_id=?",String.class,attempt)).isEqualTo("ddip-content-narrow-v1");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(attempt);
        assertThat(jdbc.queryForObject("SELECT owner_instance_id FROM ddip_management.coordinator",UUID.class)).isNotNull();
        assertThat(count("content_attempt_admissions")).isEqualTo(1);noNativeWork();
    }
    @Test void publicStartupUsesProductionCatalogAndInstalledClosureWithoutSourceRequest()throws Exception {
        var files=filesystem();var boundary=new BoundaryData("NONE");var worker=boundary.start();await(()->boundary.polls.get()>0,10);
        assertThat(worker.condition()).isEqualTo("READY");assertThat(worker.executionAvailable()).isTrue();
        assertThat(count("runs")).isZero();assertThat(count("attempts")).isZero();assertThat(count("content_attempt_admissions")).isZero();
        assertThat(filesystem()).isEqualTo(files);assertThat(boundary.nativeAttempts).hasValue(0);noNativeWork();
        worker.stop();assertThat(worker.condition()).isEqualTo("STOPPED");assertThat(worker.executionAvailable()).isFalse();
        assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances",String.class)).isEqualTo("STOPPED");
        assertThatThrownBy(worker::start).hasMessage("WORKER_NOT_READY");
    }
    @Test void missingCompositionCannotRegisterOrClaim(){
        UUID run=queue("missing");var worker=lifecycle(new BoundaryData("NONE").data,workerEnvironment());
        assertThatThrownBy(worker::start).isInstanceOf(RuntimeException.class);assertThat(count("worker_instances")).isZero();
        assertThat(state(run)).isEqualTo("QUEUED");assertThat(count("attempts")).isZero();noNativeWork();
    }
    @ParameterizedTest @ValueSource(strings={"wrong-runtime","duplicate","provider-missing","provider-changed","ingestion-changed","credentials-missing","module-missing","module-changed","wrong-python","wrong-java-home","wrong-processing-root"})
    void startupMismatchCannotRegisterOrClaim(String mismatch)throws Exception {
        UUID run=queue("startup-mismatch");var env=workerEnvironment();
        switch(mismatch){
            case "wrong-runtime"->env.setProperty("ddip.worker.runtime-profile-id","not-registered");
            case "duplicate"->insertStrict("content_pipeline_profiles","profile_id","duplicate",strictProfiles.get("pipeline"));
            case "provider-missing"->Files.delete(installedProvider);
            case "provider-changed"->Files.write(installedProvider,new byte[]{1},StandardOpenOption.APPEND);
            case "ingestion-changed"->Files.write(installedFetchJar,new byte[]{1},StandardOpenOption.APPEND);
            case "credentials-missing"->Files.delete(credentials);
            case "module-missing"->Files.delete(processing.resolve("strict_native_sink.py"));
            case "module-changed"->Files.writeString(processing.resolve("strict_native_sink.py"),"\n# changed installation\n",StandardOpenOption.APPEND);
            case "wrong-python"->env.setProperty("ddip.worker.python","/not-installed/python");
            case "wrong-java-home"->env.setProperty("ddip.worker.java-home","/not-installed/java");
            case "wrong-processing-root"->env.setProperty("ddip.worker.processing-root","/not-installed/processing");
        }
        var worker=lifecycle(new BoundaryData("NONE").data,env);assertThatThrownBy(worker::start).isInstanceOf(RuntimeException.class);
        assertThat(count("worker_instances")).isZero();assertThat(count("attempts")).isZero();assertThat(state(run)).isEqualTo("QUEUED");noNativeWork();
        try(var available=InstallationHostGuard.acquire(binding)){available.checkHeld(INSTALL);}
    }
    @Test void queueOrderAndExactProfileExcludeReliabilityAndHistoricalContent()throws Exception {
        UUID reliability=UUID.fromString(submit(reliability(false),"earlier-reliability").get("runId").stringValue());
        String legacy="10000000-0000-4000-8000-000000000097";byte[] raw=SourceJson.canonical(RunFixtures.profile(true,"rss-atom-v1"));
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",legacy,raw,SourceJson.sha(raw));
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.execution_profile_storage WHERE profile_id=?",legacy,CONTENT_PROFILE);
        var old=content().put("executionProfileId",legacy);old.withArray("retainedSilverArtifactIds").removeAll();
        UUID historical=UUID.fromString(submit(old,"earlier-historical").get("runId").stringValue());
        String other="10000000-0000-4000-8000-000000000098";
        jdbc.update("INSERT INTO ddip_management.execution_profiles SELECT ?,configuration_bytes,configuration_sha256,created_at FROM ddip_management.execution_profiles WHERE profile_id=?",other,CONTENT_PROFILE);
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.execution_profile_storage WHERE profile_id=?",other,CONTENT_PROFILE);
        var alternate=content().put("executionProfileId",other);alternate.withArray("retainedSilverArtifactIds").removeAll();
        UUID otherRun=UUID.fromString(submit(alternate,"earlier-other-profile").get("runId").stringValue());
        UUID first=queue("first-selected"),second=queue("second-selected");
        var boundary=new BoundaryData("CLAIM_ACK");var worker=boundary.start();await(()->!worker.isRunning(),15);
        assertThat(boundary.observed).isTrue();occupied(first);
        for(UUID untouched:List.of(reliability,historical,otherRun,second))assertThat(state(untouched)).isEqualTo("QUEUED");
        assertThat(count("attempts")).isEqualTo(1);assertThat(boundary.nativeAttempts).hasValue(0);
    }
    @Test void preexistingIncompatibleSelectionCannotEnterNarrowAdmission()throws Exception {
        assertThat(preexistingRun).isNotNull();UUID next=queue("after-incompatible");
        var boundary=new BoundaryData("NONE");var worker=boundary.start();await(()->!worker.isRunning(),15);
        assertThat(worker.condition()).isEqualTo("RECOVERY_REQUIRED");assertThat(state(preexistingRun)).isEqualTo("QUEUED");assertThat(state(next)).isEqualTo("QUEUED");
        assertThat(count("attempts")).isZero();assertThat(count("content_attempt_admissions")).isZero();assertThat(boundary.nativeAttempts).hasValue(0);noNativeWork();
    }
    @Test void resumeQueueMarkerCannotEnterOriginalDispatcher()throws Exception {
        UUID run=queue("historical-resume"),next=queue("after-resume");
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,action,after_state,after_version,safe_code,details_bytes) VALUES ('local-operator',?,'CONTENT_RESUME_QUEUED','QUEUED',0,'QUEUED',?)",run,SourceJson.canonical(SourceJson.object()));
        var boundary=new BoundaryData("NONE");var worker=boundary.start();await(()->!worker.isRunning(),15);
        assertThat(worker.condition()).isEqualTo("RECOVERY_REQUIRED");assertThat(state(run)).isEqualTo("QUEUED");assertThat(state(next)).isEqualTo("QUEUED");
        assertThat(count("attempts")).isZero();assertThat(count("content_attempt_admissions")).isZero();assertThat(boundary.nativeAttempts).hasValue(0);noNativeWork();
    }
    @Test void sourceRevisionAfterSubmissionDoesNotRewriteFrozenPublicSelection()throws Exception {
        UUID run=queue("immutable-source");byte[] frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run);
        var selected=SourceJson.parse(frozen);int references=0;
        for(var input:selected.get("inputs"))if(input.get("role").stringValue().equals("PUBLISHER_REFERENCE")){
            references++;assertThat(input.get("artifactId").stringValue()).isEqualTo(PUB);
            assertThat(input.get("evidenceSha256").stringValue()).isEqualTo(jdbc.queryForObject("SELECT evidence_sha256 FROM ddip_management.artifacts WHERE artifact_id=?::uuid",String.class,PUB));
            assertThat(input.get("qualification")).isEqualTo(SourceJson.parse(jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?::uuid",byte[].class,PUB)));
        }
        assertThat(references).isEqualTo(1);
        sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("Updated source",rss().put("feedEndpointId",ENDPOINT2)),"new-source-revision","\"0\""),"test");
        var boundary=new BoundaryData("CLAIM_ACK");var worker=boundary.start();await(()->!worker.isRunning(),15);occupied(run);
        assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run)).isEqualTo(frozen);
        var source=SourceJson.parse(frozen).get("sources").get(0);assertThat(source.get("revision").intValue()).isEqualTo(1);
        assertThat(source.get("endpoint").get("endpointId").stringValue()).isEqualTo(ENDPOINT);assertThat(source.get("endpoint").get("uri").stringValue()).isEqualTo(sourceUri());
        assertThat(source.get("endpointSha256").stringValue()).isEqualTo(sha(source.get("endpoint")));assertThat(boundary.observed).isTrue();assertThat(boundary.nativeAttempts).hasValue(0);
    }
    @ParameterizedTest @ValueSource(strings={"CLAIM_ACK","PREPARING_ACK","READY_ACK","PREPARING_FAILURE"})
    void claimOrAdmissionFailureRetainsClaimedOccupancyAndNeverUsesReliabilityRelease(String fault)throws Exception {
        UUID run=queue("fault"),next=queue("after-fault");var frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run);
        var beforeFiles=filesystem();var boundary=new BoundaryData(fault);var worker=boundary.start();await(()->!worker.isRunning(),20);
        assertThat(boundary.observed).isTrue();assertThat(boundary.nativeAttempts).hasValue(0);assertThat(worker.condition()).isEqualTo("RECOVERY_REQUIRED");
        occupied(run);assertThat(state(next)).isEqualTo("QUEUED");assertThat(count("attempts")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run)).isEqualTo(frozen);
        assertThat(phases()).containsExactlyElementsOf(fault.equals("READY_ACK")?List.of("PREPARING","READY"):fault.equals("PREPARING_ACK")?List.of("PREPARING"):List.of());
        if(!fault.equals("READY_ACK"))assertThat(filesystem()).isEqualTo(beforeFiles);
        var fresh=lifecycle(new BoundaryData("NONE").data,workerEnvironment());fresh.start();assertThat(fresh.condition()).isEqualTo("RECOVERY_ONLY");assertThat(fresh.isRunning()).isFalse();
        assertThat(count("attempts")).isEqualTo(1);occupied(run);
    }
    @Test void failedAtomicEnrollmentRollsBackClaimAndDispatcherNeverRetries()throws Exception {
        UUID run=queue("rollback"),next=queue("after-rollback");var beforeFiles=filesystem();var boundary=new BoundaryData("CLAIM_FAILURE");var worker=boundary.start();await(()->!worker.isRunning(),15);
        assertThat(boundary.observed).isTrue();assertThat(worker.condition()).isEqualTo("RECOVERY_REQUIRED");
        assertThat(count("attempts")).isZero();assertThat(count("content_attempt_admissions")).isZero();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(state(run)).isEqualTo("QUEUED");assertThat(state(next)).isEqualTo("QUEUED");assertThat(filesystem()).isEqualTo(beforeFiles);
        worker.tick();assertThat(boundary.polls).hasValue(1);assertThat(boundary.nativeAttempts).hasValue(0);noNativeWork();
    }
    @ParameterizedTest @ValueSource(strings={"STOP_CLAIM","STOP_PREPARING","STOP_READY"})
    void persistentStopAfterAcknowledgmentPreventsNextEffectDespiteClearedInterrupt(String point)throws Exception {
        UUID run=queue("stopping"),next=queue("waiting");var boundary=new BoundaryData(point);var worker=boundary.start();await(()->!worker.isRunning(),20);
        assertThat(boundary.observed).isTrue();assertThat(boundary.nativeAttempts).hasValue(0);assertThat(worker.condition()).isEqualTo("RECOVERY_REQUIRED");
        occupied(run);assertThat(state(next)).isEqualTo("QUEUED");
        assertThat(phases()).containsExactlyElementsOf(point.equals("STOP_READY")?List.of("PREPARING","READY"):point.equals("STOP_PREPARING")?List.of("PREPARING"):List.of());
        assertThatThrownBy(worker::start).hasMessage("WORKER_NOT_READY");
    }
    @ParameterizedTest @ValueSource(strings={"REGISTER_ACK","STOP_IDLE_ACK"})
    void lostStartupAcknowledgmentCannotResetRepositoryByRepeatedStart(String fault){
        UUID run=queue("startup-uncertain");
        if(fault.equals("STOP_IDLE_ACK"))jdbc.update("UPDATE ddip_management.coordinator SET mode='RECOVERY_ONLY',row_version=row_version+1");
        var boundary=new BoundaryData(fault);var worker=lifecycle(boundary.data,workerEnvironment());boundary.worker.set(worker);
        assertThatThrownBy(worker::start).isInstanceOf(RuntimeException.class);assertThat(boundary.observed).isTrue();
        assertThat(worker.isRunning()).isFalse();assertThat(worker.condition()).isEqualTo("RECOVERY_REQUIRED");assertThat(worker.executionAvailable()).isFalse();assertThat(count("worker_instances")).isEqualTo(1);
        var rows=jdbc.queryForList("SELECT * FROM ddip_management.worker_instances");
        var slot=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");
        assertThatThrownBy(worker::start).hasMessage("WORKER_NOT_READY");worker.tick();
        assertThat(jdbc.queryForList("SELECT * FROM ddip_management.worker_instances")).isEqualTo(rows);
        assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(slot);
        assertThat(state(run)).isEqualTo("QUEUED");assertThat(count("attempts")).isZero();assertThat(boundary.nativeAttempts).hasValue(0);noNativeWork();
    }
    @Test void stopBeforeStartNeverAcquiresGuardOrRegisters(){
        var worker=lifecycle(new BoundaryData("NONE").data,workerEnvironment());worker.stop();assertThatThrownBy(worker::start).hasMessage("WORKER_NOT_READY");
        assertThat(count("worker_instances")).isZero();assertThat(count("attempts")).isZero();noNativeWork();
        try(var available=InstallationHostGuard.acquire(binding)){available.checkHeld(INSTALL);}
    }
    @ParameterizedTest @ValueSource(strings={"STOPPING","RECOVERY_ONLY"})
    void blockedCoordinatorNeverClaimsOrAdopts(String mode){
        UUID run=queue("blocked");jdbc.update("UPDATE ddip_management.coordinator SET mode=?,row_version=row_version+1",mode);
        var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");var worker=lifecycle(new BoundaryData("NONE").data,workerEnvironment());worker.start();
        assertThat(worker.condition()).isEqualTo(mode);assertThat(worker.isRunning()).isFalse();assertThat(state(run)).isEqualTo("QUEUED");
        assertThat(count("attempts")).isZero();assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);noNativeWork();
    }
    @Test void heldGlobalGuardExcludesAnotherWorkerAndImporter()throws Exception {
        var firstData=new BoundaryData("NONE");var first=firstData.start();await(()->firstData.polls.get()>0,10);
        var second=lifecycle(new BoundaryData("NONE").data,workerEnvironment());assertThatThrownBy(second::start).hasMessage("HOST_GUARD_OCCUPIED");
        assertThatThrownBy(()->InstallationHostGuard.acquire(binding)).hasMessage("HOST_GUARD_OCCUPIED");
        assertThat(first.condition()).isEqualTo("READY");assertThat(count("worker_instances")).isEqualTo(1);assertThat(count("artifact_import_operations")).isZero();noNativeWork();
    }
    @Test void activeImportExcludesOrdinaryWorkerEvenAfterCooperativeGuardRelease(){
        UUID run=queue("after-import"),importId=UUID.randomUUID();
        try(var importer=InstallationHostGuard.acquire(binding)){
            new ImportOwnershipRepository(jdbc,tx,INSTALL).reserve(importer,new ImportOwnershipRepository.Reservation(importId,CONTENT_PROFILE,digest(CONTENT_PROFILE),SourceJson.canonical(SourceJson.object())));
        }
        var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");var worker=lifecycle(new BoundaryData("NONE").data,workerEnvironment());worker.start();
        assertThat(worker.condition()).isEqualTo("RECOVERY_ONLY");assertThat(worker.isRunning()).isFalse();assertThat(state(run)).isEqualTo("QUEUED");
        assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);assertThat(count("attempts")).isZero();noNativeWork();
    }
    @Test void reliabilityOwnedCoordinatorExcludesOrdinaryContentWithoutAdoption(){
        String shared="10000000-0000-4000-8000-000000000096";
        jdbc.update("INSERT INTO ddip_management.execution_profiles SELECT ?,configuration_bytes,configuration_sha256,created_at FROM ddip_management.execution_profiles WHERE profile_id=?",shared,JSON_PROFILE);
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,s.binding_id FROM ddip_management.execution_profile_storage s JOIN ddip_management.storage_bindings b USING(binding_id) WHERE s.profile_id=? AND b.role<>'CONTROL'",shared,JSON_PROFILE);
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage VALUES (?,?)",shared,binding.id());
        UUID reliability=UUID.fromString(submit(reliability(false).put("executionProfileId",shared),"reliability-owner").get("runId").stringValue()),content=queue("content-waits");
        WorkerRepository.Owner existing;
        try(var reliabilityGuard=InstallationHostGuard.acquire(binding)){
            var repository=WorkerTestSupport.reliability(jdbc,tx,INSTALL);
            var registered=repository.register(reliabilityGuard,shared,digest(shared),"a".repeat(64));
            existing=repository.claim(reliabilityGuard,registered,UUID.randomUUID()).owner();assertThat(existing).isNotNull();
        }
        var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");var worker=lifecycle(new BoundaryData("NONE").data,workerEnvironment());worker.start();
        assertThat(worker.condition()).isEqualTo("RECOVERY_ONLY");assertThat(worker.isRunning()).isFalse();assertThat(state(content)).isEqualTo("QUEUED");
        assertThat(state(reliability)).isEqualTo("RUNNING");assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);assertThat(count("attempts")).isEqualTo(1);noNativeWork();
    }
}
