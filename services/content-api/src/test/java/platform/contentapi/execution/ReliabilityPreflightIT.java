package platform.contentapi.execution;

import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.*;
import platform.contentapi.importing.RegisteredImportEvidence;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Gate B: actual PostgreSQL, actual native readers plus controlled process/transaction faults. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReliabilityPreflightIT extends ReliabilityExecutionFixtures {
    @Test void actualNativePreflightAndHistoryHaveNoPublication() {
        var before=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);long artifacts=jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts",Long.class);var verified=verify();assertThat(verified.nativeDependencyFingerprint()).isEqualTo("3b9354d578f017cefea8e9ace2556c6ed474afec606722ee01e9fba8e4a39a7a");assertThat(stage().get("state")).isEqualTo("VERIFIED");assertThat(stage().get("child_pid")).isInstanceOf(Long.class);assertThat(stage().get("command_sha256")).isEqualTo(SourceJson.sha(PreflightFiles.read(descriptor(),65536,true)));assertThat(stage().get("result_sha256")).isEqualTo(SourceJson.sha(PreflightFiles.read(descriptor().resolveSibling("verified.json"),65536,true)));assertThat(state()).isEqualTo("RUNNING");assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(before);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts",Long.class)).isEqualTo(artifacts);occupied();assertThat(workers.inspectCheckInputs(owner,checkpoint).phase()).isEqualTo("HISTORICAL_VERIFIED");
        var catalog=SourceJson.object().put("catalogVersion",1);for(String k:List.of("endpoints","reliabilityProfiles","storageProfiles"))catalog.putArray(k);var detail=new RunRepository(jdbc,tx,INSTALL,PreflightCatalogFixture.from(catalog)).get(run.toString());assertThat(detail.get("currentStage").stringValue()).isEqualTo("CHECK_INPUTS");assertThat(detail.get("result").isNull()).isTrue();assertThat(detail.get("counts").isNull()).isTrue();assertThat(detail.get("attempts").get("count").stringValue()).isEqualTo("1");
    }
    @Test void actualParquetPreflight()throws Exception{stop();jdbc.execute("DROP SCHEMA ddip_management CASCADE");new SchemaLifecycle(Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load(),jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);provision("PARQUET");assertThat(verify().stage().state()).isEqualTo("VERIFIED");occupied();}
    @Test void intentCommitsBeforeAnyLaunchAndChildIdentityIsObserved() {
        var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();assertThat(stage().get("state")).isEqualTo("INTENT");assertThat(stage().get("command_address")).isNull();return super.run(c,e,d,t,new Observer(){public void beforeStart(){observer.beforeStart();assertThat(stage().get("command_address")).isNotNull();assertThat(stage().get("child_pid")).isNull();}public void started(InstallationHostGuard.ProcessIdentity child){observer.started(child);assertThat(stage().get("child_pid")).isEqualTo(child.pid());}public void heartbeat(){observer.heartbeat();}});}};
        service(process).verify(guard,owner,RUNTIME,checkpoint);assertThat(stage().get("state")).isEqualTo("VERIFIED");
    }
    @Test void intentRollbackPreventsLaunchAndAllLifecyclePartialWrites(){jdbc.execute("ALTER TABLE ddip_management.stage_invocations ADD CONSTRAINT injected CHECK(stage<>'CHECK_INPUTS')");assertThatThrownBy(this::verify).hasMessage("STATE_CONFLICT");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts",String.class)).isEqualTo("CLAIMED");assertThat(jdbc.queryForObject("SELECT current_stage FROM ddip_management.runs",String.class)).isNull();assertThat(workers.inspectCheckInputs(owner,checkpoint).phase()).isEqualTo("CONFIRMED_ROLLBACK");occupied();}
    @Test void cancellationBeforeAuthorizationPreventsLaunch(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){cancel();return super.run(c,e,d,t,observer);}};assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("WORKER_NOT_READY");assertThat(stage().get("child_pid")).isNull();assertThat(stage().get("command_address")).isNull();occupied();}
    void cancel(){byte[] b=SourceJson.canonical(SourceJson.object());jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','cancel',?,?,?,'REQUESTED','local-operator')",UUID.randomUUID(),run,b,SourceJson.sha(b),jdbc.queryForObject("SELECT row_version FROM ddip_management.runs",Long.class));}
    @Test void stageVersionChangeFencesLaunch(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){jdbc.update("UPDATE ddip_management.stage_invocations SET row_version=row_version+1");return super.run(c,e,d,t,observer);}};assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("OWNERSHIP_LOST");assertThat(stage().get("child_pid")).isNull();occupied();}
    @ParameterizedTest @ValueSource(strings={"epoch","instance","attempt","run-version","attempt-version","coordinator-version","worker-version"}) void staleOwnerCannotEnterStage(String field){var w=owner.worker();var changed=new WorkerRepository.Session(w.installationId(),field.equals("instance")?UUID.randomUUID():w.instanceId(),w.profileId(),w.profileDigest(),w.deploymentDigest(),w.hostDigest(),w.status(),w.epoch()+(field.equals("epoch")?1:0),w.coordinatorVersion()+(field.equals("coordinator-version")?1:0),w.workerVersion()+(field.equals("worker-version")?1:0));var stale=new WorkerRepository.Owner(changed,run,field.equals("attempt")?UUID.randomUUID():owner.attemptId(),owner.runVersion()+(field.equals("run-version")?1:0),owner.attemptVersion()+(field.equals("attempt-version")?1:0),owner.attemptState());assertThatThrownBy(()->service().verify(guard,stale,RUNTIME,checkpoint)).hasMessage("OWNERSHIP_LOST");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();occupied();}
    JdbcTemplate commitFault(String phase) {
        var once=new AtomicBoolean(true);var data=new AbstractDataSource(){
            public Connection getConnection()throws SQLException {
                var real=jdbc.getDataSource().getConnection();var matching=new AtomicBoolean();
                return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args)->{
                    try {
                        if(method.getName().equals("prepareStatement") && args!=null && args[0] instanceof String sql && switch(phase){case "intent"->sql.startsWith("INSERT INTO ddip_management.stage_invocations");case "authorization"->sql.startsWith("UPDATE ddip_management.stage_invocations SET command_address");default->sql.startsWith("UPDATE ddip_management.stage_invocations SET state=?,result_address");})matching.set(true);
                        Object result=method.invoke(real,args);
                        if(method.getName().equals("commit") && matching.get() && once.compareAndSet(true,false))throw new SQLException("PRIVATE","08006");return result;
                    }catch(InvocationTargetException ex){throw ex.getCause();}
                });
            }
            public Connection getConnection(String u,String p)throws SQLException{return getConnection();}
        };return new JdbcTemplate(data);
    }
    @ParameterizedTest @ValueSource(strings={"intent","authorization","finalization"}) void lostCommitAcknowledgmentNeverDuplicatesLaunch(String phase) {
        var wrapped=commitFault(phase);workers=WorkerTestSupport.enabled(wrapped,transactions(wrapped),INSTALL);
        assertThatThrownBy(this::verify).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();
        var receipt=workers.inspectCheckInputs(owner,checkpoint);assertThat(receipt.phase()).isEqualTo(switch(phase){case "intent"->"INTENT_COMMITTED";case "authorization"->"LAUNCH_UNCERTAIN";default->"HISTORICAL_VERIFIED";});
        if(!phase.equals("finalization"))assertThat(stage().get("child_pid")).isNull();
        assertThatThrownBy(this::verify).hasMessage("OWNERSHIP_LOST");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isEqualTo(1);occupied();
    }
    @Test void finalAuditRollbackCannotTrustPartialStage(){jdbc.execute("ALTER TABLE ddip_management.audit_events ADD CONSTRAINT fail_verified CHECK(action<>'STAGE_VERIFIED')");assertThatThrownBy(this::verify).hasMessage("STATE_CONFLICT");assertThat(stage().get("state")).isEqualTo("RUNNING");assertThat(stage().get("result_address")).isNull();assertThat(stage().get("result_sha256")).isNull();occupied();}
    ReadOnlyVerifierProcess controlled(String mode,Duration timeout) {
        return new ReadOnlyVerifierProcess(128,Duration.ofMillis(500),Duration.ofMillis(500),Duration.ofMillis(500)){
            Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer) {
                return super.run(VerifierProcessTest.command(mode,descriptor().toString()),e,d,timeout,observer);
            }
        };
    }
    @Test void childExitZeroWithoutValidEvidenceIsUnknownAndBlocked(){assertThatThrownBy(()->service(controlled("invalid-result",Duration.ofSeconds(5))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("RESULT_INVALID");assertThat(stage().get("state")).isEqualTo("UNKNOWN");assertThat(state()).isEqualTo("RECOVERY_REQUIRED");occupied();}
    @Test void childExitZeroWithMalformedJsonIsUnknownAndBlocked(){assertThatThrownBy(()->service(controlled("malformed-result",Duration.ofSeconds(5))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("RESULT_INVALID");assertThat(stage().get("state")).isEqualTo("UNKNOWN");occupied();}
    @Test void nonzeroFixedFailureIsNotAVerifiedStage(){assertThatThrownBy(()->service(controlled("native-failed",Duration.ofSeconds(5))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("INPUT_UNAVAILABLE");assertThat(stage().get("state")).isEqualTo("FAILED");assertThat(stage().get("outcome_code")).isEqualTo("INPUT_UNAVAILABLE");occupied();}
    @Test void processStartupFailureClosesOnlyFailedStage(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){var changed=new ArrayList<>(c);changed.set(0,work.resolve("missing").toString());return super.run(changed,e,d,t,observer);}};assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("PROCESS_START_FAILED");assertThat(stage().get("state")).isEqualTo("FAILED");assertThat(stage().get("child_pid")).isNull();occupied();}
    @Test void boundedChildTimeoutCannotVerify(){assertThatThrownBy(()->service(controlled("hang",Duration.ofMillis(250))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("STAGE_TIMEOUT");assertThat(stage().get("state")).isEqualTo("FAILED");occupied();}
    @ParameterizedTest @ValueSource(strings={"stdout","stderr"}) void outputOverflowCannotVerify(String stream){assertThatThrownBy(()->service(controlled(stream,Duration.ofSeconds(5))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("STAGE_OUTPUT_LIMIT");assertThat(stage().get("state")).isEqualTo("FAILED");occupied();}
    @Test void databaseOutageBeforeAuthorizationPreventsChild(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){outage(true);return super.run(c,e,d,t,observer);}};try{assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("MANAGEMENT_UNAVAILABLE");}finally{outage(false);}assertThat(stage().get("child_pid")).isNull();assertThat(stage().get("command_address")).isNull();occupied();}
    @Test void databaseOutageAfterChildIdentityStopsAuthorizationAndCleans() {
        var child=new AtomicReference<InstallationHostGuard.ProcessIdentity>();
        var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){return super.run(c,e,d,t,new Observer(){public void beforeStart(){observer.beforeStart();}public void started(InstallationHostGuard.ProcessIdentity p){child.set(p);observer.started(p);outage(true);observer.heartbeat();}public void heartbeat(){observer.heartbeat();}});}};
        try{assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("MANAGEMENT_UNAVAILABLE");}finally{outage(false);}assertThat(ProcessHandle.of(child.get().pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();assertThat(stage().get("state")).isEqualTo("RUNNING");occupied();
    }
    @Test void databaseOutageAfterNativeVerificationDoesNotRegisterStage(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){var result=super.run(c,e,d,t,observer);outage(true);return result;}};try{assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("MANAGEMENT_UNAVAILABLE");}finally{outage(false);}assertThat(stage().get("state")).isEqualTo("RUNNING");assertThat(stage().get("result_address")).isNull();occupied();}
    @Test void importerCannotAcquireWorkerGuardOrOccupiedAttempt(){assertThatThrownBy(()->InstallationHostGuard.acquire(binding)).hasMessage("HOST_GUARD_OCCUPIED");guard.close();guard=InstallationHostGuard.acquire(binding);String digest=fixture.get("bundle").get("capturedSelection").get("executionProfile").get("configurationSha256").stringValue();assertThatThrownBy(()->new ImportOwnershipRepository(jdbc,tx,INSTALL).reserve(guard,new ImportOwnershipRepository.Reservation(UUID.randomUUID(),PROFILE,digest,OwnershipFixtures.REQUEST))).hasMessage("OWNERSHIP_STALE");occupied();}
    @Test void changedRawEvidenceAfterImportFailsFreshQualification()throws Exception {Path raw=nativeRaw();byte[] bytes=Files.readAllBytes(raw);bytes[0]='!';Files.write(raw,bytes);assertThatThrownBy(this::verify).hasMessage("INPUT_CHANGED");assertThat(stage().get("state")).isEqualTo("FAILED");occupied();}
    Path nativeRaw(){var f=fixture.get("bundle");var input=f.get("capturedSelection").get("inputs").get(0);return Path.of(f.get("bindings").get("batch").get("root").stringValue()).resolve(input.get("relativeAddress").stringValue()).resolve("data/events.jsonl");}
    @Test void laterSourceDisablementDoesNotReplaceAlreadyClaimedFrozenRevision(){byte[] before=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);jdbc.update("UPDATE ddip_management.sources SET enabled=false,row_version=row_version+1");assertThat(verify().stage().state()).isEqualTo("VERIFIED");assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(before);occupied();}
    @Test void invalidRuntimeModuleFailsBeforeIntent()throws Exception{Path module=work.resolve("deployment/service_reference.py");Files.writeString(module,"# changed",StandardOpenOption.APPEND);assertThatThrownBy(this::verify).hasMessage("RUNTIME_MISMATCH");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();occupied();}
    @Test void productionNoneCannotEnterPreflight(){workers=new WorkerRepository(jdbc,tx,INSTALL);assertThat(workers.executionAvailable()).isFalse();assertThatThrownBy(this::verify).hasMessage("WORKER_NOT_READY");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();assertThat(Files.exists(descriptor())).isFalse();occupied();}
    @Test void currentOwnerCanHeartbeatStageButStaleTokenCannotAuthorize(){var frozen=workers.frozenReliability(guard,owner);var initial=workers.beginCheckInputs(guard,owner,checkpoint,"a".repeat(64),SourceJson.sha(SourceJson.canonical(frozen)));long events=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class);var fresh=workers.heartbeatCheckInputs(guard,initial);assertThat(fresh.owner().worker().epoch()).isEqualTo(initial.owner().worker().epoch());assertThat(fresh.owner().worker().coordinatorVersion()).isEqualTo(initial.owner().worker().coordinatorVersion()+1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class)).isEqualTo(events);assertThatThrownBy(()->workers.authorizeCheckInputs(guard,initial)).hasMessage("OWNERSHIP_LOST");occupied();}
    @Test void authorizationCannotBeConsumedTwice(){var frozen=workers.frozenReliability(guard,owner);var initial=workers.beginCheckInputs(guard,owner,checkpoint,"a".repeat(64),SourceJson.sha(SourceJson.canonical(frozen)));var authorized=workers.authorizeCheckInputs(guard,initial);assertThat(authorized.authorized()).isTrue();assertThatThrownBy(()->workers.authorizeCheckInputs(guard,initial)).hasMessage("OWNERSHIP_LOST");assertThat(stage().get("child_pid")).isNull();occupied();}
    @Test void processInterruptionPreservesRecoveryOwnership(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){return super.run(c,e,d,t,new Observer(){public void beforeStart(){observer.beforeStart();}public void started(InstallationHostGuard.ProcessIdentity p){observer.started(p);Thread.currentThread().interrupt();}public void heartbeat(){observer.heartbeat();}});}};try{assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("INTERRUPTED");}finally{Thread.interrupted();}assertThat(stage().get("state")).isEqualTo("UNKNOWN");assertThat(state()).isEqualTo("RECOVERY_REQUIRED");occupied();}
    @Test void observedDescendantUncertaintyRemainsOccupied() {
        Path marker=work.resolve("descendant-started");
        var process=new ReadOnlyVerifierProcess(128,Duration.ofMillis(500),Duration.ofMillis(500),Duration.ofMillis(500)){
            Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){return super.run(VerifierProcessTest.command("descendant",marker.toString()),e,d,Duration.ofSeconds(5),new Observer(){public void beforeStart(){observer.beforeStart();}public void started(InstallationHostGuard.ProcessIdentity p){observer.started(p);long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!Files.exists(marker) && System.nanoTime()<until)java.util.concurrent.locks.LockSupport.parkNanos(1000000);assertThat(Files.exists(marker)).isTrue();}public void heartbeat(){observer.heartbeat();}});}
        };
        assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("TERMINATION_UNCONFIRMED");assertThat(stage().get("state")).isEqualTo("UNKNOWN");assertThat(state()).isEqualTo("RECOVERY_REQUIRED");occupied();
    }
    @Test void productionClosureRollsBackStageAttemptRunAndReleaseIfAuditFails()throws Exception {
        Files.writeString(nativeRaw(),"corrupt");jdbc.execute("ALTER TABLE ddip_management.audit_events ADD CONSTRAINT reject_failure CHECK(action<>'RUN_FAILED')");
        assertThatThrownBy(()->service().verifyForExecution(guard,owner,RUNTIME,checkpoint)).hasMessage("STATE_CONFLICT");assertThat(stage().get("state")).isEqualTo("RUNNING");assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts",String.class)).isEqualTo("RUNNING");occupied();
    }
    @Test void confirmedFailureReturnsCurrentHeartbeatToken() {
        var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){return super.run(VerifierProcessTest.command("native-failed",c.getLast()),e,d,t,new Observer(){public void beforeStart(){observer.beforeStart();}public void started(InstallationHostGuard.ProcessIdentity p){observer.started(p);observer.heartbeat();observer.heartbeat();}public void heartbeat(){observer.heartbeat();}});}};
        var failure=catchThrowable(()->service(process).verifyForExecution(guard,owner,RUNTIME,checkpoint));assertThat(failure).isInstanceOf(ReliabilityPreflight.DefinitiveFailure.class);var released=((ReliabilityPreflight.DefinitiveFailure)failure).worker();assertThat(released.workerVersion()).isEqualTo(2);assertThat(workers.heartbeatIdle(guard,released).workerVersion()).isEqualTo(3);assertThat(state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
    }
    @Test void preIntentRuntimeFailureCanCloseOnlyOriginalClaim()throws Exception {
        Files.writeString(work.resolve("deployment/service_reference.py"),"changed");assertThatThrownBy(()->service().verifyForExecution(guard,owner,RUNTIME,checkpoint)).isInstanceOf(ReliabilityPreflight.DefinitiveFailure.class).hasMessage("RUNTIME_MISMATCH");assertThat(state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
    }
    @Test void failedClosureAcknowledgmentNeverReturnsReleasedAuthority()throws Exception {
        Files.writeString(nativeRaw(),"corrupt");
        // Inject at the exact failure audit; completion may commit, but the old repository is fenced.
        var data=new org.springframework.jdbc.datasource.AbstractDataSource(){public Connection getConnection()throws SQLException{var real=jdbc.getDataSource().getConnection();var match=new AtomicBoolean();return(Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(p,m,a)->{try{if(m.getName().equals("prepareStatement") && a[0] instanceof String sql && sql.contains("UPDATE ddip_management.runs SET state='FAILED'"))match.set(true);var result=m.invoke(real,a);if(m.getName().equals("commit") && match.get())throw new SQLException("PRIVATE","08006");return result;}catch(InvocationTargetException ex){throw ex.getCause();}});}public Connection getConnection(String u,String p)throws SQLException{return getConnection();}};
        var fault=new JdbcTemplate(data);var repository=WorkerTestSupport.enabled(fault,transactions(fault),INSTALL);assertThatThrownBy(()->new ReliabilityPreflight(fault,transactions(fault),repository).verifyForExecution(guard,owner,RUNTIME,checkpoint)).hasMessage("MANAGEMENT_UNAVAILABLE");assertThat(state()).isEqualTo("FAILED");assertThat(stage().get("state")).isEqualTo("FAILED");assertThatThrownBy(()->repository.heartbeatIdle(guard,owner.worker())).hasMessage("OWNERSHIP_LOST");
    }

    @Test void confirmedShutdownBeforeLaunchClosesWithoutInventingAChild() {
        Throwable failure;try{failure=catchThrowable(()->service(new ReadOnlyVerifierProcess(false,()->true)).verifyForExecution(guard,owner,RUNTIME,checkpoint));}finally{Thread.interrupted();}
        assertThat(failure).isInstanceOf(ReliabilityPreflight.DefinitiveFailure.class).hasMessage("PROCESS_START_FAILED");assertThat(stage().get("state")).isEqualTo("FAILED");assertThat(stage().get("child_pid")).isNull();assertThat(state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();assertThat(workers.heartbeatIdle(guard,((ReliabilityPreflight.DefinitiveFailure)failure).worker()).status()).isEqualTo("READY");
    }

}
