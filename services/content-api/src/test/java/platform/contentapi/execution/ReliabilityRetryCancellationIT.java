package platform.contentapi.execution;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.*;
import static org.assertj.core.api.Assertions.*;

/** Actual offline retry qualification, cooperative child stop, and independent recovery. */
class ReliabilityRetryCancellationIT extends ReliabilityRetryFixtures {
    List<RecoveryQuiescence.Identity> descendants=List.of();
    @Override List<RecoveryQuiescence.Identity> reviewedDescendants(){return descendants;}
    UUID prepareRetry()throws Exception {
        descendants=List.of();recoverAbsent(true);UUID retry=admit();retryService().requeue(guard,retry,RUNTIME);
        guard.close();guard=InstallationHostGuard.acquire(binding);return retry;
    }
    ControlCommands.Command cancelCommand(String key){long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);return ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","CANCEL").putNull("attemptId").putNull("quiescenceConfirmed")),key,"\""+version+"\"");}
    UUID cancel(){return UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(cancelCommand("cancel-retry")).get("controlId").stringValue());}
    @Test void queuedRetryCancelIsAtomicIdempotentAndDoesNotMutateOutput()throws Exception {
        UUID retry=prepareRetry();var command=cancelCommand("queued-cancel");var before=nativeSnapshot();var repository=new ControlRepository(jdbc,tx,INSTALL);
        var receipt=repository.request(command);assertThat(state()).isEqualTo("CANCELLED");assertThat(repository.request(command)).isEqualTo(receipt);
        assertThat(nativeSnapshot()).isEqualTo(before);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=?",Long.class,run)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT retry_eligible FROM ddip_management.runs WHERE run_id=?",Boolean.class,run)).isFalse();
        assertThatThrownBy(()->retryService().execute(guard,retry,RUNTIME)).isInstanceOf(SourceFailure.class);
    }
    @Test void forgedQueueReceiptCannotAuthorizeCancel()throws Exception {
        prepareRetry();forgeDuplicateReceipt("RELIABILITY_RETRY_QUEUED");
        assertThatThrownBy(this::cancel).isInstanceOf(RuntimeException.class);assertThat(state()).isEqualTo("QUEUED");
    }
    @Test void cancelLostAcknowledgmentReplaysExactlyOnce()throws Exception {
        prepareRetry();var command=cancelCommand("lost-cancel");var armed=new java.util.concurrent.atomic.AtomicBoolean();
        var lost=new org.springframework.jdbc.core.JdbcTemplate(lostAck(armed).getDataSource()){
            @Override public int update(String sql,Object... args){int count=super.update(sql,args);if(sql.startsWith("INSERT INTO ddip_management.audit_events") && Arrays.asList(args).contains("RUN_CANCELLED"))armed.set(true);return count;}
        };
        assertThatThrownBy(()->new ControlRepository(lost,transactions(lost),INSTALL).request(command)).hasMessage("MANAGEMENT_UNAVAILABLE");
        var repo=new ControlRepository(jdbc,tx,INSTALL);assertThat(repo.request(command).get("state").stringValue()).isEqualTo("APPLIED");assertThat(state()).isEqualTo("CANCELLED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE run_id=? AND action='RUN_CANCELLED' AND before_state='QUEUED'",Long.class,run)).isEqualTo(1);
    }
    void forgeDuplicateReceipt(String action){
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) SELECT actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,? FROM ddip_management.audit_events WHERE run_id=? AND action=?",SourceJson.canonical(SourceJson.object()),run,action);
    }
    WorkerRepository worker(){return new WorkerRepository(jdbc,tx,INSTALL,new WorkerExecutionCapability(){public boolean available(){return true;}public boolean supports(String pipeline){return pipeline.equals("RELIABILITY_V2");}});}
    @Test void queuedCancelAndExplicitClaimSerializeWithExactVersion()throws Exception {
        prepareRetry();var worker=worker();var session=worker.register(guard,PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());var command=cancelCommand("race-cancel");var start=new CountDownLatch(1);var pool=Executors.newFixedThreadPool(2);
        try{
            var cancel=pool.submit(()->{start.await();try{return new ControlRepository(jdbc,tx,INSTALL).request(command);}catch(SourceFailure rejected){assertThat(rejected.code).isEqualTo(SourceFailure.Code.PRECONDITION_FAILED);return null;}});
            var claim=pool.submit(()->{start.await();return worker.claimRetry(guard,session,UUID.randomUUID(),run);});start.countDown();var receipt=cancel.get(30,TimeUnit.SECONDS);var claimed=claim.get(30,TimeUnit.SECONDS);
            if(receipt!=null){assertThat(state()).isEqualTo("CANCELLED");assertThat(claimed.owner()).isNull();}else{assertThat(state()).isEqualTo("RUNNING");assertThat(claimed.owner()).isNotNull();cancel();assertThat(state()).isEqualTo("CANCEL_REQUESTED");}
        }finally{pool.shutdownNow();}
    }
    @Test void queuedCancellationAuditFailureRollsBackAndStaleVersionFails()throws Exception {
        prepareRetry();var command=cancelCommand("rollback-cancel");long version=command.expectedVersion();
        jdbc.execute("CREATE FUNCTION ddip_management.reject_cancel() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='RUN_CANCELLED' THEN RAISE EXCEPTION 'test failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$");jdbc.execute("CREATE TRIGGER reject_cancel BEFORE INSERT ON ddip_management.audit_events FOR EACH ROW EXECUTE FUNCTION ddip_management.reject_cancel()");
        assertThatThrownBy(()->new ControlRepository(jdbc,tx,INSTALL).request(command)).isInstanceOf(RuntimeException.class);assertThat(state()).isEqualTo("QUEUED");assertThat(jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run)).isEqualTo(version);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=? AND idempotency_key='rollback-cancel'",Long.class,run)).isZero();
        var stale=ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","CANCEL").putNull("attemptId").putNull("quiescenceConfirmed")),"stale-cancel","\"0\"");
        assertThatThrownBy(()->new ControlRepository(jdbc,tx,INSTALL).request(stale)).isInstanceOf(SourceFailure.class).extracting(e->((SourceFailure)e).code).isEqualTo(SourceFailure.Code.PRECONDITION_FAILED);
    }
    @Test void malformedRetryClaimCannotAuthorizeRunningCancel()throws Exception {
        prepareRetry();var worker=worker();var session=worker.register(guard,PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());var claimed=worker.claimRetry(guard,session,UUID.randomUUID(),run);assertThat(claimed.owner()).isNotNull();
        forgeDuplicateReceipt("RELIABILITY_RETRY_CLAIMED");
        assertThatThrownBy(this::cancel).isInstanceOf(RuntimeException.class);assertThat(state()).isEqualTo("RUNNING");
    }
    @ParameterizedTest @ValueSource(strings={"pre-authorization","live-child","publication-complete"})
    void explicitRetrySettlesCancellationAtNativeBoundary(String point)throws Exception {
        UUID retry=prepareRetry();guard.close();
        String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));Path log=temp.resolve("retry-cancel-child.log");
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",cp,ReliabilityRetryCancellationProcess.class.getName(),url,INSTALL.toString(),binding.id(),PROFILE,retry.toString(),RUNTIME,run.toString(),point,temp.resolve("descendants.json").toString()).redirectErrorStream(true).redirectOutput(log.toFile()).start();
        try{assertThat(child.waitFor(180,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).withFailMessage(()->{try{return java.nio.file.Files.readString(log);}catch(Exception e){return e.toString();}}).isZero();}finally{child.destroyForcibly();}
        var recorded=SourceJson.parse(java.nio.file.Files.readAllBytes(temp.resolve("descendants.json")));descendants=recorded.valueStream().map(n->new RecoveryQuiescence.Identity(Long.parseLong(n.get("pid").stringValue()),java.time.Instant.parse(n.get("startedAt").stringValue()))).toList();
        assertThat(descendants).hasSizeLessThanOrEqualTo(32);
        UUID cancellation=jdbc.queryForObject("SELECT control_id FROM ddip_management.controls WHERE run_id=? AND idempotency_key='cancel-retry'",UUID.class,run);
        if(point.equals("pre-authorization")){
            assertThat(state()).isEqualTo("CANCELLED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
            assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,cancellation)).isEqualTo("APPLIED");return;
        }
        assertThat(state()).isEqualTo("RECOVERY_REQUIRED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNotNull();
        // Reopen exclusion and perform a separate, independently verified recovery command.
        guard.close();guard=InstallationHostGuard.acquire(binding);loadCurrentOwner();handoff();
        var observed=recoveryService().verify(guard,bootstrap,review);assertThat(observed.classification()).isEqualTo(point.equals("publication-complete")?"VERIFIED_PUBLICATION":"OUTPUT_ABSENT");
        var fin=new RecoveryFinalization(jdbc,tx,handoffs);var qualified=fin.qualify(guard,bootstrap,observed,freshReview());var receipt=fin.finish(guard,qualified,freshReview());
        assertThat(receipt.status()).isEqualTo(RecoveryFinalization.Status.CONFIRMED);assertThat(receipt.runState()).isEqualTo(point.equals("publication-complete")?"SUCCEEDED":"CANCELLED");
        assertThat(jdbc.queryForObject("SELECT reason FROM ddip_management.controls WHERE control_id=?",String.class,cancellation)).isEqualTo(point.equals("publication-complete")?"SUCCEEDED":"CANCELLED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,cancellation)).isEqualTo(point.equals("publication-complete")?"TOO_LATE":"APPLIED");
    }
    void loadCurrentOwner(){
        var r=jdbc.queryForMap("SELECT current_attempt_id,row_version FROM ddip_management.runs WHERE run_id=?",run);UUID attempt=(UUID)r.get("current_attempt_id");
        var a=jdbc.queryForMap("SELECT a.instance_id,a.owner_epoch,a.state,a.row_version,w.profile_sha256,w.host_binding_sha256,w.status,w.row_version AS worker_version FROM ddip_management.attempts a JOIN ddip_management.worker_instances w USING(instance_id) WHERE attempt_id=?",attempt);var slot=jdbc.queryForMap("SELECT row_version FROM ddip_management.coordinator");
        var session=new WorkerRepository.Session(INSTALL,(UUID)a.get("instance_id"),PROFILE,(String)a.get("profile_sha256"),owner.worker().deploymentDigest(),(String)a.get("host_binding_sha256"),(String)a.get("status"),((Number)a.get("owner_epoch")).longValue(),((Number)slot.get("row_version")).longValue(),((Number)a.get("worker_version")).longValue());
        owner=new WorkerRepository.Owner(session,run,attempt,((Number)r.get("row_version")).longValue(),((Number)a.get("row_version")).longValue(),(String)a.get("state"));
    }
}
