package platform.contentapi.execution;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Real recovery, fresh offline proof and second execution; no network publication. */
class ReliabilityRetryIT extends RecoveryVerificationFixtures {
    ReliabilityRetry retryService(){return new ReliabilityRetry(jdbc,tx,INSTALL);}
    RecoveryQuiescence.Review freshReview(){clock.now=Instant.now();return new RecoveryQuiescence.Review(review.targetSha256(),clock.instant(),true,true,true,review.reviewedCheckpoints(),review.descendants());}
    UUID recoverAbsent(boolean cancelled)throws Exception {
        deleteTree(publication());
        if(cancelled){var body=SourceJson.canonical(SourceJson.object().put("action","CANCEL").putNull("attemptId").putNull("quiescenceConfirmed"));new ControlRepository(jdbc,tx,INSTALL).request(ControlCommands.parse(run.toString(),body,"cancel-before-retry","\"1\""));}
        handoff();var verifier=recoveryService();var observed=verifier.verify(guard,bootstrap,review);var finalizer=new RecoveryFinalization(jdbc,tx,handoffs);var q=finalizer.qualify(guard,bootstrap,observed,freshReview());var receipt=finalizer.finish(guard,q,freshReview());
        assertThat(receipt.status()).isEqualTo(RecoveryFinalization.Status.CONFIRMED);assertThat(receipt.retryEligible()).isTrue();assertThat(receipt.runState()).isEqualTo(cancelled?"CANCELLED":"FAILED");return control;
    }
    @Override void handoff(){
        clock.now=Instant.now();var body=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
        control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(ControlCommands.parse(run.toString(),SourceJson.canonical(body),"recovery-"+owner.attemptId(),"\""+version+"\"")).get("controlId").stringValue());var registered=new WorkerRepository(jdbc,tx,INSTALL).register(guard,PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());var t=handoffs.inspect(guard,registered,control);review=new RecoveryQuiescence.Review(t.digest(),clock.instant(),true,true,true,t.unresolvedCheckpoints(),List.of());qualification=handoffs.qualify(guard,t,review,RUNTIME);recoveryOwner=handoffs.transfer(guard,qualification).owner();bootstrap=handoffs.bootstrap(guard,qualification.address(),qualification.sha256(),RUNTIME);
    }
    ControlCommands.Command retryCommand(String key){long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);return ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RETRY").putNull("attemptId").putNull("quiescenceConfirmed")),key,"\""+version+"\"");}
    UUID admit(){return UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(retryCommand("retry-one")).get("controlId").stringValue());}
    @ParameterizedTest @ValueSource(booleans={false,true}) void absentRecoveryRequeuesAndExecutesSecondAttempt(boolean cancelled)throws Exception {
        recoverAbsent(cancelled);var request=retryCommand("retry-one");var repository=new ControlRepository(jdbc,tx,INSTALL);UUID retry=UUID.fromString(repository.request(request).get("controlId").stringValue());
        assertThat(repository.request(request).get("controlId").stringValue()).isEqualTo(retry.toString());
        var plan=retryService().requeue(guard,retry,RUNTIME);assertThat(state()).isEqualTo("QUEUED");assertThat(retryService().requeue(guard,retry,RUNTIME)).isEqualTo(plan);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=?",Long.class,run)).isEqualTo(2);
        guard.close();guard=InstallationHostGuard.acquire(binding);
        var ordinary=new WorkerRepository(jdbc,tx,INSTALL,new WorkerExecutionCapability(){public boolean available(){return true;}public boolean supports(String pipeline){return pipeline.equals("RELIABILITY_V2");}});
        var idle=ordinary.register(guard,PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());assertThat(ordinary.claim(guard,idle,UUID.randomUUID()).status()).isEqualTo("EMPTY");
        guard.close();guard=InstallationHostGuard.acquire(binding);UUID artifact=retryService().execute(guard,retry,RUNTIME);assertThat(state()).isEqualTo("SUCCEEDED");assertThat(artifact).isNotNull();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=? AND kind='EXECUTE'",Long.class,run)).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT max(attempt_no) FROM ddip_management.attempts WHERE run_id=?",Integer.class,run)).isEqualTo(3);
        assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run)).isEqualTo(artifact);
        long attempts=jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=?",Long.class,run);var nativeBefore=nativeSnapshot();
        assertThat(retryService().execute(guard,retry,RUNTIME)).isEqualTo(artifact);assertThat(nativeSnapshot()).isEqualTo(nativeBefore);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=?",Long.class,run)).isEqualTo(attempts);
    }
    @Test void staleVersionAndConflictingEnvelopeCannotAdmitAnotherRetry()throws Exception {
        recoverAbsent(false);var command=retryCommand("same-key");var repo=new ControlRepository(jdbc,tx,INSTALL);UUID retry=UUID.fromString(repo.request(command).get("controlId").stringValue());
        var wrong=ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RETRY").putNull("attemptId").putNull("quiescenceConfirmed")),"same-key","\"0\"");
        assertThatThrownBy(()->repo.request(wrong)).isInstanceOf(SourceFailure.class).extracting(e->((SourceFailure)e).code).isEqualTo(SourceFailure.Code.IDEMPOTENCY_CONFLICT);
        var stale=ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RETRY").putNull("attemptId").putNull("quiescenceConfirmed")),"new-key","\"0\"");
        assertThatThrownBy(()->repo.request(stale)).isInstanceOf(SourceFailure.class).extracting(e->((SourceFailure)e).code).isEqualTo(SourceFailure.Code.PRECONDITION_FAILED);
        assertThat(repo.request(command).get("controlId").stringValue()).isEqualTo(retry.toString());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=? AND type='RETRY'",Long.class,run)).isEqualTo(1);
    }
    @Test void failedRequeueAuditRollsBackRunAndControl()throws Exception {
        recoverAbsent(false);UUID retry=admit();long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
        jdbc.execute("CREATE FUNCTION ddip_management.reject_retry() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='RELIABILITY_RETRY_QUEUED' THEN RAISE EXCEPTION 'test failure' USING ERRCODE='23514'; END IF; RETURN NEW; END $$");jdbc.execute("CREATE TRIGGER reject_retry BEFORE INSERT ON ddip_management.audit_events FOR EACH ROW EXECUTE FUNCTION ddip_management.reject_retry()");
        assertThatThrownBy(()->retryService().requeue(guard,retry,RUNTIME)).isInstanceOf(RuntimeException.class);
        assertThat(state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run)).isEqualTo(version);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,retry)).isEqualTo("REQUESTED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RELIABILITY_RETRY_QUEUED'",Long.class)).isZero();
    }
    @Test void lostRequeueAcknowledgmentResolvesFromPersistedReceiptAfterGuardReopen()throws Exception {
        recoverAbsent(false);UUID retry=admit();var armed=new java.util.concurrent.atomic.AtomicBoolean();
        var lost=new org.springframework.jdbc.core.JdbcTemplate(lostAck(armed).getDataSource()){
            @Override public int update(String sql,Object... args){int count=super.update(sql,args);if(sql.startsWith("INSERT INTO ddip_management.audit_events") && Arrays.asList(args).contains("RELIABILITY_RETRY_QUEUED"))armed.set(true);return count;}
        };
        assertThatThrownBy(()->new ReliabilityRetry(lost,transactions(lost),INSTALL).requeue(guard,retry,RUNTIME)).hasMessage("MANAGEMENT_UNAVAILABLE");
        guard.close();guard=InstallationHostGuard.acquire(binding);var plan=retryService().applied(retry);assertThat(plan).isNotNull();assertThat(retryService().requeue(guard,retry,RUNTIME)).isEqualTo(plan);assertThat(state()).isEqualTo("QUEUED");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RELIABILITY_RETRY_QUEUED'",Long.class)).isEqualTo(1);
    }
    @Test void cancelledRunRetryActuallyCrashesAfterWriterAuthorizationAndRecovers()throws Exception {
        recoverAbsent(true);UUID retry=admit();retryService().requeue(guard,retry,RUNTIME);guard.close();
        String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",cp,ReliabilityRetryCrashProcess.class.getName(),url,INSTALL.toString(),binding.id(),PROFILE,retry.toString(),RUNTIME).redirectErrorStream(true).redirectOutput(temp.resolve("retry-crash.log").toFile()).start();
        try{assertThat(child.waitFor(180,java.util.concurrent.TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).withFailMessage(()->{try{return Files.readString(temp.resolve("retry-crash.log"));}catch(Exception e){return e.toString();}}).isEqualTo(23);}finally{child.destroyForcibly();}
        guard=InstallationHostGuard.acquire(binding);
        var r=jdbc.queryForMap("SELECT current_attempt_id,row_version FROM ddip_management.runs WHERE run_id=?",run);UUID attempt=(UUID)r.get("current_attempt_id");
        var a=jdbc.queryForMap("SELECT a.instance_id,a.owner_epoch,a.state,a.row_version,w.profile_sha256,w.host_binding_sha256 FROM ddip_management.attempts a JOIN ddip_management.worker_instances w USING(instance_id) WHERE attempt_id=?",attempt);var slot=jdbc.queryForMap("SELECT row_version FROM ddip_management.coordinator");
        var session=new WorkerRepository.Session(INSTALL,(UUID)a.get("instance_id"),PROFILE,(String)a.get("profile_sha256"),owner.worker().deploymentDigest(),(String)a.get("host_binding_sha256"),"READY",((Number)a.get("owner_epoch")).longValue(),((Number)slot.get("row_version")).longValue(),0);
        owner=new WorkerRepository.Owner(session,run,attempt,((Number)r.get("row_version")).longValue(),((Number)a.get("row_version")).longValue(),(String)a.get("state"));
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='PUBLISH_RELIABILITY' AND state='INTENT' AND command_address IS NOT NULL AND child_pid IS NULL",Long.class,attempt)).isEqualTo(1);
        handoff();var observed=recoveryService().verify(guard,bootstrap,review);assertThat(observed.classification()).isEqualTo("OUTPUT_ABSENT");var fin=new RecoveryFinalization(jdbc,tx,handoffs);var q=fin.qualify(guard,bootstrap,observed,freshReview());var receipt=fin.finish(guard,q,freshReview());
        assertThat(receipt.status()).isEqualTo(RecoveryFinalization.Status.CONFIRMED);assertThat(receipt.runState()).isEqualTo("FAILED");assertThat(receipt.retryEligible()).isTrue();assertThat(fin.resolve(guard,control,q.checkpoint(),qualification.sha256())).isEqualTo(receipt);
        assertThat(jdbc.queryForObject("SELECT max(attempt_no) FROM ddip_management.attempts WHERE run_id=?",Integer.class,run)).isEqualTo(4);
        UUID next=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(retryCommand("retry-two")).get("controlId").stringValue());retryService().requeue(guard,next,RUNTIME);guard.close();guard=InstallationHostGuard.acquire(binding);
        UUID artifact=retryService().execute(guard,next,RUNTIME);assertThat(artifact).isNotNull();assertThat(state()).isEqualTo("SUCCEEDED");
        assertThatThrownBy(()->retryService().execute(guard,retry,RUNTIME)).isInstanceOf(SourceFailure.class);
        assertThat(retryService().execute(guard,next,RUNTIME)).isEqualTo(artifact);

    }
    @Test void registeredIdentityBlocksRetryEvenWithAbsentOutput()throws Exception {
        recoverAbsent(false);UUID retry=admit();byte[] evidence=SourceJson.canonical(SourceJson.object());String outputBinding=jdbc.queryForObject("SELECT binding_id FROM ddip_management.storage_bindings WHERE role='RELIABILITY'",String.class);
        jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at,native_format) VALUES (?,'RELIABILITY_V2','application-reliability',2,2,?,?,?, ?,?,clock_timestamp(),'JSONL')",UUID.randomUUID(),fingerprint(),outputBinding,"registered-identity",evidence,SourceJson.sha(evidence));
        assertThatThrownBy(()->retryService().requeue(guard,retry,RUNTIME)).isInstanceOf(RuntimeException.class);assertThat(state()).isEqualTo("FAILED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,retry)).isEqualTo("REQUESTED");
    }
    @ParameterizedTest @ValueSource(strings={"incomplete-output","changed-input","source-disabled"}) void staleAbsenceCannotAuthorizeRequeue(String mutation)throws Exception {
        recoverAbsent(false);UUID retry=admit();
        if(mutation.equals("incomplete-output")){Files.createDirectories(publication());Files.writeString(publication().resolve("unexpected"),"not a publication");}
        else if(mutation.equals("changed-input")){var input=originalPack.get("bundle").get("capturedSelection").get("inputs").get(0);String role=input.get("role").stringValue().equals("RELIABILITY_BATCH")?"batch":"reference";Path marker=Path.of(fixture.get("bundle").get("bindings").get(role).get("root").stringValue()).resolve(input.get("relativeAddress").stringValue()).resolve("complete.json");Files.writeString(marker,"changed");}
        else jdbc.update("UPDATE ddip_management.sources SET enabled=false,row_version=row_version+1");
        assertThatThrownBy(()->retryService().requeue(guard,retry,RUNTIME)).isInstanceOf(RuntimeException.class);
        assertThat(state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,retry)).isEqualTo("REQUESTED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=?",Long.class,run)).isEqualTo(2);
    }
}
