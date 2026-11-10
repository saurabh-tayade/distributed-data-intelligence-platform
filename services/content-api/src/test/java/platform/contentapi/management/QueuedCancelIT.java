package platform.contentapi.management;

import java.util.*;
import java.nio.file.*;
import platform.contentapi.execution.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

/** Real PostgreSQL atomicity, replay and local HTTP checks, without public-network or native effects. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class QueuedCancelIT extends RunApiFixtures {
    UUID run; ControlRepository controls;
    @Override void storage(String id,String product,String role){
        if(!role.equals("CONTROL")){super.storage(id,product,role);return;}
        try{var b=OwnershipFixtures.host(Files.createTempDirectory(temp,"cancel-"));var named=new InstallationHostGuard.Binding(id,INSTALL,b.root(),b.rootIdentity(),b.guardIdentity(),b.installationIdentity());jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,named.descriptor(),named.digest(),INSTALL);}catch(Exception ex){throw new IllegalStateException(ex);}
    }
    InstallationHostGuard guard()throws Exception{
        Path descriptor=Files.write(Files.createTempFile(temp,"cancel-binding-",".json"),jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.storage_bindings WHERE binding_id='reliability-CONTROL'",byte[].class));
        return InstallationHostGuard.acquire(OwnershipFixtures.readBinding(descriptor));
    }
    WorkerRepository.Session register(WorkerRepository worker,InstallationHostGuard guard){return worker.register(guard,JSON_PROFILE,jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,JSON_PROFILE),"a".repeat(64));}
    @Test void cancellationWinsBeforeRealClaim()throws Exception{
        try(var guard=guard()){var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var session=register(worker,guard);controls.request(command("cancel",0));assertThat(worker.claim(guard,session,UUID.randomUUID()).status()).isEqualTo("EMPTY");assertThat(count("attempts")).isZero();}
    }
    @Test void realClaimWinsAndFreshRunningCancellationIsRequested()throws Exception{
        try(var guard=guard()){var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var session=register(worker,guard);assertThat(worker.claim(guard,session,UUID.randomUUID()).status()).isEqualTo("CLAIMED");var slot=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");
            assertThatThrownBy(()->controls.request(command("stale",0))).hasMessage("PRECONDITION_FAILED");assertThat(controls.request(command("running",1)).get("state").stringValue()).isEqualTo("REQUESTED");assertThat(count("controls")).isEqualTo(1);assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(slot);}
    }
    @Test void concurrentRealClaimAndCancelHaveOneWinner()throws Exception{
        try(var guard=guard();var pool=Executors.newFixedThreadPool(2)){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var session=register(worker,guard);var start=new CountDownLatch(1);
            var claim=pool.submit(()->{start.await();return worker.claim(guard,session,UUID.randomUUID());});
            var cancel=pool.submit(()->{start.await();try{return controls.request(command("race",0)).get("state").stringValue();}catch(SourceFailure e){return e.getMessage();}});start.countDown();
            var claimed=claim.get(15,TimeUnit.SECONDS);var cancelled=cancel.get(15,TimeUnit.SECONDS);
            if(claimed.status().equals("EMPTY")){assertThat(cancelled).isEqualTo("APPLIED");assertThat(count("attempts")).isZero();assertThat(count("controls")).isEqualTo(1);}
            else{assertThat(claimed.status()).isEqualTo("CLAIMED");assertThat(cancelled).isEqualTo("PRECONDITION_FAILED");assertThat(count("attempts")).isEqualTo(1);assertThat(count("controls")).isZero();}
        }
    }
    @BeforeEach void queued(){run=UUID.fromString(submit(reliability(false),"original").get("runId").stringValue());controls=new ControlRepository(jdbc,tx,INSTALL);}
    JsonNode request(){return SourceJson.object().put("action","CANCEL").putNull("attemptId").putNull("quiescenceConfirmed");}
    ControlCommands.Command command(String key,long version){return ControlCommands.parse(run.toString(),SourceJson.canonical(request()),key,"\""+version+"\"");}
    String path(){return RUNS+"/"+run+"/controls";}
    @Test void atomicTerminalReceiptPreservesFrozenInputAndCoordinator(){
        var slot=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");
        var inputs=jdbc.queryForList("SELECT artifact_id FROM ddip_management.run_inputs WHERE run_id=?",run);
        var receipt=controls.request(command("cancel",0));
        assertThat(receipt.get("state").stringValue()).isEqualTo("APPLIED");assertThat(receipt.get("reason").stringValue()).isEqualTo("CANCELLED");
        assertThat(receipt.get("runVersion").stringValue()).isEqualTo("1");assertThat(receipt.get("rowVersion").stringValue()).isEqualTo("1");
        var row=jdbc.queryForMap("SELECT * FROM ddip_management.runs WHERE run_id=?",run);
        assertThat(row.get("state")).isEqualTo("CANCELLED");assertThat(row.get("finished_at")).isNotNull();assertThat(row.get("retry_eligible")).isEqualTo(false);assertThat(row.get("current_attempt_id")).isNull();assertThat(row.get("result_artifact_id")).isNull();
        assertThat(count("attempts")).isZero();assertThat(count("stage_invocations")).isZero();
        assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(slot);
        assertThat(jdbc.queryForList("SELECT artifact_id FROM ddip_management.run_inputs WHERE run_id=?",run)).isEqualTo(inputs);
        assertThat(jdbc.queryForList("SELECT action FROM ddip_management.audit_events WHERE control_id=? ORDER BY event_seq",String.class,UUID.fromString(receipt.get("controlId").stringValue()))).containsExactly("CONTROL_REQUESTED","RUN_CANCELLED","CONTROL_APPLIED");
        assertThat(controls.request(command("cancel",0))).isEqualTo(receipt);
        assertThat(count("controls")).isEqualTo(1);
        assertThatThrownBy(()->controls.request(command("cancel",1))).hasMessage("IDEMPOTENCY_CONFLICT");
        assertThatThrownBy(()->controls.request(command("different",1))).hasMessage("STATE_CONFLICT");
    }
    @Test void staleVersionAndUnknownRunLeaveNoControl(){assertThatThrownBy(()->controls.request(command("stale",9))).hasMessage("PRECONDITION_FAILED");run=UUID.randomUUID();assertThatThrownBy(()->controls.request(command("missing",0))).hasMessage("NOT_FOUND");assertThat(count("controls")).isZero();}
    @Test void failureMidTransactionRollsBackRunControlAndAudit(){
        jdbc.execute("CREATE FUNCTION ddip_management.fail_cancel() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='RUN_CANCELLED' THEN RAISE EXCEPTION 'INJECTED'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER fail_cancel BEFORE INSERT ON ddip_management.audit_events FOR EACH ROW EXECUTE FUNCTION ddip_management.fail_cancel()");
        long audits=count("audit_events");assertThatThrownBy(()->controls.request(command("atomic",0))).isInstanceOf(RuntimeException.class);
        assertThat(count("controls")).isZero();assertThat(count("audit_events")).isEqualTo(audits);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("QUEUED");
        jdbc.execute("DROP TRIGGER fail_cancel ON ddip_management.audit_events");assertThat(controls.request(command("atomic",0)).get("state").stringValue()).isEqualTo("APPLIED");
    }
    @Test void duplicateConcurrentRequestsHaveOneOutcome()throws Exception{
        try(var pool=Executors.newFixedThreadPool(2)){
            var start=new CountDownLatch(1);Callable<JsonNode> call=()->{start.await();return controls.request(command("same",0));};
            var a=pool.submit(call);var b=pool.submit(call);start.countDown();assertThat(a.get(15,TimeUnit.SECONDS)).isEqualTo(b.get(15,TimeUnit.SECONDS));
        }assertThat(count("controls")).isEqualTo(1);assertThat(count("attempts")).isZero();
    }
    @Test void coordinatorRecoveryModeDoesNotPreventCancellingUnclaimedQueue(){jdbc.update("UPDATE ddip_management.coordinator SET mode='RECOVERY_ONLY',row_version=row_version+1");var slot=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");controls.request(command("cancel",0));assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(slot);}
    @Test void pendingOtherControlBlocksCancellation(){
        var normalized=SourceJson.canonical(SourceJson.object().put("fixture",true));jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'RESUME','other',?,?,0,'REQUESTED','local-operator')",UUID.randomUUID(),run,normalized,SourceJson.sha(normalized));
        assertThatThrownBy(()->controls.request(command("cancel",0))).hasMessage("STATE_CONFLICT");assertThat(count("controls")).isEqualTo(1);
    }
    @Test void disabledSourceDoesNotPreventCancellation(){
        sources.execute(SourceCommands.command(SourceCommands.Action.ENABLE,"application-reliability","demo-requests",SourceJson.object().put("enabled",false),"disable","\"0\""),"test");
        assertThat(controls.request(command("disabled-source",0)).get("state").stringValue()).isEqualTo("APPLIED");
        assertThat(jdbc.queryForObject("SELECT enabled FROM ddip_management.sources WHERE source_id='demo-requests'",Boolean.class)).isFalse();
        assertThat(count("attempts")).isZero();
    }
    @Test void unrelatedOccupiedCoordinatorIsPreserved()throws Exception{
        try(var guard=guard()){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var session=register(worker,guard);
            var owner=worker.claim(guard,session,UUID.randomUUID());assertThat(owner.status()).isEqualTo("CLAIMED");
            UUID occupied=run;run=UUID.fromString(submit(reliability(false),"second").get("runId").stringValue());
            var slot=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");
            var attempts=jdbc.queryForList("SELECT * FROM ddip_management.attempts");
            var activeRun=jdbc.queryForMap("SELECT * FROM ddip_management.runs WHERE run_id=?",occupied);
            assertThat(controls.request(command("other-queued",0)).get("state").stringValue()).isEqualTo("APPLIED");
            assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(slot);
            assertThat(jdbc.queryForList("SELECT * FROM ddip_management.attempts")).isEqualTo(attempts);
            // JDBC materializes fresh byte arrays for frozen request and selection columns.
            assertThat(jdbc.queryForMap("SELECT * FROM ddip_management.runs WHERE run_id=?",occupied)).usingRecursiveComparison().isEqualTo(activeRun);
        }
    }
    @Test void queuedHistoricalAttemptCannotBeCancelled()throws Exception{
        try(var guard=guard()){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var session=register(worker,guard);
            var owner=worker.claim(guard,session,UUID.randomUUID()).owner();
            worker.failBeforeLaunch(guard,owner,WorkerRepository.PrelaunchFailure.PROCESS_START_FAILED);
            // Synthetic historical retry metadata, not authorization for a production retry path.
            jdbc.update("UPDATE ddip_management.runs SET state='QUEUED',current_attempt_id=NULL,current_stage=NULL,finished_at=NULL,outcome_code=NULL,row_version=row_version+1 WHERE run_id=?",run);
            long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
            var before=jdbc.queryForList("SELECT * FROM ddip_management.attempts");long audits=count("audit_events");
            assertThatThrownBy(()->controls.request(command("history",version))).hasMessage("STATE_CONFLICT");
            assertThat(count("controls")).isZero();assertThat(count("audit_events")).isEqualTo(audits);
            assertThat(jdbc.queryForList("SELECT * FROM ddip_management.attempts")).isEqualTo(before);
        }
    }
    void rejectedControls(int count){
        byte[] b=SourceJson.canonical(SourceJson.object());
        for(int i=0;i<count;i++)jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,reason,requester,applied_at) VALUES (?,?,'RETRY',?,?,?,0,'REJECTED','INPUT_INVALID','local-operator',now())",UUID.randomUUID(),run,"fixture-"+i,b,SourceJson.sha(b));
    }
    @Test void lastControlSlotCanCancelAndReplayAtCapacity(){
        rejectedControls(127);var receipt=controls.request(command("last",0));
        assertThat(count("controls")).isEqualTo(128);long audits=count("audit_events");
        assertThat(controls.request(command("last",0))).isEqualTo(receipt);assertThat(count("audit_events")).isEqualTo(audits);
    }
    @Test void exhaustedControlCapacityLeavesNoCancellation(){
        rejectedControls(128);long audits=count("audit_events");
        assertThatThrownBy(()->controls.request(command("full",0))).hasMessage("MANAGEMENT_CAPACITY_EXCEEDED");
        assertThat(count("controls")).isEqualTo(128);assertThat(count("audit_events")).isEqualTo(audits);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("QUEUED");
    }
    void auditBoundary(long auditCount){
        // Reserve 244 other unfinished lifecycles instead of allocating a million audit rows.
        jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state) SELECT gen_random_uuid(),product_id,pipeline,pipeline_version,principal_scope,'capacity-'||n,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,'QUEUED' FROM ddip_management.runs CROSS JOIN generate_series(1,244) n WHERE run_id=?",run);
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,action,details_bytes) SELECT 'local-operator','FIXTURE',? FROM generate_series(1,?)",SourceJson.canonical(SourceJson.object()),auditCount-count("audit_events"));
    }
    @Test void cancellationReleasesOwnAuditReservationAtExactCapacity(){
        auditBoundary(573);assertThat(controls.request(command("boundary",0)).get("state").stringValue()).isEqualTo("APPLIED");
        assertThat(count("audit_events")).isEqualTo(576);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state='QUEUED'",Long.class)).isEqualTo(244);
        long audits=count("audit_events");controls.request(command("boundary",0));assertThat(count("audit_events")).isEqualTo(audits);
    }
    @Test void cancellationCannotConsumeOtherRunsAuditReservations(){
        auditBoundary(574);assertThatThrownBy(()->controls.request(command("over-boundary",0))).hasMessage("MANAGEMENT_CAPACITY_EXCEEDED");
        assertThat(count("controls")).isZero();assertThat(count("audit_events")).isEqualTo(574);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("QUEUED");
    }
    @Test void httpUsesLocalBoundaryConditionalEnvelopeAndInspectableReceipt()throws Exception{
        var rejected=http(post(path()).contentType("application/json").header("Origin",origin).header("X-DDIP-Local-Request","1").header("Idempotency-Key","cancel").content(SourceJson.canonical(request())));code(rejected,428,"PRECONDITION_REQUIRED");
        var result=http(post(path()).contentType("application/json").header("Origin",origin).header("X-DDIP-Local-Request","1").header("Idempotency-Key","cancel").header("If-Match","\"0\"").content(SourceJson.canonical(request())));
        assertThat(result.getResponse().getStatus()).isEqualTo(202);assertThat(result.getResponse().getHeader("ETag")).isEqualTo("\"1\"");assertThat(body(result).get("data").get("state").stringValue()).isEqualTo("APPLIED");
        var get=http(get(result.getResponse().getHeader("Location")));assertThat(get.getResponse().getStatus()).isEqualTo(200);assertThat(body(get).get("data")).isEqualTo(body(result).get("data"));
        var runRead=http(get(RUNS+"/"+run));assertThat(runRead.getResponse().getStatus()).isEqualTo(200);assertThat(body(runRead).get("data").get("state").stringValue()).isEqualTo("CANCELLED");
    }
}
