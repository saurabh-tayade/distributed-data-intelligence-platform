package platform.contentapi.management;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

/** PostgreSQL metadata admission only. Synthetic proof never authorizes native execution. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ContentResumeControlIT extends RunApiFixtures {
    UUID run,original,recovery;ControlRepository controls;
    @Override tools.jackson.databind.node.ObjectNode content(){var n=super.content();n.withArray("retainedSilverArtifactIds").removeAll();return n;}
    @BeforeEach void paused(){seedPaused(content());}
    void seedPaused(tools.jackson.databind.JsonNode requestBody){
        run=UUID.fromString(submit(requestBody,"resume-fixture-"+UUID.randomUUID()).get("runId").stringValue());original=UUID.randomUUID();recovery=UUID.randomUUID();UUID instance=UUID.randomUUID();controls=new ControlRepository(jdbc,tx,INSTALL);
        jdbc.update("INSERT INTO ddip_management.worker_instances(instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,heartbeat_at,status) VALUES (?,?,?,?,?,1,now(),now(),'STOPPED')",instance,INSTALL,CONTENT_PROFILE,jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,CONTENT_PROFILE),SourceFixtures.HASH);
        jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code) VALUES (?,?,1,'EXECUTE',?,1,'INTERRUPTED',now(),'INTERRUPTED')",original,run,instance);
        byte[] request=SourceJson.canonical(SourceJson.object().put("action","RECONCILE"));jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,reason,requester,applied_at,target_attempt_id,quiescence_confirmed) VALUES (?,?,'RECONCILE','prior',?,?,0,'APPLIED','RECONCILED_PAUSED','local-operator',now(),?,true)",recovery,run,request,SourceJson.sha(request),original);
        jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code) VALUES (?,?,2,'RECONCILE',?,2,'SUCCEEDED',now(),'RECONCILED_PAUSED')",recovery,run,instance);
        jdbc.update("UPDATE ddip_management.runs SET state='PAUSED',current_attempt_id=?,outcome_code='RECONCILED_PAUSED',row_version=1 WHERE run_id=?",recovery,run);
        jdbc.update("UPDATE ddip_management.coordinator SET epoch=2,row_version=row_version+1");
        byte[] details=SourceJson.canonical(SourceJson.object().put("policy","E5_CONTENT_RECOVERY_V1").put("runState","PAUSED").put("code","RECOVERED_CHECKPOINT").put("originalAttemptId",original.toString()).put("observationAddress","synthetic/not-authority.json").put("observationSha256",SourceFixtures.HASH).put("qualificationSha256",SourceFixtures.HASH));
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,details_bytes) VALUES ('local-operator',?,?,?,'RECOVERY_FINALIZED',?)",run,recovery,recovery,details);
    }
    ControlCommands.Command command(String key,long v){return ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RESUME").putNull("attemptId").putNull("quiescenceConfirmed")),key,"\""+v+"\"");}
    @Test void admissionIsIntentOnlyAndReplayPrecedesEligibility(){var c=command("once",1);var first=controls.request(c);assertThat(first.get("type").stringValue()).isEqualTo("RESUME");assertThat(first.get("state").stringValue()).isEqualTo("REQUESTED");assertThat(count("attempts")).isEqualTo(2);assertThat(count("stage_invocations")).isZero();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("PAUSED");jdbc.update("UPDATE ddip_management.sources SET enabled=false,row_version=row_version+1 WHERE product_id=?","content-intelligence");assertThat(controls.request(c).get("controlId")).isEqualTo(first.get("controlId"));assertThatThrownBy(()->controls.request(command("once",2))).hasMessage("IDEMPOTENCY_CONFLICT");}
    @Test void staleVersionAndDisabledSource(){assertThatThrownBy(()->controls.request(command("stale",0))).hasMessage("PRECONDITION_FAILED");jdbc.update("UPDATE ddip_management.sources SET enabled=false,row_version=row_version+1 WHERE product_id=?","content-intelligence");assertThatThrownBy(()->controls.request(command("disabled",1))).hasMessage("SOURCE_DISABLED");assertThat(count("controls")).isEqualTo(1);}
    @ParameterizedTest @ValueSource(strings={"QUEUED","FAILED","CANCELLED","RECOVERY_REQUIRED"}) void invalidRunDisposition(String state){jdbc.update("UPDATE ddip_management.runs SET state=?,finished_at=now(),row_version=row_version+1 WHERE run_id=?",state,run);assertThatThrownBy(()->controls.request(command("state",2))).hasMessage(state.equals("RECOVERY_REQUIRED")?"COORDINATOR_RECOVERY_REQUIRED":"STATE_CONFLICT");}
    @Test void unresolvedCoordinatorBlocksEvenACompletedRecovery(){jdbc.update("UPDATE ddip_management.coordinator SET mode='RECOVERY_ONLY',row_version=row_version+1");assertThatThrownBy(()->controls.request(command("owner",1))).hasMessage("COORDINATOR_RECOVERY_REQUIRED");}
    @ParameterizedTest @ValueSource(strings={"FETCH_SOURCE","STORE_BRONZE","SILVER","PUBLISH_CONTENT"}) void partialOrAmbiguousStageCannotResume(String stage){jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256) VALUES (?,?,0,?,'INTENT',?)",original,stage,UUID.randomUUID(),SourceFixtures.HASH);assertThatThrownBy(()->controls.request(command("unsafe",1))).hasMessage("RESUME_CHECKPOINT_UNSUPPORTED");}
    @Test void queueCapacity(){for(int i=0;i<20;i++)submit(reliability(false),"queue-"+i);assertThatThrownBy(()->controls.request(command("capacity",1))).hasMessage("MANAGEMENT_CAPACITY_EXCEEDED");}
    @Test void concurrentSameKeyCreatesOneControlAndAudit()throws Exception{try(var pool=Executors.newFixedThreadPool(2)){var c=command("same",1);var a=pool.submit(()->controls.request(c));var b=pool.submit(()->controls.request(c));assertThat(a.get(10,TimeUnit.SECONDS).get("controlId")).isEqualTo(b.get(10,TimeUnit.SECONDS).get("controlId"));}assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE type='RESUME'",Integer.class)).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events e JOIN ddip_management.controls c USING(control_id) WHERE c.type='RESUME'",Integer.class)).isEqualTo(1);}
    @Test void preFetchWithRetainedSilverNeedsIndependentContinuationSupport(){seedPaused(super.content());assertThatThrownBy(()->controls.request(command("retained",1))).hasMessage("RESUME_CHECKPOINT_UNSUPPORTED");}
    @Test void staleCoordinatorEpochCannotGrantFreshAuthority(){jdbc.update("UPDATE ddip_management.coordinator SET epoch=1,row_version=row_version+1");assertThatThrownBy(()->controls.request(command("epoch",1))).hasMessage("COORDINATOR_RECOVERY_REQUIRED");}
    @Test void reconcileCannotCompeteForPausedResume(){controls.request(command("resume",1));var c=ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RECONCILE").put("attemptId",recovery.toString()).put("quiescenceConfirmed",true)),"reconcile","\"1\"");assertThatThrownBy(()->controls.request(c)).hasMessage("STATE_CONFLICT");}
    @Test void differentPendingControlIsExclusive(){controls.request(command("first",1));assertThatThrownBy(()->controls.request(command("second",1))).hasMessage("STATE_CONFLICT");}
    @Test void auditFailureRollsBackAdmission(){jdbc.execute("CREATE FUNCTION ddip_management.resume_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");jdbc.execute("CREATE TRIGGER resume_fault BEFORE INSERT ON ddip_management.audit_events FOR EACH ROW EXECUTE FUNCTION ddip_management.resume_fault()");assertThatThrownBy(()->controls.request(command("fault",1))).hasMessage("STATE_CONFLICT");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE type='RESUME'",Integer.class)).isZero();}
}
