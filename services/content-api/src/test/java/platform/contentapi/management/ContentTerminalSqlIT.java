package platform.contentapi.management;

import java.time.Instant;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Disposable PostgreSQL contract tests only. Metadata-only READY fixture is not native qualification. */
class ContentTerminalSqlIT extends NarrowContentAdmissionFixtures {
    UUID control,terminal,receipt; WorkerRepository.Session operator; JsonNode snapshot;
    static final String POLICY="ddip-content-original-safe-end-v1";
    void pending(){
        claimed();prepare();terminal=UUID.randomUUID();receipt=UUID.randomUUID();
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,owner.runId());
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);
        control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL,p->{}).request(ControlCommands.parse(owner.runId().toString(),SourceJson.canonical(request),"terminal-sql","\""+version+"\"")).get("controlId").stringValue());
        guard.close();guard=InstallationHostGuard.acquire(binding);
        operator=new WorkerRepository(jdbc,tx,INSTALL).register(guard,CONTENT_PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());
        snapshot=SourceJson.parse(jdbc.queryForObject("SELECT ddip_management.content_terminal_snapshot(?,?,?,?)::text",String.class,owner.runId(),owner.attemptId(),control,operator.instanceId()).getBytes(java.nio.charset.StandardCharsets.UTF_8));
    }
    long number(String row,String field){return snapshot.get(row).get(field).longValue();}
    void finish(String failure){tx.required(()->{
        jdbc.queryForObject("SELECT singleton_id FROM ddip_management.coordinator FOR UPDATE",Integer.class);
        byte[] raw=wire(snapshot,failure.equals("snapshot-spacing"));String sha=SourceJson.sha(raw);
        var target=SourceJson.object().put("identityVersion",1).put("policy",POLICY).put("terminalId",receipt.toString()).put("runId",owner.runId().toString()).put("originalAttemptId",owner.attemptId().toString()).put("terminalAttemptId",terminal.toString()).put("controlId",control.toString()).put("snapshotSha256",sha).put("inventorySha256","a".repeat(64)).put("inspectionNonce",UUID.randomUUID().toString());
        if(failure.equals("review-nullnonce"))target.putNull("inspectionNonce");
        byte[] targetRaw=wire(target,failure.equals("target-spacing"));String targetSha=SourceJson.sha(targetRaw);
        var review=SourceJson.object().put("reviewVersion",1).put("nonce",target.get("inspectionNonce").stringValue()).put("targetSha256",targetSha).put("inspectedAt",Instant.now().toString());
        for(String k:List.of("handlersStopped","unmanagedWritersStopped","orphanReviewComplete","sameHostAndProcessNamespace","descendantsReviewed","launchGapsReviewed","sparkGatewaysReviewed","remoteWritersStopped"))review.put(k,true);
        review.putArray("reviewedCheckpoints");review.putArray("descendants");
        switch(failure){
            case "review-extra"->review.put("extra",true);
            case "review-checkpoint"->review.withArray("reviewedCheckpoints").add(UUID.randomUUID().toString());
            case "review-pid"->review.withArray("descendants").addObject().put("pid","0").put("startedAt",Instant.now().toString());
            case "review-duplicate"->{var d=SourceJson.object().put("pid","123").put("startedAt","2026-01-01T00:00:00Z");review.withArray("descendants").add(d).add(d);}
            case "review-stale"->review.put("inspectedAt",Instant.now().minusSeconds(61).toString());
            case "review-future"->review.put("inspectedAt",Instant.now().plusSeconds(61).toString());
            case "review-infinity"->review.put("inspectedAt","infinity");
            case "review-nonce"->review.put("nonce",UUID.randomUUID().toString());
        }
        byte[] reviewRaw=wire(review,failure.equals("review-spacing"));
        if(!failure.equals("receipt"))jdbc.update("INSERT INTO ddip_management.content_original_terminal_receipts(terminal_id,identity_version,policy,run_id,control_id,original_attempt_id,terminal_attempt_id,original_instance_id,terminal_instance_id,original_epoch,original_coordinator_version,original_run_version,original_attempt_version,original_worker_version,terminal_worker_version,control_version,target_bytes,target_sha256,review_bytes,review_sha256,snapshot_bytes,snapshot_sha256) VALUES (?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",receipt,POLICY,owner.runId(),control,owner.attemptId(),terminal,owner.worker().instanceId(),operator.instanceId(),number("coordinator","epoch"),number("coordinator","row_version"),number("run","row_version"),number("attempt","row_version"),number("oldWorker","row_version"),number("terminalWorker","row_version"),number("control","row_version"),targetRaw,targetSha,reviewRaw,SourceJson.sha(reviewRaw),raw,sha);
        if(!failure.equals("prior-original"))jdbc.update("UPDATE ddip_management.attempts SET state='INTERRUPTED',outcome_code='INTERRUPTED',finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
        jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,started_at,finished_at,outcome_code,content_terminal_policy) VALUES (?,?,2,'RECONCILE',?,?,'SUCCEEDED',clock_timestamp(),clock_timestamp(),'RECONCILED_FAILED',?)",terminal,owner.runId(),operator.instanceId(),number("coordinator","epoch")+1,POLICY);
        jdbc.update("UPDATE ddip_management.runs SET state='FAILED',outcome_code='NATIVE_CONTENT_INTERRUPTED',current_attempt_id=?,current_stage=NULL,result_artifact_id=NULL,counts_bytes=NULL,retry_eligible=false,finished_at=clock_timestamp(),row_version=row_version+1 WHERE run_id=?",terminal,owner.runId());
        if(!failure.equals("prior-control"))jdbc.update("UPDATE ddip_management.controls SET state='APPLIED',reason='NATIVE_CONTENT_INTERRUPTED',applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=?",control);
        if(!failure.equals("prior-worker"))jdbc.update("UPDATE ddip_management.worker_instances SET status='STOPPED',row_version=row_version+1 WHERE instance_id=?",operator.instanceId());
        jdbc.update("UPDATE ddip_management.coordinator SET epoch=epoch+1,row_version=row_version+1,active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL");
        var details=target.deepCopy();details.put("targetSha256",targetSha);byte[] detail=wire(details,failure.equals("audit-spacing"));
        String[] actions={"ORIGINAL","ATTEMPT","RUN","CONTROL","WORKER","RELEASE"};String[] before={"CLAIMED",null,"RUNNING","REQUESTED","RECOVERY_ONLY","READY"};String[] after={"INTERRUPTED","SUCCEEDED","FAILED","APPLIED","STOPPED","READY"};
        Long[] versions={number("attempt","row_version"),null,number("run","row_version"),number("control","row_version"),number("terminalWorker","row_version"),number("coordinator","row_version")};
        for(int i=0;i<6;i++){if(failure.equals("audit") && i==0)continue;
            jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,'NATIVE_CONTENT_INTERRUPTED',?)",owner.runId(),terminal,control,"NATIVE_CONTENT_TERMINAL_"+actions[i],before[i],after[i],versions[i],versions[i]==null?0:versions[i]+1,detail);
        }

        return null;
    });}
    @Test void exactAtomicDispositionHasSixAuditsAndNoHandoff(){pending();finish("");assertThat(ContentTerminalSqlTestSupport.assertFrozenBindings(jdbc,receipt)).isZero();assertThat(jdbc.queryForObject("SELECT ddip_management.content_terminal_receipt_valid(?)",Boolean.class,receipt)).isTrue();assertThat(count("content_original_terminal_receipts")).isEqualTo(1);assertThat(count("content_fetch_recovery_handoffs")).isZero();assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();noExecution();}
    @ParameterizedTest @ValueSource(strings={"receipt","audit","review-extra","review-checkpoint","review-pid","review-duplicate","review-stale","review-future","review-infinity","review-nonce","review-nullnonce","snapshot-spacing","target-spacing","review-spacing","audit-spacing"}) void incompleteDispositionRollsBack(String failure){pending();var before=metadata();assertThatThrownBy(()->finish(failure)).isInstanceOf(RuntimeException.class);assertThat(metadata()).isEqualTo(before);assertThat(count("content_original_terminal_receipts")).isZero();}
    @ParameterizedTest @ValueSource(strings={"retry","worker","owner","delete","truncate","control","attempt","dependency"}) void permanentClosureRejectsLaterWriters(String change){pending();finish("");assertThatThrownBy(()->tx.required(()->{
        switch(change){
            case "retry"->jdbc.update("UPDATE ddip_management.runs SET retry_eligible=true,row_version=row_version+1 WHERE run_id=?",owner.runId());
            case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET status='READY',row_version=row_version+1 WHERE instance_id=?",operator.instanceId());
            case "owner"->jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=?,owner_instance_id=?,row_version=row_version+1",owner.attemptId(),owner.worker().instanceId());
            case "delete"->jdbc.execute("DELETE FROM ddip_management.content_original_terminal_receipts");
            case "truncate"->jdbc.execute("TRUNCATE ddip_management.content_original_terminal_receipts CASCADE");
            case "control"->jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','late',?, ?,0,'REQUESTED','local-operator')",UUID.randomUUID(),owner.runId(),new byte[]{123,125},SourceJson.sha(new byte[]{123,125}));
            case "attempt"->jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state) VALUES (?,?,3,'EXECUTE',?,2,'CLAIMED')",UUID.randomUUID(),owner.runId(),operator.instanceId());
            case "dependency"->jdbc.update("INSERT INTO ddip_management.artifact_dependencies(artifact_id,role,ordinal,dependency_artifact_id) SELECT artifact_id,'EXTRA',0,? FROM ddip_management.run_inputs WHERE run_id=?",UUID.fromString(SILVER),owner.runId());
        }return null;})).isInstanceOf(RuntimeException.class);assertThat(jdbc.queryForObject("SELECT ddip_management.content_terminal_receipt_valid(?)",Boolean.class,receipt)).isTrue();}
    @Test void laterQueueOwnershipAndIncomingDependenciesRemainAvailable(){pending();finish("");UUID next=queue("after-safe-end");guard.close();ready();var claimed=NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),next).owner();assertThat(claimed).isNotNull();jdbc.update("INSERT INTO ddip_management.artifact_dependencies(artifact_id,role,ordinal,dependency_artifact_id) SELECT ?, 'REFERENCE',0,artifact_id FROM ddip_management.run_inputs WHERE run_id=?",UUID.fromString(SILVER),owner.runId());assertThat(jdbc.queryForObject("SELECT ddip_management.content_terminal_receipt_valid(?)",Boolean.class,receipt)).isTrue();}
    @Test void staleSnapshotWriterCannotMissTerminalEnrollment() throws Exception {
        pending();
        try(var connection=jdbc.getDataSource().getConnection()) {
            connection.setAutoCommit(false);connection.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);
            try(var query=connection.createStatement();var rows=query.executeQuery("SELECT row_version FROM ddip_management.coordinator")){assertThat(rows.next()).isTrue();}
            finish("");
            try(var write=connection.prepareStatement("INSERT INTO ddip_management.artifact_dependencies(artifact_id,role,ordinal,dependency_artifact_id) SELECT artifact_id,'EXTRA',0,? FROM ddip_management.run_inputs WHERE run_id=?")) {
                write.setObject(1,UUID.fromString(SILVER));write.setObject(2,owner.runId());assertThatThrownBy(write::executeUpdate).isInstanceOf(java.sql.SQLException.class);
            } finally {connection.rollback();}
        }
        assertThat(jdbc.queryForObject("SELECT ddip_management.content_terminal_receipt_valid(?)",Boolean.class,receipt)).isTrue();
    }
    @Test void originalReleaseWithoutDispositionRollsBack(){pending();var before=metadata();assertThatThrownBy(()->jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,row_version=row_version+1")).isInstanceOf(RuntimeException.class);assertThat(metadata()).isEqualTo(before);}
    @Test void callerCannotInventAbsentArtifactBinding(){pending();((tools.jackson.databind.node.ArrayNode)snapshot.get("artifactBindings")).addObject().put("kind","CONTENT").put("bindingId","invented").put("relativeAddress","v1/not-derived").putNull("artifact").putArray("dependencies");var before=metadata();assertThatThrownBy(()->finish("")).isInstanceOf(RuntimeException.class);assertThat(metadata()).isEqualTo(before);}

    @ParameterizedTest @ValueSource(strings={"prior-original","prior-control","prior-worker"})
    void olderTransitionsCannotMasqueradeAsOneAtomicDisposition(String omission){pending();
        switch(omission){
            case "prior-original"->jdbc.update("UPDATE ddip_management.attempts SET state='INTERRUPTED',outcome_code='INTERRUPTED',finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
            case "prior-control"->jdbc.update("UPDATE ddip_management.controls SET state='APPLIED',reason='NATIVE_CONTENT_INTERRUPTED',applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=?",control);
            case "prior-worker"->jdbc.update("UPDATE ddip_management.worker_instances SET status='STOPPED',row_version=row_version+1 WHERE instance_id=?",operator.instanceId());
        }
        var before=metadata();assertThatThrownBy(()->finish(omission)).isInstanceOf(RuntimeException.class);assertThat(metadata()).isEqualTo(before);assertThat(count("content_original_terminal_receipts")).isZero();
    }

    static byte[] wire(JsonNode node,boolean noncanonical){byte[] raw=SourceJson.canonical(node);return noncanonical?(" "+new String(raw,java.nio.charset.StandardCharsets.UTF_8)).getBytes(java.nio.charset.StandardCharsets.UTF_8):raw;}
    @Test void sqlCanonicalEncodingMatchesJavaUtf8AndEscaping(){
        var n=SourceJson.object().put("é","雪😀").put("a","\b\f\n\r\t\\\""+((char)1)).put("𐀀",Long.MAX_VALUE).put("",Long.MIN_VALUE).put("false",false).putNull("null");n.putArray("array").add(1).add("z");
        String encoded=new String(SourceJson.canonical(n),java.nio.charset.StandardCharsets.UTF_8);
        assertThat(jdbc.queryForObject("SELECT ddip_management.content_terminal_canonical(?::jsonb)",String.class,encoded)).isEqualTo(encoded);
        assertThatThrownBy(()->jdbc.queryForObject("SELECT ddip_management.content_terminal_canonical('1.5'::jsonb)",String.class)).isInstanceOf(RuntimeException.class);
    }

    @ParameterizedTest @ValueSource(strings={"prior-original","prior-worker","prior-control"})
    void flushingOldEventsBeforeReceiptCannotForgeStartingState(String omission){pending();var before=metadata();
        assertThatThrownBy(()->tx.required(()->{
            jdbc.execute("SET CONSTRAINTS ddip_management.content_terminal_transition IMMEDIATE");
            switch(omission){
                case "prior-original"->jdbc.update("UPDATE ddip_management.attempts SET state='INTERRUPTED',outcome_code='INTERRUPTED',finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
                case "prior-worker"->jdbc.update("UPDATE ddip_management.worker_instances SET status='STOPPED',row_version=row_version+1 WHERE instance_id=?",operator.instanceId());
                case "prior-control"->jdbc.update("UPDATE ddip_management.controls SET state='APPLIED',reason='NATIVE_CONTENT_INTERRUPTED',applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=?",control);
            }
            jdbc.execute("SET CONSTRAINTS ddip_management.content_terminal_transition DEFERRED");finish(omission);return null;
        })).isInstanceOf(RuntimeException.class);assertThat(metadata()).isEqualTo(before);assertThat(count("content_original_terminal_receipts")).isZero();
    }

}
