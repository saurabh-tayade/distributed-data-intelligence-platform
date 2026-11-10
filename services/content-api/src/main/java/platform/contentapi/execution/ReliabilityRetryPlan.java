package platform.contentapi.execution;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.ContentResumePlan.require;
import static platform.contentapi.management.SourceFailure.Code.*;

/** Metadata eligibility is historical only; fresh guarded native evidence is mandatory. */
public final class ReliabilityRetryPlan {
    static final String POLICY="E5_RELIABILITY_RETRY_V1";
    private ReliabilityRetryPlan(){}
    public static JsonNode metadata(JdbcTemplate j, Map<String,Object> r) {
        require("RELIABILITY_V2".equals(r.get("pipeline")) && Set.of("FAILED","CANCELLED").contains(r.get("state")) && Boolean.TRUE.equals(r.get("retry_eligible")) && r.get("result_artifact_id")==null && r.get("current_attempt_id")!=null,STATE_CONFLICT);
        UUID run=(UUID)r.get("run_id"), recovery=(UUID)r.get("current_attempt_id");
        var rows=j.queryForList("SELECT e.details_bytes,e.after_version FROM ddip_management.audit_events e JOIN ddip_management.attempts a ON a.attempt_id=e.attempt_id JOIN ddip_management.controls k ON k.control_id=e.control_id WHERE e.run_id=? AND e.attempt_id=? AND e.control_id=? AND e.action='RECOVERY_FINALIZED' AND a.kind='RECONCILE' AND a.state='SUCCEEDED' AND a.finished_at IS NOT NULL AND k.type='RECONCILE' AND k.state='APPLIED'",run,recovery,recovery);
        require(rows.size()==1,STATE_CONFLICT);var row=rows.getFirst();var d=SourceJson.parse((byte[])row.get("details_bytes"));
        require(d.get("policy").stringValue().equals(RecoveryFinalization.POLICY) && d.get("runId").stringValue().equals(run.toString()) && d.get("controlId").stringValue().equals(recovery.toString()) && d.get("artifactId").isNull() && Long.parseLong(d.get("runVersion").stringValue())+1==((Number)r.get("row_version")).longValue() && d.get("classification").stringValue().equals("OUTPUT_ABSENT") && d.get("retryEligible").booleanValue() && d.get("runState").stringValue().equals(r.get("state")) && d.get("code").stringValue().equals(r.get("outcome_code")) && Objects.equals(row.get("after_version"),r.get("row_version")),STATE_CONFLICT);
        require(j.queryForObject("SELECT max(attempt_no)<=18 AND count(*)=max(attempt_no) FROM ddip_management.attempts WHERE run_id=?",Boolean.class,run),MANAGEMENT_CAPACITY_EXCEEDED);
        require(!j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.stage_artifacts s JOIN ddip_management.attempts a USING(attempt_id) WHERE a.run_id=? AND s.role='RESULT')",Boolean.class,run),STATE_CONFLICT);
        return SourceJson.object().put("policy",POLICY).put("runId",run.toString()).put("recoveryAttemptId",recovery.toString()).put("runVersion",r.get("row_version").toString()).put("runState",r.get("state").toString()).put("selectionSha256",r.get("selection_sha256").toString()).put("recoveryReceiptSha256",SourceJson.sha((byte[])row.get("details_bytes")));
    }
    public static JsonNode queued(JdbcTemplate j,UUID run,long version) {
        var rows=j.queryForList("SELECT e.details_bytes,e.before_version,e.before_state,c.* FROM ddip_management.audit_events e JOIN ddip_management.controls c USING(control_id,run_id) WHERE e.run_id=? AND e.action='RELIABILITY_RETRY_QUEUED' AND e.after_state='QUEUED' AND e.after_version=? AND c.type='RETRY' AND c.state='APPLIED' AND c.reason='RETRY_QUEUED'",run,version);
        require(rows.size()==1,STATE_CONFLICT);var row=rows.getFirst();var plan=SourceJson.parse((byte[])row.get("details_bytes"));
        require(plan.get("policy").stringValue().equals(POLICY) && plan.get("runId").stringValue().equals(run.toString()) && Long.parseLong(plan.get("runVersion").stringValue())+1==version && row.get("control_id").toString().equals(plan.get("retryControlId").stringValue()),STATE_CONFLICT);
        require(((Number)row.get("expected_version")).longValue()==version-1 && ((Number)row.get("before_version")).longValue()==version-1 && row.get("before_state").equals(plan.get("runState").stringValue()) && ((Number)row.get("row_version")).longValue()==1 && row.get("applied_at")!=null && row.get("target_attempt_id")==null,STATE_CONFLICT);
        var body=SourceJson.object().put("action","RETRY").putNull("attemptId").putNull("quiescenceConfirmed");
        var command=ControlCommands.parse(run.toString(),SourceJson.canonical(body),(String)row.get("idempotency_key"),"\""+(version-1)+"\"");
        require(Arrays.equals(command.normalized(),(byte[])row.get("normalized_request")) && SourceJson.sha(command.normalized()).equals(row.get("request_sha256")),STATE_CONFLICT);
        return plan;
    }
    /** Called under the coordinator/run lock. A later EXECUTE must carry the exact applied retry receipt. */
    public static void requireCancellationLineage(JdbcTemplate j,UUID run,UUID attempt) {
        var rows=j.queryForList("SELECT details_bytes,control_id FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND action='RELIABILITY_RETRY_CLAIMED' AND after_state='CLAIMED' AND after_version=0",run,attempt);
        require(rows.size()==1,STATE_CONFLICT);var row=rows.getFirst();var plan=SourceJson.parse((byte[])row.get("details_bytes"));
        long version=Long.parseLong(plan.get("runVersion").stringValue())+1;
        require(queued(j,run,version).equals(plan) && row.get("control_id").toString().equals(plan.get("retryControlId").stringValue()),STATE_CONFLICT);
        require(j.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND action='RUN_CLAIMED' AND before_state='QUEUED' AND after_state='RUNNING' AND before_version=? AND after_version=?",Long.class,run,attempt,version,version+1)==1,STATE_CONFLICT);
        require(j.queryForObject("SELECT count(*)=1 FROM ddip_management.attempts WHERE run_id=? AND attempt_id=? AND kind='EXECUTE' AND attempt_no=(SELECT max(attempt_no) FROM ddip_management.attempts WHERE run_id=?)",Boolean.class,run,attempt,run),STATE_CONFLICT);
        require(RecoveryFinalization.currentCancellations(j,run,attempt).isEmpty(),STATE_CONFLICT);
    }

}
