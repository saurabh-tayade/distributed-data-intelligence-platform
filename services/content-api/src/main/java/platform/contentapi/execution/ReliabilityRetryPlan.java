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
    static JsonNode queued(JdbcTemplate j,UUID run,long version) {
        var rows=j.queryForList("SELECT e.details_bytes FROM ddip_management.audit_events e JOIN ddip_management.controls c USING(control_id) WHERE e.run_id=? AND e.action='RELIABILITY_RETRY_QUEUED' AND e.after_version=? AND c.type='RETRY' AND c.state='APPLIED' AND c.reason='RETRY_QUEUED'",run,version);
        require(rows.size()==1,STATE_CONFLICT);var plan=SourceJson.parse((byte[])rows.getFirst().get("details_bytes"));require(plan.get("policy").stringValue().equals(POLICY) && plan.get("runId").stringValue().equals(run.toString()) && Long.parseLong(plan.get("runVersion").stringValue())+1==version,STATE_CONFLICT);return plan;
    }
}
