package platform.contentapi.execution;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.Code.*;

/** SQL-only checkpoint and admission policy. Caller locks coordinator first, then sources/run. */
public final class ContentResumePlan {
    static final String POLICY="E5_CONTENT_RESUME_V1";
    private ContentResumePlan(){}
    static void require(boolean ok,SourceFailure.Code code){if(!ok)throw new SourceFailure(code);}
    public static Map<String,Object> lockSlot(JdbcTemplate j,UUID installation){var row=j.queryForMap("SELECT * FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");require(installation.equals(row.get("installation_id")),STATE_CONFLICT);return row;}
    public static Map<String,Object> lockRun(JdbcTemplate j,UUID run){j.queryForList("SELECT s.source_id FROM ddip_management.sources s JOIN ddip_management.run_sources r USING(product_id,source_id) WHERE r.run_id=? ORDER BY s.product_id COLLATE \"C\",s.source_id COLLATE \"C\" FOR UPDATE OF s",run);var rows=j.queryForList("SELECT * FROM ddip_management.runs WHERE run_id=? FOR UPDATE",run);require(rows.size()==1,NOT_FOUND);return rows.getFirst();}
    public static void idle(JdbcTemplate j,Map<String,Object> slot){require(((Number)slot.get("epoch")).longValue()>=j.queryForObject("SELECT COALESCE(max(owner_epoch),0) FROM ddip_management.attempts",Long.class),COORDINATOR_RECOVERY_REQUIRED);require("READY".equals(slot.get("mode")) && slot.get("active_attempt_id")==null && slot.get("active_import_id")==null && slot.get("owner_instance_id")==null,COORDINATOR_RECOVERY_REQUIRED);require(!j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.runs WHERE state IN ('RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')) OR EXISTS(SELECT 1 FROM ddip_management.attempts WHERE state IN ('CLAIMED','RUNNING')) OR EXISTS(SELECT 1 FROM ddip_management.artifact_import_operations WHERE state='ACTIVE')",Boolean.class),COORDINATOR_RECOVERY_REQUIRED);}
    public static void capacity(JdbcTemplate j,UUID run){long queued=j.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);require(queued<20 && j.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=?",Long.class,run)<=128 && j.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=?",Long.class,run)<20,MANAGEMENT_CAPACITY_EXCEEDED);AuditCapacity.requireAvailable(j,3,queued+1);}
    public static void gates(JdbcTemplate j,UUID installation,Map<String,Object> run){
        var sources=j.queryForList("SELECT r.source_id,r.revision,r.captured_gate_version,r.captured_enabled,s.enabled,s.profile_family,v.configuration_bytes,v.configuration_sha256,v.endpoint_id,v.endpoint_sha256 FROM ddip_management.run_sources r JOIN ddip_management.sources s USING(product_id,source_id) JOIN ddip_management.source_revisions v ON (v.product_id,v.source_id,v.revision)=(r.product_id,r.source_id,r.revision) WHERE r.run_id=? ORDER BY r.source_id COLLATE \"C\" FOR UPDATE OF s",run.get("run_id"));
        require(sources.stream().allMatch(s->Boolean.TRUE.equals(s.get("enabled"))),SOURCE_DISABLED);
        try{var f=FrozenRunSelection.canonical((byte[])run.get("captured_selection"),(String)run.get("selection_sha256"),PreflightFiles.LARGE);var p=f.get("executionProfile");FrozenRunSelection.validate(j,installation,run,sources,p.get("profileId").stringValue(),p.get("configurationSha256").stringValue(),p.get("configuration").get("runtime").get("deploymentSha256").stringValue());}catch(org.springframework.dao.DataAccessException ex){throw ex;}catch(RuntimeException ex){throw new SourceFailure(RESUME_EVIDENCE_INVALID);}
    }
    public static JsonNode metadata(JdbcTemplate j,Map<String,Object> run){try{return resolvedMetadata(j,run);}catch(SourceFailure|org.springframework.dao.DataAccessException ex){throw ex;}catch(RuntimeException ex){throw new SourceFailure(RESUME_EVIDENCE_INVALID);}}
    private static JsonNode resolvedMetadata(JdbcTemplate j,Map<String,Object> run){
        require("CONTENT_RSS_V1".equals(run.get("pipeline")) && "PAUSED".equals(run.get("state")) && run.get("result_artifact_id")==null,STATE_CONFLICT);
        return checkpoint(j,run,(UUID)run.get("current_attempt_id"));
    }
    static JsonNode checkpoint(JdbcTemplate j,Map<String,Object> run,UUID recovery){
        require("CONTENT_RSS_V1".equals(run.get("pipeline")) && run.get("result_artifact_id")==null,STATE_CONFLICT);var rows=j.queryForList("SELECT e.details_bytes FROM ddip_management.audit_events e JOIN ddip_management.attempts a ON a.attempt_id=e.attempt_id JOIN ddip_management.controls k ON k.control_id=e.control_id WHERE e.run_id=? AND e.attempt_id=? AND e.control_id=? AND e.action='RECOVERY_FINALIZED' AND a.kind='RECONCILE' AND a.state='SUCCEEDED' AND a.outcome_code='RECONCILED_PAUSED' AND k.type='RECONCILE' AND k.state='APPLIED'",run.get("run_id"),recovery,recovery);
        require(rows.size()==1,RESUME_EVIDENCE_INVALID);var d=SourceJson.parse((byte[])rows.getFirst().get("details_bytes"));require(d.get("policy").stringValue().equals(ContentRecovery.POLICY) && d.get("runState").stringValue().equals("PAUSED") && d.get("code").stringValue().equals("RECOVERED_CHECKPOINT"),RESUME_EVIDENCE_INVALID);
        UUID original=UUID.fromString(d.get("originalAttemptId").stringValue());require(j.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE attempt_id=? AND run_id=? AND kind='EXECUTE' AND state='INTERRUPTED'",Integer.class,original,run.get("run_id"))==1,RESUME_EVIDENCE_INVALID);
        // Reconciliation can preserve an inherited prefix without granting recursive continuation.
        // An empty resumed attempt must never be mistaken for an original pre-fetch boundary.
        require(ContentResumeLineage.plan(j,(UUID)run.get("run_id"),original)==null,RESUME_CHECKPOINT_UNSUPPORTED);
        var stages=j.queryForList("SELECT stage,item_ordinal,state,checkpoint_id FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY stage,item_ordinal",original);String boundary="BEFORE_FETCH";
        if(!stages.isEmpty()){
            var frozen=SourceJson.parse((byte[])run.get("captured_selection"),PreflightFiles.LARGE);var expected=new HashSet<String>();for(int i=0;i<frozen.get("sources").size();i++)for(String name:List.of("FETCH_SOURCE","STORE_BRONZE","SILVER"))expected.add(name+":"+i);expected.add("PREPARE_GOLD:0");expected.add("PUBLISH_GOLD:0");
            boolean inputs=stages.stream().anyMatch(s->s.get("stage").equals("CHECK_INPUTS"));if(inputs)expected.add("CHECK_INPUTS:0");
            require(stages.size()==expected.size(),RESUME_CHECKPOINT_UNSUPPORTED);for(var s:stages)require("VERIFIED".equals(s.get("state")) && expected.remove(s.get("stage")+":"+s.get("item_ordinal")),RESUME_CHECKPOINT_UNSUPPORTED);require(expected.isEmpty(),RESUME_CHECKPOINT_UNSUPPORTED);boundary=inputs?"CONTENT_INPUTS":"GOLD";
        }
        if(boundary.equals("BEFORE_FETCH"))for(var input:SourceJson.parse((byte[])run.get("captured_selection"),PreflightFiles.LARGE).get("inputs"))require(!input.get("role").stringValue().equals("RETAINED_SILVER"),RESUME_CHECKPOINT_UNSUPPORTED);
        var checkpoints=j.queryForList("SELECT checkpoint_id,state FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY item_ordinal DESC",recovery);require(checkpoints.stream().allMatch(s->s.get("state").equals("VERIFIED")),RESUME_EVIDENCE_INVALID);require(boundary.equals("BEFORE_FETCH") || !checkpoints.isEmpty(),RESUME_EVIDENCE_INVALID);
        var p=SourceJson.object().put("policy",POLICY).put("runId",run.get("run_id").toString()).put("originalAttemptId",original.toString()).put("recoveryAttemptId",recovery.toString()).put("boundary",boundary).put("selectionSha256",(String)run.get("selection_sha256"));for(String key:List.of("observationAddress","observationSha256","qualificationSha256"))p.set(key,d.get(key));if(checkpoints.isEmpty())p.putNull("checkpointId");else p.put("checkpointId",checkpoints.getFirst().get("checkpoint_id").toString());return p;
    }
}
