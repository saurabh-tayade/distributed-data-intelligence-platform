package platform.contentapi.management;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.Code.*;

/** Called under the existing coordinator/run transaction; no process I/O or owner replacement. */
final class RunningCancelRepository {
    private final JdbcTemplate jdbc;private final UUID installation;
    RunningCancelRepository(JdbcTemplate jdbc,UUID installation){this.jdbc=jdbc;this.installation=installation;}
    JsonNode admit(ControlCommands.Command command,Map<String,Object> slot,Map<String,Object> run){
        UUID attempt=(UUID)run.get("current_attempt_id");
        require(installation.equals(slot.get("installation_id")) && "READY".equals(slot.get("mode")) && attempt!=null && attempt.equals(slot.get("active_attempt_id")) && slot.get("active_import_id")==null && run.get("result_artifact_id")==null);
        require(Boolean.TRUE.equals(jdbc.queryForObject("SELECT pipeline='RELIABILITY_V2' FROM ddip_management.runs WHERE run_id=?",Boolean.class,command.runId())));
        var attempts=jdbc.queryForList("SELECT * FROM ddip_management.attempts WHERE attempt_id=? FOR UPDATE",attempt);
        require(attempts.size()==1);var a=attempts.getFirst();
        require(command.runId().equals(a.get("run_id")) && "EXECUTE".equals(a.get("kind")) && Set.of("CLAIMED","RUNNING").contains(a.get("state")) && Objects.equals(slot.get("owner_instance_id"),a.get("instance_id")) && Objects.equals(slot.get("epoch"),a.get("owner_epoch")));
        require(Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.worker_instances w JOIN ddip_management.runs r ON r.execution_profile_id=w.execution_profile_id WHERE r.run_id=? AND w.instance_id=? AND w.installation_id=? AND w.status='READY')",Boolean.class,command.runId(),a.get("instance_id"),installation)));
        require(((Number)a.get("attempt_no")).longValue()==1 && jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=?",Long.class,command.runId())==1);
        require(!jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED')))",Boolean.class,command.runId()));
        if(jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=?",Long.class,command.runId())>=128)throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
        long unfinished=jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);
        AuditCapacity.requireAvailable(jdbc,2,unfinished);
        UUID control=UUID.randomUUID();
        jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL',?,?,?,?,'REQUESTED','local-operator')",control,command.runId(),command.key(),command.normalized(),SourceJson.sha(command.normalized()),command.expectedVersion());
        audit(command,attempt,control,"CONTROL_REQUESTED",null,"REQUESTED",null,0L,"ACCEPTED");
        require(jdbc.update("UPDATE ddip_management.runs SET state='CANCEL_REQUESTED',row_version=row_version+1 WHERE run_id=? AND state='RUNNING' AND row_version=? AND current_attempt_id=?",command.runId(),command.expectedVersion(),attempt)==1);
        audit(command,attempt,control,"RUN_CANCEL_REQUESTED","RUNNING","CANCEL_REQUESTED",command.expectedVersion(),command.expectedVersion()+1,"ACCEPTED");
        return SourceJson.object().put("controlId",control.toString()).put("runId",command.runId().toString());
    }
    private void audit(ControlCommands.Command c,UUID attempt,UUID id,String action,String before,String after,Long bv,Long av,String code){jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,?,?)",c.runId(),attempt,id,action,before,after,bv,av,code,SourceJson.canonical(SourceJson.object().put("protocolVersion",1)));}
    private static void require(boolean value){if(!value)throw new SourceFailure(STATE_CONFLICT);}
}
