package platform.contentapi.management;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.ManagementTransactions;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.Code.*;

/** Atomic cancellation of an original, never-claimed queued run. No worker, artifact or process I/O. */
final class QueuedCancelRepository {
    private final JdbcTemplate jdbc; private final ManagementTransactions tx; private final UUID installation;
    QueuedCancelRepository(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation){this.jdbc=jdbc;this.tx=tx;this.installation=installation;}
    JsonNode request(ControlCommands.Command command){return tx.required(()->{
        // Same ordering as claim: a winning cancellation prevents any attempt; a winning claim rejects this control.
        var slots=jdbc.queryForList("SELECT installation_id FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");
        require(slots.size()==1 && installation.equals(slots.getFirst().get("installation_id")));
        var old=jdbc.queryForList("SELECT control_id,normalized_request,request_sha256 FROM ddip_management.controls WHERE run_id=? AND idempotency_key=?",command.runId(),command.key());
        if(!old.isEmpty()){
            var row=old.getFirst();
            if(!Arrays.equals(command.normalized(),(byte[])row.get("normalized_request")) || !SourceJson.sha(command.normalized()).equals(row.get("request_sha256")))throw new SourceFailure(IDEMPOTENCY_CONFLICT);
            return receipt(command,(UUID)row.get("control_id"));
        }
        var runs=jdbc.queryForList("SELECT state,current_attempt_id,current_stage,result_artifact_id,row_version FROM ddip_management.runs WHERE run_id=? FOR UPDATE",command.runId());
        if(runs.isEmpty())throw new SourceFailure(NOT_FOUND);
        var run=runs.getFirst();
        if(((Number)run.get("row_version")).longValue()!=command.expectedVersion())throw new SourceFailure(PRECONDITION_FAILED);
        require("QUEUED".equals(run.get("state")) && run.get("current_attempt_id")==null && run.get("current_stage")==null && run.get("result_artifact_id")==null);
        require(!jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.attempts WHERE run_id=?)",Boolean.class,command.runId()));
        require(!jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND state='REQUESTED')",Boolean.class,command.runId()));
        if(jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=?",Long.class,command.runId())>=128)throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
        long unfinished=jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);
        AuditCapacity.requireAvailable(jdbc,3,unfinished-1);
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL',?,?,?,?,'REQUESTED','local-operator')",id,command.runId(),command.key(),command.normalized(),SourceJson.sha(command.normalized()),command.expectedVersion());
        audit(command,id,"CONTROL_REQUESTED",null,"REQUESTED",null,0L,"ACCEPTED");
        require(jdbc.update("UPDATE ddip_management.runs SET state='CANCELLED',finished_at=clock_timestamp(),outcome_code='CANCELLED',retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND state='QUEUED' AND row_version=?",command.runId(),command.expectedVersion())==1);
        audit(command,id,"RUN_CANCELLED","QUEUED","CANCELLED",command.expectedVersion(),command.expectedVersion()+1,"CANCELLED");
        jdbc.update("UPDATE ddip_management.controls SET state='APPLIED',reason='CANCELLED',applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=?",id);
        audit(command,id,"CONTROL_APPLIED","REQUESTED","APPLIED",0L,1L,"CANCELLED");
        return receipt(command,id);
    });}
    private JsonNode receipt(ControlCommands.Command c,UUID id){return new ControlRepository(jdbc,tx,installation).summary(c.runId(),id);}
    private void audit(ControlCommands.Command c,UUID id,String action,String before,String after,Long beforeVersion,Long afterVersion,String code){
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,?)",c.runId(),id,action,before,after,beforeVersion,afterVersion,code,SourceJson.canonical(SourceJson.object().put("protocolVersion",1)));
    }
    private static void require(boolean ok){if(!ok)throw new SourceFailure(STATE_CONFLICT);}
}
