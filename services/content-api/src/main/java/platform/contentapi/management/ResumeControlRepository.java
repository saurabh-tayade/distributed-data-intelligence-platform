package platform.contentapi.management;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFailure.Code.*;

/** Admission only. Applying RESUME requires the separate guarded, read-only operator qualification. */
final class ResumeControlRepository {
    private final JdbcTemplate j;private final ManagementTransactions tx;private final UUID installation;
    ResumeControlRepository(JdbcTemplate j,ManagementTransactions tx,UUID i){this.j=j;this.tx=tx;installation=i;}
    JsonNode request(ControlCommands.Command c){return tx.required(()->{
        var slot=ContentResumePlan.lockSlot(j,installation);
        var old=j.queryForList("SELECT control_id,normalized_request FROM ddip_management.controls WHERE run_id=? AND idempotency_key=?",c.runId(),c.key());
        if(!old.isEmpty()){if(!Arrays.equals((byte[])old.getFirst().get("normalized_request"),c.normalized()))throw new SourceFailure(IDEMPOTENCY_CONFLICT);return new ControlRepository(j,tx,installation).summary(c.runId(),(UUID)old.getFirst().get("control_id"));}
        ContentResumePlan.idle(j,slot);var run=ContentResumePlan.lockRun(j,c.runId());
        if(((Number)run.get("row_version")).longValue()!=c.expectedVersion())throw new SourceFailure(PRECONDITION_FAILED);
        if(j.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=?",Long.class,c.runId())>=128)throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
        ContentResumePlan.metadata(j,run);ContentResumePlan.gates(j,installation,run);ContentResumePlan.capacity(j,c.runId());
        if(j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND state='REQUESTED')",Boolean.class,c.runId()))throw new SourceFailure(STATE_CONFLICT);
        UUID id=UUID.randomUUID();j.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'RESUME',?,?,?,?,'REQUESTED','local-operator')",id,c.runId(),c.key(),c.normalized(),SourceJson.sha(c.normalized()),c.expectedVersion());
        j.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,control_id,action,after_state,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,'CONTROL_REQUESTED','REQUESTED',0,'ACCEPTED',?)",c.runId(),id,SourceJson.canonical(SourceJson.object().put("protocolVersion",1)));
        return new ControlRepository(j,tx,installation).summary(c.runId(),id);
    });}
}
