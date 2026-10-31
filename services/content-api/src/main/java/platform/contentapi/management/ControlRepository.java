package platform.contentapi.management;

import java.sql.*;
import java.time.*;
import java.time.format.*;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.execution.ManagementTransactions;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFailure.Code.*;

/** Intent admission only. No guard acquisition, owner mutation, native I/O, child launch or handoff. */
public final class ControlRepository {
    private static final DateTimeFormatter UTC=new DateTimeFormatterBuilder().appendInstant(6).toFormatter();
    private static final String SUMMARY="c.control_id,c.run_id,c.type,c.state,c.reason,c.requested_at,c.applied_at,c.row_version,r.row_version AS run_version";
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    private final Consumer<List<ControlProcesses.Identity>> observe;
    public ControlRepository(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation){this(jdbc,tx,installation,ControlProcesses::check);}
    /** Observation/fault seam only, not a configurable bean or executable. */
    ControlRepository(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Consumer<List<ControlProcesses.Identity>> observe){this.jdbc=jdbc;this.tx=tx;this.installation=installation;this.observe=observe;}
    private record Snapshot(Map<String,Object> coordinator,Map<String,Object> run,Map<String,Object> attempt,Map<String,Object> worker,List<Map<String,Object>> stages) {
        List<ControlProcesses.Identity> processes(){var ids=new ArrayList<ControlProcesses.Identity>();ids.add(identity(worker,"pid","process_started_at","instance_id"));for(var s:stages)if(s.get("child_pid")!=null)ids.add(identity(s,"child_pid","child_started_at","child_instance_id"));return List.copyOf(ids);}
    }
    private record Prepared(Snapshot snapshot,JsonNode replay) { }
    public JsonNode request(ControlCommands.Command command) {
        if(command.action().equals("RESUME"))return new ResumeControlRepository(jdbc,tx,installation).request(command);
        conflict(!TransactionSynchronizationManager.isActualTransactionActive());
        // Read exact replay first, before current ETag, owner eligibility or process observation.
        Prepared prepared=tx.required(()->{var c=coordinator();var replay=replay(command);return replay!=null?new Prepared(null,replay):new Prepared(snapshot(command,c),null);});
        if(prepared.replay()!=null)return prepared.replay();
        observe.accept(prepared.snapshot().processes()); // External observation never spans a SQL transaction.
        return tx.required(()->{
            var c=coordinator();var replay=replay(command);if(replay!=null)return replay;
            var current=snapshot(command,c);conflict(current.equals(prepared.snapshot()));
            long unfinished=jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);
            AuditCapacity.requireAvailable(jdbc,1,unfinished);
            UUID control=UUID.randomUUID();byte[] normalized=command.normalized();
            jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,target_attempt_id,quiescence_confirmed,state,requester) VALUES (?,?,'RECONCILE',?,?,?,?,?,true,'REQUESTED','local-operator')",control,command.runId(),command.key(),normalized,SourceJson.sha(normalized),command.expectedVersion(),command.attemptId());
            jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,after_state,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,'CONTROL_REQUESTED','REQUESTED',0,'ACCEPTED',?)",command.runId(),command.attemptId(),control,SourceJson.canonical(SourceJson.object().put("protocolVersion",1)));
            return summary(command.runId(),control);
        });
    }
    private Map<String,Object> coordinator() {
        var rows=jdbc.queryForList("SELECT installation_id,mode,epoch,active_attempt_id,owner_instance_id,active_import_id,row_version FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");
        conflict(rows.size()==1 && installation.equals(rows.getFirst().get("installation_id")));return rows.getFirst();
    }
    private JsonNode replay(ControlCommands.Command command) {
        var rows=jdbc.queryForList("SELECT control_id,normalized_request,request_sha256 FROM ddip_management.controls WHERE run_id=? AND idempotency_key=?",command.runId(),command.key());
        if(rows.isEmpty())return null;var r=rows.getFirst();
        if(!Arrays.equals(command.normalized(),(byte[])r.get("normalized_request")) || !SourceJson.sha(command.normalized()).equals(r.get("request_sha256")))throw new SourceFailure(IDEMPOTENCY_CONFLICT);
        return summary(command.runId(),(UUID)r.get("control_id"));
    }
    private Snapshot snapshot(ControlCommands.Command command,Map<String,Object> c) {
        var runs=jdbc.queryForList("SELECT run_id,state,current_attempt_id,row_version,execution_profile_id FROM ddip_management.runs WHERE run_id=? FOR UPDATE",command.runId());
        if(runs.isEmpty())throw new SourceFailure(NOT_FOUND);var r=runs.getFirst();
        if(number(r,"row_version")!=command.expectedVersion())throw new SourceFailure(PRECONDITION_FAILED);
        var attempts=jdbc.queryForList("SELECT attempt_id,run_id,instance_id,owner_epoch,kind,state,row_version FROM ddip_management.attempts WHERE attempt_id=? FOR UPDATE",command.attemptId());
        if(attempts.isEmpty())throw new SourceFailure(NOT_FOUND);var a=attempts.getFirst();
        conflict(Set.of("RUNNING","CANCEL_REQUESTED","RECOVERY_REQUIRED").contains(r.get("state")) && command.attemptId().equals(r.get("current_attempt_id")) && command.attemptId().equals(c.get("active_attempt_id")) && c.get("active_import_id")==null && command.runId().equals(a.get("run_id")) && a.get("instance_id").equals(c.get("owner_instance_id")) && number(a,"owner_epoch")==number(c,"epoch"));
        boolean active=Set.of("CLAIMED","RUNNING").contains(a.get("state"));
        boolean closed="RECOVERY_REQUIRED".equals(r.get("state")) && ("INTERRUPTED".equals(a.get("state")) || "RECONCILE".equals(a.get("kind")) && "FAILED".equals(a.get("state")));
        conflict(active || closed);
        conflict("EXECUTE".equals(a.get("kind")) || "RECOVERY_REQUIRED".equals(r.get("state")) && "RECONCILE".equals(a.get("kind")));
        var workers=jdbc.queryForList("SELECT instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,status,row_version FROM ddip_management.worker_instances WHERE instance_id=?",a.get("instance_id"));
        conflict(workers.size()==1);var w=workers.getFirst();conflict(installation.equals(w.get("installation_id")) && r.get("execution_profile_id").equals(w.get("execution_profile_id")));
        conflict(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.installation_id=? AND b.role='CONTROL' AND b.configuration_sha256=?)",Boolean.class,w.get("execution_profile_id"),installation,w.get("host_binding_sha256")));
        var stages=jdbc.queryForList("SELECT stage,item_ordinal,checkpoint_id,state,row_version,child_pid,child_started_at,child_instance_id FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY stage,item_ordinal LIMIT 513",command.attemptId());conflict(stages.size()<=512);
        if(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND target_attempt_id=? AND type='RECONCILE' AND state='REQUESTED')",Boolean.class,command.runId(),command.attemptId()))throw new SourceFailure(STATE_CONFLICT);
        if(jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=?",Long.class,command.runId())>=128)throw new SourceFailure(MANAGEMENT_CAPACITY_EXCEEDED);
        return new Snapshot(c,r,a,w,stages);
    }
    public JsonNode get(String run,String control){UUID id=UUID.fromString(SourceCommands.uuid(run)),cid=UUID.fromString(SourceCommands.uuid(control));return tx.required(()->summary(id,cid));}
    ObjectNode summary(UUID run,UUID control) {
        var rows=jdbc.query("SELECT "+SUMMARY+" FROM ddip_management.controls c JOIN ddip_management.runs r USING(run_id) WHERE c.run_id=? AND c.control_id=?",(rs,n)->{
            RunInspectionValues.check(Set.of("CANCEL","RETRY","RECONCILE","RESUME").contains(rs.getString("type")) && Set.of("REQUESTED","APPLIED","TOO_LATE","REJECTED").contains(rs.getString("state")));
            return SourceJson.object().put("controlId",rs.getObject("control_id",UUID.class).toString()).put("runId",rs.getObject("run_id",UUID.class).toString()).put("type",rs.getString("type")).put("state",rs.getString("state")).put("reason",RunInspectionValues.code(rs.getString("reason"))).put("requestedAt",UTC.format(rs.getObject("requested_at",OffsetDateTime.class).toInstant())).put("appliedAt",rs.getObject("applied_at",OffsetDateTime.class)==null?null:UTC.format(rs.getObject("applied_at",OffsetDateTime.class).toInstant())).put("rowVersion",Long.toString(rs.getLong("row_version"))).put("runVersion",Long.toString(rs.getLong("run_version")));
        },run,control);if(rows.isEmpty())throw new SourceFailure(NOT_FOUND);return rows.getFirst();
    }
    private static ControlProcesses.Identity identity(Map<String,Object> r,String pid,String start,String instance){return new ControlProcesses.Identity(number(r,pid),((Timestamp)r.get(start)).toInstant(),(UUID)r.get(instance));}
    private static long number(Map<String,Object> r,String key){return ((Number)r.get(key)).longValue();}
    private static void conflict(boolean condition){if(!condition)throw new SourceFailure(STATE_CONFLICT);}
}
