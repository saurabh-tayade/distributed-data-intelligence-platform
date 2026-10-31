package platform.contentapi.execution;

import java.sql.Timestamp;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Bounded projections only. No payloads, paths, arbitrary database strings or executable receipts. */
final class RecoveryOperatorInspection {
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    RecoveryOperatorInspection(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation){this.jdbc=jdbc;this.tx=tx;this.installation=installation;}
    ObjectNode inspect(UUID control) {
        var summary=tx.required(()->{
            var c=one(jdbc.queryForList("SELECT installation_id,mode,active_attempt_id,owner_instance_id,active_import_id,epoch,row_version FROM ddip_management.coordinator WHERE singleton_id=1 FOR SHARE"));
            need(installation.equals(c.get("installation_id")),OWNERSHIP_LOST);
            var r=one(jdbc.queryForList("SELECT k.run_id,k.target_attempt_id,k.type,k.state AS control_state,k.row_version AS control_version,k.expected_version,r.state AS run_state,r.row_version AS run_version,r.current_attempt_id,a.kind,a.state AS attempt_state,a.instance_id,a.owner_epoch,w.pid,w.process_started_at,w.status AS worker_status FROM ddip_management.controls k JOIN ddip_management.runs r USING(run_id) JOIN ddip_management.attempts a ON a.attempt_id=k.target_attempt_id JOIN ddip_management.worker_instances w USING(instance_id) WHERE k.control_id=?",control));
            need("RECONCILE".equals(r.get("type")),INVALID_REQUEST);
            var n=SourceJson.object().put("controlId",control.toString()).put("runId",r.get("run_id").toString()).put("attemptId",r.get("target_attempt_id").toString());
            n.put("controlState",fixed(r,"control_state",Set.of("REQUESTED","APPLIED","REJECTED","TOO_LATE")))
                .put("runState",fixed(r,"run_state",Set.of("PAUSED","QUEUED","RUNNING","CANCEL_REQUESTED","RECOVERY_REQUIRED","SUCCEEDED","FAILED","CANCELLED")))
                .put("attemptState",fixed(r,"attempt_state",Set.of("CLAIMED","RUNNING","SUCCEEDED","FAILED","INTERRUPTED","CANCELLED")))
                .put("attemptKind",fixed(r,"kind",Set.of("EXECUTE","RECONCILE")))
                .put("workerStatus",fixed(r,"worker_status",Set.of("READY","RECOVERY_ONLY","STOPPING","STOPPED")))
                .put("coordinatorMode",fixed(c,"mode",Set.of("READY","RECOVERY_ONLY","STOPPING")));
            for(String key:List.of("control_version","run_version","expected_version"))n.put(key,Objects.toString(r.get(key)));
            n.put("epoch",Objects.toString(c.get("epoch"))).put("coordinatorVersion",Objects.toString(c.get("row_version")));
            n.put("occupied",c.get("active_attempt_id")!=null || c.get("active_import_id")!=null);
            boolean exact=r.get("target_attempt_id").equals(c.get("active_attempt_id")) && r.get("target_attempt_id").equals(r.get("current_attempt_id")) && r.get("instance_id").equals(c.get("owner_instance_id")) && r.get("owner_epoch").equals(c.get("epoch")) && c.get("active_import_id")==null;
            n.put("selectedOwnerCurrent",exact);
            var ps=n.putArray("recordedProcesses");ps.add(identity(new RecoveryQuiescence.Identity(number(r,"pid"),((Timestamp)r.get("process_started_at")).toInstant())));
            var stages=n.putArray("stages");var unresolved=n.putArray("unresolvedCheckpoints");
            var rows=jdbc.queryForList("SELECT stage,checkpoint_id,state,child_pid,child_started_at FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY stage,item_ordinal LIMIT 513",r.get("target_attempt_id"));need(rows.size()<=512,CAPACITY_EXCEEDED);
            for(var s:rows){String state=fixed(s,"state",Set.of("INTENT","RUNNING","VERIFIED","FAILED","UNKNOWN"));stages.addObject().put("stage",fixed(s,"stage",Set.of("CHECK_INPUTS","PUBLISH_RELIABILITY","VERIFY_RESULT"))).put("checkpointId",s.get("checkpoint_id").toString()).put("state",state);if(Set.of("INTENT","RUNNING","UNKNOWN").contains(state))unresolved.add(s.get("checkpoint_id").toString());if(s.get("child_pid")!=null)ps.add(identity(new RecoveryQuiescence.Identity(number(s,"child_pid"),((Timestamp)s.get("child_started_at")).toInstant())));}
            n.put("canQualify",exact && r.get("control_state").equals("REQUESTED") && r.get("run_version").equals(r.get("expected_version")));
            n.put("authorization",false);n.putArray("manualChecks").add("STOP_EXECUTION_HANDLERS").add("REVIEW_ALL_LAUNCH_GAPS_AND_ORPHANS").add("STOP_UNMANAGED_WRITERS").add("REVIEW_KNOWN_DESCENDANT_IDENTITIES");
            return n;
        });
        boolean negative=true;
        for(var p:summary.get("recordedProcesses")) {
            var id=new RecoveryQuiescence.Identity(Long.parseLong(p.get("pid").stringValue()),java.time.Instant.parse(p.get("startedAt").stringValue()));
            var state=RecoveryQuiescence.LOCAL.inspect(id);((ObjectNode)p).put("state",state.name());negative &= state==RecoveryQuiescence.State.ABSENT || state==RecoveryQuiescence.State.REUSED;
        }
        return summary.put("canQualify",summary.get("canQualify").booleanValue() && negative);
    }
    private static ObjectNode identity(RecoveryQuiescence.Identity id){return SourceJson.object().put("pid",Long.toString(id.pid())).put("startedAt",id.startedAt().toString());}
    static ObjectNode process(RecoveryQuiescence.Identity id){return identity(id).put("state",RecoveryQuiescence.LOCAL.inspect(id).name());}
    private static String fixed(Map<String,Object> row,String key,Set<String> values){need(values.contains(row.get(key)),EVIDENCE_CHANGED);return (String)row.get(key);}
    private static long number(Map<String,Object> row,String key){return ((Number)row.get(key)).longValue();}
    private static Map<String,Object> one(List<Map<String,Object>> rows){need(rows.size()==1,OWNERSHIP_LOST);return rows.getFirst();}
}
