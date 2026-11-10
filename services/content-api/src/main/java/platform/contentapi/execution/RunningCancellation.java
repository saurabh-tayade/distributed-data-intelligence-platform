package platform.contentapi.execution;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;

/** Exact accepted cancel transition, not a renewed owner token. Survives only in this guarded JVM. */
final class RunningCancellation extends RuntimeException {
    boolean cleanupUncertain;
    final WorkerRepository.Owner owner;final UUID control;
    private RunningCancellation(WorkerRepository.Owner owner,UUID control){super("CANCEL_REQUESTED",null,false,false);this.owner=owner;this.control=control;}
    static RunningCancellation detect(JdbcTemplate jdbc,WorkerRepository.Owner owner,Map<String,Object> run){
        if(!"CANCEL_REQUESTED".equals(run.get("state")) || ((Number)run.get("row_version")).longValue()!=owner.runVersion()+1 || !owner.attemptId().equals(run.get("current_attempt_id")))return null;
        if(!Boolean.TRUE.equals(jdbc.queryForObject("SELECT pipeline='RELIABILITY_V2' FROM ddip_management.runs WHERE run_id=?",Boolean.class,owner.runId())))return null;
        var controls=jdbc.queryForList("SELECT * FROM ddip_management.controls WHERE run_id=? AND state='REQUESTED'",owner.runId());
        if(controls.size()!=1)return null;var c=controls.getFirst();
        if(!"CANCEL".equals(c.get("type")) || ((Number)c.get("expected_version")).longValue()!=owner.runVersion() || ((Number)c.get("row_version")).longValue()!=0 || c.get("target_attempt_id")!=null)return null;
        byte[] raw=(byte[])c.get("normalized_request");var request=SourceJson.parse(raw);
        if(!SourceJson.sha(raw).equals(c.get("request_sha256")) || !request.get("action").stringValue().equals("CANCEL") || !request.get("runId").stringValue().equals(owner.runId().toString()) || !request.get("expectedVersion").stringValue().equals(Long.toString(owner.runVersion())))return null;
        long audits=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND control_id=? AND action='RUN_CANCEL_REQUESTED' AND before_state='RUNNING' AND after_state='CANCEL_REQUESTED' AND before_version=? AND after_version=?",Long.class,owner.runId(),owner.attemptId(),c.get("control_id"),owner.runVersion(),owner.runVersion()+1);
        return audits==1?new RunningCancellation(owner,(UUID)c.get("control_id")):null;
    }
}
