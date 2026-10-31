package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.*;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** Immutable links, never cloned successful stage invocations or adopted execution authority. */
final class ContentResumeLineage {
    static JsonNode queued(JdbcTemplate j,UUID run,long version){
        var rows=j.queryForList("SELECT e.details_bytes FROM ddip_management.audit_events e JOIN ddip_management.controls k ON k.control_id=e.control_id WHERE e.run_id=? AND e.action='CONTENT_RESUME_QUEUED' AND e.after_version=? AND k.type='RESUME' AND k.state='APPLIED'",run,version);
        need(rows.size()==1,INPUT_INVALID);return SourceJson.parse((byte[])rows.getFirst().get("details_bytes"),PreflightFiles.LARGE);
    }
    static JsonNode plan(JdbcTemplate j,UUID run,UUID attempt){try{return readPlan(j,run,attempt);}catch(org.springframework.dao.DataAccessException ex){throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_UNAVAILABLE);}}
    private static JsonNode readPlan(JdbcTemplate j,UUID run,UUID attempt){
        var rows=j.queryForList("SELECT e.details_bytes,a.reused_checkpoint_id FROM ddip_management.audit_events e JOIN ddip_management.attempts a ON a.attempt_id=e.attempt_id AND a.run_id=e.run_id WHERE e.run_id=? AND e.attempt_id=? AND e.action='CONTENT_RESUME_CLAIMED' AND a.kind='EXECUTE'",run,attempt);
        if(rows.isEmpty())return null;need(rows.size()==1,INPUT_INVALID);var p=SourceJson.parse((byte[])rows.getFirst().get("details_bytes"),PreflightFiles.LARGE);
        need(p.get("policy").stringValue().equals(ContentResumePlan.POLICY) && p.get("runId").stringValue().equals(run.toString()) && (p.get("checkpointId").isNull()?rows.getFirst().get("reused_checkpoint_id")==null:p.get("checkpointId").stringValue().equals(Objects.toString(rows.getFirst().get("reused_checkpoint_id"),null))),INPUT_CHANGED);
        need(j.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=? AND attempt_id=? AND kind='RECONCILE' AND state='SUCCEEDED'",Integer.class,run,UUID.fromString(p.get("recoveryAttemptId").stringValue()))==1,INPUT_CHANGED);return p;
    }
    static UUID dataAttempt(JdbcTemplate j,UUID run,UUID current){var p=plan(j,run,current);return p==null || p.get("boundary").stringValue().equals("BEFORE_FETCH")?current:UUID.fromString(p.get("originalAttemptId").stringValue());}
    static UUID preparationAttempt(JdbcTemplate j,UUID run,UUID attempt){return reusedInputs(j,run,attempt)?dataAttempt(j,run,attempt):attempt;}
    static boolean reusedInputs(JdbcTemplate j,UUID run,UUID attempt){var p=plan(j,run,attempt);return p!=null && p.get("boundary").stringValue().equals("CONTENT_INPUTS");}
    static List<Map<String,Object>> stages(JdbcTemplate j,UUID run,UUID attempt){
        var rows=new ArrayList<>(j.queryForList("SELECT stage,item_ordinal,state,intended_identity,input_evidence_sha256 FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY stage,item_ordinal FOR UPDATE",attempt));var p=plan(j,run,attempt);
        if(p!=null && !p.get("boundary").stringValue().equals("BEFORE_FETCH")){
            var prior=j.queryForList("SELECT stage,item_ordinal,state,intended_identity,input_evidence_sha256 FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY stage,item_ordinal",UUID.fromString(p.get("originalAttemptId").stringValue()));
            for(var s:prior)if(Set.of("FETCH_SOURCE","STORE_BRONZE","SILVER","PREPARE_GOLD","PUBLISH_GOLD").contains(s.get("stage")) || p.get("boundary").stringValue().equals("CONTENT_INPUTS") && s.get("stage").equals("CHECK_INPUTS")){need(s.get("state").equals("VERIFIED"),INPUT_CHANGED);rows.add(s);}
        }return rows;
    }
    static void absentContent(JdbcTemplate j,UUID run,UUID attempt,RssContentRuntime runtime,String fingerprint){
        if(plan(j,run,attempt)!=null){String address="v1/processor_revision=1/publication="+fingerprint;requireUnregistered(j,"CONTENT",runtime.config.get("contentBindingId").stringValue(),address);requireAbsent(Path.of(runtime.content.get("root").stringValue()),address);}
    }
    static boolean registered(JdbcTemplate j,String kind,String binding,String address){try{return j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.artifacts WHERE kind=? AND storage_binding_id=? AND relative_address=?)",Boolean.class,kind,binding,address);}catch(org.springframework.dao.DataAccessException ex){throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_UNAVAILABLE);}}
    static void requireUnregistered(JdbcTemplate j,String kind,String binding,String address){need(!registered(j,kind,binding,address),INPUT_INVALID);}
    static void requireAbsent(Path root,String address){
        try{
            var relative=Path.of(address);need(!relative.isAbsolute() && relative.equals(relative.normalize()) && !relative.startsWith(".."),INPUT_INVALID);need(Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS) && root.equals(root.toRealPath()),INPUT_CHANGED);Path p=root;
            for(Path part:relative){p=p.resolve(part);try{var attrs=Files.readAttributes(p,java.nio.file.attribute.BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);need(attrs.isDirectory() && p.equals(p.toRealPath()),INPUT_CHANGED);}catch(NoSuchFileException absent){return;}}
            throw new PreflightFailure(INPUT_INVALID);
        }catch(java.nio.file.InvalidPathException ex){throw new PreflightFailure(INPUT_INVALID);}catch(java.io.IOException ex){throw new PreflightFailure(INPUT_UNAVAILABLE);}
    }
}
