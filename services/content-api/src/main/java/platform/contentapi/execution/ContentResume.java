package platform.contentapi.execution;

import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.ContentResumePlan.require;
import static platform.contentapi.management.SourceFailure.Code.*;

/** Explicit operator adapter. Request admission alone never qualifies, claims or starts processing. */
public final class ContentResume {
    private final JdbcTemplate j;private final ManagementTransactions tx;private final UUID installation;private final ContentResumeEvidence evidence;
    public ContentResume(JdbcTemplate j,ManagementTransactions tx,UUID i){this(j,tx,i,new ContentResumeEvidence(j,tx));}
    ContentResume(JdbcTemplate j,ManagementTransactions tx,UUID i,ContentResumeEvidence e){this.j=j;this.tx=tx;installation=i;evidence=e;}
    private record Snapshot(JsonNode plan,long runVersion,long controlVersion,long epoch,long slotVersion){}
    private Snapshot snapshot(UUID control){return tx.required(()->{
        var slot=ContentResumePlan.lockSlot(j,installation);ContentResumePlan.idle(j,slot);var ids=j.queryForMap("SELECT run_id FROM ddip_management.controls WHERE control_id=? AND type='RESUME'",control);var run=ContentResumePlan.lockRun(j,(UUID)ids.get("run_id"));
        var k=j.queryForMap("SELECT * FROM ddip_management.controls WHERE control_id=? FOR UPDATE",control);require(k.get("state").equals("REQUESTED") && ((Number)k.get("expected_version")).longValue()==((Number)run.get("row_version")).longValue(),PRECONDITION_FAILED);
        ContentResumePlan.gates(j,installation,run);ContentResumePlan.capacity(j,(UUID)run.get("run_id"));var plan=ContentResumePlan.metadata(j,run);
        require(!j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND control_id<>? AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED')))",Boolean.class,run.get("run_id"),control),STATE_CONFLICT);
        return new Snapshot(plan,((Number)run.get("row_version")).longValue(),((Number)k.get("row_version")).longValue(),((Number)slot.get("epoch")).longValue(),((Number)slot.get("row_version")).longValue());
    });}
    public JsonNode applied(UUID control){return tx.required(()->{
        ContentResumePlan.lockSlot(j,installation);var rows=j.queryForList("SELECT k.run_id,e.details_bytes,e.after_version FROM ddip_management.controls k JOIN ddip_management.audit_events e ON e.control_id=k.control_id AND e.run_id=k.run_id WHERE k.control_id=? AND k.type='RESUME' AND k.state='APPLIED' AND k.reason='RESUME_QUEUED' AND e.action='CONTENT_RESUME_QUEUED'",control);
        if(rows.isEmpty())return null;require(rows.size()==1,RESUME_EVIDENCE_INVALID);return SourceJson.parse((byte[])rows.getFirst().get("details_bytes"),PreflightFiles.LARGE);
    });}
    public JsonNode requeue(InstallationHostGuard g,UUID control,String runtimeId){
        g.checkHeld(installation);var prior=applied(control);if(prior!=null)return prior;var before=snapshot(control);var proof=evidence.verify(g,before.plan(),runtimeId,control);
        var frozen=evidence.frozen(UUID.fromString(before.plan().get("runId").stringValue()));var runtime=RssContentRuntime.load(j,tx,g,UUID.fromString(before.plan().get("runId").stringValue()),frozen,runtimeId);
        require(proof.privateSha().equals(SourceJson.sha(SourceJson.canonical(evidence.privateProof(g,before.plan(),runtime)))),RESUME_EVIDENCE_INVALID);
        var plan=(ObjectNode)before.plan().deepCopy();plan.put("resumeControlId",control.toString()).put("runtimeId",runtimeId).put("runtimeSha256",runtime.digest).put("verificationAddress",proof.address()).put("verificationSha256",proof.sha()).put("privateSha256",proof.privateSha()).put("resumeBridgeSha256",proof.bridgeSha()).put("hostBindingSha256",g.binding().digest());
        byte[] bytes=SourceJson.canonical(plan);require(bytes.length<=8192,MANAGEMENT_CAPACITY_EXCEEDED);Instant qualified=Instant.now();
        return tx.required(()->{
            var current=snapshot(control);require(current.equals(before),PRECONDITION_FAILED);g.checkHeld(installation);RecoveryQuiescence.fresh(qualified,Instant.now());UUID run=UUID.fromString(plan.get("runId").stringValue());
            exact(j.update("UPDATE ddip_management.runs SET state='QUEUED',current_attempt_id=NULL,current_stage=NULL,finished_at=NULL,counts_bytes=NULL,outcome_code='RESUME_QUEUED',retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND state='PAUSED' AND current_attempt_id=? AND row_version=?",run,UUID.fromString(plan.get("recoveryAttemptId").stringValue()),before.runVersion()));
            exact(j.update("UPDATE ddip_management.controls SET state='APPLIED',reason='RESUME_QUEUED',applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=? AND type='RESUME' AND state='REQUESTED' AND row_version=?",control,before.controlVersion()));
            audit(run,control,"CONTENT_RESUME_QUEUED","PAUSED","QUEUED",before.runVersion(),before.runVersion()+1,bytes);
            audit(run,control,"CONTROL_APPLIED","REQUESTED","APPLIED",before.controlVersion(),before.controlVersion()+1,bytes);return plan;
        });
    }
    private void audit(UUID run,UUID control,String action,String before,String after,long v,long next,byte[] bytes){j.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,'RESUME_QUEUED',?)",run,control,action,before,after,v,next,bytes);}
    public UUID execute(InstallationHostGuard g,UUID control,String runtimeId){
        g.checkHeld(installation);var plan=applied(control);require(plan!=null && plan.get("runtimeId").stringValue().equals(runtimeId) && plan.get("hostBindingSha256").stringValue().equals(g.binding().digest()),RESUME_EVIDENCE_INVALID);UUID run=UUID.fromString(plan.get("runId").stringValue());
        // Repeated execution is facts-only after success. It never claims another attempt.
        if(tx.required(()->j.queryForObject("SELECT state='SUCCEEDED' FROM ddip_management.runs WHERE run_id=?",Boolean.class,run)))return new RssContentExecution(j,tx,new WorkerRepository(j,tx,installation)).inspect(g,run,runtimeId);
        tx.required(()->{var slot=ContentResumePlan.lockSlot(j,installation);ContentResumePlan.idle(j,slot);var r=ContentResumePlan.lockRun(j,run);require(r.get("state").equals("QUEUED") && ContentResumeLineage.queued(j,run,((Number)r.get("row_version")).longValue()).equals(plan),STATE_CONFLICT);ContentResumePlan.gates(j,installation,r);return null;});
        evidence.retained(g,plan);var verified=evidence.verify(g,plan,runtimeId,control);require(verified.sha().equals(plan.get("verificationSha256").stringValue()) && verified.privateSha().equals(plan.get("privateSha256").stringValue()) && verified.bridgeSha().equals(plan.get("resumeBridgeSha256").stringValue()),RESUME_EVIDENCE_INVALID);
        var frozen=evidence.frozen(run);var profile=frozen.get("executionProfile");var workers=new WorkerRepository(j,tx,installation,new WorkerExecutionCapability(){public boolean available(){return true;}public boolean supports(String pipeline){return pipeline.equals("CONTENT_RSS_V1");}});
        var session=workers.register(g,profile.get("profileId").stringValue(),profile.get("configurationSha256").stringValue(),profile.get("configuration").get("runtime").get("deploymentSha256").stringValue());
        var claim=workers.claimResumed(g,session,UUID.randomUUID(),run);require(claim.owner()!=null,STATE_CONFLICT);var owner=claim.owner();
        try{
            var runtime=RssContentRuntime.load(j,tx,g,run,frozen,runtimeId);require(runtime.digest.equals(plan.get("runtimeSha256").stringValue()),RESUME_EVIDENCE_INVALID);
            require(verified.privateSha().equals(SourceJson.sha(SourceJson.canonical(evidence.privateProof(g,plan,runtime)))),RESUME_EVIDENCE_INVALID);
            if(plan.get("boundary").stringValue().equals("BEFORE_FETCH")){
                String bronzeId=runtime.gold.silver.config.get("bronzeRuntimeId").stringValue();var bc=tx.required(()->j.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id=?",byte[].class,bronzeId));String fetchId=SourceJson.parse(bc).get("fetchRuntimeId").stringValue();
                for(int i=0;i<frozen.get("sources").size();i++){owner=new RssFetchExecution(j,tx,workers).fetch(g,owner,i,fetchId).stage().owner();owner=new RssBronzeExecution(j,tx,workers).store(g,owner,frozen.get("sources").get(i).get("sourceId").stringValue(),bronzeId).stage().owner();}
                for(var source:frozen.get("sources"))owner=new RssSilverExecution(j,tx,workers).execute(g,owner,source.get("sourceId").stringValue(),runtime.gold.config.get("silverRuntimeId").stringValue()).owner();
                var prepared=new RssGoldExecution(j,tx,workers).prepare(g,owner,runtime.config.get("goldRuntimeId").stringValue());owner=new RssGoldExecution(j,tx,workers).publish(g,prepared,runtime.config.get("goldRuntimeId").stringValue()).owner();
            }else owner=workers.markRunning(g,owner);
            return new RssContentExecution(j,tx,workers).execute(g,owner,runtimeId).artifact();
        }catch(ManagementFailure ex){
            if(tx.required(()->j.queryForObject("SELECT state='SUCCEEDED' FROM ddip_management.runs WHERE run_id=?",Boolean.class,run)))return new RssContentExecution(j,tx,workers).inspect(g,run,runtimeId);throw ex;
        }
    }
    private static void exact(int count){require(count==1,STATE_CONFLICT);}
}
