package platform.contentapi.execution;

import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.ContentResumePlan.require;
import static platform.contentapi.management.SourceFailure.Code.*;

/** Separate local operator workflow: fresh qualification, atomic requeue, explicit execution. */
public final class ReliabilityRetry {
    private final JdbcTemplate j;private final ManagementTransactions tx;private final UUID installation;
    public ReliabilityRetry(JdbcTemplate j,ManagementTransactions tx,UUID i){this.j=j;this.tx=tx;installation=i;}
    private record Snapshot(JsonNode plan,long controlVersion,long epoch,long slotVersion){}
    private Snapshot snapshot(UUID control){return tx.required(()->{
        var slot=ContentResumePlan.lockSlot(j,installation);ContentResumePlan.idle(j,slot);
        var ids=j.queryForList("SELECT run_id FROM ddip_management.controls WHERE control_id=? AND type='RETRY'",control);require(ids.size()==1,NOT_FOUND);
        var r=ContentResumePlan.lockRun(j,(UUID)ids.getFirst().get("run_id"));var k=j.queryForMap("SELECT * FROM ddip_management.controls WHERE control_id=? FOR UPDATE",control);
        var body=SourceJson.object().put("action","RETRY").putNull("attemptId").putNull("quiescenceConfirmed");
        var command=ControlCommands.parse(r.get("run_id").toString(),SourceJson.canonical(body),(String)k.get("idempotency_key"),"\""+k.get("expected_version")+"\"");
        require(Arrays.equals(command.normalized(),(byte[])k.get("normalized_request")) && SourceJson.sha(command.normalized()).equals(k.get("request_sha256")),STATE_CONFLICT);
        require("REQUESTED".equals(k.get("state")) && Objects.equals(k.get("expected_version"),r.get("row_version")),PRECONDITION_FAILED);
        require(!j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND control_id<>? AND state='REQUESTED')",Boolean.class,r.get("run_id"),control),STATE_CONFLICT);
        ContentResumePlan.gates(j,installation,r);ContentResumePlan.capacity(j,(UUID)r.get("run_id"));
        return new Snapshot(ReliabilityRetryPlan.metadata(j,r),num(k,"row_version"),num(slot,"epoch"),num(slot,"row_version"));
    });}
    public JsonNode applied(UUID control){return tx.required(()->{
        ContentResumePlan.lockSlot(j,installation);var rows=j.queryForList("SELECT e.details_bytes,e.after_version FROM ddip_management.controls c JOIN ddip_management.audit_events e USING(control_id,run_id) WHERE c.control_id=? AND c.type='RETRY' AND c.state='APPLIED' AND c.reason='RETRY_QUEUED' AND e.action='RELIABILITY_RETRY_QUEUED'",control);
        if(rows.isEmpty())return null;require(rows.size()==1,STATE_CONFLICT);var plan=SourceJson.parse((byte[])rows.getFirst().get("details_bytes"));require(plan.get("policy").stringValue().equals(ReliabilityRetryPlan.POLICY) && plan.get("retryControlId").stringValue().equals(control.toString()) && Long.parseLong(plan.get("runVersion").stringValue())+1==num(rows.getFirst(),"after_version"),STATE_CONFLICT);return plan;
    });}
    private JsonNode frozen(UUID run){return tx.required(()->{var r=j.queryForMap("SELECT captured_selection,selection_sha256 FROM ddip_management.runs WHERE run_id=?",run);return FrozenRunSelection.canonical((byte[])r.get("captured_selection"),(String)r.get("selection_sha256"),PreflightFiles.LARGE);});}
    public JsonNode requeue(InstallationHostGuard guard,UUID control,String runtimeId){
        guard.checkHeld(installation);var prior=applied(control);if(prior!=null)return prior;
        var before=snapshot(control);var plan=(ObjectNode)before.plan().deepCopy();UUID run=UUID.fromString(plan.get("runId").stringValue());
        var proof=new ReliabilityRetryEvidence(j,tx).verify(guard,run,control,frozen(run),runtimeId);
        plan.put("retryControlId",control.toString()).put("runtimeId",runtimeId).put("runtimeSha256",proof.runtimeDigest()).put("nativeFingerprint",proof.fingerprint()).put("verificationAddress",proof.address()).put("verificationSha256",proof.sha256()).put("nativeSealSha256",proof.sealDigest()).put("hostBindingSha256",guard.binding().digest());
        byte[] bytes=SourceJson.canonical(plan);require(bytes.length<=8192,MANAGEMENT_CAPACITY_EXCEEDED);proof.recheck(guard);Instant now=Instant.now();
        return tx.required(()->{
            require(snapshot(control).equals(before),PRECONDITION_FAILED);guard.checkHeld(installation);RecoveryQuiescence.fresh(now,Instant.now());
            noArtifact(proof.fingerprint());long version=Long.parseLong(plan.get("runVersion").stringValue());
            exact(j.update("UPDATE ddip_management.runs SET state='QUEUED',current_attempt_id=NULL,current_stage=NULL,finished_at=NULL,counts_bytes=NULL,outcome_code='RETRY_QUEUED',retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND state=? AND current_attempt_id=? AND row_version=?",run,plan.get("runState").stringValue(),UUID.fromString(plan.get("recoveryAttemptId").stringValue()),version));
            exact(j.update("UPDATE ddip_management.controls SET state='APPLIED',reason='RETRY_QUEUED',applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=? AND type='RETRY' AND state='REQUESTED' AND row_version=?",control,before.controlVersion()));
            audit(run,control,"RELIABILITY_RETRY_QUEUED",plan.get("runState").stringValue(),"QUEUED",version,version+1,bytes);
            audit(run,control,"CONTROL_APPLIED","REQUESTED","APPLIED",before.controlVersion(),before.controlVersion()+1,bytes);return plan;
        });
    }
    public UUID execute(InstallationHostGuard guard,UUID control,String runtimeId){return execute(guard,control,runtimeId,new ReadOnlyVerifierProcess(true));}
    /** Package-only process boundary seam for crash qualification. */
    UUID execute(InstallationHostGuard guard,UUID control,String runtimeId,ReadOnlyVerifierProcess publisher){
        guard.checkHeld(installation);var plan=applied(control);require(plan!=null && plan.get("runtimeId").stringValue().equals(runtimeId) && plan.get("hostBindingSha256").stringValue().equals(guard.binding().digest()),STATE_CONFLICT);
        UUID run=UUID.fromString(plan.get("runId").stringValue());
        var already=tx.required(()->j.queryForList("SELECT r.result_artifact_id FROM ddip_management.runs r JOIN ddip_management.audit_events e ON e.run_id=r.run_id AND e.attempt_id=r.current_attempt_id WHERE r.run_id=? AND r.state='SUCCEEDED' AND e.action='RELIABILITY_RETRY_CLAIMED' AND e.control_id=?",run,control));if(!already.isEmpty()){require(already.size()==1 && already.getFirst().get("result_artifact_id")!=null,STATE_CONFLICT);return (UUID)already.getFirst().get("result_artifact_id");}
        tx.required(()->{var slot=ContentResumePlan.lockSlot(j,installation);ContentResumePlan.idle(j,slot);var r=ContentResumePlan.lockRun(j,run);require("QUEUED".equals(r.get("state")) && ReliabilityRetryPlan.queued(j,run,num(r,"row_version")).equals(plan),STATE_CONFLICT);ContentResumePlan.gates(j,installation,r);return null;});
        var frozen=frozen(run);var proof=new ReliabilityRetryEvidence(j,tx).verify(guard,run,control,UUID.fromString(plan.get("recoveryAttemptId").stringValue()),frozen,runtimeId);
        require(proof.fingerprint().equals(plan.get("nativeFingerprint").stringValue()) && proof.runtimeDigest().equals(plan.get("runtimeSha256").stringValue()),STATE_CONFLICT);proof.recheck(guard);
        var p=frozen.get("executionProfile");var workers=new WorkerRepository(j,tx,installation,new WorkerExecutionCapability(){public boolean available(){return true;}public boolean supports(String pipeline){return pipeline.equals("RELIABILITY_V2");}});
        var session=workers.register(guard,p.get("profileId").stringValue(),p.get("configurationSha256").stringValue(),p.get("configuration").get("runtime").get("deploymentSha256").stringValue());
        var claim=workers.claimRetry(guard,session,UUID.randomUUID(),run);require(claim.owner()!=null,STATE_CONFLICT);
        var verified=new ReliabilityPreflight(j,tx,workers).verifyForExecution(guard,claim.owner(),runtimeId,UUID.randomUUID());
        return new ReliabilityExecution(j,tx,workers,publisher,new ReadOnlyVerifierProcess()).run(guard,verified,runtimeId).artifactId();
    }
    private void noArtifact(String fingerprint){require(!j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2' AND native_identity=?)",Boolean.class,fingerprint),STATE_CONFLICT);}
    private void audit(UUID run,UUID control,String action,String before,String after,long version,long next,byte[] bytes){j.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,'RETRY_QUEUED',?)",run,control,action,before,after,version,next,bytes);}
    private static long num(Map<String,Object> r,String k){return ((Number)r.get(k)).longValue();}
    private static void exact(int n){require(n==1,STATE_CONFLICT);}
}
