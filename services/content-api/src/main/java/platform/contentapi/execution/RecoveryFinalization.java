package platform.contentapi.execution;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.AuditCapacity;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Internal C3c.3 only. No bean, command, dispatcher, retry, cancellation or native writer. */
final class RecoveryFinalization {
    static final String POLICY="E5_RECOVERY_FINALIZATION_V1";
    enum Step { ARTIFACT, DEPENDENCY, LINK, ATTEMPT, RUN, CONTROL, WORKER, COORDINATOR, AUDIT }
    enum Status { CONFIRMED, NOT_FINALIZED, UNRESOLVED }
    /** Historical facts only: no owner, session or executable continuation. */
    record Receipt(Status status,UUID runId,UUID controlId,String runState,String code,UUID artifactId,boolean retryEligible) { }
    private record Decision(String state,String code,boolean retry) { }
    static final class Qualified {
        private final RecoveryHandoff.Bootstrap bootstrap;private final RecoveryVerification.Stage original,stage;
        private final RecoveryNativeSeal nativeSeal;private final JsonNode pack,evidence,privateSeal;private final Instant qualifiedAt;
        private final Decision decision;private final String anchorSha,originalAnchorSha;
        private Qualified(RecoveryHandoff.Bootstrap b,RecoveryVerification.Stage original,RecoveryVerification.Stage stage,RecoveryNativeSeal seal,JsonNode pack,JsonNode evidence,JsonNode privateSeal,Instant now,Decision d,String sha,String originalSha){bootstrap=b;this.original=original;this.stage=stage;nativeSeal=seal;this.pack=pack.deepCopy();this.evidence=evidence.deepCopy();this.privateSeal=privateSeal.deepCopy();qualifiedAt=now;decision=d;anchorSha=sha;originalAnchorSha=originalSha;}
        UUID checkpoint(){return stage.checkpoint();}
        @Override public String toString(){return "RecoveryFinalizationQualification[redacted]";}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final RecoveryHandoff handoffs;
    private final RecoveryVerification verification;private final Consumer<Step> fault;
    RecoveryFinalization(JdbcTemplate jdbc,ManagementTransactions tx,RecoveryHandoff handoffs){this(jdbc,tx,handoffs,new RecoveryVerification(jdbc,tx,handoffs),step->{});}
    RecoveryFinalization(JdbcTemplate jdbc,ManagementTransactions tx,RecoveryHandoff handoffs,RecoveryVerification verification,Consumer<Step> fault){this.jdbc=jdbc;this.tx=tx;this.handoffs=handoffs;this.verification=verification;this.fault=fault;}
    Qualified qualify(InstallationHostGuard guard,RecoveryHandoff.Bootstrap bootstrap,RecoveryVerification.Observation prior,RecoveryQuiescence.Review review) {
        try {
            outside(guard);need(prior!=null && prior.stage().itemOrdinal()==0,INVALID_REQUEST);var old=prior.stage();
            var observed=verification.inspect(guard,old.owner().controlId());eligible(observed);
            need(prior.classification().equals(observed.classification()),EVIDENCE_CHANGED);
            handoffs.fenced(guard,old.owner(),()->{verification.stage(old);need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,old.owner().controlId())==1,OWNERSHIP_LOST);return null;});
            handoffs.verificationReview(guard,bootstrap,review,old.owner());handoffs.verificationChild(old.child());
            var pack=handoffs.verificationPack(guard,bootstrap,old.owner());var runtime=handoffs.verificationRuntime(bootstrap);
            var before=RecoveryNativeSeal.capture(runtime,pack,old.fingerprint());
            var fresh=verification.requalify(guard,bootstrap,old,review);var facts=verification.inspect(guard,old.owner().controlId(),1);eligible(facts);
            need(facts.classification().equals(observed.classification()) && facts.evidence().get("code").equals(observed.evidence().get("code")) && facts.evidence().get("publication").equals(observed.evidence().get("publication")),EVIDENCE_CHANGED);
            var after=RecoveryNativeSeal.capture(runtime,pack,old.fingerprint());need(before.same(after),EVIDENCE_CHANGED);
            handoffs.verificationReview(guard,bootstrap,review,fresh.stage().owner());handoffs.verificationChild(fresh.stage().child());
            var decision=decision(facts,after,old.fingerprint());var anchors=handoffs.fenced(guard,fresh.stage().owner(),()->{verification.stage(old);verification.stage(fresh.stage());return List.of(anchor(old),anchor(fresh.stage()));});
            return new Qualified(bootstrap,old,fresh.stage(),after,pack,facts.evidence(),capturePrivate(guard,fresh.stage(),pack),handoffs.verificationNow(),decision,anchors.get(1),anchors.get(0));
        }catch(RecoveryFailure | ManagementFailure | ImportOwnershipFailure | PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new RecoveryFailure(EVIDENCE_CHANGED);}
    }
    private static void eligible(RecoveryVerification.Inspection facts) {
        need(facts.evidence()!=null && Set.of("VERIFIED","FAILED").contains(facts.state()) && Set.of("VERIFIED_PUBLICATION","OUTPUT_ABSENT","OUTPUT_INCOMPLETE","OUTPUT_INVALID","OUTPUT_CHANGED","DEPENDENCY_CHANGED","DEPENDENCY_INVALID").contains(facts.classification()) && facts.evidence().get("readiness").stringValue().equals("READY"),EVIDENCE_UNAVAILABLE);
    }
    private static Decision decision(RecoveryVerification.Inspection facts,RecoveryNativeSeal seal,String fingerprint) {
        String code=facts.evidence().get("code").isNull()?null:facts.evidence().get("code").stringValue();
        return switch(facts.classification()) {
            case "VERIFIED_PUBLICATION" -> new Decision("SUCCEEDED","RECOVERED_PUBLICATION",false);
            case "OUTPUT_ABSENT" -> {need(code==null,EVIDENCE_CHANGED);yield new Decision("FAILED","PUBLICATION_ABSENT",true);}
            case "OUTPUT_INCOMPLETE" -> {need(code==null,EVIDENCE_CHANGED);yield new Decision("FAILED","INCOMPLETE_OUTPUT",false);}
            case "OUTPUT_INVALID" -> {need("PUBLICATION_INVALID".equals(code) && seal.demonstrablyInvalid(fingerprint),EVIDENCE_UNAVAILABLE);yield new Decision("FAILED","PUBLICATION_INVALID",false);}
            case "OUTPUT_CHANGED" -> {need("PUBLICATION_CHANGED".equals(code),EVIDENCE_UNAVAILABLE);yield new Decision("FAILED","PUBLICATION_CHANGED",false);}
            case "DEPENDENCY_CHANGED" -> {need(Set.of("EVIDENCE_CHANGED","INPUT_CHANGED","SNAPSHOT_CHANGED","DEPENDENCY_CHANGED").contains(code),EVIDENCE_UNAVAILABLE);yield new Decision("FAILED","DEPENDENCY_CHANGED",false);}
            case "DEPENDENCY_INVALID" -> {need(Set.of("BATCH_INVALID","SNAPSHOT_INVALID","DEPENDENCY_INVALID").contains(code),EVIDENCE_UNAVAILABLE);yield new Decision("FAILED","DEPENDENCY_INVALID",false);}
            default -> throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);
        };
    }
    Receipt finish(InstallationHostGuard guard,Qualified q,RecoveryQuiescence.Review review) {
        try {
            outside(guard);need(q!=null,INVALID_REQUEST);
            var existing=resolve(guard,q.stage.owner().controlId(),q.checkpoint(),q.stage.owner().qualificationSha256(),q);
            if(existing.status()!=Status.NOT_FINALIZED)return existing;
            var owner=q.stage.owner();RecoveryQuiescence.fresh(q.qualifiedAt,handoffs.verificationNow());
            handoffs.verificationReview(guard,q.bootstrap,review,owner);handoffs.verificationChild(q.original.child());handoffs.verificationChild(q.stage.child());
            var pack=handoffs.verificationPack(guard,q.bootstrap,owner);need(pack.equals(q.pack),EVIDENCE_CHANGED);
            need(verification.inspect(guard,owner.controlId(),1).evidence().equals(q.evidence) && capturePrivate(guard,q.stage,q.pack).equals(q.privateSeal),EVIDENCE_CHANGED);
            var seal=RecoveryNativeSeal.capture(handoffs.verificationRuntime(q.bootstrap),q.pack,q.stage.fingerprint());need(q.nativeSeal.same(seal),EVIDENCE_CHANGED);
            handoffs.verificationReview(guard,q.bootstrap,review,owner);handoffs.verificationChild(q.stage.child());
            RecoveryQuiescence.fresh(q.qualifiedAt,handoffs.verificationNow());
            return handoffs.fenced(guard,owner,()->apply(guard,q,review));
        }catch(RecoveryFailure | ManagementFailure | ImportOwnershipFailure | PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new RecoveryFailure(EVIDENCE_CHANGED);}
    }
    private Receipt apply(InstallationHostGuard guard,Qualified q,RecoveryQuiescence.Review review) {
        var s=q.stage;var o=s.owner();verification.stage(q.original);verification.stage(s);handoffs.verificationFresh(review);RecoveryQuiescence.fresh(q.qualifiedAt,handoffs.verificationNow());
        need(anchor(s).equals(q.anchorSha) && anchor(q.original).equals(q.originalAnchorSha),EVIDENCE_CHANGED);
        need(!jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND type='CANCEL' AND state IN ('REQUESTED','APPLIED'))",Boolean.class,o.runId()),EVIDENCE_UNAVAILABLE);
        need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,o.controlId())==2,EVIDENCE_CHANGED);
        need(AuditCapacity.available(jdbc,5,jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class)),CAPACITY_EXCEEDED);
        boolean retry=q.decision.retry() && jdbc.queryForObject("SELECT max(attempt_no)<20 FROM ddip_management.attempts WHERE run_id=?",Boolean.class,o.runId());
        UUID artifact=null;String artifactSha=null;
        if(q.decision.state().equals("SUCCEEDED")) {
            var publication=q.evidence.get("publication");byte[] bytes=SourceJson.canonical(publication);artifactSha=SourceJson.sha(bytes);artifact=register(q,publication,bytes,artifactSha);
            jdbc.update("INSERT INTO ddip_management.stage_artifacts VALUES (?,'VERIFY_RESULT',1,'RESULT',0,?)",o.controlId(),artifact);fault.accept(Step.LINK);
        }
        String attemptCode="RECONCILED_"+q.decision.state();
        exactly(jdbc.update("UPDATE ddip_management.attempts SET state='SUCCEEDED',finished_at=clock_timestamp(),outcome_code=?,row_version=row_version+1 WHERE attempt_id=? AND run_id=? AND kind='RECONCILE' AND state='RUNNING' AND instance_id=? AND owner_epoch=? AND row_version=?",attemptCode,o.controlId(),o.runId(),o.worker().instanceId(),o.worker().epoch(),o.attemptVersion()));fault.accept(Step.ATTEMPT);
        byte[] counts=artifact==null?null:SourceJson.canonical(q.evidence.get("publication").get("counts"));
        exactly(jdbc.update("UPDATE ddip_management.runs SET state=?,finished_at=clock_timestamp(),result_artifact_id=?,counts_bytes=?,outcome_code=?,retry_eligible=?,row_version=row_version+1 WHERE run_id=? AND state='RECOVERY_REQUIRED' AND current_attempt_id=? AND row_version=?",q.decision.state(),artifact,counts,q.decision.code(),retry,o.runId(),o.controlId(),o.runVersion()));fault.accept(Step.RUN);
        exactly(jdbc.update("UPDATE ddip_management.controls SET state='APPLIED',reason=?,applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=? AND run_id=? AND type='RECONCILE' AND state='REQUESTED' AND target_attempt_id=? AND row_version=?",q.decision.code(),o.controlId(),o.runId(),o.predecessor(),o.controlVersion()));fault.accept(Step.CONTROL);
        exactly(jdbc.update("UPDATE ddip_management.worker_instances SET status='STOPPED',row_version=row_version+1 WHERE instance_id=? AND installation_id=? AND status='RECOVERY_ONLY' AND row_version=?",o.worker().instanceId(),o.worker().installationId(),o.worker().workerVersion()));fault.accept(Step.WORKER);
        exactly(jdbc.update("UPDATE ddip_management.coordinator SET mode='READY',active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND mode='RECOVERY_ONLY' AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL",o.worker().installationId(),o.controlId(),o.worker().instanceId(),o.worker().epoch(),o.worker().coordinatorVersion()));fault.accept(Step.COORDINATOR);
        var details=details(q,artifact,artifactSha,retry,reviewSummary(review));byte[] raw=SourceJson.canonical(details);need(raw.length<=8192,CAPACITY_EXCEEDED);
        audit(o,"RECOVERY_ATTEMPT_FINISHED","RUNNING","SUCCEEDED",o.attemptVersion(),o.attemptVersion()+1,attemptCode,raw);
        audit(o,"RECOVERY_RUN_FINISHED","RECOVERY_REQUIRED",q.decision.state(),o.runVersion(),o.runVersion()+1,q.decision.code(),raw);
        audit(o,"RECOVERY_CONTROL_APPLIED","REQUESTED","APPLIED",o.controlVersion(),o.controlVersion()+1,q.decision.code(),raw);
        audit(o,"RECOVERY_SLOT_RELEASED","RECOVERY_ONLY","READY",o.worker().coordinatorVersion(),o.worker().coordinatorVersion()+1,q.decision.code(),raw);
        audit(o,"RECOVERY_FINALIZED","RECOVERY_REQUIRED",q.decision.state(),o.runVersion(),o.runVersion()+1,q.decision.code(),raw);fault.accept(Step.AUDIT);
        guard.checkHeld(o.worker().installationId());handoffs.verificationFresh(review);RecoveryQuiescence.fresh(q.qualifiedAt,handoffs.verificationNow());
        return new Receipt(Status.CONFIRMED,o.runId(),o.controlId(),q.decision.state(),q.decision.code(),artifact,retry);
    }
    private UUID register(Qualified q,JsonNode e,byte[] bytes,String sha) {
        var s=q.stage;need(e.get("nativeFingerprint").stringValue().equals(s.fingerprint()) && e.get("kind").stringValue().equals("RELIABILITY_V2"),EVIDENCE_CHANGED);
        String format=e.get("inputFormat").stringValue(),binding=e.get("storageBindingId").stringValue(),address=e.get("relativeAddress").stringValue();
        var rows=jdbc.queryForList("SELECT artifact_id,storage_binding_id,relative_address,evidence_sha256,evidence_bytes,native_identity,native_format FROM ddip_management.artifacts WHERE (product_id='application-reliability' AND kind='RELIABILITY_V2' AND contract_version=2 AND processor_revision=2 AND native_format=? AND native_identity=?) OR (storage_binding_id=? AND relative_address=?) FOR UPDATE",format,s.fingerprint(),binding,address);
        UUID artifact;
        var inputs=jdbc.queryForList("SELECT role,ordinal,artifact_id FROM ddip_management.run_inputs WHERE run_id=? ORDER BY role,ordinal",s.owner().runId());
        if(rows.isEmpty()) {
            artifact=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at,native_format) VALUES (?,'RELIABILITY_V2','application-reliability',2,2,?,?,?,?,?,clock_timestamp(),?)",artifact,s.fingerprint(),binding,address,bytes,sha,format);fault.accept(Step.ARTIFACT);
            for(var input:inputs){jdbc.update("INSERT INTO ddip_management.artifact_dependencies VALUES (?,?,?,?)",artifact,input.get("role"),input.get("ordinal"),input.get("artifact_id"));fault.accept(Step.DEPENDENCY);}
        }else {
            var r=one(rows);need(binding.equals(r.get("storage_binding_id")) && address.equals(r.get("relative_address")) && s.fingerprint().equals(r.get("native_identity")) && format.equals(r.get("native_format")) && sha.equals(r.get("evidence_sha256")) && Arrays.equals(bytes,(byte[])r.get("evidence_bytes")),EVIDENCE_CHANGED);artifact=(UUID)r.get("artifact_id");
            need(inputs.equals(jdbc.queryForList("SELECT role,ordinal,dependency_artifact_id AS artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? ORDER BY role,ordinal",artifact)),EVIDENCE_CHANGED);
        }
        return artifact;
    }
    private JsonNode details(Qualified q,UUID artifact,String artifactSha,boolean retry,JsonNode review) {
        var o=q.stage.owner();var w=o.worker();var n=SourceJson.object().put("policy",POLICY).put("installationId",w.installationId().toString()).put("runId",o.runId().toString()).put("controlId",o.controlId().toString()).put("predecessor",o.predecessor().toString()).put("instanceId",w.instanceId().toString()).put("epoch",Long.toString(w.epoch())).put("qualificationSha256",o.qualificationSha256()).put("checkpointId",q.checkpoint().toString()).put("originalCheckpointId",q.original.checkpoint().toString()).put("stageVersion",Long.toString(q.stage.version())).put("commandSha256",q.stage.commandSha()).put("stageResultSha256",q.anchorSha).put("originalResultSha256",q.originalAnchorSha).put("runVersion",Long.toString(o.runVersion())).put("attemptVersion",Long.toString(o.attemptVersion())).put("controlVersion",Long.toString(o.controlVersion())).put("coordinatorVersion",Long.toString(w.coordinatorVersion())).put("workerVersion",Long.toString(w.workerVersion())).put("nativeSealSha256",q.nativeSeal.digest()).put("privateSealSha256",SourceJson.sha(SourceJson.canonical(q.privateSeal))).put("nativeEvidenceSha256",SourceJson.sha(SourceJson.canonical(q.evidence))).put("classification",q.evidence.get("classification").stringValue()).put("runState",q.decision.state()).put("code",q.decision.code()).put("retryEligible",retry);
        n.set("finalReview",review);n.set("nativeCode",q.evidence.get("code"));if(artifact==null)n.putNull("artifactId").putNull("artifactEvidenceSha256");else n.put("artifactId",artifact.toString()).put("artifactEvidenceSha256",artifactSha);return n;
    }
    private JsonNode capturePrivate(InstallationHostGuard guard,RecoveryVerification.Stage stage,JsonNode pack) {
        var owner=stage.owner();var rows=handoffs.fenced(guard,owner,()->jdbc.queryForList("SELECT stage,item_ordinal,checkpoint_id,state,intended_identity,input_evidence_sha256,command_address,command_sha256,result_address,result_sha256,child_pid FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY item_ordinal",owner.controlId()));
        need(rows.size()==2,EVIDENCE_CHANGED);return RecoveryFiles.capture(guard,owner.runId(),owner.controlId(),owner.worker().epoch(),pack.get("command").get("capturedConfigSha256").stringValue(),pack.get("command").get("runtime"),rows).inventory();
    }
    private static JsonNode reviewSummary(RecoveryQuiescence.Review review) {
        var json=review.json();return SourceJson.object().put("sha256",SourceJson.sha(SourceJson.canonical(json))).put("targetSha256",review.targetSha256()).put("inspectedAt",review.inspectedAt().toString()).put("handlersStopped",review.handlersStopped()).put("unmanagedWritersStopped",review.unmanagedWritersStopped()).put("orphanReviewComplete",review.orphanReviewComplete()).put("checkpointsSha256",SourceJson.sha(SourceJson.canonical(json.get("reviewedCheckpoints")))).put("descendantsSha256",SourceJson.sha(SourceJson.canonical(json.get("descendants"))));
    }
    private String anchor(RecoveryVerification.Stage s){var r=one(jdbc.queryForList("SELECT result_sha256,result_address FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=? AND checkpoint_id=?",s.owner().controlId(),s.itemOrdinal(),s.checkpoint()));need((s.address()+"/verified.json").equals(r.get("result_address")) && r.get("result_sha256")!=null,EVIDENCE_CHANGED);return (String)r.get("result_sha256");}
    private void audit(RecoveryHandoff.Owner o,String action,String before,String after,long bv,long av,String code,byte[] details){jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,?,?)",o.runId(),o.controlId(),o.controlId(),action,before,after,bv,av,code,details);}
    Receipt resolve(InstallationHostGuard guard,UUID control,UUID checkpoint,String qualificationSha) {
        return resolve(guard,control,checkpoint,qualificationSha,null);
    }
    private Receipt resolve(InstallationHostGuard guard,UUID control,UUID checkpoint,String qualificationSha,Qualified q) {
        outside(guard);need(control!=null && checkpoint!=null && qualificationSha!=null && qualificationSha.matches("[0-9a-f]{64}"),INVALID_REQUEST);
        try {
            var result=tx.required(()->{
                var c=one(jdbc.queryForList("SELECT installation_id,mode,epoch,active_attempt_id,owner_instance_id,active_import_id,row_version FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE"));need(guard.binding().installationId().equals(c.get("installation_id")),OWNERSHIP_LOST);
                var ids=jdbc.queryForList("SELECT run_id FROM ddip_management.controls WHERE control_id=? AND type='RECONCILE'",control);if(ids.size()!=1)return unresolved(control);
                UUID run=(UUID)ids.getFirst().get("run_id");
                var r=one(jdbc.queryForList("SELECT state,current_attempt_id,row_version,result_artifact_id,outcome_code,retry_eligible,counts_bytes,finished_at FROM ddip_management.runs WHERE run_id=? FOR UPDATE",run));
                var attempts=jdbc.queryForList("SELECT kind,state,run_id,instance_id,owner_epoch,row_version,outcome_code,finished_at FROM ddip_management.attempts WHERE attempt_id=? FOR UPDATE",control);if(attempts.size()!=1)return unresolved(control);var a=attempts.getFirst();
                var k=one(jdbc.queryForList("SELECT state,reason,target_attempt_id,row_version,applied_at FROM ddip_management.controls WHERE control_id=? FOR UPDATE",control));
                var stages=jdbc.queryForList("SELECT checkpoint_id,state,row_version,command_sha256,result_sha256 FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=1 FOR UPDATE",control);
                var completions=jdbc.queryForList("SELECT details_bytes FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND control_id=? AND action='RECOVERY_FINALIZED' ORDER BY event_seq LIMIT 2",run,control,control);
                if(completions.isEmpty()) {
                    if(q==null || stages.size()!=1)return unresolved(control);var s=stages.getFirst();var o=q.stage.owner();var w=o.worker();
                    boolean pending="RECOVERY_ONLY".equals(c.get("mode")) && control.equals(c.get("active_attempt_id")) && w.instanceId().equals(c.get("owner_instance_id")) && c.get("active_import_id")==null && number(c,"epoch")==w.epoch() && number(c,"row_version")==w.coordinatorVersion() && "RECOVERY_REQUIRED".equals(r.get("state")) && control.equals(r.get("current_attempt_id")) && number(r,"row_version")==o.runVersion() && "RECONCILE".equals(a.get("kind")) && "RUNNING".equals(a.get("state")) && run.equals(o.runId()) && w.instanceId().equals(a.get("instance_id")) && number(a,"owner_epoch")==w.epoch() && number(a,"row_version")==o.attemptVersion() && "REQUESTED".equals(k.get("state")) && number(k,"row_version")==o.controlVersion() && o.predecessor().equals(k.get("target_attempt_id")) && checkpoint.equals(s.get("checkpoint_id")) && q.stage.state().equals(s.get("state")) && number(s,"row_version")==q.stage.version() && q.stage.commandSha().equals(s.get("command_sha256")) && q.anchorSha.equals(s.get("result_sha256"));
                    return pending?new Receipt(Status.NOT_FINALIZED,run,control,null,null,null,false):unresolved(control);
                }
                if(completions.size()!=1 || stages.size()!=1)return unresolved(control);
                byte[] raw=(byte[])completions.getFirst().get("details_bytes");var d=PreflightFiles.canonical(raw,8192);var s=stages.getFirst();
                need(POLICY.equals(d.get("policy").stringValue()) && d.get("installationId").stringValue().equals(guard.binding().installationId().toString()) && d.get("runId").stringValue().equals(run.toString()) && d.get("controlId").stringValue().equals(control.toString()) && d.get("checkpointId").stringValue().equals(checkpoint.toString()) && d.get("qualificationSha256").stringValue().equals(qualificationSha),EVIDENCE_CHANGED);
                need("SUCCEEDED".equals(a.get("state")) && "RECONCILE".equals(a.get("kind")) && run.equals(a.get("run_id")) && a.get("instance_id").toString().equals(d.get("instanceId").stringValue()) && number(a,"owner_epoch")==decimal(d,"epoch") && number(a,"row_version")==decimal(d,"attemptVersion")+1 && a.get("outcome_code").equals("RECONCILED_"+d.get("runState").stringValue()) && a.get("finished_at")!=null,EVIDENCE_CHANGED);
                need(control.equals(r.get("current_attempt_id")) && r.get("state").equals(d.get("runState").stringValue()) && Set.of("SUCCEEDED","FAILED").contains(r.get("state")) && number(r,"row_version")==decimal(d,"runVersion")+1 && r.get("outcome_code").equals(d.get("code").stringValue()) && r.get("retry_eligible").equals(d.get("retryEligible").booleanValue()) && r.get("finished_at")!=null,EVIDENCE_CHANGED);
                need("APPLIED".equals(k.get("state")) && k.get("reason").equals(d.get("code").stringValue()) && k.get("target_attempt_id").toString().equals(d.get("predecessor").stringValue()) && number(k,"row_version")==decimal(d,"controlVersion")+1 && k.get("applied_at")!=null,EVIDENCE_CHANGED);
                need(checkpoint.equals(s.get("checkpoint_id")) && number(s,"row_version")==decimal(d,"stageVersion") && s.get("command_sha256").equals(d.get("commandSha256").stringValue()) && s.get("result_sha256").equals(d.get("stageResultSha256").stringValue()) && (r.get("state").equals("SUCCEEDED")?s.get("state").equals("VERIFIED"):s.get("state").equals("FAILED")),EVIDENCE_CHANGED);
                var old=one(jdbc.queryForList("SELECT checkpoint_id,result_sha256 FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=0",control));need(old.get("checkpoint_id").toString().equals(d.get("originalCheckpointId").stringValue()) && old.get("result_sha256").equals(d.get("originalResultSha256").stringValue()),EVIDENCE_CHANGED);
                need(!control.equals(c.get("active_attempt_id")) && number(c,"epoch")>=decimal(d,"epoch") && number(c,"row_version")>=decimal(d,"coordinatorVersion")+1,EVIDENCE_CHANGED);
                var w=one(jdbc.queryForList("SELECT installation_id,status,row_version FROM ddip_management.worker_instances WHERE instance_id=?",a.get("instance_id")));need(guard.binding().installationId().equals(w.get("installation_id")) && "STOPPED".equals(w.get("status")) && number(w,"row_version")==decimal(d,"workerVersion")+1,EVIDENCE_CHANGED);
                var events=jdbc.queryForList("SELECT action,before_state,after_state,before_version,after_version,safe_code,details_bytes FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND control_id=? AND action IN ('RECOVERY_ATTEMPT_FINISHED','RECOVERY_RUN_FINISHED','RECOVERY_CONTROL_APPLIED','RECOVERY_SLOT_RELEASED','RECOVERY_FINALIZED')",run,control,control);
                need(events.size()==5 && events.stream().map(v->v.get("action")).distinct().count()==5,EVIDENCE_CHANGED);
                for(var event:events) {
                    need(Arrays.equals(raw,(byte[])event.get("details_bytes")),EVIDENCE_CHANGED);String action=(String)event.get("action");
                    String before=action.equals("RECOVERY_ATTEMPT_FINISHED")?"RUNNING":action.equals("RECOVERY_CONTROL_APPLIED")?"REQUESTED":action.equals("RECOVERY_SLOT_RELEASED")?"RECOVERY_ONLY":"RECOVERY_REQUIRED";
                    String after=action.equals("RECOVERY_ATTEMPT_FINISHED")?"SUCCEEDED":action.equals("RECOVERY_CONTROL_APPLIED")?"APPLIED":action.equals("RECOVERY_SLOT_RELEASED")?"READY":d.get("runState").stringValue();
                    String version=action.equals("RECOVERY_ATTEMPT_FINISHED")?"attemptVersion":action.equals("RECOVERY_CONTROL_APPLIED")?"controlVersion":action.equals("RECOVERY_SLOT_RELEASED")?"coordinatorVersion":"runVersion";
                    need(before.equals(event.get("before_state")) && after.equals(event.get("after_state")) && number(event,"before_version")==decimal(d,version) && number(event,"after_version")==decimal(d,version)+1 && event.get("safe_code").equals(action.equals("RECOVERY_ATTEMPT_FINISHED")?"RECONCILED_"+d.get("runState").stringValue():d.get("code").stringValue()),EVIDENCE_CHANGED);
                }
                var review=d.get("finalReview");SourceJson.fields(review,"sha256","targetSha256","inspectedAt","handlersStopped","unmanagedWritersStopped","orphanReviewComplete","checkpointsSha256","descendantsSha256");
                for(String key:List.of("sha256","targetSha256","checkpointsSha256","descendantsSha256"))need(review.get(key).stringValue().matches("[0-9a-f]{64}"),EVIDENCE_CHANGED);
                need(review.get("handlersStopped").booleanValue() && review.get("unmanagedWritersStopped").booleanValue() && review.get("orphanReviewComplete").booleanValue(),EVIDENCE_CHANGED);
                var handoff=one(jdbc.queryForList("SELECT details_bytes FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND control_id=? AND action='RECOVERY_HANDOFF'",run,control,control));need(SourceJson.parse((byte[])handoff.get("details_bytes")).get("snapshotSha256").equals(review.get("targetSha256")),EVIDENCE_CHANGED);
                RecoveryQuiescence.fresh(Instant.parse(review.get("inspectedAt").stringValue()),((Timestamp)r.get("finished_at")).toInstant());
                UUID artifact=d.get("artifactId").isNull()?null:UUID.fromString(d.get("artifactId").stringValue());need(Objects.equals(artifact,r.get("result_artifact_id")),EVIDENCE_CHANGED);
                if(artifact!=null) {
                    var ar=one(jdbc.queryForList("SELECT evidence_bytes,evidence_sha256 FROM ddip_management.artifacts WHERE artifact_id=? AND kind='RELIABILITY_V2'",artifact));need(ar.get("evidence_sha256").equals(d.get("artifactEvidenceSha256").stringValue()) && SourceJson.sha((byte[])ar.get("evidence_bytes")).equals(ar.get("evidence_sha256")),EVIDENCE_CHANGED);
                    var evidence=SourceJson.parse((byte[])ar.get("evidence_bytes"),PreflightFiles.LARGE);need(Arrays.equals((byte[])r.get("counts_bytes"),SourceJson.canonical(evidence.get("counts"))),EVIDENCE_CHANGED);
                    need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_artifacts WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=1 AND role='RESULT' AND ordinal=0 AND artifact_id=?",Long.class,control,artifact)==1,EVIDENCE_CHANGED);
                    need(jdbc.queryForList("SELECT role,ordinal,dependency_artifact_id AS artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? ORDER BY role,ordinal",artifact).equals(jdbc.queryForList("SELECT role,ordinal,artifact_id FROM ddip_management.run_inputs WHERE run_id=? ORDER BY role,ordinal",run)),EVIDENCE_CHANGED);
                }else need(r.get("state").equals("FAILED") && r.get("counts_bytes")==null && jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_artifacts WHERE attempt_id=?",Long.class,control)==0,EVIDENCE_CHANGED);
                if(q!=null)need(d.equals(details(q,artifact,artifact==null?null:d.get("artifactEvidenceSha256").stringValue(),d.get("retryEligible").booleanValue(),d.get("finalReview"))),EVIDENCE_CHANGED);
                return new Receipt(Status.CONFIRMED,run,control,(String)r.get("state"),(String)r.get("outcome_code"),artifact,(Boolean)r.get("retry_eligible"));
            });guard.checkHeld(guard.binding().installationId());return result;
        }catch(ManagementFailure | RecoveryFailure | PreflightFailure ex){return unresolved(control);}catch(RuntimeException ex){return unresolved(control);}
    }
    private static Receipt unresolved(UUID control){return new Receipt(Status.UNRESOLVED,null,control,null,null,null,false);}
    private static void outside(InstallationHostGuard guard){need(guard!=null && !TransactionSynchronizationManager.isActualTransactionActive(),INVALID_REQUEST);guard.checkHeld(guard.binding().installationId());}
    private static long number(Map<String,Object> row,String key){return ((Number)row.get(key)).longValue();}
    private static long decimal(JsonNode n,String key){return Long.parseLong(n.get(key).stringValue());}
    private static Map<String,Object> one(List<Map<String,Object>> rows){need(rows.size()==1,EVIDENCE_CHANGED);return rows.getFirst();}
    private static void exactly(int count){need(count==1,OWNERSHIP_LOST);}
}
