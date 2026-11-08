package platform.contentapi.execution;

import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Direct original Content safe-end. Package-private, trusted-local only; never creates a live owner. */
final class NarrowContentTerminal {
    static final String POLICY="ddip-content-original-safe-end-v1", CODE="NATIVE_CONTENT_INTERRUPTED";
    enum Boundary { BEFORE_TRANSACTION, RECEIPT, ORIGINAL, ATTEMPT, RUN, CONTROL, WORKER, COORDINATOR, AUDIT, COMMITTED }
    enum Status { COMMITTED, CONFIRMED_TERMINAL, CONFIRMED_ROLLBACK, UNRESOLVED }
    record Receipt(Status status,UUID terminalId,UUID runId,String runState,String code) {}
    /** Each field is a positive operator attestation, not a conclusion inferred from PID absence. */
    record Review(RecoveryQuiescence.Review quiescence,UUID nonce,boolean sameHostAndProcessNamespace,
                  boolean descendantsReviewed,boolean launchGapsReviewed,boolean sparkGatewaysReviewed,
                  boolean remoteWritersStopped) {
        Review { need(quiescence!=null && nonce!=null,INVALID_REQUEST); }
        JsonNode json(){var n=(ObjectNode)quiescence.json();return n.put("reviewVersion",1).put("nonce",nonce.toString())
            .put("sameHostAndProcessNamespace",sameHostAndProcessNamespace).put("descendantsReviewed",descendantsReviewed)
            .put("launchGapsReviewed",launchGapsReviewed).put("sparkGatewaysReviewed",sparkGatewaysReviewed).put("remoteWritersStopped",remoteWritersStopped);}
        @Override public String toString(){return "ContentTerminalReview[redacted]";}
    }
    static final class Target {
        private final UUID id,run,original,terminal,control,worker,nonce;
        private final JsonNode snapshot;private final String inventorySha256;
        private final NarrowContentTerminalEvidence.Snapshot evidence;
        private Target(UUID id,UUID run,UUID original,UUID terminal,UUID control,UUID worker,JsonNode snapshot,NarrowContentTerminalEvidence.Snapshot evidence,String inventorySha256,UUID nonce){
            this.id=id;this.run=run;this.original=original;this.terminal=terminal;this.control=control;this.worker=worker;this.snapshot=snapshot.deepCopy();this.evidence=evidence;this.inventorySha256=inventorySha256;this.nonce=nonce;
        }
        UUID nonce(){return nonce;}
        String digest(){return hash(target(this));}
        Set<UUID> checkpoints(){return evidence.checkpoints();}
        List<RecoveryQuiescence.Identity> processes(){return evidence.processes();}
        @Override public String toString(){return "ContentTerminalTarget[redacted]";}
    }
    static final class Qualified {
        private final Target target;private final Review review;private final AtomicBoolean used=new AtomicBoolean();
        private Qualified(Target t,Review r){target=t;review=r;}
        @Override public String toString(){return "ContentTerminalQualification[redacted]";}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    private final Clock clock;private final RecoveryQuiescence.Processes processes;private final Consumer<Boundary> fault;
    NarrowContentTerminal(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation){this(jdbc,tx,installation,Clock.systemUTC(),RecoveryQuiescence.LOCAL,b->{});}
    NarrowContentTerminal(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,RecoveryQuiescence.Processes processes,Consumer<Boundary> fault){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.installation=Objects.requireNonNull(installation);this.clock=Objects.requireNonNull(clock);this.processes=Objects.requireNonNull(processes);this.fault=Objects.requireNonNull(fault);
    }
    Target inspect(InstallationHostGuard guard,UUID run,UUID original,UUID control,UUID terminal,UUID id){
        outside();need(guard!=null && run!=null && original!=null && control!=null && terminal!=null && id!=null,INVALID_REQUEST);
        need(new HashSet<>(List.of(run,original,control,terminal,id)).size()==5,INVALID_REQUEST);guard.checkHeld(installation);
        var t=tx.required(()->{
            lock();guard.checkLeaseHeld(installation);var s=snapshot(run,original,control,guard.process().instanceId());pending(guard,s);
            need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_original_terminal_receipts WHERE terminal_id=? OR run_id=?",Long.class,id,run)==0,OWNERSHIP_LOST);
            var e=NarrowContentTerminalEvidence.read(jdbc,tx,run,original);return new Target(id,run,original,terminal,control,guard.process().instanceId(),s,e,null,UUID.randomUUID());
        });
        var observation=t.evidence.verify(guard);guard.checkHeld(installation);return new Target(t.id,t.run,t.original,t.terminal,t.control,t.worker,t.snapshot,t.evidence,observation.inventorySha256(),t.nonce);
    }
    Qualified qualify(InstallationHostGuard guard,Target t,Review r){
        outside();need(t!=null,INVALID_REQUEST);guard.checkHeld(installation);review(t,r);
        tx.required(()->{unchanged(guard,t);return null;});intact(guard,t);guard.checkHeld(installation);review(t,r);bounded(r.json(),8192);
        return new Qualified(t,r);
    }
    Receipt finish(InstallationHostGuard guard,Qualified q){
        outside();need(q!=null,INVALID_REQUEST);
        // Consumption precedes all possible lifecycle mutations and cannot be reset after failure.
        need(q.used.compareAndSet(false,true),FRESH_QUALIFICATION_REQUIRED);var t=q.target;
        guard.checkHeld(installation);review(t,q.review);intact(guard,t);guard.checkHeld(installation);review(t,q.review);
        fault.accept(Boundary.BEFORE_TRANSACTION);
        try {
            var result=tx.required(()->{
                unchanged(guard,t);RecoveryQuiescence.fresh(q.review.quiescence().inspectedAt(),clock.instant());
                long unfinished=jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);
                need(AuditCapacity.available(jdbc,6,unfinished),CAPACITY_EXCEEDED);var s=t.snapshot;
                // Enroll the exact OLD snapshot before lifecycle writes. Immediate SQL validation
                // cannot be bypassed by flushing deferred OLD-row triggers before enrollment.
                // The receipt and terminal attempt remain uncommittable until the full disposition.
                byte[] target=bounded(target(t),8192),review=bounded(q.review.json(),8192),snapshot=bounded(s,65536);
                jdbc.update("INSERT INTO ddip_management.content_original_terminal_receipts(terminal_id,identity_version,policy,run_id,control_id,original_attempt_id,terminal_attempt_id,original_instance_id,terminal_instance_id,original_epoch,original_coordinator_version,original_run_version,original_attempt_version,original_worker_version,terminal_worker_version,control_version,target_bytes,target_sha256,review_bytes,review_sha256,snapshot_bytes,snapshot_sha256) VALUES (?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t.id,POLICY,t.run,t.control,t.original,t.terminal,UUID.fromString(str(s.get("oldWorker"),"instance_id")),t.worker,num(s,"coordinator","epoch"),num(s,"coordinator","row_version"),num(s,"run","row_version"),num(s,"attempt","row_version"),num(s,"oldWorker","row_version"),num(s,"terminalWorker","row_version"),num(s,"control","row_version"),target,SourceJson.sha(target),review,SourceJson.sha(review),snapshot,SourceJson.sha(snapshot));fault.accept(Boundary.RECEIPT);
                exactly(jdbc.update("UPDATE ddip_management.attempts SET state='INTERRUPTED',outcome_code='INTERRUPTED',finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND state='CLAIMED' AND row_version=?",t.original,num(s,"attempt","row_version")));fault.accept(Boundary.ORIGINAL);
                int next=1;for(var a:s.get("history"))next=Math.max(next,a.get("attempt_no").intValue()+1);need(next<=20,CAPACITY_EXCEEDED);
                jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,outcome_code,started_at,finished_at,content_terminal_policy) VALUES (?,?,?,'RECONCILE',?,?,'SUCCEEDED','RECONCILED_FAILED',clock_timestamp(),clock_timestamp(),?)",t.terminal,t.run,next,t.worker,num(s,"coordinator","epoch")+1,POLICY);fault.accept(Boundary.ATTEMPT);
                exactly(jdbc.update("UPDATE ddip_management.runs SET state='FAILED',outcome_code=?,retry_eligible=false,current_attempt_id=?,current_stage=NULL,result_artifact_id=NULL,counts_bytes=NULL,finished_at=clock_timestamp(),row_version=row_version+1 WHERE run_id=? AND state='RUNNING' AND current_attempt_id=? AND row_version=?",CODE,t.terminal,t.run,t.original,num(s,"run","row_version")));fault.accept(Boundary.RUN);
                exactly(jdbc.update("UPDATE ddip_management.controls SET state='APPLIED',reason=?,applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=? AND state='REQUESTED' AND row_version=?",CODE,t.control,num(s,"control","row_version")));fault.accept(Boundary.CONTROL);
                exactly(jdbc.update("UPDATE ddip_management.worker_instances SET status='STOPPED',row_version=row_version+1 WHERE instance_id=? AND status='RECOVERY_ONLY' AND row_version=?",t.worker,num(s,"terminalWorker","row_version")));fault.accept(Boundary.WORKER);
                exactly(jdbc.update("UPDATE ddip_management.coordinator SET mode='READY',active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,epoch=epoch+1,row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND mode='READY' AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL",installation,t.original,UUID.fromString(str(s.get("oldWorker"),"instance_id")),num(s,"coordinator","epoch"),num(s,"coordinator","row_version")));fault.accept(Boundary.COORDINATOR);
                for(var a:audits(t)){
                    jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,?,?)",t.run,t.terminal,t.control,a.action(),a.before(),a.after(),a.version(),a.version()==null?0:a.version()+1,CODE,bounded(details(t),8192));fault.accept(Boundary.AUDIT);
                }

                guard.checkLeaseHeld(installation);RecoveryQuiescence.fresh(q.review.quiescence().inspectedAt(),clock.instant());return receipt(Status.COMMITTED,t);
            });
            // Any exception after the transaction may represent a committed disposition. Never report rollback from this catch.
            fault.accept(Boundary.COMMITTED);guard.checkHeld(installation);RecoveryQuiescence.fresh(q.review.quiescence().inspectedAt(),clock.instant());return result;
        } catch(RuntimeException ex){return resolve(q);}
    }
    Receipt resolve(Qualified q){
        outside();need(q!=null,INVALID_REQUEST);var t=q.target;
        try{return tx.required(()->{
            lock();var ids=jdbc.queryForList("SELECT terminal_id,target_sha256 FROM ddip_management.content_original_terminal_receipts WHERE terminal_id=? OR run_id=?",t.id,t.run);
            if(!ids.isEmpty())return ids.size()==1 && ids.getFirst().get("terminal_id").equals(t.id) && t.digest().equals(ids.getFirst().get("target_sha256"))?lookupLocked(t.id):receipt(Status.UNRESOLVED,t);
            var current=snapshot(t.run,t.original,t.control,t.worker);
            boolean same=current.equals(t.snapshot) && jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE run_id=? AND safe_code=?",Long.class,t.run,CODE)==0;
            return receipt(same?Status.CONFIRMED_ROLLBACK:Status.UNRESOLVED,t);
        });}catch(RuntimeException ex){return receipt(Status.UNRESOLVED,t);}
    }
    Receipt lookup(UUID id){outside();need(id!=null,INVALID_REQUEST);try{return tx.required(()->{lock();return lookupLocked(id);});}catch(RuntimeException ex){return new Receipt(Status.UNRESOLVED,id,null,null,null);}}
    private Receipt lookupLocked(UUID id){
        var rows=jdbc.queryForList("SELECT * FROM ddip_management.content_original_terminal_receipts WHERE terminal_id=?",id);need(rows.size()==1,EVIDENCE_UNAVAILABLE);var row=rows.getFirst();
        var s=canonical(row,"snapshot",65536);var target=canonical(row,"target",8192);var review=canonical(row,"review",8192);
        SourceJson.fields(review,"targetSha256","inspectedAt","handlersStopped","unmanagedWritersStopped","orphanReviewComplete","reviewedCheckpoints","descendants","reviewVersion","nonce","sameHostAndProcessNamespace","descendantsReviewed","launchGapsReviewed","sparkGatewaysReviewed","remoteWritersStopped");
        need(review.get("descendants").isArray() && review.get("descendants").size()<=32,EVIDENCE_CHANGED);var descendants=new HashSet<RecoveryQuiescence.Identity>();
        for(var p:review.get("descendants")){SourceJson.fields(p,"pid","startedAt");need(descendants.add(new RecoveryQuiescence.Identity(Long.parseLong(str(p,"pid")),Instant.parse(str(p,"startedAt")))),EVIDENCE_CHANGED);}
        need(jdbc.queryForObject("SELECT ddip_management.content_terminal_receipt_valid(?)",Boolean.class,id),EVIDENCE_CHANGED);
        var ancestry=NarrowContentTerminalEvidence.read(jdbc,tx,(UUID)row.get("run_id"),(UUID)row.get("original_attempt_id"));
        need(target.equals(target(id,(UUID)row.get("run_id"),(UUID)row.get("original_attempt_id"),(UUID)row.get("terminal_attempt_id"),(UUID)row.get("control_id"),hash(s),str(target,"inventorySha256"),UUID.fromString(str(target,"inspectionNonce")))),EVIDENCE_CHANGED);
        need(str(target,"inventorySha256").matches("[0-9a-f]{64}"),EVIDENCE_CHANGED);
        need(str(review,"targetSha256").equals(hash(target)) && review.get("reviewVersion").intValue()==1,EVIDENCE_CHANGED);
        var reviewed=new HashSet<UUID>();for(var value:review.get("reviewedCheckpoints"))need(reviewed.add(UUID.fromString(SourceJson.text(value))),EVIDENCE_CHANGED);need(reviewed.equals(ancestry.checkpoints()),EVIDENCE_CHANGED);need(str(review,"nonce").equals(str(target,"inspectionNonce")),EVIDENCE_CHANGED);UUID.fromString(str(review,"nonce"));Instant.parse(str(review,"inspectedAt"));
        for(String key:List.of("handlersStopped","unmanagedWritersStopped","orphanReviewComplete","sameHostAndProcessNamespace","descendantsReviewed","launchGapsReviewed","sparkGatewaysReviewed","remoteWritersStopped"))need(review.get(key).isBoolean() && review.get(key).booleanValue(),EVIDENCE_CHANGED);
        return new Receipt(Status.CONFIRMED_TERMINAL,id,(UUID)row.get("run_id"),"FAILED",CODE);
    }
    private void unchanged(InstallationHostGuard guard,Target t){
        lock();guard.checkLeaseHeld(installation);var actual=snapshot(t.run,t.original,t.control,t.worker);pending(guard,actual);need(actual.equals(t.snapshot),OWNERSHIP_LOST);
        var evidence=NarrowContentTerminalEvidence.read(jdbc,tx,t.run,t.original);need(evidence.digest().equals(t.evidence.digest()),EVIDENCE_CHANGED);
    }
    private JsonNode snapshot(UUID run,UUID original,UUID control,UUID worker){
        String value=jdbc.queryForObject("SELECT ddip_management.content_terminal_snapshot(?,?,?,?)::text",String.class,run,original,control,worker);
        var s=SourceJson.parse(value.getBytes(java.nio.charset.StandardCharsets.UTF_8),65536);bounded(s,65536);return s;
    }
    private void pending(InstallationHostGuard guard,JsonNode s){
        var c=s.get("coordinator");var r=s.get("run");var a=s.get("attempt");var k=s.get("control");var w=s.get("terminalWorker");var old=s.get("oldWorker");
        need(str(c,"installation_id").equals(installation.toString()) && str(c,"mode").equals("READY") && c.get("active_import_id").isNull()
            && str(c,"active_attempt_id").equals(str(a,"attempt_id")) && str(c,"owner_instance_id").equals(str(old,"instance_id")) && num(c,"epoch")==num(a,"owner_epoch"),OWNERSHIP_LOST);
        need(str(r,"state").equals("RUNNING") && str(r,"current_attempt_id").equals(str(a,"attempt_id")) && r.get("finished_at").isNull() && r.get("result_artifact_id").isNull() && r.get("counts_bytes").isNull()
            && str(a,"state").equals("CLAIMED") && !a.get("content_admission_policy").isNull() && a.get("content_recovery_policy").isNull() && a.get("content_terminal_policy").isNull()
            && str(old,"status").equals("READY") && str(w,"status").equals("RECOVERY_ONLY") && !str(w,"instance_id").equals(str(old,"instance_id")),OWNERSHIP_LOST);
        need(str(r,"pipeline").equals("CONTENT_RSS_V1") && str(a,"kind").equals("EXECUTE") && str(a,"content_admission_policy").equals("ddip-content-narrow-v1")
            && a.get("reused_checkpoint_id").isNull() && a.get("finished_at").isNull() && a.get("outcome_code").isNull() && str(a,"run_id").equals(str(r,"run_id")) && str(a,"instance_id").equals(str(old,"instance_id")),EVIDENCE_UNAVAILABLE);
        need(str(k,"type").equals("RECONCILE") && str(k,"state").equals("REQUESTED") && str(k,"run_id").equals(str(r,"run_id")) && str(k,"target_attempt_id").equals(str(a,"attempt_id"))
            && k.get("quiescence_confirmed").isBoolean() && k.get("quiescence_confirmed").booleanValue() && str(k,"requester").equals("local-operator") && num(k,"expected_version")==num(r,"row_version"),OWNERSHIP_LOST);
        need(str(w,"installation_id").equals(installation.toString()) && str(old,"installation_id").equals(installation.toString()) && str(w,"execution_profile_id").equals(str(r,"execution_profile_id")) && str(old,"execution_profile_id").equals(str(r,"execution_profile_id")) && str(w,"profile_sha256").equals(str(old,"profile_sha256")),OWNERSHIP_LOST);
        for(var prior:s.get("history"))if(!str(prior,"attempt_id").equals(str(a,"attempt_id")))need(str(prior,"kind").equals("EXECUTE") && Set.of("FAILED","CANCELLED").contains(str(prior,"state")) && prior.get("content_admission_policy").isNull() && prior.get("content_recovery_policy").isNull() && prior.get("content_terminal_policy").isNull() && prior.get("reused_checkpoint_id").isNull(),EVIDENCE_UNAVAILABLE);
        need(str(w,"instance_id").equals(guard.process().instanceId().toString()) && num(w,"pid")==guard.process().pid() && Instant.parse(str(w,"process_started_at")).equals(guard.process().startedAt()) && str(w,"host_binding_sha256").equals(guard.binding().digest()) && str(old,"host_binding_sha256").equals(guard.binding().digest()),OWNERSHIP_LOST);
        need(s.get("history").size()<20 && s.get("controls").size()<=128,CAPACITY_EXCEEDED);
        for(var control:s.get("controls"))if(!str(control,"control_id").equals(str(k,"control_id")))need(!str(control,"state").equals("REQUESTED") && !(str(control,"type").equals("CANCEL") && str(control,"state").equals("APPLIED")),EVIDENCE_UNAVAILABLE);
    }
    private void intact(InstallationHostGuard guard,Target t){need(t.inventorySha256.equals(t.evidence.verify(guard).inventorySha256()),EVIDENCE_CHANGED);}
    private void review(Target t,Review r){need(r!=null && t.nonce.equals(r.nonce()) && r.sameHostAndProcessNamespace() && r.descendantsReviewed() && r.launchGapsReviewed() && r.sparkGatewaysReviewed() && r.remoteWritersStopped(),QUIESCENCE_REQUIRED);RecoveryQuiescence.check(r.quiescence(),t.digest(),t.checkpoints(),t.processes(),processes,clock.instant());}
    private void lock(){jdbc.queryForList("SELECT singleton_id FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");need("read committed".equals(jdbc.queryForObject("SHOW transaction_isolation",String.class)),INVALID_REQUEST);}
    private record Audit(String action,String before,String after,Long version){}
    private static List<Audit> audits(Target t){var s=t.snapshot;return List.of(new Audit("NATIVE_CONTENT_TERMINAL_ORIGINAL","CLAIMED","INTERRUPTED",num(s,"attempt","row_version")),new Audit("NATIVE_CONTENT_TERMINAL_ATTEMPT",null,"SUCCEEDED",null),new Audit("NATIVE_CONTENT_TERMINAL_RUN","RUNNING","FAILED",num(s,"run","row_version")),new Audit("NATIVE_CONTENT_TERMINAL_CONTROL","REQUESTED","APPLIED",num(s,"control","row_version")),new Audit("NATIVE_CONTENT_TERMINAL_WORKER","RECOVERY_ONLY","STOPPED",num(s,"terminalWorker","row_version")),new Audit("NATIVE_CONTENT_TERMINAL_RELEASE","READY","READY",num(s,"coordinator","row_version")));}
    private static JsonNode details(Target t){var n=target(t);return n.put("targetSha256",t.digest());}
    private static ObjectNode target(Target t){return target(t.id,t.run,t.original,t.terminal,t.control,hash(t.snapshot),t.inventorySha256,t.nonce);}
    private static ObjectNode target(UUID id,UUID run,UUID original,UUID terminal,UUID control,String snapshot,String inventory,UUID nonce){return SourceJson.object().put("identityVersion",1).put("policy",POLICY).put("terminalId",id.toString()).put("runId",run.toString()).put("originalAttemptId",original.toString()).put("terminalAttemptId",terminal.toString()).put("controlId",control.toString()).put("snapshotSha256",snapshot).put("inventorySha256",inventory).put("inspectionNonce",nonce.toString());}
    private static Receipt receipt(Status status,Target t){boolean done=status==Status.COMMITTED || status==Status.CONFIRMED_TERMINAL;return new Receipt(status,t.id,t.run,done?"FAILED":null,done?CODE:null);}
    private static JsonNode canonical(Map<String,Object> row,String field,int limit){byte[] bytes=(byte[])row.get(field+"_bytes");var n=SourceJson.parse(bytes,limit);need(Arrays.equals(bytes,SourceJson.canonical(n)) && SourceJson.sha(bytes).equals(row.get(field+"_sha256")),EVIDENCE_CHANGED);return n;}
    private static byte[] bounded(JsonNode n,int max){byte[] raw=SourceJson.canonical(n);need(raw.length<=max,CAPACITY_EXCEEDED);return raw;}
    private static String hash(JsonNode n){return SourceJson.sha(SourceJson.canonical(n));}
    private static String str(JsonNode n,String key){return SourceJson.text(n.get(key));}
    private static long num(JsonNode n,String key){return Long.parseLong(n.get(key).asText());}
    private static long num(JsonNode n,String key,String field){return num(n.get(key),field);}
    private static void exactly(int n){need(n==1,OWNERSHIP_LOST);}
    private static void outside(){need(!TransactionSynchronizationManager.isActualTransactionActive(),INVALID_REQUEST);}
}
