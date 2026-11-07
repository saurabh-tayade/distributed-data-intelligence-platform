package platform.contentapi.execution;

import java.sql.Timestamp;
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

/** Trusted-local terminal-only disposition. No dispatcher, process launch, owner reconstruction or continuation. */
final class NarrowContentFetchRecoveryFinalization {
    static final String POLICY="ddip-content-fetch-terminal-recovery-v1",CODE="NATIVE_FETCH_INTERRUPTED";
    enum Boundary { BEFORE_FINISH, ATTEMPT, RUN, CONTROL, WORKER, COORDINATOR, AUDIT, RECEIPT, COMMITTED }
    enum Status { COMMITTED, CONFIRMED_TERMINAL, CONFIRMED_ROLLBACK, UNRESOLVED }
    /** Historical facts only; cannot authorize work or repeat a takeover. */
    record Receipt(Status status,UUID finalizationId,UUID handoffId,UUID runId,String runState,String code) { }
    static final class Target {
        private final NarrowContentFetchRecoveryHandoff.Owner owner;
        private final NarrowContentFetchRecoveryHandoff.Snapshot state;
        private final Map<String,Object> handoff;
        private final JsonNode snapshot;
        private final UUID id;
        private Target(NarrowContentFetchRecoveryHandoff.Owner owner,NarrowContentFetchRecoveryHandoff.Snapshot state,Map<String,Object> handoff,JsonNode snapshot,UUID id){this.owner=owner;this.state=state;this.handoff=handoff;this.snapshot=snapshot;this.id=id;}
        String digest(){return hash(target(id,handoff,snapshot));}
        UUID checkpoint(){return state.checkpoint();}
        List<RecoveryQuiescence.Identity> processes(){return state.processes();}
        @Override public String toString(){return "NativeFetchTerminalTarget[redacted]";}
    }
    static final class Qualified {
        private final Target target;private final NarrowContentFetchRecoveryHandoff.Review review;private final AtomicBoolean used=new AtomicBoolean();
        private Qualified(Target target,NarrowContentFetchRecoveryHandoff.Review review){this.target=target;this.review=review;}
        @Override public String toString(){return "NativeFetchTerminalQualification[redacted]";}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    private final Clock clock;private final RecoveryQuiescence.Processes processes;private final Consumer<Boundary> fault;
    private final NarrowContentFetchRecoveryHandoff handoffs;
    NarrowContentFetchRecoveryFinalization(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation){this(jdbc,tx,installation,Clock.systemUTC(),RecoveryQuiescence.LOCAL,b->{});}
    NarrowContentFetchRecoveryFinalization(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,RecoveryQuiescence.Processes processes,Consumer<Boundary> fault){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.installation=Objects.requireNonNull(installation);
        this.clock=Objects.requireNonNull(clock);this.processes=Objects.requireNonNull(processes);this.fault=Objects.requireNonNull(fault);
        this.handoffs=new NarrowContentFetchRecoveryHandoff(jdbc,tx,installation,clock,processes,b->{});
    }
    Target inspect(InstallationHostGuard guard,NarrowContentFetchRecoveryHandoff.Owner owner,UUID id){
        handoffs.terminalGuard(guard,owner);need(id!=null,INVALID_REQUEST);
        return tx.required(()->{
            guard.checkLeaseHeld(installation);var h=handoffs.terminalHandoff(owner);var s=handoffs.terminalRead(h);
            need(!id.equals(h.get("handoff_id")) && !id.equals(s.runId()) && !id.equals(s.oldId()) && !id.equals(h.get("recovery_attempt_id")) && !id.equals(s.controlId()),INVALID_REQUEST);
            validatePending(guard,h,s);need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_fetch_recovery_finalizations WHERE finalization_id=? OR handoff_id=?",Long.class,id,h.get("handoff_id"))==0,OWNERSHIP_LOST);
            var snapshot=snapshot(s);bounded(snapshot,65536);return new Target(owner,s,h,snapshot,id);
        });
    }
    Qualified qualify(InstallationHostGuard guard,Target target,NarrowContentFetchRecoveryHandoff.Review review){
        need(target!=null,INVALID_REQUEST);handoffs.terminalGuard(guard,target.owner);checkReview(target,review);
        tx.required(()->{unchanged(guard,target);return null;});intact(guard,target);checkReview(target,review);
        bounded(review.json(),8192);return new Qualified(target,review);
    }
    Receipt finish(InstallationHostGuard guard,Qualified q){
        outside();need(q!=null,INVALID_REQUEST);
        if(q.used.get()){
            var receipt=resolve(q);if(receipt.status()==Status.CONFIRMED_TERMINAL)return receipt;
            throw new RecoveryFailure(receipt.status()==Status.CONFIRMED_ROLLBACK?FRESH_QUALIFICATION_REQUIRED:HANDOFF_UNRESOLVED);
        }
        var t=q.target;handoffs.terminalGuard(guard,t.owner);checkReview(t,q.review);intact(guard,t);checkReview(t,q.review);
        need(q.used.compareAndSet(false,true),FRESH_QUALIFICATION_REQUIRED);fault.accept(Boundary.BEFORE_FINISH);
        handoffs.terminalGuard(guard,t.owner);
        var result=tx.required(()->{
            guard.checkLeaseHeld(installation);var s=unchanged(guard,t);RecoveryQuiescence.fresh(q.review.quiescence().inspectedAt(),clock.instant());
            long unfinished=jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);
            need(AuditCapacity.available(jdbc,5,unfinished),CAPACITY_EXCEEDED);
            UUID successor=(UUID)t.handoff.get("recovery_attempt_id"),worker=(UUID)t.handoff.get("recovery_instance_id");
            var a=one(s.history().stream().filter(v->v.get("attempt_id").equals(successor)).toList());
            exactly(jdbc.update("UPDATE ddip_management.attempts SET state='SUCCEEDED',outcome_code='RECONCILED_FAILED',finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND state='CLAIMED' AND row_version=?",successor,number(a,"row_version")));fault.accept(Boundary.ATTEMPT);
            exactly(jdbc.update("UPDATE ddip_management.runs SET state='FAILED',outcome_code=?,retry_eligible=false,finished_at=clock_timestamp(),row_version=row_version+1 WHERE run_id=? AND state='RECOVERY_REQUIRED' AND current_attempt_id=? AND row_version=?",CODE,s.runId(),successor,number(s.run(),"row_version")));fault.accept(Boundary.RUN);
            exactly(jdbc.update("UPDATE ddip_management.controls SET state='APPLIED',reason=?,applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=? AND state='REQUESTED' AND row_version=?",CODE,s.controlId(),number(s.control(),"row_version")));fault.accept(Boundary.CONTROL);
            exactly(jdbc.update("UPDATE ddip_management.worker_instances SET status='STOPPED',row_version=row_version+1 WHERE instance_id=? AND status='RECOVERY_ONLY' AND row_version=?",worker,number(s.recovery(),"row_version")));fault.accept(Boundary.WORKER);
            exactly(jdbc.update("UPDATE ddip_management.coordinator SET mode='READY',active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND mode='RECOVERY_ONLY' AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL",installation,successor,worker,number(s.coordinator(),"epoch"),number(s.coordinator(),"row_version")));fault.accept(Boundary.COORDINATOR);
            for(var event:audits(t.handoff,t.snapshot,t.id,t.digest())){
                jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,?,?)",s.runId(),successor,s.controlId(),event.get("action"),event.get("before_state"),event.get("after_state"),event.get("before_version"),event.get("after_version"),CODE,event.get("details_bytes"));fault.accept(Boundary.AUDIT);
            }
            byte[] snapshot=bounded(t.snapshot,65536),review=bounded(q.review.json(),8192),target=bounded(target(t.id,t.handoff,t.snapshot),8192);
            jdbc.update("INSERT INTO ddip_management.content_fetch_recovery_finalizations(finalization_id,handoff_id,identity_version,policy,run_id,recovery_attempt_id,review_bytes,review_sha256,snapshot_bytes,snapshot_sha256,target_bytes,target_sha256) VALUES (?,?,1,?,?,?,?,?,?,?,?,?)",t.id,t.handoff.get("handoff_id"),POLICY,s.runId(),successor,review,SourceJson.sha(review),snapshot,SourceJson.sha(snapshot),target,SourceJson.sha(target));fault.accept(Boundary.RECEIPT);
            guard.checkLeaseHeld(installation);RecoveryQuiescence.fresh(q.review.quiescence().inspectedAt(),clock.instant());
            return receipt(Status.COMMITTED,t);
        });
        fault.accept(Boundary.COMMITTED);handoffs.terminalGuard(guard,t.owner);return result;
    }
    Receipt resolve(Qualified q){
        outside();need(q!=null,INVALID_REQUEST);var t=q.target;
        try{return tx.required(()->{
            jdbc.queryForList("SELECT singleton_id FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");
            var rows=jdbc.queryForList("SELECT target_sha256 FROM ddip_management.content_fetch_recovery_finalizations WHERE finalization_id=?",String.class,t.id);
            if(!rows.isEmpty())return rows.size()==1 && rows.getFirst().equals(t.digest())?lookupLocked(t.id):receipt(Status.UNRESOLVED,t);
            var h=handoffs.terminalHandoff(t.owner);var s=handoffs.terminalRead(h);
            boolean same=snapshot(s).equals(t.snapshot) && jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_fetch_recovery_finalizations WHERE handoff_id=?",Long.class,h.get("handoff_id"))==0
                    && jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE run_id=? AND safe_code=?",Long.class,s.runId(),CODE)==0;
            return receipt(same?Status.CONFIRMED_ROLLBACK:Status.UNRESOLVED,t);
        });}catch(RuntimeException ex){return receipt(Status.UNRESOLVED,t);}
    }
    Receipt lookup(UUID id){
        outside();need(id!=null,INVALID_REQUEST);
        try{return tx.required(()->lookupLocked(id));}catch(RuntimeException ex){return new Receipt(Status.UNRESOLVED,id,null,null,null,null);}
    }
    Receipt lookupHandoffLocked(UUID handoff){
        need(TransactionSynchronizationManager.isActualTransactionActive(),INVALID_REQUEST);
        var rows=jdbc.queryForList("SELECT finalization_id FROM ddip_management.content_fetch_recovery_finalizations WHERE handoff_id=?",UUID.class,handoff);
        need(rows.size()==1,EVIDENCE_UNAVAILABLE);return lookupLocked(rows.getFirst());
    }
    private Receipt lookupLocked(UUID id){
        jdbc.queryForList("SELECT singleton_id FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");
        var row=one(jdbc.queryForList("SELECT * FROM ddip_management.content_fetch_recovery_finalizations WHERE finalization_id=?",id));
        var h=one(jdbc.queryForList("SELECT * FROM ddip_management.content_fetch_recovery_handoffs WHERE handoff_id=?",row.get("handoff_id")));
        var s=handoffs.terminalRead(h);var before=canonical(row,"snapshot",65536);var target=canonical(row,"target",8192);var review=canonical(row,"review",8192);
        need(number(row,"identity_version")==1 && POLICY.equals(row.get("policy")) && row.get("run_id").equals(h.get("run_id")) && row.get("recovery_attempt_id").equals(h.get("recovery_attempt_id"))
                && target.equals(target(id,h,before)),EVIDENCE_CHANGED);
        SourceJson.fields(before,"coordinator","run","attempt","control","oldWorker","recovery","history","lineage","controls");
        var transferred=(ObjectNode)before.deepCopy();transferred.remove("controls");handoffs.validateRecordedHandoff(h,transferred,s);
        validateReview(review,(String)row.get("target_sha256"),(UUID)h.get("checkpoint_id"),h);
        need(terminalAfter(before,snapshot(s),h),EVIDENCE_CHANGED);
        var actual=jdbc.queryForList("SELECT action,before_state,after_state,before_version,after_version,details_bytes FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND control_id=? AND safe_code=? ORDER BY event_seq LIMIT 6",s.runId(),h.get("recovery_attempt_id"),s.controlId(),CODE);
        need(RecoveryHandoff.json(actual).equals(RecoveryHandoff.json(audits(h,before,id,(String)row.get("target_sha256")))),EVIDENCE_CHANGED);
        return new Receipt(Status.CONFIRMED_TERMINAL,id,(UUID)h.get("handoff_id"),s.runId(),"FAILED",CODE);
    }
    private NarrowContentFetchRecoveryHandoff.Snapshot unchanged(InstallationHostGuard guard,Target t){
        var h=handoffs.terminalHandoff(t.owner);var s=handoffs.terminalRead(h);validatePending(guard,h,s);
        need(snapshot(s).equals(t.snapshot),OWNERSHIP_LOST);return s;
    }
    private void validatePending(InstallationHostGuard guard,Map<String,Object> h,NarrowContentFetchRecoveryHandoff.Snapshot s){
        handoffs.validateRecordedHandoff(h,s.json(),s);guard.checkLeaseHeld(installation);
        need(guard.process().instanceId().equals(s.recovery().get("instance_id")) && guard.process().pid()==number(s.recovery(),"pid")
                && guard.process().startedAt().equals(((Timestamp)s.recovery().get("process_started_at")).toInstant())
                && guard.binding().digest().equals(s.recovery().get("host_binding_sha256")),OWNERSHIP_LOST);
        need(s.run().get("result_artifact_id")==null && s.run().get("counts_bytes")==null && s.run().get("finished_at")==null,EVIDENCE_CHANGED);
        need(!jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND control_id<>? AND (state='REQUESTED' OR (type='CANCEL' AND state='APPLIED')))",Boolean.class,s.runId(),s.controlId()),EVIDENCE_UNAVAILABLE);
    }
    private JsonNode snapshot(NarrowContentFetchRecoveryHandoff.Snapshot s){
        var n=(ObjectNode)s.json();var controls=jdbc.queryForList("SELECT * FROM ddip_management.controls WHERE run_id=? ORDER BY control_id LIMIT 129",s.runId());need(controls.size()<=128,CAPACITY_EXCEEDED);
        var list=n.putArray("controls");for(var row:controls)list.addObject().put("controlId",row.get("control_id").toString()).put("sha256",hash(RecoveryHandoff.json(row)));return n;
    }
    private void intact(InstallationHostGuard guard,Target t){
        handoffs.terminalGuard(guard,t.owner);
        var a=NarrowContentFetchRecoveryAssessment.inspect(jdbc,tx,guard,t.state.runId(),t.state.oldId(),processes,b->{});
        need(a.durable().prefix()==NarrowContentFetchRecoveryAssessment.Prefix.CHECKPOINT && a.integrity()==NarrowContentFetchRecoveryAssessment.Integrity.VERIFIED_AT_INSPECTION
                && t.state.operation().equals(a.durable().operation()) && t.checkpoint().equals(a.durable().checkpoint()),EVIDENCE_UNAVAILABLE);
        handoffs.terminalGuard(guard,t.owner);
    }
    private void checkReview(Target t,NarrowContentFetchRecoveryHandoff.Review review){
        need(review!=null && review.descendantsReviewed() && review.launchGapsReviewed(),QUIESCENCE_REQUIRED);
        var original=canonical(t.handoff,"review",8192);need(review.json().get("descendants").valueStream().toList().containsAll(original.get("descendants").valueStream().toList()),QUIESCENCE_REQUIRED);
        RecoveryQuiescence.check(review.quiescence(),t.digest(),Set.of(t.checkpoint()),t.state.processes(),processes,clock.instant());
    }
    private static void validateReview(JsonNode review,String sha,UUID checkpoint,Map<String,Object> h){
        SourceJson.fields(review,"targetSha256","inspectedAt","handlersStopped","unmanagedWritersStopped","orphanReviewComplete","reviewedCheckpoints","descendants","reviewVersion","descendantsReviewed","launchGapsReviewed");
        need(sha.equals(text(review,"targetSha256")) && review.get("reviewVersion").isIntegralNumber() && review.get("reviewVersion").intValue()==1,EVIDENCE_CHANGED);
        for(String field:List.of("handlersStopped","unmanagedWritersStopped","orphanReviewComplete","descendantsReviewed","launchGapsReviewed"))need(review.get(field).isBoolean() && review.get(field).booleanValue(),EVIDENCE_CHANGED);
        need(review.get("reviewedCheckpoints").isArray() && review.get("reviewedCheckpoints").size()==1 && checkpoint.toString().equals(review.get("reviewedCheckpoints").get(0).stringValue())
                && review.get("descendants").isArray() && review.get("descendants").size()<=32,EVIDENCE_CHANGED);
        Instant.parse(text(review,"inspectedAt"));var prior=canonical(h,"review",8192);
        var identities=new HashSet<RecoveryQuiescence.Identity>();for(var p:review.get("descendants")){
            SourceJson.fields(p,"pid","startedAt");need(identities.add(new RecoveryQuiescence.Identity(Long.parseLong(text(p,"pid")),Instant.parse(text(p,"startedAt")))),EVIDENCE_CHANGED);
        }
        need(review.get("descendants").valueStream().toList().containsAll(prior.get("descendants").valueStream().toList()),EVIDENCE_CHANGED);
    }
    private static boolean terminalAfter(JsonNode before,JsonNode actual,Map<String,Object> h){
        var expected=(ObjectNode)before.deepCopy();String successor=h.get("recovery_attempt_id").toString();
        var attempt=expected.get("history").valueStream().filter(v->text(v,"attempt_id").equals(successor)).findFirst().orElseThrow();
        var actualAttempt=actual.get("history").valueStream().filter(v->text(v,"attempt_id").equals(successor)).findFirst().orElseThrow();
        terminal((ObjectNode)attempt,actualAttempt,"state","SUCCEEDED","outcome_code","RECONCILED_FAILED","finished_at");
        terminal((ObjectNode)expected.get("run"),actual.get("run"),"state","FAILED","outcome_code",CODE,"finished_at");
        terminal((ObjectNode)expected.get("control"),actual.get("control"),"state","APPLIED","reason",CODE,"applied_at");
        var worker=(ObjectNode)expected.get("recovery");worker.put("status","STOPPED").put("row_version",Long.toString(decimal(worker,"row_version")+1));
        String control=h.get("control_id").toString();for(var entry:expected.get("controls"))if(control.equals(text(entry,"controlId")))((ObjectNode)entry).put("sha256",hash(expected.get("control")));
        var c=actual.get("coordinator");var old=before.get("coordinator");long min=decimal(old,"row_version")+1;
        need(decimal(c,"row_version")>=min && decimal(c,"epoch")>=decimal(old,"epoch")
                && (c.get("active_attempt_id").isNull() || (!successor.equals(text(c,"active_attempt_id")) && !h.get("original_attempt_id").toString().equals(text(c,"active_attempt_id"))))
                && (c.get("owner_instance_id").isNull() || (!h.get("original_instance_id").toString().equals(text(c,"owner_instance_id")) && !h.get("recovery_instance_id").toString().equals(text(c,"owner_instance_id")))),EVIDENCE_CHANGED);
        if(decimal(c,"row_version")==min){var released=(ObjectNode)old.deepCopy();released.put("mode","READY").put("row_version",Long.toString(min)).putNull("active_attempt_id").putNull("owner_instance_id").putNull("heartbeat_at");need(released.equals(c),EVIDENCE_CHANGED);}
        expected.set("coordinator",c);return expected.equals(actual);
    }
    private static void terminal(ObjectNode expected,JsonNode actual,String state,String value,String code,String outcome,String time){
        need(actual.get(time).isString(),EVIDENCE_CHANGED);Instant.parse(text(actual,time));expected.put(state,value).put(code,outcome).put("row_version",Long.toString(decimal(expected,"row_version")+1));expected.set(time,actual.get(time));
    }
    private static ObjectNode target(UUID id,Map<String,Object> h,JsonNode snapshot){return SourceJson.object().put("identityVersion",1).put("policy",POLICY).put("finalizationId",id.toString()).put("handoffId",h.get("handoff_id").toString()).put("snapshotSha256",hash(snapshot)).put("handoffSha256",(String)h.get("ancestry_sha256"));}
    private static List<Map<String,Object>> audits(Map<String,Object> h,JsonNode before,UUID id,String sha){
        byte[] details=SourceJson.canonical(SourceJson.object().put("identityVersion",1).put("policy",POLICY).put("finalizationId",id.toString()).put("handoffId",h.get("handoff_id").toString()).put("targetSha256",sha).put("checkpointId",h.get("checkpoint_id").toString()));
        var a=before.get("history").valueStream().filter(v->text(v,"attempt_id").equals(h.get("recovery_attempt_id").toString())).findFirst().orElseThrow();
        return List.of(audit("NATIVE_FETCH_TERMINAL_ATTEMPT","CLAIMED","SUCCEEDED",decimal(a,"row_version"),details),audit("NATIVE_FETCH_TERMINAL_RUN","RECOVERY_REQUIRED","FAILED",decimal(before.get("run"),"row_version"),details),
                audit("NATIVE_FETCH_TERMINAL_CONTROL","REQUESTED","APPLIED",decimal(before.get("control"),"row_version"),details),audit("NATIVE_FETCH_TERMINAL_WORKER","RECOVERY_ONLY","STOPPED",decimal(before.get("recovery"),"row_version"),details),audit("NATIVE_FETCH_TERMINAL_RELEASE","RECOVERY_ONLY","READY",decimal(before.get("coordinator"),"row_version"),details));
    }
    private static Map<String,Object> audit(String action,String before,String after,long version,byte[] details){return Map.of("action",action,"before_state",before,"after_state",after,"before_version",version,"after_version",version+1,"details_bytes",details);}
    private static Receipt receipt(Status status,Target t){boolean terminal=status==Status.COMMITTED || status==Status.CONFIRMED_TERMINAL;return new Receipt(status,t.id,(UUID)t.handoff.get("handoff_id"),t.state.runId(),terminal?"FAILED":null,terminal?CODE:null);}
    private static JsonNode canonical(Map<String,Object> row,String field,int max){byte[] bytes=(byte[])row.get(field+"_bytes");var n=SourceJson.parse(bytes,max);need(SourceJson.sha(bytes).equals(row.get(field+"_sha256")) && Arrays.equals(bytes,SourceJson.canonical(n)),EVIDENCE_CHANGED);return n;}
    private static byte[] bounded(JsonNode n,int max){byte[] raw=SourceJson.canonical(n);need(raw.length<=max,CAPACITY_EXCEEDED);return raw;}
    private static String hash(JsonNode n){return SourceJson.sha(SourceJson.canonical(n));}
    private static String text(JsonNode n,String key){return SourceJson.text(n.get(key));}
    private static long decimal(JsonNode n,String key){return Long.parseLong(text(n,key));}
    private static long number(Map<String,Object> row,String key){return ((Number)row.get(key)).longValue();}
    private static Map<String,Object> one(List<Map<String,Object>> rows){need(rows.size()==1,EVIDENCE_UNAVAILABLE);return rows.getFirst();}
    private static void exactly(int count){need(count==1,OWNERSHIP_LOST);}
    private static void outside(){need(!TransactionSynchronizationManager.isActualTransactionActive(),INVALID_REQUEST);}
}
