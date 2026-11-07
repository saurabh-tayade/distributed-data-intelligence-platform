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

/** Inactive, trusted-local, checkpoint-only ownership transfer. No dispatcher or operational caller. */
final class NarrowContentFetchRecoveryHandoff {
    static final String POLICY="ddip-content-fetch-checkpoint-recovery-v1";
    enum Boundary { BEFORE_TRANSFER, OLD_ATTEMPT, SUCCESSOR, COORDINATOR, RUN, HANDOFF, AUDIT, COMMITTED }
    enum Status { COMMITTED, CONFIRMED_HANDOFF, CONFIRMED_ROLLBACK, UNRESOLVED }
    /** Facts only. Lookup cannot mint, renew or reconstruct authority. */
    record Receipt(Status status,UUID handoffId,UUID runId,UUID attemptId,UUID checkpointId) { }
    /** Distinct private type, deliberately not convertible to WorkerRepository.Owner or RecoveryHandoff.Owner. */
    static final class Owner {
        private final UUID handoff,attempt,instance;private final long epoch;private final String identity;
        private Owner(UUID handoff,UUID attempt,UUID instance,long epoch,String identity){this.handoff=handoff;this.attempt=attempt;this.instance=instance;this.epoch=epoch;this.identity=identity;}
        @Override public String toString(){return "NativeFetchRecoveryOwner[checkpoint-only; redacted]";}
    }
    record Result(Receipt receipt,Owner owner) { }
    record Review(RecoveryQuiescence.Review quiescence,boolean descendantsReviewed,boolean launchGapsReviewed) {
        Review {need(quiescence!=null,INVALID_REQUEST);}
        JsonNode json(){var n=(ObjectNode)quiescence.json();return n.put("reviewVersion",1).put("descendantsReviewed",descendantsReviewed).put("launchGapsReviewed",launchGapsReviewed);}
        @Override public String toString(){return "NativeFetchRecoveryReview[redacted]";}
    }
    private record Snapshot(Map<String,Object> coordinator,Map<String,Object> run,Map<String,Object> attempt,
                            Map<String,Object> control,Map<String,Object> oldWorker,Map<String,Object> recovery,
                            List<Map<String,Object>> history,Map<String,Object> lineage) {
        UUID runId(){return (UUID)run.get("run_id");} UUID oldId(){return (UUID)attempt.get("attempt_id");}
        UUID controlId(){return (UUID)control.get("control_id");}
        JsonNode json(){return RecoveryHandoff.json(Map.of("coordinator",coordinator,"run",run,"attempt",attempt,"control",control,
                "oldWorker",oldWorker,"recovery",recovery,"history",history,"lineage",lineage));}
        String digest(){return hash(json());}
        @SuppressWarnings("unchecked") Map<String,Object> evidence(String key){return ((List<Map<String,Object>>)lineage.get(key)).getFirst();}
        UUID checkpoint(){return (UUID)evidence("checkpoint").get("checkpoint_id");}
        UUID operation(){return (UUID)evidence("operation").get("operation_id");}
        List<RecoveryQuiescence.Identity> processes(){var ids=new ArrayList<RecoveryQuiescence.Identity>();ids.add(identity(oldWorker,"pid","process_started_at"));
            @SuppressWarnings("unchecked") var phases=(List<Map<String,Object>>)lineage.get("launch");
            for(var p:phases)if("CHILD_RECORDED".equals(p.get("phase")))ids.add(identity(p,"child_pid","child_started_at"));return List.copyOf(ids);}
    }
    static final class Target {
        private final Snapshot snapshot;private final WorkerRepository.Session recovery;private final UUID recoveryAttempt,handoff;
        private Target(Snapshot snapshot,WorkerRepository.Session recovery,UUID recoveryAttempt,UUID handoff){this.snapshot=snapshot;this.recovery=recovery;this.recoveryAttempt=recoveryAttempt;this.handoff=handoff;}
        String digest(){return hash(ancestry(this));} UUID checkpoint(){return snapshot.checkpoint();}
        UUID recoveryAttempt(){return recoveryAttempt;} UUID handoffId(){return handoff;}
        List<RecoveryQuiescence.Identity> processes(){return snapshot.processes();}
        @Override public String toString(){return "NativeFetchRecoveryTarget[redacted]";}
    }
    static final class Qualified {
        private final Target target;private final Review review;private final AtomicBoolean used=new AtomicBoolean();
        private Qualified(Target target,Review review){this.target=target;this.review=review;}
        @Override public String toString(){return "NativeFetchRecoveryQualification[redacted]";}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    private final Clock clock;private final RecoveryQuiescence.Processes processes;private final Consumer<Boundary> fault;
    NarrowContentFetchRecoveryHandoff(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation){this(jdbc,tx,installation,Clock.systemUTC(),RecoveryQuiescence.LOCAL,b->{});}
    NarrowContentFetchRecoveryHandoff(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,RecoveryQuiescence.Processes processes,Consumer<Boundary> fault){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.installation=Objects.requireNonNull(installation);
        this.clock=Objects.requireNonNull(clock);this.processes=Objects.requireNonNull(processes);this.fault=Objects.requireNonNull(fault);
    }
    Target inspect(InstallationHostGuard guard,WorkerRepository.Session recovery,UUID control,UUID recoveryAttempt,UUID handoff){
        held(guard,recovery);need(control!=null && recoveryAttempt!=null && handoff!=null && !control.equals(recoveryAttempt)
                && !handoff.equals(recoveryAttempt) && !handoff.equals(control),INVALID_REQUEST);
        return tx.required(()->{var s=read(control,recovery.instanceId());validateOriginal(guard,recovery,s);
            need(!s.oldId().equals(recoveryAttempt) && !s.oldId().equals(handoff),INVALID_REQUEST);
            need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE attempt_id=?",Long.class,recoveryAttempt)==0
                    && jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_fetch_recovery_handoffs WHERE handoff_id=? OR original_attempt_id=?",Long.class,handoff,s.oldId())==0,OWNERSHIP_LOST);
            var target=new Target(s,recovery,recoveryAttempt,handoff);bounded(ancestry(target),65536);return target;});
    }
    Qualified qualify(InstallationHostGuard guard,Target target,Review review){
        need(target!=null,INVALID_REQUEST);held(guard,target.recovery);checkReview(target,review);
        tx.required(()->{var s=read(target.snapshot.controlId(),target.recovery.instanceId());validateOriginal(guard,target.recovery,s);need(s.digest().equals(target.snapshot.digest()),OWNERSHIP_LOST);return null;});
        intact(guard,target);checkReview(target,review);bounded(review.json(),8192);return new Qualified(target,review);
    }
    Result transfer(InstallationHostGuard guard,Qualified q){
        need(q!=null,INVALID_REQUEST);held(guard,q.target.recovery);
        if(q.used.get()){var r=resolve(q);if(r.status()==Status.CONFIRMED_HANDOFF)return new Result(r,null);throw new RecoveryFailure(r.status()==Status.CONFIRMED_ROLLBACK?FRESH_QUALIFICATION_REQUIRED:HANDOFF_UNRESOLVED);}
        var t=q.target;checkReview(t,q.review);intact(guard,t);checkReview(t,q.review);
        // Consumed before the first possible SQL mutation, including uncertain commit acknowledgment.
        need(q.used.compareAndSet(false,true),FRESH_QUALIFICATION_REQUIRED);fault.accept(Boundary.BEFORE_TRANSFER);
        held(guard,t.recovery);
        var result=tx.required(()->{
            guard.checkLeaseHeld(installation);
            var s=read(t.snapshot.controlId(),t.recovery.instanceId());validateOriginal(guard,t.recovery,s);
            need(s.digest().equals(t.snapshot.digest()),OWNERSHIP_LOST);RecoveryQuiescence.fresh(q.review.quiescence().inspectedAt(),clock.instant());
            need(s.history.size()<20 && jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=?",Long.class,s.runId())<=128,CAPACITY_EXCEEDED);
            long epoch=Math.addExact(number(s.coordinator,"epoch"),1),next=s.history.stream().mapToLong(v->number(v,"attempt_no")).max().orElse(0)+1;
            need(next<=20 && epoch>0,CAPACITY_EXCEEDED);
            long unfinished=jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);
            need(AuditCapacity.available(jdbc,3,unfinished),CAPACITY_EXCEEDED);
            exactly(jdbc.update("UPDATE ddip_management.attempts SET state='INTERRUPTED',finished_at=clock_timestamp(),outcome_code='INTERRUPTED',row_version=row_version+1 WHERE attempt_id=? AND state=? AND row_version=?",s.oldId(),s.attempt.get("state"),number(s.attempt,"row_version")));fault.accept(Boundary.OLD_ATTEMPT);
            jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,content_recovery_policy) VALUES (?,?,?,'RECONCILE',?,?,'CLAIMED',?)",t.recoveryAttempt,s.runId(),next,t.recovery.instanceId(),epoch,POLICY);fault.accept(Boundary.SUCCESSOR);
            exactly(jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=?,owner_instance_id=?,epoch=?,mode='RECOVERY_ONLY',heartbeat_at=clock_timestamp(),row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL",t.recoveryAttempt,t.recovery.instanceId(),epoch,installation,s.oldId(),s.oldWorker.get("instance_id"),number(s.coordinator,"epoch"),number(s.coordinator,"row_version")));fault.accept(Boundary.COORDINATOR);
            exactly(jdbc.update("UPDATE ddip_management.runs SET state='RECOVERY_REQUIRED',current_attempt_id=?,outcome_code='RECOVERY_REQUIRED',retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND current_attempt_id=? AND state=? AND row_version=?",t.recoveryAttempt,s.runId(),s.oldId(),s.run.get("state"),number(s.run,"row_version")));fault.accept(Boundary.RUN);
            byte[] ancestry=bounded(ancestry(t),65536),review=bounded(q.review.json(),8192);
            jdbc.update("INSERT INTO ddip_management.content_fetch_recovery_handoffs(handoff_id,identity_version,policy,run_id,control_id,original_attempt_id,recovery_attempt_id,operation_id,checkpoint_id,original_instance_id,recovery_instance_id,original_epoch,original_coordinator_version,original_run_version,original_attempt_version,original_worker_version,recovery_worker_version,control_version,review_bytes,review_sha256,ancestry_bytes,ancestry_sha256) VALUES (?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",t.handoff,POLICY,s.runId(),s.controlId(),s.oldId(),t.recoveryAttempt,s.operation(),s.checkpoint(),s.oldWorker.get("instance_id"),t.recovery.instanceId(),number(s.coordinator,"epoch"),number(s.coordinator,"row_version"),number(s.run,"row_version"),number(s.attempt,"row_version"),number(s.oldWorker,"row_version"),number(s.recovery,"row_version"),number(s.control,"row_version"),review,SourceJson.sha(review),ancestry,SourceJson.sha(ancestry));fault.accept(Boundary.HANDOFF);
            for(var a:audits(s,t.recoveryAttempt,t.handoff,t.digest())){
                jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,'NATIVE_FETCH_HANDOFF',?)",s.runId(),a.get("attempt_id"),s.controlId(),a.get("action"),a.get("before_state"),a.get("after_state"),a.get("before_version"),a.get("after_version"),a.get("details_bytes"));fault.accept(Boundary.AUDIT);
            }
            guard.checkLeaseHeld(installation);
            RecoveryQuiescence.fresh(q.review.quiescence().inspectedAt(),clock.instant());
            return new Result(receipt(Status.COMMITTED,t),new Owner(t.handoff,t.recoveryAttempt,t.recovery.instanceId(),epoch,t.digest()));
        });
        fault.accept(Boundary.COMMITTED);held(guard,t.recovery);return result;
    }
    /** Read-only acknowledgment inspection. Even after a lost acknowledgment it never returns an Owner. */
    Receipt resolve(Qualified q){
        outside();need(q!=null,INVALID_REQUEST);var t=q.target;
        try{return tx.required(()->{
            var rows=jdbc.queryForList("SELECT ancestry_sha256 FROM ddip_management.content_fetch_recovery_handoffs WHERE handoff_id=?",String.class,t.handoff);
            if(!rows.isEmpty())return rows.size()==1 && rows.getFirst().equals(t.digest())?lookupLocked(t.handoff):receipt(Status.UNRESOLVED,t);
            var s=read(t.snapshot.controlId(),t.recovery.instanceId());
            boolean unchanged=s.digest().equals(t.snapshot.digest()) && jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE attempt_id=?",Long.class,t.recoveryAttempt)==0
                    && jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE control_id=? AND safe_code='NATIVE_FETCH_HANDOFF'",Long.class,s.controlId())==0;
            return receipt(unchanged?Status.CONFIRMED_ROLLBACK:Status.UNRESOLVED,t);
        });}catch(RuntimeException ex){return receipt(Status.UNRESOLVED,t);}
    }
    Receipt lookup(UUID handoff){
        outside();need(handoff!=null,INVALID_REQUEST);
        try{return tx.required(()->lookupLocked(handoff));}catch(RuntimeException ex){return new Receipt(Status.UNRESOLVED,handoff,null,null,null);}
    }
    private Receipt lookupLocked(UUID handoff){
        // Coordinator first, then an immutable handoff, then all current metadata. No file/process IO.
        jdbc.queryForList("SELECT singleton_id FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");
        var row=one(jdbc.queryForList("SELECT * FROM ddip_management.content_fetch_recovery_handoffs WHERE handoff_id=?",handoff));
        var s=read((UUID)row.get("control_id"),(UUID)row.get("recovery_instance_id"));
        var n=canonical((byte[])row.get("ancestry_bytes"),(String)row.get("ancestry_sha256"),65536);
        var review=canonical((byte[])row.get("review_bytes"),(String)row.get("review_sha256"),8192);
        var before=n.get("snapshot");need(before!=null && before.isObject() && hash(before).equals(text(n,"snapshotSha256")),EVIDENCE_CHANGED);
        need(number(row,"identity_version")==1 && POLICY.equals(row.get("policy")) && n.get("identityVersion").intValue()==1 && POLICY.equals(text(n,"policy")),EVIDENCE_CHANGED);
        for(String[] pair:List.of(new String[]{"handoffId","handoff_id"},new String[]{"runId","run_id"},new String[]{"controlId","control_id"},new String[]{"originalAttemptId","original_attempt_id"},new String[]{"recoveryAttemptId","recovery_attempt_id"},new String[]{"operationId","operation_id"},new String[]{"checkpointId","checkpoint_id"}))need(text(n,pair[0]).equals(row.get(pair[1]).toString()),EVIDENCE_CHANGED);
        need(text(review,"targetSha256").equals(SourceJson.sha((byte[])row.get("ancestry_bytes"))) && review.get("reviewVersion").intValue()==1
                && review.get("handlersStopped").booleanValue() && review.get("unmanagedWritersStopped").booleanValue() && review.get("orphanReviewComplete").booleanValue()
                && review.get("descendantsReviewed").booleanValue() && review.get("launchGapsReviewed").booleanValue(),EVIDENCE_CHANGED);
        need(unchangedAfter(before,s,row),EVIDENCE_CHANGED);
        // read() independently reconstructs the native reservation, launch and checkpoint SQL.
        checkLineageHeader(n,s);
        var expected=expectedAudits(before,s,row,(String)row.get("ancestry_sha256"));
        var actual=jdbc.queryForList("SELECT attempt_id,action,before_state,after_state,before_version,after_version,details_bytes FROM ddip_management.audit_events WHERE run_id=? AND control_id=? AND safe_code='NATIVE_FETCH_HANDOFF' ORDER BY event_seq LIMIT 4",s.runId(),s.controlId());
        need(hash(RecoveryHandoff.json(actual)).equals(hash(RecoveryHandoff.json(expected))),EVIDENCE_CHANGED);
        return new Receipt(Status.CONFIRMED_HANDOFF,handoff,s.runId(),(UUID)row.get("recovery_attempt_id"),s.checkpoint());
    }
    private boolean unchangedAfter(JsonNode before,Snapshot after,Map<String,Object> h){
        var expected=(ObjectNode)before.deepCopy();var actual=(ObjectNode)after.json();
        need(text(before.get("attempt"),"attempt_id").equals(h.get("original_attempt_id").toString()) && text(before.get("run"),"run_id").equals(h.get("run_id").toString())
                && text(before.get("control"),"control_id").equals(h.get("control_id").toString())
                && text(before.get("oldWorker"),"instance_id").equals(h.get("original_instance_id").toString())
                && text(before.get("recovery"),"instance_id").equals(h.get("recovery_instance_id").toString()),EVIDENCE_CHANGED);
        for(String[] p:List.of(new String[]{"coordinator","row_version","original_coordinator_version"},new String[]{"coordinator","epoch","original_epoch"},new String[]{"run","row_version","original_run_version"},new String[]{"attempt","row_version","original_attempt_version"},new String[]{"oldWorker","row_version","original_worker_version"},new String[]{"recovery","row_version","recovery_worker_version"},new String[]{"control","row_version","control_version"}))need(text(before.get(p[0]),p[1]).equals(h.get(p[2]).toString()),EVIDENCE_CHANGED);
        var a=(ObjectNode)expected.get("attempt");need(Set.of("CLAIMED","RUNNING").contains(text(a,"state")) && actual.get("attempt").get("finished_at").isString(),EVIDENCE_CHANGED);
        a.put("state","INTERRUPTED").put("outcome_code","INTERRUPTED").put("row_version",Long.toString(number(h,"original_attempt_version")+1));a.set("finished_at",actual.get("attempt").get("finished_at"));
        var c=(ObjectNode)expected.get("coordinator");c.put("active_attempt_id",h.get("recovery_attempt_id").toString()).put("owner_instance_id",h.get("recovery_instance_id").toString()).put("epoch",Long.toString(number(h,"original_epoch")+1)).put("row_version",Long.toString(number(h,"original_coordinator_version")+1)).put("mode","RECOVERY_ONLY");c.set("heartbeat_at",actual.get("coordinator").get("heartbeat_at"));
        var r=(ObjectNode)expected.get("run");r.put("current_attempt_id",h.get("recovery_attempt_id").toString()).put("state","RECOVERY_REQUIRED").put("outcome_code","RECOVERY_REQUIRED").put("retry_eligible",false).put("row_version",Long.toString(number(h,"original_run_version")+1));
        var history=expected.putArray("history");for(var prior:before.get("history")){history.add(text(prior,"attempt_id").equals(h.get("original_attempt_id").toString())?a:prior);}
        var successors=after.history.stream().filter(v->v.get("attempt_id").equals(h.get("recovery_attempt_id"))).toList();need(successors.size()==1,EVIDENCE_CHANGED);var successor=successors.getFirst();
        need(successor.get("run_id").equals(h.get("run_id")) && "RECONCILE".equals(successor.get("kind")) && "CLAIMED".equals(successor.get("state")) && POLICY.equals(successor.get("content_recovery_policy"))
                && successor.get("instance_id").equals(h.get("recovery_instance_id")) && number(successor,"owner_epoch")==number(h,"original_epoch")+1 && number(successor,"row_version")==0
                && successor.get("finished_at")==null && successor.get("outcome_code")==null && successor.get("content_admission_policy")==null && successor.get("reused_checkpoint_id")==null
                && number(successor,"attempt_no")==before.get("history").valueStream().mapToLong(v->Long.parseLong(text(v,"attempt_no"))).max().orElse(0)+1,EVIDENCE_CHANGED);
        history.add(RecoveryHandoff.json(successor));return expected.equals(actual);
    }
    private void intact(InstallationHostGuard guard,Target t){
        held(guard,t.recovery);
        var a=NarrowContentFetchRecoveryAssessment.inspect(jdbc,tx,guard,t.snapshot.runId(),t.snapshot.oldId(),processes,b->{});
        need(a.durable().prefix()==NarrowContentFetchRecoveryAssessment.Prefix.CHECKPOINT && a.integrity()==NarrowContentFetchRecoveryAssessment.Integrity.VERIFIED_AT_INSPECTION
                && t.snapshot.operation().equals(a.durable().operation()) && t.checkpoint().equals(a.durable().checkpoint()),EVIDENCE_UNAVAILABLE);
        held(guard,t.recovery);
    }
    private void checkReview(Target t,Review review){
        need(review!=null && review.descendantsReviewed() && review.launchGapsReviewed(),QUIESCENCE_REQUIRED);
        RecoveryQuiescence.check(review.quiescence(),t.digest(),Set.of(t.checkpoint()),t.processes(),processes,clock.instant());
    }
    private Snapshot read(UUID control,UUID recovery){
        need("read committed".equals(jdbc.queryForObject("SHOW transaction_isolation",String.class)),INVALID_REQUEST);
        var c=one(jdbc.queryForList("SELECT * FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE"));need(installation.equals(c.get("installation_id")),OWNERSHIP_LOST);
        var ids=one(jdbc.queryForList("SELECT run_id,target_attempt_id FROM ddip_management.controls WHERE control_id=?",control));
        var r=one(jdbc.queryForList("SELECT * FROM ddip_management.runs WHERE run_id=? FOR UPDATE",ids.get("run_id")));
        var a=one(jdbc.queryForList("SELECT * FROM ddip_management.attempts WHERE attempt_id=? FOR UPDATE",ids.get("target_attempt_id")));
        var k=one(jdbc.queryForList("SELECT * FROM ddip_management.controls WHERE control_id=? FOR UPDATE",control));
        var workers=jdbc.queryForList("SELECT * FROM ddip_management.worker_instances WHERE instance_id IN (?,?) ORDER BY instance_id FOR UPDATE",a.get("instance_id"),recovery);need(workers.size()==2,OWNERSHIP_LOST);
        var old=one(workers.stream().filter(w->w.get("instance_id").equals(a.get("instance_id"))).toList());var next=one(workers.stream().filter(w->w.get("instance_id").equals(recovery)).toList());
        var history=jdbc.queryForList("SELECT * FROM ddip_management.attempts WHERE run_id=? ORDER BY attempt_no LIMIT 21",r.get("run_id"));need(history.size()<=20,CAPACITY_EXCEEDED);
        need(!jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.stage_invocations s JOIN ddip_management.attempts a USING(attempt_id) WHERE a.run_id=?) OR EXISTS(SELECT 1 FROM ddip_management.rss_deliveries WHERE run_id=?) OR EXISTS(SELECT 1 FROM ddip_management.content_operations o JOIN ddip_management.attempts a USING(attempt_id) WHERE a.run_id=?)",Boolean.class,r.get("run_id"),r.get("run_id"),r.get("run_id")),EVIDENCE_UNAVAILABLE);
        var lineage=new LinkedHashMap<String,Object>();
        lineage.put("admission",jdbc.queryForList("SELECT * FROM ddip_management.content_attempt_admissions WHERE attempt_id=?",a.get("attempt_id")));
        lineage.put("milestones",jdbc.queryForList("SELECT * FROM ddip_management.content_admission_milestones WHERE attempt_id=? ORDER BY phase",a.get("attempt_id")));
        var operations=jdbc.queryForList("SELECT * FROM ddip_management.content_native_operations WHERE attempt_id=? LIMIT 2",a.get("attempt_id"));need(operations.size()==1,EVIDENCE_UNAVAILABLE);var op=operations.getFirst();lineage.put("operation",operations);
        for(String[] table:List.of(new String[]{"launch","content_native_launch_phases"},new String[]{"intent","content_fetch_completion_intents"},new String[]{"checkpoint","content_fetch_checkpoints"}))lineage.put(table[0],jdbc.queryForList("SELECT * FROM ddip_management."+table[1]+" WHERE operation_id=?"+(table[0].equals("launch")?" ORDER BY phase":""),op.get("operation_id")));
        @SuppressWarnings("unchecked") var checkpoints=(List<Map<String,Object>>)lineage.get("checkpoint");need(checkpoints.size()==1,EVIDENCE_UNAVAILABLE);
        lineage.put("sources",jdbc.queryForList("SELECT * FROM ddip_management.run_sources WHERE run_id=? ORDER BY product_id,source_id",r.get("run_id")));
        lineage.put("inputs",jdbc.queryForList("SELECT * FROM ddip_management.run_inputs WHERE run_id=? ORDER BY role,ordinal",r.get("run_id")));
        // SQL-only reader validates every immutable native prefix, command and checkpoint correspondence.
        need(NarrowContentFetchCompletion.inspect(jdbc,tx,(UUID)op.get("operation_id")).isPresent(),EVIDENCE_UNAVAILABLE);
        NarrowContentAdmission.inspectHistorical(jdbc,(UUID)r.get("run_id"),(UUID)a.get("attempt_id"));
        return new Snapshot(c,r,a,k,old,next,history,lineage);
    }
    private void validateOriginal(InstallationHostGuard guard,WorkerRepository.Session w,Snapshot s){
        need(s.history.stream().allMatch(a->a.get("attempt_id").equals(s.oldId()) ||
                "EXECUTE".equals(a.get("kind")) && Set.of("FAILED","CANCELLED").contains(a.get("state"))
                && a.get("content_admission_policy")==null && a.get("content_recovery_policy")==null
                && a.get("reused_checkpoint_id")==null && number(a,"attempt_no")<number(s.attempt,"attempt_no")),EVIDENCE_UNAVAILABLE);
        need(Set.of("READY","RECOVERY_ONLY").contains(s.coordinator.get("mode")) && s.coordinator.get("active_import_id")==null
                && s.oldId().equals(s.coordinator.get("active_attempt_id")) && s.oldWorker.get("instance_id").equals(s.coordinator.get("owner_instance_id"))
                && number(s.attempt,"owner_epoch")==number(s.coordinator,"epoch") && w.epoch()==number(s.coordinator,"epoch") && w.coordinatorVersion()==number(s.coordinator,"row_version"),OWNERSHIP_LOST);
        need(s.runId().equals(s.control.get("run_id")) && s.runId().equals(s.attempt.get("run_id")) && s.oldId().equals(s.run.get("current_attempt_id"))
                && Set.of("RUNNING","CANCEL_REQUESTED","RECOVERY_REQUIRED").contains(s.run.get("state")) && number(s.run,"row_version")==number(s.control,"expected_version")
                && "EXECUTE".equals(s.attempt.get("kind")) && Set.of("CLAIMED","RUNNING").contains(s.attempt.get("state")) && NarrowContentProfile.POLICY.equals(s.attempt.get("content_admission_policy")),OWNERSHIP_LOST);
        need("RECONCILE".equals(s.control.get("type")) && "REQUESTED".equals(s.control.get("state")) && Boolean.TRUE.equals(s.control.get("quiescence_confirmed")) && "local-operator".equals(s.control.get("requester")),INVALID_REQUEST);
        need("RECOVERY_ONLY".equals(w.status()) && "RECOVERY_ONLY".equals(s.recovery.get("status")) && w.workerVersion()==number(s.recovery,"row_version")
                && installation.equals(s.recovery.get("installation_id")) && w.profileId().equals(s.recovery.get("execution_profile_id")) && w.profileDigest().equals(s.recovery.get("profile_sha256"))
                && w.hostDigest().equals(s.recovery.get("host_binding_sha256")) && guard.process().pid()==number(s.recovery,"pid") && guard.process().startedAt().equals(((Timestamp)s.recovery.get("process_started_at")).toInstant()),OWNERSHIP_LOST);
        need(installation.equals(s.oldWorker.get("installation_id")) && w.profileId().equals(s.oldWorker.get("execution_profile_id")) && w.profileDigest().equals(s.oldWorker.get("profile_sha256"))
                && w.hostDigest().equals(s.oldWorker.get("host_binding_sha256")) && w.profileId().equals(s.run.get("execution_profile_id")),RUNTIME_MISMATCH);
        var body=SourceJson.object().put("action","RECONCILE").put("attemptId",s.oldId().toString()).put("quiescenceConfirmed",true);
        var command=ControlCommands.parse(s.runId().toString(),SourceJson.canonical(body),(String)s.control.get("idempotency_key"),"\""+s.control.get("expected_version")+"\"");
        need(Arrays.equals(command.normalized(),(byte[])s.control.get("normalized_request")) && SourceJson.sha(command.normalized()).equals(s.control.get("request_sha256")),EVIDENCE_CHANGED);
        var sources=jdbc.queryForList("SELECT r.source_id,r.revision,r.captured_gate_version,r.captured_enabled,s.profile_family,v.configuration_bytes,v.configuration_sha256,v.endpoint_id,v.endpoint_sha256 FROM ddip_management.run_sources r JOIN ddip_management.sources s USING(product_id,source_id) JOIN ddip_management.source_revisions v ON (v.product_id,v.source_id,v.revision)=(r.product_id,r.source_id,r.revision) WHERE r.run_id=? ORDER BY r.product_id,r.source_id",s.runId());
        FrozenRunSelection.validate(jdbc,installation,s.run,sources,w.profileId(),w.profileDigest(),w.deploymentDigest());
        need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_fetch_recovery_handoffs WHERE run_id=?",Long.class,s.runId())==0,OWNERSHIP_LOST);
    }
    private static ObjectNode ancestry(Target t){
        var s=t.snapshot;var n=SourceJson.object().put("identityVersion",1).put("policy",POLICY).put("handoffId",t.handoff.toString()).put("controlId",s.controlId().toString())
                .put("runId",s.runId().toString()).put("originalAttemptId",s.oldId().toString()).put("recoveryAttemptId",t.recoveryAttempt.toString())
                .put("operationId",s.operation().toString()).put("checkpointId",s.checkpoint().toString()).put("snapshotSha256",s.digest());
        n.set("snapshot",s.json());lineageHeader(n,s);return n;
    }
    private static void lineageHeader(ObjectNode n,Snapshot s){
        n.put("admissionSha256",(String)s.evidence("admission").get("admission_sha256"));
        @SuppressWarnings("unchecked") var milestones=(List<Map<String,Object>>)s.lineage.get("milestones");
        var ready=one(milestones.stream().filter(v->"READY".equals(v.get("phase"))).toList());n.put("readySha256",(String)ready.get("receipt_sha256"));
        for(String[] field:List.of(new String[]{"commandSha256","operation","command_sha256"},new String[]{"bootstrapSha256","operation","bootstrap_sha256"},new String[]{"candidateSha256","intent","candidate_sha256"},new String[]{"completionSha256","intent","completion_sha256"},new String[]{"journalSha256","checkpoint","journal_sha256"},new String[]{"expectedSha256","checkpoint","expected_sha256"}))n.put(field[0],(String)s.evidence(field[1]).get(field[2]));
    }
    private static void checkLineageHeader(JsonNode n,Snapshot s){var expected=SourceJson.object();lineageHeader(expected,s);for(String key:expected.propertyNames())need(expected.get(key).equals(n.get(key)),EVIDENCE_CHANGED);}
    private static List<Map<String,Object>> expectedAudits(JsonNode before,Snapshot s,Map<String,Object> h,String sha){
        var old=new HashMap<>(s.attempt);old.put("state",text(before.get("attempt"),"state"));old.put("row_version",number(h,"original_attempt_version"));
        var run=new HashMap<>(s.run);run.put("state",text(before.get("run"),"state"));run.put("row_version",number(h,"original_run_version"));
        return audits(new Snapshot(s.coordinator,run,old,s.control,s.oldWorker,s.recovery,s.history,s.lineage),(UUID)h.get("recovery_attempt_id"),(UUID)h.get("handoff_id"),sha);
    }
    private static List<Map<String,Object>> audits(Snapshot s,UUID attempt,UUID handoff,String sha){
        byte[] details=SourceJson.canonical(SourceJson.object().put("identityVersion",1).put("policy",POLICY).put("handoffId",handoff.toString()).put("ancestrySha256",sha).put("checkpointId",s.checkpoint().toString()));
        return List.of(audit(s.oldId(),"ATTEMPT_INTERRUPTED",(String)s.attempt.get("state"),"INTERRUPTED",number(s.attempt,"row_version"),number(s.attempt,"row_version")+1,details),
                audit(attempt,"ATTEMPT_CLAIMED",null,"CLAIMED",null,0L,details),audit(attempt,"NATIVE_FETCH_HANDOFF",(String)s.run.get("state"),"RECOVERY_REQUIRED",number(s.run,"row_version"),number(s.run,"row_version")+1,details));
    }
    private static Map<String,Object> audit(UUID attempt,String action,String before,String after,Long bv,Long av,byte[] details){var n=new HashMap<String,Object>();n.put("attempt_id",attempt);n.put("action",action);n.put("before_state",before);n.put("after_state",after);n.put("before_version",bv);n.put("after_version",av);n.put("details_bytes",details);return n;}
    private void held(InstallationHostGuard guard,WorkerRepository.Session w){outside();need(guard!=null && w!=null,INVALID_REQUEST);guard.checkHeld(installation);need(installation.equals(w.installationId()) && guard.process().instanceId().equals(w.instanceId()) && guard.binding().digest().equals(w.hostDigest()) && "RECOVERY_ONLY".equals(w.status()),OWNERSHIP_LOST);}
    private static void outside(){need(!TransactionSynchronizationManager.isActualTransactionActive(),INVALID_REQUEST);}
    private static byte[] bounded(JsonNode n,int max){byte[] bytes=SourceJson.canonical(n);need(bytes.length<=max,CAPACITY_EXCEEDED);return bytes;}
    private static JsonNode canonical(byte[] bytes,String sha,int max){var n=SourceJson.parse(bytes,max);need(SourceJson.sha(bytes).equals(sha) && Arrays.equals(bytes,SourceJson.canonical(n)),EVIDENCE_CHANGED);return n;}
    private static String hash(JsonNode n){return SourceJson.sha(SourceJson.canonical(n));}
    private static String text(JsonNode n,String key){return SourceJson.text(n.get(key));}
    private static long number(Map<String,Object> row,String key){return ((Number)row.get(key)).longValue();}
    private static Map<String,Object> one(List<Map<String,Object>> rows){need(rows.size()==1,EVIDENCE_UNAVAILABLE);return rows.getFirst();}
    private static void exactly(int rows){need(rows==1,OWNERSHIP_LOST);}
    private static RecoveryQuiescence.Identity identity(Map<String,Object> row,String pid,String start){return new RecoveryQuiescence.Identity(number(row,pid),((Timestamp)row.get(start)).toInstant());}
    private static Receipt receipt(Status status,Target t){return new Receipt(status,t.handoff,t.snapshot.runId(),status==Status.COMMITTED || status==Status.CONFIRMED_HANDOFF?t.recoveryAttempt:null,t.checkpoint());}
}
