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
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** C3b internal seam only: deliberately no bean, public factory, application role, HTTP or dispatcher caller.
 * Transfers an occupied slot; has no native executor, stage-launch, final-outcome or release operation.
 */
final class RecoveryHandoff {
    enum Step { OLD_ATTEMPT, STAGES, SUCCESSOR, COORDINATOR, RUN, AUDIT }
    enum Status { COMMITTED, CONFIRMED_HANDOFF, CONFIRMED_ROLLBACK, UNRESOLVED }
    /** Historical facts, never authority; in particular lookup never supplies or refreshes an owner token. */
    record Receipt(Status status,UUID controlId,UUID runId,UUID attemptId,UUID instanceId,long epoch) { }
    record Owner(WorkerRepository.Session worker,UUID runId,UUID controlId,UUID predecessor,
                 long runVersion,long attemptVersion,long controlVersion,String qualificationSha256) { }
    record Result(Receipt receipt,Owner owner) { }
    private record Snapshot(Map<String,Object> coordinator,Map<String,Object> run,Map<String,Object> attempt,
                            Map<String,Object> control,Map<String,Object> oldWorker,Map<String,Object> recovery,
                            List<Map<String,Object>> stages,List<Map<String,Object>> history) {
        String digest(){return hash(List.of(coordinator,run,attempt,control,oldWorker,recovery,stages,history));}
        UUID runId(){return (UUID)run.get("run_id");} UUID oldId(){return (UUID)attempt.get("attempt_id");} UUID controlId(){return (UUID)control.get("control_id");}
        Set<UUID> unresolved(){var ids=new HashSet<UUID>();for(var s:stages)if(Set.of("INTENT","RUNNING","UNKNOWN").contains(s.get("state")))ids.add((UUID)s.get("checkpoint_id"));return Set.copyOf(ids);}
        List<RecoveryQuiescence.Identity> processes(){var ids=new ArrayList<RecoveryQuiescence.Identity>();ids.add(identity(oldWorker,"pid","process_started_at"));for(var s:stages)if(s.get("child_pid")!=null)ids.add(identity(s,"child_pid","child_started_at"));return List.copyOf(ids);}
    }
    static final class Target {
        private final Snapshot snapshot;private final WorkerRepository.Session recovery;
        private Target(Snapshot snapshot,WorkerRepository.Session recovery){this.snapshot=snapshot;this.recovery=recovery;}
        String digest(){return snapshot.digest();} UUID controlId(){return snapshot.controlId();} UUID runId(){return snapshot.runId();} UUID attemptId(){return snapshot.oldId();}
        Set<UUID> unresolvedCheckpoints(){return snapshot.unresolved();}
        @Override public String toString(){return "RecoveryTarget[redacted]";}
    }
    static final class Qualified {
        private final Target target;private final RecoveryQuiescence.Review review;private final ReliabilityRuntime runtime;
        private final RecoveryFiles.Capture files;private final String address,sha;private final Instant qualifiedAt;private final AtomicBoolean used=new AtomicBoolean();
        private Qualified(Target target,RecoveryQuiescence.Review review,ReliabilityRuntime runtime,RecoveryFiles.Capture files,String address,String sha,Instant at){this.target=target;this.review=review;this.runtime=runtime;this.files=files;this.address=address;this.sha=sha;qualifiedAt=at;}
        String address(){return address;} String sha256(){return sha;}
        @Override public String toString(){return "RecoveryQualification[redacted]";}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    private final Clock clock;private final RecoveryQuiescence.Processes processes;private final Consumer<Step> fault;
    RecoveryHandoff(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation){this(jdbc,tx,installation,Clock.systemUTC(),RecoveryQuiescence.LOCAL,step->{});}
    /** Deterministic clock/observation/transaction-fault seams; never configurable in a production runtime. */
    RecoveryHandoff(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,RecoveryQuiescence.Processes processes,Consumer<Step> fault){this.jdbc=jdbc;this.tx=tx;this.installation=installation;this.clock=clock;this.processes=processes;this.fault=fault;}

    Target inspect(InstallationHostGuard guard,WorkerRepository.Session recovery,UUID control) {
        held(guard,recovery);need(control!=null,INVALID_REQUEST);
        return tx.required(()->{var s=read(control,recovery.instanceId());validateOriginal(guard,recovery,s);return new Target(s,recovery);});
    }
    Qualified qualify(InstallationHostGuard guard,Target target,RecoveryQuiescence.Review review,String runtimeProfile) {
        need(target!=null,INVALID_REQUEST);held(guard,target.recovery);
        var s=tx.required(()->{var current=read(target.controlId(),target.recovery.instanceId());validateOriginal(guard,target.recovery,current);need(current.digest().equals(target.digest()),EVIDENCE_CHANGED);return current;});
        checkProcesses(s,review,target.digest());
        ReliabilityRuntime runtime;
        try {
            var frozen=FrozenRunSelection.canonical((byte[])s.run.get("captured_selection"),(String)s.run.get("selection_sha256"),PreflightFiles.LARGE);
            runtime=ReliabilityRuntime.load(jdbc,tx,guard,new WorkerRepository.Owner(target.recovery,s.runId(),s.oldId(),number(s.run,"row_version"),number(s.attempt,"row_version"),(String)s.attempt.get("state")),runtimeProfile,frozen);
            runtime.check(); // Hash/root/module inspection only. Never readiness(): it would launch Python.
        }catch(ManagementFailure ex){throw ex;}catch(RuntimeException ex){throw new RecoveryFailure(RUNTIME_MISMATCH);}
        var captured=RecoveryFiles.capture(guard,s.runId(),s.oldId(),number(s.coordinator,"epoch"),(String)s.run.get("selection_sha256"),runtime.json(),s.stages);checkHints(s,review,captured);
        Instant at=clock.instant();RecoveryQuiescence.fresh(review.inspectedAt(),at);
        UUID qualificationId=UUID.randomUUID();
        var descriptor=SourceJson.object().put("qualificationVersion",1).put("policy","E5_EXECUTION_QUIESCENCE_V1").put("installationId",installation.toString()).put("controlId",s.controlId().toString()).put("runId",s.runId().toString()).put("oldAttemptId",s.oldId().toString()).put("oldInstanceId",s.oldWorker.get("instance_id").toString()).put("oldEpoch",s.coordinator.get("epoch").toString()).put("recoveryInstanceId",target.recovery.instanceId().toString()).put("hostBindingSha256",guard.binding().digest()).put("snapshotSha256",s.digest()).put("selectionSha256",(String)s.run.get("selection_sha256")).put("runtimeSha256",SourceJson.sha(SourceJson.canonical(runtime.json()))).put("qualifiedAt",at.toString());
        descriptor.put("qualificationId",qualificationId.toString()).put("oldCoordinatorVersion",s.coordinator.get("row_version").toString()).put("oldRunVersion",s.run.get("row_version").toString()).put("oldAttemptVersion",s.attempt.get("row_version").toString()).put("controlVersion",s.control.get("row_version").toString()).put("runProfileId",target.recovery.profileId()).put("runProfileSha256",target.recovery.profileDigest());
        descriptor.set("originalMetadata",json(List.of(s.coordinator,s.run,s.attempt,s.control,s.oldWorker,s.recovery,s.stages,s.history)));
        descriptor.set("operatorReview",review.json());descriptor.set("privateEvidence",captured.inventory());
        byte[] bytes=SourceJson.canonical(descriptor);String address=RecoveryFiles.write(guard,s.controlId(),qualificationId,bytes);
        var q=new Qualified(target,review,runtime,captured,address,SourceJson.sha(bytes),at);fresh(guard,q);return q;
    }
    Result transfer(InstallationHostGuard guard,Qualified q) {
        need(q!=null,INVALID_REQUEST);held(guard,q.target.recovery);
        // Repeated invocation is read-only. An uncertain/rolled-back consumed qualification is never retried.
        if(q.used.get()){var receipt=resolve(q);if(receipt.status()==Status.CONFIRMED_HANDOFF)return new Result(receipt,null);throw new RecoveryFailure(receipt.status()==Status.CONFIRMED_ROLLBACK?FRESH_QUALIFICATION_REQUIRED:HANDOFF_UNRESOLVED);}
        fresh(guard,q);need(q.used.compareAndSet(false,true),FRESH_QUALIFICATION_REQUIRED);
        Result result=tx.required(()->{
            Snapshot before=q.target.snapshot;var s=read(before.controlId(),q.target.recovery.instanceId());
            validateOriginal(guard,q.target.recovery,s);need(s.digest().equals(before.digest()),OWNERSHIP_LOST);RecoveryQuiescence.fresh(q.qualifiedAt,clock.instant());
            need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE run_id=?",Long.class,s.runId())<20 && jdbc.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=?",Long.class,s.runId())<=128,CAPACITY_EXCEEDED);
            need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE attempt_id=?",Long.class,s.controlId())==0,OWNERSHIP_LOST);
            long next=jdbc.queryForObject("SELECT coalesce(max(attempt_no),0)+1 FROM ddip_management.attempts WHERE run_id=?",Long.class,s.runId());need(next<=20,CAPACITY_EXCEEDED);
            long epoch=number(s.coordinator,"epoch");need(epoch<Long.MAX_VALUE,CAPACITY_EXCEEDED);long newEpoch=epoch+1;
            boolean active=Set.of("CLAIMED","RUNNING").contains(s.attempt.get("state"));long unknown=s.stages.stream().filter(v->Set.of("INTENT","RUNNING").contains(v.get("state"))).count();
            long unfinished=jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class);
            need(AuditCapacity.available(jdbc,2+unknown+(active?1:0),unfinished),CAPACITY_EXCEEDED);
            if(active)exactly(jdbc.update("UPDATE ddip_management.attempts SET state='INTERRUPTED',finished_at=clock_timestamp(),outcome_code='INTERRUPTED',row_version=row_version+1 WHERE attempt_id=? AND run_id=? AND instance_id=? AND owner_epoch=? AND state=? AND row_version=?",s.oldId(),s.runId(),s.oldWorker.get("instance_id"),epoch,s.attempt.get("state"),number(s.attempt,"row_version")));
            fault.accept(Step.OLD_ATTEMPT);
            for(var stage:s.stages)if(Set.of("INTENT","RUNNING").contains(stage.get("state")))exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state='UNKNOWN',outcome_code='RECOVERY_HANDOFF',row_version=row_version+1 WHERE attempt_id=? AND stage=? AND item_ordinal=? AND checkpoint_id=? AND state=? AND row_version=?",s.oldId(),stage.get("stage"),stage.get("item_ordinal"),stage.get("checkpoint_id"),stage.get("state"),number(stage,"row_version")));
            fault.accept(Step.STAGES);
            jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state) VALUES (?,?,?,'RECONCILE',?,?,'CLAIMED')",s.controlId(),s.runId(),next,q.target.recovery.instanceId(),newEpoch);
            fault.accept(Step.SUCCESSOR);
            exactly(jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=?,owner_instance_id=?,epoch=?,mode='RECOVERY_ONLY',heartbeat_at=clock_timestamp(),row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL AND mode=?",s.controlId(),q.target.recovery.instanceId(),newEpoch,installation,s.oldId(),s.oldWorker.get("instance_id"),epoch,number(s.coordinator,"row_version"),s.coordinator.get("mode")));
            fault.accept(Step.COORDINATOR);
            exactly(jdbc.update("UPDATE ddip_management.runs SET state='RECOVERY_REQUIRED',current_attempt_id=?,outcome_code='RECOVERY_REQUIRED',retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND current_attempt_id=? AND state=? AND row_version=?",s.controlId(),s.runId(),s.oldId(),s.run.get("state"),number(s.run,"row_version")));
            fault.accept(Step.RUN);
            if(active)audit(s,s.oldId(),"ATTEMPT_INTERRUPTED",(String)s.attempt.get("state"),"INTERRUPTED",number(s.attempt,"row_version"),number(s.attempt,"row_version")+1,q);
            for(var stage:s.stages)if(Set.of("INTENT","RUNNING").contains(stage.get("state")))audit(s,s.oldId(),"STAGE_UNKNOWN",(String)stage.get("state"),"UNKNOWN",number(stage,"row_version"),number(stage,"row_version")+1,q);
            audit(s,s.controlId(),"ATTEMPT_CLAIMED",null,"CLAIMED",null,0L,q);
            audit(s,s.controlId(),"RECOVERY_HANDOFF",(String)s.run.get("state"),"RECOVERY_REQUIRED",number(s.run,"row_version"),number(s.run,"row_version")+1,q);
            var worker=q.target.recovery;var bound=new WorkerRepository.Session(installation,worker.instanceId(),worker.profileId(),worker.profileDigest(),worker.deploymentDigest(),worker.hostDigest(),"RECOVERY_ONLY",newEpoch,number(s.coordinator,"row_version")+1,worker.workerVersion());
            var owner=new Owner(bound,s.runId(),s.controlId(),s.oldId(),number(s.run,"row_version")+1,0,number(s.control,"row_version"),q.sha);
            return new Result(receipt(Status.COMMITTED,q),owner);
        });
        held(guard,result.owner().worker());return result;
    }
    /** Bounded, coordinator-locked acknowledgment lookup. Never returns an owner/session or does external I/O. */
    Receipt resolve(Qualified q) {
        need(q!=null && !TransactionSynchronizationManager.isActualTransactionActive(),INVALID_REQUEST);
        try{return tx.required(()->{
            var s=read(q.target.controlId(),q.target.recovery.instanceId());
            var successors=jdbc.queryForList("SELECT attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,row_version FROM ddip_management.attempts WHERE attempt_id=? FOR UPDATE",q.target.controlId());
            if(successors.isEmpty())return receipt(s.digest().equals(q.target.digest())?Status.CONFIRMED_ROLLBACK:Status.UNRESOLVED,q);
            var a=successors.getFirst();var b=q.target.snapshot;
            boolean same=hash(s.control).equals(hash(b.control)) && hash(s.oldWorker).equals(hash(b.oldWorker)) && hash(s.recovery).equals(hash(b.recovery))
                    && s.controlId().equals(s.coordinator.get("active_attempt_id")) && q.target.recovery.instanceId().equals(s.coordinator.get("owner_instance_id")) && number(s.coordinator,"epoch")==number(b.coordinator,"epoch")+1 && number(s.coordinator,"row_version")==number(b.coordinator,"row_version")+1 && "RECOVERY_ONLY".equals(s.coordinator.get("mode")) && s.coordinator.get("active_import_id")==null
                    && s.controlId().equals(s.run.get("current_attempt_id")) && "RECOVERY_REQUIRED".equals(s.run.get("state")) && number(s.run,"row_version")==number(b.run,"row_version")+1
                    && s.runId().equals(a.get("run_id")) && "RECONCILE".equals(a.get("kind")) && q.target.recovery.instanceId().equals(a.get("instance_id")) && number(a,"owner_epoch")==number(b.coordinator,"epoch")+1 && "CLAIMED".equals(a.get("state")) && number(a,"row_version")==0 && number(a,"attempt_no")==b.history.stream().mapToLong(v->number(v,"attempt_no")).max().orElse(0)+1;
            same &= preservedHistory(b,s);
            var audits=jdbc.queryForList("SELECT attempt_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes FROM ddip_management.audit_events WHERE run_id=? AND control_id=? AND action IN ('RECOVERY_HANDOFF','ATTEMPT_CLAIMED','ATTEMPT_INTERRUPTED','STAGE_UNKNOWN') ORDER BY event_seq LIMIT 516",s.runId(),s.controlId());
            var expected=new ArrayList<Map<String,Object>>();
            if(Set.of("CLAIMED","RUNNING").contains(b.attempt.get("state")))expected.add(auditRow(b.oldId(),"ATTEMPT_INTERRUPTED",(String)b.attempt.get("state"),"INTERRUPTED",number(b.attempt,"row_version"),number(b.attempt,"row_version")+1,q));
            for(var stage:b.stages)if(Set.of("INTENT","RUNNING").contains(stage.get("state")))expected.add(auditRow(b.oldId(),"STAGE_UNKNOWN",(String)stage.get("state"),"UNKNOWN",number(stage,"row_version"),number(stage,"row_version")+1,q));
            expected.add(auditRow(b.controlId(),"ATTEMPT_CLAIMED",null,"CLAIMED",null,0L,q));expected.add(auditRow(b.controlId(),"RECOVERY_HANDOFF",(String)b.run.get("state"),"RECOVERY_REQUIRED",number(b.run,"row_version"),number(b.run,"row_version")+1,q));
            same &= hash(audits).equals(hash(expected));
            return receipt(same?Status.CONFIRMED_HANDOFF:Status.UNRESOLVED,q);
        });}catch(ManagementFailure | RecoveryFailure ex){return receipt(Status.UNRESOLVED,q);}
    }
    /** C3b permits only fenced recovery heartbeats, never stage execution or slot release. */
    Owner heartbeat(InstallationHostGuard guard,Owner owner) {
        held(guard,owner.worker());return tx.required(()->{
            var s=read(owner.controlId(),owner.worker().instanceId());var w=owner.worker();
            need(s.controlId().equals(s.coordinator.get("active_attempt_id")) && w.instanceId().equals(s.coordinator.get("owner_instance_id")) && w.epoch()==number(s.coordinator,"epoch") && w.coordinatorVersion()==number(s.coordinator,"row_version") && "RECOVERY_ONLY".equals(s.coordinator.get("mode")) && s.coordinator.get("active_import_id")==null,OWNERSHIP_LOST);
            need("REQUESTED".equals(s.control.get("state")) && number(s.control,"row_version")==owner.controlVersion() && owner.predecessor().equals(s.oldId()) && owner.runId().equals(s.runId()) && owner.controlId().equals(s.run.get("current_attempt_id")) && "RECOVERY_REQUIRED".equals(s.run.get("state")) && number(s.run,"row_version")==owner.runVersion(),OWNERSHIP_LOST);
            var a=one(jdbc.queryForList("SELECT instance_id,owner_epoch,kind,state,row_version FROM ddip_management.attempts WHERE attempt_id=? FOR UPDATE",owner.controlId()));
            need(w.instanceId().equals(a.get("instance_id")) && w.epoch()==number(a,"owner_epoch") && "RECONCILE".equals(a.get("kind")) && "CLAIMED".equals(a.get("state")) && number(a,"row_version")==owner.attemptVersion(),OWNERSHIP_LOST);
            recoveryWorker(guard,w,s.recovery);
            var anchors=jdbc.queryForList("SELECT details_bytes FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND control_id=? AND action='RECOVERY_HANDOFF' ORDER BY event_seq LIMIT 2",owner.runId(),owner.controlId(),owner.controlId());
            need(anchors.size()==1 && SourceJson.parse((byte[])anchors.getFirst().get("details_bytes")).get("qualificationSha256").stringValue().equals(owner.qualificationSha256()),OWNERSHIP_LOST);
            exactly(jdbc.update("UPDATE ddip_management.worker_instances SET heartbeat_at=clock_timestamp(),row_version=row_version+1 WHERE instance_id=? AND installation_id=? AND status='RECOVERY_ONLY' AND row_version=?",w.instanceId(),installation,w.workerVersion()));
            exactly(jdbc.update("UPDATE ddip_management.coordinator SET heartbeat_at=clock_timestamp(),row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND mode='RECOVERY_ONLY' AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL",installation,owner.controlId(),w.instanceId(),w.epoch(),w.coordinatorVersion()));
            var next=new WorkerRepository.Session(installation,w.instanceId(),w.profileId(),w.profileDigest(),w.deploymentDigest(),w.hostDigest(),"RECOVERY_ONLY",w.epoch(),w.coordinatorVersion()+1,w.workerVersion()+1);
            return new Owner(next,owner.runId(),owner.controlId(),owner.predecessor(),owner.runVersion(),owner.attemptVersion(),owner.controlVersion(),owner.qualificationSha256());
        });
    }
    private Snapshot read(UUID control,UUID recovery) {
        var c=one(jdbc.queryForList("SELECT installation_id,mode,epoch,active_attempt_id,owner_instance_id,active_import_id,heartbeat_at,row_version FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE"));
        need(installation.equals(c.get("installation_id")),OWNERSHIP_LOST);
        // Targets are immutable. Resolve IDs after coordinator lock, then lock R -> A -> K.
        var ids=one(jdbc.queryForList("SELECT run_id,target_attempt_id FROM ddip_management.controls WHERE control_id=?",control));
        var r=one(jdbc.queryForList("SELECT run_id,product_id,pipeline,pipeline_version,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state,current_stage,current_attempt_id,row_version,finished_at,result_artifact_id,outcome_code,counts_bytes,retry_eligible FROM ddip_management.runs WHERE run_id=? FOR UPDATE",ids.get("run_id")));
        var a=one(jdbc.queryForList("SELECT attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,started_at,finished_at,outcome_code,row_version FROM ddip_management.attempts WHERE attempt_id=? FOR UPDATE",ids.get("target_attempt_id")));
        var k=one(jdbc.queryForList("SELECT control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,target_attempt_id,quiescence_confirmed,state,reason,requester,requested_at,applied_at,row_version FROM ddip_management.controls WHERE control_id=? FOR UPDATE",control));
        String workerSql="SELECT instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,status,heartbeat_at,row_version FROM ddip_management.worker_instances WHERE instance_id=? FOR UPDATE";
        // UUID ordering is deterministic even when a competing recovery instance is registered.
        var workers=jdbc.queryForList("SELECT instance_id FROM ddip_management.worker_instances WHERE instance_id IN (?,?) ORDER BY instance_id FOR UPDATE",a.get("instance_id"),recovery);need(workers.size()==2,OWNERSHIP_LOST);
        var old=one(jdbc.queryForList(workerSql,a.get("instance_id")));var n=one(jdbc.queryForList(workerSql,recovery));
        var stages=jdbc.queryForList("SELECT stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,intended_identity,intended_namespace,command_address,command_sha256,result_address,result_sha256,child_pid,child_started_at,child_instance_id,started_at,finished_at,outcome_code,counts_bytes,row_version FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY stage COLLATE \"C\",item_ordinal LIMIT 513 FOR UPDATE",a.get("attempt_id"));
        need(stages.size()<=512,CAPACITY_EXCEEDED);
        need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations s JOIN ddip_management.attempts a USING(attempt_id) WHERE a.run_id=?",Long.class,r.get("run_id"))<=512,CAPACITY_EXCEEDED);
        var history=jdbc.queryForList("SELECT attempt_id,attempt_no,kind,instance_id,owner_epoch,state,row_version FROM ddip_management.attempts WHERE run_id=? ORDER BY attempt_no LIMIT 21",r.get("run_id"));need(history.size()<=20,CAPACITY_EXCEEDED);
        return new Snapshot(c,r,a,k,old,n,stages,history);
    }
    private void validateOriginal(InstallationHostGuard guard,WorkerRepository.Session recovery,Snapshot s) {
        need(Set.of("READY","RECOVERY_ONLY").contains(s.coordinator.get("mode")) && s.coordinator.get("active_import_id")==null && s.oldId().equals(s.coordinator.get("active_attempt_id")) && s.oldWorker.get("instance_id").equals(s.coordinator.get("owner_instance_id")) && number(s.attempt,"owner_epoch")==number(s.coordinator,"epoch") && recovery.epoch()==number(s.coordinator,"epoch") && recovery.coordinatorVersion()==number(s.coordinator,"row_version"),OWNERSHIP_LOST);
        need(s.runId().equals(s.control.get("run_id")) && s.runId().equals(s.attempt.get("run_id")) && s.oldId().equals(s.run.get("current_attempt_id")) && Set.of("RUNNING","CANCEL_REQUESTED","RECOVERY_REQUIRED").contains(s.run.get("state")) && number(s.run,"row_version")==number(s.control,"expected_version"),OWNERSHIP_LOST);
        need("RECONCILE".equals(s.control.get("type")) && "REQUESTED".equals(s.control.get("state")) && Boolean.TRUE.equals(s.control.get("quiescence_confirmed")) && "local-operator".equals(s.control.get("requester")),INVALID_REQUEST);
        boolean active=Set.of("CLAIMED","RUNNING").contains(s.attempt.get("state"));boolean prior="RECOVERY_REQUIRED".equals(s.run.get("state")) && ("INTERRUPTED".equals(s.attempt.get("state")) || "RECONCILE".equals(s.attempt.get("kind")) && "FAILED".equals(s.attempt.get("state")));
        need(active || prior,OWNERSHIP_LOST);need("EXECUTE".equals(s.attempt.get("kind")) || "RECOVERY_REQUIRED".equals(s.run.get("state")),OWNERSHIP_LOST);
        recoveryWorker(guard,recovery,s.recovery);
        need(installation.equals(s.oldWorker.get("installation_id")) && recovery.profileId().equals(s.oldWorker.get("execution_profile_id")) && recovery.profileDigest().equals(s.oldWorker.get("profile_sha256")) && recovery.hostDigest().equals(s.oldWorker.get("host_binding_sha256")) && recovery.profileId().equals(s.run.get("execution_profile_id")),RUNTIME_MISMATCH);
        need(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=? AND b.role='CONTROL' AND b.configuration_sha256=? AND b.configuration_bytes=?)",Boolean.class,recovery.profileId(),guard.binding().id(),installation,guard.binding().digest(),guard.binding().descriptor()),RUNTIME_MISMATCH);
        try {
            var body=SourceJson.object().put("action","RECONCILE").put("attemptId",s.oldId().toString()).put("quiescenceConfirmed",true);
            var command=ControlCommands.parse(s.runId().toString(),SourceJson.canonical(body),(String)s.control.get("idempotency_key"),"\""+s.control.get("expected_version")+"\"");
            need(Arrays.equals(command.normalized(),(byte[])s.control.get("normalized_request")) && SourceJson.sha(command.normalized()).equals(s.control.get("request_sha256")),EVIDENCE_CHANGED);
            var sources=jdbc.queryForList("SELECT r.source_id,r.revision,r.captured_gate_version,r.captured_enabled,s.profile_family,v.configuration_bytes,v.configuration_sha256,v.endpoint_id,v.endpoint_sha256 FROM ddip_management.run_sources r JOIN ddip_management.sources s USING(product_id,source_id) JOIN ddip_management.source_revisions v ON (v.product_id,v.source_id,v.revision)=(r.product_id,r.source_id,r.revision) WHERE r.run_id=? ORDER BY r.product_id COLLATE \"C\",r.source_id COLLATE \"C\"",s.runId());
            need("RELIABILITY_V2".equals(s.run.get("pipeline")),RUNTIME_MISMATCH);
            FrozenRunSelection.validate(jdbc,installation,s.run,sources,recovery.profileId(),recovery.profileDigest(),recovery.deploymentDigest());
        }catch(RecoveryFailure ex){throw ex;}catch(org.springframework.dao.DataAccessException ex){throw ex;}catch(RuntimeException ex){throw new RecoveryFailure(RUNTIME_MISMATCH);}
    }
    private void recoveryWorker(InstallationHostGuard guard,WorkerRepository.Session w,Map<String,Object> row) {
        need("RECOVERY_ONLY".equals(w.status()) && "RECOVERY_ONLY".equals(row.get("status")) && installation.equals(row.get("installation_id")) && w.instanceId().equals(row.get("instance_id")) && w.profileId().equals(row.get("execution_profile_id")) && w.profileDigest().equals(row.get("profile_sha256")) && w.hostDigest().equals(row.get("host_binding_sha256")) && w.workerVersion()==number(row,"row_version") && guard.process().pid()==number(row,"pid") && guard.process().startedAt().equals(((Timestamp)row.get("process_started_at")).toInstant()),OWNERSHIP_LOST);
    }
    private void held(InstallationHostGuard guard,WorkerRepository.Session recovery) {
        need(!TransactionSynchronizationManager.isActualTransactionActive() && guard!=null && recovery!=null,INVALID_REQUEST);guard.checkHeld(installation);
        need(installation.equals(recovery.installationId()) && guard.process().instanceId().equals(recovery.instanceId()) && guard.binding().digest().equals(recovery.hostDigest()) && "RECOVERY_ONLY".equals(recovery.status()),OWNERSHIP_LOST);
    }
    private void checkProcesses(Snapshot s,RecoveryQuiescence.Review review,String target){RecoveryQuiescence.check(review,target,s.unresolved(),s.processes(),processes,clock.instant());}
    private void checkHints(Snapshot s,RecoveryQuiescence.Review review,RecoveryFiles.Capture captured){
        for(long pid:captured.descendantPids()) {
            // A gateway PID-only hint is not the worker/Python identity, even if its number was reused.
            var identities=review.descendants().stream().filter(p->p.pid()==pid).toList();need(identities.size()==1,PROCESS_UNRESOLVED);
            RecoveryQuiescence.State observed;try{observed=processes.inspect(identities.getFirst());}catch(RuntimeException ex){throw new RecoveryFailure(PROCESS_UNRESOLVED);}
            // Without a retained native start instant we cannot disambiguate any currently live PID.
            need(observed==RecoveryQuiescence.State.ABSENT,PROCESS_UNRESOLVED);
        }
    }
    private void fresh(InstallationHostGuard guard,Qualified q) {
        held(guard,q.target.recovery);RecoveryQuiescence.fresh(q.qualifiedAt,clock.instant());checkProcesses(q.target.snapshot,q.review,q.target.digest());
        need(SourceJson.sha(RecoveryFiles.read(guard,q.address)).equals(q.sha),EVIDENCE_CHANGED);
        try{q.runtime.check();}catch(RuntimeException ex){throw new RecoveryFailure(RUNTIME_MISMATCH);}
        var s=q.target.snapshot;var current=RecoveryFiles.capture(guard,s.runId(),s.oldId(),number(s.coordinator,"epoch"),(String)s.run.get("selection_sha256"),q.runtime.json(),s.stages);need(current.inventory().equals(q.files.inventory()),EVIDENCE_CHANGED);checkHints(s,q.review,current);
    }
    private boolean preservedHistory(Snapshot before,Snapshot after) {
        var old=new HashMap<>(before.attempt);boolean active=Set.of("CLAIMED","RUNNING").contains(old.get("state"));
        if(active){old.put("state","INTERRUPTED");old.put("outcome_code","INTERRUPTED");old.put("row_version",number(before.attempt,"row_version")+1);old.put("finished_at",after.attempt.get("finished_at"));if(old.get("finished_at")==null)return false;}
        if(!hash(old).equals(hash(after.attempt)) || before.stages.size()!=after.stages.size())return false;
        for(int i=0;i<before.stages.size();i++){var expected=new HashMap<>(before.stages.get(i));if(Set.of("INTENT","RUNNING").contains(expected.get("state"))){expected.put("state","UNKNOWN");expected.put("outcome_code","RECOVERY_HANDOFF");expected.put("row_version",number(expected,"row_version")+1);}if(!hash(expected).equals(hash(after.stages.get(i))))return false;}
        var r=new HashMap<>(before.run);r.put("state","RECOVERY_REQUIRED");r.put("current_attempt_id",before.controlId());r.put("outcome_code","RECOVERY_REQUIRED");r.put("retry_eligible",false);r.put("row_version",number(before.run,"row_version")+1);
        if(!hash(r).equals(hash(after.run)) || after.history.size()!=before.history.size()+1)return false;
        for(var h:before.history){var matches=after.history.stream().filter(v->v.get("attempt_id").equals(h.get("attempt_id"))).toList();if(matches.size()!=1)return false;var expected=new HashMap<>(h);if(h.get("attempt_id").equals(before.oldId()) && active){expected.put("state","INTERRUPTED");expected.put("row_version",number(h,"row_version")+1);}if(!hash(expected).equals(hash(matches.getFirst())))return false;}
        return true;
    }
    private byte[] auditDetails(Qualified q) {
        var s=q.target.snapshot;return SourceJson.canonical(SourceJson.object().put("protocolVersion",1).put("policy","E5_EXECUTION_QUIESCENCE_V1").put("oldAttemptId",s.oldId().toString()).put("oldInstanceId",s.oldWorker.get("instance_id").toString()).put("oldEpoch",s.coordinator.get("epoch").toString()).put("oldCoordinatorVersion",s.coordinator.get("row_version").toString()).put("oldRunVersion",s.run.get("row_version").toString()).put("oldAttemptVersion",s.attempt.get("row_version").toString()).put("snapshotSha256",s.digest()).put("qualificationAddress",q.address).put("qualificationSha256",q.sha));
    }
    private void audit(Snapshot s,UUID attempt,String action,String before,String after,Long bv,Long av,Qualified q) {
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,'RECOVERY_HANDOFF',?)",s.runId(),attempt,s.controlId(),action,before,after,bv,av,auditDetails(q));fault.accept(Step.AUDIT);
    }
    private Map<String,Object> auditRow(UUID attempt,String action,String before,String after,Long bv,Long av,Qualified q){var row=new HashMap<String,Object>();row.put("attempt_id",attempt);row.put("action",action);row.put("before_state",before);row.put("after_state",after);row.put("before_version",bv);row.put("after_version",av);row.put("safe_code","RECOVERY_HANDOFF");row.put("details_bytes",auditDetails(q));return row;}
    private static Receipt receipt(Status status,Qualified q){
        var s=q.target.snapshot;
        if(status==Status.CONFIRMED_ROLLBACK)return new Receipt(status,s.controlId(),s.runId(),s.oldId(),(UUID)s.oldWorker.get("instance_id"),number(s.coordinator,"epoch"));
        if(status==Status.UNRESOLVED)return new Receipt(status,s.controlId(),s.runId(),null,null,0);
        return new Receipt(status,s.controlId(),s.runId(),s.controlId(),q.target.recovery.instanceId(),number(s.coordinator,"epoch")+1);
    }
    private static RecoveryQuiescence.Identity identity(Map<String,Object> row,String pid,String start){return new RecoveryQuiescence.Identity(number(row,pid),((Timestamp)row.get(start)).toInstant());}
    private static long number(Map<String,Object> row,String key){return ((Number)row.get(key)).longValue();}
    private static Map<String,Object> one(List<Map<String,Object>> rows){need(rows.size()==1,OWNERSHIP_LOST);return rows.getFirst();}
    private static void exactly(int rows){need(rows==1,OWNERSHIP_LOST);}
    private static String hash(Object value){return SourceJson.sha(SourceJson.canonical(json(value)));}
    private static JsonNode json(Object value) {
        if(value instanceof Map<?,?> map){var n=SourceJson.object();for(var e:map.entrySet())n.set(e.getKey().toString(),json(e.getValue()));return n;}
        if(value instanceof Collection<?> list){var a=SourceJson.object().putArray("v");list.forEach(v->a.add(json(v)));return a;}
        if(value instanceof byte[] raw)return SourceJson.object().put("sha256",SourceJson.sha(raw)).put("length",Integer.toString(raw.length));
        var n=SourceJson.object();if(value==null)return n.putNull("v").get("v");if(value instanceof Boolean b)return n.put("v",b).get("v");if(value instanceof Timestamp t)return n.put("v",t.toInstant().toString()).get("v");return n.put("v",value.toString()).get("v");
    }
}
