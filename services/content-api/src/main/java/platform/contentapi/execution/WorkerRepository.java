package platform.contentapi.execution;

import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import static platform.contentapi.execution.WorkerFailure.Code.*;
import static platform.contentapi.execution.WorkerFailure.require;

/** One coordinator, short transactions, coordinator -> sorted sources -> run -> attempt locks.
 * JDBC metadata only: fixed claims/stages and verified completion, never native I/O or dispatch.
 */
public final class WorkerRepository {
    public record Session(UUID installationId,UUID instanceId,String profileId,String profileDigest,String deploymentDigest,
                          String hostDigest,String status,long epoch,long coordinatorVersion,long workerVersion) { }
    public record Owner(Session worker,UUID runId,UUID attemptId,long runVersion,long attemptVersion,String attemptState) { }
    public record Claim(String status,UUID runId,String code,Session worker,Owner owner) { }
    /** Historical receipt only; never authorizes a stage or adopts the stored owner's token. */
    public record Receipt(UUID attemptId,UUID runId,UUID instanceId,long epoch,int attemptNo,String state,String runState) { }
    public enum PrelaunchFailure { PROCESS_START_FAILED, RUNTIME_MISMATCH }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    private final WorkerExecutionCapability capability;
    private volatile boolean authorizationStopped;
    public WorkerRepository(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation) {this(jdbc,tx,installation,WorkerExecutionCapability.NONE);}
    WorkerRepository(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,WorkerExecutionCapability capability) {
        this.jdbc=jdbc;this.tx=tx;this.installation=installation;this.capability=Objects.requireNonNull(capability);
    }
    public boolean executionAvailable(){return capability.available();}
    public Session register(InstallationHostGuard guard,String profileId,String profileDigest,String deploymentDigest) {
        outsideTransaction();
        guard.checkHeld(installation);
        return ownerTransaction(()->{
            var c=slot();var profiles=jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",profileId);
            require(profiles.size()==1 && profileDigest.equals(profiles.getFirst().get("configuration_sha256")),RUNTIME_MISMATCH);
            try{FrozenRunSelection.profile((byte[])profiles.getFirst().get("configuration_bytes"),profileDigest,deploymentDigest);}catch(RuntimeException ex){throw new WorkerFailure(RUNTIME_MISMATCH);}
            require(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.storage_bindings b JOIN ddip_management.execution_profile_storage p USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=? AND b.role='CONTROL' AND b.configuration_sha256=? AND b.configuration_bytes=?)",Boolean.class,profileId,guard.binding().id(),installation,guard.binding().digest(),guard.binding().descriptor()),WORKER_CONFIGURATION_INVALID);
            // Same historical installation binding as imports; a free second guard is not permission.
            require(!jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.worker_instances WHERE installation_id=? AND host_binding_sha256<>?) OR EXISTS(SELECT 1 FROM ddip_management.artifact_import_operations WHERE installation_id=? AND host_binding_sha256<>?)",Boolean.class,installation,guard.binding().digest(),installation,guard.binding().digest()),WORKER_CONFIGURATION_INVALID);
            String status="STOPPING".equals(c.get("mode"))?"STOPPING":idle(c)?"READY":"RECOVERY_ONLY";
            var process=guard.process();
            jdbc.update("INSERT INTO ddip_management.worker_instances(instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,heartbeat_at,status) VALUES (?,?,?,?,?,?,?,clock_timestamp(),?)",process.instanceId(),installation,profileId,profileDigest,guard.binding().digest(),process.pid(),java.sql.Timestamp.from(process.startedAt()),status);
            return new Session(installation,process.instanceId(),profileId,profileDigest,deploymentDigest,guard.binding().digest(),status,number(c,"epoch"),number(c,"row_version"),0);
        });
    }
    public Claim claim(InstallationHostGuard guard,Session session,UUID attemptId) {
        held(guard,session);require(attemptId!=null,WORKER_CONFIGURATION_INVALID);
        // Only the fixed packaged adapter or isolated test capability supplies eligibility.
        if(!capability.available())return new Claim("NO_EXECUTOR",null,null,session,null);
        return ownerTransaction(()->{
            var c=slot();checkSession(c,session);require("READY".equals(session.status()) && idle(c),WORKER_NOT_READY);
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE attempt_id=?",Long.class,attemptId)==0,CLAIM_UNCERTAIN);
            var next=jdbc.queryForList("SELECT run_id FROM ddip_management.runs WHERE state='QUEUED' AND execution_profile_id=? AND ((pipeline='CONTENT_RSS_V1' AND ?) OR (pipeline='RELIABILITY_V2' AND ?)) ORDER BY submission_seq LIMIT 1",session.profileId(),capability.supports("CONTENT_RSS_V1"),capability.supports("RELIABILITY_V2"));
            if(next.isEmpty())return new Claim("EMPTY",null,null,session,null);
            UUID runId=(UUID)next.getFirst().get("run_id");
            var sources=jdbc.queryForList("SELECT s.source_id,s.enabled,s.profile_family,r.revision,r.captured_gate_version,r.captured_enabled,v.configuration_bytes,v.configuration_sha256,v.endpoint_id,v.endpoint_sha256 FROM ddip_management.run_sources r JOIN ddip_management.sources s ON (s.product_id,s.source_id)=(r.product_id,r.source_id) JOIN ddip_management.source_revisions v ON (v.product_id,v.source_id,v.revision)=(r.product_id,r.source_id,r.revision) WHERE r.run_id=? ORDER BY s.product_id COLLATE \"C\",s.source_id COLLATE \"C\" FOR UPDATE OF s",runId);
            var r=one(jdbc.queryForList("SELECT run_id,product_id,pipeline,pipeline_version,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state,row_version,current_attempt_id FROM ddip_management.runs WHERE run_id=? FOR UPDATE",runId));
            require("QUEUED".equals(r.get("state")) && r.get("current_attempt_id")==null,OWNERSHIP_LOST);
            String failure=null;
            if(sources.stream().anyMatch(s->!Boolean.TRUE.equals(s.get("enabled"))))failure="SOURCE_DISABLED";
            else {
                try {
                    require(capability.supports((String)r.get("pipeline")),RUNTIME_MISMATCH);
                    FrozenRunSelection.validate(jdbc,installation,r,sources,session.profileId(),session.profileDigest(),session.deploymentDigest());
                }catch(org.springframework.dao.DataAccessException ex){throw ex;}
                catch(RuntimeException ex){failure="RUNTIME_MISMATCH";}
            }
            if(failure!=null){
                exactly(jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=clock_timestamp(),outcome_code=?,retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND state='QUEUED' AND row_version=? AND current_attempt_id IS NULL",failure,runId,number(r,"row_version")));
                audit(runId,null,failure.equals("SOURCE_DISABLED")?"SOURCE_DISABLED_AT_DISPATCH":"RUN_FAILED","QUEUED","FAILED",number(r,"row_version"),number(r,"row_version")+1,failure);
                return new Claim("REJECTED",runId,failure,session,null);
            }
            int attemptNo=jdbc.queryForObject("SELECT COALESCE(max(attempt_no),0)+1 FROM ddip_management.attempts WHERE run_id=?",Integer.class,runId);
            require(attemptNo<=20,WORKER_NOT_READY);long epoch=Math.addExact(session.epoch(),1);
            jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state) VALUES (?,?,?,'EXECUTE',?,?,'CLAIMED')",attemptId,runId,attemptNo,session.instanceId(),epoch);
            exactly(jdbc.update("UPDATE ddip_management.runs SET state='RUNNING',current_attempt_id=?,row_version=row_version+1 WHERE run_id=? AND state='QUEUED' AND row_version=? AND current_attempt_id IS NULL",attemptId,runId,number(r,"row_version")));
            exactly(jdbc.update("UPDATE ddip_management.coordinator SET epoch=?,active_attempt_id=?,owner_instance_id=?,heartbeat_at=clock_timestamp(),row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND mode='READY' AND epoch=? AND row_version=? AND active_attempt_id IS NULL AND owner_instance_id IS NULL AND active_import_id IS NULL",epoch,attemptId,session.instanceId(),installation,session.epoch(),session.coordinatorVersion()));
            audit(runId,attemptId,"RUN_CLAIMED","QUEUED","RUNNING",number(r,"row_version"),number(r,"row_version")+1,"CLAIMED");
            audit(runId,attemptId,"ATTEMPT_CLAIMED",null,"CLAIMED",null,0L,"CLAIMED");
            Session bound=with(session,session.status(),epoch,session.coordinatorVersion()+1,session.workerVersion());
            Owner owner=new Owner(bound,runId,attemptId,number(r,"row_version")+1,0,"CLAIMED");
            return new Claim("CLAIMED",runId,"CLAIMED",bound,owner);
        });
    }
    public Optional<Receipt> lookup(UUID attemptId) {
        require(attemptId!=null,WORKER_CONFIGURATION_INVALID);
        return tx.required(()->jdbc.query("SELECT a.attempt_id,a.run_id,a.instance_id,a.owner_epoch,a.attempt_no,a.state,r.state AS run_state FROM ddip_management.attempts a JOIN ddip_management.runs r USING(run_id) JOIN ddip_management.worker_instances w USING(instance_id) WHERE a.attempt_id=? AND w.installation_id=?",(rs,n)->new Receipt(rs.getObject(1,UUID.class),rs.getObject(2,UUID.class),rs.getObject(3,UUID.class),rs.getLong(4),rs.getInt(5),rs.getString(6),rs.getString(7)),attemptId,installation).stream().findFirst());
    }
    public Session heartbeatIdle(InstallationHostGuard guard,Session session) {
        held(guard,session);
        return ownerTransaction(()->{var c=slot();checkSession(c,session);
            require(!session.instanceId().equals(c.get("owner_instance_id")),WORKER_NOT_READY);
            exactly(jdbc.update("UPDATE ddip_management.worker_instances SET heartbeat_at=clock_timestamp(),row_version=row_version+1 WHERE instance_id=? AND installation_id=? AND row_version=? AND status=?",session.instanceId(),installation,session.workerVersion(),session.status()));
            return with(session,session.status(),session.epoch(),session.coordinatorVersion(),session.workerVersion()+1);
        });
    }
    public Owner heartbeat(InstallationHostGuard guard,Owner owner) {
        held(guard,owner.worker());
        return ownerTransaction(()->{owned(owner);
            Session s=owner.worker();
            exactly(jdbc.update("UPDATE ddip_management.worker_instances SET heartbeat_at=clock_timestamp(),row_version=row_version+1 WHERE instance_id=? AND installation_id=? AND row_version=? AND status='READY'",s.instanceId(),installation,s.workerVersion()));
            exactly(jdbc.update("UPDATE ddip_management.coordinator SET heartbeat_at=clock_timestamp(),row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL AND mode='READY'",installation,owner.attemptId(),s.instanceId(),s.epoch(),s.coordinatorVersion()));
            return new Owner(with(s,"READY",s.epoch(),s.coordinatorVersion()+1,s.workerVersion()+1),owner.runId(),owner.attemptId(),owner.runVersion(),owner.attemptVersion(),owner.attemptState());
        });
    }
    /** Metadata transition only. It does not authorize a child; C2 must commit stage INTENT first. */
    public Owner markRunning(InstallationHostGuard guard,Owner owner) {
        held(guard,owner.worker());require(owner.attemptState().equals("CLAIMED"),OWNERSHIP_LOST);
        return ownerTransaction(()->{owned(owner);controls(owner);
            exactly(jdbc.update("UPDATE ddip_management.attempts SET state='RUNNING',row_version=row_version+1 WHERE attempt_id=? AND run_id=? AND instance_id=? AND owner_epoch=? AND state='CLAIMED' AND row_version=?",owner.attemptId(),owner.runId(),owner.worker().instanceId(),owner.worker().epoch(),owner.attemptVersion()));
            audit(owner.runId(),owner.attemptId(),"ATTEMPT_STARTED","CLAIMED","RUNNING",owner.attemptVersion(),owner.attemptVersion()+1,"STARTED");
            return new Owner(owner.worker(),owner.runId(),owner.attemptId(),owner.runVersion(),owner.attemptVersion()+1,"RUNNING");
        });
    }
    /** C2 seam: recheck fencing/controls in the SAME short transaction that inserts stage INTENT.
     * This callback must contain JDBC metadata only, never I/O, process launch or publication.
     * C1 production has no caller. The adapter must recheck again before launch after intent commits.
     */
    public void stageIntentTransaction(InstallationHostGuard guard,Owner owner,Runnable metadataIntent) {
        held(guard,owner.worker());require(owner.attemptState().equals("RUNNING"),OWNERSHIP_LOST);
        ownerTransaction(()->{owned(owner);controls(owner);metadataIntent.run();return null;});
    }
    /** Fixed CHECK_INPUTS evidence; no result here represents a publication or terminal run. */
    public record CheckInputs(Owner owner,UUID checkpointId,long stageVersion,String namespace,String commandSha256,
                              String selectionSha256,String state,boolean authorized) { }
    public record StageReceipt(String phase,String state,long rowVersion,boolean currentOwner) { }
    public tools.jackson.databind.JsonNode frozenReliability(InstallationHostGuard guard,Owner owner) {
        held(guard,owner.worker());
        return ownerTransaction(()->{
            slot();
            var sources=jdbc.queryForList("SELECT s.source_id,s.enabled,s.profile_family,r.revision,r.captured_gate_version,r.captured_enabled,v.configuration_bytes,v.configuration_sha256,v.endpoint_id,v.endpoint_sha256 FROM ddip_management.run_sources r JOIN ddip_management.sources s ON (s.product_id,s.source_id)=(r.product_id,r.source_id) JOIN ddip_management.source_revisions v ON (v.product_id,v.source_id,v.revision)=(r.product_id,r.source_id,r.revision) WHERE r.run_id=? ORDER BY s.product_id COLLATE \"C\",s.source_id COLLATE \"C\" LIMIT 33 FOR UPDATE OF s",owner.runId());
            owned(owner);controls(owner);
            var run=one(jdbc.queryForList("SELECT run_id,product_id,pipeline,pipeline_version,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id FROM ddip_management.runs WHERE run_id=?",owner.runId()));
            require("RELIABILITY_V2".equals(run.get("pipeline")),RUNTIME_MISMATCH);
            try{FrozenRunSelection.validate(jdbc,installation,run,sources,owner.worker().profileId(),owner.worker().profileDigest(),owner.worker().deploymentDigest());return FrozenRunSelection.canonical((byte[])run.get("captured_selection"),(String)run.get("selection_sha256"),16*1024*1024);}
            catch(org.springframework.dao.DataAccessException ex){throw ex;}catch(RuntimeException ex){throw new WorkerFailure(RUNTIME_MISMATCH);}
        });
    }
    public CheckInputs beginCheckInputs(InstallationHostGuard guard,Owner owner,UUID checkpoint,String commandSha,String selectionSha) {
        held(guard,owner.worker());require(capability.available() && capability.supports("RELIABILITY_V2") && checkpoint!=null && hash(commandSha) && hash(selectionSha),WORKER_NOT_READY);
        String namespace="runs/"+owner.runId()+"/attempts/"+owner.attemptId()+"/check-inputs/"+checkpoint;
        return ownerTransaction(()->{owned(owner);controls(owner);
            require(Boolean.TRUE.equals(jdbc.queryForObject("SELECT pipeline='RELIABILITY_V2' AND selection_sha256=? AND current_stage IS NULL FROM ddip_management.runs WHERE run_id=?",Boolean.class,selectionSha,owner.runId())),RUNTIME_MISMATCH);
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,owner.attemptId())==0,OWNERSHIP_LOST);
            long av=owner.attemptVersion();
            if(owner.attemptState().equals("CLAIMED")) {
                exactly(jdbc.update("UPDATE ddip_management.attempts SET state='RUNNING',row_version=row_version+1 WHERE attempt_id=? AND run_id=? AND instance_id=? AND owner_epoch=? AND state='CLAIMED' AND row_version=?",owner.attemptId(),owner.runId(),owner.worker().instanceId(),owner.worker().epoch(),av));
                audit(owner.runId(),owner.attemptId(),"ATTEMPT_STARTED","CLAIMED","RUNNING",av,av+1,"STARTED");av++;
            } else require(owner.attemptState().equals("RUNNING"),OWNERSHIP_LOST);
            jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,intended_identity,intended_namespace) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?,?,?)",owner.attemptId(),checkpoint,selectionSha,commandSha,namespace);
            exactly(jdbc.update("UPDATE ddip_management.runs SET current_stage='CHECK_INPUTS',row_version=row_version+1 WHERE run_id=? AND current_attempt_id=? AND state='RUNNING' AND row_version=?",owner.runId(),owner.attemptId(),owner.runVersion()));
            audit(owner.runId(),owner.attemptId(),"STAGE_INTENT",null,"INTENT",null,0L,"ACCEPTED");
            return new CheckInputs(new Owner(owner.worker(),owner.runId(),owner.attemptId(),owner.runVersion()+1,av,"RUNNING"),checkpoint,0,namespace,commandSha,selectionSha,"INTENT",false);
        });
    }
    /** One durable authorization: binding command bytes consumes the unbound INTENT. Never retry it. */
    public CheckInputs authorizeCheckInputs(InstallationHostGuard guard,CheckInputs stage) {
        held(guard,stage.owner().worker());require(!stage.authorized() && stage.state().equals("INTENT"),OWNERSHIP_LOST);
        return ownerTransaction(()->{owned(stage.owner());controls(stage.owner());checkStage(stage);
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET command_address=?,command_sha256=?,row_version=row_version+1 WHERE attempt_id=? AND stage='CHECK_INPUTS' AND item_ordinal=0 AND checkpoint_id=? AND state='INTENT' AND command_address IS NULL AND command_sha256 IS NULL AND child_pid IS NULL AND row_version=?",stage.namespace()+"/command.json",stage.commandSha256(),stage.owner().attemptId(),stage.checkpointId(),stage.stageVersion()));
            audit(stage.owner().runId(),stage.owner().attemptId(),"STAGE_LAUNCH_AUTHORIZED","INTENT","INTENT",stage.stageVersion(),stage.stageVersion()+1,"STARTED");
            return nextStage(stage,stage.stageVersion()+1,"INTENT",true);
        });
    }
    public CheckInputs recordVerifierProcess(InstallationHostGuard guard,CheckInputs stage,InstallationHostGuard.ProcessIdentity child) {
        held(guard,stage.owner().worker());require(stage.authorized() && stage.state().equals("INTENT") && child!=null && child.pid()>0 && child.startedAt()!=null && child.instanceId()!=null,OWNERSHIP_LOST);
        return ownerTransaction(()->{owned(stage.owner());controls(stage.owner());checkStage(stage);
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state='RUNNING',child_pid=?,child_started_at=?,child_instance_id=?,started_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND stage='CHECK_INPUTS' AND item_ordinal=0 AND checkpoint_id=? AND state='INTENT' AND row_version=? AND child_pid IS NULL AND command_sha256=?",child.pid(),java.sql.Timestamp.from(child.startedAt()),child.instanceId(),stage.owner().attemptId(),stage.checkpointId(),stage.stageVersion(),stage.commandSha256()));
            audit(stage.owner().runId(),stage.owner().attemptId(),"STAGE_STARTED","INTENT","RUNNING",stage.stageVersion(),stage.stageVersion()+1,"STARTED");return nextStage(stage,stage.stageVersion()+1,"RUNNING",true);
        });
    }
    public CheckInputs heartbeatCheckInputs(InstallationHostGuard guard,CheckInputs stage) {
        Owner next=heartbeat(guard,stage.owner());
        return new CheckInputs(next,stage.checkpointId(),stage.stageVersion(),stage.namespace(),stage.commandSha256(),stage.selectionSha256(),stage.state(),stage.authorized());
    }
    public void checkVerifierOwner(InstallationHostGuard guard,CheckInputs stage) {
        held(guard,stage.owner().worker());ownerTransaction(()->{owned(stage.owner());controls(stage.owner());checkStage(stage);return null;});
    }
    /** Parent-validated private descriptor anchors result + native evidence digests. Never completes a run. */
    public CheckInputs finishCheckInputs(InstallationHostGuard guard,CheckInputs stage,boolean verified,String code,String resultSha,byte[] counts) {
        held(guard,stage.owner().worker());
        require(hash(resultSha) && counts!=null && counts.length<=65536 && (verified?"VERIFIED".equals(code):Set.of("INPUT_UNAVAILABLE","INPUT_INVALID","INPUT_CHANGED","RUNTIME_MISMATCH","RESOURCE_LIMIT","PROCESS_START_FAILED","STAGE_TIMEOUT","STAGE_OUTPUT_LIMIT").contains(code)),WORKER_NOT_READY);
        require(verified?stage.state().equals("RUNNING"):Set.of("INTENT","RUNNING").contains(stage.state()),OWNERSHIP_LOST);
        return ownerTransaction(()->{owned(stage.owner());checkStage(stage);
            String state=verified?"VERIFIED":"FAILED";
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state=?,result_address=?,result_sha256=?,counts_bytes=?,outcome_code=?,finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND stage='CHECK_INPUTS' AND item_ordinal=0 AND checkpoint_id=? AND state=? AND row_version=?",state,stage.namespace()+"/verified.json",resultSha,counts,code,stage.owner().attemptId(),stage.checkpointId(),stage.state(),stage.stageVersion()));
            audit(stage.owner().runId(),stage.owner().attemptId(),verified?"STAGE_VERIFIED":"STAGE_FAILED",stage.state(),state,stage.stageVersion(),stage.stageVersion()+1,code);return nextStage(stage,stage.stageVersion()+1,state,stage.authorized());
        });
    }
    /** Only the read-only supervisor may attest a definitive CHECK_INPUTS failure. Atomic closure. */
    public Session failCheckInputs(InstallationHostGuard guard,Owner owner,CheckInputs stage,String code,String resultSha) {
        held(guard,owner.worker());require(Set.of("INPUT_UNAVAILABLE","INPUT_INVALID","INPUT_CHANGED","RUNTIME_MISMATCH","RESOURCE_LIMIT","PROCESS_START_FAILED","STAGE_TIMEOUT","STAGE_OUTPUT_LIMIT").contains(code),WORKER_NOT_READY);
        return ownerTransaction(()->{owned(owner);
            if(stage==null)require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,owner.attemptId())==0,OWNERSHIP_LOST);
            else {
                require(owner.equals(stage.owner()) && hash(resultSha) && Set.of("INTENT","RUNNING").contains(stage.state()),OWNERSHIP_LOST);checkStage(stage);
                exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state='FAILED',result_address=?,result_sha256=?,counts_bytes=?,outcome_code=?,finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND stage='CHECK_INPUTS' AND item_ordinal=0 AND checkpoint_id=? AND state=? AND row_version=?",stage.namespace()+"/verified.json",resultSha,SourceJson.canonical(SourceJson.object()),code,owner.attemptId(),stage.checkpointId(),stage.state(),stage.stageVersion()));
                audit(owner.runId(),owner.attemptId(),"STAGE_FAILED",stage.state(),"FAILED",stage.stageVersion(),stage.stageVersion()+1,code);
            }
            return failOwned(owner,code);
        });
    }
    /** Unknown child/result: mark unknown and interrupt atomically, retaining the occupied pointer. */
    public void blockCheckInputs(InstallationHostGuard guard,CheckInputs stage) {
        held(guard,stage.owner().worker());ownerTransaction(()->{owned(stage.owner());checkStage(stage);
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state='UNKNOWN',outcome_code='RECOVERY_REQUIRED',row_version=row_version+1 WHERE attempt_id=? AND stage='CHECK_INPUTS' AND item_ordinal=0 AND checkpoint_id=? AND state=? AND row_version=?",stage.owner().attemptId(),stage.checkpointId(),stage.state(),stage.stageVersion()));
            audit(stage.owner().runId(),stage.owner().attemptId(),"STAGE_UNKNOWN",stage.state(),"UNKNOWN",stage.stageVersion(),stage.stageVersion()+1,"RECOVERY_REQUIRED");
            // Same transaction/lock and expected versions as C1 interruption, without adopting a token.
            Owner owner=stage.owner();finishAttempt(owner,"INTERRUPTED","RECOVERY_REQUIRED");
            exactly(jdbc.update("UPDATE ddip_management.runs SET state='RECOVERY_REQUIRED',outcome_code='RECOVERY_REQUIRED',retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND current_attempt_id=? AND state='RUNNING' AND row_version=?",owner.runId(),owner.attemptId(),owner.runVersion()));
            Session s=owner.worker();
            exactly(jdbc.update("UPDATE ddip_management.coordinator SET mode='RECOVERY_ONLY',row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL AND mode='READY'",installation,owner.attemptId(),s.instanceId(),s.epoch(),s.coordinatorVersion()));
            exactly(jdbc.update("UPDATE ddip_management.worker_instances SET status='RECOVERY_ONLY',row_version=row_version+1 WHERE instance_id=? AND installation_id=? AND status='READY' AND row_version=?",s.instanceId(),installation,s.workerVersion()));
            audit(owner.runId(),owner.attemptId(),"RUN_RECOVERY_REQUIRED","RUNNING","RECOVERY_REQUIRED",owner.runVersion(),owner.runVersion()+1,"RECOVERY_REQUIRED");return null;
        });
    }
    /** Locked historical classification only. No returned value renews launch authority. */
    public StageReceipt inspectCheckInputs(Owner expected,UUID checkpoint) {
        return tx.required(()->{
            var c=slot();boolean current=expected.attemptId().equals(c.get("active_attempt_id")) && expected.worker().instanceId().equals(c.get("owner_instance_id")) && expected.worker().epoch()==number(c,"epoch");
            var rows=jdbc.queryForList("SELECT checkpoint_id,state,row_version,command_address,child_pid FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='CHECK_INPUTS' AND item_ordinal=0",expected.attemptId());
            if(rows.isEmpty()) {
                boolean same=current && number(c,"row_version")==expected.worker().coordinatorVersion() && Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.runs r JOIN ddip_management.attempts a ON r.current_attempt_id=a.attempt_id WHERE r.run_id=? AND r.current_attempt_id=? AND r.state='RUNNING' AND r.row_version=? AND a.row_version=? AND a.state=?)",Boolean.class,expected.runId(),expected.attemptId(),expected.runVersion(),expected.attemptVersion(),expected.attemptState()));
                return new StageReceipt(same?"CONFIRMED_ROLLBACK":"UNRESOLVED",null,0,current);
            }
            var row=rows.getFirst();if(!checkpoint.equals(row.get("checkpoint_id")))return new StageReceipt("UNRESOLVED",null,0,false);
            String state=(String)row.get("state");String phase=state.equals("VERIFIED")?"HISTORICAL_VERIFIED":row.get("child_pid")!=null?"CHILD_RECORDED":row.get("command_address")!=null?"LAUNCH_UNCERTAIN":"INTENT_COMMITTED";
            return new StageReceipt(phase,state,number(row,"row_version"),current);
        });
    }
    private void checkStage(CheckInputs stage) {
        var row=one(jdbc.queryForList("SELECT checkpoint_id,state,row_version,input_evidence_sha256,intended_identity,intended_namespace,command_address,command_sha256 FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='CHECK_INPUTS' AND item_ordinal=0 FOR UPDATE",stage.owner().attemptId()));
        require(stage.checkpointId().equals(row.get("checkpoint_id")) && stage.stageVersion()==number(row,"row_version") && stage.state().equals(row.get("state")) && stage.selectionSha256().equals(row.get("input_evidence_sha256")) && stage.commandSha256().equals(row.get("intended_identity")) && stage.namespace().equals(row.get("intended_namespace")),OWNERSHIP_LOST);
        require(stage.authorized()?stage.commandSha256().equals(row.get("command_sha256")) && (stage.namespace()+"/command.json").equals(row.get("command_address")):row.get("command_address")==null && row.get("command_sha256")==null,OWNERSHIP_LOST);
    }
    private static CheckInputs nextStage(CheckInputs s,long version,String state,boolean authorized){return new CheckInputs(s.owner(),s.checkpointId(),version,s.namespace(),s.commandSha256(),s.selectionSha256(),state,authorized);}
    /** Fixed Reliability stages only. This is not a configurable workflow engine or dispatcher. */
    public record ReliabilityStage(Owner owner,UUID checkpoint,String stage,String state,long version,String namespace,
                                   String commandSha,String inputSha,String fingerprint,boolean authorized) { }
    public record StageEvidence(UUID checkpoint,String commandAddress,String commandSha,String resultAddress,String resultSha) { }
    public StageEvidence verifiedCheckInputs(InstallationHostGuard guard,Owner owner,UUID checkpoint) {
        held(guard,owner.worker());return ownerTransaction(()->{owned(owner);
            var s=one(jdbc.queryForList("SELECT checkpoint_id,command_address,command_sha256,result_address,result_sha256 FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='CHECK_INPUTS' AND item_ordinal=0 AND state='VERIFIED'",owner.attemptId()));
            require(checkpoint.equals(s.get("checkpoint_id")),OWNERSHIP_LOST);
            return new StageEvidence(checkpoint,(String)s.get("command_address"),(String)s.get("command_sha256"),(String)s.get("result_address"),(String)s.get("result_sha256"));
        });
    }
    public ReliabilityStage beginReliabilityStage(InstallationHostGuard guard,Owner owner,String stage,UUID checkpoint,String commandSha,String inputSha,String fingerprint) {
        held(guard,owner.worker());require(capability.available() && capability.supports("RELIABILITY_V2") && Set.of("PUBLISH_RELIABILITY","VERIFY_RESULT").contains(stage) && checkpoint!=null && hash(commandSha) && hash(inputSha) && hash(fingerprint),WORKER_NOT_READY);
        String previous=stage.equals("PUBLISH_RELIABILITY")?"CHECK_INPUTS":"PUBLISH_RELIABILITY";
        String namespace="runs/"+owner.runId()+"/attempts/"+owner.attemptId()+"/"+stage.toLowerCase(Locale.ROOT).replace('_','-')+"/"+checkpoint;
        return ownerTransaction(()->{owned(owner);controls(owner);attemptDeadline(owner);
            require(Boolean.TRUE.equals(jdbc.queryForObject("SELECT pipeline='RELIABILITY_V2' AND current_stage=? FROM ddip_management.runs WHERE run_id=?",Boolean.class,previous,owner.runId())),OWNERSHIP_LOST);
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage=? AND item_ordinal=0 AND state='VERIFIED'",Long.class,owner.attemptId(),previous)==1,OWNERSHIP_LOST);
            jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,intended_identity,intended_namespace) VALUES (?,?,0,?,'INTENT',?,?,?)",owner.attemptId(),stage,checkpoint,commandSha,fingerprint,"v2/processor_revision=2/publication="+fingerprint);
            exactly(jdbc.update("UPDATE ddip_management.runs SET current_stage=?,row_version=row_version+1 WHERE run_id=? AND row_version=? AND state='RUNNING' AND current_attempt_id=?",stage,owner.runId(),owner.runVersion(),owner.attemptId()));
            audit(owner.runId(),owner.attemptId(),"STAGE_INTENT",null,"INTENT",null,0L,"ACCEPTED");
            return new ReliabilityStage(new Owner(owner.worker(),owner.runId(),owner.attemptId(),owner.runVersion()+1,owner.attemptVersion(),owner.attemptState()),checkpoint,stage,"INTENT",0,namespace,commandSha,commandSha,fingerprint,false);
        });
    }
    public ReliabilityStage authorizeReliability(InstallationHostGuard guard,ReliabilityStage s) {
        held(guard,s.owner().worker());return ownerTransaction(()->{owned(s.owner());controls(s.owner());attemptDeadline(s.owner());nativeStage(s);require(!s.authorized() && s.state().equals("INTENT"),OWNERSHIP_LOST);
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET command_address=?,command_sha256=?,row_version=row_version+1 WHERE attempt_id=? AND stage=? AND item_ordinal=0 AND checkpoint_id=? AND state='INTENT' AND row_version=? AND command_address IS NULL AND child_pid IS NULL",s.namespace()+"/command.json",s.commandSha(),s.owner().attemptId(),s.stage(),s.checkpoint(),s.version()));
            audit(s.owner().runId(),s.owner().attemptId(),"STAGE_LAUNCH_AUTHORIZED","INTENT","INTENT",s.version(),s.version()+1,"STARTED");return nextNative(s,s.owner(),"INTENT",true);
        });
    }
    public ReliabilityStage recordReliabilityChild(InstallationHostGuard guard,ReliabilityStage s,InstallationHostGuard.ProcessIdentity child) {
        held(guard,s.owner().worker());return ownerTransaction(()->{owned(s.owner());controls(s.owner());nativeStage(s);require(s.authorized() && s.state().equals("INTENT"),OWNERSHIP_LOST);
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state='RUNNING',child_pid=?,child_started_at=?,child_instance_id=?,started_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND stage=? AND item_ordinal=0 AND checkpoint_id=? AND state='INTENT' AND row_version=?",child.pid(),java.sql.Timestamp.from(child.startedAt()),child.instanceId(),s.owner().attemptId(),s.stage(),s.checkpoint(),s.version()));
            audit(s.owner().runId(),s.owner().attemptId(),"STAGE_STARTED","INTENT","RUNNING",s.version(),s.version()+1,"STARTED");return nextNative(s,s.owner(),"RUNNING",true);
        });
    }
    public ReliabilityStage heartbeatReliability(InstallationHostGuard guard,ReliabilityStage s) {
        Owner next=heartbeat(guard,s.owner());
        var updated=new ReliabilityStage(next,s.checkpoint(),s.stage(),s.state(),s.version(),s.namespace(),s.commandSha(),s.inputSha(),s.fingerprint(),s.authorized());
        held(guard,next.worker());ownerTransaction(()->{owned(next);controls(next);attemptDeadline(next);nativeStage(updated);return null;});return updated;
    }
    public java.time.Duration remainingReliability(InstallationHostGuard guard,Owner owner) {
        held(guard,owner.worker());return ownerTransaction(()->{owned(owner);attemptDeadline(owner);
            var seconds=jdbc.queryForObject("SELECT extract(epoch FROM started_at+interval '2 hours'-clock_timestamp()) FROM ddip_management.attempts WHERE attempt_id=?",java.math.BigDecimal.class,owner.attemptId());
            return java.time.Duration.ofMillis(Math.max(1,seconds.multiply(java.math.BigDecimal.valueOf(1000)).longValue()));
        });
    }
    private void attemptDeadline(Owner owner) {require(Boolean.TRUE.equals(jdbc.queryForObject("SELECT clock_timestamp() < started_at+interval '2 hours' FROM ddip_management.attempts WHERE attempt_id=?",Boolean.class,owner.attemptId())),WORKER_NOT_READY);}
    private void nativeStage(ReliabilityStage s) {
        var row=one(jdbc.queryForList("SELECT checkpoint_id,state,row_version,input_evidence_sha256,intended_identity,command_address,command_sha256 FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage=? AND item_ordinal=0 FOR UPDATE",s.owner().attemptId(),s.stage()));
        require(s.checkpoint().equals(row.get("checkpoint_id")) && s.version()==number(row,"row_version") && s.state().equals(row.get("state")) && s.inputSha().equals(row.get("input_evidence_sha256")) && s.fingerprint().equals(row.get("intended_identity")),OWNERSHIP_LOST);
        require(s.authorized()?s.commandSha().equals(row.get("command_sha256")) && (s.namespace()+"/command.json").equals(row.get("command_address")):row.get("command_address")==null && row.get("command_sha256")==null,OWNERSHIP_LOST);
    }
    private static ReliabilityStage nextNative(ReliabilityStage s,Owner o,String state,boolean authorized){return new ReliabilityStage(o,s.checkpoint(),s.stage(),state,s.version()+1,s.namespace(),s.commandSha(),s.inputSha(),s.fingerprint(),authorized);}
    public ReliabilityStage publishedReliability(InstallationHostGuard guard,ReliabilityStage s,String resultSha) {
        held(guard,s.owner().worker());return ownerTransaction(()->{owned(s.owner());nativeStage(s);require(s.stage().equals("PUBLISH_RELIABILITY") && s.state().equals("RUNNING") && hash(resultSha),OWNERSHIP_LOST);
            finishNative(s,"VERIFIED","VERIFIED",resultSha,null);return nextNative(s,s.owner(),"VERIFIED",true);
        });
    }
    private void finishNative(ReliabilityStage s,String state,String code,String hash,byte[] counts) {
        exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state=?,outcome_code=?,result_address=?,result_sha256=?,counts_bytes=?,finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND stage=? AND item_ordinal=0 AND checkpoint_id=? AND state=? AND row_version=?",state,code,s.namespace()+"/verified.json",hash,counts,s.owner().attemptId(),s.stage(),s.checkpoint(),s.state(),s.version()));
        audit(s.owner().runId(),s.owner().attemptId(),state.equals("VERIFIED")?"STAGE_VERIFIED":"STAGE_FAILED",s.state(),state,s.version(),s.version()+1,code);
    }
    public void blockReliability(InstallationHostGuard guard,ReliabilityStage s) {
        held(guard,s.owner().worker());ownerTransaction(()->{owned(s.owner());nativeStage(s);
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state='UNKNOWN',outcome_code='RECOVERY_REQUIRED',row_version=row_version+1 WHERE attempt_id=? AND stage=? AND item_ordinal=0 AND checkpoint_id=? AND row_version=?",s.owner().attemptId(),s.stage(),s.checkpoint(),s.version()));
            audit(s.owner().runId(),s.owner().attemptId(),"STAGE_UNKNOWN",s.state(),"UNKNOWN",s.version(),s.version()+1,"RECOVERY_REQUIRED");interruptOwned(s.owner());return null;
        });
    }
    /** Definitive, quiescent failure. No caller may use this on uncertain child/DB outcomes. */
    public Session failReliability(InstallationHostGuard guard,Owner owner,ReliabilityStage stage,String code,String resultSha) {
        held(guard,owner.worker());require(Set.of("INPUT_INVALID","INPUT_CHANGED","INPUT_UNAVAILABLE","RUNTIME_MISMATCH","RESOURCE_LIMIT","PROCESS_START_FAILED","INCOMPLETE_OUTPUT").contains(code),WORKER_NOT_READY);
        return ownerTransaction(()->{owned(owner);
            if(stage==null)require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage IN ('PUBLISH_RELIABILITY','VERIFY_RESULT')",Long.class,owner.attemptId())==0,OWNERSHIP_LOST);
            else {nativeStage(stage);finishNative(stage,"FAILED",code,resultSha,null);}
            return failOwned(owner,code);
        });
    }
    private Session failOwned(Owner owner,String code) {
        finishAttempt(owner,"FAILED",code);
        exactly(jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=clock_timestamp(),outcome_code=?,retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND current_attempt_id=? AND state='RUNNING' AND row_version=?",code,owner.runId(),owner.attemptId(),owner.runVersion()));
        releaseOwned(owner);audit(owner.runId(),owner.attemptId(),"RUN_FAILED","RUNNING","FAILED",owner.runVersion(),owner.runVersion()+1,code);
        Session w=owner.worker();return with(w,"READY",w.epoch(),w.coordinatorVersion()+1,w.workerVersion());
    }

    public UUID succeedReliability(InstallationHostGuard guard,ReliabilityStage s,byte[] evidence,String resultSha) {
        held(guard,s.owner().worker());return ownerTransaction(()->{owned(s.owner());nativeStage(s);attemptDeadline(s.owner());require(s.stage().equals("VERIFY_RESULT") && s.state().equals("RUNNING") && hash(resultSha),OWNERSHIP_LOST);
            var e=platform.contentapi.management.FrozenRunSelection.canonical(evidence,platform.contentapi.management.SourceJson.sha(evidence),16*1024*1024);
            require(e.get("kind").stringValue().equals("RELIABILITY_V2") && e.get("productId").stringValue().equals("application-reliability") && e.get("contractVersion").intValue()==2 && e.get("processorRevision").intValue()==2 && e.get("nativeFingerprint").stringValue().equals(s.fingerprint()),RUNTIME_MISMATCH);
            String binding=e.get("storageBindingId").stringValue(),address=e.get("relativeAddress").stringValue(),format=e.get("inputFormat").stringValue();String digest=platform.contentapi.management.SourceJson.sha(evidence);
            require(address.equals("v2/processor_revision=2/publication="+s.fingerprint()) && Set.of("JSONL","PARQUET").contains(format),RUNTIME_MISMATCH);
            var registered=jdbc.queryForList("SELECT artifact_id,storage_binding_id,relative_address,evidence_sha256,evidence_bytes,native_identity,native_format FROM ddip_management.artifacts WHERE (product_id='application-reliability' AND kind='RELIABILITY_V2' AND contract_version=2 AND processor_revision=2 AND native_format=? AND native_identity=?) OR (storage_binding_id=? AND relative_address=?) FOR UPDATE",format,s.fingerprint(),binding,address);
            UUID artifact;
            if(registered.isEmpty()) {artifact=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at,native_format) VALUES (?,'RELIABILITY_V2','application-reliability',2,2,?,?,?,?,?,clock_timestamp(),?)",artifact,s.fingerprint(),binding,address,evidence,digest,format);
                for(var input:jdbc.queryForList("SELECT role,ordinal,artifact_id FROM ddip_management.run_inputs WHERE run_id=? ORDER BY role,ordinal",s.owner().runId()))jdbc.update("INSERT INTO ddip_management.artifact_dependencies VALUES (?,?,?,?)",artifact,input.get("role"),input.get("ordinal"),input.get("artifact_id"));
            } else {require(registered.size()==1,RUNTIME_MISMATCH);var r=registered.getFirst();require(binding.equals(r.get("storage_binding_id")) && address.equals(r.get("relative_address")) && s.fingerprint().equals(r.get("native_identity")) && format.equals(r.get("native_format")) && digest.equals(r.get("evidence_sha256")) && Arrays.equals(evidence,(byte[])r.get("evidence_bytes")),RUNTIME_MISMATCH);artifact=(UUID)r.get("artifact_id");
                var links=jdbc.queryForList("SELECT role,ordinal,dependency_artifact_id AS artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? ORDER BY role,ordinal",artifact);require(links.equals(jdbc.queryForList("SELECT role,ordinal,artifact_id FROM ddip_management.run_inputs WHERE run_id=? ORDER BY role,ordinal",s.owner().runId())),RUNTIME_MISMATCH);
            }
            byte[] counts=platform.contentapi.management.SourceJson.canonical(e.get("counts"));finishNative(s,"VERIFIED","VERIFIED",resultSha,counts);
            jdbc.update("INSERT INTO ddip_management.stage_artifacts VALUES (?,'VERIFY_RESULT',0,'RESULT',0,?)",s.owner().attemptId(),artifact);
            finishAttempt(s.owner(),"SUCCEEDED","SUCCEEDED");
            exactly(jdbc.update("UPDATE ddip_management.runs SET state='SUCCEEDED',finished_at=clock_timestamp(),result_artifact_id=?,counts_bytes=?,outcome_code='SUCCEEDED',retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND current_attempt_id=? AND state='RUNNING' AND row_version=?",artifact,counts,s.owner().runId(),s.owner().attemptId(),s.owner().runVersion()));
            releaseOwned(s.owner());audit(s.owner().runId(),s.owner().attemptId(),"RUN_SUCCEEDED","RUNNING","SUCCEEDED",s.owner().runVersion(),s.owner().runVersion()+1,"SUCCEEDED");return artifact;
        });
    }
    private void interruptOwned(Owner owner) {
            finishAttempt(owner,"INTERRUPTED","RECOVERY_REQUIRED");
            exactly(jdbc.update("UPDATE ddip_management.runs SET state='RECOVERY_REQUIRED',outcome_code='RECOVERY_REQUIRED',retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND current_attempt_id=? AND state='RUNNING' AND row_version=?",owner.runId(),owner.attemptId(),owner.runVersion()));
            Session s=owner.worker();
            exactly(jdbc.update("UPDATE ddip_management.coordinator SET mode='RECOVERY_ONLY',row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL AND mode='READY'",installation,owner.attemptId(),s.instanceId(),s.epoch(),s.coordinatorVersion()));
            exactly(jdbc.update("UPDATE ddip_management.worker_instances SET status='RECOVERY_ONLY',row_version=row_version+1 WHERE instance_id=? AND installation_id=? AND status='READY' AND row_version=?",s.instanceId(),installation,s.workerVersion()));
            audit(owner.runId(),owner.attemptId(),"RUN_RECOVERY_REQUIRED","RUNNING","RECOVERY_REQUIRED",owner.runVersion(),owner.runVersion()+1,"RECOVERY_REQUIRED");
    }
    private void releaseOwned(Owner o) {Session s=o.worker();exactly(jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL AND mode='READY'",installation,o.attemptId(),s.instanceId(),s.epoch(),s.coordinatorVersion()));}
    private static boolean hash(String s){return s!=null && s.matches("[0-9a-f]{64}");}
    /** Allowed only before RUNNING or ANY stage intent; the accepted protocol forbids side effects here. */
    public Session failBeforeLaunch(InstallationHostGuard guard,Owner owner,PrelaunchFailure failure) {
        held(guard,owner.worker());require(owner.attemptState().equals("CLAIMED") && failure!=null,OWNERSHIP_LOST);
        return ownerTransaction(()->{owned(owner);
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,owner.attemptId())==0,WORKER_NOT_READY);
            finishAttempt(owner,"FAILED",failure.name());
            exactly(jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=clock_timestamp(),outcome_code=?,retry_eligible=false,row_version=row_version+1 WHERE run_id=? AND current_attempt_id=? AND state='RUNNING' AND row_version=?",failure.name(),owner.runId(),owner.attemptId(),owner.runVersion()));
            Session s=owner.worker();
            exactly(jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,row_version=row_version+1 WHERE singleton_id=1 AND installation_id=? AND active_attempt_id=? AND owner_instance_id=? AND epoch=? AND row_version=? AND active_import_id IS NULL AND mode='READY'",installation,owner.attemptId(),s.instanceId(),s.epoch(),s.coordinatorVersion()));
            audit(owner.runId(),owner.attemptId(),"RUN_FAILED","RUNNING","FAILED",owner.runVersion(),owner.runVersion()+1,failure.name());
            return with(s,"READY",s.epoch(),s.coordinatorVersion()+1,s.workerVersion());
        });
    }
    /** Uncertain execution stays occupied. This does NOT reconcile, prove quiescence or release anything. */
    public Session interrupt(InstallationHostGuard guard,Owner owner) {
        held(guard,owner.worker());return ownerTransaction(()->{owned(owner);
            interruptOwned(owner);Session s=owner.worker();
            return with(s,"RECOVERY_ONLY",s.epoch(),s.coordinatorVersion()+1,s.workerVersion()+1);
        });
    }
    public void stopIdle(InstallationHostGuard guard,Session s) {
        held(guard,s);ownerTransaction(()->{var c=slot();checkSession(c,s);require(!s.instanceId().equals(c.get("owner_instance_id")),WORKER_NOT_READY);
            exactly(jdbc.update("UPDATE ddip_management.worker_instances SET status='STOPPED',row_version=row_version+1 WHERE instance_id=? AND installation_id=? AND status=? AND row_version=?",s.instanceId(),installation,s.status(),s.workerVersion()));return null;});
    }
    private <T> T ownerTransaction(java.util.function.Supplier<T> operation) {
        require(!authorizationStopped,OWNERSHIP_LOST);
        try{return tx.required(operation);}
        catch(ManagementFailure ex){authorizationStopped=true;throw ex;}
        catch(WorkerFailure ex){if(ex.code()==OWNERSHIP_LOST || ex.code()==CLAIM_UNCERTAIN)authorizationStopped=true;throw ex;}
    }
    private void controls(Owner owner) {require(!jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND type='CANCEL' AND state IN ('REQUESTED','APPLIED'))",Boolean.class,owner.runId()),WORKER_NOT_READY);}
    private void finishAttempt(Owner o,String state,String code) {
        exactly(jdbc.update("UPDATE ddip_management.attempts SET state=?,finished_at=clock_timestamp(),outcome_code=?,row_version=row_version+1 WHERE attempt_id=? AND run_id=? AND instance_id=? AND owner_epoch=? AND state=? AND row_version=?",state,code,o.attemptId(),o.runId(),o.worker().instanceId(),o.worker().epoch(),o.attemptState(),o.attemptVersion()));
        audit(o.runId(),o.attemptId(),state.equals("INTERRUPTED")?"ATTEMPT_INTERRUPTED":"ATTEMPT_FINISHED",o.attemptState(),state,o.attemptVersion(),o.attemptVersion()+1,code);
    }
    private void owned(Owner o) {
        var c=slot();checkSession(c,o.worker());
        require("READY".equals(c.get("mode")) && o.attemptId().equals(c.get("active_attempt_id")) && o.worker().instanceId().equals(c.get("owner_instance_id")) && c.get("active_import_id")==null && "READY".equals(o.worker().status()),OWNERSHIP_LOST);
        var r=one(jdbc.queryForList("SELECT state,row_version,current_attempt_id FROM ddip_management.runs WHERE run_id=? FOR UPDATE",o.runId()));
        require("RUNNING".equals(r.get("state")) && o.attemptId().equals(r.get("current_attempt_id")) && number(r,"row_version")==o.runVersion(),OWNERSHIP_LOST);
        var a=one(jdbc.queryForList("SELECT run_id,instance_id,owner_epoch,state,row_version FROM ddip_management.attempts WHERE attempt_id=? FOR UPDATE",o.attemptId()));
        require(o.runId().equals(a.get("run_id")) && o.worker().instanceId().equals(a.get("instance_id")) && o.worker().epoch()==number(a,"owner_epoch") && o.attemptState().equals(a.get("state")) && number(a,"row_version")==o.attemptVersion(),OWNERSHIP_LOST);
    }
    private void held(InstallationHostGuard g,Session s) {
        outsideTransaction();
        require(!authorizationStopped,OWNERSHIP_LOST);g.checkHeld(installation);require(installation.equals(s.installationId()) && g.process().instanceId().equals(s.instanceId()) && g.binding().digest().equals(s.hostDigest()),OWNERSHIP_LOST);
    }
    private void checkSession(Map<String,Object> c,Session s) {
        require(Set.of("READY","STOPPING","RECOVERY_ONLY").contains(s.status()) && installation.equals(s.installationId()) && number(c,"epoch")==s.epoch() && number(c,"row_version")==s.coordinatorVersion(),OWNERSHIP_LOST);
        var w=one(jdbc.queryForList("SELECT installation_id,execution_profile_id,profile_sha256,host_binding_sha256,status,row_version FROM ddip_management.worker_instances WHERE instance_id=?",s.instanceId()));
        require(installation.equals(w.get("installation_id")) && s.profileId().equals(w.get("execution_profile_id")) && s.profileDigest().equals(w.get("profile_sha256")) && s.hostDigest().equals(w.get("host_binding_sha256")) && s.status().equals(w.get("status")) && s.workerVersion()==number(w,"row_version"),OWNERSHIP_LOST);
    }
    private Map<String,Object> slot(){var c=one(jdbc.queryForList("SELECT installation_id,mode,epoch,active_attempt_id,owner_instance_id,active_import_id,row_version FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE"));require(installation.equals(c.get("installation_id")),WORKER_CONFIGURATION_INVALID);return c;}
    private boolean idle(Map<String,Object> c) {
        return "READY".equals(c.get("mode")) && c.get("active_attempt_id")==null && c.get("owner_instance_id")==null && c.get("active_import_id")==null && !jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.attempts WHERE state IN ('CLAIMED','RUNNING')) OR EXISTS(SELECT 1 FROM ddip_management.runs WHERE state IN ('RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')) OR EXISTS(SELECT 1 FROM ddip_management.artifact_import_operations WHERE state='ACTIVE')",Boolean.class);
    }
    private void audit(UUID run,UUID attempt,String action,String before,String after,Long bv,Long av,String code){
        require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class)<1000000,WORKER_NOT_READY);
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,?)",run,attempt,action,before,after,bv,av,code,SourceJson.canonical(SourceJson.object().put("protocolVersion",1)));
    }
    private static Session with(Session s,String status,long epoch,long cv,long wv){return new Session(s.installationId(),s.instanceId(),s.profileId(),s.profileDigest(),s.deploymentDigest(),s.hostDigest(),status,epoch,cv,wv);}
    private static long number(Map<String,Object> row,String key){return ((Number)row.get(key)).longValue();}
    private static Map<String,Object> one(List<Map<String,Object>> rows){require(rows.size()==1,OWNERSHIP_LOST);return rows.getFirst();}
    private static void exactly(int affected){require(affected==1,OWNERSHIP_LOST);}
    private static void outsideTransaction(){require(!org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);}
}
