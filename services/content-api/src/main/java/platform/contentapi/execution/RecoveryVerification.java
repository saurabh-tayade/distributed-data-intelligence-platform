package platform.contentapi.execution;

import java.io.IOException;
import java.nio.file.*;
import java.sql.Timestamp;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.SourceJson;
import platform.contentapi.management.AuditCapacity;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.RecoveryFailure.Code.*;
import static platform.contentapi.execution.RecoveryFailure.need;

/** Internal C3c.2 only. One read-only child; no publication, run/control disposition or slot release. */
final class RecoveryVerification {
    static final String POLICY="E5_RECOVERY_VERIFICATION_V1";
    static final Set<String> OBSERVATIONS=Set.of("VERIFIED_PUBLICATION","OUTPUT_ABSENT","OUTPUT_INCOMPLETE","OUTPUT_CHANGED","OUTPUT_INVALID","OUTPUT_UNAVAILABLE","DEPENDENCY_CHANGED","DEPENDENCY_INVALID","DEPENDENCY_UNAVAILABLE","RUNTIME_INCOMPATIBLE","RESOURCE_LIMIT");
    private static final Set<String> SUPERVISION=Set.of("PROCESS_START_FAILED","STAGE_TIMEOUT","STAGE_OUTPUT_LIMIT","EVIDENCE_INVALID","EVIDENCE_UNAVAILABLE","INTERRUPTED","TERMINATION_UNCONFIRMED");
    enum Step { INTENT, AUTHORIZED, CHILD, RESULT }
    record Stage(RecoveryHandoff.Owner owner,UUID checkpoint,String state,long version,String address,String commandSha,String fingerprint,InstallationHostGuard.ProcessIdentity child) {
        @Override public String toString(){return "RecoveryVerificationStage[redacted]";}
    }
    record Observation(Stage stage,String classification) { }
    /** Historical evidence only. Deliberately no executable token or launch operation. */
    record Inspection(String state,String classification,JsonNode evidence) {
        @Override public String toString(){return "RecoveryVerificationInspection[redacted]";}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final RecoveryHandoff handoffs;
    private final ReadOnlyVerifierProcess process;private final Consumer<Step> fault;
    RecoveryVerification(JdbcTemplate jdbc,ManagementTransactions tx,RecoveryHandoff handoffs){this(jdbc,tx,handoffs,new ReadOnlyVerifierProcess(),step->{});}
    RecoveryVerification(JdbcTemplate jdbc,ManagementTransactions tx,RecoveryHandoff handoffs,ReadOnlyVerifierProcess process,Consumer<Step> fault){this.jdbc=jdbc;this.tx=tx;this.handoffs=handoffs;this.process=process;this.fault=fault;}
    private static final byte[] BRIDGE=loadBridge();
    private static final String BRIDGE_SHA=SourceJson.sha(BRIDGE);
    private static byte[] loadBridge(){try(var in=RecoveryVerification.class.getResourceAsStream("/recovery/reliability_verification.py")){need(in!=null,RUNTIME_MISMATCH);return in.readAllBytes();}catch(IOException ex){throw new RecoveryFailure(RUNTIME_MISMATCH);}}
    private static byte[] bridge(){return BRIDGE;}
    static String bridgeSha(){return BRIDGE_SHA;}
    private static final class Current {Stage stage;}
    Observation verify(InstallationHostGuard guard,RecoveryHandoff.Bootstrap bootstrap,RecoveryQuiescence.Review review) {
        try{return verifyOnce(guard,bootstrap,review);}catch(RecoveryFailure | ManagementFailure | ImportOwnershipFailure | PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new RecoveryFailure(EVIDENCE_CHANGED);}
    }
    private Observation verifyOnce(InstallationHostGuard guard,RecoveryHandoff.Bootstrap bootstrap,RecoveryQuiescence.Review review) {
        var owner=handoffs.authorize(guard,bootstrap,review);var runtime=handoffs.verificationRuntime(bootstrap);
        var pack=handoffs.verificationPack(guard,bootstrap,owner);UUID checkpoint=UUID.randomUUID();
        var files=new PreflightFiles(guard,owner.runId(),owner.controlId(),checkpoint,"VERIFY_RESULT");
        byte[] retained=SourceJson.canonical(pack);String fingerprint=pack.get("evidence").get("nativeDependencyFingerprint").stringValue();
        var inner=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","ownerEpoch","itemOrdinal","capturedConfigSha256","runtime"))inner.set(key,pack.get("command").get(key));
        inner.put("stage","VERIFY_RESULT").put("checkpointId",checkpoint.toString()).put("retainedSha256",SourceJson.sha(retained)).put("nativeFingerprint",fingerprint);
        var command=inner.deepCopy();command.put("recoveryVersion",1).put("policy",POLICY).put("attemptId",owner.controlId().toString()).put("ownerEpoch",Long.toString(owner.worker().epoch())).put("controlId",owner.controlId().toString()).put("instanceId",owner.worker().instanceId().toString()).put("qualificationSha256",owner.qualificationSha256()).put("bridgeSha256",bridgeSha());command.set("originalCommand",inner);command.set("launchReview",review.json());command.set("expectedPublication",handoffs.verificationExpectedPublication(guard,bootstrap));
        byte[] raw=SourceJson.canonical(command);files.write("command.json",raw,PreflightFiles.SMALL);files.write("retained.json",retained,PreflightFiles.LARGE);
        var commandIdentity=InstallationHostGuard.identity(files.directory.resolve("command.json"));var retainedIdentity=InstallationHostGuard.identity(files.directory.resolve("retained.json"));
        handoffs.verificationReview(guard,bootstrap,review,owner);
        var current=new Current();current.stage=begin(guard,owner,checkpoint,files.address,SourceJson.sha(raw),fingerprint);
        try {
            compatible(runtime);var once=new AtomicBoolean();
            var child=process.run(List.of(runtime.json().get("configuration").get("python").stringValue(),"-I","-B","-X","utf8","-c",new String(bridge(),java.nio.charset.StandardCharsets.UTF_8),files.directory.resolve("command.json").toString(),bridgeSha()),runtime.environment(),runtime.scratch(),Duration.ofSeconds(runtime.seconds()),new ReadOnlyVerifierProcess.Observer(){
                public void beforeStart(){need(once.compareAndSet(false,true),OWNERSHIP_LOST);runtime.admission();checkRequest(files,raw,retained,commandIdentity,retainedIdentity);handoffs.verificationReview(guard,bootstrap,review,current.stage.owner());current.stage=authorize(guard,current.stage,review);}
                public void started(InstallationHostGuard.ProcessIdentity child){current.stage=recordChild(guard,current.stage,child);}
                public void heartbeat(){runtime.check();checkRequest(files,raw,retained,commandIdentity,retainedIdentity);current.stage=RecoveryVerification.this.heartbeat(guard,current.stage);}
            });
            need(current.stage.child()!=null && current.stage.child().equals(child.child()) && child.descendants().isEmpty(),PROCESS_UNRESOLVED);runtime.check();checkRequest(files,raw,retained,commandIdentity,retainedIdentity);
            byte[] response,evidence;JsonNode result,qualified;
            try{response=files.read("result.json",PreflightFiles.SMALL);evidence=files.read("evidence.json",PreflightFiles.LARGE);result=PreflightFiles.canonical(response,PreflightFiles.SMALL);qualified=PreflightFiles.canonical(evidence,PreflightFiles.LARGE);validate(command,result,qualified,child);}
            catch(RuntimeException ex){throw new PreflightFailure(PreflightFailure.Code.RESULT_INVALID);}
            need(Arrays.equals(response,files.read("result.json",PreflightFiles.SMALL)) && Arrays.equals(evidence,files.read("evidence.json",PreflightFiles.LARGE)),EVIDENCE_CHANGED);
            String classification=qualified.get("classification").stringValue();
            var anchor=anchor(current.stage,command,classification,"OBSERVED_CLEAN_EXIT");anchor.put("resultSha256",SourceJson.sha(response)).put("evidenceSha256",SourceJson.sha(evidence));
            byte[] validated=SourceJson.canonical(anchor);files.write("verified.json",validated,PreflightFiles.SMALL);
            current.stage=finish(guard,current.stage,classification.equals("VERIFIED_PUBLICATION")?"VERIFIED":"FAILED",classification,SourceJson.sha(validated));return new Observation(current.stage,classification);
        }catch(PreflightFailure ex) {
            boolean interrupted=Thread.interrupted();try {
                String code=ex.code()==PreflightFailure.Code.RESULT_INVALID?"EVIDENCE_INVALID":ex.code().name();
                if(!SUPERVISION.contains(code) && !OBSERVATIONS.contains(code))code=ex.code()==PreflightFailure.Code.RUNTIME_MISMATCH?"RUNTIME_INCOMPATIBLE":ex.code()==PreflightFailure.Code.INPUT_UNAVAILABLE?"EVIDENCE_UNAVAILABLE":"EVIDENCE_INVALID";
                var anchor=anchor(current.stage,command,code,ex.quiescent()?(current.stage.child()==null && (code.equals("PROCESS_START_FAILED") || current.stage.state().equals("INTENT"))?"NO_CHILD_LAUNCHED":"CONFIRMED_TERMINATED"):"UNCONFIRMED");byte[] validated=SourceJson.canonical(anchor);
                files.write("verified.json",validated,PreflightFiles.SMALL);current.stage=finish(guard,current.stage,ex.quiescent()?"FAILED":"UNKNOWN",code,SourceJson.sha(validated));return new Observation(current.stage,code);
            }finally{if(interrupted)Thread.currentThread().interrupt();}
        }
        // Ownership, database and evidence failures escape without refresh, retry, final outcome or release.
    }
    private static void compatible(ReliabilityRuntime runtime) {
        try {
        Path root=Path.of(runtime.json().get("configuration").get("processingRoot").stringValue());
        for(var entry:Map.of("reliability_execution.py","11a881190b4c77fb6c1c887c0d8cb64e6cd10d5893a44c2fcb4cff759b9af982","reliability_execution_preflight.py","c528e4ec78ced7b574ada3efc4e62fbffde4e86149abe1ffd82dddad07192927").entrySet())
            if(!SourceJson.sha(PreflightFiles.read(root.resolve(entry.getKey()),1024*1024,false)).equals(entry.getValue()))throw new PreflightFailure(PreflightFailure.Code.RUNTIME_MISMATCH);
        }catch(PreflightFailure ex){throw new PreflightFailure(PreflightFailure.Code.RUNTIME_MISMATCH);}
    }
    private static void checkRequest(PreflightFiles f,byte[] raw,byte[] pack,InstallationHostGuard.Identity command,InstallationHostGuard.Identity retained){need(InstallationHostGuard.identity(f.directory.resolve("command.json")).equals(command) && InstallationHostGuard.identity(f.directory.resolve("retained.json")).equals(retained) && Arrays.equals(raw,f.read("command.json",PreflightFiles.SMALL)) && Arrays.equals(pack,f.read("retained.json",PreflightFiles.LARGE)),EVIDENCE_CHANGED);}
    private static tools.jackson.databind.node.ObjectNode anchor(Stage s,JsonNode command,String code,String cleanup){var n=SourceJson.object().put("policy",POLICY).put("checkpointId",s.checkpoint().toString()).put("commandSha256",s.commandSha()).put("retainedSha256",command.get("retainedSha256").stringValue()).put("bridgeSha256",bridgeSha()).put("classification",code).put("cleanup",cleanup);if(s.child()==null)n.putNull("child");else n.putObject("child").put("instanceId",s.child().instanceId().toString()).put("pid",Long.toString(s.child().pid())).put("startedAt",s.child().startedAt().toString());return n;}
    private Stage begin(InstallationHostGuard guard,RecoveryHandoff.Owner owner,UUID checkpoint,String address,String sha,String fingerprint) {
        return handoffs.fenced(guard,owner,()->{
            need(owner.attemptVersion()==0 && jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Long.class,owner.controlId())==0,OWNERSHIP_LOST);
            need(address.equals("runs/"+owner.runId()+"/attempts/"+owner.controlId()+"/verify-result/"+checkpoint) && sha.matches("[0-9a-f]{64}") && fingerprint.matches("[0-9a-f]{64}"),INVALID_REQUEST);
            need(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations s JOIN ddip_management.attempts a USING(attempt_id) WHERE a.run_id=?",Long.class,owner.runId())<512,CAPACITY_EXCEEDED);
            capacity(2);exactly(jdbc.update("UPDATE ddip_management.attempts SET state='RUNNING',row_version=row_version+1 WHERE attempt_id=? AND kind='RECONCILE' AND state='CLAIMED' AND row_version=?",owner.controlId(),owner.attemptVersion()));
            jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,intended_identity,intended_namespace,command_address,command_sha256) VALUES (?,'VERIFY_RESULT',0,?,'INTENT',?,?,?,?,?)",owner.controlId(),checkpoint,sha,fingerprint,"v2/processor_revision=2/publication="+fingerprint,address+"/command.json",sha);
            var next=new RecoveryHandoff.Owner(owner.worker(),owner.runId(),owner.controlId(),owner.predecessor(),owner.runVersion(),1,owner.controlVersion(),owner.qualificationSha256());var s=new Stage(next,checkpoint,"INTENT",0,address,sha,fingerprint,null);
            audit(s,"RECOVERY_ATTEMPT_RUNNING","CLAIMED","RUNNING",0L,1L,"STARTED");audit(s,"RECOVERY_STAGE_INTENT",null,"INTENT",null,0L,"ACCEPTED");fault.accept(Step.INTENT);return s;
        });
    }
    private Stage authorize(InstallationHostGuard guard,Stage s,RecoveryQuiescence.Review review) {
        return handoffs.fenced(guard,s.owner(),()->{stage(s);handoffs.verificationFresh(review);need(s.state().equals("INTENT") && s.child()==null,OWNERSHIP_LOST);capacity(1);
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state='RUNNING',started_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=0 AND checkpoint_id=? AND state='INTENT' AND row_version=? AND child_pid IS NULL",s.owner().controlId(),s.checkpoint(),s.version()));
            audit(s,"RECOVERY_LAUNCH_AUTHORIZED","INTENT","RUNNING",s.version(),s.version()+1,"STARTED");fault.accept(Step.AUTHORIZED);return next(s,s.owner(),"RUNNING",null);
        });
    }
    private Stage recordChild(InstallationHostGuard guard,Stage s,InstallationHostGuard.ProcessIdentity child) {
        return handoffs.fenced(guard,s.owner(),()->{stage(s);need(s.state().equals("RUNNING") && s.child()==null && child!=null && child.pid()>0 && child.startedAt()!=null && child.instanceId()!=null,OWNERSHIP_LOST);capacity(1);
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET child_pid=?,child_started_at=?,child_instance_id=?,row_version=row_version+1 WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=0 AND checkpoint_id=? AND state='RUNNING' AND row_version=? AND child_pid IS NULL",child.pid(),Timestamp.from(child.startedAt()),child.instanceId(),s.owner().controlId(),s.checkpoint(),s.version()));
            audit(s,"RECOVERY_CHILD_STARTED","RUNNING","RUNNING",s.version(),s.version()+1,"STARTED");fault.accept(Step.CHILD);return next(s,s.owner(),"RUNNING",child);
        });
    }
    private Stage heartbeat(InstallationHostGuard guard,Stage s) {handoffs.fenced(guard,s.owner(),()->{stage(s);need(s.state().equals("RUNNING"),OWNERSHIP_LOST);return null;});var owner=handoffs.heartbeat(guard,s.owner());return new Stage(owner,s.checkpoint(),s.state(),s.version(),s.address(),s.commandSha(),s.fingerprint(),s.child());}
    private Stage finish(InstallationHostGuard guard,Stage s,String state,String code,String sha) {
        return handoffs.fenced(guard,s.owner(),()->{stage(s);need(Set.of("INTENT","RUNNING").contains(s.state()) && (OBSERVATIONS.contains(code)||SUPERVISION.contains(code)),OWNERSHIP_LOST);capacity(1);
            exactly(jdbc.update("UPDATE ddip_management.stage_invocations SET state=?,outcome_code=?,result_address=?,result_sha256=?,finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=0 AND checkpoint_id=? AND state=? AND row_version=?",state,code,s.address()+"/verified.json",sha,s.owner().controlId(),s.checkpoint(),s.state(),s.version()));
            audit(s,"RECOVERY_STAGE_OBSERVED",s.state(),state,s.version(),s.version()+1,code);fault.accept(Step.RESULT);return next(s,s.owner(),state,s.child());
        });
    }
    private void stage(Stage s) {
        var row=one(jdbc.queryForList("SELECT checkpoint_id,state,row_version,command_address,command_sha256,input_evidence_sha256,intended_identity,child_pid,child_started_at,child_instance_id FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=0 FOR UPDATE",s.owner().controlId()));
        need(s.owner().attemptVersion()==1 && s.checkpoint().equals(row.get("checkpoint_id")) && s.state().equals(row.get("state")) && s.version()==number(row,"row_version") && s.commandSha().equals(row.get("command_sha256")) && s.commandSha().equals(row.get("input_evidence_sha256")) && s.fingerprint().equals(row.get("intended_identity")) && (s.address()+"/command.json").equals(row.get("command_address")),OWNERSHIP_LOST);
        need(s.child()==null?row.get("child_pid")==null:s.child().pid()==number(row,"child_pid") && s.child().instanceId().equals(row.get("child_instance_id")) && s.child().startedAt().equals(((Timestamp)row.get("child_started_at")).toInstant()),OWNERSHIP_LOST);
    }
    private static Stage next(Stage s,RecoveryHandoff.Owner owner,String state,InstallationHostGuard.ProcessIdentity child){return new Stage(owner,s.checkpoint(),state,s.version()+1,s.address(),s.commandSha(),s.fingerprint(),child);}
    private void capacity(int count){need(AuditCapacity.available(jdbc,count,jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class)),CAPACITY_EXCEEDED);}
    private void audit(Stage s,String action,String before,String after,Long bv,Long av,String code){byte[] details=SourceJson.canonical(SourceJson.object().put("policy",POLICY).put("checkpointId",s.checkpoint().toString()).put("commandSha256",s.commandSha()).put("qualificationSha256",s.owner().qualificationSha256()).put("bridgeSha256",bridgeSha()));jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,?,?)",s.owner().runId(),s.owner().controlId(),s.owner().controlId(),action,before,after,bv,av,code,details);}
    private static void validate(JsonNode c,JsonNode result,JsonNode e,ReadOnlyVerifierProcess.Result child) {
        try {
            SourceJson.fields(result,"protocolVersion","runId","attemptId","checkpointId","classification","evidenceSha256");
            SourceJson.fields(e,"policy","runId","attemptId","checkpointId","ownerEpoch","controlId","instanceId","qualificationSha256","bridgeSha256","originalAttemptId","originalEpoch","nativeFingerprint","classification","phase","readiness","code","cleanup","publication");
            need(child.exitCode()==0 && child.descendants().isEmpty() && result.get("protocolVersion").isIntegralNumber() && result.get("protocolVersion").intValue()==1, EVIDENCE_CHANGED);
            for(String key:List.of("runId","attemptId","checkpointId"))need(result.get(key).equals(c.get(key)) && e.get(key).equals(c.get(key)),EVIDENCE_CHANGED);
            for(String key:List.of("policy","ownerEpoch","controlId","instanceId","qualificationSha256","bridgeSha256","nativeFingerprint"))need(e.get(key).equals(c.get(key)),EVIDENCE_CHANGED);
            need(e.get("originalAttemptId").equals(c.get("originalCommand").get("attemptId")) && e.get("originalEpoch").equals(c.get("originalCommand").get("ownerEpoch")) && result.get("classification").equals(e.get("classification")) && result.get("evidenceSha256").stringValue().equals(SourceJson.sha(SourceJson.canonical(e))),EVIDENCE_CHANGED);
            String classification=e.get("classification").stringValue();need(OBSERVATIONS.contains(classification) && e.get("cleanup").equals(SourceJson.object().put("kind","READ_ONLY")),EVIDENCE_CHANGED);
            need(Set.of("RUNTIME","DEPENDENCIES","OUTPUT").contains(e.get("phase").stringValue()) && Set.of("READY","FAILED").contains(e.get("readiness").stringValue()),EVIDENCE_CHANGED);
            if(!e.get("code").isNull())need(ReliabilityPreflight.NATIVE_CODES.contains(e.get("code").stringValue()) || Set.of("PUBLICATION_INVALID","PUBLICATION_CHANGED","PUBLICATION_UNAVAILABLE","PUBLICATION_INCOMPLETE","QUERY_FAILED").contains(e.get("code").stringValue()),EVIDENCE_CHANGED);
            if(!classification.equals("RUNTIME_INCOMPATIBLE"))need(e.get("readiness").stringValue().equals("READY"),EVIDENCE_CHANGED);
            if(classification.equals("VERIFIED_PUBLICATION")) {
                need(e.get("code").isNull() && e.get("readiness").stringValue().equals("READY") && e.get("phase").stringValue().equals("OUTPUT"),EVIDENCE_CHANGED);
                var inner=c.get("originalCommand");var r=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))r.set(key,inner.get(key));r.put("status","OK").putNull("code").put("evidenceId",inner.get("checkpointId").stringValue());r.set("counts",e.get("publication").get("counts"));
                var nativeEvidence=SourceJson.object().put("policy","E5_RELIABILITY_RESULT_V1");for(String key:List.of("runId","attemptId","checkpointId","nativeFingerprint"))nativeEvidence.set(key,inner.get(key));nativeEvidence.set("cleanup",e.get("cleanup"));nativeEvidence.set("publication",e.get("publication"));
                ReliabilityExecution.validate(inner,r,nativeEvidence,child);
            }else need(e.get("publication").isNull(),EVIDENCE_CHANGED);
        }catch(RuntimeException ex){throw new PreflightFailure(PreflightFailure.Code.RESULT_INVALID);}
    }
    Inspection inspect(InstallationHostGuard guard,UUID attempt) {
        try{return inspectRecorded(guard,attempt);}catch(RecoveryFailure | ManagementFailure | ImportOwnershipFailure | PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new RecoveryFailure(EVIDENCE_CHANGED);}
    }
    private Inspection inspectRecorded(InstallationHostGuard guard,UUID attempt) {
        need(guard!=null && attempt!=null && !TransactionSynchronizationManager.isActualTransactionActive(),INVALID_REQUEST);guard.checkHeld(guard.binding().installationId());
        var row=tx.required(()->{
            var coordinator=one(jdbc.queryForList("SELECT installation_id FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE"));need(guard.binding().installationId().equals(coordinator.get("installation_id")),OWNERSHIP_LOST);
            var s=one(jdbc.queryForList("SELECT s.checkpoint_id,s.state,s.row_version,s.command_address,s.command_sha256,s.input_evidence_sha256,s.result_address,s.result_sha256,s.outcome_code,s.child_pid,s.child_started_at,s.child_instance_id,s.started_at,a.run_id,a.kind,w.installation_id FROM ddip_management.stage_invocations s JOIN ddip_management.attempts a USING(attempt_id) JOIN ddip_management.worker_instances w USING(instance_id) WHERE s.attempt_id=? AND s.stage='VERIFY_RESULT' AND s.item_ordinal=0",attempt));
            need("RECONCILE".equals(s.get("kind")) && guard.binding().installationId().equals(s.get("installation_id")),EVIDENCE_CHANGED);
            var audit=one(jdbc.queryForList("SELECT details_bytes FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND control_id=? AND action='RECOVERY_HANDOFF' ORDER BY event_seq LIMIT 2",s.get("run_id"),attempt,attempt));s.put("handoff_details",audit.get("details_bytes"));
            if(s.get("result_sha256")!=null){var observed=one(jdbc.queryForList("SELECT after_state,after_version,safe_code,details_bytes FROM ddip_management.audit_events WHERE run_id=? AND attempt_id=? AND control_id=? AND action='RECOVERY_STAGE_OBSERVED' ORDER BY event_seq LIMIT 2",s.get("run_id"),attempt,attempt));need(s.get("state").equals(observed.get("after_state")) && number(s,"row_version")==number(observed,"after_version") && s.get("outcome_code").equals(observed.get("safe_code")),EVIDENCE_CHANGED);s.put("observed_details",observed.get("details_bytes"));}
            return s;
        });
        var details=SourceJson.parse((byte[])row.get("handoff_details"));String address="runs/"+row.get("run_id")+"/attempts/"+attempt+"/verify-result/"+row.get("checkpoint_id");
        String qAddress=details.get("qualificationAddress").stringValue(),qSha=details.get("qualificationSha256").stringValue();var packet=RecoveryPacket.parse(RecoveryFiles.read(guard,qAddress),qAddress,qSha,guard);
        need(packet.descriptor.get("controlId").stringValue().equals(attempt.toString()) && packet.descriptor.get("runId").stringValue().equals(row.get("run_id").toString()),EVIDENCE_CHANGED);
        need((address+"/command.json").equals(row.get("command_address")),EVIDENCE_CHANGED);
        byte[] raw=RecoveryFiles.readPrivate(guard,address+"/command.json",PreflightFiles.SMALL);var c=PreflightFiles.canonical(raw,PreflightFiles.SMALL);
        need(SourceJson.sha(raw).equals(row.get("command_sha256")) && row.get("input_evidence_sha256").equals(row.get("command_sha256")) && c.get("policy").stringValue().equals(POLICY) && c.get("attemptId").stringValue().equals(attempt.toString()) && c.get("checkpointId").stringValue().equals(row.get("checkpoint_id").toString()) && c.get("qualificationSha256").stringValue().equals(qSha) && c.get("bridgeSha256").stringValue().equals(bridgeSha()),EVIDENCE_CHANGED);
        var review=RecoveryPacket.review(c.get("launchReview"));need(review.targetSha256().equals(packet.descriptor.get("snapshotSha256").stringValue()) && review.handlersStopped() && review.unmanagedWritersStopped() && review.orphanReviewComplete() && review.reviewedCheckpoints().equals(packet.review.reviewedCheckpoints()) && review.descendants().containsAll(packet.review.descendants()),EVIDENCE_CHANGED);
        if(row.get("started_at")!=null)RecoveryQuiescence.fresh(review.inspectedAt(),((Timestamp)row.get("started_at")).toInstant());
        need(SourceJson.sha(RecoveryFiles.readPrivate(guard,address+"/retained.json",PreflightFiles.LARGE)).equals(c.get("retainedSha256").stringValue()),EVIDENCE_CHANGED);
        if(row.get("result_sha256")==null)return new Inspection((String)row.get("state"),"INCONCLUSIVE",null);
        need((address+"/verified.json").equals(row.get("result_address")),EVIDENCE_CHANGED);byte[] validated=RecoveryFiles.readPrivate(guard,address+"/verified.json",PreflightFiles.SMALL);need(SourceJson.sha(validated).equals(row.get("result_sha256")),EVIDENCE_CHANGED);var a=PreflightFiles.canonical(validated,PreflightFiles.SMALL);
        need(a.get("policy").stringValue().equals(POLICY) && a.get("classification").stringValue().equals(row.get("outcome_code")) && a.get("commandSha256").stringValue().equals(SourceJson.sha(raw)) && a.get("retainedSha256").equals(c.get("retainedSha256")) && a.get("bridgeSha256").equals(c.get("bridgeSha256")) && a.get("checkpointId").equals(c.get("checkpointId")),EVIDENCE_CHANGED);
        var audit=SourceJson.parse((byte[])row.get("observed_details"));need(audit.equals(SourceJson.object().put("policy",POLICY).put("checkpointId",c.get("checkpointId").stringValue()).put("commandSha256",SourceJson.sha(raw)).put("qualificationSha256",qSha).put("bridgeSha256",bridgeSha())),EVIDENCE_CHANGED);
        InstallationHostGuard.ProcessIdentity child=null;
        if(row.get("child_pid")!=null){child=new InstallationHostGuard.ProcessIdentity((UUID)row.get("child_instance_id"),number(row,"child_pid"),((Timestamp)row.get("child_started_at")).toInstant());need(a.get("child").equals(SourceJson.object().put("instanceId",child.instanceId().toString()).put("pid",Long.toString(child.pid())).put("startedAt",child.startedAt().toString())),EVIDENCE_CHANGED);}else need(a.get("child").isNull(),EVIDENCE_CHANGED);
        String code=a.get("classification").stringValue();JsonNode e=null;
        if(a.has("resultSha256")) {
            need(child!=null && a.get("cleanup").stringValue().equals("OBSERVED_CLEAN_EXIT"),EVIDENCE_CHANGED);
            byte[] response=RecoveryFiles.readPrivate(guard,address+"/result.json",PreflightFiles.SMALL),evidence=RecoveryFiles.readPrivate(guard,address+"/evidence.json",PreflightFiles.LARGE);
            need(SourceJson.sha(response).equals(a.get("resultSha256").stringValue()) && SourceJson.sha(evidence).equals(a.get("evidenceSha256").stringValue()),EVIDENCE_CHANGED);e=PreflightFiles.canonical(evidence,PreflightFiles.LARGE);validate(c,PreflightFiles.canonical(response,PreflightFiles.SMALL),e,new ReadOnlyVerifierProcess.Result(0,child,0,0,Set.of()));
            need(code.equals("VERIFIED_PUBLICATION")?row.get("state").equals("VERIFIED"):row.get("state").equals("FAILED"),EVIDENCE_CHANGED);
        }else {need(SUPERVISION.contains(code) || Set.of("RUNTIME_INCOMPATIBLE","RESOURCE_LIMIT").contains(code),EVIDENCE_CHANGED);need(a.get("cleanup").stringValue().equals("UNCONFIRMED")?row.get("state").equals("UNKNOWN"):Set.of("CONFIRMED_TERMINATED","NO_CHILD_LAUNCHED").contains(a.get("cleanup").stringValue()) && row.get("state").equals("FAILED"),EVIDENCE_CHANGED);}
        if(a.get("cleanup").stringValue().equals("NO_CHILD_LAUNCHED"))need(child==null,EVIDENCE_CHANGED);
        guard.checkHeld(guard.binding().installationId());return new Inspection((String)row.get("state"),code,e);
    }
    private static long number(Map<String,Object> row,String key){return ((Number)row.get(key)).longValue();}
    private static Map<String,Object> one(List<Map<String,Object>> rows){need(rows.size()==1,EVIDENCE_CHANGED);return rows.getFirst();}
    private static void exactly(int count){need(count==1,OWNERSHIP_LOST);}
}
