package platform.contentapi.execution;

import java.nio.file.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.ContentResumePlan.require;
import static platform.contentapi.management.SourceFailure.Code.*;

/** Existing proof readers and a supervised read-only verifier; never holds a transaction during I/O. */
final class ContentResumeEvidence {
    record Verified(JsonNode result,String address,String sha,String privateSha,String bridgeSha){}
    final JdbcTemplate j;final ManagementTransactions tx;private final RecoveryQuiescence.Processes processes;
    ContentResumeEvidence(JdbcTemplate j,ManagementTransactions tx){this(j,tx,RecoveryQuiescence.LOCAL);}
    ContentResumeEvidence(JdbcTemplate j,ManagementTransactions tx,RecoveryQuiescence.Processes p){this.j=j;this.tx=tx;processes=p;}
    JsonNode frozen(UUID run){return tx.required(()->{var r=j.queryForMap("SELECT captured_selection,selection_sha256 FROM ddip_management.runs WHERE run_id=?",run);return FrozenRunSelection.canonical((byte[])r.get("captured_selection"),(String)r.get("selection_sha256"),PreflightFiles.LARGE);});}
    JsonNode privateProof(InstallationHostGuard g,JsonNode plan,RssContentRuntime runtime){
        UUID run=UUID.fromString(plan.get("runId").stringValue()),original=UUID.fromString(plan.get("originalAttemptId").stringValue()),recovery=UUID.fromString(plan.get("recoveryAttemptId").stringValue());
        var anchor=tx.required(()->j.queryForList("SELECT details_bytes FROM ddip_management.audit_events WHERE attempt_id=? AND control_id=? AND action='RECOVERY_HANDOFF'",recovery,recovery));require(anchor.size()==1,RESUME_EVIDENCE_INVALID);var d=SourceJson.parse((byte[])anchor.getFirst().get("details_bytes"));String address=d.get("qualificationAddress").stringValue(),sha=d.get("qualificationSha256").stringValue();
        require(sha.equals(plan.get("qualificationSha256").stringValue()),RESUME_EVIDENCE_INVALID);var packet=RecoveryPacket.parse(RecoveryFiles.read(g,address),address,sha,g);
        require(packet.descriptor.get("oldAttemptId").stringValue().equals(original.toString()),RESUME_CHECKPOINT_UNSUPPORTED);
        require(packet.descriptor.get("runId").stringValue().equals(run.toString()) && packet.descriptor.get("selectionSha256").equals(plan.get("selectionSha256")),RESUME_EVIDENCE_INVALID);
        var runtimeJson=RecoveryRuntime.contentJson(runtime);
        require(SourceJson.sha(SourceJson.canonical(runtimeJson)).equals(packet.descriptor.get("runtimeSha256").stringValue()),RESUME_EVIDENCE_INVALID);
        var current=ContentRecoveryFiles.capture(g,run,original,packet.stages,runtime.config.get("contentRuntimeVersion").intValue()==2);require(!current.inventory().has("processEvidenceIncomplete"),RESUME_EVIDENCE_UNAVAILABLE);require(current.inventory().equals(packet.descriptor.get("privateEvidence")),RESUME_EVIDENCE_INVALID);
        var identities=new ArrayList<RecoveryQuiescence.Identity>();var worker=packet.rows.get(4);identities.add(new RecoveryQuiescence.Identity(((Number)worker.get("pid")).longValue(),((Timestamp)worker.get("process_started_at")).toInstant()));
        for(var s:packet.stages)if(s.get("child_pid")!=null)identities.add(new RecoveryQuiescence.Identity(((Number)s.get("child_pid")).longValue(),((Timestamp)s.get("child_started_at")).toInstant()));identities.addAll(packet.review.descendants());
        for(var identity:identities){var state=processes.inspect(identity);require(state==RecoveryQuiescence.State.ABSENT || state==RecoveryQuiescence.State.REUSED,RESUME_EVIDENCE_UNAVAILABLE);}
        for(long pid:current.descendantPids())require(ProcessHandle.of(pid).filter(ProcessHandle::isAlive).isEmpty(),RESUME_EVIDENCE_UNAVAILABLE);
        byte[] raw=RecoveryFiles.read(g,plan.get("observationAddress").stringValue());require(SourceJson.sha(raw).equals(plan.get("observationSha256").stringValue()),RESUME_EVIDENCE_INVALID);var observed=SourceJson.parse(raw,RecoveryFiles.MAXIMUM);require(observed.get("state").stringValue().equals("PAUSED") && observed.get("code").stringValue().equals("RECOVERED_CHECKPOINT"),RESUME_EVIDENCE_INVALID);
        var recoveryStages=tx.required(()->j.queryForList("SELECT * FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY item_ordinal",recovery));var recoveryFiles=ContentRecoveryFiles.capture(g,run,recovery,recoveryStages,runtime.config.get("contentRuntimeVersion").intValue()==2);require(!recoveryFiles.inventory().has("processEvidenceIncomplete"),RESUME_EVIDENCE_UNAVAILABLE);
        for(var s:recoveryStages)if(s.get("child_pid")!=null){var state=processes.inspect(new RecoveryQuiescence.Identity(((Number)s.get("child_pid")).longValue(),((Timestamp)s.get("child_started_at")).toInstant()));require(state==RecoveryQuiescence.State.ABSENT || state==RecoveryQuiescence.State.REUSED,RESUME_EVIDENCE_UNAVAILABLE);}
        for(long pid:recoveryFiles.descendantPids())require(ProcessHandle.of(pid).filter(ProcessHandle::isAlive).isEmpty(),RESUME_EVIDENCE_UNAVAILABLE);
        return SourceJson.object().put("observationSha256",SourceJson.sha(raw)).put("originalSha256",SourceJson.sha(SourceJson.canonical(current.inventory()))).put("recoverySha256",SourceJson.sha(SourceJson.canonical(recoveryFiles.inventory())));
    }
    /** A crashed earlier qualifier cannot be inferred quiescent from its parent/PID alone. */
    static void previous(InstallationHostGuard g,UUID run,UUID control){previous(g,run,control,false);}
    private static void previous(InstallationHostGuard g,UUID run,UUID control,boolean bounded){try{previousFiles(g,run,control,bounded);}catch(SourceFailure ex){if(ex.code==INVALID_ARGUMENT)throw new SourceFailure(RESUME_EVIDENCE_INVALID);throw ex;}catch(IllegalArgumentException|NullPointerException ex){throw new SourceFailure(RESUME_EVIDENCE_INVALID);}}
    private static void previousFiles(InstallationHostGuard g,UUID run,UUID control,boolean bounded){
        var capture=ContentRecoveryFiles.capture(g,run,control,List.of(),bounded);
        require(!capture.inventory().has("processEvidenceIncomplete"),RESUME_EVIDENCE_UNAVAILABLE);
        for(var entry:capture.inventory().get("files"))if(!entry.get("directory").booleanValue() && entry.get("address").stringValue().endsWith("/command.json")){
            Path dir=g.binding().root().resolve(entry.get("address").stringValue()).getParent();
            var seal=PreflightFiles.canonical(PreflightFiles.read(dir.resolve("verified.json"),65536,true),65536);SourceJson.fields(seal,"resumeVerifierVersion","commandSha256","resultSha256","childSha256","quiescent");require(seal.get("resumeVerifierVersion").intValue()==1 && seal.get("quiescent").booleanValue(),RESUME_EVIDENCE_UNAVAILABLE);
            for(String kind:List.of("command","result","child"))require(SourceJson.sha(PreflightFiles.read(dir.resolve(kind+".json"),kind.equals("command")?PreflightFiles.LARGE:65536,true)).equals(seal.get(kind+"Sha256").stringValue()),RESUME_EVIDENCE_INVALID);
            var child=PreflightFiles.canonical(PreflightFiles.read(dir.resolve("child.json"),65536,true),65536);var state=RecoveryQuiescence.LOCAL.inspect(new RecoveryQuiescence.Identity(Long.parseLong(child.get("pid").stringValue()),Instant.parse(child.get("startedAt").stringValue())));require(state==RecoveryQuiescence.State.ABSENT || state==RecoveryQuiescence.State.REUSED,RESUME_EVIDENCE_UNAVAILABLE);
        }
    }
    static void completed(PreflightFiles files){
        var seal=SourceJson.object().put("resumeVerifierVersion",1).put("quiescent",true);for(String kind:List.of("command","result","child"))seal.put(kind+"Sha256",SourceJson.sha(files.read(kind+".json",kind.equals("command")?PreflightFiles.LARGE:65536)));files.write("verified.json",SourceJson.canonical(seal),65536);
    }
    void retained(InstallationHostGuard g,JsonNode plan){
        try{previous(g,UUID.fromString(plan.get("runId").stringValue()),UUID.fromString(plan.get("resumeControlId").stringValue()),ContentEvidenceProfile.bounded(frozen(UUID.fromString(plan.get("runId").stringValue()))));byte[] raw=RecoveryFiles.readPrivate(g,plan.get("verificationAddress").stringValue(),PreflightFiles.LARGE);require(SourceJson.sha(raw).equals(plan.get("verificationSha256").stringValue()),RESUME_EVIDENCE_INVALID);PreflightFiles.canonical(raw,PreflightFiles.LARGE);}catch(RecoveryFailure ex){throw new SourceFailure(ex.getMessage().equals("EVIDENCE_CHANGED")?RESUME_EVIDENCE_INVALID:RESUME_EVIDENCE_UNAVAILABLE);}catch(PreflightFailure ex){throw new SourceFailure(ex.code()==PreflightFailure.Code.INPUT_CHANGED?RESUME_EVIDENCE_INVALID:RESUME_EVIDENCE_UNAVAILABLE);}
    }
    Verified verify(InstallationHostGuard g,JsonNode plan,String runtimeId,UUID control){
        try{
            g.checkHeld(g.binding().installationId());UUID run=UUID.fromString(plan.get("runId").stringValue()),original=UUID.fromString(plan.get("originalAttemptId").stringValue());previous(g,run,control,ContentEvidenceProfile.bounded(frozen(run)));tx.required(()->{var expected=ContentResumePlan.checkpoint(j,j.queryForMap("SELECT * FROM ddip_management.runs WHERE run_id=?",run),UUID.fromString(plan.get("recoveryAttemptId").stringValue()));for(String key:expected.propertyNames())require(expected.get(key).equals(plan.get(key)),RESUME_EVIDENCE_INVALID);return null;});var frozen=frozen(run);var runtime=RssContentRuntime.load(j,tx,g,run,frozen,runtimeId);var before=privateProof(g,plan,runtime);var reference=RssContentInputs.reference(frozen);
            JsonNode contentCommand=null;
            if(!plan.get("boundary").stringValue().equals("BEFORE_FETCH")){
                var gold=new RssGoldExecution(j,tx,new WorkerRepository(j,tx,g.binding().installationId())).committed(g,run,original,runtime.config.get("goldRuntimeId").stringValue());
                tx.required(()->{RssContentInputs.check(j,run,original,frozen,gold,reference);return null;});JsonNode prepared=null;
                if(plan.get("boundary").stringValue().equals("CONTENT_INPUTS")){
                    var row=tx.required(()->j.queryForMap("SELECT checkpoint_id,result_sha256 FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='CHECK_INPUTS' AND state='VERIFIED'",original));var old=new PreflightFiles(g,run,original,(UUID)row.get("checkpoint_id"),"CHECK_INPUTS");RssGoldExecution.verifyStageAnchor(old);require(SourceJson.sha(old.read("verified.json",65536)).equals(row.get("result_sha256")),RESUME_EVIDENCE_INVALID);prepared=SourceJson.parse(old.read("evidence.json",PreflightFiles.LARGE),PreflightFiles.LARGE).get("prepared");
                }
                contentCommand=RssContentExecution.resumeCommand(run,original,frozen,gold,reference,runtime,prepared);
            }
            var c=SourceJson.object().put("boundary",plan.get("boundary").stringValue());c.set("reference",reference);c.set("referenceBinding",runtime.binding(runtime.reference));c.set("content",runtime.binding(runtime.content));c.set("contentCommand",contentCommand);
            ContentEvidenceProfile.initialize(g,frozen,run,control);
            var files=new PreflightFiles(g,run,control,UUID.randomUUID(),"VERIFY_RESULT");byte[] command=SourceJson.canonical(c);files.write("command.json",command,PreflightFiles.LARGE);
            boolean bounded=runtime.config.get("contentRuntimeVersion").intValue()==2;
            Path bridge=Path.of(runtime.gold.silver.config.get("processingRoot").stringValue()).resolve(bounded?"bounded_content_producer.py":"content_resume_verifier.py");String bridgeSha=SourceJson.sha(PreflightFiles.read(bridge,1024*1024,false));boolean spark=!plan.get("boundary").stringValue().equals("BEFORE_FETCH");
            var invocation=bounded?ContentEvidenceProfile.command(runtime.gold.silver.config,"RESUME",files.directory.resolve("command.json")):List.of(runtime.gold.config.get("python").stringValue(),"-I","-B","-X","utf8",bridge.toString(),files.directory.resolve("command.json").toString());
            var result=new ReadOnlyVerifierProcess(spark).run(invocation,runtime.gold.silver.environment(),files.directory,Duration.ofSeconds(runtime.config.get("seconds").intValue()),new ReadOnlyVerifierProcess.Observer(){
                public void beforeStart(){g.checkHeld(g.binding().installationId());runtime.check();require(Arrays.equals(command,files.read("command.json",PreflightFiles.LARGE)),RESUME_EVIDENCE_INVALID);}
                public void started(InstallationHostGuard.ProcessIdentity child){RssGoldExecution.recordChild(files,child);}
                public void heartbeat(){g.checkHeld(g.binding().installationId());runtime.check();}
            });
            completed(files);previous(g,run,control);var response=PreflightFiles.canonical(files.read("result.json",65536),65536);require(response.get("commandSha256").stringValue().equals(SourceJson.sha(command)),RESUME_EVIDENCE_INVALID);
            if(result.exitCode()!=0){String code=response.get("code").stringValue();throw new SourceFailure(code.equals("RESUME_NAMESPACE_UNSAFE")?RESUME_NAMESPACE_UNSAFE:code.equals("RESUME_EVIDENCE_INVALID")?RESUME_EVIDENCE_INVALID:RESUME_EVIDENCE_UNAVAILABLE);}
            require(response.get("status").stringValue().equals("VERIFIED") && response.get("cleanup").stringValue().equals("CONFIRMED") && response.get("code").isNull(),RESUME_EVIDENCE_UNAVAILABLE);
            byte[] raw=files.read("evidence.json",PreflightFiles.LARGE);require(SourceJson.sha(raw).equals(response.get("evidenceSha256").stringValue()) && before.equals(privateProof(g,plan,runtime)) && bridgeSha.equals(SourceJson.sha(PreflightFiles.read(bridge,1024*1024,false))) && Arrays.equals(command,files.read("command.json",PreflightFiles.LARGE)),RESUME_EVIDENCE_INVALID);
            var value=PreflightFiles.canonical(raw,PreflightFiles.LARGE);require(value.get("boundary").equals(plan.get("boundary")),RESUME_EVIDENCE_INVALID);if(!plan.get("boundary").stringValue().equals("BEFORE_FETCH"))require(!tx.required(()->ContentResumeLineage.registered(j,"CONTENT",runtime.config.get("contentBindingId").stringValue(),"v1/processor_revision=1/publication="+value.get("prepared").get("fingerprint").stringValue())),RESUME_NAMESPACE_UNSAFE);
            return new Verified(value,files.address+"/evidence.json",SourceJson.sha(raw),SourceJson.sha(SourceJson.canonical(before)),bridgeSha);
        }catch(SourceFailure|ManagementFailure ex){throw ex;}catch(RecoveryFailure ex){throw new SourceFailure(ex.getMessage().equals("EVIDENCE_CHANGED")?RESUME_EVIDENCE_INVALID:RESUME_EVIDENCE_UNAVAILABLE);}catch(PreflightFailure ex){throw new SourceFailure(ex.code()==PreflightFailure.Code.INPUT_CHANGED?RESUME_EVIDENCE_INVALID:RESUME_EVIDENCE_UNAVAILABLE);}catch(IllegalArgumentException|NullPointerException ex){throw new SourceFailure(RESUME_EVIDENCE_INVALID);}
    }
}
