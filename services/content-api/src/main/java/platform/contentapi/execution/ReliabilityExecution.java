package platform.contentapi.execution;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Fixed Reliability adapter. Publication activation and recovery remain separate operations. */
public final class ReliabilityExecution {
    public record Completed(UUID artifactId,WorkerRepository.Session worker) { }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    private final ReadOnlyVerifierProcess publisher,verifier;
    public ReliabilityExecution(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){this(jdbc,tx,workers,new ReadOnlyVerifierProcess(true),new ReadOnlyVerifierProcess());}
    ReliabilityExecution(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,ReadOnlyVerifierProcess publisher,ReadOnlyVerifierProcess verifier){this.jdbc=jdbc;this.tx=tx;this.workers=workers;this.publisher=publisher;this.verifier=verifier;}
    public Completed run(InstallationHostGuard guard,ReliabilityPreflight.Verified prepared,String runtimeId) {
        if(!workers.executionAvailable())throw new WorkerFailure(WorkerFailure.Code.WORKER_NOT_READY);
        var owner=prepared.stage().owner();JsonNode frozen=workers.frozenReliability(guard,owner);
        ReliabilityRuntime runtime;JsonNode pack;
        try {
            runtime=ReliabilityRuntime.load(jdbc,tx,guard,owner,runtimeId,frozen);runtime.admission();
            var receipt=workers.verifiedCheckInputs(guard,owner,prepared.stage().checkpointId());
            var prior=new PreflightFiles(guard,owner.runId(),owner.attemptId(),receipt.checkpoint());
            byte[] command=prior.read("command.json",PreflightFiles.SMALL),anchor=prior.read("verified.json",PreflightFiles.SMALL),result=prior.read("result.json",PreflightFiles.SMALL),evidence=prior.read("evidence.json",PreflightFiles.LARGE),bundle=prior.read("selection.json",PreflightFiles.LARGE);
            need(receipt.commandAddress().equals(prior.address+"/command.json") && receipt.resultAddress().equals(prior.address+"/verified.json") && SourceJson.sha(command).equals(receipt.commandSha()) && SourceJson.sha(anchor).equals(receipt.resultSha()),INPUT_CHANGED);
            var c=PreflightFiles.canonical(command,PreflightFiles.SMALL);var a=PreflightFiles.canonical(anchor,PreflightFiles.SMALL);var r=PreflightFiles.canonical(result,PreflightFiles.SMALL);var e=PreflightFiles.canonical(evidence,PreflightFiles.LARGE);var b=PreflightFiles.canonical(bundle,PreflightFiles.LARGE);
            need(a.get("commandSha256").stringValue().equals(SourceJson.sha(command)) && a.get("resultSha256").stringValue().equals(SourceJson.sha(result)) && a.get("evidenceSha256").stringValue().equals(SourceJson.sha(evidence)) && a.get("cleanup").stringValue().equals("OBSERVED_CLEAN_EXIT") && c.get("runtime").equals(runtime.json()) && b.get("capturedSelection").equals(frozen),INPUT_CHANGED);
            ReliabilityPreflight.validateResult(c,r,0);ReliabilityPreflight.validateEvidence(c,frozen,r,e);
            need(prepared.nativeDependencyFingerprint().equals(e.get("nativeDependencyFingerprint").stringValue()) && prepared.evidenceSha256().equals(SourceJson.sha(evidence)),INPUT_CHANGED);
            var p=SourceJson.object();p.set("command",c);p.set("bundle",b);p.set("result",r);p.set("evidence",e);p.set("anchor",a);pack=p;
        }catch(PreflightFailure ex){var released=workers.failReliability(guard,owner,null,code(ex.code()),null);throw new DefinitiveFailure(code(ex.code()),released);}
        catch(ManagementFailure | WorkerFailure ex){throw ex;}
        catch(RuntimeException ex){var released=workers.failReliability(guard,owner,null,"INPUT_INVALID",null);throw new DefinitiveFailure("INPUT_INVALID",released);}
        var published=stage(guard,owner,runtime,pack,"PUBLISH_RELIABILITY",publisher);
        var verified=stage(guard,published.owner(),runtime,pack,"VERIFY_RESULT",verifier);
        var s=verified.stage;UUID artifact=workers.succeedReliability(guard,s,SourceJson.canonical(verified.evidence.get("publication")),verified.anchorSha);
        var w=s.owner().worker();return new Completed(artifact,new WorkerRepository.Session(w.installationId(),w.instanceId(),w.profileId(),w.profileDigest(),w.deploymentDigest(),w.hostDigest(),"READY",w.epoch(),w.coordinatorVersion()+1,w.workerVersion()));
    }
    private record Finished(WorkerRepository.ReliabilityStage stage,JsonNode evidence,String anchorSha) {WorkerRepository.Owner owner(){return stage.owner();}}
    private static final class Current {WorkerRepository.ReliabilityStage stage;}
    private Finished stage(InstallationHostGuard guard,WorkerRepository.Owner owner,ReliabilityRuntime runtime,JsonNode pack,String name,ReadOnlyVerifierProcess process) {
        runtime.admission();UUID checkpoint=UUID.randomUUID();var files=new PreflightFiles(guard,owner.runId(),owner.attemptId(),checkpoint,name);
        byte[] retained=SourceJson.canonical(pack);String fingerprint=pack.get("evidence").get("nativeDependencyFingerprint").stringValue();
        var command=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","ownerEpoch","itemOrdinal","capturedConfigSha256","runtime"))command.set(key,pack.get("command").get(key));
        command.put("stage",name).put("checkpointId",checkpoint.toString()).put("retainedSha256",SourceJson.sha(retained)).put("nativeFingerprint",fingerprint);
        byte[] raw=SourceJson.canonical(command);files.write("retained.json",retained,PreflightFiles.LARGE);files.write("command.json",raw,PreflightFiles.SMALL);
        var current=new Current();current.stage=workers.beginReliabilityStage(guard,owner,name,checkpoint,SourceJson.sha(raw),SourceJson.sha(retained),fingerprint);var once=new AtomicBoolean();
        try {
            Duration maximum=name.equals("PUBLISH_RELIABILITY")?Duration.ofMinutes(30):Duration.ofMinutes(15);
            Duration remaining=workers.remainingReliability(guard,current.stage.owner());Duration deadline=remaining.compareTo(maximum)<0?remaining:maximum;
            var exited=process.run(runtime.command(files.directory.resolve("command.json"),true),runtime.environment(),runtime.scratch(),deadline,new ReadOnlyVerifierProcess.Observer(){
                public void beforeStart(){need(once.compareAndSet(false,true),RESULT_INVALID);runtime.admission();need(Arrays.equals(raw,files.read("command.json",PreflightFiles.SMALL)) && Arrays.equals(retained,files.read("retained.json",PreflightFiles.LARGE)),INPUT_CHANGED);current.stage=workers.authorizeReliability(guard,current.stage);}
                public void started(InstallationHostGuard.ProcessIdentity child){current.stage=workers.recordReliabilityChild(guard,current.stage,child);}
                public void heartbeat(){runtime.admission();current.stage=workers.heartbeatReliability(guard,current.stage);}
            });
            runtime.admission();JsonNode result,evidence;byte[] response,qualified;
            try {
                response=files.read("result.json",PreflightFiles.SMALL);result=PreflightFiles.canonical(response,PreflightFiles.SMALL);
                qualified=files.read("evidence.json",PreflightFiles.LARGE);evidence=PreflightFiles.canonical(qualified,PreflightFiles.LARGE);
                validate(command,result,evidence,exited);
            }catch(RuntimeException ex){throw new PreflightFailure(RESULT_INVALID);}
            need(Arrays.equals(raw,files.read("command.json",PreflightFiles.SMALL)) && Arrays.equals(retained,files.read("retained.json",PreflightFiles.LARGE)) && Arrays.equals(response,files.read("result.json",PreflightFiles.SMALL)) && Arrays.equals(qualified,files.read("evidence.json",PreflightFiles.LARGE)),INPUT_CHANGED);
            var anchor=SourceJson.object().put("protocolVersion",1).put("checkpointId",checkpoint.toString()).put("commandSha256",SourceJson.sha(raw)).put("resultSha256",SourceJson.sha(response)).put("evidenceSha256",SourceJson.sha(qualified)).put("retainedSha256",SourceJson.sha(retained)).put("cleanup","OBSERVED_CLEAN_EXIT");
            byte[] validated=SourceJson.canonical(anchor);files.write("verified.json",validated,PreflightFiles.SMALL);String hash=SourceJson.sha(validated);
            if(result.get("status").stringValue().equals("FAILED")) {
                need(name.equals("PUBLISH_RELIABILITY") && Set.of("ABSENT","INCOMPLETE","NOT_AUTHORIZED").contains(evidence.get("outputState").stringValue()),RESULT_INVALID);
                String nativeCode=result.get("code").stringValue();
                String failure=evidence.get("outputState").stringValue().equals("INCOMPLETE")?"INCOMPLETE_OUTPUT":nativeCode.endsWith("_CHANGED")?"INPUT_CHANGED":nativeCode.endsWith("_UNAVAILABLE")?"INPUT_UNAVAILABLE":Set.of("RUNTIME_MISMATCH","RUNTIME_INCOMPATIBLE","UNSUPPORTED_VERSION").contains(nativeCode)?"RUNTIME_MISMATCH":nativeCode.equals("RESOURCE_LIMIT")?"RESOURCE_LIMIT":"INPUT_INVALID";
                var released=workers.failReliability(guard,current.stage.owner(),current.stage,failure,hash);
                throw new DefinitiveFailure(failure,released);
            }
            if(name.equals("PUBLISH_RELIABILITY"))current.stage=workers.publishedReliability(guard,current.stage,hash);
            return new Finished(current.stage,evidence,hash);
        }catch(PreflightFailure ex) {
            if(name.equals("PUBLISH_RELIABILITY") && ex.code()==PROCESS_START_FAILED && ex.quiescent()) {
                boolean interrupted=Thread.interrupted();
                try {
                var fail=SourceJson.canonical(SourceJson.object().put("code","PROCESS_START_FAILED"));files.write("verified.json",fail,PreflightFiles.SMALL);
                var released=workers.failReliability(guard,current.stage.owner(),current.stage,"PROCESS_START_FAILED",SourceJson.sha(fail));
                throw new DefinitiveFailure("PROCESS_START_FAILED",released);
                }finally{if(interrupted)Thread.currentThread().interrupt();}
            } else workers.blockReliability(guard,current.stage);
            throw ex;
        }
        // Unknown DB commits/ownership never refresh tokens, retry launch, release or overwrite evidence.
    }
    public static final class DefinitiveFailure extends RuntimeException {
        private final WorkerRepository.Session worker;
        private DefinitiveFailure(String code,WorkerRepository.Session worker){super(code,null,false,false);this.worker=worker;}
        /** Confirmed released token only; ambiguous commits never return this value. */
        public WorkerRepository.Session worker(){return worker;}
    }
    private static void validate(JsonNode command,JsonNode result,JsonNode evidence,ReadOnlyVerifierProcess.Result child) {
        SourceJson.fields(result,"protocolVersion","runId","attemptId","stage","itemOrdinal","status","code","evidenceId","counts");
        for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))need(result.get(key).equals(command.get(key)),RESULT_INVALID);
        for(String key:List.of("runId","attemptId","checkpointId","nativeFingerprint"))need(evidence.get(key).equals(command.get(key)),RESULT_INVALID);
        need(evidence.get("policy").stringValue().equals("E5_RELIABILITY_RESULT_V1"),RESULT_INVALID);
        String cleanup=evidence.get("cleanup").get("kind").stringValue();
        if(cleanup.equals("PYSPARK_GATEWAY_REAPED"))need(child.descendants().contains(evidence.get("cleanup").get("pid").stringValue()),TERMINATION_UNCONFIRMED);
        else need(Set.of("READ_ONLY","READ_ONLY_REPLAY","READ_ONLY_FAILURE").contains(cleanup) && child.descendants().isEmpty(),TERMINATION_UNCONFIRMED);
        if(result.get("status").stringValue().equals("FAILED")) {
            SourceJson.fields(evidence,"policy","runId","attemptId","checkpointId","nativeFingerprint","cleanup","outputState","failureCode");
            need(result.get("code").isString() && (ReliabilityPreflight.NATIVE_CODES.contains(result.get("code").stringValue()) || Set.of("EVENT_IDENTITY_CONFLICT","PUBLICATION_FAILED","PUBLICATION_INVALID","PUBLICATION_CHANGED","PUBLICATION_UNAVAILABLE","PUBLICATION_INCOMPLETE","TERMINATION_UNCONFIRMED","QUERY_FAILED").contains(result.get("code").stringValue())),RESULT_INVALID);
            need(!result.get("code").stringValue().equals("TERMINATION_UNCONFIRMED"),TERMINATION_UNCONFIRMED);
            need(child.exitCode()==1 && result.get("evidenceId").isNull() && evidence.get("failureCode").equals(result.get("code")) && result.get("counts").isEmpty(),RESULT_INVALID);return;
        }
        need(result.get("status").stringValue().equals("OK") && child.exitCode()==0 && result.get("code").isNull() && result.get("evidenceId").equals(command.get("checkpointId")),RESULT_INVALID);
        if(command.get("stage").stringValue().equals("PUBLISH_RELIABILITY")) {
            SourceJson.fields(evidence,"policy","runId","attemptId","checkpointId","nativeFingerprint","cleanup","publicationStatus","completionBase64");
            need(Set.of("published","replayed","recovered").contains(evidence.get("publicationStatus").stringValue()) && result.get("counts").isEmpty(),RESULT_INVALID);
            var marker=SourceJson.parse(Base64.getDecoder().decode(evidence.get("completionBase64").stringValue()),65536);need(marker.get("product_fingerprint").equals(command.get("nativeFingerprint")),RESULT_INVALID);
        } else {
            SourceJson.fields(evidence,"policy","runId","attemptId","checkpointId","nativeFingerprint","cleanup","publication");var p=evidence.get("publication");
            SourceJson.fields(p,"qualificationVersion","policy","productId","kind","contractVersion","processorRevision","inputFormat","nativeFingerprint","storageBindingId","relativeAddress","completionBase64","inputsBase64","files","counts");
            need(p.get("nativeFingerprint").equals(command.get("nativeFingerprint")) && p.get("productId").stringValue().equals("application-reliability") && p.get("kind").stringValue().equals("RELIABILITY_V2") && p.get("contractVersion").intValue()==2 && p.get("processorRevision").intValue()==2 && p.get("counts").equals(result.get("counts")),RESULT_INVALID);
            var counts=p.get("counts");SourceJson.fields(counts,"occurrences","accepted","rejected","events","windows");
            for(String key:List.of("occurrences","accepted","rejected","events","windows"))need(counts.get(key).isString() && counts.get(key).stringValue().matches("0|[1-9][0-9]{0,6}") && Long.parseLong(counts.get(key).stringValue())<=2000000,RESULT_INVALID);
            need(Long.parseLong(counts.get("occurrences").stringValue())==Long.parseLong(counts.get("accepted").stringValue())+Long.parseLong(counts.get("rejected").stringValue()) && Long.parseLong(counts.get("events").stringValue())<=Long.parseLong(counts.get("accepted").stringValue()) && Long.parseLong(counts.get("windows").stringValue())<=Long.parseLong(counts.get("events").stringValue()),RESULT_INVALID);
            need(SourceJson.sha(Base64.getDecoder().decode(p.get("inputsBase64").stringValue())).equals(command.get("nativeFingerprint").stringValue()),RESULT_INVALID);
            need(p.get("storageBindingId").equals(command.get("runtime").get("configuration").get("bindings").get("output")) && p.get("relativeAddress").stringValue().equals("v2/processor_revision=2/publication="+command.get("nativeFingerprint").stringValue()),RESULT_INVALID);
        }
    }
    private static String code(PreflightFailure.Code c){return Set.of(INPUT_INVALID,INPUT_CHANGED,INPUT_UNAVAILABLE,RUNTIME_MISMATCH,RESOURCE_LIMIT).contains(c)?c.name():"INPUT_INVALID";}
}
