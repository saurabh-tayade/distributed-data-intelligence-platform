package platform.contentapi.execution;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** Fixed read-only preflight; production closure returns only a transaction-confirmed session. */
public final class ReliabilityPreflight {
    public record Verified(WorkerRepository.CheckInputs stage,String evidenceSha256,String nativeDependencyFingerprint) { }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;private final ReadOnlyVerifierProcess process;
    public ReliabilityPreflight(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){this(jdbc,tx,workers,new ReadOnlyVerifierProcess());}
    ReliabilityPreflight(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,ReadOnlyVerifierProcess process){this.jdbc=jdbc;this.tx=tx;this.workers=workers;this.process=process;}
    public Verified verify(InstallationHostGuard guard,WorkerRepository.Owner owner,String runtimeId,UUID checkpoint) {
        return verify(guard,owner,runtimeId,checkpoint,false);
    }
    public Verified verifyForExecution(InstallationHostGuard guard,WorkerRepository.Owner owner,String runtimeId,UUID checkpoint) {
        return verify(guard,owner,runtimeId,checkpoint,true);
    }
    public static final class DefinitiveFailure extends RuntimeException {
        private final WorkerRepository.Session worker;
        private DefinitiveFailure(String code,WorkerRepository.Session worker){super(code,null,false,false);this.worker=worker;}
        public WorkerRepository.Session worker(){return worker;}
    }
    private Verified verify(InstallationHostGuard guard,WorkerRepository.Owner owner,String runtimeId,UUID checkpoint,boolean closeFailure) {
        need(checkpoint!=null,RUNTIME_MISMATCH);
        if(!workers.executionAvailable())throw new WorkerFailure(WorkerFailure.Code.WORKER_NOT_READY);
        var state=new Current(null);
        try {
        JsonNode frozen=workers.frozenReliability(guard,owner);
        ReliabilityRuntime runtime=ReliabilityRuntime.load(jdbc,tx,guard,owner,runtimeId,frozen);runtime.admission();
        var files=new PreflightFiles(guard,owner.runId(),owner.attemptId(),checkpoint);
        var bundle=SourceJson.object();bundle.set("capturedSelection",frozen);bundle.set("bindings",runtime.bindings());byte[] selected=SourceJson.canonical(bundle);
        String selectionSha=SourceJson.sha(SourceJson.canonical(frozen));
        var command=SourceJson.object().put("protocolVersion",1).put("runId",owner.runId().toString()).put("attemptId",owner.attemptId().toString()).put("ownerEpoch",Long.toString(owner.worker().epoch())).put("stage","CHECK_INPUTS").put("itemOrdinal",0).put("capturedConfigSha256",selectionSha).put("checkpointId",checkpoint.toString()).put("selection","selection.json").put("bundleSha256",SourceJson.sha(selected));command.set("runtime",runtime.json());
        byte[] request=SourceJson.canonical(command);files.write("selection.json",selected,PreflightFiles.LARGE);files.write("command.json",request,PreflightFiles.SMALL);
        state.stage=workers.beginCheckInputs(guard,owner,checkpoint,SourceJson.sha(request),selectionSha);
        var once=new AtomicBoolean();
        try {
            var result=process.run(runtime.command(files.directory.resolve("command.json")),runtime.environment(),runtime.scratch(),Duration.ofSeconds(runtime.seconds()),new ReadOnlyVerifierProcess.Observer(){
                public void beforeStart(){need(once.compareAndSet(false,true),RESULT_INVALID);runtime.admission();need(Arrays.equals(files.read("command.json",PreflightFiles.SMALL),request) && Arrays.equals(files.read("selection.json",PreflightFiles.LARGE),selected),INPUT_CHANGED);state.stage=workers.authorizeCheckInputs(guard,state.stage);}
                public void started(InstallationHostGuard.ProcessIdentity child){state.stage=workers.recordVerifierProcess(guard,state.stage,child);}
                public void heartbeat(){runtime.admission();state.stage=workers.heartbeatCheckInputs(guard,state.stage);workers.checkVerifierOwner(guard,state.stage);}
            });
            runtime.admission();guard.checkHeld(owner.worker().installationId());
            byte[] raw;JsonNode response;
            try{raw=files.read("result.json",PreflightFiles.SMALL);response=PreflightFiles.canonical(raw,PreflightFiles.SMALL);validateResult(command,response,result.exitCode());}
            catch(PreflightFailure ex){throw new PreflightFailure(RESULT_INVALID);}
            if(response.get("status").stringValue().equals("FAILED"))throw nativeFailure(response.get("code").stringValue());
            byte[] evidence=files.read("evidence.json",PreflightFiles.LARGE);var qualified=PreflightFiles.canonical(evidence,PreflightFiles.LARGE);
            validateEvidence(command,frozen,response,qualified);
            need(Arrays.equals(request,files.read("command.json",PreflightFiles.SMALL)) && Arrays.equals(selected,files.read("selection.json",PreflightFiles.LARGE)) && Arrays.equals(raw,files.read("result.json",PreflightFiles.SMALL)) && Arrays.equals(evidence,files.read("evidence.json",PreflightFiles.LARGE)),INPUT_CHANGED);
            var verified=SourceJson.object().put("protocolVersion",1).put("checkpointId",checkpoint.toString()).put("commandSha256",SourceJson.sha(request)).put("resultSha256",SourceJson.sha(raw)).put("evidenceSha256",SourceJson.sha(evidence)).put("selectionSha256",selectionSha).put("nativeDependencyFingerprint",qualified.get("nativeDependencyFingerprint").stringValue()).put("cleanup","OBSERVED_CLEAN_EXIT");
            byte[] anchor=SourceJson.canonical(verified);files.write("verified.json",anchor,PreflightFiles.SMALL);
            state.stage=workers.finishCheckInputs(guard,state.stage,true,"VERIFIED",SourceJson.sha(anchor),SourceJson.canonical(response.get("counts")));
            return new Verified(state.stage,SourceJson.sha(evidence),qualified.get("nativeDependencyFingerprint").stringValue());
        }catch(PreflightFailure ex) {
            if(ex.quiescent() && ex.code()!=RESULT_INVALID && ex.code()!=INTERRUPTED) {
                // Supervisor restored interruption after proving no child launch. Permit bounded journal/SQL
                // closure, then restore the signal; the persistent lifecycle stop gate still forbids launches.
                boolean interrupted=Thread.interrupted();
                try {
                var failure=SourceJson.object().put("protocolVersion",1).put("checkpointId",checkpoint.toString()).put("commandSha256",SourceJson.sha(request)).put("code",publicCode(ex.code()));
                byte[] anchor=SourceJson.canonical(failure);files.write("verified.json",anchor,PreflightFiles.SMALL);
                if(closeFailure)throw new DefinitiveFailure(publicCode(ex.code()),workers.failCheckInputs(guard,state.stage.owner(),state.stage,publicCode(ex.code()),SourceJson.sha(anchor)));
                workers.finishCheckInputs(guard,state.stage,false,publicCode(ex.code()),SourceJson.sha(anchor),SourceJson.canonical(SourceJson.object()));
                }finally{if(interrupted)Thread.currentThread().interrupt();}
            } else workers.blockCheckInputs(guard,state.stage);
            throw ex;
        }
        }catch(PreflightFailure ex) {
            // Before INTENT no child could have been authorized. Never apply this to unknown exceptions.
            if(closeFailure && state.stage==null && ex.quiescent() && ex.code()!=INTERRUPTED && ex.code()!=RESULT_INVALID) {
                boolean interrupted=Thread.interrupted();
                try{throw new DefinitiveFailure(publicCode(ex.code()),workers.failCheckInputs(guard,owner,null,publicCode(ex.code()),null));}
                finally{if(interrupted)Thread.currentThread().interrupt();}
            }
            throw ex;
        }
        // Database/ownership failures deliberately escape: never release or refresh/adopt a token.
    }
    private static final class Current {WorkerRepository.CheckInputs stage;Current(WorkerRepository.CheckInputs stage){this.stage=stage;}}
    static void validateResult(JsonNode command,JsonNode result,int exit) {
        try {
            SourceJson.fields(result,"protocolVersion","runId","attemptId","stage","itemOrdinal","status","code","evidenceId","counts");
            need(result.get("protocolVersion").isIntegralNumber() && result.get("protocolVersion").intValue()==1 && result.get("itemOrdinal").isIntegralNumber() && result.get("itemOrdinal").intValue()==0,RESULT_INVALID);
            for(String key:List.of("runId","attemptId","stage"))need(result.get(key).equals(command.get(key)),RESULT_INVALID);
            if(result.get("status").stringValue().equals("OK"))need(exit==0 && result.get("code").isNull() && result.get("evidenceId").equals(command.get("checkpointId")),RESULT_INVALID);
            else need(exit==1 && result.get("status").stringValue().equals("FAILED") && result.get("evidenceId").isNull() && result.get("counts").isObject() && result.get("counts").isEmpty() && result.get("code").isString() && NATIVE_CODES.contains(result.get("code").stringValue()),RESULT_INVALID);
        }catch(PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(RESULT_INVALID);}
    }
    static void validateEvidence(JsonNode command,JsonNode frozen,JsonNode result,JsonNode evidence) {
        try {
            SourceJson.fields(evidence,"evidenceVersion","policy","runId","attemptId","ownerEpoch","checkpointId","capturedConfigSha256","runtimeProfileId","runtimeProfileSha256","deploymentSha256","runtime","inputFormat","nativeDependencyFingerprint","nativeDependencies","artifacts","counts");
            need(evidence.get("evidenceVersion").isIntegralNumber() && evidence.get("evidenceVersion").intValue()==1 && evidence.get("policy").stringValue().equals("E5_RELIABILITY_PREFLIGHT_V1"),RESULT_INVALID);
            for(String key:List.of("runId","attemptId","ownerEpoch","checkpointId","capturedConfigSha256"))need(evidence.get(key).equals(command.get(key)),RESULT_INVALID);
            need(evidence.get("runtimeProfileId").equals(command.get("runtime").get("profileId")) && evidence.get("runtimeProfileSha256").equals(command.get("runtime").get("profileSha256")) && evidence.get("deploymentSha256").equals(command.get("runtime").get("configuration").get("deploymentSha256")),RESULT_INVALID);
            need(evidence.get("runtime").equals(SourceJson.parse("{\"duckdb\":\"1.4.5\",\"java\":\"21\",\"pyspark\":\"4.0.2\",\"python\":\"3.12.15\"}".getBytes(java.nio.charset.StandardCharsets.UTF_8))),RESULT_INVALID);
            String format=frozen.get("executionProfile").get("configuration").get("sourceProfile").stringValue().equals("request-jsonl-v2")?"JSONL":"PARQUET";
            var nativeInputs=evidence.get("nativeDependencies");need(evidence.get("inputFormat").stringValue().equals(format) && nativeInputs.get("input_format").stringValue().equals(format) && SourceJson.sha(SourceJson.canonical(nativeInputs)).equals(evidence.get("nativeDependencyFingerprint").stringValue()),RESULT_INVALID);
            Map<String,JsonNode> selected=new TreeMap<>();for(var input:frozen.get("inputs"))selected.put(input.get("artifactId").stringValue(),input);
            need(evidence.get("artifacts").isArray() && evidence.get("artifacts").size()==selected.size(),RESULT_INVALID);int i=0;long files=0,bytes=0,rows=0;
            for(var entry:selected.entrySet()) {
                var actual=evidence.get("artifacts").get(i++);SourceJson.fields(actual,"artifactId","registeredEvidenceSha256","capture");var expected=entry.getValue();
                need(actual.get("artifactId").stringValue().equals(entry.getKey()) && actual.get("registeredEvidenceSha256").equals(expected.get("evidenceSha256")) && actual.get("capture").equals(expected.get("qualification").get("verification").get("capture")),RESULT_INVALID);
                if(expected.get("role").stringValue().equals("RELIABILITY_BATCH"))for(var f:expected.get("qualification").get("verification").get("native").get("inventory")){files++;bytes=Math.addExact(bytes,f.get("byte_length").longValue());rows=Math.addExact(rows,f.get("record_count").longValue());}
            }
            need(files<=128 && bytes<=1024L*1024*1024 && rows<=2000000 && evidence.get("counts").equals(result.get("counts")),RESULT_INVALID);
            var counts=SourceJson.object().put("files",Long.toString(files)).put("bytes",Long.toString(bytes)).put("rows",Long.toString(rows));need(counts.equals(result.get("counts")),RESULT_INVALID);
        }catch(PreflightFailure ex){throw ex;}catch(RuntimeException ex){throw new PreflightFailure(RESULT_INVALID);}
    }
    static final Set<String> NATIVE_CODES=Set.of("ARGUMENT_INVALID","PATH_UNSAFE","UNSUPPORTED_VERSION","INTAKE_NOT_APPROVED","INPUT_UNAVAILABLE","INPUT_CHANGED","BATCH_UNAVAILABLE","BATCH_INCOMPLETE","BATCH_INVALID","BATCH_CONFLICT","CSV_INVALID","VOCABULARY_INVALID","SNAPSHOT_UNAVAILABLE","SNAPSHOT_INVALID","SNAPSHOT_CHANGED","SNAPSHOT_INCOMPLETE","IDENTITY_MISMATCH","DEPENDENCY_UNAVAILABLE","DEPENDENCY_INVALID","DEPENDENCY_CHANGED","PARQUET_INVALID","PROVENANCE_UNSUPPORTED","RESOURCE_LIMIT","EVIDENCE_UNAVAILABLE","EVIDENCE_INVALID","EVIDENCE_CHANGED","RUNTIME_INCOMPATIBLE","RUNTIME_MISMATCH","ADAPTER_INVALID","INPUT_INVALID");
    private static PreflightFailure nativeFailure(String code){return new PreflightFailure(code.equals("RESOURCE_LIMIT")?RESOURCE_LIMIT:Set.of("RUNTIME_MISMATCH","RUNTIME_INCOMPATIBLE","UNSUPPORTED_VERSION").contains(code)?RUNTIME_MISMATCH:code.endsWith("_CHANGED")?INPUT_CHANGED:code.endsWith("_UNAVAILABLE")?INPUT_UNAVAILABLE:INPUT_INVALID);}
    private static String publicCode(PreflightFailure.Code code){return switch(code){case RESULT_INVALID->"INPUT_INVALID";case INTERRUPTED,TERMINATION_UNCONFIRMED->"RECOVERY_REQUIRED";default->code.name();};}
}
