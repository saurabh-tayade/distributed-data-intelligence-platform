package platform.contentapi.execution;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** D2 internal checkpoint. No Content dispatch, retry framework, fetch or downstream execution. */
final class RssBronzeExecution {
    enum Boundary { INTENT, BEFORE_AUTHORIZATION, AFTER_CHILD, VERIFIED, BEFORE_CHECKPOINT, ACKNOWLEDGED }
    interface Fault {void at(Boundary point,WorkerRepository.BronzeStage stage);}
    record Stored(WorkerRepository.BronzeStage stage,byte[] receipt) {Stored{receipt=receipt.clone();}@Override public byte[] receipt(){return receipt.clone();}@Override public String toString(){return "StoredBronze[redacted]";}}
    record Inspection(String state,boolean receiptConfirmed,boolean retryClassification,byte[] receipt) {Inspection{receipt=receipt==null?null:receipt.clone();}@Override public byte[] receipt(){return receipt==null?null:receipt.clone();}@Override public String toString(){return "BronzeInspection["+state+"]";}}
    static final class StorageFailure extends RuntimeException {StorageFailure(String code){super(code,null,false,false);}}
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;private final Fault fault;
    RssBronzeExecution(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){this(jdbc,tx,workers,(p,s)->{});}
    RssBronzeExecution(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,Fault fault){this.jdbc=jdbc;this.tx=tx;this.workers=workers;this.fault=fault;}
    private static final class Current {WorkerRepository.BronzeStage stage;}
    Stored store(InstallationHostGuard guard,WorkerRepository.Owner owner,String source,String runtimeId) {
        workers.frozenContent(guard,owner);
        var context=RssBronzeContext.load(jdbc,tx,guard,owner.runId(),source,runtimeId);context.check();
        need(context.state.equals("FETCH_JOURNALED"),INPUT_INVALID);
        UUID checkpoint=UUID.randomUUID();byte[] command=command(context,checkpoint,owner.worker().epoch(),true);
        var files=new PreflightFiles(guard,context.run,context.attempt,checkpoint,"STORE_BRONZE");
        var prepared=PreparedCommand.prepare(guard,files.directory,command,65536);
        var current=new Current();current.stage=workers.beginBronze(guard,owner,context,checkpoint,SourceJson.sha(command),prepared);fault.at(Boundary.INTENT,current.stage);
        try {
            var child=new ReadOnlyVerifierProcess().run(context.runtime.bronzeCommand(files.directory.resolve("command.json")),context.runtime.environment(),files.directory,Duration.ofSeconds(180),new ReadOnlyVerifierProcess.Observer(){
                public void beforeStart(){fault.at(Boundary.BEFORE_AUTHORIZATION,current.stage);context.check();need(Arrays.equals(command,files.read("command.json",65536)),INPUT_CHANGED);current.stage=workers.authorizeBronze(guard,current.stage);}
                public void started(InstallationHostGuard.ProcessIdentity child){current.stage=workers.recordBronzeChild(guard,current.stage,child);}
                public void heartbeat(){context.check();current.stage=workers.heartbeatBronze(guard,current.stage);}
            });
            fault.at(Boundary.AFTER_CHILD,current.stage);workers.checkBronze(guard,current.stage);context.check();
            var result=validate(context,files,command,child);String state=result.get("state").stringValue();
            byte[] evidence=files.read("evidence.json",65536),machine=files.read("result.json",65536);
            var anchor=SourceJson.object().put("protocolVersion",1).put("checkpointId",checkpoint.toString()).put("commandSha256",SourceJson.sha(command)).put("evidenceSha256",SourceJson.sha(evidence)).put("resultSha256",SourceJson.sha(machine)).put("cleanup","OBSERVED_CLEAN_EXIT");
            anchor.put("childPid",Long.toString(child.child().pid())).put("childStartedAt",child.child().startedAt().toString());
            byte[] verified=SourceJson.canonical(anchor);files.write("verified.json",verified,65536);
            if(!state.equals("EXACT")){workers.blockBronze(guard,current.stage,Set.of("CONFLICT","METADATA_ONLY").contains(state));throw new StorageFailure("BRONZE_"+state);}
            fault.at(Boundary.VERIFIED,current.stage);workers.checkBronze(guard,current.stage);context.check();
            byte[] receipt=SourceJson.canonical(result.get("receipt"));
            fault.at(Boundary.BEFORE_CHECKPOINT,current.stage);context.check();
            need(Arrays.equals(command,files.read("command.json",65536)) && Arrays.equals(machine,files.read("result.json",65536)) && Arrays.equals(evidence,files.read("evidence.json",65536)) && Arrays.equals(verified,files.read("verified.json",65536)),INPUT_CHANGED);
            workers.checkpointBronze(guard,current.stage,receipt,SourceJson.sha(verified));fault.at(Boundary.ACKNOWLEDGED,current.stage);return new Stored(current.stage,receipt);
        }catch(ManagementFailure | WorkerFailure | StorageFailure ex){throw ex;}
        catch(PreflightFailure ex){workers.blockBronze(guard,current.stage,false);throw ex;}
    }
    /** Independent GET-only replay/inspection. No Owner token, receipt writes, adoption or launch retry. */
    Inspection inspect(InstallationHostGuard guard,UUID run,String source,String runtimeId) {
        var context=RssBronzeContext.load(jdbc,tx,guard,run,source,runtimeId);context.check();UUID checkpoint=UUID.randomUUID();
        var files=new PreflightFiles(guard,run,context.attempt,checkpoint,"INSPECT_BRONZE");byte[] command=command(context,checkpoint,SourceJson.parse(context.descriptor).get("ownerEpoch").longValue(),false);files.write("command.json",command,65536);
        var child=new ReadOnlyVerifierProcess().run(context.runtime.bronzeCommand(files.directory.resolve("command.json")),context.runtime.environment(),files.directory,Duration.ofSeconds(180),new ReadOnlyVerifierProcess.Observer(){
            public void beforeStart(){context.check();} public void started(InstallationHostGuard.ProcessIdentity child){} public void heartbeat(){context.check();}
        });
        context.check();var result=validate(context,files,command,child);String state=result.get("state").stringValue();
        var anchor=SourceJson.object().put("commandSha256",SourceJson.sha(command)).put("evidenceSha256",SourceJson.sha(files.read("evidence.json",65536))).put("resultSha256",SourceJson.sha(files.read("result.json",65536))).put("childPid",Long.toString(child.child().pid())).put("childStartedAt",child.child().startedAt().toString()).put("cleanup","OBSERVED_CLEAN_EXIT");files.write("verified.json",SourceJson.canonical(anchor),65536);
        boolean confirmed=false;byte[] receipt=state.equals("EXACT")?SourceJson.canonical(result.get("receipt")):null;
        if(context.state.equals("BRONZE_VERIFIED")){need(state.equals("EXACT") && context.receipt!=null && Arrays.equals(receipt,context.receipt),INPUT_CHANGED);verifyCommittedStage(guard,context);confirmed=true;}
        return new Inspection(state,confirmed,Set.of("ABSENT","PAYLOAD_ONLY").contains(state),receipt);
    }
    private void verifyCommittedStage(InstallationHostGuard guard,RssBronzeContext context) {
        var rows=tx.required(()->jdbc.queryForList("SELECT checkpoint_id,intended_namespace,command_sha256,result_sha256 FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='STORE_BRONZE' AND item_ordinal=? AND state='VERIFIED' AND intended_identity=?",context.attempt,context.ordinal,SourceJson.parse(context.descriptor).get("ingestionId").stringValue()));need(rows.size()==1,INPUT_INVALID);var row=rows.getFirst();
        String expected="runs/"+context.run+"/attempts/"+context.attempt+"/store-bronze/"+row.get("checkpoint_id");need(expected.equals(row.get("intended_namespace")),INPUT_INVALID);Path directory=guard.binding().root().resolve(expected);PreflightFiles.privateEntry(directory,true);
        byte[] anchor=PreflightFiles.read(directory.resolve("verified.json"),65536,true);need(SourceJson.sha(anchor).equals(row.get("result_sha256")),INPUT_CHANGED);var a=PreflightFiles.canonical(anchor,65536);
        byte[] command=PreflightFiles.read(directory.resolve("command.json"),65536,true),result=PreflightFiles.read(directory.resolve("result.json"),65536,true),evidence=PreflightFiles.read(directory.resolve("evidence.json"),65536,true);
        need(a.get("cleanup").stringValue().equals("OBSERVED_CLEAN_EXIT") && a.get("checkpointId").stringValue().equals(row.get("checkpoint_id").toString()) && SourceJson.sha(command).equals(row.get("command_sha256")) && SourceJson.sha(command).equals(a.get("commandSha256").stringValue()) && SourceJson.sha(result).equals(a.get("resultSha256").stringValue()) && SourceJson.sha(evidence).equals(a.get("evidenceSha256").stringValue()),INPUT_CHANGED);
        var e=SourceJson.parse(evidence);need(e.get("state").stringValue().equals("EXACT") && Arrays.equals(SourceJson.canonical(e.get("receipt")),context.receipt),INPUT_CHANGED);
    }
    static byte[] command(RssBronzeContext c,UUID checkpoint,long epoch,boolean publish){var n=SourceJson.object().put("protocolVersion",1).put("runId",c.run.toString()).put("attemptId",c.attempt.toString()).put("ownerEpoch",epoch).put("stage",publish?"STORE_BRONZE":"INSPECT_BRONZE").put("itemOrdinal",c.ordinal).put("checkpointId",checkpoint.toString()).put("mode",publish?"PUBLISH":"INSPECT").put("runtimeSha256",c.runtimeSha);n.set("journal",c.journalCommand());n.set("storage",c.storageCommand());byte[] raw=SourceJson.canonical(n);need(raw.length<=65536,RESOURCE_LIMIT);return raw;}
    private static JsonNode validate(RssBronzeContext context,PreflightFiles files,byte[] command,ReadOnlyVerifierProcess.Result child) {
        try {
            need(child.descendants().isEmpty() && Arrays.equals(command,files.read("command.json",65536)),RESULT_INVALID);
            Set<String> names=new HashSet<>();try(var list=Files.list(files.directory)){list.forEach(p->names.add(p.getFileName().toString()));}need(names.equals(Set.of("command.json","result.json","evidence.json")),RESULT_INVALID);
            var c=SourceJson.parse(command);var r=SourceJson.parse(files.read("result.json",65536));SourceJson.fields(r,"protocolVersion","runId","attemptId","stage","itemOrdinal","status","code","evidenceId","counts");
            for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))need(r.get(key).equals(c.get(key)),RESULT_INVALID);
            var e=SourceJson.parse(files.read("evidence.json",65536));SourceJson.fields(e,"bronzeEvidenceVersion","commandSha256","state","receipt");need(e.get("bronzeEvidenceVersion").isIntegralNumber() && e.get("bronzeEvidenceVersion").canConvertToInt() && e.get("bronzeEvidenceVersion").intValue()==1 && e.get("commandSha256").stringValue().equals(SourceJson.sha(command)),RESULT_INVALID);
            String state=e.get("state").stringValue();need(Set.of("EXACT","ABSENT","PAYLOAD_ONLY","METADATA_ONLY","CONFLICT","UNAVAILABLE").contains(state) && r.get("counts").isObject() && r.get("counts").isEmpty(),RESULT_INVALID);
            if(state.equals("EXACT")){need(child.exitCode()==0 && r.get("status").stringValue().equals("OK") && r.get("code").isNull() && r.get("evidenceId").equals(c.get("checkpointId")),RESULT_INVALID);need(Arrays.equals(SourceJson.canonical(expectedReceipt(context,e.get("receipt").get("payloadContentType").stringValue())),SourceJson.canonical(e.get("receipt"))),RESULT_INVALID);}
            else need(child.exitCode()==1 && r.get("status").stringValue().equals("FAILED") && r.get("code").stringValue().equals("BRONZE_"+state) && r.get("evidenceId").isNull() && e.get("receipt").isNull(),RESULT_INVALID);
            return e;
        }catch(PreflightFailure ex){throw ex;}catch(Exception ex){throw new PreflightFailure(RESULT_INVALID);}
    }
    static JsonNode expectedReceipt(RssBronzeContext c,String type){need(Set.of("application/octet-stream","application/xml").contains(type),RESULT_INVALID);var d=SourceJson.parse(c.descriptor);var n=SourceJson.object().put("receiptVersion",1);for(String key:List.of("runId","attemptId","sourceId","revision","capturedConfigSha256","ingestionId","ingestedAt","payloadByteLength","payloadSha256"))n.set(key,d.get(key));n.put("journalSha256",c.journalSha).put("storageBindingId",c.storage.get("bindingId").stringValue()).put("storageBindingSha256",c.storage.get("configurationSha256").stringValue()).put("bucket",c.storage.get("configuration").get("bucket").stringValue());n.set("expected",d.get("expected"));n.put("payloadContentType",type).put("metadataContentType","application/json");return n;}
}
