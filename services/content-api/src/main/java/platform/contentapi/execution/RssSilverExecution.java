package platform.contentapi.execution;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import platform.contentapi.importing.RegisteredImportEvidence;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.PreflightFailure.Code.*;
import static platform.contentapi.execution.PreflightFailure.need;

/** D3 internal seam only. No production worker bean, automatic recovery or downstream dispatch. */
final class RssSilverExecution {
    enum Boundary { INTENT, BEFORE_AUTHORIZATION, AUTHORIZED, CHILD_STARTED, HEARTBEAT, AFTER_CHILD, VERIFIED, BEFORE_CHECKPOINT, ACKNOWLEDGED }
    interface Fault {void at(Boundary point,WorkerRepository.SilverStage stage);}
    record Stored(WorkerRepository.Owner owner,UUID artifact,byte[] evidence){Stored{evidence=evidence.clone();}@Override public byte[] evidence(){return evidence.clone();}}
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;private final Fault fault;
    RssSilverExecution(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){this(jdbc,tx,workers,(p,s)->{});}
    RssSilverExecution(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,Fault fault){this.jdbc=jdbc;this.tx=tx;this.workers=workers;this.fault=fault;}
    private record Context(RssBronzeContext bronze,RssSilverRuntime runtime,JsonNode artifact){void check(){bronze.check();runtime.check();}}
    private Context context(InstallationHostGuard guard,UUID run,String source,String runtimeId){
        var rows=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",runtimeId));need(rows.size()==1,RUNTIME_MISMATCH);
        var config=FrozenRunSelection.canonical((byte[])rows.getFirst().get("configuration_bytes"),(String)rows.getFirst().get("configuration_sha256"),65536);
        String bronzeId=config.get("bronzeRuntimeId").stringValue();
        var bronze=RssBronzeContext.load(jdbc,tx,guard,run,source,bronzeId);need(bronze.state.equals("BRONZE_VERIFIED") && bronze.receipt!=null,INPUT_UNAVAILABLE);
        need(new RssBronzeExecution(jdbc,tx,workers).inspect(guard,run,source,bronzeId).receiptConfirmed(),INPUT_INVALID);
        var runtime=RssSilverRuntime.load(jdbc,tx,guard,bronze,runtimeId);var r=SourceJson.parse(bronze.receipt);var expected=r.get("expected");
        String key=expected.get("metadataKey").stringValue();String address="v1/"+SourceJson.sha(r.get("bucket").stringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8))+"/"+key.substring("bronze/rss/".length(),key.length()-"metadata.json".length())+"complete.json";
        var a=SourceJson.object().put("kind","SILVER").put("productId","content-intelligence").put("storageBindingId",runtime.config.get("outputBindingId").stringValue()).put("relativeAddress",address).put("contractVersion",1).put("processorRevision",1);
        var i=a.putObject("identity");for(String name:List.of("sourceId","ingestionId"))i.set(name,r.get(name));i.set("bronzeStorageBindingId",r.get("storageBindingId"));i.set("bronzeBucket",r.get("bucket"));i.set("bronzeMetadataKey",expected.get("metadataKey"));i.set("bronzePayloadKey",expected.get("payloadKey"));
        return new Context(bronze,runtime,a);
    }
    private static final class Current {WorkerRepository.SilverStage stage;}
    Stored execute(InstallationHostGuard guard,WorkerRepository.Owner owner,String source,String runtimeId){
        workers.frozenContent(guard,owner);var c=context(guard,owner.runId(),source,runtimeId);c.check();UUID checkpoint=UUID.randomUUID();
        byte[] command=command(c,checkpoint,owner.worker().epoch(),"PUBLISH");var current=new Current();current.stage=workers.beginSilver(guard,owner,c.bronze,checkpoint,SourceJson.sha(command),SourceJson.sha(SourceJson.canonical(c.artifact)));
        fault.at(Boundary.INTENT,current.stage);
        try {
            var files=new PreflightFiles(guard,owner.runId(),owner.attemptId(),checkpoint,"SILVER");files.write("command.json",command,65536);
            var writer=new ReadOnlyVerifierProcess(true).run(c.runtime.command(files.directory.resolve("command.json")),c.runtime.environment(),files.directory,Duration.ofSeconds(c.runtime.config.get("seconds").intValue()),new ReadOnlyVerifierProcess.Observer(){
                public void beforeStart(){fault.at(Boundary.BEFORE_AUTHORIZATION,current.stage);c.check();need(Arrays.equals(command,files.read("command.json",65536)),INPUT_CHANGED);current.stage=workers.authorizeSilver(guard,current.stage);fault.at(Boundary.AUTHORIZED,current.stage);}
                public void started(InstallationHostGuard.ProcessIdentity child){recordChild(files,child);current.stage=workers.recordSilverChild(guard,current.stage,child);fault.at(Boundary.CHILD_STARTED,current.stage);}
                public void heartbeat(){fault.at(Boundary.HEARTBEAT,current.stage);c.check();current.stage=workers.heartbeatSilver(guard,current.stage);}
            });
            fault.at(Boundary.AFTER_CHILD,current.stage);workers.checkSilver(guard,current.stage);c.check();
            var response=response(files,command,writer);String status=response.get("status").stringValue();need(Set.of("PROCESSED","EXISTING").contains(status),RESULT_INVALID);
            if(status.equals("PROCESSED")){var cleanup=response.get("cleanup");SourceJson.fields(cleanup,"kind","pid");need(cleanup.get("kind").stringValue().equals("PYSPARK_GATEWAY_REAPED") && cleanup.get("pid").stringValue().matches("[1-9][0-9]{0,18}"),TERMINATION_UNCONFIRMED);}
            byte[] evidence=verify(guard,c,checkpoint,current.stage.owner().worker().epoch(),()->{c.check();workers.checkSilver(guard,current.stage);},()->{c.check();current.stage=workers.heartbeatSilver(guard,current.stage);});
            fault.at(Boundary.VERIFIED,current.stage);
            Path verification=guard.binding().root().resolve("runs/"+owner.runId()+"/attempts/"+owner.attemptId()+"/verify-silver/"+checkpoint);
            var nativeSelection=PreflightFiles.canonical(PreflightFiles.read(verification.resolve("selection.json"),512*1024,true),512*1024);
            var anchor=SourceJson.object().put("commandSha256",SourceJson.sha(command)).put("resultSha256",SourceJson.sha(files.read("result.json",65536))).put("receiptSha256",SourceJson.sha(c.bronze.receipt)).put("artifactEvidenceSha256",SourceJson.sha(evidence)).put("runtimeSha256",c.runtime.digest).put("verificationSha256",SourceJson.sha(verificationAnchor(verification))).put("nativeInputSetFingerprint",nativeSelection.get("inputSetFingerprint").stringValue()).put("childPid",Long.toString(writer.child().pid())).put("childStartedAt",writer.child().startedAt().toString()).put("cleanup","OBSERVED_CLEAN_EXIT");
            byte[] verified=SourceJson.canonical(anchor);files.write("verified.json",verified,65536);
            fault.at(Boundary.BEFORE_CHECKPOINT,current.stage);workers.checkSilver(guard,current.stage);c.check();
            // Last bounded read-only recheck follows fault injection, immediately before the short commit.
            recheck(guard,c,checkpoint,current.stage.owner().worker().epoch(),evidence,()->workers.checkSilver(guard,current.stage));
            need(Arrays.equals(command,files.read("command.json",65536)) && Arrays.equals(verified,files.read("verified.json",65536)),INPUT_CHANGED);
            need(SourceJson.sha(verificationAnchor(verification)).equals(anchor.get("verificationSha256").stringValue()) && SourceJson.sha(files.read("result.json",65536)).equals(anchor.get("resultSha256").stringValue()),INPUT_CHANGED);
            UUID artifact=workers.checkpointSilver(guard,current.stage,evidence,SourceJson.sha(verified));fault.at(Boundary.ACKNOWLEDGED,current.stage);return new Stored(current.stage.owner(),artifact,evidence);
        }catch(PreflightFailure ex){workers.blockSilver(guard,current.stage);throw ex;}
    }
    /** Full read-only qualification and committed linkage lookup after an uncertain acknowledgment.
     * No Owner adoption, stage mutation or Spark writer authorization. */
    UUID inspect(InstallationHostGuard guard,UUID run,String source,String runtimeId){
        var c=context(guard,run,source,runtimeId);var original=committed(guard,c);
        byte[] evidence=verify(guard,c,UUID.randomUUID(),SourceJson.parse(c.bronze.descriptor).get("ownerEpoch").longValue(),c::check,c::check);
        need(Arrays.equals(evidence,SourceJson.canonical(original.get("qualification"))),INPUT_CHANGED);return UUID.fromString(original.get("artifactId").stringValue());
    }
    /** Original D3 anchor/receipt proof only. Downstream MUST independently qualify the selected files. */
    JsonNode committed(InstallationHostGuard guard,UUID run,String source,String runtimeId){return committed(guard,context(guard,run,source,runtimeId));}
    private JsonNode committed(InstallationHostGuard guard,Context c){
        UUID run=c.bronze.run;
        var rows=tx.required(()->jdbc.queryForList("SELECT s.*,a.artifact_id,b.evidence_bytes,b.evidence_sha256,b.native_identity,b.storage_binding_id,b.relative_address,b.kind,b.product_id,b.contract_version,b.processor_revision FROM ddip_management.stage_invocations s JOIN ddip_management.stage_artifacts a USING(attempt_id,stage,item_ordinal) JOIN ddip_management.artifacts b USING(artifact_id) WHERE s.attempt_id=? AND s.stage='SILVER' AND s.item_ordinal=? AND s.state='VERIFIED' AND a.role='RESULT' AND a.ordinal=0",c.bronze.attempt,c.bronze.ordinal));
        need(rows.size()==1,INPUT_UNAVAILABLE);var row=rows.getFirst();String namespace=(String)row.get("intended_namespace");
        need(namespace.equals("runs/"+run+"/attempts/"+c.bronze.attempt+"/silver/"+row.get("checkpoint_id")),INPUT_INVALID);
        Path dir=guard.binding().root().resolve(namespace);byte[] anchor=PreflightFiles.read(dir.resolve("verified.json"),65536,true);need(SourceJson.sha(anchor).equals(row.get("result_sha256")),INPUT_CHANGED);var a=PreflightFiles.canonical(anchor,65536);
        byte[] command=PreflightFiles.read(dir.resolve("command.json"),65536,true),result=PreflightFiles.read(dir.resolve("result.json"),65536,true);
        need(SourceJson.sha(command).equals(row.get("command_sha256")) && SourceJson.sha(command).equals(a.get("commandSha256").stringValue()) && SourceJson.sha(result).equals(a.get("resultSha256").stringValue()) && SourceJson.sha(c.bronze.receipt).equals(a.get("receiptSha256").stringValue()) && c.runtime.digest.equals(a.get("runtimeSha256").stringValue()) && SourceJson.sha(SourceJson.canonical(c.artifact)).equals(row.get("intended_identity")),INPUT_CHANGED);
        Path verification=dir.getParent().getParent().resolve("verify-silver").resolve(row.get("checkpoint_id").toString());
        byte[] verificationAnchor=verificationAnchor(verification);need(SourceJson.sha(verificationAnchor).equals(a.get("verificationSha256").stringValue()),INPUT_CHANGED);
        var v=PreflightFiles.canonical(verificationAnchor,65536);byte[] nativeSelection=PreflightFiles.read(verification.resolve("selection.json"),512*1024,true);
        need(SourceJson.sha(nativeSelection).equals(v.get("selectionSha256").stringValue()) && SourceJson.parse(nativeSelection).get("inputSetFingerprint").equals(a.get("nativeInputSetFingerprint")),INPUT_CHANGED);
        byte[] evidence=(byte[])row.get("evidence_bytes");var qualified=RegisteredImportEvidence.validate(evidence,(String)row.get("evidence_sha256"));
        need(SourceJson.sha(evidence).equals(a.get("artifactEvidenceSha256").stringValue()) && qualified.evidence().get("selection").equals(c.artifact) && qualified.evidence().get("verification").get("inventoryIdentity").get("sha256").stringValue().equals(row.get("native_identity")) && qualified.selection().bindingId().equals(row.get("storage_binding_id")) && qualified.selection().address().equals(row.get("relative_address")) && "SILVER".equals(row.get("kind")) && "content-intelligence".equals(row.get("product_id")) && ((Number)row.get("contract_version")).intValue()==1 && ((Number)row.get("processor_revision")).intValue()==1,INPUT_CHANGED);
        var selected=SourceJson.object().put("artifactId",row.get("artifact_id").toString()).put("originRunId",run.toString()).put("sourceId",c.bronze.source).put("evidenceSha256",SourceJson.sha(evidence));selected.set("qualification",qualified.evidence());selected.set("receipt",SourceJson.parse(c.bronze.receipt));selected.set("bronze",c.bronze.storage.get("configuration"));selected.put("credentialsSha256",SourceJson.sha(c.bronze.credentials));return selected;
    }
    private byte[] verify(InstallationHostGuard guard,Context c,UUID checkpoint,long epoch,Runnable before,Runnable heartbeat){
        var files=new PreflightFiles(guard,c.bronze.run,c.bronze.attempt,checkpoint,"VERIFY_SILVER");byte[] command=command(c,checkpoint,epoch,"VERIFY");files.write("command.json",command,65536);
        var child=run(c,files,before,heartbeat,true);need(response(files,command,child).get("status").stringValue().equals("VERIFIED"),RESULT_INVALID);
        byte[] nativeEvidence=files.read("evidence.json",512*1024);var evidence=SourceJson.object().put("qualificationVersion",1);evidence.set("selection",c.artifact);evidence.set("verification",PreflightFiles.canonical(nativeEvidence,512*1024));byte[] raw=SourceJson.canonical(evidence);
        RegisteredImportEvidence.validate(raw,SourceJson.sha(raw));
        byte[] selection=files.read("selection.json",512*1024);var selected=PreflightFiles.canonical(selection,512*1024);SourceJson.fields(selected,"manifest","inputSetFingerprint");need(SourceJson.sha(SourceJson.canonical(selected.get("manifest"))).equals(selected.get("inputSetFingerprint").stringValue()) && selected.get("manifest").get("inputs").size()==1 && selected.get("manifest").get("inputs").get(0).get("marker_path").equals(c.artifact.get("relativeAddress")),RESULT_INVALID);
        var anchor=SourceJson.object().put("commandSha256",SourceJson.sha(command)).put("evidenceSha256",SourceJson.sha(nativeEvidence)).put("selectionSha256",SourceJson.sha(selection)).put("resultSha256",SourceJson.sha(files.read("result.json",65536))).put("childPid",Long.toString(child.child().pid())).put("childStartedAt",child.child().startedAt().toString()).put("childInstanceId",child.child().instanceId().toString()).put("cleanup","OBSERVED_CLEAN_EXIT");files.write("verified.json",SourceJson.canonical(anchor),65536);
        before.run();return raw;
    }
    private void recheck(InstallationHostGuard guard,Context c,UUID checkpoint,long epoch,byte[] evidence,Runnable check){
        var files=new PreflightFiles(guard,c.bronze.run,c.bronze.attempt,checkpoint,"RECHECK_SILVER");byte[] command=command(c,checkpoint,epoch,"RECHECK");files.write("command.json",command,65536);files.write("evidence.json",SourceJson.canonical(SourceJson.parse(evidence).get("verification")),512*1024);
        var child=run(c,files,check,check,false);need(response(files,command,child).get("status").stringValue().equals("UNCHANGED"),INPUT_CHANGED);
        files.write("verified.json",SourceJson.canonical(SourceJson.object().put("commandSha256",SourceJson.sha(command)).put("evidenceSha256",SourceJson.sha(files.read("evidence.json",512*1024))).put("resultSha256",SourceJson.sha(files.read("result.json",65536))).put("childSha256",SourceJson.sha(files.read("child.json",65536))).put("cleanup","OBSERVED_CLEAN_EXIT")),65536);c.check();check.run();
    }
    private ReadOnlyVerifierProcess.Result run(Context c,PreflightFiles files,Runnable before,Runnable heartbeat,boolean spark){return new ReadOnlyVerifierProcess(spark).run(c.runtime.command(files.directory.resolve("command.json")),c.runtime.environment(),files.directory,Duration.ofSeconds(c.runtime.config.get("seconds").intValue()),new ReadOnlyVerifierProcess.Observer(){public void beforeStart(){c.check();before.run();}public void started(InstallationHostGuard.ProcessIdentity child){recordChild(files,child);c.check();before.run();}public void heartbeat(){c.check();heartbeat.run();}});}
    private static void recordChild(PreflightFiles files,InstallationHostGuard.ProcessIdentity child){files.write("child.json",SourceJson.canonical(SourceJson.object().put("pid",Long.toString(child.pid())).put("startedAt",child.startedAt().toString()).put("instanceId",child.instanceId().toString())),65536);}
    private static JsonNode response(PreflightFiles files,byte[] command,ReadOnlyVerifierProcess.Result child){
        need(Arrays.equals(command,files.read("command.json",65536)),RESULT_INVALID);var r=PreflightFiles.canonical(files.read("result.json",65536),65536);SourceJson.fields(r,"protocolVersion","commandSha256","status","cleanup","code");need(r.get("protocolVersion").intValue()==1 && SourceJson.sha(command).equals(r.get("commandSha256").stringValue()),RESULT_INVALID);
        if(child.exitCode()!=0){String code=r.get("code").stringValue();throw new PreflightFailure(code.equals("SPARK_CLEANUP_UNCERTAIN")?TERMINATION_UNCONFIRMED:code.endsWith("_CHANGED")?INPUT_CHANGED:code.endsWith("_UNAVAILABLE")?INPUT_UNAVAILABLE:code.equals("RUNTIME_INCOMPATIBLE")?RUNTIME_MISMATCH:INPUT_INVALID);}
        need(r.get("code").isNull(),RESULT_INVALID);return r;
    }
    private static byte[] verificationAnchor(Path directory){
        byte[] raw=PreflightFiles.read(directory.resolve("verified.json"),65536,true);var anchor=PreflightFiles.canonical(raw,65536);
        for(String name:List.of("command","result","evidence","selection"))need(SourceJson.sha(PreflightFiles.read(directory.resolve(name+".json"),name.equals("command") || name.equals("result")?65536:512*1024,true)).equals(anchor.get(name+"Sha256").stringValue()),INPUT_CHANGED);
        need(anchor.get("cleanup").stringValue().equals("OBSERVED_CLEAN_EXIT"),INPUT_INVALID);return raw;
    }
    private static byte[] command(Context c,UUID checkpoint,long epoch,String mode){var n=SourceJson.object().put("protocolVersion",1).put("mode",mode).put("runId",c.bronze.run.toString()).put("attemptId",c.bronze.attempt.toString()).put("checkpointId",checkpoint.toString()).put("ownerEpoch",epoch).put("runtimeSha256",c.runtime.digest);n.set("receipt",SourceJson.parse(c.bronze.receipt));n.set("artifact",c.artifact);n.set("binding",c.runtime.binding());n.set("bronze",c.bronze.storage.get("configuration"));n.set("runtime",c.runtime.nativeRuntime());return SourceJson.canonical(n);}
}
