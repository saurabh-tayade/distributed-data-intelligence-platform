package platform.contentapi.execution;

import java.nio.file.*;
import java.sql.Timestamp;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.RecoveryFailure.*;
import static platform.contentapi.execution.RecoveryFailure.Code.*;

/** Explicit read-only Content reconciliation. Ownership and process authority belong to C3c. */
final class ContentRecovery {
    static final String POLICY="E5_CONTENT_RECOVERY_V1";
    record Receipt(UUID run,String state,String code,UUID artifact,boolean retryEligible){}
    static final class Qualified {
        final RecoveryHandoff.Bootstrap bootstrap; final JsonNode pack,observation,privateSeal; final RecoveryHandoff.Owner owner;
        final List<InstallationHostGuard.ProcessIdentity> children; final Instant at;final String observationAddress;final RecoveryFiles.Retained observationFile;
        Qualified(RecoveryHandoff.Bootstrap b,JsonNode p,JsonNode e,RecoveryHandoff.Owner o,List<InstallationHostGuard.ProcessIdentity> c,Instant at,JsonNode seal,String address,RecoveryFiles.Retained retained){observationFile=retained;observationAddress=address;privateSeal=seal.deepCopy();bootstrap=b;pack=p.deepCopy();observation=e.deepCopy();owner=o;children=List.copyOf(c);this.at=at;}
    }
    private final JdbcTemplate j;private final ManagementTransactions tx;private final RecoveryHandoff handoffs;
    ContentRecovery(JdbcTemplate j,ManagementTransactions tx,RecoveryHandoff h){this.j=j;this.tx=tx;handoffs=h;}
    Qualified qualify(InstallationHostGuard guard,RecoveryHandoff.Bootstrap b,RecoveryQuiescence.Review review){
        var owner=handoffs.authorize(guard,b,review);var pack=handoffs.contentPack(guard,b,owner);
        var children=new ArrayList<InstallationHostGuard.ProcessIdentity>();var cursor=new Cursor(owner);
        JsonNode first=observe(guard,b,pack,review,cursor,children);
        handoffs.verificationReview(guard,b,review,cursor.owner);children.forEach(handoffs::verificationChild);
        JsonNode second=first; // Finalization performs the second full observation after a fresh operator review.
        handoffs.verificationReview(guard,b,review,cursor.owner);children.forEach(handoffs::verificationChild);
        String address=RecoveryFiles.write(guard,cursor.owner.controlId(),UUID.randomUUID(),SourceJson.canonical(second));
        return new Qualified(b,pack,second,cursor.owner,children,handoffs.verificationNow(),captureRecovery(guard,cursor.owner),address,RecoveryFiles.retained(guard,address));
    }
    private static final class Cursor{RecoveryHandoff.Owner owner;int ordinal;Cursor(RecoveryHandoff.Owner o){owner=o;}}
    private JsonNode observe(InstallationHostGuard g,RecoveryHandoff.Bootstrap b,JsonNode pack,RecoveryQuiescence.Review review,Cursor cursor,List<InstallationHostGuard.ProcessIdentity> children){
        var runtime=handoffs.contentRuntime(b).content;runtime.check();
        UUID run=UUID.fromString(pack.get("runId").stringValue()),original=UUID.fromString(pack.get("attemptId").stringValue());
        var frozen=pack.get("frozen");var observed=SourceJson.object().put("state","PAUSED").put("code","RECOVERED_CHECKPOINT");var receipts=observed.putArray("receipts");
        var fetches=stages(pack,"FETCH_SOURCE");var stores=stages(pack,"STORE_BRONZE");
        Set<Integer> intended=new HashSet<>();for(var stage:fetches)need(intended.add(Integer.parseInt(stage.get("item_ordinal").stringValue())),EVIDENCE_CHANGED);
        need(intended.stream().allMatch(i->i>=0 && i<frozen.get("sources").size()),EVIDENCE_CHANGED);
        if(intended.size()<frozen.get("sources").size())for(String later:List.of("SILVER","PREPARE_GOLD","PUBLISH_GOLD","CHECK_INPUTS","PUBLISH_CONTENT","VERIFY_RESULT"))need(stages(pack,later).isEmpty(),EVIDENCE_CHANGED);
        for(int i=0;i<frozen.get("sources").size();i++){
            String source=frozen.get("sources").get(i).get("sourceId").stringValue();
            if(!intended.contains(i)){need(tx.required(()->j.queryForObject("SELECT count(*) FROM ddip_management.rss_deliveries WHERE run_id=? AND source_id=?",Integer.class,run,source))==0,EVIDENCE_CHANGED);continue;}
            if(!new RssFetchExecution(j,tx,new WorkerRepository(j,tx,g.binding().installationId())).committedJournalIntact(g,run,source)){need(!pack.has("ancestry"),EVIDENCE_CHANGED);return outcome("FAILED","FETCH_EVIDENCE_LOST");}
        }
        ObjectNode storageFailure=null;
        int sourceOrdinal=0;for(var source:frozen.get("sources")){
            String id=source.get("sourceId").stringValue();int ordinal=sourceOrdinal++;if(!intended.contains(ordinal))continue;
            var context=RssBronzeContext.load(j,tx,g,run,id,runtime.gold.silver.config.get("bronzeRuntimeId").stringValue());context.check();
            byte[] command=RssBronzeExecution.command(context,UUID.randomUUID(),Long.parseLong(pack.get("epoch").stringValue()),false);
            var bronze=nativeRead(g,b,review,cursor,children,SourceJson.parse(command),context.runtime.bronzeCommand(Path.of("COMMAND")),context.runtime.environment(),false,"BRONZE");
            String state=bronze.get("state").stringValue();
            need(!Set.of("UNKNOWN","UNAVAILABLE").contains(state),EVIDENCE_UNAVAILABLE);
            if(pack.has("ancestry") && !pack.get("ancestry").get("plan").get("boundary").stringValue().equals("BEFORE_FETCH"))need(state.equals("EXACT"),EVIDENCE_CHANGED);
            if(!state.equals("EXACT")){need(Set.of("ABSENT","PAYLOAD_ONLY","METADATA_ONLY","CONFLICT").contains(state),EVIDENCE_UNAVAILABLE);boolean conflict=state.equals("CONFLICT") || state.equals("METADATA_ONLY");if(!conflict && stores.stream().noneMatch(stage->Integer.parseInt(stage.get("item_ordinal").stringValue())==ordinal)){observed.put("checkpoint","FETCH_SOURCE");continue;}if(storageFailure==null || conflict)storageFailure=outcome("FAILED",conflict?"BRONZE_CONFLICT":"BRONZE_"+state);continue;}
            need(Arrays.equals(SourceJson.canonical(RssBronzeExecution.expectedReceipt(context,bronze.get("receipt").get("payloadContentType").stringValue())),SourceJson.canonical(bronze.get("receipt"))),EVIDENCE_CHANGED);
            receipts.add(bronze.get("receipt"));
        }
        if(storageFailure!=null){storageFailure.set("receipts",receipts);return storageFailure;}
        // Original execution checkpoints stay immutable; only independently verified complete sets qualify.
        var silver=stages(pack,"SILVER");var gold=stages(pack,"PUBLISH_GOLD");var content=stages(pack,"PUBLISH_CONTENT");var preflight=stages(pack,"CHECK_INPUTS");
        if(pack.has("ancestry") && content.isEmpty() && !pack.get("ancestry").get("plan").get("boundary").stringValue().equals("BEFORE_FETCH")){
            // No intended publisher exists in this attempt. An occupied namespace cannot be called an earlier safe boundary.
            String fingerprint=pack.get("ancestry").get("verification").get("prepared").get("fingerprint").stringValue();
            need(!tx.required(()->ContentResumeLineage.registered(j,"CONTENT",runtime.config.get("contentBindingId").stringValue(),"v1/processor_revision=1/publication="+fingerprint)),EVIDENCE_CHANGED);
            try{ContentResumeLineage.requireAbsent(Path.of(runtime.content.get("root").stringValue()),"v1/processor_revision=1/publication="+fingerprint);}catch(PreflightFailure ex){throw new RecoveryFailure(EVIDENCE_UNAVAILABLE);}
        }
        if(content.size()==1){
            requirePrior(pack,frozen.get("sources").size(),true);
            var c=originalCommand(g,pack,content.get(0));need(c.get("prepared").get("fingerprint").equals(content.get(0).get("intended_identity")),EVIDENCE_CHANGED);
            tx.required(()->{RssContentInputs.check(j,run,original,frozen,c.get("goldProof"),c.get("reference"));return null;});
            need(c.get("runtimeSha256").stringValue().equals(runtime.digest) && c.get("inputSha256").stringValue().equals(SourceJson.sha(SourceJson.canonical(RssContentInputs.dependencies(c.get("goldProof"),c.get("reference"))))) && c.get("reference").equals(RssContentInputs.reference(frozen)),EVIDENCE_CHANGED);
            var check=(ObjectNode)c.deepCopy();check.put("mode","VERIFY");check.putNull("verified");
            JsonNode result=nativeRead(g,b,review,cursor,children,check,runtime.command(Path.of("COMMAND")),runtime.gold.silver.environment(),false,"CONTENT");
            if(result.has("recoveryOutcome")){var failed=outcome("FAILED",result.get("recoveryOutcome").stringValue());failed.set("proof",result);return failed;}
            need(result.get("status").stringValue().equals("VERIFIED") && result.get("prepared").equals(c.get("prepared")),EVIDENCE_CHANGED);
            var artifact=SourceJson.object().put("managedContentVersion",1).put("kind","CONTENT").put("productId","content-intelligence").put("contractVersion",1).put("processorRevision",1).put("nativeFingerprint",c.get("prepared").get("fingerprint").stringValue()).put("storageBindingId",runtime.config.get("contentBindingId").stringValue()).put("relativeAddress","v1/processor_revision=1/publication="+c.get("prepared").get("fingerprint").stringValue());
            artifact.set("inputs",RssContentInputs.dependencies(c.get("goldProof"),c.get("reference")));artifact.set("verification",result);
            observed.put("state","SUCCEEDED").put("code","RECOVERED_PUBLICATION");observed.set("artifact",artifact);return observed;
        }
        if(preflight.size()==1){
            requirePrior(pack,frozen.get("sources").size(),false);var c=originalCommand(g,pack,preflight.get(0));
            tx.required(()->{RssContentInputs.check(j,run,original,frozen,c.get("goldProof"),c.get("reference"));return null;});
            var check=(ObjectNode)c.deepCopy();check.put("mode","PREPARE");
            observed.set("checkpoint",nativeRead(g,b,review,cursor,children,check,runtime.command(Path.of("COMMAND")),runtime.gold.silver.environment(),false,"CONTENT"));return observed;
        }
        if(gold.size()==1 || !stages(pack,"PREPARE_GOLD").isEmpty()){
            need(silver.size()==frozen.get("sources").size() && silver.stream().allMatch(s->s.get("state").stringValue().equals("VERIFIED")),EVIDENCE_UNAVAILABLE);
            var s=gold.isEmpty()?stages(pack,"PREPARE_GOLD").getFirst():gold.getFirst();var c=originalCommand(g,pack,s);
            tx.required(()->{RssGoldInputs.check(j,run,ContentResumeLineage.dataAttempt(j,run,original),frozen,c.get("inputs"));return null;});
            var check=(ObjectNode)c.deepCopy();check.put("mode",gold.isEmpty()?"PREPARE":"VERIFY");
            var result=nativeRead(g,b,review,cursor,children,check,runtime.gold.command(Path.of("COMMAND")),runtime.gold.silver.environment(),true,"GOLD");
            if(result.has("recoveryOutcome")){need(!s.has("recordedAttemptId"),EVIDENCE_CHANGED);var failed=outcome("FAILED",result.get("recoveryOutcome").stringValue());failed.set("proof",result);return failed;}committedBaseline(recordedAttempt(pack,s),s,result,"verification");observed.set("checkpoint",result);return observed;
        }
        if(!silver.isEmpty()){
            need(silver.size()==frozen.get("sources").size(),EVIDENCE_UNAVAILABLE);var qualified=observed.putArray("silver");
            for(var s:silver){var c=(ObjectNode)originalCommand(g,pack,s).deepCopy();c.put("mode","VERIFY");var result=nativeRead(g,b,review,cursor,children,c,runtime.gold.silver.command(Path.of("COMMAND")),runtime.gold.silver.environment(),true,"SILVER");if(result.has("recoveryOutcome")){var failed=outcome("FAILED",result.get("recoveryOutcome").stringValue());failed.set("proof",result);return failed;}committedBaseline(original,s,result.get("evidence"),"verification");qualified.add(result);}
        }
        return observed;
    }
    private void committedBaseline(UUID attempt,JsonNode stage,JsonNode verified,String field){
        if(!stage.get("state").stringValue().equals("VERIFIED") || stage.get("stage").stringValue().equals("PREPARE_GOLD"))return;
        var rows=tx.required(()->j.queryForList("SELECT a.evidence_bytes,a.evidence_sha256 FROM ddip_management.stage_artifacts x JOIN ddip_management.artifacts a USING(artifact_id) WHERE x.attempt_id=? AND x.stage=? AND x.item_ordinal=? AND x.role='RESULT' AND x.ordinal=0",attempt,stage.get("stage").stringValue(),Integer.parseInt(stage.get("item_ordinal").stringValue())));
        need(rows.size()==1,EVIDENCE_UNAVAILABLE);var row=rows.getFirst();byte[] raw=(byte[])row.get("evidence_bytes");need(SourceJson.sha(raw).equals(row.get("evidence_sha256")) && SourceJson.parse(raw,PreflightFiles.LARGE).get(field).equals(verified),EVIDENCE_CHANGED);
    }
    private static ObjectNode outcome(String state,String code){return SourceJson.object().put("state",state).put("code",code);}
    private static List<JsonNode> stages(JsonNode p,String name){var list=new ArrayList<JsonNode>();for(var s:p.get("stages"))if(s.get("stage").stringValue().equals(name))list.add(s);return list;}
    private static void requirePrior(JsonNode p,int sources,boolean check){for(String name:List.of("FETCH_SOURCE","STORE_BRONZE","SILVER","PREPARE_GOLD","PUBLISH_GOLD")){var rows=stages(p,name);need(rows.size()==(Set.of("FETCH_SOURCE","STORE_BRONZE","SILVER").contains(name)?sources:1) && rows.stream().allMatch(s->s.get("state").stringValue().equals("VERIFIED")),EVIDENCE_UNAVAILABLE);}if(check)need(stages(p,"CHECK_INPUTS").size()==1 && stages(p,"CHECK_INPUTS").getFirst().get("state").stringValue().equals("VERIFIED"),EVIDENCE_UNAVAILABLE);}
    private static JsonNode originalCommand(InstallationHostGuard g,JsonNode pack,JsonNode s){
        need(!s.get("command_address").isNull(),EVIDENCE_UNAVAILABLE);String address=s.get("command_address").stringValue();
        byte[] raw=RecoveryFiles.readPrivate(g,address,PreflightFiles.LARGE);need(SourceJson.sha(raw).equals(s.get("command_sha256").stringValue()),EVIDENCE_CHANGED);var c=PreflightFiles.canonical(raw,PreflightFiles.LARGE);
        need(address.startsWith("runs/"+pack.get("runId").stringValue()+"/attempts/"+recordedAttempt(pack,s)+"/"),EVIDENCE_CHANGED);
        need(c.get("runId").equals(pack.get("runId")) && c.get("attemptId").stringValue().equals(recordedAttempt(pack,s).toString()) && c.get("checkpointId").equals(s.get("checkpoint_id")) && (c.get("ownerEpoch").isString()?c.get("ownerEpoch").stringValue():c.get("ownerEpoch").toString()).equals(s.has("recordedEpoch")?s.get("recordedEpoch").stringValue():pack.get("epoch").stringValue()),EVIDENCE_CHANGED);return c;
    }
    private static UUID recordedAttempt(JsonNode pack,JsonNode stage){return UUID.fromString((stage.has("recordedAttemptId")?stage.get("recordedAttemptId"):pack.get("attemptId")).stringValue());}
    private JsonNode nativeRead(InstallationHostGuard g,RecoveryHandoff.Bootstrap b,RecoveryQuiescence.Review review,Cursor cursor,List<InstallationHostGuard.ProcessIdentity> children,JsonNode original,List<String> invocation,Map<String,String> env,boolean spark,String kind){
        int item=cursor.ordinal++;UUID checkpoint=UUID.randomUUID();var files=new PreflightFiles(g,cursor.owner.runId(),cursor.owner.controlId(),checkpoint,"VERIFY_RESULT");
        JsonNode command=original;var call=new ArrayList<>(invocation);
        if(!kind.equals("BRONZE")){
            command=SourceJson.object().put("kind",kind).set("original",original);
            var runtime=handoffs.contentRuntime(b).content;
            call=new ArrayList<>(List.of(runtime.gold.config.get("python").stringValue(),"-I","-B","-X","utf8",Path.of(runtime.gold.silver.config.get("processingRoot").stringValue()).resolve("content_recovery_verifier.py").toString(),"COMMAND"));
        }
        byte[] bytes=SourceJson.canonical(command);files.write("command.json",bytes,PreflightFiles.LARGE);String sha=SourceJson.sha(bytes);
        var old=cursor.owner;
        cursor.owner=handoffs.fenced(g,old,()->{
            need(j.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations s JOIN ddip_management.attempts a USING(attempt_id) WHERE a.run_id=?",Integer.class,old.runId())<512,CAPACITY_EXCEEDED);
            if(old.attemptVersion()==0){exact(j.update("UPDATE ddip_management.attempts SET state='RUNNING',row_version=row_version+1 WHERE attempt_id=? AND state='CLAIMED' AND row_version=0",old.controlId()));audit(old,"RECOVERY_ATTEMPT_RUNNING","CLAIMED","RUNNING","STARTED",SourceJson.object().put("policy",POLICY));}
            j.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,command_address,command_sha256) VALUES (?,'VERIFY_RESULT',?,?,'INTENT',?,?,?)",old.controlId(),item,checkpoint,sha,g.binding().root().relativize(files.directory.resolve("command.json")).toString(),sha);
            audit(old,"RECOVERY_STAGE_INTENT",null,"INTENT","ACCEPTED",SourceJson.object().put("policy",POLICY).put("checkpointId",checkpoint.toString()));
            return new RecoveryHandoff.Owner(old.worker(),old.runId(),old.controlId(),old.predecessor(),old.runVersion(),1,old.controlVersion(),old.qualificationSha256());
        });
        call.set(call.size()-1,files.directory.resolve("command.json").toString());
        var approved=handoffs.contentRuntime(b).content;int seconds=kind.equals("BRONZE")?180:(kind.equals("SILVER")?approved.gold.silver.config:kind.equals("GOLD")?approved.gold.config:approved.config).get("seconds").intValue();
        var child=new ReadOnlyVerifierProcess(spark).run(call,env,files.directory,Duration.ofSeconds(seconds),new ReadOnlyVerifierProcess.Observer(){
            public void beforeStart(){need(Arrays.equals(bytes,files.read("command.json",PreflightFiles.LARGE)),EVIDENCE_CHANGED);handoffs.verificationReview(g,b,review,cursor.owner);handoffs.fenced(g,cursor.owner,()->{exact(j.update("UPDATE ddip_management.stage_invocations SET state='RUNNING',started_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=? AND state='INTENT' AND row_version=0",cursor.owner.controlId(),item));return null;});need(Arrays.equals(bytes,files.read("command.json",PreflightFiles.LARGE)),EVIDENCE_CHANGED);}
            public void started(InstallationHostGuard.ProcessIdentity p){children.add(p);handoffs.fenced(g,cursor.owner,()->{exact(j.update("UPDATE ddip_management.stage_invocations SET child_pid=?,child_started_at=?,child_instance_id=?,row_version=row_version+1 WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=? AND state='RUNNING' AND row_version=1",p.pid(),Timestamp.from(p.startedAt()),p.instanceId(),cursor.owner.controlId(),item));return null;});}
            public void heartbeat(){cursor.owner=handoffs.heartbeat(g,cursor.owner);}
        });
        need(Arrays.equals(bytes,files.read("command.json",PreflightFiles.LARGE)),EVIDENCE_CHANGED);handoffs.verificationChild(child.child());
        var response=kind.equals("BRONZE")?SourceJson.parse(files.read("result.json",65536)):PreflightFiles.canonical(files.read("result.json",65536),65536);JsonNode evidence;
        if(kind.equals("BRONZE")){
            for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))need(response.get(key).equals(original.get(key)),EVIDENCE_CHANGED);
            evidence=SourceJson.parse(files.read("evidence.json",65536));need(evidence.get("commandSha256").stringValue().equals(sha),EVIDENCE_CHANGED);
            String state=evidence.get("state").stringValue();need(state.equals("EXACT")?child.exitCode()==0 && response.get("status").stringValue().equals("OK") && response.get("evidenceId").equals(original.get("checkpointId")):child.exitCode()==1 && response.get("code").stringValue().equals("BRONZE_"+state),EVIDENCE_CHANGED);
        }else{if(child.exitCode()!=0){String failure=response.get("code").stringValue();if(kind.equals("CONTENT") && failure.equals("PUBLICATION_INVALID")){
                var seal=RecoveryNativeSeal.captureContent(handoffs.contentRuntime(b).content,original);
                need(seal.demonstrablyInvalid(original.get("prepared").get("fingerprint").stringValue()),EVIDENCE_UNAVAILABLE);
                var invalid=SourceJson.object().put("recoveryOutcome","PUBLICATION_INVALID").put("nativeSealSha256",seal.digest());
                // Retain a successful recovery observation of independently demonstrated corrupt bytes.
                byte[] raw=SourceJson.canonical(invalid);files.write("evidence.json",raw,PreflightFiles.LARGE);
                var anchor=SourceJson.object().put("policy",POLICY).put("commandSha256",sha).put("evidenceSha256",SourceJson.sha(raw)).put("resultSha256",SourceJson.sha(files.read("result.json",65536))).put("cleanup","CONFIRMED");files.write("verified.json",SourceJson.canonical(anchor),65536);
                handoffs.fenced(g,cursor.owner,()->{exact(j.update("UPDATE ddip_management.stage_invocations SET state='VERIFIED',result_address=?,result_sha256=?,outcome_code='PUBLICATION_INVALID',finished_at=clock_timestamp(),row_version=row_version+1 WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=? AND state='RUNNING' AND row_version=2",g.binding().root().relativize(files.directory.resolve("verified.json")).toString(),SourceJson.sha(SourceJson.canonical(anchor)),cursor.owner.controlId(),item));return null;});return invalid;
            }throw new RecoveryFailure(Set.of("INPUT_CHANGED","EVIDENCE_CHANGED","PUBLICATION_CHANGED","SNAPSHOT_CHANGED","BRONZE_CHANGED").contains(failure)?EVIDENCE_CHANGED:EVIDENCE_UNAVAILABLE);}need(child.exitCode()==0 && response.get("cleanup").stringValue().equals("CONFIRMED") && response.get("code").isNull() && response.get("commandSha256").stringValue().equals(sha),EVIDENCE_UNAVAILABLE);byte[] raw=files.read("evidence.json",PreflightFiles.LARGE);need(SourceJson.sha(raw).equals(response.get("evidenceSha256").stringValue()),EVIDENCE_CHANGED);evidence=PreflightFiles.canonical(raw,PreflightFiles.LARGE);}
        var anchor=SourceJson.object().put("policy",POLICY).put("commandSha256",sha).put("evidenceSha256",SourceJson.sha(SourceJson.canonical(evidence))).put("resultSha256",SourceJson.sha(files.read("result.json",65536))).put("cleanup","CONFIRMED");byte[] raw=SourceJson.canonical(anchor);files.write("verified.json",raw,65536);
        handoffs.fenced(g,cursor.owner,()->{exact(j.update("UPDATE ddip_management.stage_invocations SET state='VERIFIED',result_address=?,result_sha256=?,finished_at=clock_timestamp(),outcome_code='VERIFIED',row_version=row_version+1 WHERE attempt_id=? AND stage='VERIFY_RESULT' AND item_ordinal=? AND state='RUNNING' AND row_version=2",g.binding().root().relativize(files.directory.resolve("verified.json")).toString(),SourceJson.sha(raw),cursor.owner.controlId(),item));audit(cursor.owner,"RECOVERY_STAGE_OBSERVED","RUNNING","VERIFIED","VERIFIED",anchor);return null;});
        return evidence;
    }
    Receipt finish(InstallationHostGuard g,Qualified q,RecoveryQuiescence.Review review){
        var existing=resolve(g,q.owner.controlId());if(existing!=null)return existing;
        RecoveryQuiescence.fresh(q.at,handoffs.verificationNow());handoffs.verificationReview(g,q.bootstrap,review,q.owner);q.children.forEach(handoffs::verificationChild);
        need(handoffs.contentPack(g,q.bootstrap,q.owner).equals(q.pack),EVIDENCE_CHANGED);
        preserved(q.privateSeal,captureRecovery(g,q.owner));
        var retained=RecoveryFiles.retained(g,q.observationAddress);
        need(retained.identity().equals(q.observationFile.identity()) && retained.modifiedAt().equals(q.observationFile.modifiedAt()) && Arrays.equals(retained.bytes(),SourceJson.canonical(q.observation)),EVIDENCE_CHANGED);
        // A new final independent observation uses the current explicit review and exact owner.
        var cursor=new Cursor(q.owner);cursor.ordinal=handoffs.fenced(g,q.owner,()->j.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=?",Integer.class,q.owner.controlId()));
        var children=new ArrayList<>(q.children);var fresh=observe(g,q.bootstrap,q.pack,review,cursor,children);need(fresh.equals(q.observation),EVIDENCE_CHANGED);
        handoffs.verificationReview(g,q.bootstrap,review,cursor.owner);children.forEach(handoffs::verificationChild);
        var finalSeal=captureRecovery(g,cursor.owner);preserved(q.privateSeal,finalSeal);
        need(finalSeal.equals(captureRecovery(g,cursor.owner)),EVIDENCE_CHANGED);
        return handoffs.fenced(g,cursor.owner,()->apply(g,q.pack,fresh,cursor.owner,review,q.observationAddress));
    }
    private JsonNode captureRecovery(InstallationHostGuard g,RecoveryHandoff.Owner o){
        var rows=handoffs.fenced(g,o,()->j.queryForList("SELECT * FROM ddip_management.stage_invocations WHERE attempt_id=? ORDER BY item_ordinal",o.controlId()));
        for(var row:rows)if(row.get("child_pid")!=null)handoffs.verificationChild(new InstallationHostGuard.ProcessIdentity((UUID)row.get("child_instance_id"),((Number)row.get("child_pid")).longValue(),((Timestamp)row.get("child_started_at")).toInstant()));
        return ContentRecoveryFiles.capture(g,o.runId(),o.controlId(),rows).inventory();
    }
    private static void preserved(JsonNode before,JsonNode after){
        var current=new HashMap<String,JsonNode>();for(var f:after.get("files"))current.put(f.get("address").stringValue(),f);
        for(var f:before.get("files")){var now=current.get(f.get("address").stringValue());need(now!=null,EVIDENCE_CHANGED);if(f.get("directory").booleanValue())need(f.get("device").equals(now.get("device")) && f.get("inode").equals(now.get("inode")),EVIDENCE_CHANGED);else need(f.equals(now),EVIDENCE_CHANGED);}
    }
    private Receipt apply(InstallationHostGuard g,JsonNode pack,JsonNode observation,RecoveryHandoff.Owner o,RecoveryQuiescence.Review review,String observationAddress){
        handoffs.verificationFresh(review);String state=observation.get("state").stringValue(),code=observation.get("code").stringValue();
        need(j.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=? AND state<>'VERIFIED'",Integer.class,o.controlId())==0,EVIDENCE_UNAVAILABLE);
        boolean cancel=j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.controls WHERE run_id=? AND type='CANCEL' AND state IN ('REQUESTED','APPLIED'))",Boolean.class,o.runId());
        if(cancel && !state.equals("SUCCEEDED")){state="CANCELLED";code="CANCELLED";}
        int pendingControls=j.queryForObject("SELECT count(*) FROM ddip_management.controls WHERE run_id=? AND type='CANCEL' AND state='REQUESTED'",Integer.class,o.runId());
        need(AuditCapacity.available(j,8+pendingControls,j.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state IN ('QUEUED','RUNNING','CANCEL_REQUESTED','RECOVERY_REQUIRED')",Long.class)),CAPACITY_EXCEEDED);
        boolean retry=state.equals("FAILED") && Set.of("PUBLICATION_ABSENT","BRONZE_ABSENT","BRONZE_PAYLOAD_ONLY").contains(code) && j.queryForObject("SELECT max(attempt_no)<20 FROM ddip_management.attempts WHERE run_id=?",Boolean.class,o.runId());
        if(code.startsWith("BRONZE_")){for(var stage:pack.get("stages"))if(!Set.of("FETCH_SOURCE","STORE_BRONZE").contains(stage.get("stage").stringValue()))retry=false;}
        UUID artifact=null;byte[] counts=null;
        if(state.equals("SUCCEEDED")){
            var e=observation.get("artifact");byte[] bytes=SourceJson.canonical(e);String sha=SourceJson.sha(bytes);
            var rows=j.queryForList("SELECT * FROM ddip_management.artifacts WHERE (kind='CONTENT' AND native_identity=?) OR (storage_binding_id=? AND relative_address=?) FOR UPDATE",e.get("nativeFingerprint").stringValue(),e.get("storageBindingId").stringValue(),e.get("relativeAddress").stringValue());
            if(rows.isEmpty()){
                artifact=UUID.randomUUID();j.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at) VALUES (?,'CONTENT','content-intelligence',1,1,?,?,?,?,?,clock_timestamp())",artifact,e.get("nativeFingerprint").stringValue(),e.get("storageBindingId").stringValue(),e.get("relativeAddress").stringValue(),bytes,sha);
                for(var input:e.get("inputs"))j.update("INSERT INTO ddip_management.artifact_dependencies VALUES (?,?,0,?)",artifact,input.get("role").stringValue(),UUID.fromString(input.get("artifactId").stringValue()));
            }else{need(rows.size()==1 && Arrays.equals(bytes,(byte[])rows.getFirst().get("evidence_bytes")) && sha.equals(rows.getFirst().get("evidence_sha256")) && "CONTENT".equals(rows.getFirst().get("kind")) && "content-intelligence".equals(rows.getFirst().get("product_id")) && e.get("nativeFingerprint").stringValue().equals(rows.getFirst().get("native_identity")) && e.get("storageBindingId").stringValue().equals(rows.getFirst().get("storage_binding_id")) && e.get("relativeAddress").stringValue().equals(rows.getFirst().get("relative_address")),EVIDENCE_CHANGED);artifact=(UUID)rows.getFirst().get("artifact_id");var links=j.queryForList("SELECT role,dependency_artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? ORDER BY role,ordinal",artifact);need(links.size()==2,EVIDENCE_CHANGED);for(int i=0;i<2;i++)need(e.get("inputs").get(i).get("role").stringValue().equals(links.get(i).get("role")) && e.get("inputs").get(i).get("artifactId").stringValue().equals(links.get(i).get("dependency_artifact_id").toString()),EVIDENCE_CHANGED);}
            int ordinal=j.queryForObject("SELECT max(item_ordinal) FROM ddip_management.stage_invocations WHERE attempt_id=?",Integer.class,o.controlId());j.update("INSERT INTO ddip_management.stage_artifacts VALUES (?,'VERIFY_RESULT',?,'RESULT',0,?)",o.controlId(),ordinal,artifact);
            var c=e.get("verification").get("completion").get("output_counts");counts=SourceJson.canonical(SourceJson.object().put("articles",c.get("content_catalog_v1").toString()).put("rows",c.get("content_source_discovery_daily_v1").toString()));
        }
        if(observation.has("receipts"))for(var r:observation.get("receipts")){
            byte[] bytes=SourceJson.canonical(r);var row=j.queryForMap("SELECT state,receipt_bytes,row_version FROM ddip_management.rss_deliveries WHERE run_id=? AND source_id=? FOR UPDATE",o.runId(),r.get("sourceId").stringValue());
            if(row.get("receipt_bytes")!=null)need(Arrays.equals(bytes,(byte[])row.get("receipt_bytes")),EVIDENCE_CHANGED);
            else exact(j.update("UPDATE ddip_management.rss_deliveries SET state='BRONZE_VERIFIED',metadata_sha256=?,receipt_bytes=?,row_version=row_version+1 WHERE run_id=? AND source_id=? AND owning_attempt_id=? AND row_version=? AND receipt_bytes IS NULL",r.get("expected").get("metadataSha256").stringValue(),bytes,o.runId(),r.get("sourceId").stringValue(),UUID.fromString(pack.get("attemptId").stringValue()),row.get("row_version")));
        }
        exact(j.update("UPDATE ddip_management.attempts SET state='SUCCEEDED',finished_at=clock_timestamp(),outcome_code=?,row_version=row_version+1 WHERE attempt_id=? AND row_version=? AND state IN ('CLAIMED','RUNNING')","RECONCILED_"+state,o.controlId(),o.attemptVersion()));
        exact(j.update("UPDATE ddip_management.runs SET state=?,finished_at=CASE WHEN ?='PAUSED' THEN NULL ELSE clock_timestamp() END,result_artifact_id=?,counts_bytes=?,outcome_code=?,retry_eligible=?,row_version=row_version+1 WHERE run_id=? AND state='RECOVERY_REQUIRED' AND row_version=? AND current_attempt_id=?",state,state,artifact,counts,code,retry,o.runId(),o.runVersion(),o.controlId()));
        exact(j.update("UPDATE ddip_management.controls SET state='APPLIED',reason=?,applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=? AND state='REQUESTED' AND row_version=?",code,o.controlId(),o.controlVersion()));
        if(cancel && state.equals("SUCCEEDED")){
            var pending=j.queryForList("SELECT control_id,state,row_version FROM ddip_management.controls WHERE run_id=? AND type='CANCEL' AND state IN ('REQUESTED','APPLIED') FOR UPDATE",o.runId());
            need(pending.stream().allMatch(k->k.get("state").equals("REQUESTED")),EVIDENCE_UNAVAILABLE);
            for(var k:pending){exact(j.update("UPDATE ddip_management.controls SET state='TOO_LATE',reason='TOO_LATE',applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=? AND state='REQUESTED'",k.get("control_id")));controlAudit(o,k,"TOO_LATE","TOO_LATE");}
        }
        if(cancel && state.equals("CANCELLED")){
            for(var pending:j.queryForList("SELECT control_id,row_version FROM ddip_management.controls WHERE run_id=? AND type='CANCEL' AND state='REQUESTED' FOR UPDATE",o.runId())){
                exact(j.update("UPDATE ddip_management.controls SET state='APPLIED',reason='CANCELLED',applied_at=clock_timestamp(),row_version=row_version+1 WHERE control_id=? AND state='REQUESTED'",pending.get("control_id")));
                controlAudit(o,pending,"APPLIED","CANCELLED");
            }
        }
        exact(j.update("UPDATE ddip_management.worker_instances SET status='STOPPED',row_version=row_version+1 WHERE instance_id=? AND status='RECOVERY_ONLY' AND row_version=?",o.worker().instanceId(),o.worker().workerVersion()));
        exact(j.update("UPDATE ddip_management.coordinator SET mode='READY',active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,row_version=row_version+1 WHERE singleton_id=1 AND mode='RECOVERY_ONLY' AND epoch=? AND row_version=? AND active_attempt_id=? AND owner_instance_id=?",o.worker().epoch(),o.worker().coordinatorVersion(),o.controlId(),o.worker().instanceId()));
        var details=SourceJson.object().put("policy",POLICY).put("qualificationSha256",o.qualificationSha256()).put("observationSha256",SourceJson.sha(SourceJson.canonical(observation))).put("runState",state).put("code",code).put("retryEligible",retry).put("originalAttemptId",pack.get("attemptId").stringValue()).put("originalEpoch",pack.get("epoch").stringValue()).put("observationAddress",observationAddress);if(artifact!=null)details.put("artifactId",artifact.toString());
        audit(o,"RECOVERY_ATTEMPT_FINISHED",o.attemptVersion()==0?"CLAIMED":"RUNNING","SUCCEEDED","RECONCILED_"+state,details);
        audit(o,"RECOVERY_RUN_FINISHED","RECOVERY_REQUIRED",state,code,details);
        audit(o,"RECOVERY_CONTROL_APPLIED","REQUESTED","APPLIED",code,details);
        audit(o,"RECOVERY_SLOT_RELEASED","RECOVERY_ONLY","READY",code,details);
        audit(o,"RECOVERY_FINALIZED","RECOVERY_REQUIRED",state,code,details);g.checkHeld(o.worker().installationId());handoffs.verificationFresh(review);return new Receipt(o.runId(),state,code,artifact,retry);
    }
    Receipt resolve(InstallationHostGuard g,UUID control){
        g.checkHeld(g.binding().installationId());return tx.required(()->{
            j.queryForMap("SELECT installation_id FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");
            var rows=j.queryForList("SELECT a.details_bytes,r.run_id,r.state,r.outcome_code,r.result_artifact_id,r.retry_eligible,k.state AS control_state,t.state AS attempt_state FROM ddip_management.audit_events a JOIN ddip_management.runs r USING(run_id) JOIN ddip_management.controls k ON k.control_id=a.control_id JOIN ddip_management.attempts t ON t.attempt_id=a.attempt_id WHERE a.control_id=? AND a.action='RECOVERY_FINALIZED'",control);
            if(rows.isEmpty())return null;need(rows.size()==1,EVIDENCE_CHANGED);var r=rows.getFirst();var d=SourceJson.parse((byte[])r.get("details_bytes"));
            if(d.get("runState").stringValue().equals("PAUSED") && (!d.get("runState").stringValue().equals(r.get("state")) || !d.get("code").stringValue().equals(r.get("outcome_code")))){
                need(d.get("policy").stringValue().equals(POLICY) && d.get("code").stringValue().equals("RECOVERED_CHECKPOINT") && !d.get("retryEligible").booleanValue() && "APPLIED".equals(r.get("control_state")) && "SUCCEEDED".equals(r.get("attempt_state")),EVIDENCE_CHANGED);
                var resumed=j.queryForList("SELECT e.details_bytes FROM ddip_management.audit_events e JOIN ddip_management.controls k ON k.control_id=e.control_id AND k.run_id=e.run_id WHERE e.run_id=? AND e.action='CONTENT_RESUME_QUEUED' AND e.before_state='PAUSED' AND e.after_state='QUEUED' AND k.type='RESUME' AND k.state='APPLIED' AND k.reason='RESUME_QUEUED'",r.get("run_id"));int linked=0;
                for(var row:resumed){var p=SourceJson.parse((byte[])row.get("details_bytes"),PreflightFiles.LARGE);if(p.get("recoveryAttemptId").stringValue().equals(control.toString())){need(p.get("policy").stringValue().equals(ContentResumePlan.POLICY) && p.get("runId").stringValue().equals(r.get("run_id").toString()) && p.get("originalAttemptId").equals(d.get("originalAttemptId")) && p.get("qualificationSha256").equals(d.get("qualificationSha256")) && p.get("observationSha256").equals(d.get("observationSha256")),EVIDENCE_CHANGED);linked++;}}
                need(linked==1,EVIDENCE_CHANGED);return new Receipt((UUID)r.get("run_id"),"PAUSED","RECOVERED_CHECKPOINT",null,false); // historical acknowledgment only; never authority or release
            }
            need(d.get("policy").stringValue().equals(POLICY) && d.get("runState").stringValue().equals(r.get("state")) && d.get("code").stringValue().equals(r.get("outcome_code")) && "APPLIED".equals(r.get("control_state")) && "SUCCEEDED".equals(r.get("attempt_state")),EVIDENCE_CHANGED);
            if(r.get("result_artifact_id")!=null)need(d.get("artifactId").stringValue().equals(r.get("result_artifact_id").toString()),EVIDENCE_CHANGED);
            return new Receipt((UUID)r.get("run_id"),(String)r.get("state"),(String)r.get("outcome_code"),(UUID)r.get("result_artifact_id"),Boolean.TRUE.equals(r.get("retry_eligible")));
        });
    }
    private void controlAudit(RecoveryHandoff.Owner o,Map<String,Object> row,String state,String code){
        long version=((Number)row.get("row_version")).longValue();j.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,'REQUESTED',?,?,?,?,?)",o.runId(),o.controlId(),row.get("control_id"),state.equals("TOO_LATE")?"CONTROL_TOO_LATE":"CONTROL_APPLIED",state,version,version+1,code,SourceJson.canonical(SourceJson.object().put("policy",POLICY).put("reconcileControlId",o.controlId().toString())));
    }
    private void audit(RecoveryHandoff.Owner o,String action,String before,String after,String code,JsonNode details){
        Long version=switch(action){case "RECOVERY_ATTEMPT_RUNNING","RECOVERY_ATTEMPT_FINISHED"->o.attemptVersion();case "RECOVERY_RUN_FINISHED","RECOVERY_FINALIZED"->o.runVersion();case "RECOVERY_CONTROL_APPLIED"->o.controlVersion();case "RECOVERY_SLOT_RELEASED"->o.worker().coordinatorVersion();case "RECOVERY_STAGE_OBSERVED"->2L;default->null;};
        Long next=version==null?(action.equals("RECOVERY_STAGE_INTENT")?0L:null):version+1;
        j.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,attempt_id,control_id,action,before_state,after_state,before_version,after_version,safe_code,details_bytes) VALUES ('local-operator',?,?,?,?,?,?,?,?,?,?)",o.runId(),o.controlId(),o.controlId(),action,before,after,version,next,code,SourceJson.canonical(details));
    }
    private static void exact(int n){need(n==1,OWNERSHIP_LOST);}
}
