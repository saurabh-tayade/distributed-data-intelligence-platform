package platform.contentapi.execution;

import java.io.IOException;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;
import static platform.contentapi.execution.NarrowContentOperationRouting.Operation;

/** Read-only retained-prefix file classification. Never grants execution or terminal authority.
 * SQL history eligibility and stopped-writer review remain separate mandatory checks. */
final class NarrowContentTerminalEvidence {
    private static final List<String> PHASES=List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED");
    private static final int MB=1024*1024;
    record Observation(String sqlSha256,String inventorySha256,long bytes,int entries,List<String> limitations) {
        Observation { limitations=List.copyOf(limitations); }
        boolean authorization(){return false;}
        boolean historicalFilesystemIdentityEstablished(){return false;}
    }
    /** Detached SQL material only. No callback, owner, native request or writable handle. */
    record OperationEvidence(Operation operation,List<String> phases,byte[] command,String candidate,
                             Map<String,byte[]> checkpointFiles,byte[] anchor,byte[] anchorStamp,boolean checkpoint) {
        OperationEvidence {
            phases=List.copyOf(phases);command=command.clone();
            var copy=new TreeMap<String,byte[]>();checkpointFiles.forEach((k,v)->copy.put(k,v.clone()));checkpointFiles=Collections.unmodifiableMap(copy);
            anchor=anchor==null?null:anchor.clone();anchorStamp=anchorStamp==null?null:anchorStamp.clone();
        }
    }

    private record Entry(boolean directory,long bytes,String identity,String modified,String digest) {}

    static Observation inspect(JdbcTemplate j,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt){
        require(j!=null && tx!=null && guard!=null && run!=null && attempt!=null && !TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        Snapshot before=tx.required(()->read(j,tx,run,attempt));
        Observation observation=before.verify(guard);
        Snapshot after=tx.required(()->read(j,tx,run,attempt));
        require(before.digest().equals(after.digest()),RUNTIME_MISMATCH);
        return observation;
    }
    static Observation inspect(InstallationHostGuard guard,NarrowContentEvidenceContext.Expected ready,List<OperationEvidence> operations,String sqlSignature){
        require(!TransactionSynchronizationManager.isActualTransactionActive() && ready!=null && ready.kind()==NarrowContentEvidenceContext.PlanKind.ORIGINAL,WORKER_NOT_READY);
        require(operations!=null && operations.size()<=17,RUNTIME_MISMATCH);
        var context=new NarrowContentEvidenceContext(ready);
        for(int i=0;i<operations.size();i++){
            var op=operations.get(i);require(op.operation().ordinal()==i && (i==0 || operations.get(i-1).checkpoint()),RUNTIME_MISMATCH);
            require(op.phases().size()<=6 && op.phases().equals(PHASES.subList(0,op.phases().size())) && (!op.checkpoint() || op.phases().size()==6),RUNTIME_MISMATCH);
        }
        var before=tree(context,guard,operations);validate(context,operations,before);
        var after=tree(context,guard,operations);require(before.equals(after),RUNTIME_MISMATCH);
        var inventory=SourceJson.object();long bytes=0;
        for(var e:after.entrySet()) {var v=e.getValue();bytes+=v.directory()?0:v.bytes();inventory.putObject(e.getKey()).put("directory",v.directory()).put("bytes",v.bytes()).put("identity",v.identity()).put("modified",v.modified()).put("sha256",v.digest());}
        return new Observation(sqlSignature,SourceJson.sha(SourceJson.canonical(inventory)),bytes,after.size(),List.of(
            "Current bounded bytes and stamps do not establish uninterrupted historical inode custody.",
            "Bronze candidate directory metadata is not reconstructible after authorized anchor publication; checkpoint bytes and retained anchor stamp are checked instead.",
            "Native output outside this managed tree is unverified; stopped-writer review remains required."));
    }
    private static Map<String,Entry> tree(NarrowContentEvidenceContext context,InstallationHostGuard guard,List<OperationEvidence> operations){
        context.check(guard,context.frozen());Path root=context.root();
        var dirs=new HashSet<Path>();dirs.add(root);
        for(var op:operations)if(!op.phases().isEmpty())for(Path p=context.originalDirectory(op.operation().ledgerStage,op.operation().ledgerOrdinal);p.startsWith(root);p=p.getParent())dirs.add(p);
        byte[] ledger=PreflightFiles.read(root.resolve(ContentEvidenceBudget.MANIFEST),ContentEvidenceBudget.LEDGER,true);
        var value=PreflightFiles.canonical(ledger,ContentEvidenceBudget.LEDGER);context.ledger(ledger,value);
        require(!Files.exists(root.resolve(ContentEvidenceBudget.MANIFEST+".preparing"),LinkOption.NOFOLLOW_LINKS),RUNTIME_MISMATCH);
        var result=new TreeMap<String,Entry>();long total=0;
        try(var walk=Files.walk(root,4)){
            var paths=walk.limit(222).toList();require(paths.size()<=221,RUNTIME_MISMATCH);
            for(var p:paths){
                var a=Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);boolean directory=a.isDirectory();PreflightFiles.privateEntry(p,directory);
                var id=InstallationHostGuard.identity(p);String relative=root.relativize(p).toString();String digest=null;
                if(directory)require(dirs.contains(p),RUNTIME_MISMATCH);
                else {
                    int cap=relative.equals(ContentEvidenceBudget.MANIFEST)?ContentEvidenceBudget.LEDGER:relative.equals(ContentEvidenceBudget.LOCK)?0:value.get("files").has(relative)?value.get("files").get(relative).intValue():-1;
                    require(cap>=0 && a.size()<=cap,RUNTIME_MISMATCH);total+=a.size();require(total<=64L*MB,RUNTIME_MISMATCH);
                    digest=SourceJson.sha(PreflightFiles.read(p,Math.max(cap,1),true));
                }
                var b=Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
                require(id.equals(InstallationHostGuard.identity(p)) && a.size()==b.size() && a.lastModifiedTime().equals(b.lastModifiedTime()),RUNTIME_MISMATCH);
                result.put(relative,new Entry(directory,a.size(),id.device()+":"+id.inode(),a.lastModifiedTime().toString(),digest));
            }
        }catch(IOException ex){throw new WorkerFailure(WORKER_NOT_READY);}
        require(result.containsKey(ContentEvidenceBudget.MANIFEST) && result.containsKey(ContentEvidenceBudget.LOCK),RUNTIME_MISMATCH);
        context.check(guard,context.frozen());return result;
    }
    private static void validate(NarrowContentEvidenceContext context,List<OperationEvidence> operations,Map<String,Entry> tree){
        var allowed=new HashSet<String>(List.of(ContentEvidenceBudget.MANIFEST,ContentEvidenceBudget.LOCK));
        for(var op:operations){
            Path directory=context.originalDirectory(op.operation().ledgerStage,op.operation().ledgerOrdinal);String prefix=context.root().relativize(directory)+"/";
            var names=new TreeSet<String>();for(var e:tree.entrySet())if(!e.getValue().directory() && e.getKey().startsWith(prefix))names.add(e.getKey().substring(prefix.length()));
            validateOperation(directory,op,names,tree,prefix);
            for(String name:names)allowed.add(prefix+name);
        }
        for(var e:tree.entrySet())if(!e.getValue().directory())require(allowed.contains(e.getKey()),RUNTIME_MISMATCH);
    }
    private static void validateOperation(Path directory,OperationEvidence op,Set<String> names,Map<String,Entry> tree,String prefix){
        int phase=op.phases().size();
        if(phase==0){require(names.isEmpty() && !tree.containsKey(prefix.substring(0,prefix.length()-1)),RUNTIME_MISMATCH);return;}
        if(phase==1){
            require(names.isEmpty() || names.equals(Set.of("command.json.preparing")) || names.equals(Set.of("command.json")),RUNTIME_MISMATCH);
            if(names.contains("command.json"))exact(directory,"command.json",op.command());return;
        }
        require(names.contains("command.json"),RUNTIME_MISMATCH);exact(directory,"command.json",op.command());
        if(phase<5){require(names.equals(Set.of("command.json")),RUNTIME_MISMATCH);return;}
        String anchor=op.operation()==Operation.FETCH_SOURCE?"complete.json":op.operation()==Operation.STORE_BRONZE?"verified.json":null;
        var child=new TreeSet<>(names);child.remove("command.json");
        if(anchor!=null){
            boolean finalAnchor=child.remove(anchor),partialAnchor=child.remove(anchor+".preparing");
            require(!(finalAnchor && partialAnchor) && (!(finalAnchor || partialAnchor) || phase==6 && op.anchor()!=null),RUNTIME_MISMATCH);
            if(finalAnchor){exact(directory,anchor,op.anchor());if(op.anchorStamp()!=null)require(Arrays.equals(SourceJson.canonical(stamp(directory.resolve(anchor))),op.anchorStamp()),RUNTIME_MISMATCH);}
            if(op.checkpoint())require(finalAnchor && !partialAnchor,RUNTIME_MISMATCH);
        }else require(op.anchor()==null && op.anchorStamp()==null,RUNTIME_MISMATCH);
        var payloads=payloads(op.operation());var allowed=new HashSet<String>();allowed.add("result.json");allowed.add("result.json.preparing");for(String p:payloads){allowed.add(p);allowed.add(p+".preparing");}
        require(allowed.containsAll(child),RUNTIME_MISMATCH);
        boolean finalResult=child.contains("result.json"),claim=child.contains("result.json.preparing");
        require(!(finalResult && claim),RUNTIME_MISMATCH);
        require(child.isEmpty() || finalResult || claim,RUNTIME_MISMATCH);
        boolean ended=false;
        for(String p:payloads){boolean done=child.contains(p),partial=child.contains(p+".preparing");require(!(done&&partial) && (!ended || !done && !partial),RUNTIME_MISMATCH);if(partial || !done)ended=true;}
        boolean success=false;
        if(finalResult){
            var r=PreflightFiles.canonical(PreflightFiles.read(directory.resolve("result.json"),65536,true),65536);
            require(r.has("status") && r.get("status").isString(),RUNTIME_MISMATCH);success=!r.get("status").stringValue().equals("FAILED");
            result(op,directory,r,success);
            if(success){require(!ended && child.stream().noneMatch(n->n.endsWith(".preparing")),RUNTIME_MISMATCH);}
        }
        if(phase==6){require(finalResult && success && !claim && !ended && child.size()==payloads.size()+1 && op.candidate()!=null,RUNTIME_MISMATCH);
            // The Bronze directory changes when its authorized anchor is created. Its original
            // directory mtime/size were hashed, not durably retained as individual observations.
            if(op.operation()!=Operation.STORE_BRONZE || !names.contains("verified.json") && !names.contains("verified.json.preparing"))
                require(candidate(directory,op,payloads).equals(op.candidate()),RUNTIME_MISMATCH);
        }
        for(var file:op.checkpointFiles().entrySet())exact(directory,file.getKey(),file.getValue());
    }
    private static List<String> payloads(Operation op){
        if(op==Operation.FETCH_SOURCE)return List.of("payload.xml","metadata.json","descriptor.json");
        if(op==Operation.PUBLISH_SILVER || op==Operation.RECHECK_SILVER)return List.of();
        return op==Operation.VERIFY_SILVER?List.of("evidence.json","selection.json"):List.of("evidence.json");
    }
    private static void result(OperationEvidence op,Path directory,JsonNode r,boolean success){
        var c=PreflightFiles.canonical(op.command(),MB);
        if(op.operation().ordinal()<2){
            SourceJson.fields(r,"protocolVersion","runId","attemptId","stage","itemOrdinal","status","code","evidenceId","counts");
            for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))require(r.get(key).equals(c.get(key)),RUNTIME_MISMATCH);
            require(r.get("counts").isObject() && r.get("counts").isEmpty(),RUNTIME_MISMATCH);
            require(success?"OK".equals(r.get("status").stringValue()) && r.get("code").isNull() && r.get("evidenceId").equals(c.get("checkpointId")):r.get("code").isString() && r.get("evidenceId").isNull(),RUNTIME_MISMATCH);
            if(success && op.operation()==Operation.STORE_BRONZE){var ev=PreflightFiles.canonical(PreflightFiles.read(directory.resolve("evidence.json"),65536,true),65536);require("EXACT".equals(ev.get("state").stringValue()) && ev.get("commandSha256").stringValue().equals(SourceJson.sha(op.command())),RUNTIME_MISMATCH);}
        }else{
            SourceJson.fields(r,"protocolVersion","family","mode","runId","attemptId","ownerEpoch","checkpointId","nativeCheckpointId","runtimeSha256","strictRuntimeSha256","commandSha256","candidateOnly","status","evidenceSha256","selectionSha256","cleanup","code");
            for(String key:List.of("runId","attemptId","ownerEpoch","runtimeSha256"))require(r.get(key).equals(c.get(key)),RUNTIME_MISMATCH);
            require(r.get("protocolVersion").isInt() && r.get("protocolVersion").intValue()==2 && r.get("candidateOnly").isBoolean() && r.get("candidateOnly").booleanValue() && r.get("family").stringValue().equals(op.operation().family.name()) && r.get("mode").stringValue().equals(op.operation().mode) && r.get("commandSha256").stringValue().equals(SourceJson.sha(op.command())) && r.get("nativeCheckpointId").equals(c.get("checkpointId")) && r.get("checkpointId").stringValue().equals(directory.getFileName().toString()) && r.get("strictRuntimeSha256").stringValue().equals(StrictContentPipelineRuntime.protocolDigest()),RUNTIME_MISMATCH);
            var cleanup=r.get("cleanup");SourceJson.fields(cleanup,"status","gatewayPid");
            require(cleanup.get("status").isString() && Set.of("NOT_STARTED","CONFIRMED","UNCONFIRMED").contains(cleanup.get("status").stringValue())
                && (cleanup.get("gatewayPid").isNull() || cleanup.get("gatewayPid").isString() && cleanup.get("gatewayPid").stringValue().matches("[1-9][0-9]{0,18}")),RUNTIME_MISMATCH);
            if(success)require(NarrowContentPipeline.spark(op.operation())?"CONFIRMED".equals(cleanup.get("status").stringValue()) && cleanup.get("gatewayPid").isString():"NOT_STARTED".equals(cleanup.get("status").stringValue()) && cleanup.get("gatewayPid").isNull(),RUNTIME_MISMATCH);
            String status=op.operation()==Operation.PUBLISH_SILVER?"PROCESSED":op.operation()==Operation.RECHECK_SILVER || op.operation()==Operation.RECHECK_GOLD?"UNCHANGED":op.operation().mode.equals("PREPARE")?"PREPARED":"VERIFIED";
            require(success?status.equals(r.get("status").stringValue()) && r.get("code").isNull():r.get("code").isString(),RUNTIME_MISMATCH);
            for(String name:List.of("evidence","selection")){
                var hash=r.get(name+"Sha256");boolean expected=success && payloads(op.operation()).contains(name+".json");
                require(expected?hash.isString() && hash.stringValue().equals(SourceJson.sha(PreflightFiles.read(directory.resolve(name+".json"),MB,true))):hash.isNull(),RUNTIME_MISMATCH);
            }
        }
    }
    private static String candidate(Path directory,OperationEvidence op,List<String> payloads){
        var inventory=SourceJson.object();var names=new TreeSet<>(payloads);names.add("command.json");names.add("result.json");
        if(op.operation()==Operation.STORE_BRONZE)inventory.set("directory",stamp(directory));
        for(String name:names){Path p=directory.resolve(name);byte[] raw=PreflightFiles.read(p,name.equals("payload.xml")?5*MB:MB,true);var node=inventory.putObject(name);
            if(op.operation().ordinal()<2){node.put("bytes",raw.length).put("sha256",SourceJson.sha(raw));if(op.operation()==Operation.STORE_BRONZE)node.set("stamp",stamp(p));}
            else {var id=InstallationHostGuard.identity(p);node.put("sha256",SourceJson.sha(raw)).put("bytes",raw.length).put("device",Long.toString(id.device())).put("inode",Long.toString(id.inode())).put("modified",modified(p));}
        }return SourceJson.sha(SourceJson.canonical(inventory));
    }
    private static JsonNode stamp(Path p){try{var id=InstallationHostGuard.identity(p);return SourceJson.object().put("identity",id.device()+":"+id.inode()).put("modified",modified(p)).put("size",Files.size(p));}catch(IOException ex){throw new WorkerFailure(WORKER_NOT_READY);}}
    private static String modified(Path p){try{return Files.getLastModifiedTime(p,LinkOption.NOFOLLOW_LINKS).toString();}catch(IOException ex){throw new WorkerFailure(WORKER_NOT_READY);}}
    private static void exact(Path directory,String name,byte[] expected){require(expected!=null && Arrays.equals(expected,PreflightFiles.read(directory.resolve(name),Math.max(expected.length,1),true)),RUNTIME_MISMATCH);}
    static final class Snapshot {
        private final NarrowContentEvidenceContext.Expected ready;
        private final List<OperationEvidence> operations;
        private final String digest;
        private final Set<UUID> checkpoints;
        private final List<RecoveryQuiescence.Identity> processes;
        private final byte[] fetchDescriptor,fetchReference;
        Snapshot(NarrowContentEvidenceContext.Expected ready,List<OperationEvidence> operations,String digest,Set<UUID> checkpoints,List<RecoveryQuiescence.Identity> processes,byte[] descriptor,byte[] reference){
            this.ready=ready;this.operations=List.copyOf(operations);this.digest=digest;this.checkpoints=Set.copyOf(checkpoints);this.processes=List.copyOf(processes);fetchDescriptor=descriptor==null?null:descriptor.clone();fetchReference=reference==null?null:reference.clone();
        }
        String digest(){return digest;}
        Set<UUID> checkpoints(){return checkpoints;}
        List<RecoveryQuiescence.Identity> processes(){return processes;}
        Observation verify(InstallationHostGuard guard){
            Observation before=inspect(guard,ready,operations,digest);
            if(fetchReference!=null){
                var context=new NarrowContentEvidenceContext(ready);Path dir=context.originalDirectory("FETCH_SOURCE",0);
                var journal=new RssJournalFiles(guard,ready.run(),ready.attempt(),UUID.fromString(dir.getFileName().toString()),ready.source(),guard.binding().root().relativize(dir).toString(),false);
                var reopened=journal.reopen(operations.getFirst().command(),SourceJson.sha(operations.getFirst().anchor()));
                require(Arrays.equals(fetchDescriptor,reopened.descriptorBytes()) && Arrays.equals(fetchReference,reopened.checkpointReference()),RUNTIME_MISMATCH);
            }
            Observation after=inspect(guard,ready,operations,digest);require(before.equals(after),RUNTIME_MISMATCH);return after;
        }
    }
    /** SQL-only reconstruction. Caller must additionally fence terminal lifecycle state and bindings. */
    static Snapshot read(JdbcTemplate j,ManagementTransactions tx,UUID run,UUID attempt){
        String history=NarrowContentPipelineRecoveryAssessment.historicalSqlDigest(j,tx,run,attempt);
        var admission=NarrowContentAdmission.inspectHistorical(j,run,attempt);var ready=admission.ready();require(ready!=null && ready.kind()==NarrowContentEvidenceContext.PlanKind.ORIGINAL,RUNTIME_MISMATCH);
        var operations=new ArrayList<OperationEvidence>();var checkpoints=new HashSet<UUID>();var processes=new ArrayList<RecoveryQuiescence.Identity>();
        var signature=SourceJson.object().put("version",1).put("history",history);var rows=signature.putArray("rows");
        var owners=j.queryForList("SELECT w.pid,w.process_started_at FROM ddip_management.attempts a JOIN ddip_management.worker_instances w USING(instance_id) WHERE a.run_id=? AND a.attempt_id=? LIMIT 2",run,attempt);
        require(owners.size()==1,RUNTIME_MISMATCH);processes.add(new RecoveryQuiescence.Identity(((Number)owners.getFirst().get("pid")).longValue(),((java.sql.Timestamp)owners.getFirst().get("process_started_at")).toInstant()));
        byte[] descriptor=null,reference=null;
        var fetchRows=j.queryForList("SELECT * FROM ddip_management.content_native_operations WHERE attempt_id=? LIMIT 2",attempt);require(fetchRows.size()<=1,RUNTIME_MISMATCH);
        if(!fetchRows.isEmpty()){
            var row=fetchRows.getFirst();UUID id=(UUID)row.get("operation_id");checkpoints.add((UUID)row.get("checkpoint_id"));var launch=NarrowContentFetchLaunch.inspect(j,tx,id);var completion=NarrowContentFetchCompletion.inspect(j,tx,id);var command=NarrowContentFetchReservation.reconstruct(j,tx,ready,id).commandBytes();
            var intent=bounded(j,"content_fetch_completion_intents",id,1);byte[] anchor=intent.isEmpty()?null:canonicalBytes(intent.getFirst(),"completion",65536);if(!intent.isEmpty())rows.add(rowDigest(intent.getFirst()));
            var cp=bounded(j,"content_fetch_checkpoints",id,1);if(!cp.isEmpty()){require(completion.isPresent(),RUNTIME_MISMATCH);descriptor=(byte[])cp.getFirst().get("journal_descriptor");reference=(byte[])cp.getFirst().get("expected_evidence");rows.add(rowDigest(cp.getFirst()));}
            operations.add(new OperationEvidence(Operation.FETCH_SOURCE,launch.phases(),command,launch.candidateSha256(),Map.of(),anchor,null,completion.isPresent()));rows.add(rowDigest(row));if(launch.child()!=null)processes.add(new RecoveryQuiescence.Identity(launch.child().pid(),launch.child().startedAt()));
            for(var p:bounded(j,"content_native_launch_phases",id,6))rows.add(rowDigest(p));
        }
        var bronzeRows=j.queryForList("SELECT * FROM ddip_management.content_bronze_reservations WHERE attempt_id=? LIMIT 2",attempt);require(bronzeRows.size()<=1,RUNTIME_MISMATCH);
        if(!bronzeRows.isEmpty()){
            var row=bronzeRows.getFirst();UUID id=(UUID)row.get("operation_id");checkpoints.add((UUID)row.get("checkpoint_id"));var launch=NarrowContentBronzeLaunch.inspect(j,tx,id);var completion=NarrowContentBronzeCompletion.inspect(j,tx,id);var material=NarrowContentBronzeReservation.historicalMaterial(j,tx,ready,id);
            byte[] anchor=null,anchorStamp=null;var required=new HashMap<String,byte[]>();
            for(var p:bounded(j,"content_bronze_verification_phases",id,5)){
                rows.add(rowDigest(p));if("OBSERVED".equals(p.get("phase"))){
                    anchor=canonicalBytes(p,"verified",65536);
                    var ev=SourceJson.object().put("bronzeEvidenceVersion",1).put("commandSha256",SourceJson.sha(material.command())).put("state","EXACT");
                    ev.set("receipt",PreflightFiles.canonical(canonicalBytes(p,"receipt",65536),65536));
                    required.put("evidence.json",SourceJson.canonical(ev));required.put("result.json",javaSuccess(material.command()));
                }
                if(p.get("child_pid")!=null)processes.add(new RecoveryQuiescence.Identity(((Number)p.get("child_pid")).longValue(),((java.sql.Timestamp)p.get("child_started_at")).toInstant()));
            }
            var cp=bounded(j,"content_bronze_checkpoints",id,1);
            if(!cp.isEmpty()){
                require(completion.isPresent(),RUNTIME_MISMATCH);var c=cp.getFirst();rows.add(rowDigest(c));anchorStamp=canonicalBytes(c,"verified_stamp",65536);
                var ev=SourceJson.object().put("bronzeEvidenceVersion",1).put("commandSha256",SourceJson.sha(material.command())).put("state","EXACT");ev.set("receipt",PreflightFiles.canonical(canonicalBytes(c,"receipt",65536),65536));required.put("evidence.json",SourceJson.canonical(ev));required.put("result.json",javaSuccess(material.command()));
            }
            operations.add(new OperationEvidence(Operation.STORE_BRONZE,launch.phases(),material.command(),launch.candidateSha256(),required,anchor,anchorStamp,completion.isPresent()));rows.add(rowDigest(row));if(launch.child()!=null)processes.add(new RecoveryQuiescence.Identity(launch.child().pid(),launch.child().startedAt()));
            for(var p:bounded(j,"content_bronze_launch_phases",id,6))rows.add(rowDigest(p));
        }
        var suffix=j.queryForList("SELECT * FROM ddip_management.content_pipeline_operations WHERE attempt_id=? ORDER BY step_index LIMIT 16",attempt);require(suffix.size()<=15,RUNTIME_MISMATCH);
        for(int index=0;index<suffix.size();index++){
            var row=suffix.get(index);UUID id=(UUID)row.get("operation_id");checkpoints.add((UUID)row.get("checkpoint_id"));int step=((Number)row.get("step_index")).intValue();require(step==index+2,RUNTIME_MISMATCH);
            var phaseRows=bounded(j,"content_pipeline_phases",id,6);phaseRows.sort(Comparator.comparingInt(p->PHASES.indexOf((String)p.get("phase"))));String candidate=null;
            for(var p:phaseRows){rows.add(rowDigest(p));var phase=PreflightFiles.canonical(canonicalBytes(p,"phase",65536),65536);if("CANDIDATE_EXITED".equals(p.get("phase")))candidate=phase.get("candidateSha256").stringValue();if("CHILD_RECORDED".equals(p.get("phase"))){var child=phase.get("child");processes.add(new RecoveryQuiescence.Identity(Long.parseLong(child.get("pid").stringValue()),java.time.Instant.parse(child.get("startedAt").stringValue())));}}
            var cp=bounded(j,"content_pipeline_checkpoints",id,1);var required=new HashMap<String,byte[]>();
            if(!cp.isEmpty()){var c=cp.getFirst();rows.add(rowDigest(c));for(String key:List.of("result","evidence","selection"))if(c.get(key+"_bytes")!=null){byte[] bytes=(byte[])c.get(key+"_bytes");PreflightFiles.canonical(bytes,key.equals("result")?65536:MB);required.put(key+".json",bytes.clone());}}
            operations.add(new OperationEvidence(Operation.values()[step],phaseRows.stream().map(p->(String)p.get("phase")).toList(),canonicalBytes(row,"command",MB),candidate,required,null,null,!cp.isEmpty()));rows.add(rowDigest(row));
        }
        return new Snapshot(ready,operations,SourceJson.sha(SourceJson.canonical(signature)),checkpoints,processes,descriptor,reference);
    }
    private static byte[] javaSuccess(byte[] command){var c=PreflightFiles.canonical(command,65536);var r=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))r.set(key,c.get(key));r.put("status","OK").putNull("code");r.set("evidenceId",c.get("checkpointId"));r.putObject("counts");return SourceJson.canonical(r);}
    private static List<Map<String,Object>> bounded(JdbcTemplate j,String table,UUID operation,int max){var rows=j.queryForList("SELECT * FROM ddip_management."+table+" WHERE operation_id=? LIMIT "+(max+1),operation);require(rows.size()<=max,RUNTIME_MISMATCH);rows.sort(Comparator.comparing(NarrowContentTerminalEvidence::rowDigest));return rows;}
    private static byte[] canonicalBytes(Map<String,Object> row,String key,int cap){byte[] raw=(byte[])row.get(key+"_bytes");require(raw!=null && SourceJson.sha(raw).equals(row.get(key+"_sha256")),RUNTIME_MISMATCH);PreflightFiles.canonical(raw,cap);return raw.clone();}
    private static String rowDigest(Map<String,Object> row){var n=SourceJson.object();for(String key:new TreeSet<>(row.keySet())){Object value=row.get(key);if(value instanceof byte[] bytes)n.put(key,bytes.length+":"+SourceJson.sha(bytes));else n.put(key,value==null?null:value.toString());}return SourceJson.sha(SourceJson.canonical(n));}
    private NarrowContentTerminalEvidence(){}
}
