package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive original-owner bridge. A native checkpoint is not a legacy stage, delivery or continuation. */
final class NarrowContentFetchCompletion {
    enum Boundary { FRESH_VERIFIED, BEFORE_INTENT, INTENT_INSERTED, INTENT_COMMITTED, BEFORE_MARKER, PREFIX_CREATED, PREFIX_WRITTEN,
        PREFIX_FORCED, MARKER_RENAMED, DIRECTORY_FORCED, MARKER_WRITTEN, JOURNAL_REOPENED, REOPENED, BEFORE_CHECKPOINT,
        CHECKPOINT_INSERTED, CHECKPOINT_COMMITTED }
    record Observation(UUID operation,UUID run,UUID attempt,UUID checkpoint,String candidateSha256,String completionSha256) {
        boolean checkpointed(){return true;}
        boolean nativeLaunchCompatible(){return false;}
        @Override public String toString(){return "NativeFetchCheckpoint[raw HTTP journal; no continuation]";}
    }
    private record Intent(UUID operation,String candidate,byte[] marker,String completion) {
        Intent {marker=marker.clone();}
        @Override public byte[] marker(){return marker.clone();}
        @Override public String toString(){return "FetchCompletionIntent[redacted]";}
    }
    private record Stamp(InstallationHostGuard.Identity identity,String modified,long size) { }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    NarrowContentFetchCompletion(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.workers=Objects.requireNonNull(workers);
        workers.requireAdmissionRepository(jdbc,tx);
    }
    Observation complete(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation){return complete(guard,owner,operation,b->{});}
    Observation complete(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,Consumer<Boundary> fault){
        require(guard!=null && owner!=null && operation!=null && fault!=null,WORKER_CONFIGURATION_INVALID);
        // A consumed intent is never a normal-completion entry point, even if no file was written.
        fence(guard,owner,()->{require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_fetch_completion_intents WHERE operation_id=?",Long.class,operation)==0,WORKER_NOT_READY);return null;});
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var context=new NarrowContentEvidenceContext(ready);
        var payload=NarrowContentFetchReservation.reconstruct(jdbc,tx,ready,operation);
        Path directory=ContentOperationPlan.original(context.root(),owner.runId(),owner.attemptId(),ready.source()).directory("FETCH_SOURCE",0);
        var journal=new RssJournalFiles(guard,owner.runId(),owner.attemptId(),payload.checkpoint(),ready.source(),guard.binding().root().relativize(directory).toString(),false);
        var parents=parents(context.root(),directory);
        byte[] marker=journal.completionBytes(payload.commandBytes());
        var observed=NarrowContentFetchCandidateVerification.inspect(jdbc,tx,guard,operation);
        require(observed.run().equals(owner.runId()) && observed.attempt().equals(owner.attemptId())
                && parents.equals(parents(context.root(),directory)) && Arrays.equals(marker,journal.completionBytes(payload.commandBytes())),OWNERSHIP_LOST);
        fault.accept(Boundary.FRESH_VERIFIED);
        var intent=new Intent(operation,observed.candidateSha256(),marker,SourceJson.sha(marker));
        require(markerCandidate(marker,payload.commandBytes()).equals(observed.candidateSha256()),RUNTIME_MISMATCH);
        fault.accept(Boundary.BEFORE_INTENT);
        candidate(guard,owner,operation,context,journal,payload,intent,parents);
        fence(guard,owner,()->{
            require(NarrowContentFetchLaunch.inspect(jdbc,tx,operation).candidateSha256().equals(intent.candidate()),RUNTIME_MISMATCH);
            jdbc.update("INSERT INTO ddip_management.content_fetch_completion_intents(operation_id,candidate_sha256,completion_bytes,completion_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?)",
                    operation,intent.candidate(),marker,intent.completion(),owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());fault.accept(Boundary.INTENT_INSERTED);return null;
        });
        // Any exception/uncertainty above ends the call. A read cannot authorize resuming this write.
        fault.accept(Boundary.INTENT_COMMITTED);fault.accept(Boundary.BEFORE_MARKER);
        candidate(guard,owner,operation,context,journal,payload,intent,parents);
        var published=new java.util.concurrent.atomic.AtomicReference<Map<String,Stamp>>();
        PreparedCommand.writeOnce(directory,"complete.json",marker,RssJournalFiles.SMALL,b->{
            if(b==PreparedCommand.WriteBoundary.MARKER_RENAMED)published.set(completionStamps(directory));
            fault.accept(Boundary.valueOf(b.name()));
            context.check(guard,context.frozen());require(parents.equals(parents(context.root(),directory)),RUNTIME_MISMATCH);
            if(published.get()!=null)require(published.get().equals(completionStamps(directory)),RUNTIME_MISMATCH);
            fence(guard,owner,()->null);
        });
        fault.accept(Boundary.MARKER_WRITTEN);
        var evidence=reopen(guard,context,journal,payload,intent,parents,published.get(),fault);
        fault.accept(Boundary.REOPENED);fault.accept(Boundary.BEFORE_CHECKPOINT);
        evidence=reopen(guard,context,journal,payload,intent,parents,published.get(),fault);
        var expected=evidence;
        try{
            fence(guard,owner,()->{
                exactIntent(jdbc,intent);
                jdbc.update("INSERT INTO ddip_management.content_fetch_checkpoints(operation_id,checkpoint_id,journal_address,journal_descriptor,journal_sha256,expected_evidence,expected_sha256,completion_sha256,payload_length,payload_sha256) VALUES (?,?,?,?,?,?,?,?,?,?)",
                        operation,payload.checkpoint(),expected.address(),expected.descriptorBytes(),SourceJson.sha(expected.descriptorBytes()),expected.checkpointReference(),SourceJson.sha(expected.checkpointReference()),intent.completion(),expected.descriptor().get("payloadByteLength").longValue(),expected.descriptor().get("payloadSha256").stringValue());
                fault.accept(Boundary.CHECKPOINT_INSERTED);return null;
            });
        }catch(ManagementFailure ex){
            if(ex.code()!=ManagementFailure.Code.MANAGEMENT_UNAVAILABLE)throw ex;
            // The owner channel is stopped. Only exact read-only acknowledgement confirmation is allowed.
            if(!confirm(jdbc,tx,intent,expected))throw ex;
            return inspect(jdbc,tx,operation).orElseThrow(()->new WorkerFailure(RUNTIME_MISMATCH));
        }
        fault.accept(Boundary.CHECKPOINT_COMMITTED);
        require(confirm(jdbc,tx,intent,expected),RUNTIME_MISMATCH);
        return inspect(jdbc,tx,operation).orElseThrow(()->new WorkerFailure(RUNTIME_MISMATCH));
    }
    private void candidate(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,NarrowContentEvidenceContext context,
                           RssJournalFiles journal,NarrowContentFetchReservation.Payload payload,Intent intent,Map<Path,Stamp> parents){
        var observed=NarrowContentFetchCandidateVerification.inspect(jdbc,tx,guard,operation);
        require(observed.candidateSha256().equals(intent.candidate()) && parents.equals(parents(context.root(),journal.directory))
                && Arrays.equals(intent.marker(),journal.completionBytes(payload.commandBytes())),RUNTIME_MISMATCH);
        context.check(guard,context.frozen());fence(guard,owner,()->null);
    }
    private static RssJournalFiles.Evidence reopen(InstallationHostGuard guard,NarrowContentEvidenceContext context,RssJournalFiles journal,
                                                   NarrowContentFetchReservation.Payload payload,Intent intent,Map<Path,Stamp> parents,Map<String,Stamp> published,Consumer<Boundary> fault){
        ContentEvidenceBudget.inspectFetchCompletion(context,guard,journal.directory);
        require(parents.equals(parents(context.root(),journal.directory)) && Arrays.equals(intent.marker(),journal.read("complete.json")),RUNTIME_MISMATCH);
        NarrowContentFetchCandidateVerification.metadataSchema(journal.read("metadata.json"));
        PreflightFiles.canonical(journal.read("descriptor.json"),RssJournalFiles.SMALL);
        require(markerCandidate(intent.marker(),payload.commandBytes()).equals(intent.candidate()),RUNTIME_MISMATCH);
        var originalFiles=completionStamps(journal.directory);
        require(published!=null && published.equals(originalFiles),RUNTIME_MISMATCH);
        var evidence=journal.reopen(payload.commandBytes(),intent.completion());
        fault.accept(Boundary.JOURNAL_REOPENED);
        require(originalFiles.equals(completionStamps(journal.directory)),RUNTIME_MISMATCH);
        // Reopen binds every original candidate byte/stamp and directory identity to the exact planned marker.
        ContentEvidenceBudget.inspectFetchCompletion(context,guard,journal.directory);
        require(parents.equals(parents(context.root(),journal.directory)) && Arrays.equals(intent.marker(),journal.read("complete.json"))
                && originalFiles.equals(completionStamps(journal.directory)),RUNTIME_MISMATCH);
        return evidence;
    }
    private <T>T fence(InstallationHostGuard guard,WorkerRepository.Owner owner,java.util.function.Supplier<T> action){return workers.narrowOperationTransaction(guard,owner,action);}
    private static Map<Path,Stamp> parents(Path root,Path directory){
        var result=new HashMap<Path,Stamp>();
        try{
            for(Path p:List.of(root,root.resolve(ContentEvidenceBudget.MANIFEST),root.resolve(ContentEvidenceBudget.LOCK),directory.getParent().getParent(),directory.getParent(),directory)){
                var a=Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);PreflightFiles.privateEntry(p,a.isDirectory());
                // Only this directory's entry metadata may change when the reserved marker is published.
                result.put(p,new Stamp(InstallationHostGuard.identity(p),p.equals(directory)?null:a.lastModifiedTime().toString(),p.equals(directory)?0:a.size()));
            }
            return Map.copyOf(result);
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
    }
    private static Map<String,Stamp> completionStamps(Path directory){
        var result=new HashMap<String,Stamp>();
        try{
            var names=new HashSet<>(RssJournalFiles.CANDIDATE);names.add("complete.json");
            for(String name:names){
                Path p=directory.resolve(name);var a=Files.readAttributes(p,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);PreflightFiles.privateEntry(p,false);
                result.put(name,new Stamp(InstallationHostGuard.identity(p),a.lastModifiedTime().toString(),a.size()));
            }
            return Map.copyOf(result);
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
    }
    static String markerCandidate(byte[] bytes,byte[] command){
        var m=PreflightFiles.canonical(bytes,RssJournalFiles.SMALL);SourceJson.fields(m,"journalCompletionVersion","directoryIdentity","files");
        require(m.get("journalCompletionVersion").isIntegralNumber() && m.get("journalCompletionVersion").canConvertToInt() && m.get("journalCompletionVersion").intValue()==1
                && m.get("directoryIdentity").isString() && m.get("directoryIdentity").stringValue().matches("[0-9]+:[0-9]+")
                && m.get("files").isArray() && m.get("files").size()==5,RUNTIME_MISMATCH);
        var inventory=SourceJson.object();var names=new ArrayList<String>();
        for(var f:m.get("files")){
            SourceJson.fields(f,"name","byteLength","sha256","stamp");String name=SourceJson.text(f.get("name"));names.add(name);
            require(RssJournalFiles.CANDIDATE.contains(name) && f.get("byteLength").isIntegralNumber() && f.get("byteLength").canConvertToInt()
                    && f.get("byteLength").intValue()>=0 && f.get("byteLength").intValue()<=(name.equals("payload.xml")?RssJournalFiles.PAYLOAD:RssJournalFiles.SMALL)
                    && f.get("sha256").isString() && f.get("sha256").stringValue().matches("[0-9a-f]{64}"),RUNTIME_MISMATCH);
            stamp(f.get("stamp"),f.get("byteLength").longValue());
            if(name.equals("command.json"))require(f.get("byteLength").intValue()==command.length && f.get("sha256").stringValue().equals(SourceJson.sha(command)),RUNTIME_MISMATCH);
            inventory.putObject(name).put("bytes",f.get("byteLength").longValue()).put("sha256",f.get("sha256").stringValue());
        }
        require(names.equals(new ArrayList<>(new TreeSet<>(RssJournalFiles.CANDIDATE))),RUNTIME_MISMATCH);
        return SourceJson.sha(SourceJson.canonical(inventory));
    }
    private static void stamp(JsonNode n,long size){
        SourceJson.fields(n,"identity","modified","size");
        require(n.get("identity").isString() && n.get("identity").stringValue().matches("[0-9]+:[0-9]+")
                && n.get("modified").isString() && !n.get("modified").stringValue().isEmpty() && n.get("modified").stringValue().length()<=64
                && n.get("size").isIntegralNumber() && n.get("size").canConvertToLong() && n.get("size").longValue()==size,RUNTIME_MISMATCH);
    }
    private static Intent exactIntent(JdbcTemplate jdbc,Intent expected){
        var row=one(jdbc.queryForList("SELECT * FROM ddip_management.content_fetch_completion_intents WHERE operation_id=?",expected.operation()));
        require(expected.candidate().equals(row.get("candidate_sha256")) && expected.completion().equals(row.get("completion_sha256"))
                && Arrays.equals(expected.marker(),(byte[])row.get("completion_bytes")),RUNTIME_MISMATCH);return expected;
    }
    private static boolean confirm(JdbcTemplate jdbc,ManagementTransactions tx,Intent intent,RssJournalFiles.Evidence expected){
        return tx.required(()->{
            exactIntent(jdbc,intent);var rows=jdbc.queryForList("SELECT * FROM ddip_management.content_fetch_checkpoints WHERE operation_id=?",intent.operation());
            if(rows.isEmpty())return false;var r=one(rows);
            require(expected.address().equals(r.get("journal_address")) && intent.completion().equals(r.get("completion_sha256"))
                    && Arrays.equals(expected.descriptorBytes(),(byte[])r.get("journal_descriptor")) && Arrays.equals(expected.checkpointReference(),(byte[])r.get("expected_evidence"))
                    && SourceJson.sha(expected.descriptorBytes()).equals(r.get("journal_sha256")) && SourceJson.sha(expected.checkpointReference()).equals(r.get("expected_sha256")),RUNTIME_MISMATCH);
            return true;
        });
    }
    /** Historical SQL-only inspection. No filesystem adoption, live Owner, runtime loading or repair. */
    static Optional<Observation> inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){
        require(jdbc!=null && tx!=null && operation!=null,WORKER_CONFIGURATION_INVALID);
        return tx.required(()->{
            var operationFacts=NarrowContentFetchReservation.inspect(jdbc,tx,operation);
            var launch=NarrowContentFetchLaunch.inspect(jdbc,tx,operation);
            var intents=jdbc.queryForList("SELECT * FROM ddip_management.content_fetch_completion_intents WHERE operation_id=?",operation);
            if(intents.isEmpty())return Optional.empty();var i=one(intents);
            byte[] command=jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_native_operations WHERE operation_id=?",byte[].class,operation);
            byte[] marker=(byte[])i.get("completion_bytes");String completion=(String)i.get("completion_sha256");
            require(launch.phases().equals(NarrowContentFetchLaunch.PHASES) && launch.candidateSha256().equals(i.get("candidate_sha256"))
                    && completion.equals(SourceJson.sha(marker)) && markerCandidate(marker,command).equals(launch.candidateSha256()),RUNTIME_MISMATCH);
            var rows=jdbc.queryForList("SELECT * FROM ddip_management.content_fetch_checkpoints WHERE operation_id=?",operation);
            if(rows.isEmpty())return Optional.empty();var r=one(rows);
            byte[] descriptor=(byte[])r.get("journal_descriptor"),reference=(byte[])r.get("expected_evidence");
            var d=PreflightFiles.canonical(descriptor,RssJournalFiles.SMALL);var ref=PreflightFiles.canonical(reference,RssJournalFiles.SMALL);
            var c=PreflightFiles.canonical(command,RssJournalFiles.SMALL);
            descriptor(d,c);
            require(operationFacts.checkpoint().equals(r.get("checkpoint_id")) && completion.equals(r.get("completion_sha256"))
                    && SourceJson.sha(descriptor).equals(r.get("journal_sha256")) && SourceJson.sha(reference).equals(r.get("expected_sha256")),RUNTIME_MISMATCH);
            SourceJson.fields(ref,"completionSha256","completionStamp","bronze");
            require(ref.get("completionSha256").isString() && ref.get("completionSha256").stringValue().equals(completion) && ref.get("bronze").equals(d.get("expected")),RUNTIME_MISMATCH);
            stamp(ref.get("completionStamp"),marker.length);
            for(String key:List.of("runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId","revision","capturedConfigSha256","endpointSha256","runtimeSha256"))require(c.get(key).equals(d.get(key)),RUNTIME_MISMATCH);
            require(d.get("commandSha256").stringValue().equals(SourceJson.sha(command)) && d.get("payloadByteLength").longValue()==((Number)r.get("payload_length")).longValue()
                    && d.get("payloadSha256").stringValue().equals(r.get("payload_sha256")),RUNTIME_MISMATCH);
            for(var f:SourceJson.parse(marker).get("files")){
                String name=f.get("name").stringValue();
                if(name.equals("descriptor.json"))require(f.get("sha256").stringValue().equals(SourceJson.sha(descriptor)) && f.get("byteLength").longValue()==descriptor.length,RUNTIME_MISMATCH);
                if(name.equals("metadata.json"))require(f.get("sha256").equals(d.get("expected").get("metadataSha256")) && f.get("byteLength").equals(d.get("expected").get("metadataByteLength")),RUNTIME_MISMATCH);
                if(name.equals("payload.xml"))require(f.get("sha256").equals(d.get("payloadSha256")) && f.get("byteLength").equals(d.get("payloadByteLength")),RUNTIME_MISMATCH);
            }
            UUID run=UUID.fromString(c.get("runId").stringValue());
            String address="runs/"+run+"/attempts/"+operationFacts.attempt()+"/fetch-source/"+c.get("sourceId").stringValue()+"/"+operationFacts.checkpoint();
            require(address.equals(r.get("journal_address")),RUNTIME_MISMATCH);
            return Optional.of(new Observation(operation,run,operationFacts.attempt(),operationFacts.checkpoint(),launch.candidateSha256(),completion));
        });
    }
    private static void descriptor(JsonNode d,JsonNode c){
        SourceJson.fields(d,"journalVersion","commandSha256","runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId","revision","capturedConfigSha256","endpointSha256","runtimeSha256","ingestionId","ingestedAt","httpStatus","contentType","declaredContentLength","payloadByteLength","payloadSha256","expected");
        String ingestion=SourceJson.text(d.get("ingestionId")),at=SourceJson.text(d.get("ingestedAt"));
        require(d.get("journalVersion").isIntegralNumber() && d.get("journalVersion").canConvertToInt() && d.get("journalVersion").intValue()==1
                && UUID.fromString(ingestion).toString().equals(ingestion) && java.time.Instant.parse(at).toString().equals(at) && at.matches("[0-9]{4}-.*Z")
                && d.get("httpStatus").isIntegralNumber() && d.get("httpStatus").canConvertToInt() && d.get("httpStatus").intValue()>=200 && d.get("httpStatus").intValue()<300
                && (d.get("contentType").isNull() || d.get("contentType").isString())
                && (d.get("declaredContentLength").isNull() || d.get("declaredContentLength").isIntegralNumber() && d.get("declaredContentLength").canConvertToLong() && d.get("declaredContentLength").longValue()>=0),RUNTIME_MISMATCH);
        var e=d.get("expected");SourceJson.fields(e,"payloadKey","metadataKey","metadataByteLength","metadataSha256");
        String prefix="bronze/rss/source_id="+Base64.getUrlEncoder().withoutPadding().encodeToString(c.get("sourceId").stringValue().getBytes(java.nio.charset.StandardCharsets.UTF_8))
                +"/ingestion_date="+java.time.Instant.parse(at).atOffset(java.time.ZoneOffset.UTC).toLocalDate()+"/ingestion_id="+ingestion+"/";
        require(e.get("payloadKey").isString() && e.get("payloadKey").stringValue().equals(prefix+"payload.xml")
                && e.get("metadataKey").isString() && e.get("metadataKey").stringValue().equals(prefix+"metadata.json")
                && e.get("metadataByteLength").isIntegralNumber() && e.get("metadataByteLength").canConvertToInt() && e.get("metadataByteLength").intValue()>=0 && e.get("metadataByteLength").intValue()<=RssJournalFiles.SMALL
                && e.get("metadataSha256").isString() && e.get("metadataSha256").stringValue().matches("[0-9a-f]{64}"),RUNTIME_MISMATCH);
    }
    private static Map<String,Object> one(List<Map<String,Object>> rows){require(rows.size()==1,RUNTIME_MISMATCH);return rows.getFirst();}
}
