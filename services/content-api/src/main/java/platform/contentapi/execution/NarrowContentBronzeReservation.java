package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.*;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive original-live-owner successor reservation. Never a launch packet or legacy delivery. */
final class NarrowContentBronzeReservation {
    enum Boundary { INSPECTED, JOURNAL_REOPENED, BEFORE_RESERVATION, INSERTED, RESERVED }
    record Facts(UUID operation,UUID fetchOperation,UUID attempt,UUID checkpoint,String commandSha256,String reservationSha256) {
        boolean nativeLaunchCompatible(){return false;}
        @Override public String toString(){return "NativeBronzeReservation[historical; non-launchable]";}
    }
    private record Material(NarrowContentEvidenceContext.Expected ready,UUID fetchOperation,UUID fetchCheckpoint,
                            byte[] fetchCommand,byte[] marker,byte[] descriptor,byte[] reference,
                            byte[] command,byte[] reservation,Path credentials,String credentialsSha,JsonNode credentialsStamp) { }
    private record Stamp(InstallationHostGuard.Identity identity,String modified,long size) { }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    NarrowContentBronzeReservation(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.workers=Objects.requireNonNull(workers);
        workers.requireAdmissionRepository(jdbc,tx);
    }
    Facts reserve(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetchOperation,UUID operation){return reserve(guard,owner,fetchOperation,operation,b->{});}
    Facts reserve(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetchOperation,UUID operation,Consumer<Boundary> fault){
        require(guard!=null && owner!=null && fetchOperation!=null && operation!=null && !operation.equals(fetchOperation) && fault!=null
                && !TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        // A historical checkpoint or successful read cannot reopen a stopped authorization channel.
        workers.narrowOperationTransaction(guard,owner,()->{unconsumed(owner.attemptId());return null;});
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var runtime=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        var bronze=runtime.configuration("BRONZE");JsonNode storage=null;
        for(var b:runtime.frozenSelection().get("storageBindings"))if(b.get("bindingId").equals(bronze.get("storageBindingId")))storage=b;
        require(storage!=null,RUNTIME_MISMATCH);
        Path credentials=Path.of(storage.get("configuration").get("credentialsFile").stringValue());
        var credential=credential(credentials);
        var material=tx.required(()->material(jdbc,tx,ready,fetchOperation,operation,owner.worker().epoch(),credential));
        reopen(guard,material,fault);fault.accept(Boundary.INSPECTED);
        fault.accept(Boundary.BEFORE_RESERVATION);
        reopen(guard,material,fault);
        var currentReady=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        require(ready.equals(currentReady),RUNTIME_MISMATCH);
        absent(ready);
        workers.narrowOperationTransaction(guard,owner,()->{
            unconsumed(owner.attemptId());
            var current=material(jdbc,tx,ready,fetchOperation,operation,owner.worker().epoch(),credential);
            require(Arrays.equals(material.command(),current.command()) && Arrays.equals(material.reservation(),current.reservation())
                    && Arrays.equals(material.marker(),current.marker()),RUNTIME_MISMATCH);
            jdbc.update("INSERT INTO ddip_management.content_bronze_reservations(operation_id,fetch_operation_id,attempt_id,checkpoint_id,command_bytes,command_sha256,reservation_bytes,reservation_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    operation,fetchOperation,ready.attempt(),checkpoint(ready),material.command(),SourceJson.sha(material.command()),material.reservation(),SourceJson.sha(material.reservation()),
                    owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
            fault.accept(Boundary.INSERTED);return null;
        });
        // No acknowledgement recovery here: historical inspection never grants another reservation.
        fault.accept(Boundary.RESERVED);return inspect(jdbc,tx,operation);
    }
    private void unconsumed(UUID attempt){require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_bronze_reservations WHERE attempt_id=?",Long.class,attempt)==0,WORKER_NOT_READY);}
    /** SQL-only inspection reconstructs immutable identity; credentials/files and live owners are unnecessary. */
    static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){
        require(operation!=null,WORKER_CONFIGURATION_INVALID);
        return tx.required(()->{
            var row=one(jdbc.queryForList("SELECT b.*,a.run_id,a.owner_epoch FROM ddip_management.content_bronze_reservations b JOIN ddip_management.attempts a USING(attempt_id) WHERE b.operation_id=?",operation));
            UUID attempt=(UUID)row.get("attempt_id"),run=(UUID)row.get("run_id"),fetch=(UUID)row.get("fetch_operation_id");
            var ready=NarrowContentAdmission.inspectHistorical(jdbc,run,attempt).ready();require(ready!=null,RUNTIME_MISMATCH);
            byte[] command=(byte[])row.get("command_bytes"),reservation=(byte[])row.get("reservation_bytes");
            require(SourceJson.sha(command).equals(row.get("command_sha256")) && SourceJson.sha(reservation).equals(row.get("reservation_sha256")),RUNTIME_MISMATCH);
            var c=PreflightFiles.canonical(command,65536);PreflightFiles.canonical(reservation,65535);
            var storage=c.get("storage");var credential=SourceJson.object().put("sha256",SourceJson.text(storage.get("credentialsSha256")));credential.set("stamp",storage.get("credentialsStamp"));
            validateCredential(credential);
            var expected=material(jdbc,tx,ready,fetch,operation,((Number)row.get("owner_epoch")).longValue(),credential);
            require(checkpoint(ready).equals(row.get("checkpoint_id")) && Arrays.equals(command,expected.command()) && Arrays.equals(reservation,expected.reservation()),RUNTIME_MISMATCH);
            return new Facts(operation,fetch,attempt,checkpoint(ready),(String)row.get("command_sha256"),(String)row.get("reservation_sha256"));
        });
    }
    private static Material material(JdbcTemplate jdbc,ManagementTransactions tx,NarrowContentEvidenceContext.Expected ready,
            UUID fetch,UUID operation,long epoch,JsonNode credential){
        require(ready.kind()==NarrowContentEvidenceContext.PlanKind.ORIGINAL,RUNTIME_MISMATCH);validateCredential(credential);
        var completed=NarrowContentFetchCompletion.inspect(jdbc,tx,fetch).orElseThrow(()->new WorkerFailure(WORKER_NOT_READY));
        require(completed.run().equals(ready.run()) && completed.attempt().equals(ready.attempt()),RUNTIME_MISMATCH);
        var payload=NarrowContentFetchReservation.reconstruct(jdbc,tx,ready,fetch);
        var fetchCommand=PreflightFiles.canonical(payload.commandBytes(),65536);
        require(fetchCommand.get("ownerEpoch").longValue()==epoch,RUNTIME_MISMATCH);
        var row=one(jdbc.queryForList("SELECT c.*,i.completion_bytes FROM ddip_management.content_fetch_checkpoints c JOIN ddip_management.content_fetch_completion_intents i USING(operation_id) WHERE c.operation_id=?",fetch));
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);var bronze=registered.configuration("BRONZE");JsonNode storage=null;
        for(var b:registered.frozenSelection().get("storageBindings"))if(b.get("bindingId").equals(bronze.get("storageBindingId")))storage=b;
        require(storage!=null && storage.get("role").stringValue().equals("BRONZE"),RUNTIME_MISMATCH);
        byte[] descriptor=(byte[])row.get("journal_descriptor"),reference=(byte[])row.get("expected_evidence"),marker=(byte[])row.get("completion_bytes");
        var command=SourceJson.object().put("protocolVersion",1).put("runId",ready.run().toString()).put("attemptId",ready.attempt().toString())
                .put("ownerEpoch",epoch).put("stage","STORE_BRONZE").put("itemOrdinal",0).put("checkpointId",checkpoint(ready).toString()).put("mode","PUBLISH").put("runtimeSha256",registered.digest("BRONZE"));
        var journal=command.putObject("journal").put("directory",ready.host().root().resolve((String)row.get("journal_address")).toString())
                .put("commandSha256",SourceJson.sha(payload.commandBytes())).put("descriptorSha256",SourceJson.sha(descriptor));journal.set("reference",SourceJson.parse(reference));
        var target=command.putObject("storage").put("bindingId",storage.get("bindingId").stringValue()).put("bindingSha256",storage.get("configurationSha256").stringValue())
                .put("credentialsSha256",credential.get("sha256").stringValue());target.set("configuration",storage.get("configuration"));target.set("credentialsStamp",credential.get("stamp"));
        byte[] bytes=SourceJson.canonical(command);require(bytes.length<=65536,RUNTIME_MISMATCH);
        var reservation=SourceJson.object().put("reservationVersion",1).put("mode","STORE_BRONZE_RESERVATION").put("operationId",operation.toString())
                .put("fetchOperationId",fetch.toString()).put("fetchCheckpointId",completed.checkpoint().toString()).put("fetchCommandSha256",SourceJson.sha(payload.commandBytes()))
                .put("fetchJournalSha256",SourceJson.sha(descriptor)).put("fetchReferenceSha256",SourceJson.sha(reference)).put("fetchCompletionSha256",completed.completionSha256())
                .put("policy",ready.policy()).put("runId",ready.run().toString()).put("attemptId",ready.attempt().toString()).put("checkpointId",checkpoint(ready).toString())
                .put("commandSha256",SourceJson.sha(bytes)).put("selectionSha256",ready.frozen().selectionSha256()).put("profileSha256",ready.frozen().profileSha256())
                .put("contentRuntimeSha256",ready.frozen().runtimeSha256()).put("bronzeRuntimeSha256",registered.digest("BRONZE"))
                .put("ledgerSha256",ready.ledgerSha256()).put("controlBindingSha256",ready.host().digest());
        byte[] facts=SourceJson.canonical(reservation);require(facts.length<65536,RUNTIME_MISMATCH);
        return new Material(ready,fetch,completed.checkpoint(),payload.commandBytes(),marker,descriptor,reference,bytes,facts,
                Path.of(storage.get("configuration").get("credentialsFile").stringValue()),credential.get("sha256").stringValue(),credential.get("stamp"));
    }
    private static void reopen(InstallationHostGuard guard,Material m,Consumer<Boundary> fault){
        var ready=m.ready();var context=new NarrowContentEvidenceContext(ready);context.check(guard,ready.frozen());absent(ready);
        Path directory=ContentOperationPlan.original(context.root(),ready.run(),ready.attempt(),ready.source()).directory("FETCH_SOURCE",0);
        ContentEvidenceBudget.inspectFetchCompletion(context,guard,directory);var before=stamps(context.root(),directory);
        var journal=new RssJournalFiles(guard,ready.run(),ready.attempt(),m.fetchCheckpoint(),ready.source(),guard.binding().root().relativize(directory).toString(),false);
        require(Arrays.equals(m.marker(),journal.read("complete.json")),RUNTIME_MISMATCH);
        NarrowContentFetchCandidateVerification.metadataSchema(journal.read("metadata.json"));PreflightFiles.canonical(journal.read("descriptor.json"),65536);
        var evidence=journal.reopen(m.fetchCommand(),SourceJson.sha(m.marker()));fault.accept(Boundary.JOURNAL_REOPENED);
        require(Arrays.equals(m.descriptor(),evidence.descriptorBytes()) && Arrays.equals(m.reference(),evidence.checkpointReference()),RUNTIME_MISMATCH);
        ContentEvidenceBudget.inspectFetchCompletion(context,guard,directory);
        var finalEvidence=journal.reopen(m.fetchCommand(),SourceJson.sha(m.marker()));
        require(Arrays.equals(m.descriptor(),finalEvidence.descriptorBytes()) && Arrays.equals(m.reference(),finalEvidence.checkpointReference())
                && before.equals(stamps(context.root(),directory)),RUNTIME_MISMATCH);
        var credential=credential(m.credentials());require(m.credentialsSha().equals(credential.get("sha256").stringValue()) && m.credentialsStamp().equals(credential.get("stamp")),RUNTIME_MISMATCH);
        absent(ready);context.check(guard,ready.frozen());
    }
    private static JsonNode credential(Path path){
        try{
            require(path.isAbsolute() && path.normalize().equals(path),RUNTIME_MISMATCH);PreflightFiles.privateEntry(path,false);
            require(Files.getPosixFilePermissions(path).equals(PosixFilePermissions.fromString("rw-------")),RUNTIME_MISMATCH);
            var before=stamp(path);byte[] bytes=PreflightFiles.read(path,8192,true);var value=SourceJson.parse(bytes);SourceJson.fields(value,"accessKey","secretKey");
            for(String key:List.of("accessKey","secretKey")){String v=SourceJson.text(value.get(key));require(!v.isBlank() && v.getBytes(java.nio.charset.StandardCharsets.UTF_8).length<=4096 && v.codePoints().noneMatch(Character::isISOControl),RUNTIME_MISMATCH);}
            require(before.equals(stamp(path)),RUNTIME_MISMATCH);
            var result=SourceJson.object().put("sha256",SourceJson.sha(bytes));result.set("stamp",before);validateCredential(result);return result;
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
    }
    private static void validateCredential(JsonNode c){
        SourceJson.fields(c,"sha256","stamp");require(SourceJson.text(c.get("sha256")).matches("[0-9a-f]{64}"),RUNTIME_MISMATCH);
        var s=c.get("stamp");SourceJson.fields(s,"identity","modified","size");
        require(SourceJson.text(s.get("identity")).matches("[0-9]+:[0-9]+") && !SourceJson.text(s.get("modified")).isEmpty() && SourceJson.text(s.get("modified")).length()<=64
                && s.get("size").isIntegralNumber() && s.get("size").canConvertToLong() && s.get("size").longValue()>0 && s.get("size").longValue()<=8192,RUNTIME_MISMATCH);
    }
    private static JsonNode stamp(Path path)throws java.io.IOException{var id=InstallationHostGuard.identity(path);return SourceJson.object().put("identity",id.device()+":"+id.inode()).put("modified",Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS).toString()).put("size",Files.size(path));}
    private static Map<Path,Stamp> stamps(Path root,Path directory){
        var paths=new ArrayList<>(List.of(root,root.resolve(ContentEvidenceBudget.MANIFEST),root.resolve(ContentEvidenceBudget.LOCK),directory.getParent().getParent(),directory.getParent(),directory));
        for(String name:RssJournalFiles.CANDIDATE)paths.add(directory.resolve(name));paths.add(directory.resolve("complete.json"));var result=new HashMap<Path,Stamp>();
        try{for(Path path:paths){var a=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);PreflightFiles.privateEntry(path,a.isDirectory());result.put(path,new Stamp(InstallationHostGuard.identity(path),a.lastModifiedTime().toString(),a.size()));}return Map.copyOf(result);}
        catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
    }
    private static Path directory(NarrowContentEvidenceContext.Expected ready){Path root=ready.host().root().resolve("runs/"+ready.run()+"/attempts/"+ready.attempt());return ContentOperationPlan.original(root,ready.run(),ready.attempt(),ready.source()).directory("STORE_BRONZE",0);}
    private static UUID checkpoint(NarrowContentEvidenceContext.Expected ready){return UUID.fromString(directory(ready).getFileName().toString());}
    private static void absent(NarrowContentEvidenceContext.Expected ready){require(Files.notExists(directory(ready),LinkOption.NOFOLLOW_LINKS),WORKER_NOT_READY);}
    private static Map<String,Object> one(List<Map<String,Object>> rows){require(rows.size()==1,WORKER_NOT_READY);return rows.getFirst();}
}
