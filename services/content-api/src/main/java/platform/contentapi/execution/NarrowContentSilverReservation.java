package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive original-live-owner SQL reservation. No native Silver executable packet or inspection claim. */
final class NarrowContentSilverReservation {
    enum Boundary { INSPECTED, BEFORE_RESERVATION, INSERTED, RESERVED }
    record Facts(UUID operation,UUID bronzeOperation,UUID attempt,UUID checkpoint,UUID requiredInspectionCheckpoint,
                 String commandSha256,String reservationSha256) {
        boolean nativeLaunchCompatible(){return false;}boolean requiresBronzeInspection(){return true;}
        @Override public String toString(){return "NativeSilverReservation[historical; inspection required; non-launchable]";}
    }
    private record Material(byte[] command,byte[] reservation) { }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    NarrowContentSilverReservation(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.workers=Objects.requireNonNull(workers);workers.requireAdmissionRepository(jdbc,tx);
    }
    Facts reserve(InstallationHostGuard guard,NarrowContentBronzeCompletion.LiveCheckpoint live,UUID operation,Consumer<Boundary> fault){
        return reserveThen(guard,live,operation,fault,owner->inspect(jdbc,tx,operation));
    }
    /** Acknowledged one-use live callback; the immutable reservation itself stays non-launchable. */
    <T>T reserveThen(InstallationHostGuard guard,NarrowContentBronzeCompletion.LiveCheckpoint live,UUID operation,
            Consumer<Boundary> fault,java.util.function.Function<WorkerRepository.Owner,T> successor){
        require(guard!=null && live!=null && operation!=null && !operation.equals(live.operation()) && fault!=null
                && !TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        UUID bronze=live.operation();var owner=live.consume(bronze);
        workers.narrowOperationTransaction(guard,owner,()->{unconsumed(owner.attemptId());return null;});
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var material=tx.required(()->material(jdbc,tx,ready,bronze,operation,owner.worker().epoch()));
        // Retained candidate pins and exact published anchor are required. The pre-anchor three-file
        // candidate reader cannot reopen this four-file subtree. No remote verification happens here.
        live.check();storageRoots(material);absent(ready);fault.accept(Boundary.INSPECTED);fault.accept(Boundary.BEFORE_RESERVATION);
        require(ready.equals(NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId())),RUNTIME_MISMATCH);
        live.check();storageRoots(material);absent(ready);
        workers.narrowOperationTransaction(guard,owner,()->{
            unconsumed(owner.attemptId());var current=material(jdbc,tx,ready,bronze,operation,owner.worker().epoch());
            require(Arrays.equals(material.command(),current.command()) && Arrays.equals(material.reservation(),current.reservation()),RUNTIME_MISMATCH);
            jdbc.update("INSERT INTO ddip_management.content_silver_reservations(operation_id,bronze_operation_id,attempt_id,checkpoint_id,command_bytes,command_sha256,reservation_bytes,reservation_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",
                    operation,bronze,ready.attempt(),checkpoint(ready,"SILVER"),material.command(),SourceJson.sha(material.command()),material.reservation(),SourceJson.sha(material.reservation()),
                    owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
            fault.accept(Boundary.INSERTED);return null;
        });
        // No acknowledgement retry or readback restores this consumed live channel.
        fault.accept(Boundary.RESERVED);live.check();return successor.apply(owner);
    }
    private void unconsumed(UUID attempt){require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_silver_reservations WHERE attempt_id=?",Long.class,attempt)==0,WORKER_NOT_READY);}
    static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){
        require(jdbc!=null && tx!=null && operation!=null,WORKER_CONFIGURATION_INVALID);
        return tx.required(()->{
            var row=one(jdbc.queryForList("SELECT s.*,a.run_id,a.owner_epoch FROM ddip_management.content_silver_reservations s JOIN ddip_management.attempts a USING(attempt_id) WHERE s.operation_id=?",operation));
            UUID attempt=(UUID)row.get("attempt_id"),run=(UUID)row.get("run_id"),bronze=(UUID)row.get("bronze_operation_id");
            var ready=NarrowContentAdmission.inspectHistorical(jdbc,run,attempt).ready();require(ready!=null,RUNTIME_MISMATCH);
            var expected=material(jdbc,tx,ready,bronze,operation,((Number)row.get("owner_epoch")).longValue());
            require(checkpoint(ready,"SILVER").equals(row.get("checkpoint_id"))
                    && Arrays.equals(expected.command(),(byte[])row.get("command_bytes")) && SourceJson.sha(expected.command()).equals(row.get("command_sha256"))
                    && Arrays.equals(expected.reservation(),(byte[])row.get("reservation_bytes")) && SourceJson.sha(expected.reservation()).equals(row.get("reservation_sha256")),RUNTIME_MISMATCH);
            return new Facts(operation,bronze,attempt,checkpoint(ready,"SILVER"),checkpoint(ready,"INSPECT_BRONZE"),(String)row.get("command_sha256"),(String)row.get("reservation_sha256"));
        });
    }
    /** SQL-only reconstruction; command is explicitly not a historical executable protocol-v1 command. */
    private static Material material(JdbcTemplate jdbc,ManagementTransactions tx,NarrowContentEvidenceContext.Expected ready,UUID bronze,UUID operation,long epoch){
        require(ready.kind()==NarrowContentEvidenceContext.PlanKind.ORIGINAL && !operation.equals(bronze),RUNTIME_MISMATCH);
        var completed=NarrowContentBronzeCompletion.inspect(jdbc,tx,bronze).orElseThrow(()->new WorkerFailure(WORKER_NOT_READY));
        require(completed.run().equals(ready.run()) && completed.attempt().equals(ready.attempt()) && !operation.equals(completed.fetchOperation()),RUNTIME_MISMATCH);
        var reservation=NarrowContentBronzeReservation.inspect(jdbc,tx,bronze);
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        var silver=registered.configuration("SILVER");var bronzeRuntime=registered.configuration("BRONZE");
        var checkpoint=one(jdbc.queryForList("SELECT * FROM ddip_management.content_bronze_checkpoints WHERE operation_id=?",bronze));
        var observed=one(jdbc.queryForList("SELECT verified_bytes FROM ddip_management.content_bronze_verification_phases WHERE operation_id=? AND phase='OBSERVED'",bronze));
        String verifier=jdbc.queryForObject("SELECT verification_profile_sha256 FROM ddip_management.content_bronze_verification_phases WHERE operation_id=? AND phase='VERIFY_BOUND'",String.class,bronze);
        var fetch=one(jdbc.queryForList("SELECT c.*,o.command_sha256 FROM ddip_management.content_fetch_checkpoints c JOIN ddip_management.content_native_operations o USING(operation_id) WHERE c.operation_id=?",completed.fetchOperation()));
        var descriptor=PreflightFiles.canonical((byte[])fetch.get("journal_descriptor"),65536);
        var bronzeCommand=PreflightFiles.canonical(jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_bronze_reservations WHERE operation_id=?",byte[].class,bronze),65536);
        require(bronzeCommand.get("ownerEpoch").longValue()==epoch,RUNTIME_MISMATCH);
        var command=SourceJson.object().put("silverReservationCommandVersion",1).put("mode","SILVER_RESERVATION")
                .put("runId",ready.run().toString()).put("attemptId",ready.attempt().toString()).put("ownerEpoch",epoch)
                .put("checkpointId",checkpoint(ready,"SILVER").toString()).put("nativeLaunchCompatible",false);
        command.putObject("requiresInspection").put("stage","INSPECT_BRONZE").put("itemOrdinal",0).put("checkpointId",checkpoint(ready,"INSPECT_BRONZE").toString());
        var b=command.putObject("bronze").put("operationId",bronze.toString()).put("checkpointId",completed.checkpoint().toString())
                .put("commandSha256",reservation.commandSha256()).put("reservationSha256",reservation.reservationSha256()).put("candidateSha256",completed.candidateSha256())
                .put("receiptSha256",completed.receiptSha256()).put("verifiedSha256",completed.verifiedSha256())
                .put("verifiedStampSha256",(String)checkpoint.get("verified_stamp_sha256")).put("verificationProfileSha256",verifier);
        b.set("receipt",PreflightFiles.canonical((byte[])checkpoint.get("receipt_bytes"),65536));
        b.set("verified",PreflightFiles.canonical((byte[])observed.get("verified_bytes"),65536));
        b.set("verifiedStamp",PreflightFiles.canonical((byte[])checkpoint.get("verified_stamp_bytes"),65536));
        var f=command.putObject("fetch").put("operationId",completed.fetchOperation().toString()).put("checkpointId",fetch.get("checkpoint_id").toString())
                .put("commandSha256",(String)fetch.get("command_sha256")).put("descriptorSha256",(String)fetch.get("journal_sha256"))
                .put("referenceSha256",(String)fetch.get("expected_sha256")).put("completionSha256",(String)fetch.get("completion_sha256"));
        f.set("ingestionId",descriptor.get("ingestionId"));f.set("ingestedAt",descriptor.get("ingestedAt"));
        command.putObject("runtime").put("silverRuntimeId",registered.id(NarrowContentRuntimeQualification.Family.SILVER)).put("silverRuntimeSha256",registered.digest("SILVER"))
                .put("bronzeRuntimeId",silver.get("bronzeRuntimeId").stringValue()).put("bronzeRuntimeSha256",registered.digest("BRONZE"))
                .put("fetchRuntimeId",bronzeRuntime.get("fetchRuntimeId").stringValue()).put("fetchRuntimeSha256",registered.digest("FETCH")).put("registeredLedgerVersion",1);
        var bindings=command.putObject("bindings");bindings.set("silver",binding(registered,silver.get("outputBindingId").stringValue(),"SILVER"));
        bindings.set("temp",binding(registered,silver.get("tempBindingId").stringValue(),"TEMP"));bindings.set("control",binding(registered,ready.host().id(),"CONTROL"));
        var credential=command.putObject("credential");credential.set("sha256",bronzeCommand.get("storage").get("credentialsSha256"));credential.set("stamp",bronzeCommand.get("storage").get("credentialsStamp"));
        byte[] bytes=SourceJson.canonical(command);require(bytes.length<=65536,RUNTIME_MISMATCH);
        var record=SourceJson.object().put("reservationVersion",1).put("mode","SILVER_RESERVATION").put("operationId",operation.toString())
                .put("bronzeOperationId",bronze.toString()).put("bronzeCheckpointId",completed.checkpoint().toString()).put("fetchOperationId",completed.fetchOperation().toString())
                .put("checkpointId",checkpoint(ready,"SILVER").toString()).put("commandSha256",SourceJson.sha(bytes)).put("policy",ready.policy())
                .put("runId",ready.run().toString()).put("attemptId",ready.attempt().toString()).put("selectionSha256",ready.frozen().selectionSha256())
                .put("profileSha256",ready.frozen().profileSha256()).put("contentRuntimeSha256",ready.frozen().runtimeSha256()).put("silverRuntimeSha256",registered.digest("SILVER"))
                .put("ledgerSha256",ready.ledgerSha256()).put("controlBindingSha256",ready.host().digest());
        byte[] facts=SourceJson.canonical(record);require(facts.length<65536,RUNTIME_MISMATCH);return new Material(bytes,facts);
    }
    private static JsonNode binding(NarrowContentRuntimeQualification.Snapshot runtime,String id,String role){
        var found=new ArrayList<JsonNode>();for(var b:runtime.frozenSelection().get("storageBindings"))if(b.get("bindingId").stringValue().equals(id))found.add(b);
        require(found.size()==1 && found.getFirst().get("role").stringValue().equals(role),RUNTIME_MISMATCH);return found.getFirst();
    }
    private static void storageRoots(Material material){
        var bindings=PreflightFiles.canonical(material.command(),65536).get("bindings");
        for(String role:List.of("silver","temp")){
            var config=bindings.get(role).get("configuration");Path root=Path.of(config.get("root").stringValue());
            try{
                require(root.isAbsolute() && root.equals(root.toRealPath()) && Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS),RUNTIME_MISMATCH);
                var identity=InstallationHostGuard.identity(root);
                require(Long.toString(identity.device()).equals(config.get("rootDevice").stringValue())
                        && Long.toString(identity.inode()).equals(config.get("rootInode").stringValue()),RUNTIME_MISMATCH);
                // Existing Silver storage allows readable output roots; only TEMP is private.
                if(role.equals("temp"))PreflightFiles.privateEntry(root,true);
            }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
        }
    }
    private static UUID checkpoint(NarrowContentEvidenceContext.Expected ready,String stage){return UUID.fromString(new NarrowContentEvidenceContext(ready).originalDirectory(stage,0).getFileName().toString());}
    private static void absent(NarrowContentEvidenceContext.Expected ready){
        var context=new NarrowContentEvidenceContext(ready);
        for(String stage:List.of("SILVER","INSPECT_BRONZE")){
            Path path=context.originalDirectory(stage,0);require(Files.notExists(path,LinkOption.NOFOLLOW_LINKS) && Files.notExists(path.getParent(),LinkOption.NOFOLLOW_LINKS),WORKER_NOT_READY);
        }
    }
    private static Map<String,Object> one(List<Map<String,Object>> rows){require(rows.size()==1,RUNTIME_MISMATCH);return rows.getFirst();}
}
