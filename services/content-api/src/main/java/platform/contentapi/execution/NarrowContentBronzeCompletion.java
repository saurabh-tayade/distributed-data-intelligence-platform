package platform.contentapi.execution;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive original-live-owner GET verification and native checkpoint. Never a successor capability. */
final class NarrowContentBronzeCompletion {
    enum Boundary { INVENTORY_READ, VERIFY_BOUND, START_AUTHORIZED, CHILD_RECORDED, RELEASE_AUTHORIZED,
        BEFORE_DELIVERY, HEARTBEAT, REMOTE_VERIFIED, BEFORE_OBSERVED, OBSERVED_INSERTED, OBSERVED_COMMITTED,
        BEFORE_ANCHOR, PREFIX_CREATED, PREFIX_WRITTEN, PREFIX_FORCED, MARKER_RENAMED, DIRECTORY_FORCED,
        ANCHOR_WRITTEN, BEFORE_CHECKPOINT, CHECKPOINT_INSERTED, CHECKPOINT_COMMITTED }
    static final List<String> PHASES=List.of("VERIFY_BOUND","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","OBSERVED");
    record Observation(UUID operation,UUID run,UUID attempt,UUID checkpoint,UUID fetchOperation,
                       String candidateSha256,String receiptSha256,String verifiedSha256) {
        boolean checkpointed(){return true;}boolean nativeLaunchCompatible(){return false;}
        @Override public String toString(){return "NativeBronzeCheckpoint[historical GET observation; no continuation]";}
    }
    private record Stamp(InstallationHostGuard.Identity identity,String modified,long size,String sha256) { }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    NarrowContentBronzeCompletion(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.workers=Objects.requireNonNull(workers);workers.requireAdmissionRepository(jdbc,tx);
    }
    /** Only the private-constructor channel from a just-completed launch can enter. */
    Observation complete(InstallationHostGuard guard,NarrowContentBronzeLaunch.LiveCandidate live,String profile,Consumer<Boundary> fault){
        require(guard!=null && live!=null && fault!=null && !org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        UUID operation=live.operation();var current=new WorkerRepository.Owner[]{live.consume(operation)};
        require(live.candidate().state().equals("EXACT") && live.candidate().exitCode()==0,WORKER_NOT_READY);
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,current[0].runId(),current[0].attemptId());
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        var runtime=StrictBronzeVerificationRuntime.load(jdbc,tx,guard,registered,profile);
        var material=NarrowContentBronzeReservation.reconstruct(jdbc,tx,ready,operation);
        var launch=NarrowContentBronzeLaunch.inspect(jdbc,tx,operation);
        require(launch.phases().equals(NarrowContentBronzeLaunch.PHASES) && launch.attempt().equals(current[0].attemptId())
                && launch.candidateSha256().equals(live.candidate().sha256()) && launch.storageState().equals("EXACT") && launch.exitCode()==0,RUNTIME_MISMATCH);
        var context=new NarrowContentEvidenceContext(ready);Path directory=context.originalDirectory("STORE_BRONZE",0);
        var pins=pins(context,directory);check(guard,current[0],material,runtime,launch,pins,false,false);
        fault.accept(Boundary.INVENTORY_READ);
        var metadata=tx.required(()->NarrowContentFetchReservation.metadata(jdbc,ready.attempt()));
        require(metadata.ready().equals(ready) && metadata.current().equals(new NarrowContentFetchReservation.Identity(
                current[0].worker().installationId(),current[0].worker().instanceId(),current[0].worker().epoch(),ready.run(),ready.attempt())),RUNTIME_MISMATCH);
        byte[] bootstrap=bootstrap(material,registered,metadata.expected(),metadata.current(),runtime.protocolDigest(),launch.candidateSha256());
        check(guard,current[0],material,runtime,launch,pins,false,false);
        phase(guard,current[0],operation,"VERIFY_BOUND",runtime,launch.candidateSha256(),null,null,null,null,()->{});fault.accept(Boundary.VERIFY_BOUND);
        var result=ReadOnlyVerifierProcess.receiptReader().run(runtime.command(),runtime.environment(),guard.binding().root(),Duration.ofSeconds(90),new ReadOnlyVerifierProcess.BootstrapObserver(){
            public void beforeStart(){
                check(guard,current[0],material,runtime,launch,pins,false,false);
                phase(guard,current[0],operation,"START_AUTHORIZED",null,null,null,null,null,null,()->{});fault.accept(Boundary.START_AUTHORIZED);
                check(guard,current[0],material,runtime,launch,pins,false,false);
            }
            public void started(InstallationHostGuard.ProcessIdentity child){
                phase(guard,current[0],operation,"CHILD_RECORDED",null,null,child,null,null,null,()->{});fault.accept(Boundary.CHILD_RECORDED);
            }
            public byte[] release(InstallationHostGuard.ProcessIdentity child){
                check(guard,current[0],material,runtime,launch,pins,false,false);byte[] packet=packet(bootstrap,child.pid());
                phase(guard,current[0],operation,"RELEASE_AUTHORIZED",null,null,null,SourceJson.sha(packet),null,null,()->{});fault.accept(Boundary.RELEASE_AUTHORIZED);
                check(guard,current[0],material,runtime,launch,pins,false,false);return packet;
            }
            public void beforeDelivery(){fault.accept(Boundary.BEFORE_DELIVERY);check(guard,current[0],material,runtime,launch,pins,false,false);}
            public void heartbeat(){
                fault.accept(Boundary.HEARTBEAT);check(guard,current[0],material,runtime,launch,pins,false,false);
                current[0]=workers.heartbeat(guard,current[0]);check(guard,current[0],material,runtime,launch,pins,false,false);
            }
        });
        // A clean publishing candidate is not proof. Only this separately qualified GET result is considered.
        require(result.exitCode()==0 && result.descendants().isEmpty() && NarrowContentBronzeLaunch.stderrAccepted(result)
                && result.stdout()!=null && result.stdoutBytes()>0 && result.stdoutBytes()<=65536 && result.stdout().length==result.stdoutBytes(),WORKER_NOT_READY);
        byte[] receipt=result.stdout();validateReceipt(receipt,material.command(),material.descriptor());
        fault.accept(Boundary.REMOTE_VERIFIED);check(guard,current[0],material,runtime,launch,pins,false,false);
        byte[] anchor=anchor(operation,material,launch.candidateSha256(),runtime.digest(),receipt);
        fault.accept(Boundary.BEFORE_OBSERVED);check(guard,current[0],material,runtime,launch,pins,false,false);
        phase(guard,current[0],operation,"OBSERVED",null,null,null,null,receipt,anchor,()->fault.accept(Boundary.OBSERVED_INSERTED));
        // An uncertain phase acknowledgement stops here. No readback can authorize the anchor.
        fault.accept(Boundary.OBSERVED_COMMITTED);fault.accept(Boundary.BEFORE_ANCHOR);
        check(guard,current[0],material,runtime,launch,pins,false,false);
        var published=new java.util.concurrent.atomic.AtomicReference<JsonNode>();
        var preparation=new java.util.concurrent.atomic.AtomicReference<InstallationHostGuard.Identity>();
        PreparedCommand.writeOnce(directory,"verified.json",anchor,65536,b->{
            Path active=directory.resolve(b==PreparedCommand.WriteBoundary.MARKER_RENAMED || b==PreparedCommand.WriteBoundary.DIRECTORY_FORCED?"verified.json":"verified.json.preparing");
            if(b==PreparedCommand.WriteBoundary.PREFIX_CREATED)preparation.set(InstallationHostGuard.identity(active));
            require(preparation.get().equals(InstallationHostGuard.identity(active)),RUNTIME_MISMATCH);
            if(b==PreparedCommand.WriteBoundary.MARKER_RENAMED)published.set(stamp(active));
            fault.accept(Boundary.valueOf(b.name()));
            require(preparation.get().equals(InstallationHostGuard.identity(active)),RUNTIME_MISMATCH);
            if(b!=PreparedCommand.WriteBoundary.PREFIX_CREATED)require(Arrays.equals(anchor,PreflightFiles.read(active,65536,true)),RUNTIME_MISMATCH);
            check(guard,current[0],material,runtime,launch,pins,true,true);
            if(published.get()!=null)anchor(directory,anchor,published.get());
        });
        fault.accept(Boundary.ANCHOR_WRITTEN);
        check(guard,current[0],material,runtime,launch,pins,true,false);anchor(directory,anchor,published.get());
        fault.accept(Boundary.BEFORE_CHECKPOINT);
        check(guard,current[0],material,runtime,launch,pins,true,false);anchor(directory,anchor,published.get());
        byte[] stamp=SourceJson.canonical(published.get());UUID checkpoint=UUID.fromString(directory.getFileName().toString());
        try{
            workers.narrowOperationTransaction(guard,current[0],()->{
                jdbc.update("INSERT INTO ddip_management.content_bronze_checkpoints(operation_id,checkpoint_id,fetch_operation_id,candidate_sha256,receipt_bytes,receipt_sha256,verified_sha256,verified_stamp_bytes,verified_stamp_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                        operation,checkpoint,material.fetchOperation(),launch.candidateSha256(),receipt,SourceJson.sha(receipt),SourceJson.sha(anchor),stamp,SourceJson.sha(stamp),
                        current[0].worker().coordinatorVersion(),current[0].worker().workerVersion(),current[0].runVersion(),current[0].attemptVersion());
                fault.accept(Boundary.CHECKPOINT_INSERTED);return null;
            });
        }catch(ManagementFailure ex){
            if(ex.code()!=ManagementFailure.Code.MANAGEMENT_UNAVAILABLE || !confirm(operation,checkpoint,material.fetchOperation(),launch.candidateSha256(),receipt,anchor,stamp,current[0]))throw ex;
            // Read-only acknowledgement confirmation returns facts; the owner's channel remains stopped.
            return inspect(jdbc,tx,operation).orElseThrow(()->new WorkerFailure(RUNTIME_MISMATCH));
        }
        fault.accept(Boundary.CHECKPOINT_COMMITTED);
        require(confirm(operation,checkpoint,material.fetchOperation(),launch.candidateSha256(),receipt,anchor,stamp,current[0]),RUNTIME_MISMATCH);
        return inspect(jdbc,tx,operation).orElseThrow(()->new WorkerFailure(RUNTIME_MISMATCH));
    }
    private void check(InstallationHostGuard guard,WorkerRepository.Owner owner,NarrowContentBronzeReservation.Material material,
                       StrictBronzeVerificationRuntime runtime,NarrowContentBronzeLaunch.Facts launch,Map<Path,Stamp> pins,boolean anchor,boolean locked){
        runtime.check();
        if(locked)NarrowContentBronzeReservation.reopenDuringCheckpointWrite(guard,material);
        else if(anchor)NarrowContentBronzeReservation.reopenForCheckpoint(guard,material);
        else NarrowContentBronzeReservation.reopenForLaunch(guard,material,true);
        var context=new NarrowContentEvidenceContext(material.ready());Path directory=context.originalDirectory("STORE_BRONZE",0);
        require(pins.equals(pins(context,directory)),RUNTIME_MISMATCH);
        if(!anchor)require(new NarrowContentBronzeLaunch.Candidate(launch.candidateSha256(),"EXACT",0)
                .equals(NarrowContentBronzeLaunch.candidate(directory,material.command(),material.descriptor(),0)),RUNTIME_MISMATCH);
        workers.narrowOperationTransaction(guard,owner,()->null);
    }
    private void phase(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String phase,StrictBronzeVerificationRuntime runtime,
                       String candidate,InstallationHostGuard.ProcessIdentity child,String bootstrap,byte[] receipt,byte[] anchor,Runnable inserted){
        workers.narrowOperationTransaction(guard,owner,()->{
            jdbc.update("INSERT INTO ddip_management.content_bronze_verification_phases(operation_id,phase,verification_profile_id,verification_profile_sha256,candidate_sha256,child_instance_id,child_pid,child_started_at,bootstrap_sha256,receipt_bytes,receipt_sha256,verified_bytes,verified_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    operation,phase,runtime==null?null:runtime.id(),runtime==null?null:runtime.digest(),candidate,child==null?null:child.instanceId(),child==null?null:child.pid(),child==null?null:java.sql.Timestamp.from(child.startedAt()),
                    bootstrap,receipt,receipt==null?null:SourceJson.sha(receipt),anchor,anchor==null?null:SourceJson.sha(anchor),
                    owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());inserted.run();return null;
        });
    }
    static byte[] bootstrap(NarrowContentBronzeReservation.Material m,NarrowContentRuntimeQualification.Snapshot registered,
            NarrowContentFetchReservation.Identity expected,NarrowContentFetchReservation.Identity current,String strictDigest,String candidate){
        require(candidate!=null && candidate.matches("[0-9a-f]{64}"),RUNTIME_MISMATCH);
        var result=(ObjectNode)SourceJson.parse(NarrowContentBronzeLaunch.bootstrap(m,registered,expected,current,strictDigest));
        result.put("mode","STORE_BRONZE_VERIFY").put("candidateSha256",candidate);byte[] bytes=SourceJson.canonical(result);require(bytes.length<65500,RUNTIME_MISMATCH);return bytes;
    }
    static byte[] packet(byte[] bootstrap,long pid){
        require(pid>0,RUNTIME_MISMATCH);var value=PreflightFiles.canonical(bootstrap,65535);
        SourceJson.fields(value,"bootstrapVersion","mode","strictRuntimeSha256","expected","current","bronze","candidateSha256");
        require(value.get("bootstrapVersion").isInt() && value.get("bootstrapVersion").intValue()==1
                && value.get("mode").stringValue().equals("STORE_BRONZE_VERIFY") && value.get("candidateSha256").stringValue().matches("[0-9a-f]{64}"),RUNTIME_MISMATCH);
        var packet=(ObjectNode)value.deepCopy();packet.put("childPid",Long.toString(pid));byte[] bytes=SourceJson.canonical(packet);require(bytes.length<65536,RUNTIME_MISMATCH);return bytes;
    }
    static void validateReceipt(byte[] bytes,byte[] command,byte[] descriptor){
        var receipt=PreflightFiles.canonical(bytes,65536);String type=SourceJson.text(receipt.get("payloadContentType"));
        require(Set.of("application/xml","application/octet-stream").contains(type),RUNTIME_MISMATCH);
        var expected=SourceJson.object().put("receiptVersion",1);var d=PreflightFiles.canonical(descriptor,65536);var c=PreflightFiles.canonical(command,65536);
        for(String key:List.of("runId","attemptId","sourceId","revision","capturedConfigSha256","ingestionId","ingestedAt","payloadByteLength","payloadSha256"))expected.set(key,d.get(key));
        var storage=c.get("storage");expected.put("journalSha256",SourceJson.sha(descriptor)).set("storageBindingId",storage.get("bindingId"));
        expected.set("storageBindingSha256",storage.get("bindingSha256"));expected.set("bucket",storage.get("configuration").get("bucket"));expected.set("expected",d.get("expected"));
        expected.put("payloadContentType",type).put("metadataContentType","application/json");require(Arrays.equals(bytes,SourceJson.canonical(expected)),RUNTIME_MISMATCH);
    }
    private static byte[] anchor(UUID operation,NarrowContentBronzeReservation.Material material,String candidate,String verifier,byte[] receipt){
        var c=PreflightFiles.canonical(material.command(),65536);
        var value=SourceJson.object().put("bronzeCheckpointVersion",1).put("operationId",operation.toString()).put("fetchOperationId",material.fetchOperation().toString())
                .put("checkpointId",c.get("checkpointId").stringValue()).put("candidateSha256",candidate).put("verificationProfileSha256",verifier).put("receiptSha256",SourceJson.sha(receipt));
        value.set("receipt",SourceJson.parse(receipt));byte[] bytes=SourceJson.canonical(value);require(bytes.length<=65536,RUNTIME_MISMATCH);return bytes;
    }
    private static Map<Path,Stamp> pins(NarrowContentEvidenceContext context,Path directory){
        var result=new HashMap<Path,Stamp>();
        for(Path path:List.of(context.root(),context.root().resolve(ContentEvidenceBudget.MANIFEST),context.root().resolve(ContentEvidenceBudget.LOCK),directory.getParent(),directory,
                directory.resolve("command.json"),directory.resolve("evidence.json"),directory.resolve("result.json"))){
            try{
                boolean dir=Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS);PreflightFiles.privateEntry(path,dir);
                var identity=InstallationHostGuard.identity(path);String modified=Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS).toString();long size=Files.size(path);
                String sha=dir?null:SourceJson.sha(PreflightFiles.read(path,path.getFileName().toString().equals(ContentEvidenceBudget.MANIFEST)?256*1024:65536,true));
                require(identity.equals(InstallationHostGuard.identity(path)) && modified.equals(Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS).toString()) && size==Files.size(path),RUNTIME_MISMATCH);
                result.put(path,new Stamp(identity,path.equals(directory)?null:modified,path.equals(directory)?0:size,sha));
            }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
        }
        return Map.copyOf(result);
    }
    private static JsonNode stamp(Path path){
        try{PreflightFiles.privateEntry(path,false);var id=InstallationHostGuard.identity(path);return SourceJson.object().put("identity",id.device()+":"+id.inode())
                .put("modified",Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS).toString()).put("size",Files.size(path));}
        catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
    }
    private static void anchor(Path directory,byte[] expected,JsonNode observed){
        // Active writes admit only the reserved prefix; completion admits exactly the four final files.
        try(var files=Files.list(directory)){
            require(files.limit(5).map(p->p.getFileName().toString()).collect(java.util.stream.Collectors.toSet())
                    .equals(Set.of("command.json","evidence.json","result.json","verified.json")),RUNTIME_MISMATCH);
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
        require(observed!=null && observed.equals(stamp(directory.resolve("verified.json"))) && Arrays.equals(expected,PreflightFiles.read(directory.resolve("verified.json"),65536,true))
                && observed.equals(stamp(directory.resolve("verified.json"))),RUNTIME_MISMATCH);
    }
    private boolean confirm(UUID operation,UUID checkpoint,UUID fetch,String candidate,byte[] receipt,byte[] anchor,byte[] stamp,WorkerRepository.Owner owner){
        return tx.required(()->{
            var rows=jdbc.queryForList("SELECT * FROM ddip_management.content_bronze_checkpoints WHERE operation_id=?",operation);if(rows.isEmpty())return false;require(rows.size()==1,RUNTIME_MISMATCH);var row=rows.getFirst();
            require(checkpoint.equals(row.get("checkpoint_id")) && fetch.equals(row.get("fetch_operation_id")) && candidate.equals(row.get("candidate_sha256"))
                    && Arrays.equals(receipt,(byte[])row.get("receipt_bytes")) && SourceJson.sha(receipt).equals(row.get("receipt_sha256"))
                    && SourceJson.sha(anchor).equals(row.get("verified_sha256")) && Arrays.equals(stamp,(byte[])row.get("verified_stamp_bytes")) && SourceJson.sha(stamp).equals(row.get("verified_stamp_sha256"))
                    && ((Number)row.get("owner_coordinator_version")).longValue()==owner.worker().coordinatorVersion()
                    && ((Number)row.get("owner_worker_version")).longValue()==owner.worker().workerVersion()
                    && ((Number)row.get("owner_run_version")).longValue()==owner.runVersion() && ((Number)row.get("owner_attempt_version")).longValue()==owner.attemptVersion(),RUNTIME_MISMATCH);
            var observed=jdbc.queryForList("SELECT receipt_bytes,verified_bytes FROM ddip_management.content_bronze_verification_phases WHERE operation_id=? AND phase='OBSERVED'",operation);
            require(observed.size()==1 && Arrays.equals(receipt,(byte[])observed.getFirst().get("receipt_bytes")) && Arrays.equals(anchor,(byte[])observed.getFirst().get("verified_bytes")),RUNTIME_MISMATCH);return true;
        });
    }
    /** Historical SQL-only inspection: never installs, invokes GETs, repairs a file or yields an owner. */
    static Optional<Observation> inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){
        require(jdbc!=null && tx!=null && operation!=null,WORKER_CONFIGURATION_INVALID);
        return tx.required(()->{
            var rows=jdbc.queryForList("SELECT * FROM ddip_management.content_bronze_checkpoints WHERE operation_id=?",operation);if(rows.isEmpty())return Optional.empty();require(rows.size()==1,RUNTIME_MISMATCH);var row=rows.getFirst();
            var reservation=NarrowContentBronzeReservation.inspect(jdbc,tx,operation);var launch=NarrowContentBronzeLaunch.inspect(jdbc,tx,operation);
            var ready=NarrowContentFetchReservation.metadata(jdbc,reservation.attempt()).ready();
            var material=NarrowContentBronzeReservation.historicalMaterial(jdbc,tx,ready,operation);
            var phases=jdbc.queryForList("SELECT * FROM ddip_management.content_bronze_verification_phases WHERE operation_id=? ORDER BY CASE phase WHEN 'VERIFY_BOUND' THEN 1 WHEN 'START_AUTHORIZED' THEN 2 WHEN 'CHILD_RECORDED' THEN 3 WHEN 'RELEASE_AUTHORIZED' THEN 4 ELSE 5 END",operation);
            require(phases.stream().map(p->(String)p.get("phase")).toList().equals(PHASES),RUNTIME_MISMATCH);
            var bound=phases.getFirst();String profile=(String)bound.get("verification_profile_id"),digest=(String)bound.get("verification_profile_sha256");
            var profileRows=jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_bronze_verification_profiles WHERE verification_profile_id=?",profile);
            require(profileRows.size()==1,RUNTIME_MISMATCH);byte[] config=(byte[])profileRows.getFirst().get("configuration_bytes");
            require(digest.equals(profileRows.getFirst().get("configuration_sha256")) && digest.equals(SourceJson.sha(config)),RUNTIME_MISMATCH);
            var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);StrictBronzeVerificationRuntime.validate(PreflightFiles.canonical(config,65536),registered,ready.host());
            var metadata=NarrowContentFetchReservation.metadata(jdbc,ready.attempt());
            byte[] bootstrap=bootstrap(material,registered,metadata.expected(),metadata.current(),SourceJson.parse(config).get("strictRuntimeSha256").stringValue(),launch.candidateSha256());
            require(SourceJson.sha(packet(bootstrap,((Number)phases.get(2).get("child_pid")).longValue())).equals(phases.get(3).get("bootstrap_sha256")),RUNTIME_MISMATCH);
            byte[] receipt=(byte[])row.get("receipt_bytes"),stamp=(byte[])row.get("verified_stamp_bytes");validateReceipt(receipt,material.command(),material.descriptor());
            byte[] anchor=anchor(operation,material,launch.candidateSha256(),digest,receipt);var observed=phases.getLast();
            require(reservation.checkpoint().equals(row.get("checkpoint_id")) && reservation.fetchOperation().equals(row.get("fetch_operation_id"))
                    && launch.storageState().equals("EXACT") && launch.candidateSha256().equals(bound.get("candidate_sha256")) && launch.candidateSha256().equals(row.get("candidate_sha256"))
                    && Arrays.equals(receipt,(byte[])observed.get("receipt_bytes")) && SourceJson.sha(receipt).equals(observed.get("receipt_sha256")) && SourceJson.sha(receipt).equals(row.get("receipt_sha256"))
                    && Arrays.equals(anchor,(byte[])observed.get("verified_bytes")) && SourceJson.sha(anchor).equals(observed.get("verified_sha256")) && SourceJson.sha(anchor).equals(row.get("verified_sha256"))
                    && SourceJson.sha(stamp).equals(row.get("verified_stamp_sha256")),RUNTIME_MISMATCH);
            var st=PreflightFiles.canonical(stamp,65536);SourceJson.fields(st,"identity","modified","size");
            require(st.get("identity").isString() && st.get("identity").stringValue().matches("[0-9]+:[0-9]+") && st.get("modified").isString()
                    && !st.get("modified").stringValue().isEmpty() && st.get("modified").stringValue().length()<=64 && st.get("size").isIntegralNumber()
                    && st.get("size").canConvertToLong() && st.get("size").longValue()==anchor.length,RUNTIME_MISMATCH);
            return Optional.of(new Observation(operation,ready.run(),ready.attempt(),reservation.checkpoint(),reservation.fetchOperation(),launch.candidateSha256(),SourceJson.sha(receipt),SourceJson.sha(anchor)));
        });
    }
}
