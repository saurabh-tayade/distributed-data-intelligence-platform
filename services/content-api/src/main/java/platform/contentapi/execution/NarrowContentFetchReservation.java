package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive, one-shot native FETCH_SOURCE/0 reservation. No file, process or outcome capability. */
final class NarrowContentFetchReservation {
    enum Boundary { INSPECTED, RESERVED }
    /** Historical consumption facts only, including after uncertain commit or writer death. */
    record Facts(UUID operation,UUID attempt,UUID checkpoint,String commandSha256,String bootstrapSha256) {
        boolean nativeLaunchCompatible(){return false;}
        @Override public String toString(){return "NarrowContentFetchReservationFacts[non-launchable]";}
    }
    /** Reservation material, NOT a child packet: no child identity or release authority. */
    record Payload(UUID operation,UUID attempt,UUID checkpoint,byte[] commandBytes,byte[] bootstrapBytes) {
        Payload {commandBytes=commandBytes.clone();bootstrapBytes=bootstrapBytes.clone();}
        @Override public byte[] commandBytes(){return commandBytes.clone();}
        @Override public byte[] bootstrapBytes(){return bootstrapBytes.clone();}
        @Override public String toString(){return "NarrowContentFetchReservationPayload[redacted; non-launchable]";}
    }
    record Identity(UUID installation,UUID instance,long epoch,UUID run,UUID attempt) {
        Identity {require(installation!=null && instance!=null && epoch>0 && run!=null && attempt!=null,RUNTIME_MISMATCH);}
        @Override public String toString(){return "ReservationIdentity[redacted]";}
    }
    record Metadata(NarrowContentEvidenceContext.Expected ready,Identity expected,Identity current) { }
    private final JdbcTemplate jdbc;
    private final ManagementTransactions tx;
    private final WorkerRepository workers;

    NarrowContentFetchReservation(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.workers=Objects.requireNonNull(workers);
        workers.requireAdmissionRepository(jdbc,tx);
    }
    Facts reserve(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation){return reserve(guard,owner,operation,b->{});}
    Facts reserve(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,Consumer<Boundary> fault){
        require(fault!=null,WORKER_CONFIGURATION_INVALID);
        Payload prepared=prepare(guard,owner,operation);
        fault.accept(Boundary.INSPECTED);
        // Filesystem inspection ends before SQL; no external effect follows it.
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        absent(ready);
        workers.narrowOperationTransaction(guard,owner,()->{
            var metadata=metadata(jdbc,owner.attemptId());
            require(ready.equals(metadata.ready()),RUNTIME_MISMATCH);
            Payload current=build(metadata.ready(),NarrowContentRuntimeQualification.inspect(jdbc,tx,metadata.ready()),
                    metadata.expected(),metadata.current(),operation);
            require(same(prepared,current),RUNTIME_MISMATCH);
            // An uncertain commit stops this owner's authorization channel in WorkerRepository.
            // Neither this call nor inspection repairs/retries the consumed reservation.
            jdbc.update("INSERT INTO ddip_management.content_native_operations(operation_id,attempt_id,stage,item_ordinal,checkpoint_id,command_bytes,command_sha256,bootstrap_bytes,bootstrap_sha256) VALUES (?,?,'FETCH_SOURCE',0,?,?,?,?,?)",
                    operation,owner.attemptId(),prepared.checkpoint(),prepared.commandBytes(),SourceJson.sha(prepared.commandBytes()),
                    prepared.bootstrapBytes(),SourceJson.sha(prepared.bootstrapBytes()));
            return null;
        });
        fault.accept(Boundary.RESERVED);
        return inspect(operation);
    }
    /** Read-only construction seam. All identity inputs come from independently read metadata. */
    Payload prepare(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation){
        require(guard!=null && owner!=null && operation!=null,WORKER_CONFIGURATION_INVALID);
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        absent(ready);
        return tx.required(()->{
            var m=metadata(jdbc,owner.attemptId());
            require(ready.equals(m.ready()) && m.current().equals(new Identity(owner.worker().installationId(),owner.worker().instanceId(),
                    owner.worker().epoch(),owner.runId(),owner.attemptId())),RUNTIME_MISMATCH);
            return build(ready,NarrowContentRuntimeQualification.inspect(jdbc,tx,ready),m.expected(),m.current(),operation);
        });
    }
    /** SQL-only historical inspection. Never grants a live owner, path effect or session. */
    Facts inspect(UUID operation){return inspect(jdbc,tx,operation);}
    static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){
        require(operation!=null,WORKER_CONFIGURATION_INVALID);
        return tx.required(()->{
            var row=one(jdbc.queryForList("SELECT attempt_id,stage,item_ordinal,checkpoint_id,command_bytes,command_sha256,bootstrap_bytes,bootstrap_sha256 FROM ddip_management.content_native_operations WHERE operation_id=?",operation));
            var m=metadata(jdbc,(UUID)row.get("attempt_id"));
            var expected=build(m.ready(),NarrowContentRuntimeQualification.inspect(jdbc,tx,m.ready()),m.expected(),m.current(),operation);
            var command=canonical((byte[])row.get("command_bytes"),(String)row.get("command_sha256"),65536);
            var bootstrap=canonical((byte[])row.get("bootstrap_bytes"),(String)row.get("bootstrap_sha256"),65535);
            require("FETCH_SOURCE".equals(row.get("stage")) && ((Number)row.get("item_ordinal")).intValue()==0
                    && expected.checkpoint().equals(row.get("checkpoint_id"))
                    && Arrays.equals(expected.commandBytes(),SourceJson.canonical(command))
                    && Arrays.equals(expected.bootstrapBytes(),SourceJson.canonical(bootstrap)),RUNTIME_MISMATCH);
            return new Facts(operation,expected.attempt(),expected.checkpoint(),(String)row.get("command_sha256"),(String)row.get("bootstrap_sha256"));
        });
    }
    /** Read-only reconstruction against separately established admission provenance. No live owner. */
    static Payload reconstruct(JdbcTemplate jdbc,ManagementTransactions tx,NarrowContentEvidenceContext.Expected ready,UUID operation){
        return tx.required(()->{
            var facts=inspect(jdbc,tx,operation);
            require(facts.attempt().equals(ready.attempt()),RUNTIME_MISMATCH);
            var m=metadata(jdbc,ready.attempt());
            require(ready.equals(m.ready()),RUNTIME_MISMATCH);
            return build(ready,NarrowContentRuntimeQualification.inspect(jdbc,tx,ready),m.expected(),m.current(),operation);
        });
    }
    private static boolean same(Payload a,Payload b){
        return a.operation().equals(b.operation()) && a.attempt().equals(b.attempt()) && a.checkpoint().equals(b.checkpoint())
                && Arrays.equals(a.commandBytes(),b.commandBytes()) && Arrays.equals(a.bootstrapBytes(),b.bootstrapBytes());
    }
    private static void absent(NarrowContentEvidenceContext.Expected ready){
        // Even an empty pre-existing directory cannot be adopted as an original invocation.
        require(Files.notExists(directory(ready),LinkOption.NOFOLLOW_LINKS),WORKER_NOT_READY);
    }
    private static Path directory(NarrowContentEvidenceContext.Expected ready){
        Path root=ready.host().root().resolve("runs/"+ready.run()+"/attempts/"+ready.attempt());
        return ContentOperationPlan.original(root,ready.run(),ready.attempt(),ready.source()).directory("FETCH_SOURCE",0);
    }
    /** Pure constructor: never derive independent expectations from a command under inspection. */
    static Payload build(NarrowContentEvidenceContext.Expected ready,NarrowContentRuntimeQualification.Snapshot registered,
                         Identity expected,Identity current,UUID operation){
        require(ready!=null && registered!=null && expected!=null && current!=null && operation!=null,WORKER_CONFIGURATION_INVALID);
        require(ready.kind()==NarrowContentEvidenceContext.PlanKind.ORIGINAL && expected.equals(current)
                && expected.installation().equals(ready.host().installationId()) && expected.run().equals(ready.run())
                && expected.attempt().equals(ready.attempt()) && registered.run().equals(ready.run())
                && registered.attempt().equals(ready.attempt()),RUNTIME_MISMATCH);
        var frozen=registered.frozenSelection();
        require(SourceJson.sha(SourceJson.canonical(frozen)).equals(ready.frozen().selectionSha256())
                && registered.digest("CONTENT").equals(ready.frozen().runtimeSha256()),RUNTIME_MISMATCH);
        var selected=frozen.get("sources").get(0);var runtime=registered.configuration("FETCH");
        require(ready.source().equals(text(selected,"sourceId")),RUNTIME_MISMATCH);
        UUID checkpoint=UUID.fromString(directory(ready).getFileName().toString());
        var fetch=SourceJson.object().put("revision",selected.get("revision").intValue())
                .put("capturedConfigSha256",text(selected,"configurationSha256")).put("endpointSha256",text(selected,"endpointSha256"))
                .put("feedUri",text(selected.get("endpoint"),"uri")).put("runtimeSha256",registered.digest("FETCH"))
                .put("javaRuntime",text(runtime.get("manifest"),"javaRuntime")).put("testEndpoint",runtime.get("testEndpoint").booleanValue())
                .put("httpTimeoutMillis",runtime.get("httpTimeoutMillis").intValue());
        var command=SourceJson.object().put("protocolVersion",1).put("runId",current.run().toString())
                .put("attemptId",current.attempt().toString()).put("ownerEpoch",current.epoch()).put("stage","FETCH_SOURCE")
                .put("itemOrdinal",0).put("checkpointId",checkpoint.toString()).put("sourceId",ready.source());
        for(String name:fetch.propertyNames())command.set(name,fetch.get(name));
        byte[] commandBytes=SourceJson.canonical(command);require(commandBytes.length<=65536,RUNTIME_MISMATCH);
        String commandSha=SourceJson.sha(commandBytes);
        var e=SourceJson.object().put("installationId",ready.host().installationId().toString()).put("policy",ready.policy())
                .put("planKind","ORIGINAL").put("runId",ready.run().toString()).put("originalAttemptId",ready.attempt().toString())
                .put("sourceId",ready.source()).put("controlRoot",ready.host().root().toString()).put("ledgerSha256",ready.ledgerSha256());
        e.set("controlIdentity",identity(ready.host().rootIdentity()));e.set("rootIdentity",identity(ready.rootIdentity()));
        e.set("lockIdentity",identity(ready.lockIdentity()));e.set("frozen",frozen(ready.frozen()));e.set("writer",writer(expected,checkpoint,commandSha));
        // Current values are independently constructed from SQL runtime/owner observations,
        // never copied from expected or from a supplied bootstrap/command.
        var c=SourceJson.object().put("policy",NarrowContentProfile.POLICY);
        c.set("frozen",SourceJson.object().put("selectionSha256",SourceJson.sha(SourceJson.canonical(frozen)))
                .put("profileSha256",text(frozen.get("executionProfile"),"configurationSha256"))
                .put("runtimeSha256",registered.digest("CONTENT")));
        c.set("writer",writer(current,checkpoint,commandSha));
        require(c.get("frozen").equals(e.get("frozen")) && c.get("writer").equals(e.get("writer")),RUNTIME_MISMATCH);
        var bootstrap=SourceJson.object().put("reservationVersion",1).put("mode","FETCH_SOURCE_RESERVATION").put("operationId",operation.toString());
        bootstrap.set("expected",e);bootstrap.set("current",c);bootstrap.set("fetch",fetch);
        byte[] bootstrapBytes=SourceJson.canonical(bootstrap);require(bootstrapBytes.length<65536,RUNTIME_MISMATCH);
        return new Payload(operation,ready.attempt(),checkpoint,commandBytes,bootstrapBytes);
    }
    /** Immutable enrollment and READY, plus separately observed durable attempt ownership. */
    static Metadata metadata(JdbcTemplate jdbc,UUID attempt){
        try{
            var row=one(jdbc.queryForList("SELECT n.admission_bytes,n.admission_sha256,m.receipt_bytes,m.receipt_sha256,a.run_id,a.instance_id,a.owner_epoch,w.installation_id FROM ddip_management.content_attempt_admissions n JOIN ddip_management.content_admission_milestones m ON m.attempt_id=n.attempt_id AND m.phase='READY' JOIN ddip_management.attempts a ON a.attempt_id=n.attempt_id JOIN ddip_management.worker_instances w ON w.instance_id=a.instance_id WHERE n.attempt_id=?",attempt));
            var n=canonical((byte[])row.get("admission_bytes"),(String)row.get("admission_sha256"),65536);
            var r=canonical((byte[])row.get("receipt_bytes"),(String)row.get("receipt_sha256"),65536);
            require(text(r,"admissionSha256").equals(row.get("admission_sha256")) && text(n,"attemptId").equals(attempt.toString())
                    && text(n,"planKind").equals("ORIGINAL") && text(n,"policy").equals(NarrowContentProfile.POLICY),RUNTIME_MISMATCH);
            var host=n.get("controlBinding");
            var binding=new InstallationHostGuard.Binding(text(host,"bindingId"),uuid(host,"installationId"),Path.of(text(host,"root")),
                    identity(host,"root"),identity(host,"guard"),identity(host,"installation"));
            require(Arrays.equals(binding.descriptor(),SourceJson.canonical(host)) && binding.digest().equals(text(n,"controlBindingSha256")),RUNTIME_MISMATCH);
            var ready=new NarrowContentEvidenceContext.Expected(text(n,"policy"),uuid(n,"runId"),attempt,NarrowContentEvidenceContext.PlanKind.ORIGINAL,
                    text(n,"sourceId"),binding,identity(r,"root"),identity(r,"lock"),new NarrowContentEvidenceContext.FrozenIdentities(
                    text(n,"selectionSha256"),text(n,"profileSha256"),text(n,"runtimeSha256")),text(r,"ledgerSha256"));
            var expected=new Identity(uuid(n,"installationId"),uuid(n,"workerInstanceId"),decimal(n,"ownerEpoch"),uuid(n,"runId"),attempt);
            var current=new Identity((UUID)row.get("installation_id"),(UUID)row.get("instance_id"),((Number)row.get("owner_epoch")).longValue(),(UUID)row.get("run_id"),attempt);
            return new Metadata(ready,expected,current);
        }catch(org.springframework.dao.DataAccessException|WorkerFailure ex){throw ex;}
        catch(RuntimeException ex){throw new WorkerFailure(RUNTIME_MISMATCH);}
    }
    private static ObjectNode identity(InstallationHostGuard.Identity value){return SourceJson.object().put("device",Long.toString(value.device())).put("inode",Long.toString(value.inode()));}
    private static InstallationHostGuard.Identity identity(JsonNode n,String prefix){return new InstallationHostGuard.Identity(decimal(n,prefix+"Device"),decimal(n,prefix+"Inode"));}
    private static ObjectNode frozen(NarrowContentEvidenceContext.FrozenIdentities f){return SourceJson.object().put("selectionSha256",f.selectionSha256()).put("profileSha256",f.profileSha256()).put("runtimeSha256",f.runtimeSha256());}
    private static ObjectNode writer(Identity i,UUID checkpoint,String commandSha){return SourceJson.object().put("installationId",i.installation().toString())
            .put("workerInstanceId",i.instance().toString()).put("epoch",Long.toString(i.epoch())).put("runId",i.run().toString())
            .put("attemptId",i.attempt().toString()).put("stage","FETCH_SOURCE").put("ordinal",0).put("checkpointId",checkpoint.toString()).put("commandSha256",commandSha);}
    private static String text(JsonNode n,String key){return SourceJson.text(n.get(key));}
    private static UUID uuid(JsonNode n,String key){String value=text(n,key);UUID id=UUID.fromString(value);require(id.toString().equals(value),RUNTIME_MISMATCH);return id;}
    private static long decimal(JsonNode n,String key){String value=text(n,key);long number=Long.parseLong(value);require(Long.toString(number).equals(value),RUNTIME_MISMATCH);return number;}
    private static JsonNode canonical(byte[] bytes,String digest,int limit){require(bytes!=null && SourceJson.sha(bytes).equals(digest),RUNTIME_MISMATCH);return PreflightFiles.canonical(bytes,limit);}
    private static Map<String,Object> one(List<Map<String,Object>> rows){require(rows.size()==1,WORKER_NOT_READY);return rows.getFirst();}
}
