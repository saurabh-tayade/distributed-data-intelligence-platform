package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive, one-shot ORIGINAL CHECK_INPUTS/0 read-only transport probe, never native execution. */
final class NarrowContentOperationJournal {
    enum Boundary { RESERVED, DIRECTORY_CREATED, COMMAND_WRITTEN, FILES_READY, START_AUTHORIZED,
        CHILD_RECORDED, BEFORE_RELEASE, RELEASE_AUTHORIZED, BEFORE_DELIVERY, PROBE_COMPLETED }
    /** Historical facts only. Incomplete means explicit recovery, never permission to repeat. */
    record Facts(UUID operation,UUID attempt,List<String> phases,InstallationHostGuard.ProcessIdentity child) {
        Facts {phases=List.copyOf(phases);}
        boolean completed(){return phases.contains("PROBE_COMPLETED");}
        @Override public String toString(){return "NarrowContentOperationFacts[redacted]";}
    }
    /** Fixed local probe modules, not a qualified Content producer/runtime registration. */
    record ProbeRuntime(Path python,Path processing) {
        ProbeRuntime {
            require(python!=null && processing!=null && python.isAbsolute() && processing.isAbsolute()
                    && python.normalize().equals(python) && processing.normalize().equals(processing),RUNTIME_MISMATCH);
        }
        @Override public String toString(){return "NarrowContentProbeRuntime[redacted]";}
        List<String> command(){return List.of(python.toString(),"-I","-B","-X","pycache_prefix=/dev/null","-c",
                "import sys; sys.path.insert(0, sys.argv[1]); from narrow_startup_probe import main; sys.argv=sys.argv[:1]; sys.exit(main())",processing.toString());}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    NarrowContentOperationJournal(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.workers=Objects.requireNonNull(workers);
        workers.requireAdmissionRepository(jdbc,tx);
    }
    Facts probe(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,byte[] command,ProbeRuntime runtime){
        return probe(guard,owner,operation,command,runtime,b->{});
    }
    Facts probe(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,byte[] command,ProbeRuntime runtime,Consumer<Boundary> fault){
        require(operation!=null && runtime!=null && fault!=null,WORKER_CONFIGURATION_INVALID);
        PreflightFiles.canonical(command,1024*1024);
        // Read-only prechecks precede reservation. They create no directories or children.
        var expected=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var context=new NarrowContentEvidenceContext(expected);
        Path directory=ContentOperationPlan.original(context.root(),owner.runId(),owner.attemptId(),expected.source()).directory("CHECK_INPUTS",0);
        UUID checkpoint=UUID.fromString(directory.getFileName().toString());
        var base=bootstrap(expected,owner,operation,checkpoint,SourceJson.sha(command));
        byte[] bytes=SourceJson.canonical(base);require(bytes.length<65536,RUNTIME_MISMATCH);
        // Every operation ID and fixed slot is consumed permanently, including uncertain commits.
        fence(guard,owner,()->jdbc.update("INSERT INTO ddip_management.content_operations(operation_id,attempt_id,stage,item_ordinal,checkpoint_id,bootstrap_bytes,bootstrap_sha256) VALUES (?,?,'CHECK_INPUTS',0,?,?,?)",operation,owner.attemptId(),checkpoint,bytes,SourceJson.sha(bytes)));
        fault.accept(Boundary.RESERVED);
        // A fresh reservation cannot adopt even an empty pre-existing operation directory.
        require(!Files.exists(directory,LinkOption.NOFOLLOW_LINKS),WORKER_NOT_READY);
        try{
            for(Path path:List.of(directory.getParent(),directory)){
                check(guard,owner,context); // Last current-owner fence before each namespace effect.
                try{Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}
                catch(FileAlreadyExistsException ex){require(!path.equals(directory),WORKER_NOT_READY);}
                PreflightFiles.privateEntry(path,true);PreparedCommand.forceDirectory(path.getParent());
                fault.accept(Boundary.DIRECTORY_CREATED);
            }
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
        check(guard,owner,context);
        var prepared=PreparedCommand.prepare(guard,directory,command,1024*1024);
        fault.accept(Boundary.COMMAND_WRITTEN);
        checkCommand(guard,owner,context,prepared,directory);
        phase(guard,owner,operation,"FILES_READY",null,null);fault.accept(Boundary.FILES_READY);
        // Retain exact local module bytes across launch; not a claim of native runtime qualification.
        var modules=seal(runtime);
        var current=new WorkerRepository.Owner[]{owner};
        var result=new ReadOnlyVerifierProcess().run(runtime.command(),Map.of(),runtime.processing(),Duration.ofSeconds(15),new ReadOnlyVerifierProcess.BootstrapObserver(){
            public void beforeStart(){
                require(modules.equals(seal(runtime)),RUNTIME_MISMATCH);
                checkCommand(guard,current[0],context,prepared,directory);
                phase(guard,current[0],operation,"START_AUTHORIZED",null,null);fault.accept(Boundary.START_AUTHORIZED);
                check(guard,current[0],context);
            }
            public void started(InstallationHostGuard.ProcessIdentity child){
                phase(guard,current[0],operation,"CHILD_RECORDED",child,null);fault.accept(Boundary.CHILD_RECORDED);
            }
            public byte[] release(InstallationHostGuard.ProcessIdentity child){
                fault.accept(Boundary.BEFORE_RELEASE);
                checkCommand(guard,current[0],context,prepared,directory);
                require(modules.equals(seal(runtime)),RUNTIME_MISMATCH);
                byte[] packet=SourceJson.canonical(base.deepCopy().put("childPid",Long.toString(child.pid())));
                require(packet.length<65536,RUNTIME_MISMATCH);
                phase(guard,current[0],operation,"RELEASE_AUTHORIZED",null,SourceJson.sha(packet));
                fault.accept(Boundary.RELEASE_AUTHORIZED);
                // A stale/cancelled owner after the acknowledged phase must still send no packet.
                check(guard,current[0],context);return packet;
            }
            public void beforeDelivery(){
                fault.accept(Boundary.BEFORE_DELIVERY);
                // This fence runs on the actual pipe writer, after any scheduling delay.
                checkCommand(guard,current[0],context,prepared,directory);
            }
            public void heartbeat(){current[0]=workers.heartbeat(guard,current[0]);check(guard,current[0],context);}
        });
        require(result.exitCode()==0 && result.stdoutBytes()==0 && result.stderrBytes()==0,WORKER_NOT_READY);
        checkCommand(guard,current[0],context,prepared,directory);
        phase(guard,current[0],operation,"PROBE_COMPLETED",null,null);fault.accept(Boundary.PROBE_COMPLETED);
        return inspect(operation);
    }
    Facts inspect(UUID operation){
        require(operation!=null,WORKER_CONFIGURATION_INVALID);
        return tx.required(()->{
            var rows=jdbc.queryForList("SELECT attempt_id,bootstrap_bytes,bootstrap_sha256 FROM ddip_management.content_operations WHERE operation_id=?",operation);
            require(rows.size()==1,WORKER_NOT_READY);
            byte[] reserved=(byte[])rows.getFirst().get("bootstrap_bytes");
            var base=PreflightFiles.canonical(reserved,65536);
            require(SourceJson.sha(reserved).equals(rows.getFirst().get("bootstrap_sha256"))
                    && operation.toString().equals(base.get("operationId").stringValue()),RUNTIME_MISMATCH);
            var phases=jdbc.queryForList("SELECT phase,child_instance_id,child_pid,child_started_at,bootstrap_sha256 FROM ddip_management.content_operation_phases WHERE operation_id=? ORDER BY CASE phase WHEN 'FILES_READY' THEN 1 WHEN 'START_AUTHORIZED' THEN 2 WHEN 'CHILD_RECORDED' THEN 3 WHEN 'RELEASE_AUTHORIZED' THEN 4 ELSE 5 END",operation);
            InstallationHostGuard.ProcessIdentity child=null;
            for(var row:phases)if(row.get("phase").equals("CHILD_RECORDED"))child=new InstallationHostGuard.ProcessIdentity((UUID)row.get("child_instance_id"),((Number)row.get("child_pid")).longValue(),((java.sql.Timestamp)row.get("child_started_at")).toInstant());
            var names=List.of("FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","PROBE_COMPLETED");
            require(phases.size()<=names.size() && phases.stream().map(r->r.get("phase")).toList().equals(names.subList(0,phases.size())),RUNTIME_MISMATCH);
            if(phases.size()>=4){
                require(child!=null,RUNTIME_MISMATCH);
                byte[] packet=SourceJson.canonical(((ObjectNode)base.deepCopy()).put("childPid",Long.toString(child.pid())));
                require(SourceJson.sha(packet).equals(phases.get(3).get("bootstrap_sha256")),RUNTIME_MISMATCH);
            }
            return new Facts(operation,(UUID)rows.getFirst().get("attempt_id"),phases.stream().map(r->(String)r.get("phase")).toList(),child);
        });
    }
    private void check(InstallationHostGuard guard,WorkerRepository.Owner owner,NarrowContentEvidenceContext context){
        context.inspect(guard,context.frozen());
        fence(guard,owner,()->null);
    }
    private void checkCommand(InstallationHostGuard guard,WorkerRepository.Owner owner,NarrowContentEvidenceContext context,PreparedCommand command,Path directory){
        context.inspect(guard,context.frozen());
        command.verify(guard,owner,guard.binding().root().relativize(directory).toString(),command.sha256);
        fence(guard,owner,()->null);
    }
    private <T> T fence(InstallationHostGuard guard,WorkerRepository.Owner owner,java.util.function.Supplier<T> work){return workers.narrowOperationTransaction(guard,owner,work);}
    private void phase(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String phase,InstallationHostGuard.ProcessIdentity child,String digest){
        fence(guard,owner,()->{
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_operations WHERE operation_id=? AND attempt_id=?",Long.class,operation,owner.attemptId())==1,WORKER_NOT_READY);
            return jdbc.update("INSERT INTO ddip_management.content_operation_phases(operation_id,phase,child_instance_id,child_pid,child_started_at,bootstrap_sha256) VALUES (?,?,?,?,?,?)",operation,phase,child==null?null:child.instanceId(),child==null?null:child.pid(),child==null?null:java.sql.Timestamp.from(child.startedAt()),digest);
        });
    }
    static ObjectNode bootstrap(NarrowContentEvidenceContext.Expected e,WorkerRepository.Owner owner,UUID operation,UUID checkpoint,String commandSha){
        require(e.run().equals(owner.runId()) && e.attempt().equals(owner.attemptId()) && e.kind()==NarrowContentEvidenceContext.PlanKind.ORIGINAL,WORKER_CONFIGURATION_INVALID);
        return SourceJson.object().put("bootstrapVersion",1).put("mode","READ_ONLY_PROBE").put("operationId",operation.toString())
                .put("installationId",e.host().installationId().toString()).put("policy",e.policy()).put("planKind","ORIGINAL")
                .put("runId",e.run().toString()).put("attemptId",e.attempt().toString()).put("sourceId",e.source())
                .put("controlRoot",e.host().root().toString()).put("controlDevice",Long.toString(e.host().rootIdentity().device())).put("controlInode",Long.toString(e.host().rootIdentity().inode()))
                .put("rootDevice",Long.toString(e.rootIdentity().device())).put("rootInode",Long.toString(e.rootIdentity().inode()))
                .put("lockDevice",Long.toString(e.lockIdentity().device())).put("lockInode",Long.toString(e.lockIdentity().inode()))
                .put("ledgerSha256",e.ledgerSha256()).put("selectionSha256",e.frozen().selectionSha256()).put("profileSha256",e.frozen().profileSha256())
                .put("runtimeSha256",e.frozen().runtimeSha256()).put("workerInstanceId",owner.worker().instanceId().toString()).put("ownerEpoch",Long.toString(owner.worker().epoch()))
                .put("stage","CHECK_INPUTS").put("ordinal",0).put("checkpointId",checkpoint.toString()).put("commandSha256",commandSha);
    }
    private static Map<String,String> seal(ProbeRuntime runtime){
        require(Files.isExecutable(runtime.python()),RUNTIME_MISMATCH);
        var result=new TreeMap<String,String>();
        for(String name:List.of("narrow_startup_probe.py","narrow_evidence_sink.py","narrow_transport_json.py"))
            result.put(name,SourceJson.sha(PreflightFiles.read(runtime.processing().resolve(name),256*1024,false)));
        return Map.copyOf(result);
    }
}
