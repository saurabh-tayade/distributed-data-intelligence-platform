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

/** Inactive versioned native launch seam. No dispatcher, retry or recovered continuation authority. */
final class NarrowContentFetchLaunch {
    enum Boundary { RESERVED, LAUNCH_BOUND, DIRECTORY_CREATED, COMMAND_WRITTEN, FILES_READY, START_AUTHORIZED,
        CHILD_RECORDED, BEFORE_RELEASE, BEFORE_RELEASE_PHASE, RELEASE_AUTHORIZED, BEFORE_DELIVERY, HEARTBEAT, BEFORE_CANDIDATE, BEFORE_CANDIDATE_PHASE, CANDIDATE_EXITED }
    static final List<String> PHASES=List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED");
    record Facts(UUID operation,UUID attempt,List<String> phases,InstallationHostGuard.ProcessIdentity child,
                 String launchProfileId,String launchProfileSha256,String candidateSha256) {
        Facts {phases=List.copyOf(phases);}
        boolean candidateForVerification(){return phases.contains("CANDIDATE_EXITED");}
        boolean completed(){return false;}
        boolean nativeLaunchCompatible(){return false;}
        @Override public String toString(){return "NarrowContentFetchLaunchFacts[candidate only; no continuation]";}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    NarrowContentFetchLaunch(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.workers=Objects.requireNonNull(workers);
        workers.requireAdmissionRepository(jdbc,tx);
    }
    Facts launch(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String launchProfile){return launch(guard,owner,operation,launchProfile,b->{});}
    Facts launch(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String launchProfile,Consumer<Boundary> fault){
        return launchThen(guard,owner,operation,launchProfile,fault,current->inspect(operation));
    }
    /** Same live invocation only. The refreshed owner never escapes as historical or restart authority. */
    NarrowContentFetchCompletion.Observation launchAndCheckpoint(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String launchProfile,
                                                                Consumer<NarrowContentFetchCompletion.Boundary> completionFault){
        return launchThen(guard,owner,operation,launchProfile,b->{},current->new NarrowContentFetchCompletion(jdbc,tx,workers).complete(guard,current,operation,completionFault));
    }
    /** Original live chain only; no owner escapes and no Bronze child can launch. */
    NarrowContentBronzeReservation.Facts launchCheckpointAndReserveBronze(InstallationHostGuard guard,WorkerRepository.Owner owner,
            UUID fetchOperation,String launchProfile,UUID bronzeOperation,Consumer<NarrowContentBronzeReservation.Boundary> fault){
        require(bronzeOperation!=null && fault!=null && !bronzeOperation.equals(fetchOperation),WORKER_CONFIGURATION_INVALID);
        return launchThen(guard,owner,fetchOperation,launchProfile,b->{},current->{
            new NarrowContentFetchCompletion(jdbc,tx,workers).complete(guard,current,fetchOperation);
            return new NarrowContentBronzeReservation(jdbc,tx,workers).reserve(guard,current,fetchOperation,bronzeOperation,fault);
        });
    }
    /** Original uninterrupted owner chain only; the Bronze result grants no successor authority. */
    NarrowContentBronzeLaunch.Facts launchCheckpointAndLaunchBronze(InstallationHostGuard guard,WorkerRepository.Owner owner,
            UUID fetchOperation,String fetchProfile,UUID bronzeOperation,String bronzeProfile,Consumer<NarrowContentBronzeLaunch.Boundary> fault){
        require(bronzeOperation!=null && !bronzeOperation.equals(fetchOperation) && bronzeProfile!=null
                && bronzeProfile.matches("[A-Za-z0-9_-]{1,128}") && fault!=null,WORKER_CONFIGURATION_INVALID);
        // Qualify before FETCH effects too, so a missing Bronze profile cannot strand a new chain.
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        StrictBronzeLaunchRuntime.load(jdbc,tx,guard,NarrowContentRuntimeQualification.inspect(jdbc,tx,ready),bronzeProfile);
        return launchThen(guard,owner,fetchOperation,fetchProfile,b->{},current->{
            new NarrowContentFetchCompletion(jdbc,tx,workers).complete(guard,current,fetchOperation);
            return new NarrowContentBronzeLaunch(jdbc,tx,workers).reserveAndLaunch(guard,current,fetchOperation,bronzeOperation,bronzeProfile,fault);
        });
    }
    /** Complete both native checkpoints without exporting or reconstructing live ownership. */
    NarrowContentBronzeCompletion.Observation launchCheckpointAndCompleteBronze(InstallationHostGuard guard,WorkerRepository.Owner owner,
            UUID fetchOperation,String fetchProfile,UUID bronzeOperation,String bronzeProfile,String verificationProfile,
            Consumer<NarrowContentBronzeCompletion.Boundary> fault){
        require(guard!=null && owner!=null && fetchOperation!=null && bronzeOperation!=null && !bronzeOperation.equals(fetchOperation)
                && fetchProfile!=null && fetchProfile.matches("[A-Za-z0-9_-]{1,128}")
                && bronzeProfile!=null && bronzeProfile.matches("[A-Za-z0-9_-]{1,128}")
                && verificationProfile!=null && verificationProfile.matches("[A-Za-z0-9_-]{1,128}") && fault!=null
                && !org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        StrictBronzeLaunchRuntime.load(jdbc,tx,guard,registered,bronzeProfile);
        StrictBronzeVerificationRuntime.load(jdbc,tx,guard,registered,verificationProfile);
        return launchThen(guard,owner,fetchOperation,fetchProfile,b->{},current->{
            new NarrowContentFetchCompletion(jdbc,tx,workers).complete(guard,current,fetchOperation);
            return new NarrowContentBronzeLaunch(jdbc,tx,workers).reserveLaunchAndCheckpoint(guard,current,fetchOperation,bronzeOperation,bronzeProfile,verificationProfile,fault);
        });
    }
    /** Original live FETCH/Bronze chain ending at SQL-only Silver reservation, never Silver execution. */
    NarrowContentSilverReservation.Facts launchCheckpointAndReserveSilver(InstallationHostGuard guard,WorkerRepository.Owner owner,
            UUID fetchOperation,String fetchProfile,UUID bronzeOperation,String bronzeProfile,String verificationProfile,UUID silverOperation,
            Consumer<NarrowContentBronzeCompletion.Boundary> completionFault,Consumer<NarrowContentSilverReservation.Boundary> fault){
        require(guard!=null && owner!=null && fetchOperation!=null && bronzeOperation!=null && silverOperation!=null
                && !bronzeOperation.equals(fetchOperation) && !silverOperation.equals(fetchOperation) && !silverOperation.equals(bronzeOperation)
                && fetchProfile!=null && fetchProfile.matches("[A-Za-z0-9_-]{1,128}") && bronzeProfile!=null && bronzeProfile.matches("[A-Za-z0-9_-]{1,128}")
                && verificationProfile!=null && verificationProfile.matches("[A-Za-z0-9_-]{1,128}") && fault!=null && completionFault!=null
                && !org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        StrictBronzeLaunchRuntime.load(jdbc,tx,guard,registered,bronzeProfile);
        StrictBronzeVerificationRuntime.load(jdbc,tx,guard,registered,verificationProfile);
        return launchThen(guard,owner,fetchOperation,fetchProfile,b->{},current->{
            new NarrowContentFetchCompletion(jdbc,tx,workers).complete(guard,current,fetchOperation);
            return new NarrowContentBronzeLaunch(jdbc,tx,workers).reserveLaunchCheckpointAndReserveSilver(guard,current,fetchOperation,bronzeOperation,
                    bronzeProfile,verificationProfile,silverOperation,completionFault,fault);
        });
    }
    /** Test-gated full original invocation. This does not expose an owner through historical inspection. */
    <T>T originalPipeline(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetch,String profile,
            java.util.function.Function<WorkerRepository.Owner,T> successor){
        return launchThen(guard,owner,fetch,profile,b->{},current->{
            new NarrowContentFetchCompletion(jdbc,tx,workers).complete(guard,current,fetch);
            return successor.apply(current);
        });
    }
    private <T>T launchThen(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String launchProfile,Consumer<Boundary> fault,
                           java.util.function.Function<WorkerRepository.Owner,T> finish){
        require(operation!=null && fault!=null,WORKER_CONFIGURATION_INVALID);
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        var profile=ContentFetchLaunchRuntime.registered(jdbc,tx,registered.registration(),guard.binding(),launchProfile);
        var runtime=ContentFetchLaunchRuntime.load(jdbc,tx,guard,registered,launchProfile);
        var publicProfile=profile.version()==2?profile:null;
        var reservation=new NarrowContentFetchReservation(jdbc,tx,workers);
        var payload=reservation.prepare(guard,owner,operation,publicProfile);
        var consumed=reservation.reserve(guard,owner,operation,publicProfile,b->{});
        require(consumed.commandSha256().equals(SourceJson.sha(payload.commandBytes()))
                && consumed.bootstrapSha256().equals(SourceJson.sha(payload.bootstrapBytes())),RUNTIME_MISMATCH);
        fault.accept(Boundary.RESERVED);
        var context=new NarrowContentEvidenceContext(ready);
        Path directory=ContentOperationPlan.original(context.root(),owner.runId(),owner.attemptId(),ready.source()).directory("FETCH_SOURCE",0);
        // The launch is always fresh. Facts lookup is deliberately not an alternate entry path.
        require(Files.notExists(directory,LinkOption.NOFOLLOW_LINKS),WORKER_NOT_READY);
        check(guard,owner,context,directory,false);
        phase(guard,owner,operation,"LAUNCH_BOUND",runtime,null,null);fault.accept(Boundary.LAUNCH_BOUND);
        try{
            for(Path path:List.of(directory.getParent().getParent(),directory.getParent(),directory)){
                check(guard,owner,context,directory,false);
                // An occupied namespace is never adopted, even if empty.
                Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
                PreflightFiles.privateEntry(path,true);PreparedCommand.forceDirectory(path.getParent());fault.accept(Boundary.DIRECTORY_CREATED);
            }
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
        check(guard,owner,context,directory,false);
        var command=PreparedCommand.prepare(guard,directory,payload.commandBytes(),65536);fault.accept(Boundary.COMMAND_WRITTEN);
        checkCommand(guard,owner,context,directory,command,false);
        phase(guard,owner,operation,"FILES_READY",null,null,null);fault.accept(Boundary.FILES_READY);
        var current=new WorkerRepository.Owner[]{owner};
        var result=new ReadOnlyVerifierProcess().run(runtime.command(),runtime.environment(),guard.binding().root(),Duration.ofSeconds(45),new ReadOnlyVerifierProcess.BootstrapObserver(){
            public void beforeStart(){
                runtime.check();checkCommand(guard,current[0],context,directory,command,false);
                phase(guard,current[0],operation,"START_AUTHORIZED",null,null,null);fault.accept(Boundary.START_AUTHORIZED);
                runtime.check();checkCommand(guard,current[0],context,directory,command,false);
            }
            public void started(InstallationHostGuard.ProcessIdentity child){
                phase(guard,current[0],operation,"CHILD_RECORDED",null,child,null);fault.accept(Boundary.CHILD_RECORDED);
            }
            public byte[] release(InstallationHostGuard.ProcessIdentity child){
                fault.accept(Boundary.BEFORE_RELEASE);runtime.check();checkCommand(guard,current[0],context,directory,command,false);
                byte[] packet=packet(payload.bootstrapBytes(),child.pid());
                fault.accept(Boundary.BEFORE_RELEASE_PHASE);
                phase(guard,current[0],operation,"RELEASE_AUTHORIZED",null,null,SourceJson.sha(packet));fault.accept(Boundary.RELEASE_AUTHORIZED);
                checkCommand(guard,current[0],context,directory,command,false);return packet;
            }
            public void beforeDelivery(){
                fault.accept(Boundary.BEFORE_DELIVERY);
                // Actual writer-thread gate, after scheduling delay and before the first byte.
                runtime.check();checkCommand(guard,current[0],context,directory,command,false);
            }
            public void heartbeat(){
                fault.accept(Boundary.HEARTBEAT);
                // Do not reclassify a live result prefix as READY. Check exact operation scope,
                // immutable ledger and command, then fence controls/deadline before heartbeat.
                checkCommand(guard,current[0],context,directory,command,true);
                require(inspect(operation).phases().equals(PHASES.subList(0,5)),WORKER_NOT_READY);
                current[0]=workers.heartbeat(guard,current[0]);
                checkCommand(guard,current[0],context,directory,command,true);
            }
        });
        require(result.exitCode()==0 && result.stdoutBytes()==0 && result.stderrBytes()==0 && result.descendants().isEmpty(),WORKER_NOT_READY);
        fault.accept(Boundary.BEFORE_CANDIDATE);
        checkCommand(guard,current[0],context,directory,command,true);
        String candidate=candidate(directory,payload.commandBytes());
        checkCommand(guard,current[0],context,directory,command,true);
        require(candidate.equals(candidate(directory,payload.commandBytes())),RUNTIME_MISMATCH);
        fault.accept(Boundary.BEFORE_CANDIDATE_PHASE);
        phase(guard,current[0],operation,"CANDIDATE_EXITED",null,null,candidate);fault.accept(Boundary.CANDIDATE_EXITED);
        return finish.apply(current[0]);
    }
    /** Historical facts only: no files, installed runtime, owner, packet, or resume token returned. */
    Facts inspect(UUID operation){return inspect(jdbc,tx,operation);}
    static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){
        var consumed=NarrowContentFetchReservation.inspect(jdbc,tx,operation);
        return tx.required(()->{
            var rows=jdbc.queryForList("SELECT phase,launch_profile_id,launch_profile_sha256,child_instance_id,child_pid,child_started_at,bootstrap_sha256,candidate_sha256 FROM ddip_management.content_native_launch_phases WHERE operation_id=? ORDER BY CASE phase WHEN 'LAUNCH_BOUND' THEN 1 WHEN 'FILES_READY' THEN 2 WHEN 'START_AUTHORIZED' THEN 3 WHEN 'CHILD_RECORDED' THEN 4 WHEN 'RELEASE_AUTHORIZED' THEN 5 ELSE 6 END",operation);
            var names=rows.stream().map(r->(String)r.get("phase")).toList();
            require(names.size()<=PHASES.size() && names.equals(PHASES.subList(0,names.size())),RUNTIME_MISMATCH);
            InstallationHostGuard.ProcessIdentity child=null;String id=null,digest=null,candidate=null;
            if(!rows.isEmpty()){
                id=(String)rows.getFirst().get("launch_profile_id");digest=(String)rows.getFirst().get("launch_profile_sha256");
                var configs=jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_fetch_launch_profiles WHERE launch_profile_id=?",id);
                require(configs.size()==1 && digest.equals(configs.getFirst().get("configuration_sha256")),RUNTIME_MISMATCH);
                byte[] bytes=(byte[])configs.getFirst().get("configuration_bytes");require(digest.equals(SourceJson.sha(bytes)),RUNTIME_MISMATCH);var launchConfiguration=PreflightFiles.canonical(bytes,65536);
                // The reservation independently validated its public launch/policy chain even
                // before LAUNCH_BOUND existed. Later phases must bind that same identity.
                var reservation=PreflightFiles.canonical(jdbc.queryForObject("SELECT bootstrap_bytes FROM ddip_management.content_native_operations WHERE operation_id=?",byte[].class,operation),65535);
                require(launchConfiguration.get("strictFetchLaunchVersion").isInt()
                        && launchConfiguration.get("strictFetchLaunchVersion").intValue()==reservation.get("reservationVersion").intValue(),RUNTIME_MISMATCH);
                if(reservation.get("reservationVersion").intValue()==2){
                    var binding=reservation.get("publicFetch");
                    require(id.equals(binding.get("launchProfileId").stringValue())
                            && digest.equals(binding.get("launchProfileSha256").stringValue()),RUNTIME_MISMATCH);
                }
            }
            if(rows.size()>=4){var row=rows.get(3);child=new InstallationHostGuard.ProcessIdentity((UUID)row.get("child_instance_id"),((Number)row.get("child_pid")).longValue(),((java.sql.Timestamp)row.get("child_started_at")).toInstant());}
            if(rows.size()>=5){
                byte[] bytes=jdbc.queryForObject("SELECT bootstrap_bytes FROM ddip_management.content_native_operations WHERE operation_id=?",byte[].class,operation);
                require(child!=null && SourceJson.sha(packet(bytes,child.pid())).equals(rows.get(4).get("bootstrap_sha256")),RUNTIME_MISMATCH);
            }
            if(rows.size()==6)candidate=(String)rows.get(5).get("candidate_sha256");
            return new Facts(operation,consumed.attempt(),names,child,id,digest,candidate);
        });
    }
    private void check(InstallationHostGuard guard,WorkerRepository.Owner owner,NarrowContentEvidenceContext context,Path directory,boolean released){
        ContentEvidenceBudget.inspectFetch(context,guard,directory,released);workers.narrowOperationTransaction(guard,owner,()->null);
    }
    private void checkCommand(InstallationHostGuard guard,WorkerRepository.Owner owner,NarrowContentEvidenceContext context,Path directory,PreparedCommand command,boolean released){
        ContentEvidenceBudget.inspectFetch(context,guard,directory,released);
        command.verify(guard,owner,guard.binding().root().relativize(directory).toString(),command.sha256);
        workers.narrowOperationTransaction(guard,owner,()->null);
    }
    private void phase(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String phase,ContentFetchLaunchRuntime runtime,InstallationHostGuard.ProcessIdentity child,String digest){
        workers.narrowOperationTransaction(guard,owner,()->{
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_native_operations WHERE operation_id=? AND attempt_id=?",Long.class,operation,owner.attemptId())==1,WORKER_NOT_READY);
            return jdbc.update("INSERT INTO ddip_management.content_native_launch_phases(operation_id,phase,launch_profile_id,launch_profile_sha256,child_instance_id,child_pid,child_started_at,bootstrap_sha256,candidate_sha256) VALUES (?,?,?,?,?,?,?,?,?)",
                    operation,phase,runtime==null?null:runtime.id(),runtime==null?null:runtime.digest(),child==null?null:child.instanceId(),child==null?null:child.pid(),child==null?null:java.sql.Timestamp.from(child.startedAt()),
                    phase.equals("RELEASE_AUTHORIZED")?digest:null,phase.equals("CANDIDATE_EXITED")?digest:null);
        });
    }
    static byte[] packet(byte[] reservation,long pid){
        require(pid>0,RUNTIME_MISMATCH);var facts=PreflightFiles.canonical(reservation,65535);
        require(facts.get("reservationVersion")!=null && facts.get("reservationVersion").isInt(),RUNTIME_MISMATCH);
        int version=facts.get("reservationVersion").intValue();
        require(version==1 || version==2,RUNTIME_MISMATCH);
        require(facts.get("mode").stringValue().equals(version==1?"FETCH_SOURCE_RESERVATION":"PUBLIC_FETCH_SOURCE_RESERVATION"),RUNTIME_MISMATCH);
        if(version==1)SourceJson.fields(facts,"reservationVersion","mode","operationId","expected","current","fetch");
        else SourceJson.fields(facts,"reservationVersion","mode","operationId","expected","current","fetch","publicFetch");
        var packet=SourceJson.object().put("bootstrapVersion",version).put("mode",version==1?"FETCH_SOURCE_CANDIDATE":"PUBLIC_FETCH_SOURCE_CANDIDATE").put("childPid",Long.toString(pid));
        for(String field:List.of("expected","current","fetch"))packet.set(field,facts.get(field));
        if(version==2)packet.set("publicFetch",facts.get("publicFetch"));
        byte[] bytes=SourceJson.canonical(packet);require(bytes.length<65536,RUNTIME_MISMATCH);return bytes;
    }
    /** Candidate inventory only, not independent native semantic verification or checkpoint success. */
    static String candidate(Path directory,byte[] command){
        try{
            Set<String> names=Set.of("command.json","payload.xml","metadata.json","descriptor.json","result.json");
            try(var files=Files.list(directory)){require(files.map(p->p.getFileName().toString()).collect(java.util.stream.Collectors.toSet()).equals(names),RUNTIME_MISMATCH);}
            var inventory=SourceJson.object();
            for(String name:new TreeSet<>(names)){
                byte[] bytes=PreflightFiles.read(directory.resolve(name),name.equals("payload.xml")?5*1024*1024:65536,true);
                if(name.equals("command.json"))require(Arrays.equals(bytes,command),RUNTIME_MISMATCH);
                if(name.equals("result.json")){
                    var result=PreflightFiles.canonical(bytes,65536);var c=PreflightFiles.canonical(command,65536);
                    var expected=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))expected.set(key,c.get(key));
                    expected.put("status","OK").putNull("code");expected.set("evidenceId",c.get("checkpointId"));expected.putObject("counts");
                    require(Arrays.equals(bytes,SourceJson.canonical(expected)),RUNTIME_MISMATCH);
                }
                inventory.putObject(name).put("bytes",bytes.length).put("sha256",SourceJson.sha(bytes));
            }
            return SourceJson.sha(SourceJson.canonical(inventory));
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
    }
}
