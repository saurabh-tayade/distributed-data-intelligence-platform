package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;

/** Inactive original-live-owner launch. Candidate inventory is not remote verification or a receipt. */
final class NarrowContentBronzeLaunch {
    enum Boundary { RESERVED, LAUNCH_BOUND, DIRECTORY_CREATED, COMMAND_WRITTEN, FILES_READY, START_AUTHORIZED,
        CHILD_RECORDED, BEFORE_RELEASE, BEFORE_RELEASE_PHASE, RELEASE_AUTHORIZED, BEFORE_DELIVERY, HEARTBEAT,
        BEFORE_CANDIDATE, BEFORE_CANDIDATE_PHASE, CANDIDATE_EXITED }
    static final List<String> PHASES=NarrowContentFetchLaunch.PHASES;
    static final Set<String> STATES=Set.of("EXACT","ABSENT","PAYLOAD_ONLY","METADATA_ONLY","CONFLICT","UNAVAILABLE");
    record Facts(UUID operation,UUID fetchOperation,UUID attempt,List<String> phases,InstallationHostGuard.ProcessIdentity child,
                 String launchProfileId,String launchProfileSha256,String candidateSha256,String storageState,Integer exitCode){
        Facts {phases=List.copyOf(phases);}
        boolean candidateForVerification(){return phases.contains("CANDIDATE_EXITED");}
        boolean completed(){return false;}
        boolean nativeLaunchCompatible(){return false;}
        @Override public String toString(){return "NarrowContentBronzeLaunchFacts[candidate only; no continuation]";}
    }
    record Candidate(String sha256,String state,int exitCode) { }
    /** Unexported one-use live channel; historical facts cannot construct this object. */
    static final class LiveCandidate {
        private final WorkerRepository.Owner owner;private final UUID operation;private final Candidate candidate;private boolean consumed;
        private LiveCandidate(WorkerRepository.Owner owner,UUID operation,Candidate candidate){this.owner=owner;this.operation=operation;this.candidate=candidate;}
        synchronized WorkerRepository.Owner consume(UUID expected){require(!consumed && operation.equals(expected),WORKER_NOT_READY);consumed=true;return owner;}
        UUID operation(){return operation;}Candidate candidate(){return candidate;}
        @Override public String toString(){return "LiveBronzeCandidate[private; one use]";}
    }
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    NarrowContentBronzeLaunch(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){
        this.jdbc=Objects.requireNonNull(jdbc);this.tx=Objects.requireNonNull(tx);this.workers=Objects.requireNonNull(workers);
        workers.requireAdmissionRepository(jdbc,tx);
    }
    /** Reserve and launch once in this invocation. No entry point accepts an existing reservation. */
    Facts reserveAndLaunch(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetchOperation,UUID operation,
                           String launchProfile,Consumer<Boundary> fault){
        return launchThen(guard,owner,fetchOperation,operation,launchProfile,fault,current->inspect(operation));
    }
    /** Only this original live call can cross from candidate to checkpoint. No resume API exists. */
    NarrowContentBronzeCompletion.Observation reserveLaunchAndCheckpoint(InstallationHostGuard guard,WorkerRepository.Owner owner,
            UUID fetchOperation,UUID operation,String launchProfile,String verificationProfile,Consumer<NarrowContentBronzeCompletion.Boundary> fault){
        require(guard!=null && owner!=null && fetchOperation!=null && operation!=null && !operation.equals(fetchOperation)
                && launchProfile!=null && launchProfile.matches("[A-Za-z0-9_-]{1,128}")
                && verificationProfile!=null && verificationProfile.matches("[A-Za-z0-9_-]{1,128}") && fault!=null
                && !org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        StrictBronzeVerificationRuntime.load(jdbc,tx,guard,NarrowContentRuntimeQualification.inspect(jdbc,tx,ready),verificationProfile);
        return launchThen(guard,owner,fetchOperation,operation,launchProfile,b->{},current->
                new NarrowContentBronzeCompletion(jdbc,tx,workers).complete(guard,current,verificationProfile,fault));
    }
    /** Reservation-only successor of an acknowledged checkpoint in this same live invocation. */
    NarrowContentSilverReservation.Facts reserveLaunchCheckpointAndReserveSilver(InstallationHostGuard guard,WorkerRepository.Owner owner,
            UUID fetchOperation,UUID operation,String launchProfile,String verificationProfile,UUID silverOperation,
            Consumer<NarrowContentBronzeCompletion.Boundary> completionFault,Consumer<NarrowContentSilverReservation.Boundary> fault){
        require(guard!=null && owner!=null && fetchOperation!=null && operation!=null && silverOperation!=null
                && !operation.equals(fetchOperation) && !silverOperation.equals(operation) && !silverOperation.equals(fetchOperation)
                && launchProfile!=null && launchProfile.matches("[A-Za-z0-9_-]{1,128}")
                && verificationProfile!=null && verificationProfile.matches("[A-Za-z0-9_-]{1,128}") && fault!=null && completionFault!=null
                && !org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        StrictBronzeVerificationRuntime.load(jdbc,tx,guard,NarrowContentRuntimeQualification.inspect(jdbc,tx,ready),verificationProfile);
        return launchThen(guard,owner,fetchOperation,operation,launchProfile,b->{},current->
                new NarrowContentBronzeCompletion(jdbc,tx,workers).completeAndReserveSilver(guard,current,verificationProfile,silverOperation,completionFault,fault));
    }
    /** Same live call only; the callback is never reconstructed from a reservation or candidate fact. */
    <T>T originalPipeline(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetch,UUID operation,
            String launchProfile,String verificationProfile,java.util.function.Function<NarrowContentBronzeCompletion.LiveCheckpoint,T> successor){
        return launchThen(guard,owner,fetch,operation,launchProfile,b->{},candidate->{
            var result=new java.util.concurrent.atomic.AtomicReference<T>();
            new NarrowContentBronzeCompletion(jdbc,tx,workers).completeThen(guard,candidate,verificationProfile,b->{},live->result.set(successor.apply(live)));
            require(result.get()!=null,OWNERSHIP_LOST);return result.get();
        });
    }
    private <T>T launchThen(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetchOperation,UUID operation,
            String launchProfile,Consumer<Boundary> fault,java.util.function.Function<LiveCandidate,T> finish){
        require(guard!=null && owner!=null && operation!=null && fetchOperation!=null && !operation.equals(fetchOperation) && fault!=null,WORKER_CONFIGURATION_INVALID);
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        var runtime=StrictBronzeLaunchRuntime.load(jdbc,tx,guard,registered,launchProfile);
        var consumed=new NarrowContentBronzeReservation(jdbc,tx,workers).reserve(guard,owner,fetchOperation,operation);
        fault.accept(Boundary.RESERVED);
        var material=NarrowContentBronzeReservation.reconstruct(jdbc,tx,ready,operation);
        require(consumed.commandSha256().equals(SourceJson.sha(material.command())) && material.fetchOperation().equals(fetchOperation),RUNTIME_MISMATCH);
        // These are independently read original enrollment and durable current owner facts.
        var metadata=tx.required(()->NarrowContentFetchReservation.metadata(jdbc,owner.attemptId()));
        require(metadata.ready().equals(ready) && metadata.current().equals(new NarrowContentFetchReservation.Identity(
                owner.worker().installationId(),owner.worker().instanceId(),owner.worker().epoch(),owner.runId(),owner.attemptId())),RUNTIME_MISMATCH);
        byte[] bootstrap=bootstrap(material,registered,metadata.expected(),metadata.current(),runtime.protocolDigest());
        var context=new NarrowContentEvidenceContext(ready);Path directory=context.originalDirectory("STORE_BRONZE",0);
        require(Files.notExists(directory,LinkOption.NOFOLLOW_LINKS) && Files.notExists(directory.getParent(),LinkOption.NOFOLLOW_LINKS),WORKER_NOT_READY);
        check(guard,owner,material,false);runtime.check();
        phase(guard,owner,operation,"LAUNCH_BOUND",runtime,null,null,null);fault.accept(Boundary.LAUNCH_BOUND);
        try{
            // FETCH ancestors already exist. Only this reserved stage parent and leaf are fresh.
            for(Path path:List.of(directory.getParent(),directory)){
                check(guard,owner,material,false);
                Files.createDirectory(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
                PreflightFiles.privateEntry(path,true);PreparedCommand.forceDirectory(path.getParent());fault.accept(Boundary.DIRECTORY_CREATED);
            }
        }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
        check(guard,owner,material,false);
        var command=PreparedCommand.prepare(guard,directory,material.command(),65536);fault.accept(Boundary.COMMAND_WRITTEN);
        checkCommand(guard,owner,material,directory,command,false);
        phase(guard,owner,operation,"FILES_READY",null,null,null,null);fault.accept(Boundary.FILES_READY);
        var current=new WorkerRepository.Owner[]{owner};
        var result=new ReadOnlyVerifierProcess().run(runtime.command(),runtime.environment(),guard.binding().root(),Duration.ofSeconds(180),new ReadOnlyVerifierProcess.BootstrapObserver(){
            public void beforeStart(){
                runtime.check();checkCommand(guard,current[0],material,directory,command,false);
                phase(guard,current[0],operation,"START_AUTHORIZED",null,null,null,null);fault.accept(Boundary.START_AUTHORIZED);
                runtime.check();checkCommand(guard,current[0],material,directory,command,false);
            }
            public void started(InstallationHostGuard.ProcessIdentity child){
                phase(guard,current[0],operation,"CHILD_RECORDED",null,child,null,null);fault.accept(Boundary.CHILD_RECORDED);
            }
            public byte[] release(InstallationHostGuard.ProcessIdentity child){
                fault.accept(Boundary.BEFORE_RELEASE);runtime.check();checkCommand(guard,current[0],material,directory,command,false);
                byte[] packet=packet(bootstrap,child.pid());fault.accept(Boundary.BEFORE_RELEASE_PHASE);
                phase(guard,current[0],operation,"RELEASE_AUTHORIZED",null,null,SourceJson.sha(packet),null);fault.accept(Boundary.RELEASE_AUTHORIZED);
                checkCommand(guard,current[0],material,directory,command,false);return packet;
            }
            public void beforeDelivery(){
                fault.accept(Boundary.BEFORE_DELIVERY);runtime.check();checkCommand(guard,current[0],material,directory,command,false);
            }
            public void heartbeat(){
                fault.accept(Boundary.HEARTBEAT);runtime.check();checkCommand(guard,current[0],material,directory,command,true);
                require(inspect(operation).phases().equals(PHASES.subList(0,5)),WORKER_NOT_READY);
                current[0]=workers.heartbeat(guard,current[0]);checkCommand(guard,current[0],material,directory,command,true);
            }
        });
        require((result.exitCode()==0 || result.exitCode()==1) && outputAccepted(result) && result.descendants().isEmpty(),WORKER_NOT_READY);
        fault.accept(Boundary.BEFORE_CANDIDATE);runtime.check();checkCommand(guard,current[0],material,directory,command,true);
        var candidate=candidate(directory,material.command(),material.descriptor(),result.exitCode());
        checkCommand(guard,current[0],material,directory,command,true);
        require(candidate.equals(candidate(directory,material.command(),material.descriptor(),result.exitCode())),RUNTIME_MISMATCH);
        fault.accept(Boundary.BEFORE_CANDIDATE_PHASE);
        // A delayed phase gate must not bless files replaced after the earlier inventory.
        runtime.check();checkCommand(guard,current[0],material,directory,command,true);
        require(candidate.equals(candidate(directory,material.command(),material.descriptor(),result.exitCode())),RUNTIME_MISMATCH);
        phase(guard,current[0],operation,"CANDIDATE_EXITED",null,null,null,candidate);fault.accept(Boundary.CANDIDATE_EXITED);
        return finish.apply(new LiveCandidate(current[0],operation,candidate));
    }
    /** Native SDK's exact no-provider diagnostic is allowed; no arbitrary stderr is retained or accepted. */
    static boolean outputAccepted(ReadOnlyVerifierProcess.Result result){
        return result.stdoutBytes()==0 && stderrAccepted(result);
    }
    static boolean stderrAccepted(ReadOnlyVerifierProcess.Result result){
        byte[] warning=("SLF4J: Failed to load class \"org.slf4j.impl.StaticLoggerBinder\".\n"
                +"SLF4J: Defaulting to no-operation (NOP) logger implementation\n"
                +"SLF4J: See http://www.slf4j.org/codes.html#StaticLoggerBinder for further details.\n").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        return result.stderrBytes()==0 || result.stderrBytes()==warning.length && SourceJson.sha(warning).equals(result.stderrSha256());
    }
    private void check(InstallationHostGuard guard,WorkerRepository.Owner owner,NarrowContentBronzeReservation.Material material,boolean released){
        NarrowContentBronzeReservation.reopenForLaunch(guard,material,released);workers.narrowOperationTransaction(guard,owner,()->null);
    }
    private void checkCommand(InstallationHostGuard guard,WorkerRepository.Owner owner,NarrowContentBronzeReservation.Material material,
                              Path directory,PreparedCommand command,boolean released){
        check(guard,owner,material,released);command.verify(guard,owner,guard.binding().root().relativize(directory).toString(),command.sha256);
        workers.narrowOperationTransaction(guard,owner,()->null);
    }
    private void phase(InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String phase,StrictBronzeLaunchRuntime runtime,
                       InstallationHostGuard.ProcessIdentity child,String bootstrap,Candidate candidate){
        workers.narrowOperationTransaction(guard,owner,()->{
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_bronze_reservations WHERE operation_id=? AND attempt_id=?",Long.class,operation,owner.attemptId())==1,WORKER_NOT_READY);
            return jdbc.update("INSERT INTO ddip_management.content_bronze_launch_phases(operation_id,phase,launch_profile_id,launch_profile_sha256,child_instance_id,child_pid,child_started_at,bootstrap_sha256,candidate_sha256,exit_code,storage_state,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    operation,phase,runtime==null?null:runtime.id(),runtime==null?null:runtime.digest(),child==null?null:child.instanceId(),child==null?null:child.pid(),child==null?null:java.sql.Timestamp.from(child.startedAt()),
                    bootstrap,candidate==null?null:candidate.sha256(),candidate==null?null:candidate.exitCode(),candidate==null?null:candidate.state(),
                    owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
        });
    }
    /** Historical metadata only: no paths, bootstrap, live owner or installed authority returned. */
    Facts inspect(UUID operation){return inspect(jdbc,tx,operation);}
    static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){
        var consumed=NarrowContentBronzeReservation.inspect(jdbc,tx,operation);
        return tx.required(()->{
            var rows=jdbc.queryForList("SELECT * FROM ddip_management.content_bronze_launch_phases WHERE operation_id=? ORDER BY CASE phase WHEN 'LAUNCH_BOUND' THEN 1 WHEN 'FILES_READY' THEN 2 WHEN 'START_AUTHORIZED' THEN 3 WHEN 'CHILD_RECORDED' THEN 4 WHEN 'RELEASE_AUTHORIZED' THEN 5 ELSE 6 END",operation);
            var names=rows.stream().map(r->(String)r.get("phase")).toList();require(names.size()<=PHASES.size() && names.equals(PHASES.subList(0,names.size())),RUNTIME_MISMATCH);
            InstallationHostGuard.ProcessIdentity child=null;String id=null,digest=null,candidate=null,state=null;Integer exit=null;JsonNode profile=null;
            if(!rows.isEmpty()){
                id=(String)rows.getFirst().get("launch_profile_id");digest=(String)rows.getFirst().get("launch_profile_sha256");
                var profiles=jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_bronze_launch_profiles WHERE launch_profile_id=?",id);
                require(profiles.size()==1 && digest.equals(profiles.getFirst().get("configuration_sha256")),RUNTIME_MISMATCH);
                byte[] bytes=(byte[])profiles.getFirst().get("configuration_bytes");require(digest.equals(SourceJson.sha(bytes)),RUNTIME_MISMATCH);
                var ready=NarrowContentFetchReservation.metadata(jdbc,consumed.attempt()).ready();
                profile=PreflightFiles.canonical(bytes,65536);StrictBronzeLaunchRuntime.validateRegistration(jdbc,tx,profile,NarrowContentRuntimeQualification.inspect(jdbc,tx,ready).registration(),ready.host());
            }
            if(rows.size()>=4){var row=rows.get(3);child=new InstallationHostGuard.ProcessIdentity((UUID)row.get("child_instance_id"),((Number)row.get("child_pid")).longValue(),((java.sql.Timestamp)row.get("child_started_at")).toInstant());}
            if(rows.size()>=5){
                var metadata=NarrowContentFetchReservation.metadata(jdbc,consumed.attempt());
                var material=NarrowContentBronzeReservation.historicalMaterial(jdbc,tx,metadata.ready(),operation);
                byte[] rebuilt=bootstrap(material,NarrowContentRuntimeQualification.inspect(jdbc,tx,metadata.ready()),metadata.expected(),metadata.current(),profile.get("strictRuntimeSha256").stringValue());
                require(SourceJson.sha(packet(rebuilt,child.pid())).equals(rows.get(4).get("bootstrap_sha256")),RUNTIME_MISMATCH);
            }
            if(rows.size()==6){var row=rows.getLast();candidate=(String)row.get("candidate_sha256");state=(String)row.get("storage_state");exit=((Number)row.get("exit_code")).intValue();
                require(candidate.matches("[0-9a-f]{64}") && STATES.contains(state) && exit==(state.equals("EXACT")?0:1),RUNTIME_MISMATCH);}
            return new Facts(operation,consumed.fetchOperation(),consumed.attempt(),names,child,id,digest,candidate,state,exit);
        });
    }
    /** Pure fresh Bronze construction from independent SQL facts, not a converted FETCH packet. */
    static byte[] bootstrap(NarrowContentBronzeReservation.Material m,NarrowContentRuntimeQualification.Snapshot registered,
                            NarrowContentFetchReservation.Identity expected,NarrowContentFetchReservation.Identity current,String strictDigest){
        var ready=m.ready();require(ready.kind()==NarrowContentEvidenceContext.PlanKind.ORIGINAL && expected.equals(current)
                && expected.installation().equals(ready.host().installationId()) && expected.run().equals(ready.run()) && expected.attempt().equals(ready.attempt())
                && registered.run().equals(ready.run()) && registered.attempt().equals(ready.attempt()),RUNTIME_MISMATCH);
        var frozen=registered.frozenSelection();require(SourceJson.sha(SourceJson.canonical(frozen)).equals(ready.frozen().selectionSha256())
                && registered.digest("CONTENT").equals(ready.frozen().runtimeSha256()),RUNTIME_MISMATCH);
        var command=PreflightFiles.canonical(m.command(),65536);String hash=SourceJson.sha(m.command());
        UUID checkpoint=UUID.fromString(new NarrowContentEvidenceContext(ready).originalDirectory("STORE_BRONZE",0).getFileName().toString());
        require(command.get("checkpointId").stringValue().equals(checkpoint.toString()) && command.get("runtimeSha256").stringValue().equals(registered.digest("BRONZE"))
                && command.get("ownerEpoch").longValue()==current.epoch(),RUNTIME_MISMATCH);
        var e=SourceJson.object().put("installationId",ready.host().installationId().toString()).put("policy",ready.policy()).put("planKind","ORIGINAL")
                .put("runId",ready.run().toString()).put("originalAttemptId",ready.attempt().toString()).put("sourceId",ready.source()).put("controlRoot",ready.host().root().toString()).put("ledgerSha256",ready.ledgerSha256());
        e.set("controlIdentity",identity(ready.host().rootIdentity()));e.set("rootIdentity",identity(ready.rootIdentity()));e.set("lockIdentity",identity(ready.lockIdentity()));
        e.set("frozen",SourceJson.object().put("selectionSha256",ready.frozen().selectionSha256()).put("profileSha256",ready.frozen().profileSha256()).put("runtimeSha256",ready.frozen().runtimeSha256()));
        e.set("writer",writer(expected,checkpoint,hash));
        var c=SourceJson.object().put("policy",NarrowContentProfile.POLICY);
        c.set("frozen",SourceJson.object().put("selectionSha256",SourceJson.sha(SourceJson.canonical(frozen)))
                .put("profileSha256",frozen.get("executionProfile").get("configurationSha256").stringValue()).put("runtimeSha256",registered.digest("CONTENT")));
        c.set("writer",writer(current,checkpoint,hash));require(e.get("frozen").equals(c.get("frozen")) && e.get("writer").equals(c.get("writer")),RUNTIME_MISMATCH);
        var bronze=SourceJson.object().put("runtimeSha256",registered.digest("BRONZE"));bronze.set("storage",command.get("storage"));
        var fetch=bronze.putObject("fetch").put("checkpointId",m.fetchCheckpoint().toString()).put("commandSha256",SourceJson.sha(m.fetchCommand()))
                .put("descriptorSha256",SourceJson.sha(m.descriptor())).put("runtimeSha256",registered.digest("FETCH"));fetch.set("reference",SourceJson.parse(m.reference()));
        var source=frozen.get("sources").get(0);require(source.get("sourceId").stringValue().equals(ready.source()),RUNTIME_MISMATCH);
        fetch.set("revision",source.get("revision"));fetch.set("capturedConfigSha256",source.get("configurationSha256"));fetch.set("endpointSha256",source.get("endpointSha256"));fetch.set("feedUri",source.get("endpoint").get("uri"));
        var packet=SourceJson.object().put("bootstrapVersion",1).put("mode","STORE_BRONZE_CANDIDATE").put("strictRuntimeSha256",strictDigest);
        packet.set("expected",e);packet.set("current",c);packet.set("bronze",bronze);byte[] bytes=SourceJson.canonical(packet);require(bytes.length<65500,RUNTIME_MISMATCH);return bytes;
    }
    static byte[] packet(byte[] bootstrap,long pid){
        require(pid>0,RUNTIME_MISMATCH);var value=PreflightFiles.canonical(bootstrap,65535);
        SourceJson.fields(value,"bootstrapVersion","mode","strictRuntimeSha256","expected","current","bronze");
        require(value.get("bootstrapVersion").isInt() && value.get("bootstrapVersion").intValue()==1
                && value.get("mode").stringValue().equals("STORE_BRONZE_CANDIDATE"),RUNTIME_MISMATCH);
        var packet=(ObjectNode)value.deepCopy();packet.put("childPid",Long.toString(pid));byte[] bytes=SourceJson.canonical(packet);require(bytes.length<65536,RUNTIME_MISMATCH);return bytes;
    }
    private static ObjectNode identity(InstallationHostGuard.Identity i){return SourceJson.object().put("device",Long.toString(i.device())).put("inode",Long.toString(i.inode()));}
    private static ObjectNode writer(NarrowContentFetchReservation.Identity i,UUID checkpoint,String command){return SourceJson.object().put("installationId",i.installation().toString())
            .put("workerInstanceId",i.instance().toString()).put("epoch",Long.toString(i.epoch())).put("runId",i.run().toString()).put("attemptId",i.attempt().toString())
            .put("stage","STORE_BRONZE").put("ordinal",0).put("checkpointId",checkpoint.toString()).put("commandSha256",command);}
    /** Local shape/correspondence only. Never performs GET, PUT, receipt adoption or semantic verification. */
    static Candidate candidate(Path directory,byte[] command,byte[] descriptor,int exit){
        try{
            var before=stamp(directory,true);Set<String> names=Set.of("command.json","evidence.json","result.json");
            try(var files=Files.list(directory)){require(files.map(p->p.getFileName().toString()).collect(java.util.stream.Collectors.toSet()).equals(names),RUNTIME_MISMATCH);}
            var inventory=SourceJson.object();inventory.set("directory",before);var contents=new HashMap<String,byte[]>();
            for(String name:new TreeSet<>(names)){
                Path path=directory.resolve(name);var stamp=stamp(path,false);byte[] bytes=PreflightFiles.read(path,65536,true);
                require(stamp.equals(stamp(path,false)),RUNTIME_MISMATCH);contents.put(name,bytes);
                inventory.putObject(name).put("bytes",bytes.length).put("sha256",SourceJson.sha(bytes)).set("stamp",stamp);
            }
            require(Arrays.equals(command,contents.get("command.json")),RUNTIME_MISMATCH);
            var c=PreflightFiles.canonical(command,65536);var e=PreflightFiles.canonical(contents.get("evidence.json"),65536);
            SourceJson.fields(e,"bronzeEvidenceVersion","commandSha256","state","receipt");
            require(e.get("bronzeEvidenceVersion").isInt() && e.get("bronzeEvidenceVersion").intValue()==1 && e.get("commandSha256").stringValue().equals(SourceJson.sha(command)),RUNTIME_MISMATCH);
            String state=SourceJson.text(e.get("state"));require(STATES.contains(state) && exit==(state.equals("EXACT")?0:1),RUNTIME_MISMATCH);
            var result=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,c.get(key));
            if(state.equals("EXACT")){
                result.put("status","OK").putNull("code");result.set("evidenceId",c.get("checkpointId"));
                var receipt=e.get("receipt");require(receipt.isObject(),RUNTIME_MISMATCH);
                String type=SourceJson.text(receipt.get("payloadContentType"));require(Set.of("application/xml","application/octet-stream").contains(type),RUNTIME_MISMATCH);
                var expected=SourceJson.object().put("receiptVersion",1);var d=PreflightFiles.canonical(descriptor,65536);
                for(String key:List.of("runId","attemptId","sourceId","revision","capturedConfigSha256","ingestionId","ingestedAt","payloadByteLength","payloadSha256"))expected.set(key,d.get(key));
                var storage=c.get("storage");expected.put("journalSha256",SourceJson.sha(descriptor)).set("storageBindingId",storage.get("bindingId"));
                expected.set("storageBindingSha256",storage.get("bindingSha256"));expected.set("bucket",storage.get("configuration").get("bucket"));expected.set("expected",d.get("expected"));
                expected.put("payloadContentType",type).put("metadataContentType","application/json");require(Arrays.equals(SourceJson.canonical(expected),SourceJson.canonical(receipt)),RUNTIME_MISMATCH);
            }else{result.put("status","FAILED").put("code","BRONZE_"+state).putNull("evidenceId");require(e.get("receipt").isNull(),RUNTIME_MISMATCH);}
            result.putObject("counts");require(Arrays.equals(SourceJson.canonical(result),contents.get("result.json")) && before.equals(stamp(directory,true)),RUNTIME_MISMATCH);
            return new Candidate(SourceJson.sha(SourceJson.canonical(inventory)),state,exit);
        }catch(WorkerFailure|PreflightFailure ex){throw ex;}catch(Exception ex){throw new WorkerFailure(RUNTIME_MISMATCH);}
    }
    private static JsonNode stamp(Path path,boolean directory)throws java.io.IOException{
        PreflightFiles.privateEntry(path,directory);var id=InstallationHostGuard.identity(path);
        return SourceJson.object().put("identity",id.device()+":"+id.inode()).put("modified",Files.getLastModifiedTime(path,LinkOption.NOFOLLOW_LINKS).toString()).put("size",Files.size(path));
    }
}
