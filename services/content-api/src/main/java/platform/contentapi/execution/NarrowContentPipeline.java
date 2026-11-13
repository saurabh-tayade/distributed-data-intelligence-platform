package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import platform.contentapi.importing.RegisteredImportEvidence;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;
import static platform.contentapi.execution.NarrowContentOperationRouting.Operation;
import static platform.contentapi.execution.NarrowContentPipelineCommands.*;

/** Full original-invocation implementation. Live lifecycle caller only; no retry or recovered continuation. */
final class NarrowContentPipeline {
    enum Boundary { PREFLIGHT, RESERVED, FILES_READY, START_AUTHORIZED, CHILD_RECORDED, RELEASE_AUTHORIZED,
        BEFORE_DELIVERY, HEARTBEAT, BEFORE_CANDIDATE, CANDIDATE_EXITED, BEFORE_CHECKPOINT, CHECKPOINT_INSERTED, CHECKPOINT_COMMITTED,
        BEFORE_SUCCESS, SUCCESS_INSERTED, SUCCESS_COMMITTED }
    record Event(Operation operation,Boundary boundary) {}
    record Completed(UUID run,UUID attempt,UUID artifact,String proofSha256,WorkerRepository.Session worker) {}
    private record Proof(UUID operation,Operation kind,JsonNode command,JsonNode result,JsonNode evidence,JsonNode selection,String sha256) {}
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final WorkerRepository workers;
    NarrowContentPipeline(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers){this.jdbc=jdbc;this.tx=tx;this.workers=workers;workers.requireAdmissionRepository(jdbc,tx);}
    static List<Operation> core(){return List.of(Operation.values()).subList(0,17);}
    /** Executes only the claimed original invocation; completion returns its acknowledged live Session. */
    Completed executeOriginal(InstallationHostGuard guard,WorkerRepository.Owner owner,String profile,Consumer<Event> fault){
        require(guard!=null && owner!=null && fault!=null && !org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        var ready=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var registered=NarrowContentRuntimeQualification.inspect(jdbc,tx,ready);
        var runtime=StrictContentPipelineRuntime.load(jdbc,tx,guard,registered,profile);
        workers.narrowOperationTransaction(guard,owner,()->{
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_operations WHERE attempt_id=?",Long.class,owner.attemptId())==0,WORKER_NOT_READY);
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_pipeline_operations WHERE attempt_id=?",Long.class,owner.attemptId())==0,WORKER_NOT_READY);return null;
        });
        qualifyBootstrapCapacity(ready,owner,registered);
        var current=new WorkerRepository.Owner[]{owner};
        runtime.probe(()->workers.narrowOperationTransaction(guard,current[0],()->null),
                ()->current[0]=workers.heartbeat(guard,current[0]),workers::stopping);
        fault.accept(new Event(Operation.FETCH_SOURCE,Boundary.PREFLIGHT));
        runtime.check();workers.narrowOperationTransaction(guard,current[0],()->null);
        UUID fetch=UUID.randomUUID(),bronze=UUID.randomUUID(),silver=UUID.randomUUID();
        return new NarrowContentFetchLaunch(jdbc,tx,workers).originalPipeline(guard,current[0],fetch,runtime.config.get("fetchLaunchProfileId").stringValue(),afterFetch->
                new NarrowContentBronzeLaunch(jdbc,tx,workers).originalPipeline(guard,afterFetch,fetch,bronze,
                        runtime.config.get("bronzeLaunchProfileId").stringValue(),runtime.config.get("bronzeVerificationProfileId").stringValue(),live->
                    new NarrowContentSilverReservation(jdbc,tx,workers).reserveThen(guard,live,silver,b->{},afterBronze->{
                        var chain=new Original(guard,afterBronze,ready,runtime,fetch,bronze,silver,fault);
                        // Validate the retained original candidate pins again AFTER capturing the
                        // new scope, before any successor directory can change ancestor metadata.
                        live.check();chain.check(false);return chain.run();
                    })));
    }
    private final class Original {
        final InstallationHostGuard guard;WorkerRepository.Owner owner;
        final NarrowContentEvidenceContext.Expected ready;final StrictContentPipelineRuntime runtime;
        final UUID fetch,bronze,silverReservation;final Consumer<Event> fault;final NarrowContentPipelineScope scope;
        final Map<Operation,Proof> proofs=new LinkedHashMap<>();final JsonNode receipt;final String credentialsSha;
        JsonNode silverInput,goldProof,goldPrepared,contentPrepared;
        Original(InstallationHostGuard guard,WorkerRepository.Owner owner,NarrowContentEvidenceContext.Expected ready,StrictContentPipelineRuntime runtime,
                UUID fetch,UUID bronze,UUID silver,Consumer<Event> fault){
            this.guard=guard;this.owner=owner;this.ready=ready;this.runtime=runtime;this.fetch=fetch;this.bronze=bronze;silverReservation=silver;this.fault=fault;
            // Transfer the acknowledged live checkpoint into a scope permitting only ordered new subtrees.
            scope=new NarrowContentPipelineScope(ready,guard);
            var row=tx.required(()->jdbc.queryForMap("SELECT receipt_bytes,receipt_sha256,verified_sha256 FROM ddip_management.content_bronze_checkpoints WHERE operation_id=?",bronze));
            byte[] raw=(byte[])row.get("receipt_bytes");require(SourceJson.sha(raw).equals(row.get("receipt_sha256")),RUNTIME_MISMATCH);receipt=PreflightFiles.canonical(raw,65536);
            var command=PreflightFiles.canonical(tx.required(()->jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_bronze_reservations WHERE operation_id=?",byte[].class,bronze)),65536);
            credentialsSha=command.get("storage").get("credentialsSha256").stringValue();
            require(SourceJson.sha(PreflightFiles.read(new NarrowContentEvidenceContext(ready).originalDirectory("STORE_BRONZE",0).resolve("verified.json"),65536,true)).equals(row.get("verified_sha256")),RUNTIME_MISMATCH);
            check(false);
        }
        Completed run(){
            for(var operation:core().subList(2,17)){
                JsonNode prepared=null,verified=null,capture=null;
                if(operation.family==NarrowContentOperationRouting.Family.SILVER && operation==Operation.RECHECK_SILVER)capture=proofs.get(Operation.VERIFY_SILVER).evidence().get("capture");
                if(operation.family==NarrowContentOperationRouting.Family.GOLD){
                    prepared=operation==Operation.PREPARE_GOLD?null:goldPrepared;
                    verified=operation==Operation.RECHECK_GOLD?proofs.get(Operation.VERIFY_GOLD).evidence():null;
                }
                if(operation.family==NarrowContentOperationRouting.Family.CONTENT){
                    prepared=operation==Operation.PREPARE_CONTENT?null:contentPrepared;
                    verified=operation==Operation.RECHECK_CONTENT?proofs.get(Operation.VERIFY_CONTENT).evidence():null;
                }
                var command=command(operation,ready,runtime.registered,owner.worker().epoch(),receipt,silverInput,goldProof,prepared,verified,capture);
                Proof proof=execute(operation,command);proofs.put(operation,proof);
                switch(operation){
                    case RECHECK_SILVER -> silverInput=registerSilver();
                    case PREPARE_GOLD -> goldPrepared=proof.evidence().get("prepared");
                    case RECHECK_GOLD -> goldProof=registerGold();
                    case PREPARE_CONTENT -> contentPrepared=proof.evidence().get("prepared");
                    default -> {}
                }
            }
            return succeed();
        }
        private Proof execute(Operation op,ObjectNode command){
            check(false);absentNative(op,command);UUID operation=UUID.randomUUID();UUID predecessor=proofs.isEmpty()?bronze:proofs.values().stream().reduce((a,b)->b).orElseThrow().operation();
            UUID checkpoint=UUID.fromString(new NarrowContentEvidenceContext(ready).originalDirectory(op.ledgerStage,op.ledgerOrdinal).getFileName().toString());
            byte[] raw=SourceJson.canonical(command);String commandSha=SourceJson.sha(raw);
            fence(()->jdbc.update("INSERT INTO ddip_management.content_pipeline_operations(operation_id,attempt_id,bronze_operation_id,silver_reservation_id,step_index,stage,item_ordinal,checkpoint_id,native_checkpoint_id,command_bytes,command_sha256,profile_id,profile_sha256,predecessor_operation_id,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    operation,owner.attemptId(),bronze,silverReservation,op.ordinal(),op.ledgerStage,op.ledgerOrdinal,checkpoint,UUID.fromString(command.get("checkpointId").stringValue()),raw,commandSha,runtime.id,runtime.digest,predecessor,
                    owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion()));
            event(op,Boundary.RESERVED);
            phase(operation,op,"LAUNCH_BOUND",commandSha,null,null,null);
            Path directory=scope.start(op);
            try{
                for(Path p:List.of(directory.getParent(),directory)){
                    check(false);
                    try{Files.createDirectory(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}
                    catch(FileAlreadyExistsException ex){require(!p.equals(directory),WORKER_NOT_READY);}
                    PreflightFiles.privateEntry(p,true);PreparedCommand.forceDirectory(p.getParent());
                }
            }catch(java.io.IOException ex){throw new PreflightFailure(PreflightFailure.Code.INPUT_UNAVAILABLE);}
            scope.directoryReady();check(false);var prepared=PreparedCommand.prepare(guard,directory,raw,ContentEvidenceBudget.slots(op.ledgerStage).get("command.json"));
            phase(operation,op,"FILES_READY",commandSha,null,null,null);event(op,Boundary.FILES_READY);
            var base=bootstrap(op,ready,owner,operation,command,runtime.registered,credentialsSha);
            boolean spark=spark(op);
            var child=new ReadOnlyVerifierProcess(spark,workers::stopping).run(runtime.command(),runtime.environment(),guard.binding().root(),Duration.ofSeconds(runtime.seconds(op.family.name())),new ReadOnlyVerifierProcess.BootstrapObserver(){
                private void verify(boolean released){check(released);prepared.verify(guard,owner,guard.binding().root().relativize(directory).toString(),commandSha);}
                public void beforeStart(){verify(false);absentNative(op,command);phase(operation,op,"START_AUTHORIZED",commandSha,null,null,null);event(op,Boundary.START_AUTHORIZED);verify(false);}
                public void started(InstallationHostGuard.ProcessIdentity child){phase(operation,op,"CHILD_RECORDED",commandSha,child,null,null);event(op,Boundary.CHILD_RECORDED);}
                public byte[] release(InstallationHostGuard.ProcessIdentity child){verify(false);var packet=base.deepCopy().put("childPid",Long.toString(child.pid()));byte[] bytes=SourceJson.canonical(packet);require(bytes.length<65536,RUNTIME_MISMATCH);
                    phase(operation,op,"RELEASE_AUTHORIZED",commandSha,null,SourceJson.sha(bytes),null);event(op,Boundary.RELEASE_AUTHORIZED);verify(false);return bytes;}
                public void beforeDelivery(){event(op,Boundary.BEFORE_DELIVERY);verify(false);absentNative(op,command);}
                public void heartbeat(){event(op,Boundary.HEARTBEAT);verify(true);owner=workers.heartbeat(guard,owner);verify(true);}
            });
            require(child.exitCode()==0 && child.stdoutBytes()==0,WORKER_NOT_READY);check(true);
            prepared.verify(guard,owner,guard.binding().root().relativize(directory).toString(),commandSha);
            var observed=new LinkedHashMap<String,NarrowContentPipelineScope.Observation>();
            observed.put("command.json",NarrowContentPipelineScope.observe(directory.resolve("command.json"),raw.length));
            require(Arrays.equals(raw,observed.get("command.json").bytes()),RUNTIME_MISMATCH);
            var resultFile=NarrowContentPipelineScope.observe(directory.resolve("result.json"),65536);observed.put("result.json",resultFile);
            byte[] resultRaw=resultFile.bytes();var result=PreflightFiles.canonical(resultRaw,65536);
            validateResult(op,command,checkpoint,result,child);
            JsonNode evidence=null,selection=null;byte[] evidenceRaw=null,selectionRaw=null;
            if(!result.get("evidenceSha256").isNull()){var file=NarrowContentPipelineScope.observe(directory.resolve("evidence.json"),ContentEvidenceBudget.slots(op.ledgerStage).get("evidence.json"));observed.put("evidence.json",file);evidenceRaw=file.bytes();require(SourceJson.sha(evidenceRaw).equals(result.get("evidenceSha256").stringValue()),RUNTIME_MISMATCH);evidence=PreflightFiles.canonical(evidenceRaw,1024*1024);}
            if(!result.get("selectionSha256").isNull()){var file=NarrowContentPipelineScope.observe(directory.resolve("selection.json"),ContentEvidenceBudget.slots(op.ledgerStage).get("selection.json"));observed.put("selection.json",file);selectionRaw=file.bytes();require(SourceJson.sha(selectionRaw).equals(result.get("selectionSha256").stringValue()),RUNTIME_MISMATCH);selection=PreflightFiles.canonical(selectionRaw,1024*1024);}
            semantic(op,command,evidence,selection);
            event(op,Boundary.BEFORE_CANDIDATE);
            String candidate=scope.complete(observed);
            prepared.verify(guard,owner,guard.binding().root().relativize(directory).toString(),commandSha);check(false);
            phase(operation,op,"CANDIDATE_EXITED",commandSha,null,null,candidate);event(op,Boundary.CANDIDATE_EXITED);
            var proof=SourceJson.object().put("pipelineCheckpointVersion",1).put("operationId",operation.toString()).put("stepIndex",op.ordinal()).put("commandSha256",commandSha).put("candidateSha256",candidate);
            proof.set("result",result);proof.set("evidence",copy(evidence));proof.set("selection",copy(selection));byte[] proofRaw=SourceJson.canonical(proof);require(proofRaw.length<=3*1024*1024,RUNTIME_MISMATCH);
            event(op,Boundary.BEFORE_CHECKPOINT);check(false);final byte[] e=evidenceRaw,s=selectionRaw;
            fence(()->{int inserted=jdbc.update("INSERT INTO ddip_management.content_pipeline_checkpoints(operation_id,proof_bytes,proof_sha256,result_bytes,evidence_bytes,selection_bytes,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?)",
                    operation,proofRaw,SourceJson.sha(proofRaw),resultRaw,e,s,owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());event(op,Boundary.CHECKPOINT_INSERTED);return inserted;});
            event(op,Boundary.CHECKPOINT_COMMITTED);check(false);
            return new Proof(operation,op,command,result,evidence,selection,SourceJson.sha(proofRaw));
        }
        private void phase(UUID operation,Operation op,String phase,String commandSha,InstallationHostGuard.ProcessIdentity child,String bootstrap,String candidate){
            var data=SourceJson.object().put("phaseVersion",1).put("operationId",operation.toString()).put("phase",phase).put("profileSha256",runtime.digest).put("commandSha256",commandSha);
            if(child==null)data.putNull("child");else data.putObject("child").put("instanceId",child.instanceId().toString()).put("pid",Long.toString(child.pid())).put("startedAt",child.startedAt().toString());
            if(bootstrap==null)data.putNull("bootstrapSha256");else data.put("bootstrapSha256",bootstrap);
            if(candidate==null)data.putNull("candidateSha256");else data.put("candidateSha256",candidate);
            byte[] bytes=SourceJson.canonical(data);
            fence(()->jdbc.update("INSERT INTO ddip_management.content_pipeline_phases(operation_id,phase,phase_bytes,phase_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?)",
                    operation,phase,bytes,SourceJson.sha(bytes),owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion()));
        }
        private void validateResult(Operation op,JsonNode command,UUID slot,JsonNode result,ReadOnlyVerifierProcess.Result child){
            SourceJson.fields(result,"protocolVersion","family","mode","runId","attemptId","ownerEpoch","checkpointId","nativeCheckpointId","runtimeSha256","strictRuntimeSha256","commandSha256","candidateOnly","status","evidenceSha256","selectionSha256","cleanup","code");
            require(result.get("protocolVersion").isInt() && result.get("protocolVersion").intValue()==2 && result.get("candidateOnly").isBoolean() && result.get("candidateOnly").booleanValue()
                    && result.get("family").stringValue().equals(op.family.name()) && result.get("mode").stringValue().equals(op.mode)
                    && result.get("runId").equals(command.get("runId")) && result.get("attemptId").equals(command.get("attemptId")) && result.get("ownerEpoch").isIntegralNumber() && result.get("ownerEpoch").canConvertToLong() && result.get("ownerEpoch").longValue()==command.get("ownerEpoch").longValue()
                    && result.get("checkpointId").stringValue().equals(slot.toString()) && result.get("nativeCheckpointId").equals(command.get("checkpointId"))
                    && result.get("runtimeSha256").equals(command.get("runtimeSha256")) && result.get("strictRuntimeSha256").stringValue().equals(StrictContentPipelineRuntime.protocolDigest())
                    && result.get("commandSha256").stringValue().equals(hash(command)) && result.get("code").isNull(),RUNTIME_MISMATCH);
            SourceJson.fields(result.get("cleanup"),"status","gatewayPid");String cleanup=result.get("cleanup").get("status").stringValue();
            if(spark(op))require(cleanup.equals("CONFIRMED") && !result.get("cleanup").get("gatewayPid").isNull()
                    && result.get("cleanup").get("gatewayPid").stringValue().matches("[1-9][0-9]{0,18}"),WORKER_NOT_READY);
            else require(cleanup.equals("NOT_STARTED") && result.get("cleanup").get("gatewayPid").isNull() && child.descendants().isEmpty() && child.stderrBytes()==0,WORKER_NOT_READY);
            String status=result.get("status").stringValue();
            require(op==Operation.PUBLISH_SILVER?status.equals("PROCESSED"):op==Operation.RECHECK_SILVER || op==Operation.RECHECK_GOLD?status.equals("UNCHANGED"):
                    op.mode.equals("PREPARE")?status.equals("PREPARED"):status.equals("VERIFIED"),WORKER_NOT_READY);
        }
        private void semantic(Operation op,JsonNode command,JsonNode evidence,JsonNode selection){
            if(op.family==NarrowContentOperationRouting.Family.BRONZE)require(evidence!=null && evidence.get("status").stringValue().equals("VERIFIED") && evidence.get("receipt").equals(receipt),RUNTIME_MISMATCH);
            if(op==Operation.VERIFY_SILVER){var q=SourceJson.object().put("qualificationVersion",1);q.set("selection",command.get("artifact"));q.set("verification",evidence);byte[] raw=NarrowContentProfile.silverQualification(q);RegisteredImportEvidence.validate(raw,SourceJson.sha(raw));NarrowContentProfile.silverSelection(selection);
                require(selection.get("inputSetFingerprint").stringValue().equals(hash(selection.get("manifest"))) && selection.get("manifest").get("inputs").size()==1
                        && selection.get("manifest").get("inputs").get(0).get("marker_path").equals(command.get("artifact").get("relativeAddress")),RUNTIME_MISMATCH);}
            if(op==Operation.VERIFY_GOLD)require(evidence.equals(proofs.get(Operation.PUBLISH_GOLD).evidence()),RUNTIME_MISMATCH);
            if(op==Operation.VERIFY_CONTENT)require(evidence.equals(proofs.get(Operation.PUBLISH_CONTENT).evidence()),RUNTIME_MISMATCH);
            if(op==Operation.RECHECK_CONTENT)require(evidence.equals(proofs.get(Operation.VERIFY_CONTENT).evidence()),RUNTIME_MISMATCH);
            if(Set.of(Operation.PREPARE_GOLD,Operation.PREPARE_CONTENT).contains(op))require(evidence!=null && evidence.get("status").stringValue().equals("PREPARED") && evidence.get("prepared").isObject(),RUNTIME_MISMATCH);
        }
        private JsonNode registerSilver(){
            var v=proofs.get(Operation.VERIFY_SILVER);var q=SourceJson.object().put("qualificationVersion",1);q.set("selection",v.command().get("artifact"));q.set("verification",v.evidence());
            byte[] raw=NarrowContentProfile.silverQualification(q);var checked=RegisteredImportEvidence.validate(raw,SourceJson.sha(raw));UUID id=UUID.randomUUID();
            check(false);fence(()->insertArtifact(id,"SILVER",v.evidence().get("inventoryIdentity").get("sha256").stringValue(),checked.selection().bindingId(),checked.selection().address(),raw,List.of()));
            var selected=SourceJson.object().put("artifactId",id.toString()).put("originRunId",owner.runId().toString()).put("sourceId",ready.source()).put("evidenceSha256",SourceJson.sha(raw));
            selected.set("qualification",q);selected.set("receipt",receipt);selected.set("bronze",binding(runtime.registered,"BRONZE").get("configuration"));selected.put("credentialsSha256",credentialsSha);return selected;
        }
        private JsonNode registerGold(){
            var publish=proofs.get(Operation.PUBLISH_GOLD).command();var verification=proofs.get(Operation.VERIFY_GOLD).evidence();var artifact=artifact("GOLD",publish,verification);
            byte[] raw=SourceJson.canonical(artifact);UUID id=UUID.randomUUID();check(false);
            fence(()->insertArtifact(id,"GOLD",artifact.get("nativeFingerprint").stringValue(),artifact.get("storageBindingId").stringValue(),artifact.get("relativeAddress").stringValue(),raw,
                    List.of(new Dependency("SILVER",UUID.fromString(silverInput.get("artifactId").stringValue())))));
            var proof=SourceJson.object().put("artifactId",id.toString()).put("evidenceSha256",SourceJson.sha(raw));proof.set("artifact",artifact);proof.set("command",publish);return proof;
        }
        private Completed succeed(){
            var finalProof=proofs.get(Operation.RECHECK_CONTENT);var artifact=artifact("CONTENT",proofs.get(Operation.PUBLISH_CONTENT).command(),proofs.get(Operation.VERIFY_CONTENT).evidence());
            UUID id=UUID.randomUUID();byte[] raw=SourceJson.canonical(artifact);var proof=SourceJson.object().put("pipelineCompletionVersion",1).put("runId",owner.runId().toString()).put("attemptId",owner.attemptId().toString())
                    .put("fetchOperationId",fetch.toString()).put("bronzeOperationId",bronze.toString()).put("artifactId",id.toString()).put("artifactSha256",SourceJson.sha(raw));
            tx.required(()->{proof.put("fetchCheckpointSha256",jdbc.queryForObject("SELECT completion_sha256 FROM ddip_management.content_fetch_checkpoints WHERE operation_id=?",String.class,fetch));
                proof.put("bronzeVerifiedSha256",jdbc.queryForObject("SELECT verified_sha256 FROM ddip_management.content_bronze_checkpoints WHERE operation_id=?",String.class,bronze));return null;});
            var operations=proof.putArray("operations");for(var p:proofs.values())operations.addObject().put("operationId",p.operation().toString()).put("stepIndex",p.kind().ordinal()).put("proofSha256",p.sha256());
            byte[] bytes=SourceJson.canonical(proof);event(Operation.RECHECK_CONTENT,Boundary.BEFORE_SUCCESS);check(false);
            var released=workers.finishNarrowContent(guard,owner,id,()->{
                insertArtifact(id,"CONTENT",artifact.get("nativeFingerprint").stringValue(),artifact.get("storageBindingId").stringValue(),artifact.get("relativeAddress").stringValue(),raw,
                        List.of(new Dependency("GOLD",UUID.fromString(goldProof.get("artifactId").stringValue())),new Dependency("PUBLISHER_REFERENCE",UUID.fromString(RssContentInputs.reference(runtime.registered.frozenSelection()).get("artifactId").stringValue()))));
                jdbc.update("INSERT INTO ddip_management.content_pipeline_completions(attempt_id,final_operation_id,artifact_id,proof_bytes,proof_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?)",
                        owner.attemptId(),finalProof.operation(),id,bytes,SourceJson.sha(bytes),owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());event(Operation.RECHECK_CONTENT,Boundary.SUCCESS_INSERTED);
            });
            event(Operation.RECHECK_CONTENT,Boundary.SUCCESS_COMMITTED);return new Completed(owner.runId(),owner.attemptId(),id,SourceJson.sha(bytes),released);
        }
        private void absentNative(Operation op,JsonNode command){
            if(!Set.of(Operation.PUBLISH_SILVER,Operation.PUBLISH_GOLD,Operation.PUBLISH_CONTENT).contains(op))return;
            Path root,path;
            if(op==Operation.PUBLISH_SILVER){root=Path.of(command.get("binding").get("root").stringValue());path=Path.of(command.get("artifact").get("relativeAddress").stringValue()).getParent();}
            else{String kind=op.family.name().toLowerCase(Locale.ROOT);root=Path.of(command.get(kind).get("root").stringValue());String key=op==Operation.PUBLISH_GOLD?"inputSetFingerprint":"fingerprint";
                path=Path.of("v1/processor_revision=1/"+(op==Operation.PUBLISH_GOLD?"input_set=":"publication=")+command.get("prepared").get(key).stringValue());}
            require(Files.notExists(root.resolve(path),LinkOption.NOFOLLOW_LINKS),WORKER_NOT_READY);
        }
        private void check(boolean released){runtime.check();scope.inspect(released);fence(()->null);}
        private <T>T fence(java.util.function.Supplier<T> work){return workers.narrowOperationTransaction(guard,owner,work);}
        private void event(Operation op,Boundary boundary){fault.accept(new Event(op,boundary));}
        private int insertArtifact(UUID id,String kind,String nativeId,String binding,String address,byte[] raw,List<Dependency> dependencies){
            require(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE storage_binding_id=? AND relative_address=?",Long.class,binding,address)==0,RUNTIME_MISMATCH);
            int inserted=jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at) VALUES (?,?,'content-intelligence',1,1,?,?,?,?,?,clock_timestamp())",id,kind,nativeId,binding,address,raw,SourceJson.sha(raw));
            for(var d:dependencies)jdbc.update("INSERT INTO ddip_management.artifact_dependencies VALUES (?,?,0,?)",id,d.role(),d.id());return inserted;
        }
    }
    private record Dependency(String role,UUID id) {}
    static boolean spark(Operation op){return op.family==NarrowContentOperationRouting.Family.SILVER?op!=Operation.RECHECK_SILVER:
            op.family==NarrowContentOperationRouting.Family.GOLD?op!=Operation.RECHECK_GOLD:op.family==NarrowContentOperationRouting.Family.CONTENT && op!=Operation.PREPARE_CONTENT;}
    static ObjectNode artifact(String kind,JsonNode command,JsonNode verification){
        require(Set.of("GOLD","CONTENT").contains(kind) && command.get("mode").stringValue().equals("PUBLISH") && command.get("prepared").equals(verification.get("prepared")) && verification.get("status").stringValue().equals("VERIFIED"),RUNTIME_MISMATCH);
        String fingerprint=verification.get("prepared").get(kind.equals("GOLD")?"inputSetFingerprint":"fingerprint").stringValue();
        var result=SourceJson.object().put(kind.equals("GOLD")?"managedGoldVersion":"managedContentVersion",1).put("kind",kind).put("productId","content-intelligence")
                .put("contractVersion",1).put("processorRevision",1).put("nativeFingerprint",fingerprint).put("storageBindingId",command.get("projectionContext").get("outputBindingId").stringValue())
                .put("relativeAddress","v1/processor_revision=1/"+(kind.equals("GOLD")?"input_set=":"publication=")+fingerprint);result.set("verification",verification);
        if(kind.equals("GOLD")){var inputs=result.putArray("inputs");for(var input:command.get("inputs"))inputs.addObject().put("artifactId",input.get("artifactId").stringValue()).put("evidenceSha256",input.get("evidenceSha256").stringValue());}
        else result.set("inputs",RssContentInputs.dependencies(command.get("goldProof"),command.get("reference")));
        require(SourceJson.canonical(result).length<=1024*1024,RUNTIME_MISMATCH);return result;
    }
}
