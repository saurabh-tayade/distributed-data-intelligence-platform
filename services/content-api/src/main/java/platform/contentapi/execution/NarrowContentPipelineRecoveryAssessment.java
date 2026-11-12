package platform.contentapi.execution;

import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.WorkerFailure.require;
import static platform.contentapi.execution.WorkerFailure.Code.*;
import static platform.contentapi.execution.NarrowContentOperationRouting.Operation;

/** Detached, bounded original-prefix assessment. No owner, receipt, callback or mutation capability escapes. */
final class NarrowContentPipelineRecoveryAssessment {
    enum Prefix { ADMISSION, FETCH_INCOMPLETE, FETCH_CHECKPOINT, BRONZE_INCOMPLETE, BRONZE_CHECKPOINT,
        SILVER_RESERVED, OPERATION_INCOMPLETE, OPERATION_CHECKPOINT, TERMINAL_SUCCESS }
    enum Integrity { NOT_REQUESTED, UNSUPPORTED_INCOMPLETE_PREFIX, CURRENT_BYTES_AND_INVENTORY_MATCH,
        UNAVAILABLE_OR_CHANGED }
    record OperationFact(int stepIndex, UUID operation, String stage, List<String> phases, boolean checkpoint,
                         String commandSha256, String proofSha256) {
        OperationFact { phases=List.copyOf(phases); }
    }
    record ArtifactFact(String kind,UUID artifact,String evidenceSha256) {}
    record ProcessFact(String role,UUID operation,long pid,Instant startedAt,RecoveryQuiescence.State observation) {}
    record Assessment(UUID run,UUID attempt,Prefix prefix,List<OperationFact> operations,Integrity integrity,
                      List<ProcessFact> recordedProcesses,List<ArtifactFact> artifacts,List<String> limitations) {
        Assessment { operations=List.copyOf(operations);recordedProcesses=List.copyOf(recordedProcesses);artifacts=List.copyOf(artifacts);limitations=List.copyOf(limitations); }
        boolean authorization(){return false;}
        boolean quiescenceEstablished(){return false;}
        boolean nativeLaunchCompatible(){return false;}
        boolean historicalFilesystemIdentityEstablished(){return false;}
    }
    private record Recorded(String role,UUID operation,RecoveryQuiescence.Identity identity) {}
    private record Fileset(Path directory,Map<String,byte[]> files,String candidate) {}
    private record Snapshot(Prefix prefix,NarrowContentEvidenceContext.Expected ready,List<OperationFact> facts,
                            List<Recorded> processes,List<Fileset> files,Map<String,byte[]> fetchFiles,List<ArtifactFact> artifacts,String signature) {}
    private static final List<String> PHASES=List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED");
    private static final int MB=1024*1024;

    /** SQL-only detached fingerprint. It grants no authority and cannot replace a caller's own fences or evidence review. */
    static String historicalSqlDigest(JdbcTemplate jdbc,ManagementTransactions tx,UUID run,UUID attempt){
        require(jdbc!=null && tx!=null && run!=null && attempt!=null,WORKER_CONFIGURATION_INVALID);
        return tx.required(()->snapshot(jdbc,tx,run,attempt).signature());
    }
    static Assessment inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt){
        return inspect(jdbc,tx,guard,run,attempt,null);
    }
    static Assessment inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt,RecoveryQuiescence.Processes observer){
        require(jdbc!=null && tx!=null && run!=null && attempt!=null && !TransactionSynchronizationManager.isActualTransactionActive(),WORKER_CONFIGURATION_INVALID);
        Snapshot s=tx.required(()->snapshot(jdbc,tx,run,attempt));
        var processFacts=new ArrayList<ProcessFact>();
        var processes=observer!=null?observer:guard!=null && s.ready()!=null && guard.binding().equals(s.ready().host())?RecoveryQuiescence.LOCAL:(RecoveryQuiescence.Processes)id->RecoveryQuiescence.State.UNAVAILABLE;
        for(var p:s.processes()){
            RecoveryQuiescence.State state;
            try{state=processes.inspect(p.identity());if(state==null)state=RecoveryQuiescence.State.UNAVAILABLE;}
            catch(RuntimeException ex){state=RecoveryQuiescence.State.UNAVAILABLE;}
            processFacts.add(new ProcessFact(p.role(),p.operation(),p.identity().pid(),p.identity().startedAt(),state));
        }
        Integrity integrity=Integrity.NOT_REQUESTED;
        if(guard!=null){
            integrity=Integrity.UNSUPPORTED_INCOMPLETE_PREFIX;
            if(s.ready()!=null && (!s.files().isEmpty() || !s.fetchFiles().isEmpty()))try{verifyFiles(s,guard);verifyFiles(s,guard);integrity=s.prefix()==Prefix.OPERATION_INCOMPLETE || s.prefix()==Prefix.BRONZE_INCOMPLETE || s.prefix()==Prefix.FETCH_INCOMPLETE?Integrity.UNSUPPORTED_INCOMPLETE_PREFIX:Integrity.CURRENT_BYTES_AND_INVENTORY_MATCH;}
            catch(RuntimeException ex){integrity=Integrity.UNAVAILABLE_OR_CHANGED;}
        }
        // A changing journal is not a stable assessment. This second bounded pass also detects
        // append-only progress during filesystem/process IO without taking execution locks.
        Snapshot after=tx.required(()->snapshot(jdbc,tx,run,attempt));
        require(s.signature().equals(after.signature()),WORKER_NOT_READY);
        return new Assessment(run,attempt,s.prefix(),s.facts(),integrity,processFacts,s.artifacts(),List.of(
            "Historical candidate inventories were hashed, not retained as independently reopenable identity records.",
            "Current byte/inventory matching does not establish uninterrupted custody or native-store integrity.",
            "Recorded processes omit possible launch gaps, descendants and unmanaged writers; observations never establish trusted-local quiescence.",
            "Execution handlers and all writers require separate trusted-local review; no continuation or recovery authority is granted."));
    }
    private static Snapshot snapshot(JdbcTemplate j,ManagementTransactions tx,UUID run,UUID attempt){
        require(!j.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.content_operations WHERE attempt_id=?) OR EXISTS(SELECT 1 FROM ddip_management.stage_invocations WHERE attempt_id=?) OR EXISTS(SELECT 1 FROM ddip_management.rss_deliveries WHERE run_id=?)",Boolean.class,attempt,attempt,run),RUNTIME_MISMATCH);
        var admission=NarrowContentAdmission.inspectHistorical(j,run,attempt);var ready=admission.ready();
        var owner=one(j.queryForList("SELECT a.instance_id,a.owner_epoch,a.state AS attempt_state,a.row_version AS attempt_version,a.finished_at AS attempt_finished,r.finished_at AS run_finished,r.counts_bytes,r.state AS run_state,r.row_version AS run_version,r.result_artifact_id,w.pid,w.process_started_at FROM ddip_management.attempts a JOIN ddip_management.runs r USING(run_id) JOIN ddip_management.worker_instances w USING(instance_id) WHERE a.attempt_id=? AND a.run_id=? LIMIT 2",attempt,run));
        var recorded=new ArrayList<Recorded>();recorded.add(new Recorded("WORKER",null,new RecoveryQuiescence.Identity(((Number)owner.get("pid")).longValue(),((java.sql.Timestamp)owner.get("process_started_at")).toInstant())));
        var artifacts=new ArrayList<ArtifactFact>();var facts=new ArrayList<OperationFact>();var files=new ArrayList<Fileset>();var fetchFiles=new HashMap<String,byte[]>();var signature=SourceJson.object().put("admission",admission.admissionSha256()).put("phases",admission.phases().toString()).put("owner",owner.entrySet().stream().filter(e->!e.getKey().equals("counts_bytes")).toList().toString());var signatures=signature.putArray("rows");
        Prefix prefix=Prefix.ADMISSION;
        UUID fetch=id(j,"content_native_operations",attempt),bronze=id(j,"content_bronze_reservations",attempt),silver=id(j,"content_silver_reservations",attempt);
        boolean fetchComplete=false,bronzeComplete=false,publicSource=false;
        String publicLaunchId=null,publicLaunchDigest=null;
        if(fetch!=null){
            require(ready!=null,RUNTIME_MISMATCH);var r=NarrowContentFetchReservation.inspect(j,tx,fetch);var l=NarrowContentFetchLaunch.inspect(j,tx,fetch);var c=NarrowContentFetchCompletion.inspect(j,tx,fetch);
            require(r.attempt().equals(attempt),RUNTIME_MISMATCH);fetchComplete=c.isPresent();prefix=fetchComplete?Prefix.FETCH_CHECKPOINT:Prefix.FETCH_INCOMPLETE;
            facts.add(new OperationFact(0,fetch,"FETCH_SOURCE",l.phases(),fetchComplete,r.commandSha256(),c.map(x->x.completionSha256()).orElse(null)));
            child(recorded,"FETCH_CHILD",fetch,l.child());
            var reservation=one(j.queryForList("SELECT bootstrap_bytes,bootstrap_sha256 FROM ddip_management.content_native_operations WHERE operation_id=? LIMIT 2",fetch));
            var bootstrap=canonical(reservation,"bootstrap",65535);
            if(bootstrap.get("reservationVersion").intValue()==2){
                // Commit the independently validated recorded launch/policy binding. Do not
                // reselect today's pipeline registration for an earlier consumed invocation.
                // V1 fingerprints remain byte-for-byte unchanged.
                publicSource=true;publicLaunchId=bootstrap.get("publicFetch").get("launchProfileId").stringValue();
                publicLaunchDigest=bootstrap.get("publicFetch").get("launchProfileSha256").stringValue();
                signature.put("publicFetchReservationSha256",(String)reservation.get("bootstrap_sha256"));
            }
            if(fetchComplete){
                fetchFiles.put("command",NarrowContentFetchReservation.reconstruct(j,tx,ready,fetch).commandBytes());
                var cp=one(j.queryForList("SELECT journal_descriptor,expected_evidence FROM ddip_management.content_fetch_checkpoints WHERE operation_id=? LIMIT 2",fetch));
                fetchFiles.put("descriptor",(byte[])cp.get("journal_descriptor"));fetchFiles.put("reference",(byte[])cp.get("expected_evidence"));
            }
        }
        if(bronze!=null){
            require(fetchComplete,RUNTIME_MISMATCH);var r=NarrowContentBronzeReservation.inspect(j,tx,bronze);var l=NarrowContentBronzeLaunch.inspect(j,tx,bronze);var c=NarrowContentBronzeCompletion.inspect(j,tx,bronze);
            if(l.launchProfileId()!=null){
                var profile=ContentFetchLaunchRuntime.read(j,tx,"content_bronze_launch_profiles","launch_profile_id",l.launchProfileId());
                require(profile.digest().equals(l.launchProfileSha256())
                        && profile.configuration().get("strictBronzeLaunchVersion").intValue()==(publicSource?2:1),RUNTIME_MISMATCH);
                if(publicSource){
                    require(profile.configuration().get("fetchLaunchProfileId").stringValue().equals(publicLaunchId)
                            && profile.configuration().get("fetchLaunchProfileSha256").stringValue().equals(publicLaunchDigest),RUNTIME_MISMATCH);
                    signature.put("publicBronzeLaunchProfileSha256",profile.digest());
                }
            }
            require(r.attempt().equals(attempt) && r.fetchOperation().equals(fetch),RUNTIME_MISMATCH);bronzeComplete=c.isPresent();prefix=bronzeComplete?Prefix.BRONZE_CHECKPOINT:Prefix.BRONZE_INCOMPLETE;
            facts.add(new OperationFact(1,bronze,"STORE_BRONZE",l.phases(),bronzeComplete,r.commandSha256(),c.map(x->x.verifiedSha256()).orElse(null)));child(recorded,"BRONZE_CHILD",bronze,l.child());
            for(var row:j.queryForList("SELECT child_pid,child_started_at FROM ddip_management.content_bronze_verification_phases WHERE operation_id=? AND phase='CHILD_RECORDED' LIMIT 2",bronze))
                recorded.add(new Recorded("BRONZE_VERIFIER",bronze,new RecoveryQuiescence.Identity(((Number)row.get("child_pid")).longValue(),((java.sql.Timestamp)row.get("child_started_at")).toInstant())));
        }
        if(bronze!=null){
            var verification=j.queryForList("SELECT * FROM ddip_management.content_bronze_verification_phases WHERE operation_id=? LIMIT 6",bronze);
            var ordered=List.of("VERIFY_BOUND","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","OBSERVED");require(verification.size()<=5,RUNTIME_MISMATCH);
            verification.sort(Comparator.comparingInt(v->ordered.indexOf((String)v.get("phase"))));require(verification.stream().map(v->(String)v.get("phase")).toList().equals(ordered.subList(0,verification.size())),RUNTIME_MISMATCH);
            for(var v:verification){
                if("VERIFY_BOUND".equals(v.get("phase"))){
                    var profile=ContentFetchLaunchRuntime.read(j,tx,"content_bronze_verification_profiles","verification_profile_id",(String)v.get("verification_profile_id"));
                    var registered=NarrowContentRuntimeQualification.inspect(j,tx,ready);
                    StrictBronzeVerificationRuntime.validateRegistration(j,tx,profile.configuration(),registered.registration(),ready.host());
                    require(profile.digest().equals(v.get("verification_profile_sha256"))
                            && profile.configuration().get("strictBronzeVerificationVersion").intValue()==(publicSource?2:1),RUNTIME_MISMATCH);
                    if(publicSource){
                        require(profile.configuration().get("fetchLaunchProfileId").stringValue().equals(publicLaunchId)
                                && profile.configuration().get("fetchLaunchProfileSha256").stringValue().equals(publicLaunchDigest),RUNTIME_MISMATCH);
                        signature.put("publicBronzeVerificationProfileSha256",profile.digest());
                    }
                }
                for(String key:List.of("receipt","verified"))if(v.get(key+"_bytes")!=null)canonical(v,key,65536);
                signatures.add(v.entrySet().stream().filter(e->!e.getKey().endsWith("_bytes")).toList().toString());
            }
        }
        if(bronzeComplete){
            var material=NarrowContentBronzeReservation.historicalMaterial(j,tx,ready,bronze);
            var cp=one(j.queryForList("SELECT receipt_bytes,verified_stamp_bytes FROM ddip_management.content_bronze_checkpoints WHERE operation_id=? LIMIT 2",bronze));
            var observed=one(j.queryForList("SELECT verified_bytes FROM ddip_management.content_bronze_verification_phases WHERE operation_id=? AND phase='OBSERVED' LIMIT 2",bronze));
            var ev=SourceJson.object().put("bronzeEvidenceVersion",1).put("commandSha256",SourceJson.sha(material.command())).put("state","EXACT");ev.set("receipt",PreflightFiles.canonical((byte[])cp.get("receipt_bytes"),65536));
            var command=PreflightFiles.canonical(material.command(),65536);var result=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,command.get(key));result.put("status","OK").putNull("code");result.set("evidenceId",command.get("checkpointId"));result.putObject("counts");
            files.add(new Fileset(new NarrowContentEvidenceContext(ready).originalDirectory("STORE_BRONZE",0),Map.of("command.json",material.command(),"evidence.json",SourceJson.canonical(ev),"result.json",SourceJson.canonical(result),"verified.json",(byte[])observed.get("verified_bytes")),null));
        }
        if(silver!=null){require(bronzeComplete,RUNTIME_MISMATCH);var r=NarrowContentSilverReservation.inspect(j,tx,silver);require(r.attempt().equals(attempt) && r.bronzeOperation().equals(bronze),RUNTIME_MISMATCH);prefix=Prefix.SILVER_RESERVED;signatures.add(r.reservationSha256());}
        var rows=j.queryForList("SELECT * FROM ddip_management.content_pipeline_operations WHERE attempt_id=? ORDER BY step_index LIMIT 16",attempt);
        require(rows.size()<=15 && (rows.isEmpty() || silver!=null),RUNTIME_MISMATCH);
        var commands=new HashMap<Integer,JsonNode>();var evidence=new HashMap<Integer,JsonNode>();var proofs=new ArrayList<String>();var ids=new ArrayList<UUID>();
        var runtime=rows.isEmpty()?null:NarrowContentRuntimeQualification.inspect(j,tx,ready);
        var context=ready==null?null:new NarrowContentEvidenceContext(ready);
        JsonNode receipt=bronzeComplete?canonical(one(j.queryForList("SELECT receipt_bytes,receipt_sha256 FROM ddip_management.content_bronze_checkpoints WHERE operation_id=? LIMIT 2",bronze)),"receipt",65536):null;
        String credentials=bronzeComplete?bytes(one(j.queryForList("SELECT command_bytes FROM ddip_management.content_bronze_reservations WHERE operation_id=? LIMIT 2",bronze)),"command",65536).get("storage").get("credentialsSha256").stringValue():null;
        JsonNode silverInput=null,goldProof=null;UUID predecessor=bronze;boolean previousComplete=true;Map<String,Object> previousVersion=null;
        for(int index=0;index<rows.size();index++){
            var row=rows.get(index);if(previousVersion!=null)versions(previousVersion,row);int step=index+2;var op=Operation.values()[step];UUID operation=(UUID)row.get("operation_id");
            require(previousComplete && ((Number)row.get("step_index")).intValue()==step && op.ledgerStage.equals(row.get("stage")) && ((Number)row.get("item_ordinal")).intValue()==op.ledgerOrdinal
                && bronze.equals(row.get("bronze_operation_id")) && silver.equals(row.get("silver_reservation_id")) && predecessor.equals(row.get("predecessor_operation_id")),RUNTIME_MISMATCH);
            var command=canonical(row,"command",MB);commands.put(step,command);
            var directory=context.originalDirectory(op.ledgerStage,op.ledgerOrdinal);var origin=op.nativeCheckpointOrigin();
            require(directory.getFileName().toString().equals(row.get("checkpoint_id").toString()) && context.originalDirectory(origin.ledgerStage,origin.ledgerOrdinal).getFileName().toString().equals(row.get("native_checkpoint_id").toString()),RUNTIME_MISMATCH);
            var profile=one(j.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_pipeline_profiles WHERE profile_id=? LIMIT 2",row.get("profile_id")));
            var config=canonical(profile,"configuration",65536);validateProfile(j,tx,config,ready,runtime);
            require(config.get("strictContentPipelineVersion").intValue()==(publicSource?2:1),RUNTIME_MISMATCH);
            if(publicSource){
                publicLinks(j,fetch,bronze,config);
                signatures.add("public-pipeline-profile:"+row.get("profile_id")+":"+profile.get("configuration_sha256"));
            }
            require(row.get("profile_sha256").equals(profile.get("configuration_sha256")) && config.get("runProfileSha256").stringValue().equals(ready.frozen().profileSha256()) && config.get("contentRuntimeSha256").stringValue().equals(ready.frozen().runtimeSha256()) && config.get("strictRuntimeSha256").stringValue().equals(StrictContentPipelineRuntime.protocolDigest()),RUNTIME_MISMATCH);
            if(step==7)silverInput=silverInput(j,commands.get(4),evidence.get(4),receipt,commands.get(2),run,ready.source(),bytes(one(j.queryForList("SELECT command_bytes FROM ddip_management.content_bronze_reservations WHERE operation_id=? LIMIT 2",bronze)),"command",65536).get("storage").get("credentialsSha256"));
            if(step==13){var artifact=NarrowContentPipeline.artifact("GOLD",commands.get(9),evidence.get(10));var a=artifact(j,artifact,"GOLD");goldProof=SourceJson.object().put("artifactId",a.get("artifact_id").toString()).put("evidenceSha256",(String)a.get("evidence_sha256"));((tools.jackson.databind.node.ObjectNode)goldProof).set("artifact",artifact);((tools.jackson.databind.node.ObjectNode)goldProof).set("command",commands.get(9));}
            JsonNode prepared=op.family==NarrowContentOperationRouting.Family.GOLD && step!=7?value(evidence.get(7),"prepared"):op.family==NarrowContentOperationRouting.Family.CONTENT && step!=13?value(evidence.get(13),"prepared"):null;
            JsonNode verified=step==11?evidence.get(10):step==16?evidence.get(15):null;
            var expected=NarrowContentPipelineCommands.command(op,ready,runtime,((Number)owner.get("owner_epoch")).longValue(),receipt,silverInput,goldProof,prepared,verified,step==5?value(evidence.get(4),"capture"):null);
            require(Arrays.equals((byte[])row.get("command_bytes"),SourceJson.canonical(expected)),RUNTIME_MISMATCH);
            var phases=j.queryForList("SELECT * FROM ddip_management.content_pipeline_phases WHERE operation_id=? LIMIT 7",operation);require(phases.size()<=6,RUNTIME_MISMATCH);
            phases.sort(Comparator.comparingInt(p->PHASES.indexOf((String)p.get("phase"))));var names=phases.stream().map(p->(String)p.get("phase")).toList();require(names.equals(PHASES.subList(0,names.size())),RUNTIME_MISMATCH);
            String candidate=null;Map<String,Object> version=row;String childPid=null;
            for(var p:phases){versions(version,p);version=p;var n=canonical(p,"phase",65536);SourceJson.fields(n,"phaseVersion","operationId","phase","profileSha256","commandSha256","child","bootstrapSha256","candidateSha256");
                require(n.get("phaseVersion").isInt() && n.get("phaseVersion").intValue()==1 && n.get("operationId").stringValue().equals(operation.toString()) && n.get("phase").stringValue().equals(p.get("phase")) && n.get("commandSha256").stringValue().equals(row.get("command_sha256")) && n.get("profileSha256").stringValue().equals(row.get("profile_sha256")),RUNTIME_MISMATCH);
                String phase=(String)p.get("phase");if(phase.equals("CHILD_RECORDED")){var c=n.get("child");SourceJson.fields(c,"instanceId","pid","startedAt");UUID.fromString(c.get("instanceId").stringValue());childPid=c.get("pid").stringValue();recorded.add(new Recorded("PIPELINE_CHILD",operation,new RecoveryQuiescence.Identity(Long.parseLong(c.get("pid").stringValue()),Instant.parse(c.get("startedAt").stringValue()))));}else require(n.get("child").isNull(),RUNTIME_MISMATCH);
                if(phase.equals("CANDIDATE_EXITED")){candidate=n.get("candidateSha256").stringValue();hash(candidate);}else require(n.get("candidateSha256").isNull(),RUNTIME_MISMATCH);
                if(phase.equals("RELEASE_AUTHORIZED")){hash(n.get("bootstrapSha256").stringValue());require(bootstrapDigest(ready,owner,operation,op,command,runtime,credentials,childPid).equals(n.get("bootstrapSha256").stringValue()),RUNTIME_MISMATCH);}else require(n.get("bootstrapSha256").isNull(),RUNTIME_MISMATCH);
                signatures.add((String)p.get("phase_sha256"));
            }
            var checkpoints=j.queryForList("SELECT * FROM ddip_management.content_pipeline_checkpoints WHERE operation_id=? LIMIT 2",operation);require(checkpoints.size()<=1,RUNTIME_MISMATCH);previousComplete=!checkpoints.isEmpty();String digest=null;
            if(previousComplete){require(names.equals(PHASES),RUNTIME_MISMATCH);var cp=checkpoints.getFirst();versions(version,cp);version=cp;var proof=canonical(cp,"proof",3*MB);digest=(String)cp.get("proof_sha256");
                var result=bytes(cp,"result",65536);var ev=optional(cp,"evidence",MB);var selection=optional(cp,"selection",MB);evidence.put(step,ev);validateResult(op,command,result,ev,selection,evidence);
                var expectedProof=SourceJson.object().put("pipelineCheckpointVersion",1).put("operationId",operation.toString()).put("stepIndex",step).put("commandSha256",(String)row.get("command_sha256")).put("candidateSha256",candidate);
                expectedProof.set("result",result);expectedProof.set("evidence",ev==null?SourceJson.parse("null".getBytes()):ev);expectedProof.set("selection",selection==null?SourceJson.parse("null".getBytes()):selection);require(proof.equals(expectedProof),RUNTIME_MISMATCH);
                require(result.get("commandSha256").stringValue().equals(row.get("command_sha256")) && result.get("checkpointId").stringValue().equals(row.get("checkpoint_id").toString()) && result.get("nativeCheckpointId").equals(command.get("checkpointId")) && result.get("family").stringValue().equals(op.family.name()) && result.get("mode").stringValue().equals(op.mode) && result.get("runId").equals(command.get("runId")) && result.get("attemptId").equals(command.get("attemptId")) && result.get("candidateOnly").asBoolean() && result.get("code").isNull(),RUNTIME_MISMATCH);
                require(ev==null?result.get("evidenceSha256").isNull():SourceJson.sha((byte[])cp.get("evidence_bytes")).equals(result.get("evidenceSha256").stringValue()),RUNTIME_MISMATCH);
                require(selection==null?result.get("selectionSha256").isNull():SourceJson.sha((byte[])cp.get("selection_bytes")).equals(result.get("selectionSha256").stringValue()),RUNTIME_MISMATCH);
                var fs=new LinkedHashMap<String,byte[]>();fs.put("command.json",(byte[])row.get("command_bytes"));fs.put("result.json",(byte[])cp.get("result_bytes"));if(ev!=null)fs.put("evidence.json",(byte[])cp.get("evidence_bytes"));if(selection!=null)fs.put("selection.json",(byte[])cp.get("selection_bytes"));files.add(new Fileset(directory,Map.copyOf(fs),candidate));
            }
            previousVersion=version;facts.add(new OperationFact(step,operation,op.ledgerStage,names,previousComplete,(String)row.get("command_sha256"),digest));proofs.add(digest);ids.add(operation);predecessor=operation;prefix=previousComplete?Prefix.OPERATION_CHECKPOINT:Prefix.OPERATION_INCOMPLETE;
        }
        if(evidence.containsKey(5)){
            var q=SourceJson.object().put("qualificationVersion",1);q.set("selection",commands.get(4).get("artifact"));q.set("verification",evidence.get(4));collectArtifact(j,q,"SILVER",artifacts);
        }
        if(evidence.containsKey(11))collectArtifact(j,NarrowContentPipeline.artifact("GOLD",commands.get(9),evidence.get(10)),"GOLD",artifacts);
        var completed=j.queryForList("SELECT * FROM ddip_management.content_pipeline_completions WHERE attempt_id=? LIMIT 2",attempt);require(completed.size()<=1,RUNTIME_MISMATCH);
        if(!completed.isEmpty()){
            require(rows.size()==15 && previousComplete,RUNTIME_MISMATCH);var c=completed.getFirst();versions(previousVersion,c);var p=canonical(c,"proof",65536);require(((Number)owner.get("attempt_version")).longValue()==((Number)c.get("owner_attempt_version")).longValue()+1 && ((Number)owner.get("run_version")).longValue()==((Number)c.get("owner_run_version")).longValue()+1,RUNTIME_MISMATCH);var expected=SourceJson.object().put("pipelineCompletionVersion",1).put("runId",run.toString()).put("attemptId",attempt.toString()).put("fetchOperationId",fetch.toString()).put("bronzeOperationId",bronze.toString()).put("artifactId",c.get("artifact_id").toString());
            var artifact=NarrowContentPipeline.artifact("CONTENT",commands.get(14),evidence.get(15));var a=artifact(j,artifact,"CONTENT");require(a.get("artifact_id").equals(c.get("artifact_id")),RUNTIME_MISMATCH);expected.put("artifactSha256",SourceJson.sha(SourceJson.canonical(artifact))).put("fetchCheckpointSha256",facts.get(0).proofSha256()).put("bronzeVerifiedSha256",facts.get(1).proofSha256());var os=expected.putArray("operations");for(int i=0;i<ids.size();i++)os.addObject().put("operationId",ids.get(i).toString()).put("stepIndex",i+2).put("proofSha256",proofs.get(i));
            require(p.equals(expected) && c.get("final_operation_id").equals(ids.getLast()) && "SUCCEEDED".equals(owner.get("attempt_state")) && "SUCCEEDED".equals(owner.get("run_state")) && owner.get("result_artifact_id").equals(c.get("artifact_id")) && owner.get("attempt_finished")!=null && owner.get("run_finished")!=null,RUNTIME_MISMATCH);var counts=artifact.get("verification").get("completion").get("output_counts");var expectedCounts=SourceJson.object().put("articles",counts.get("content_catalog_v1").asText()).put("rows",counts.get("content_source_discovery_daily_v1").asText());require(Arrays.equals((byte[])owner.get("counts_bytes"),SourceJson.canonical(expectedCounts)),RUNTIME_MISMATCH);
            var reference=commands.get(14).get("reference");var ref=one(j.queryForList("SELECT evidence_bytes,evidence_sha256 FROM ddip_management.artifacts WHERE artifact_id=? AND kind='PUBLISHER_REFERENCE' LIMIT 2",UUID.fromString(reference.get("artifactId").stringValue())));
            require(Arrays.equals((byte[])ref.get("evidence_bytes"),SourceJson.canonical(reference.get("qualification"))) && ref.get("evidence_sha256").equals(reference.get("evidenceSha256").stringValue()),RUNTIME_MISMATCH);
            require(j.queryForList("SELECT role FROM ddip_management.artifact_dependencies WHERE artifact_id=? LIMIT 1",String.class,UUID.fromString(reference.get("artifactId").stringValue())).isEmpty(),RUNTIME_MISMATCH);
            collectArtifact(j,artifact,"CONTENT",artifacts);signatures.add((String)c.get("proof_sha256"));prefix=Prefix.TERMINAL_SUCCESS;
        }else require(!"SUCCEEDED".equals(owner.get("attempt_state")) && !"SUCCEEDED".equals(owner.get("run_state")),RUNTIME_MISMATCH);
        for(var f:facts)signatures.add(f.toString());for(var p:recorded)signatures.add(p.toString());for(var a:artifacts)signatures.add(a.toString());
        return new Snapshot(prefix,ready,List.copyOf(facts),List.copyOf(recorded),List.copyOf(files),Map.copyOf(fetchFiles),List.copyOf(artifacts),SourceJson.sha(SourceJson.canonical(signature)));
    }
    private static void publicLinks(JdbcTemplate jdbc,UUID fetch,UUID bronze,JsonNode config){
        var links=List.of(new String[]{"content_native_launch_phases","launch_profile_id","launch_profile_sha256","LAUNCH_BOUND","fetchLaunchProfile"},
                new String[]{"content_bronze_launch_phases","launch_profile_id","launch_profile_sha256","LAUNCH_BOUND","bronzeLaunchProfile"},
                new String[]{"content_bronze_verification_phases","verification_profile_id","verification_profile_sha256","VERIFY_BOUND","bronzeVerificationProfile"});
        for(int index=0;index<links.size();index++){
            UUID operation=index==0?fetch:bronze;if(operation==null)continue;var link=links.get(index);
            var rows=jdbc.queryForList("SELECT "+link[1]+","+link[2]+" FROM ddip_management."+link[0]+" WHERE operation_id=? AND phase=? LIMIT 2",operation,link[3]);
            require(rows.size()<=1,RUNTIME_MISMATCH);
            if(!rows.isEmpty())require(config.get(link[4]+"Id").stringValue().equals(rows.getFirst().get(link[1]))
                    && config.get(link[4]+"Sha256").stringValue().equals(rows.getFirst().get(link[2])),RUNTIME_MISMATCH);
        }
    }
    private static void validateProfile(JdbcTemplate jdbc,ManagementTransactions tx,JsonNode c,NarrowContentEvidenceContext.Expected ready,NarrowContentRuntimeQualification.Snapshot runtime){
        if(c.get("strictContentPipelineVersion")!=null && c.get("strictContentPipelineVersion").isInt() && c.get("strictContentPipelineVersion").intValue()==2){
            StrictContentPipelineRuntime.validateComposition(jdbc,tx,c,runtime.registration(),ready.host());
            for(String name:c.get("modules").propertyNames()){require(name.matches("[A-Za-z0-9_]+[.]py"),RUNTIME_MISMATCH);hash(c.get("modules").get(name).stringValue());}
            for(String name:List.of("strict_native_content.py","strict_native_runtime.py","strict_native_sink.py"))require(c.get("modules").has(name),RUNTIME_MISMATCH);
            return;
        }
        SourceJson.fields(c,"strictContentPipelineVersion","runProfileId","runProfileSha256","contentRuntimeId","contentRuntimeSha256","controlBindingId","strictRuntimeSha256","python","processingRoot","javaHome","modules","versions","fetchLaunchProfileId","bronzeLaunchProfileId","bronzeVerificationProfileId");
        require(c.get("strictContentPipelineVersion").isInt() && c.get("strictContentPipelineVersion").intValue()==1 && c.get("runProfileId").equals(runtime.frozenSelection().get("executionProfile").get("profileId")) && c.get("contentRuntimeId").stringValue().equals(runtime.id(NarrowContentRuntimeQualification.Family.CONTENT)) && c.get("controlBindingId").stringValue().equals(ready.host().id()),RUNTIME_MISMATCH);
        for(String key:List.of("python","processingRoot","javaHome"))require(c.get(key).equals(runtime.configuration("SILVER").get(key)),RUNTIME_MISMATCH);
        require(c.get("versions").equals(StrictContentPipelineRuntime.versions()) && c.get("modules").isObject() && c.get("modules").size()>0 && c.get("modules").size()<=128,RUNTIME_MISMATCH);
        for(String name:c.get("modules").propertyNames()){require(name.matches("[A-Za-z0-9_]+[.]py"),RUNTIME_MISMATCH);hash(c.get("modules").get(name).stringValue());}
        for(String name:List.of("strict_native_content.py","strict_native_runtime.py","strict_native_sink.py"))require(c.get("modules").has(name),RUNTIME_MISMATCH);
        for(String key:List.of("fetchLaunchProfileId","bronzeLaunchProfileId","bronzeVerificationProfileId"))require(c.get(key).stringValue().matches("[A-Za-z0-9_-]{1,128}"),RUNTIME_MISMATCH);
    }
    private static void versions(Map<String,Object> earlier,Map<String,Object> later){
        for(String key:List.of("owner_run_version","owner_attempt_version"))require(((Number)earlier.get(key)).longValue()==((Number)later.get(key)).longValue(),RUNTIME_MISMATCH);
        for(String key:List.of("owner_coordinator_version","owner_worker_version"))require(((Number)earlier.get(key)).longValue()<=((Number)later.get(key)).longValue(),RUNTIME_MISMATCH);
    }
    /** Rebuild only digest material, never an Owner or executable bootstrap object. */
    private static String bootstrapDigest(NarrowContentEvidenceContext.Expected e,Map<String,Object> owner,UUID operation,Operation op,JsonNode command,NarrowContentRuntimeQualification.Snapshot runtime,String credentials,String childPid){
        var n=SourceJson.object().put("bootstrapVersion",2).put("mode","STRICT_CONTENT").put("operationId",operation.toString())
            .put("installationId",e.host().installationId().toString()).put("policy",e.policy()).put("planKind","ORIGINAL").put("runId",e.run().toString()).put("attemptId",e.attempt().toString()).put("sourceId",e.source())
            .put("controlRoot",e.host().root().toString()).put("controlDevice",Long.toString(e.host().rootIdentity().device())).put("controlInode",Long.toString(e.host().rootIdentity().inode()))
            .put("rootDevice",Long.toString(e.rootIdentity().device())).put("rootInode",Long.toString(e.rootIdentity().inode())).put("lockDevice",Long.toString(e.lockIdentity().device())).put("lockInode",Long.toString(e.lockIdentity().inode()))
            .put("ledgerSha256",e.ledgerSha256()).put("selectionSha256",e.frozen().selectionSha256()).put("profileSha256",e.frozen().profileSha256()).put("runtimeSha256",e.frozen().runtimeSha256())
            .put("workerInstanceId",owner.get("instance_id").toString()).put("ownerEpoch",owner.get("owner_epoch").toString()).put("stage",op.ledgerStage).put("ordinal",op.ledgerOrdinal)
            .put("checkpointId",new NarrowContentEvidenceContext(e).originalDirectory(op.ledgerStage,op.ledgerOrdinal).getFileName().toString()).put("commandSha256",SourceJson.sha(SourceJson.canonical(command)))
            .put("family",op.family.name()).put("nativeMode",op.mode).put("nativeRuntimeSha256",runtime.digest(op.family.name())).put("strictRuntimeSha256",StrictContentPipelineRuntime.protocolDigest()).put("childPid",childPid);
        n.set("expectations",NarrowContentPipelineCommands.expectations(op,runtime,command,credentials));byte[] raw=SourceJson.canonical(n);require(raw.length<65536,RUNTIME_MISMATCH);return SourceJson.sha(raw);
    }
    private static void validateResult(Operation op,JsonNode command,JsonNode result,JsonNode evidence,JsonNode selection,Map<Integer,JsonNode> earlier){
        SourceJson.fields(result,"protocolVersion","family","mode","runId","attemptId","ownerEpoch","checkpointId","nativeCheckpointId","runtimeSha256","strictRuntimeSha256","commandSha256","candidateOnly","status","evidenceSha256","selectionSha256","cleanup","code");
        require(result.get("protocolVersion").isInt() && result.get("protocolVersion").intValue()==2 && result.get("candidateOnly").isBoolean() && result.get("candidateOnly").booleanValue()
            && result.get("ownerEpoch").equals(command.get("ownerEpoch")) && result.get("runtimeSha256").equals(command.get("runtimeSha256")) && result.get("strictRuntimeSha256").stringValue().equals(StrictContentPipelineRuntime.protocolDigest()),RUNTIME_MISMATCH);
        String expected=op==Operation.PUBLISH_SILVER?"PROCESSED":op==Operation.RECHECK_SILVER || op==Operation.RECHECK_GOLD?"UNCHANGED":op.mode.equals("PREPARE")?"PREPARED":"VERIFIED";
        require(expected.equals(result.get("status").stringValue()),RUNTIME_MISMATCH);var cleanup=result.get("cleanup");SourceJson.fields(cleanup,"status","gatewayPid");
        if(NarrowContentPipeline.spark(op))require("CONFIRMED".equals(cleanup.get("status").stringValue()) && cleanup.get("gatewayPid").isString() && cleanup.get("gatewayPid").stringValue().matches("[1-9][0-9]{0,18}"),RUNTIME_MISMATCH);
        else require("NOT_STARTED".equals(cleanup.get("status").stringValue()) && cleanup.get("gatewayPid").isNull(),RUNTIME_MISMATCH);
        if(op.family==NarrowContentOperationRouting.Family.BRONZE){var expectedEvidence=SourceJson.object().put("status","VERIFIED");expectedEvidence.set("receipt",command.get("receipt"));require(expectedEvidence.equals(evidence) && selection==null,RUNTIME_MISMATCH);}
        else if(op==Operation.PUBLISH_SILVER || op==Operation.RECHECK_SILVER)require(evidence==null && selection==null,RUNTIME_MISMATCH);
        else if(op==Operation.VERIFY_SILVER){require(evidence!=null && selection!=null,RUNTIME_MISMATCH);var q=SourceJson.object().put("qualificationVersion",1);q.set("selection",command.get("artifact"));q.set("verification",evidence);NarrowContentProfile.silverQualification(q);NarrowContentProfile.silverSelection(selection);require(selection.get("inputSetFingerprint").stringValue().equals(SourceJson.sha(SourceJson.canonical(selection.get("manifest")))) && selection.get("manifest").get("inputs").size()==1 && selection.get("manifest").get("inputs").get(0).get("marker_path").equals(command.get("artifact").get("relativeAddress")),RUNTIME_MISMATCH);}
        else {require(evidence!=null && selection==null,RUNTIME_MISMATCH);
            if(op.mode.equals("PREPARE"))require("PREPARED".equals(evidence.get("status").stringValue()) && evidence.get("prepared").isObject(),RUNTIME_MISMATCH);
            if(op==Operation.RECHECK_GOLD)require(evidence.equals(SourceJson.object().put("status","UNCHANGED")),RUNTIME_MISMATCH);
            if(op.mode.equals("PUBLISH") || op.mode.equals("VERIFY"))require("VERIFIED".equals(evidence.get("status").stringValue()) && evidence.get("prepared").equals(command.get("prepared")),RUNTIME_MISMATCH);
            if(op==Operation.VERIFY_GOLD)require(evidence.equals(earlier.get(9)),RUNTIME_MISMATCH);
            if(op==Operation.VERIFY_CONTENT)require(evidence.equals(earlier.get(14)),RUNTIME_MISMATCH);
            if(op==Operation.RECHECK_CONTENT)require(evidence.equals(earlier.get(15)),RUNTIME_MISMATCH);
        }
    }
    private static JsonNode silverInput(JdbcTemplate j,JsonNode command,JsonNode verification,JsonNode receipt,JsonNode bronze,UUID run,String source,JsonNode credentials){
        var qualification=SourceJson.object().put("qualificationVersion",1);qualification.set("selection",command.get("artifact"));qualification.set("verification",verification);var a=artifact(j,qualification,"SILVER");
        var result=SourceJson.object().put("artifactId",a.get("artifact_id").toString()).put("originRunId",run.toString()).put("sourceId",source).put("evidenceSha256",(String)a.get("evidence_sha256"));result.set("qualification",qualification);result.set("receipt",receipt);result.set("bronze",bronze.get("bronze"));result.set("credentialsSha256",credentials);return result;
    }
    private static void collectArtifact(JdbcTemplate j,JsonNode expected,String kind,List<ArtifactFact> out){
        var selected=kind.equals("SILVER")?expected.get("selection"):expected;var rows=j.queryForList("SELECT artifact_id FROM ddip_management.artifacts WHERE storage_binding_id=? AND relative_address=? LIMIT 2",UUID.class,selected.get("storageBindingId").stringValue(),selected.get("relativeAddress").stringValue());
        require(rows.size()<=1,RUNTIME_MISMATCH);if(!rows.isEmpty()){var a=artifact(j,expected,kind);out.add(new ArtifactFact(kind,(UUID)a.get("artifact_id"),(String)a.get("evidence_sha256")));}
    }
    private static Map<String,Object> artifact(JdbcTemplate j,JsonNode expected,String kind){
        byte[] raw=SourceJson.canonical(expected);var selected=kind.equals("SILVER")?expected.get("selection"):expected;var row=one(j.queryForList("SELECT artifact_id,kind,evidence_bytes,evidence_sha256,product_id,contract_version,processor_revision,storage_binding_id,relative_address,native_identity FROM ddip_management.artifacts WHERE storage_binding_id=? AND relative_address=? LIMIT 2",selected.get("storageBindingId").stringValue(),selected.get("relativeAddress").stringValue()));
        require(kind.equals(row.get("kind")) && SourceJson.sha(raw).equals(row.get("evidence_sha256")) && Arrays.equals(raw,(byte[])row.get("evidence_bytes")) && "content-intelligence".equals(row.get("product_id")) && ((Number)row.get("contract_version")).intValue()==1 && ((Number)row.get("processor_revision")).intValue()==1,RUNTIME_MISMATCH);
        require(selected.get("storageBindingId").stringValue().equals(row.get("storage_binding_id")) && selected.get("relativeAddress").stringValue().equals(row.get("relative_address")),RUNTIME_MISMATCH);
        String nativeId=kind.equals("SILVER")?expected.get("verification").get("inventoryIdentity").get("sha256").stringValue():expected.get("nativeFingerprint").stringValue();require(nativeId.equals(row.get("native_identity")),RUNTIME_MISMATCH);
        var deps=j.queryForList("SELECT role,ordinal,dependency_artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? ORDER BY role LIMIT 4",row.get("artifact_id"));require(deps.size()<=2,RUNTIME_MISMATCH);
        var expectedDeps=new TreeMap<String,UUID>();if(kind.equals("GOLD"))expectedDeps.put("SILVER",UUID.fromString(expected.get("inputs").get(0).get("artifactId").stringValue()));
        if(kind.equals("CONTENT"))for(var item:expected.get("inputs"))expectedDeps.put(item.get("role").stringValue(),UUID.fromString(item.get("artifactId").stringValue()));
        var actual=new TreeMap<String,UUID>();for(var dep:deps){require(((Number)dep.get("ordinal")).intValue()==0,RUNTIME_MISMATCH);require(actual.put((String)dep.get("role"),(UUID)dep.get("dependency_artifact_id"))==null,RUNTIME_MISMATCH);}require(actual.equals(expectedDeps),RUNTIME_MISMATCH);return row;
    }
    private static void verifyFiles(Snapshot s,InstallationHostGuard guard){
        var context=new NarrowContentEvidenceContext(s.ready());context.inspect(guard,context.frozen());
        if(!s.fetchFiles().isEmpty()){
            Path directory=context.originalDirectory("FETCH_SOURCE",0);
            var journal=new RssJournalFiles(guard,s.ready().run(),s.ready().attempt(),UUID.fromString(directory.getFileName().toString()),s.ready().source(),guard.binding().root().relativize(directory).toString(),false);
            var evidence=journal.reopen(s.fetchFiles().get("command"),s.facts().getFirst().proofSha256());
            require(Arrays.equals(evidence.descriptorBytes(),s.fetchFiles().get("descriptor")) && Arrays.equals(evidence.checkpointReference(),s.fetchFiles().get("reference")),RUNTIME_MISMATCH);
        }
        for(var set:s.files()){
            try(var listing=Files.list(set.directory())){require(listing.limit(6).map(p->p.getFileName().toString()).collect(java.util.stream.Collectors.toSet()).equals(set.files().keySet()),RUNTIME_MISMATCH);}catch(java.io.IOException ex){throw new WorkerFailure(WORKER_NOT_READY);}
            var inventory=SourceJson.object();for(String name:new TreeSet<>(set.files().keySet())){
                Path p=set.directory().resolve(name);PreflightFiles.privateEntry(p,false);var before=InstallationHostGuard.identity(p);var modified=modified(p);byte[] raw=PreflightFiles.read(p,MB,true);
                require(Arrays.equals(raw,set.files().get(name)) && before.equals(InstallationHostGuard.identity(p)) && modified.equals(modified(p)),RUNTIME_MISMATCH);
                inventory.putObject(name).put("sha256",SourceJson.sha(raw)).put("bytes",raw.length).put("device",Long.toString(before.device())).put("inode",Long.toString(before.inode())).put("modified",modified);
            }if(set.candidate()!=null)require(SourceJson.sha(SourceJson.canonical(inventory)).equals(set.candidate()),RUNTIME_MISMATCH);
        }
        var allowedDirectories=new HashSet<Path>();allowedDirectories.add(context.root());
        for(var fact:s.facts()) {var op=Operation.values()[fact.stepIndex()];for(Path path=context.originalDirectory(op.ledgerStage,op.ledgerOrdinal);path.startsWith(context.root());path=path.getParent())allowedDirectories.add(path);}
        try(var walk=Files.walk(context.root(),4)) {var paths=walk.limit(222).toList();require(paths.size()<=221,RUNTIME_MISMATCH);for(var path:paths)if(Files.isDirectory(path,LinkOption.NOFOLLOW_LINKS))require(allowedDirectories.contains(path),RUNTIME_MISMATCH);}
        catch(java.io.IOException ex){throw new WorkerFailure(WORKER_NOT_READY);}
        context.inspect(guard,context.frozen());
    }
    private static String modified(Path p){try{return Files.getLastModifiedTime(p,LinkOption.NOFOLLOW_LINKS).toString();}catch(java.io.IOException ex){throw new WorkerFailure(WORKER_NOT_READY);}}
    private static UUID id(JdbcTemplate j,String table,UUID attempt){var rows=j.queryForList("SELECT operation_id FROM ddip_management."+table+" WHERE attempt_id=? LIMIT 2",UUID.class,attempt);require(rows.size()<=1,RUNTIME_MISMATCH);return rows.isEmpty()?null:rows.getFirst();}
    private static void child(List<Recorded> records,String role,UUID operation,InstallationHostGuard.ProcessIdentity child){if(child!=null)records.add(new Recorded(role,operation,new RecoveryQuiescence.Identity(child.pid(),child.startedAt())));}
    private static JsonNode value(JsonNode n,String field){return n==null?null:n.get(field);}
    private static JsonNode canonical(Map<String,Object> row,String key,int maximum){byte[] raw=(byte[])row.get(key+"_bytes");require(raw!=null && raw.length<=maximum && SourceJson.sha(raw).equals(row.get(key+"_sha256")),RUNTIME_MISMATCH);return PreflightFiles.canonical(raw,maximum);}
    private static JsonNode bytes(Map<String,Object> row,String key,int maximum){return PreflightFiles.canonical((byte[])row.get(key+"_bytes"),maximum);}
    private static JsonNode optional(Map<String,Object> row,String key,int maximum){return row.get(key+"_bytes")==null?null:bytes(row,key,maximum);}
    private static Map<String,Object> one(List<Map<String,Object>> rows){require(rows.size()==1,RUNTIME_MISMATCH);return rows.getFirst();}
    private static void hash(String hash){require(hash!=null && hash.matches("[a-f0-9]{64}"),RUNTIME_MISMATCH);}
    private NarrowContentPipelineRecoveryAssessment(){}
}
