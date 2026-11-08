package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentEvidenceContext.*;
import static platform.contentapi.execution.NarrowContentOperationRouting.Operation;

class NarrowContentTerminalEvidenceTest {
    @TempDir Path temp;
    InstallationHostGuard guard;Path root,directory;Expected ready;
    UUID run=UUID.randomUUID(),attempt=UUID.randomUUID();
    static final List<String> PHASES=List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED");
    byte[] command;
    @BeforeEach void setup()throws Exception {
        guard=InstallationHostGuard.acquire(OwnershipFixtures.host(temp.toRealPath()));
        ContentEvidenceBudget.initialize(guard,run,attempt);root=guard.binding().root().resolve("runs/"+run+"/attempts/"+attempt);
        ContentOperationPlan.original(root,run,attempt,"source").reserve();
        ready=new Expected(NarrowContentProfile.POLICY,run,attempt,PlanKind.ORIGINAL,"source",guard.binding(),InstallationHostGuard.identity(root),InstallationHostGuard.identity(root.resolve(ContentEvidenceBudget.LOCK)),new FrozenIdentities("a".repeat(64),"b".repeat(64),"c".repeat(64)),SourceJson.sha(Files.readAllBytes(root.resolve(ContentEvidenceBudget.MANIFEST))));
        directory=new NarrowContentEvidenceContext(ready).originalDirectory("FETCH_SOURCE",0);
        command=SourceJson.canonical(SourceJson.object().put("protocolVersion",1).put("runId",run.toString()).put("attemptId",attempt.toString()).put("stage","FETCH_SOURCE").put("itemOrdinal",0).put("checkpointId",directory.getFileName().toString()));
    }
    @AfterEach void close(){guard.close();}
    NarrowContentTerminalEvidence.OperationEvidence op(int phase,String candidate,byte[] anchor,boolean checkpoint){return new NarrowContentTerminalEvidence.OperationEvidence(Operation.FETCH_SOURCE,PHASES.subList(0,phase),command,candidate,Map.of(),anchor,null,checkpoint);}
    void directories()throws Exception {for(Path p=root;p.getNameCount()<directory.getNameCount();){p=directory.subpath(0,p.getNameCount()+1);p=Path.of("/").resolve(p);if(!Files.exists(p))Files.createDirectory(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));}}
    void write(String name,String text)throws Exception{directories();Files.writeString(directory.resolve(name),text);Files.setPosixFilePermissions(directory.resolve(name),PosixFilePermissions.fromString("rw-------"));}
    void command()throws Exception{write("command.json",new String(command,java.nio.charset.StandardCharsets.UTF_8));}
    NarrowContentTerminalEvidence.Observation inspect(NarrowContentTerminalEvidence.OperationEvidence op){return NarrowContentTerminalEvidence.inspect(guard,ready,List.of(op),"a".repeat(64));}
    void reject(NarrowContentTerminalEvidence.OperationEvidence op){assertThatThrownBy(()->inspect(op)).isInstanceOfAny(WorkerFailure.class,PreflightFailure.class);}
    @Test void sealedReadyAndReservationOnlyHaveNoOperationFiles()throws Exception{
        var empty=NarrowContentTerminalEvidence.inspect(guard,ready,List.of(),"a".repeat(64));assertThat(empty.entries()).isEqualTo(3);assertThat(empty.authorization()).isFalse();
        inspect(op(0,null,null,false));directories();reject(op(0,null,null,false));
    }
    @Test void launchBoundAcceptsMissingEmptyOrSinglePreparingCommandAndPreservesBytes()throws Exception{
        inspect(op(1,null,null,false));directories();inspect(op(1,null,null,false));write("command.json.preparing","{");var before=Files.getLastModifiedTime(directory.resolve("command.json.preparing"));inspect(op(1,null,null,false));assertThat(Files.readString(directory.resolve("command.json.preparing"))).isEqualTo("{");assertThat(Files.getLastModifiedTime(directory.resolve("command.json.preparing"))).isEqualTo(before);
        command();reject(op(1,null,null,false));Files.delete(directory.resolve("command.json.preparing"));inspect(op(1,null,null,false));
    }
    @Test void filesReadyRequiresExactCommandAndForbidsAnyChildFile()throws Exception{
        reject(op(2,null,null,false));command();for(int phase=2;phase<=4;phase++)inspect(op(phase,null,null,false));write("result.json.preparing","");reject(op(4,null,null,false));
    }
    @Test void releasedPayloadsMustFollowWriterOrderAndRetainResultClaim()throws Exception{
        command();inspect(op(5,null,null,false));write("payload.xml.preparing","<");reject(op(5,null,null,false));write("result.json.preparing","");inspect(op(5,null,null,false));write("metadata.json","{}");reject(op(5,null,null,false));Files.delete(directory.resolve("metadata.json"));Files.move(directory.resolve("payload.xml.preparing"),directory.resolve("payload.xml"));write("metadata.json.preparing","{");inspect(op(5,null,null,false));
        write("descriptor.json","{}");reject(op(5,null,null,false));
    }
    @Test void failedFinalResultRetainsEarlierPayloadButDoesNotBecomeCandidate()throws Exception{
        command();write("payload.xml","<rss/>");var r=SourceJson.parse(command);var result=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,r.get(key));result.put("status","FAILED").put("code","FETCH_FAILED").putNull("evidenceId").putObject("counts");write("result.json",new String(SourceJson.canonical(result),java.nio.charset.StandardCharsets.UTF_8));inspect(op(5,null,null,false));reject(op(6,"a".repeat(64),null,false));
    }
    @Test void candidateDigestAndCompletionIntentAreRequiredIndependently()throws Exception{
        command();write("payload.xml","<rss/>");write("metadata.json","{}");write("descriptor.json","{}");var r=SourceJson.parse(command);var result=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))result.set(key,r.get(key));result.put("status","OK").putNull("code");result.set("evidenceId",r.get("checkpointId"));result.putObject("counts");write("result.json",new String(SourceJson.canonical(result),java.nio.charset.StandardCharsets.UTF_8));
        String candidate=NarrowContentFetchLaunch.candidate(directory,command);inspect(op(6,candidate,null,false));reject(op(6,"a".repeat(64),null,false));write("complete.json.preparing","{");reject(op(6,candidate,null,false));inspect(op(6,candidate,"{}".getBytes(),false));reject(op(6,candidate,"{}".getBytes(),true));Files.delete(directory.resolve("complete.json.preparing"));write("complete.json","{}");inspect(op(6,candidate,"{}".getBytes(),true));write("payload.xml","changed");reject(op(6,candidate,"{}".getBytes(),true));
    }
    @Test void futureDirectoriesAliasesUnknownFilesAndOversizedPrefixVeto()throws Exception{
        command();Path future=new NarrowContentEvidenceContext(ready).originalDirectory("STORE_BRONZE",0).getParent();Files.createDirectory(future,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));reject(op(5,null,null,false));Files.delete(future);
        write("unknown","x");reject(op(5,null,null,false));Files.delete(directory.resolve("unknown"));Files.createLink(temp.resolve("alias"),directory.resolve("command.json"));reject(op(5,null,null,false));Files.delete(temp.resolve("alias"));
        write("result.json.preparing","x".repeat(65537));reject(op(5,null,null,false));
    }
    @Test void validPartialChangeAltersReviewInventoryWithoutPromotingOutput()throws Exception{
        command();write("result.json.preparing","");write("payload.xml.preparing","<");
        var before=inspect(op(5,null,null,false));write("payload.xml.preparing","<rss");var after=inspect(op(5,null,null,false));
        assertThat(after.inventorySha256()).isNotEqualTo(before.inventorySha256());assertThat(after.authorization()).isFalse();
        assertThat(after.historicalFilesystemIdentityEstablished()).isFalse();
    }
    @Test void bronzeAnchorCannotHideChangedObservedCandidateBytes()throws Exception{
        command();write("payload.xml","<rss/>");write("metadata.json","{}");write("descriptor.json","{}");
        var c=SourceJson.parse(command);var r=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))r.set(key,c.get(key));r.put("status","OK").putNull("code");r.set("evidenceId",c.get("checkpointId"));r.putObject("counts");write("result.json",new String(SourceJson.canonical(r),java.nio.charset.StandardCharsets.UTF_8));
        String fetchCandidate=NarrowContentFetchLaunch.candidate(directory,command);write("complete.json","{}");var fetch=op(6,fetchCandidate,"{}".getBytes(),true);
        Path bronze=new NarrowContentEvidenceContext(ready).originalDirectory("STORE_BRONZE",0);
        Files.createDirectories(bronze);Files.setPosixFilePermissions(bronze.getParent(),PosixFilePermissions.fromString("rwx------"));Files.setPosixFilePermissions(bronze,PosixFilePermissions.fromString("rwx------"));
        var bc=SourceJson.object().put("protocolVersion",1).put("runId",run.toString()).put("attemptId",attempt.toString()).put("stage","STORE_BRONZE").put("itemOrdinal",0).put("checkpointId",bronze.getFileName().toString());byte[] commandBytes=SourceJson.canonical(bc);
        var br=SourceJson.object();for(String key:List.of("protocolVersion","runId","attemptId","stage","itemOrdinal"))br.set(key,bc.get(key));br.put("status","OK").putNull("code");br.set("evidenceId",bc.get("checkpointId"));br.putObject("counts");
        byte[] evidence;
        // Build the same SQL-bound evidence shape, then retain an interrupted parent anchor.
        var ev=SourceJson.object().put("bronzeEvidenceVersion",1).put("commandSha256",SourceJson.sha(commandBytes)).put("state","EXACT");ev.putObject("receipt").put("receiptVersion",1);evidence=SourceJson.canonical(ev);
        var files=Map.of("command.json",commandBytes,"evidence.json",evidence,"result.json",SourceJson.canonical(br),"verified.json.preparing","{".getBytes());
        for(var f:files.entrySet()){Files.write(bronze.resolve(f.getKey()),f.getValue());Files.setPosixFilePermissions(bronze.resolve(f.getKey()),PosixFilePermissions.fromString("rw-------"));}
        var operation=new NarrowContentTerminalEvidence.OperationEvidence(Operation.STORE_BRONZE,PHASES,commandBytes,"a".repeat(64),Map.of("evidence.json",evidence,"result.json",SourceJson.canonical(br)),"{}".getBytes(),null,false);
        NarrowContentTerminalEvidence.inspect(guard,ready,List.of(fetch,operation),"a".repeat(64));
        Files.delete(bronze.resolve("verified.json.preparing"));Files.writeString(bronze.resolve("verified.json"),"{}");Files.setPosixFilePermissions(bronze.resolve("verified.json"),PosixFilePermissions.fromString("rw-------"));
        Path anchor=bronze.resolve("verified.json");var identity=InstallationHostGuard.identity(anchor);
        byte[] stamp=SourceJson.canonical(SourceJson.object().put("identity",identity.device()+":"+identity.inode()).put("modified",Files.getLastModifiedTime(anchor).toString()).put("size",Files.size(anchor)));
        var completed=new NarrowContentTerminalEvidence.OperationEvidence(Operation.STORE_BRONZE,PHASES,commandBytes,"a".repeat(64),Map.of("evidence.json",evidence,"result.json",SourceJson.canonical(br)),"{}".getBytes(),stamp,true);
        NarrowContentTerminalEvidence.inspect(guard,ready,List.of(fetch,completed),"a".repeat(64));
        byte[] wrongStamp=SourceJson.canonical(SourceJson.object().put("identity",identity.device()+":"+identity.inode()).put("modified",Files.getLastModifiedTime(anchor).toString()).put("size",Files.size(anchor)+1));
        var changedStamp=new NarrowContentTerminalEvidence.OperationEvidence(Operation.STORE_BRONZE,PHASES,commandBytes,"a".repeat(64),Map.of("evidence.json",evidence,"result.json",SourceJson.canonical(br)),"{}".getBytes(),wrongStamp,true);
        assertThatThrownBy(()->NarrowContentTerminalEvidence.inspect(guard,ready,List.of(fetch,changedStamp),"a".repeat(64))).isInstanceOf(WorkerFailure.class);
        ((tools.jackson.databind.node.ObjectNode)ev.get("receipt")).put("receiptVersion",2);Files.write(bronze.resolve("evidence.json"),SourceJson.canonical(ev));
        assertThatThrownBy(()->NarrowContentTerminalEvidence.inspect(guard,ready,List.of(fetch,operation),"a".repeat(64))).isInstanceOf(WorkerFailure.class);
    }
    @Test void missingSealedReadyAndChangedLedgerCannotQualify(){
        assertThatThrownBy(()->NarrowContentTerminalEvidence.inspect(guard,null,List.of(),"a".repeat(64))).isInstanceOf(WorkerFailure.class);
        var changed=new Expected(ready.policy(),run,attempt,ready.kind(),ready.source(),ready.host(),ready.rootIdentity(),ready.lockIdentity(),ready.frozen(),"d".repeat(64));assertThatThrownBy(()->NarrowContentTerminalEvidence.inspect(guard,changed,List.of(),"a".repeat(64))).isInstanceOf(PreflightFailure.class);
    }
}
