package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentOperationRouting.*;

/** Synthetic command identity vectors. They are deliberately NOT native publication evidence. */
class NarrowContentCommandCorrespondenceTest {
    final NarrowContentRuntimeQualificationTest.Fixture f=new NarrowContentRuntimeQualificationTest.Fixture();
    final Map<Operation,UUID> checkpoints=new EnumMap<>(Operation.class);
    final Path root=f.host.root().resolve("runs/"+f.run+"/attempts/"+f.attempt);
    final ContentOperationPlan plan=ContentOperationPlan.original(root,f.run,f.attempt,"source-one");
    NarrowContentCommandCorrespondenceTest(){for(var operation:Operation.values())checkpoints.put(operation,UUID.randomUUID());}
    JsonNode binding(String id){for(var b:f.frozen.get("storageBindings"))if(id.equals(b.get("bindingId").stringValue())){var n=SourceJson.object();for(String key:List.of("root","rootDevice","rootInode"))n.set(key,b.get("configuration").get(key));return n;}throw new AssertionError();}
    ObjectNode command(Operation operation){
        var runtime=f.inspect();var n=SourceJson.object();
        for(String field:fields(operation.family).split(" "))n.putNull(field);
        n.put("protocolVersion",1).put("runId",f.run.toString()).put("attemptId",f.attempt.toString()).put("ownerEpoch",7)
                .put("checkpointId",checkpoints.get(operation.nativeCheckpointOrigin()).toString()).put("runtimeSha256",runtime.digest(operation.family.name()));
        if(operation.mode!=null)n.put("mode",operation.mode);
        if(operation.family==Family.FETCH || operation.family==Family.BRONZE)n.put("stage",operation.ledgerStage).put("itemOrdinal",0);
        if(operation.family==Family.FETCH){
            var selected=f.frozen.get("sources").get(0);var fetch=runtime.configuration("FETCH");
            for(String key:List.of("sourceId","revision","endpointSha256"))n.set(key,selected.get(key));
            n.set("capturedConfigSha256",selected.get("configurationSha256"));n.set("feedUri",selected.get("endpoint").get("uri"));
            n.set("javaRuntime",fetch.get("manifest").get("javaRuntime"));for(String key:List.of("testEndpoint","httpTimeoutMillis"))n.set(key,fetch.get(key));
        }
        if(Set.of(Family.SILVER,Family.GOLD,Family.CONTENT).contains(operation.family)){
            var scratch=binding("temp-store");n.set("runtime",SourceJson.object().put("javaHome","/nonexistent/java").put("scratchRoot",scratch.get("root").stringValue())
                    .put("scratchDevice",scratch.get("rootDevice").stringValue()).put("scratchInode",scratch.get("rootInode").stringValue()));
            switch(operation.family){
                case SILVER->n.set("binding",binding("silver-store"));
                case GOLD->{n.put("silverBindingId","silver-store");n.set("silver",binding("silver-store"));n.set("gold",binding("gold-store"));}
                case CONTENT->{n.put("referenceBindingId","reference-store");n.set("referenceBinding",binding("reference-store"));n.set("content",binding("content-store"));n.set("reference",f.frozen.get("inputs").get(0));}
                default->throw new AssertionError();
            }
        }
        if(operation.family==Family.GOLD || operation.family==Family.CONTENT){
            if(!operation.mode.equals("PREPARE"))n.set("prepared",SourceJson.object());
            if(operation.mode.equals("RECHECK") || operation==Operation.INSPECT_CONTENT)n.set("verified",SourceJson.object());
        }
        return n;
    }
    Correspondence inspect(Operation operation,ObjectNode n){return inspectCommand(operation,plan,f.run,f.attempt,"source-one",7,checkpoints,f.inspect(),SourceJson.canonical(n));}
    @ParameterizedTest @EnumSource(Operation.class)
    void allNineteenHeadersMatchIndependentlyRegisteredFamilyAndCheckpointWithoutExecution(Operation operation){
        var n=command(operation);var facts=inspect(operation,n);
        assertThat(facts.commandSha256()).isEqualTo(SourceJson.sha(SourceJson.canonical(n)));
        assertThat(facts.nativeCheckpoint()).isEqualTo(checkpoints.get(operation.nativeCheckpointOrigin()));
        assertThat(facts.directoryCheckpoint().toString()).isEqualTo(operation.directory(plan).getFileName().toString());
        assertThat(facts.nativeLaunchCompatible()).isFalse();assertThat(facts.nativeInputsVerified()).isFalse();
        assertThat(facts.commandProtocolVersion()).isEqualTo(1);assertThat(facts.runtimeLedgerVersion()).isEqualTo(1);
        assertThat(root).doesNotExist();
    }
    @ParameterizedTest @EnumSource(Operation.class)
    void independentBindingsRejectForeignIdentityEpochCheckpointAndRuntime(Operation operation){
        for(String key:List.of("runId","attemptId","checkpointId","runtimeSha256","ownerEpoch","protocolVersion")){
            var n=command(operation);
            if(key.equals("ownerEpoch"))n.put(key,8);else if(key.equals("protocolVersion"))n.put(key,2);else n.put(key,key.equals("runtimeSha256")?"f".repeat(64):UUID.randomUUID().toString());
            assertThatThrownBy(()->inspect(operation,n)).isInstanceOf(PreflightFailure.class);
        }
    }
    @Test void ledgerOrdinalsCannotBecomeNativeSourceIndicesOrSqlCheckpointIds(){
        for(int i=1;i<=4;i++){var operation=operation("INSPECT_BRONZE",i);var n=command(operation).put("itemOrdinal",i);assertThatThrownBy(()->inspect(operation,n)).hasMessage("INPUT_INVALID");}
        for(var operation:List.of(Operation.RECHECK_SILVER,Operation.VERIFY_GOLD,Operation.RECHECK_GOLD,Operation.RECHECK_CONTENT,Operation.INSPECT_CONTENT)){
            var n=command(operation).put("checkpointId",operation.directory(plan).getFileName().toString());
            assertThatThrownBy(()->inspect(operation,n)).hasMessage("INPUT_CHANGED");
        }
    }
    @Test void modesShapeProbeAndProjectionCommandsCannotBeSubstituted(){
        var op=Operation.RECHECK_GOLD;
        for(String mode:List.of("PREPARE","READ_ONLY_PROBE","PUBLISH"))assertThatThrownBy(()->inspect(op,command(op).put("mode",mode))).hasMessage("INPUT_INVALID");
        var n=command(op);n.putObject("projectionContext").put("policy",NarrowContentProfile.POLICY);n.put("protocolVersion",2);
        assertThatThrownBy(()->inspect(op,n)).isInstanceOf(PreflightFailure.class);
        var wrong=command(Operation.INSPECT_CONTENT);wrong.putNull("verified");assertThatThrownBy(()->inspect(Operation.INSPECT_CONTENT,wrong)).hasMessage("INPUT_INVALID");
        var noCheckpoint=command(op);checkpoints.remove(Operation.PUBLISH_GOLD);assertThatThrownBy(()->inspect(op,noCheckpoint)).hasMessage("INPUT_INVALID");
    }
    @Test void recoveryPlanOtherSourceAndOtherAttemptSnapshotCannotEnterOriginalCatalog(){
        byte[] n=SourceJson.canonical(command(Operation.VERIFY_GOLD));var runtime=f.inspect();
        assertThatThrownBy(()->inspectCommand(Operation.VERIFY_GOLD,ContentOperationPlan.recovery(root,f.run,f.attempt),f.run,f.attempt,"source-one",7,checkpoints,runtime,n)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->inspectCommand(Operation.VERIFY_GOLD,plan,f.run,f.attempt,"other",7,checkpoints,runtime,n)).isInstanceOf(PreflightFailure.class);
        var other=new NarrowContentRuntimeQualificationTest.Fixture().inspect();
        assertThatThrownBy(()->inspectCommand(Operation.VERIFY_GOLD,plan,f.run,f.attempt,"source-one",7,checkpoints,other,n)).hasMessage("RUNTIME_MISMATCH");
    }
    @Test void registeredBindingRuntimeAndFrozenFetchDetailsAreNotCommandSuppliedAuthority(){
        var n=command(Operation.FETCH_SOURCE).put("feedUri","http://127.0.0.1/wrong");assertThatThrownBy(()->inspect(Operation.FETCH_SOURCE,n)).hasMessage("INPUT_CHANGED");
        var gold=command(Operation.PREPARE_GOLD);((ObjectNode)gold.get("runtime")).put("scratchRoot","/wrong");assertThatThrownBy(()->inspect(Operation.PREPARE_GOLD,gold)).hasMessage("RUNTIME_MISMATCH");
        var content=command(Operation.PREPARE_CONTENT).put("referenceBindingId","silver-store");assertThatThrownBy(()->inspect(Operation.PREPARE_CONTENT,content)).hasMessage("RUNTIME_MISMATCH");
    }
    @Test void callerMutationAfterParsingCannotChangeTheReportedCommandHash(){
        var operation=Operation.RECHECK_GOLD;byte[] bytes=SourceJson.canonical(command(operation));String sha=SourceJson.sha(bytes);
        var changing=new EnumMap<Operation,UUID>(checkpoints){
            @Override public UUID get(Object key){bytes[0]='[';return super.get(key);}
        };
        var facts=inspectCommand(operation,plan,f.run,f.attempt,"source-one",7,changing,f.inspect(),bytes);
        assertThat(facts.commandSha256()).isEqualTo(sha);assertThat(SourceJson.sha(bytes)).isNotEqualTo(sha);
    }
    @Test void malformedNoncanonicalDuplicateFieldsAndOversizedCommandsFailClosed(){
        var runtime=f.inspect();for(byte[] bytes:List.of("{}".getBytes(),"{\"protocolVersion\":1,\"protocolVersion\":1}".getBytes(),new byte[65537]))
            assertThatThrownBy(()->inspectCommand(Operation.FETCH_SOURCE,plan,f.run,f.attempt,"source-one",7,checkpoints,runtime,bytes)).isInstanceOf(PreflightFailure.class);
        byte[] bytes=(new String(SourceJson.canonical(command(Operation.FETCH_SOURCE)))+"\n").getBytes();
        assertThatThrownBy(()->inspectCommand(Operation.FETCH_SOURCE,plan,f.run,f.attempt,"source-one",7,checkpoints,runtime,bytes)).isInstanceOf(PreflightFailure.class);
    }
}
