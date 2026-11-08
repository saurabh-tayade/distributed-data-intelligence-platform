package platform.contentapi.execution;

import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentOperationRouting.Operation;

/** Pure protocol contracts. These fixtures grant no launch or filesystem authority. */
class NarrowContentPipelineCommandsTest {
    final NarrowContentRuntimeQualificationTest.Fixture fixture=new NarrowContentRuntimeQualificationTest.Fixture();
    NarrowContentRuntimeQualification.Snapshot runtime(){
        ((ObjectNode)fixture.frozen.get("inputs").get(0)).put("artifactId",UUID.randomUUID().toString()).put("evidenceSha256","a".repeat(64));
        return fixture.inspect();
    }
    ObjectNode receipt(){
        var r=SourceJson.object().put("bucket","bronze").put("sourceId","source-one").put("ingestionId","ingestion-one").put("storageBindingId","bronze-store");
        r.putObject("expected").put("metadataKey","bronze/rss/source-one/ingestion-one/metadata.json").put("payloadKey","bronze/rss/source-one/ingestion-one/payload.xml");return r;
    }
    ObjectNode proof(JsonNode input){var p=SourceJson.object().put("artifactId",UUID.randomUUID().toString()).put("evidenceSha256","b".repeat(64));p.putObject("command").putArray("inputs").add(input);return p;}
    @ParameterizedTest @EnumSource(value=Operation.class,names={"FETCH_SOURCE"},mode=EnumSource.Mode.EXCLUDE)
    void nativeCommandsUseProtocolTwoAndExactCheckpointAliases(Operation op){
        var r=runtime();var input=NarrowContentPipelineCommands.silverSelection(receipt(),"silver-store");
        var c=NarrowContentPipelineCommands.command(op,fixture.ready(),r,7,receipt(),input,proof(input),null,null,null);
        assertThat(c.get("protocolVersion").intValue()).isEqualTo(2);
        assertThat(c.get("ownerEpoch").longValue()).isEqualTo(7);
        assertThat(c.get("runtimeSha256").stringValue()).isEqualTo(r.digest(op.family.name()));
        assertThat(c.get("mode").stringValue()).isEqualTo(op.mode);
        var origin=op.nativeCheckpointOrigin();
        assertThat(c.get("checkpointId").stringValue()).isEqualTo(new NarrowContentEvidenceContext(fixture.ready()).originalDirectory(origin.ledgerStage,origin.ledgerOrdinal).getFileName().toString());
        assertThat(SourceJson.canonical(c).length).isLessThanOrEqualTo(ContentEvidenceBudget.slots(op.ledgerStage).get("command.json"));
        var expected=NarrowContentPipelineCommands.expectations(op,r,c,"c".repeat(64));
        if(op.family==NarrowContentOperationRouting.Family.SILVER){
            assertThat(c.get("expectedCapture").isNull()).isTrue();
            for(String key:List.of("receipt","artifact","binding","bronze","runtime","expectedCapture"))assertThat(expected.get(key+"Sha256").stringValue()).isEqualTo(NarrowContentPipelineCommands.hash(c.get(key)));
        }else if(op.family==NarrowContentOperationRouting.Family.GOLD || op.family==NarrowContentOperationRouting.Family.CONTENT){
            assertThat(c.get("prepared").isNull()).isTrue();assertThat(c.get("verified").isNull()).isTrue();
            assertThat(expected.get("bindings").propertyNames()).containsExactlyInAnyOrder("GOLD","CONTENT","REFERENCE","SILVER","TEMP");
            assertThat(expected.get("silverInputSha256").stringValue()).isEqualTo(NarrowContentPipelineCommands.hash(input));
            assertThat(c.get("projectionContext").get("kind").stringValue()).isEqualTo(op.family.name());
        }
    }
    @Test void originalManagedChainHasSeventeenFixedOperationsAndNoInspectionTail(){
        var chain=NarrowContentPipeline.core();
        assertThat(chain).extracting(Enum::name).containsExactly("FETCH_SOURCE","STORE_BRONZE","BRONZE_BEFORE_SILVER","PUBLISH_SILVER","VERIFY_SILVER","RECHECK_SILVER","BRONZE_BEFORE_GOLD_PREPARATION","PREPARE_GOLD","BRONZE_BEFORE_GOLD_PUBLICATION","PUBLISH_GOLD","VERIFY_GOLD","RECHECK_GOLD","BRONZE_BEFORE_CONTENT_PREPARATION","PREPARE_CONTENT","PUBLISH_CONTENT","VERIFY_CONTENT","RECHECK_CONTENT");
        assertThat(chain.stream().map(op->op.ledgerStage+":"+op.ledgerOrdinal).distinct()).hasSize(17);
        assertThat(chain).noneMatch(op->op.history==NarrowContentOperationRouting.History.AFTER_SUCCESS);
    }
    @Test void bootstrapSeparatesOperationSlotFromNativeCheckpointAlias(){
        var r=runtime();var ready=fixture.ready();var operation=UUID.randomUUID();
        var session=new WorkerRepository.Session(ready.host().installationId(),UUID.randomUUID(),"profile",fixture.profileHash,fixture.deployment,ready.host().digest(),"ACTIVE",7,1,1);
        var owner=new WorkerRepository.Owner(session,ready.run(),ready.attempt(),1,1,"CLAIMED");
        var c=NarrowContentPipelineCommands.command(Operation.RECHECK_CONTENT,ready,r,7,receipt(),null,proof(SourceJson.object()),SourceJson.object(),SourceJson.object(),null);
        var b=NarrowContentPipelineCommands.bootstrap(Operation.RECHECK_CONTENT,ready,owner,operation,c,r,"a".repeat(64));
        assertThat(b.get("bootstrapVersion").intValue()).isEqualTo(2);assertThat(b.get("mode").stringValue()).isEqualTo("STRICT_CONTENT");
        assertThat(b.get("stage").stringValue()).isEqualTo("CHECK_INPUTS");assertThat(b.get("ordinal").intValue()).isEqualTo(2);
        assertThat(b.get("checkpointId")).isNotEqualTo(c.get("checkpointId"));
        assertThat(b.get("commandSha256").stringValue()).isEqualTo(NarrowContentPipelineCommands.hash(c));
        assertThat(b.get("strictRuntimeSha256").stringValue()).isEqualTo(StrictContentPipelineRuntime.protocolDigest());
        assertThat(b.get("operationId").stringValue()).isEqualTo(operation.toString());
        assertThat(b.get("ownerEpoch").stringValue()).isEqualTo("7");
    }
    @Test void bootstrapCapacityQualifiesEntireDownstreamShapeWithoutIo(){
        var r=runtime();var ready=fixture.ready();int before=fixture.jdbc.calls.size();
        var session=new WorkerRepository.Session(ready.host().installationId(),UUID.randomUUID(),"profile",fixture.profileHash,fixture.deployment,ready.host().digest(),"ACTIVE",7,1,1);
        var owner=new WorkerRepository.Owner(session,ready.run(),ready.attempt(),1,1,"CLAIMED");
        assertThatCode(()->NarrowContentPipelineCommands.qualifyBootstrapCapacity(ready,owner,r)).doesNotThrowAnyException();
        assertThat(fixture.jdbc.calls).hasSize(before);
    }
    @Test void oversizedFixedBootstrapHeaderFailsBeforeAnyNativeEffect(){
        var r=runtime();var original=fixture.ready();
        var host=new InstallationHostGuard.Binding(original.host().id(),original.host().installationId(),java.nio.file.Path.of("/"+"x".repeat(70000)),fixture.identity,fixture.identity,fixture.identity);
        var ready=new NarrowContentEvidenceContext.Expected(original.policy(),original.run(),original.attempt(),original.kind(),original.source(),host,original.rootIdentity(),original.lockIdentity(),original.frozen(),original.ledgerSha256());
        var session=new WorkerRepository.Session(host.installationId(),UUID.randomUUID(),"profile",fixture.profileHash,fixture.deployment,original.host().digest(),"ACTIVE",7,1,1);
        var owner=new WorkerRepository.Owner(session,ready.run(),ready.attempt(),1,1,"CLAIMED");int before=fixture.jdbc.calls.size();
        assertThatThrownBy(()->NarrowContentPipelineCommands.qualifyBootstrapCapacity(ready,owner,r)).hasMessage("RESOURCE_LIMIT");
        assertThat(fixture.jdbc.calls).hasSize(before);
    }
    @Test void silverAddressIsDerivedFromReceiptAndRejectsNonBronzeAddress(){
        var r=receipt();var selected=NarrowContentPipelineCommands.silverSelection(r,"silver-store");
        assertThat(selected.get("relativeAddress").stringValue()).isEqualTo("v1/"+SourceJson.sha("bronze".getBytes(java.nio.charset.StandardCharsets.UTF_8))+"/source-one/ingestion-one/complete.json");
        assertThat(selected.get("identity").get("bronzeMetadataKey")).isEqualTo(r.get("expected").get("metadataKey"));
        ((ObjectNode)r.get("expected")).put("metadataKey","other/metadata.json");
        assertThatThrownBy(()->NarrowContentPipelineCommands.silverSelection(r,"silver-store")).hasMessage("INPUT_INVALID");
    }
    @Test void missingLivePredecessorProofsFailBeforeACommandCanBeEmitted(){
        var r=runtime();
        assertThatThrownBy(()->NarrowContentPipelineCommands.command(Operation.PREPARE_GOLD,fixture.ready(),r,1,receipt(),null,null,null,null,null)).hasMessage("INPUT_INVALID");
        assertThatThrownBy(()->NarrowContentPipelineCommands.command(Operation.PREPARE_CONTENT,fixture.ready(),r,1,receipt(),null,null,null,null,null)).hasMessage("INPUT_INVALID");
    }
    /** Synthetic aggregate-boundary corpus only: real Java builders/readers, but no claim
     * that padded bodies are native-valid publications or reachable fresh-source evidence. */
    @ParameterizedTest @EnumSource(value=Operation.class,names={"PUBLISH_GOLD","RECHECK_GOLD","PUBLISH_CONTENT","RECHECK_CONTENT"})
    void syntheticComposedCommandsAcceptExactSlotAndRejectOneAdditionalByte(Operation op){
        var r=runtime();var ready=fixture.ready();var receipt=receipt();
        var input=syntheticSilverInput();var gold=syntheticGoldProof(r,ready,input);
        var prepared=SourceJson.object().put("fingerprint","d".repeat(64));
        var verified=SourceJson.object().put("status","VERIFIED");
        boolean recheck=op.mode.equals("RECHECK");
        // Charge a substantial predecessor independently from the current verification.
        if(op.family==NarrowContentOperationRouting.Family.CONTENT)syntheticGoldPadding(gold);
        if(recheck)syntheticPadding(verified,250000);
        ObjectNode padded=recheck?verified:prepared;
        var fill=padded.putArray("aggregatePadding");for(int i=0;i<64;i++)fill.add("");
        java.util.function.Supplier<ObjectNode> build=()->NarrowContentPipelineCommands.command(
                op,ready,r,7,receipt,input,gold,prepared,recheck?verified:null,null);
        int maximum=ContentEvidenceBudget.slots(op.ledgerStage).get("command.json");
        int remaining=maximum-SourceJson.canonical(build.get()).length;
        assertThat(remaining).isPositive();
        for(int i=0;i<fill.size();i++){
            int length=Math.min(32768,remaining);fill.set(i,SourceJson.object().put("v","x".repeat(length)).get("v"));remaining-=length;
        }
        assertThat(remaining).isZero();
        var exact=build.get();byte[] raw=SourceJson.canonical(exact);
        assertThat(raw).hasSize(maximum);
        assertThat(SourceJson.canonical(SourceJson.parse(raw,maximum))).isEqualTo(raw);
        if(op.family==NarrowContentOperationRouting.Family.CONTENT){
            assertThat(exact.get("goldProof").get("command").get("mode").stringValue()).isEqualTo("PUBLISH");
            assertThat(exact.get("goldProof").get("artifact").get("verification").get("status").stringValue()).isEqualTo("VERIFIED");
        }
        // Every component and every string fits independently. Only the composed command
        // crosses the aggregate slot; parse it with the large reader to rule out grammar/depth.
        fill.set(63,SourceJson.object().put("v",fill.get(63).stringValue()+"x").get("v"));
        var oversized=exact.deepCopy();oversized.set(recheck?"verified":"prepared",padded.deepCopy());
        byte[] plusOne=SourceJson.canonical(oversized);assertThat(plusOne).hasSize(maximum+1);
        assertThat(SourceJson.canonical(SourceJson.parse(plusOne,PreflightFiles.LARGE))).isEqualTo(plusOne);
        for(JsonNode component:List.of(input,gold,prepared,verified)){
            byte[] componentRaw=SourceJson.canonical(component);
            assertThat(componentRaw.length).isLessThan(maximum);
            assertThat(SourceJson.canonical(SourceJson.parse(componentRaw,maximum))).isEqualTo(componentRaw);
        }
        int before=fixture.jdbc.calls.size();
        assertThatThrownBy(build::get).hasMessage("RESOURCE_LIMIT");
        assertThat(fixture.jdbc.calls).hasSize(before);
    }
    @Test void syntheticLargePredecessorChangesBootstrapHashesButNotItsSize(){
        var r=runtime();var ready=fixture.ready();var input=syntheticSilverInput();var gold=syntheticGoldProof(r,ready,input);
        var session=new WorkerRepository.Session(ready.host().installationId(),UUID.randomUUID(),"profile",fixture.profileHash,fixture.deployment,ready.host().digest(),"ACTIVE",7,1,1);
        var owner=new WorkerRepository.Owner(session,ready.run(),ready.attempt(),1,1,"CLAIMED");var operation=UUID.randomUUID();
        var prepared=SourceJson.object();var verified=SourceJson.object();
        var small=NarrowContentPipelineCommands.command(Operation.RECHECK_CONTENT,ready,r,7,receipt(),input,gold,prepared,verified,null);
        var before=NarrowContentPipelineCommands.bootstrap(Operation.RECHECK_CONTENT,ready,owner,operation,small,r,"a".repeat(64));
        int smallCommandLength=SourceJson.canonical(small).length;
        syntheticGoldPadding(gold);syntheticPadding(verified,250000);
        var large=NarrowContentPipelineCommands.command(Operation.RECHECK_CONTENT,ready,r,7,receipt(),input,gold,prepared,verified,null);
        var after=NarrowContentPipelineCommands.bootstrap(Operation.RECHECK_CONTENT,ready,owner,operation,large,r,"a".repeat(64));
        assertThat(SourceJson.canonical(large).length-smallCommandLength).isGreaterThan(550000);
        byte[] beforeRaw=SourceJson.canonical(before),afterRaw=SourceJson.canonical(after);
        assertThat(afterRaw).hasSize(beforeRaw.length);assertThat(afterRaw.length).isLessThan(65536);
        assertThat(SourceJson.canonical(SourceJson.parse(afterRaw))).isEqualTo(afterRaw);
        assertThat(after.get("commandSha256")).isNotEqualTo(before.get("commandSha256"));
        assertThat(after.get("expectations").get("goldProofSha256")).isNotEqualTo(before.get("expectations").get("goldProofSha256"));
        assertThat(after.get("expectations").get("goldProofSha256").stringValue()).isEqualTo(NarrowContentPipelineCommands.hash(gold));
        assertThat(after.get("expectations").has("goldProof")).isFalse();
        assertThat(after.get("expectations").has("verified")).isFalse();
    }
    private ObjectNode syntheticSilverInput(){
        return ((ObjectNode)NarrowContentPipelineCommands.silverSelection(receipt(),"silver-store")).deepCopy()
                .put("artifactId",UUID.randomUUID().toString()).put("evidenceSha256","a".repeat(64));
    }
    private ObjectNode syntheticGoldProof(NarrowContentRuntimeQualification.Snapshot runtime,NarrowContentEvidenceContext.Expected ready,JsonNode input){
        var prepared=SourceJson.object().put("inputSetFingerprint","d".repeat(64));
        var publish=NarrowContentPipelineCommands.command(Operation.PUBLISH_GOLD,ready,runtime,7,receipt(),input,null,prepared,null,null);
        var verification=SourceJson.object().put("status","VERIFIED");verification.set("prepared",prepared);
        var artifact=NarrowContentPipeline.artifact("GOLD",publish,verification);
        var proof=SourceJson.object().put("artifactId",UUID.randomUUID().toString()).put("evidenceSha256",NarrowContentPipelineCommands.hash(artifact));
        proof.set("artifact",artifact);proof.set("command",publish);return proof;
    }
    private static void syntheticGoldPadding(ObjectNode proof){
        syntheticPadding((ObjectNode)proof.get("artifact").get("verification"),300000);
        proof.put("evidenceSha256",NarrowContentPipelineCommands.hash(proof.get("artifact")));
    }
    private static void syntheticPadding(ObjectNode node,int characters){
        var values=node.putArray("syntheticPadding");
        while(characters>0){int length=Math.min(32768,characters);values.add("x".repeat(length));characters-=length;}
    }
    @Test void suppliedPreparedProofIsDefensivelyCopied(){
        var r=runtime();var prepared=SourceJson.object().put("state","PREPARED");
        var c=NarrowContentPipelineCommands.command(Operation.PUBLISH_GOLD,fixture.ready(),r,1,receipt(),SourceJson.object(),null,prepared,null,null);
        prepared.put("state","TAMPERED");assertThat(c.get("prepared").get("state").stringValue()).isEqualTo("PREPARED");
    }
}
