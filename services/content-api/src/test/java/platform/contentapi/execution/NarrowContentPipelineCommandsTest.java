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
    @Test void suppliedPreparedProofIsDefensivelyCopied(){
        var r=runtime();var prepared=SourceJson.object().put("state","PREPARED");
        var c=NarrowContentPipelineCommands.command(Operation.PUBLISH_GOLD,fixture.ready(),r,1,receipt(),SourceJson.object(),null,prepared,null,null);
        prepared.put("state","TAMPERED");assertThat(c.get("prepared").get("state").stringValue()).isEqualTo("PREPARED");
    }
}
