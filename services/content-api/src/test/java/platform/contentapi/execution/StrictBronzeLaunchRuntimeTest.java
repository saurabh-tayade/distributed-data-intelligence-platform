package platform.contentapi.execution;

import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class StrictBronzeLaunchRuntimeTest {
    final NarrowContentRuntimeQualificationTest.Fixture f=new NarrowContentRuntimeQualificationTest.Fixture();
    ObjectNode configuration(){
        var snapshot=f.inspect();var bronze=snapshot.configuration("BRONZE");var fetch=snapshot.configuration("FETCH");
        var result=SourceJson.object().put("strictBronzeLaunchVersion",1).put("bronzeRuntimeId","bronze").put("bronzeRuntimeSha256",snapshot.digest("BRONZE")).put("testEndpoint",true);
        for(String name:List.of("runProfileId","runProfileSha256","controlBindingId","storageBindingId","storageBindingSha256"))result.set(name,bronze.get(name));
        result.set("javaHome",fetch.get("javaHome"));var manifest=((ObjectNode)fetch.get("manifest").deepCopy()).put("adapter",StrictBronzeLaunchRuntime.MAIN);
        result.set("manifest",manifest);result.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)))
                .put("strictRuntimeSha256",StrictBronzeLaunchRuntime.protocolDigest(manifest.get("javaRuntime").stringValue()));return result;
    }
    @Test void launchIdentitySeparatesInstalledManifestProtocolAndHistoricalBronze(){
        byte[] before=SourceJson.canonical(f.configs.get("bronze"));var launch=configuration();StrictBronzeLaunchRuntime.validate(launch,f.inspect(),f.host);
        assertThat(SourceJson.canonical(f.configs.get("bronze"))).isEqualTo(before);
        assertThat(launch.get("deploymentSha256").stringValue()).isNotEqualTo(f.deployment).isNotEqualTo(launch.get("strictRuntimeSha256").stringValue());
        assertThat(f.configs.get("bronze").get("adapter").stringValue()).isEqualTo("platform.ingestion.managed.BoundedManagedBronze");
    }
    @ParameterizedTest @ValueSource(strings={"version","bronze","deployment","protocol","adapter","storage","control","unknown"})
    void rejectsSelfConsistentSubstitutions(String mutation){
        var c=configuration();switch(mutation){case "version"->c.put("strictBronzeLaunchVersion","1");case "bronze"->c.put("bronzeRuntimeSha256","0".repeat(64));
            case "deployment"->c.put("deploymentSha256","0".repeat(64));case "protocol"->c.put("strictRuntimeSha256",c.get("deploymentSha256").stringValue());
            case "adapter"->((ObjectNode)c.get("manifest")).put("adapter","platform.ingestion.managed.BoundedManagedBronze");
            case "storage"->c.put("storageBindingId","other");case "control"->c.put("controlBindingId","other");case "unknown"->c.put("unknown",true);}
        assertThatThrownBy(()->StrictBronzeLaunchRuntime.validate(c,f.inspect(),f.host)).hasMessage("RUNTIME_MISMATCH");
    }
    @Test void freshBronzeBootstrapUsesIndependentOriginalAndCurrentIdentities(){
        var ready=f.ready();var snapshot=f.inspect();var identity=new NarrowContentFetchReservation.Identity(f.host.installationId(),UUID.randomUUID(),7,f.run,f.attempt);
        var fetch=NarrowContentFetchReservation.build(ready,snapshot,identity,identity,UUID.randomUUID());
        var context=new NarrowContentEvidenceContext(ready);String checkpoint=context.originalDirectory("STORE_BRONZE",0).getFileName().toString();
        var command=SourceJson.object().put("protocolVersion",1).put("runId",f.run.toString()).put("attemptId",f.attempt.toString()).put("ownerEpoch",7)
                .put("stage","STORE_BRONZE").put("itemOrdinal",0).put("checkpointId",checkpoint).put("mode","PUBLISH").put("runtimeSha256",snapshot.digest("BRONZE"));command.putObject("storage");
        var material=new NarrowContentBronzeReservation.Material(ready,UUID.randomUUID(),fetch.checkpoint(),fetch.commandBytes(),new byte[0],SourceJson.canonical(SourceJson.object()),SourceJson.canonical(SourceJson.object()),
                SourceJson.canonical(command),new byte[0],java.nio.file.Path.of("/unused"),"a".repeat(64),SourceJson.object());
        var raw=NarrowContentBronzeLaunch.bootstrap(material,snapshot,identity,identity,StrictBronzeLaunchRuntime.protocolDigest("21"));
        var packet=SourceJson.parse(NarrowContentBronzeLaunch.packet(raw,123));
        SourceJson.fields(packet,"bootstrapVersion","mode","childPid","strictRuntimeSha256","expected","current","bronze");
        assertThat(packet.get("mode").stringValue()).isEqualTo("STORE_BRONZE_CANDIDATE");assertThat(packet.get("childPid").stringValue()).isEqualTo("123");
        assertThat(packet.get("expected").get("writer").get("stage").stringValue()).isEqualTo("STORE_BRONZE");
        assertThat(packet.get("expected").get("writer").get("checkpointId").stringValue()).isEqualTo(checkpoint).isNotEqualTo(fetch.checkpoint().toString());
        assertThat(packet.get("bronze").get("fetch").get("runtimeSha256").stringValue()).isEqualTo(snapshot.digest("FETCH"));
        assertThat(packet.get("bronze").get("runtimeSha256").stringValue()).isEqualTo(snapshot.digest("BRONZE"));
        var wrong=new NarrowContentFetchReservation.Identity(identity.installation(),UUID.randomUUID(),7,f.run,f.attempt);
        assertThatThrownBy(()->NarrowContentBronzeLaunch.bootstrap(material,snapshot,identity,wrong,"a".repeat(64))).hasMessage("RUNTIME_MISMATCH");
        assertThatThrownBy(()->NarrowContentBronzeLaunch.packet(raw,0)).hasMessage("RUNTIME_MISMATCH");
        assertThatThrownBy(()->NarrowContentBronzeLaunch.packet(fetch.bootstrapBytes(),123)).isInstanceOf(RuntimeException.class);
    }
}
