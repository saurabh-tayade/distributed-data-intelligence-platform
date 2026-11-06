package platform.contentapi.execution;

import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class StrictFetchLaunchRuntimeTest {
    final NarrowContentRuntimeQualificationTest.Fixture f=new NarrowContentRuntimeQualificationTest.Fixture();
    ObjectNode configuration(){
        var snapshot=f.inspect();var fetch=snapshot.configuration("FETCH");
        var result=SourceJson.object().put("strictFetchLaunchVersion",1).put("fetchRuntimeId","fetch").put("fetchRuntimeSha256",snapshot.digest("FETCH")).put("testEndpoint",true);
        for(String name:List.of("runProfileId","runProfileSha256","controlBindingId","javaHome"))result.set(name,fetch.get(name));
        var manifest=((ObjectNode)fetch.get("manifest").deepCopy()).put("adapter",StrictFetchLaunchRuntime.MAIN);
        result.set("manifest",manifest);result.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)));return result;
    }
    @Test void separateLaunchIdentityNeverChangesHistoricalFetch(){
        byte[] before=SourceJson.canonical(f.configs.get("fetch"));var launch=configuration();
        StrictFetchLaunchRuntime.validate(launch,f.inspect(),f.host);
        assertThat(SourceJson.canonical(f.configs.get("fetch"))).isEqualTo(before);
        assertThat(launch.get("deploymentSha256").stringValue()).isNotEqualTo(f.deployment);
        var identity=new NarrowContentFetchReservation.Identity(f.host.installationId(),UUID.randomUUID(),7,f.run,f.attempt);
        var reservation=NarrowContentFetchReservation.build(f.ready(),f.inspect(),identity,identity,UUID.randomUUID());
        var packet=SourceJson.parse(NarrowContentFetchLaunch.packet(reservation.bootstrapBytes(),123));
        SourceJson.fields(packet,"bootstrapVersion","mode","childPid","expected","current","fetch");
        assertThat(packet.get("mode").stringValue()).isEqualTo("FETCH_SOURCE_CANDIDATE");assertThat(packet.get("childPid").stringValue()).isEqualTo("123");
        assertThatThrownBy(()->NarrowContentFetchLaunch.packet(reservation.bootstrapBytes(),0)).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest @ValueSource(strings={"https://example.com/feed","http://localhost/feed","http://127.0.0.2/feed","http://127.0.0.1:0/feed","http://127.0.0.1:65536/feed","http://127.0.0.1/feed?query=1","http://user@127.0.0.1/feed","http://127.0.0.1/feed#fragment","file:///feed"})
    void launchQualificationRejectsEveryNonLiteralLoopbackOrUnsafeEndpoint(String endpoint){
        ((ObjectNode)f.frozen.get("sources").get(0).get("endpoint")).put("uri",endpoint);
        assertThatThrownBy(()->StrictFetchLaunchRuntime.validate(configuration(),f.inspect(),f.host)).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest @ValueSource(strings={"version","fetch","deployment","adapter","unknown"})
    void launchQualificationRejectsSelfConsistentSubstitution(String mutation){
        var config=configuration();
        switch(mutation){case "version"->config.put("strictFetchLaunchVersion","1");case "fetch"->config.put("fetchRuntimeSha256","0".repeat(64));case "deployment"->config.put("deploymentSha256","0".repeat(64));case "adapter"->((ObjectNode)config.get("manifest")).put("adapter","platform.ingestion.managed.BoundedManagedRssFetch");case "unknown"->config.put("unknown",true);}
        assertThatThrownBy(()->StrictFetchLaunchRuntime.validate(config,f.inspect(),f.host)).hasMessage("RUNTIME_MISMATCH");
    }
}
