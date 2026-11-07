package platform.contentapi.execution;

import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class StrictBronzeVerificationRuntimeTest {
    final NarrowContentRuntimeQualificationTest.Fixture f=new NarrowContentRuntimeQualificationTest.Fixture();
    ObjectNode configuration(){
        var snapshot=f.inspect();var bronze=snapshot.configuration("BRONZE");var fetch=snapshot.configuration("FETCH");
        var result=SourceJson.object().put("strictBronzeVerificationVersion",1).put("bronzeRuntimeId","bronze").put("bronzeRuntimeSha256",snapshot.digest("BRONZE")).put("testEndpoint",true);
        for(String name:List.of("runProfileId","runProfileSha256","controlBindingId","storageBindingId","storageBindingSha256"))result.set(name,bronze.get(name));
        result.set("javaHome",fetch.get("javaHome"));var manifest=((ObjectNode)fetch.get("manifest").deepCopy()).put("adapter",StrictBronzeVerificationRuntime.MAIN);
        result.set("manifest",manifest);result.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)))
                .put("strictRuntimeSha256",StrictBronzeVerificationRuntime.protocolDigest(manifest.get("javaRuntime").stringValue()));return result;
    }
    @Test void verificationIdentitySeparatesInstalledManifestProtocolAndHistoricalBronze(){
        byte[] before=SourceJson.canonical(f.configs.get("bronze"));var launch=configuration();StrictBronzeVerificationRuntime.validate(launch,f.inspect(),f.host);
        assertThat(SourceJson.canonical(f.configs.get("bronze"))).isEqualTo(before);
        assertThat(launch.get("deploymentSha256").stringValue()).isNotEqualTo(f.deployment).isNotEqualTo(launch.get("strictRuntimeSha256").stringValue());
        assertThat(f.configs.get("bronze").get("adapter").stringValue()).isEqualTo("platform.ingestion.managed.BoundedManagedBronze");
    }
    @ParameterizedTest @ValueSource(strings={"version","bronze","deployment","protocol","adapter","storage","control","unknown"})
    void rejectsSelfConsistentSubstitutions(String mutation){
        var c=configuration();switch(mutation){case "version"->c.put("strictBronzeVerificationVersion","1");case "bronze"->c.put("bronzeRuntimeSha256","0".repeat(64));
            case "deployment"->c.put("deploymentSha256","0".repeat(64));case "protocol"->c.put("strictRuntimeSha256",c.get("deploymentSha256").stringValue());
            case "adapter"->((ObjectNode)c.get("manifest")).put("adapter","platform.ingestion.managed.BoundedManagedBronze");
            case "storage"->c.put("storageBindingId","other");case "control"->c.put("controlBindingId","other");case "unknown"->c.put("unknown",true);}
        assertThatThrownBy(()->StrictBronzeVerificationRuntime.validate(c,f.inspect(),f.host)).hasMessage("RUNTIME_MISMATCH");
    }
}
