package platform.contentapi.management;

import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.PublicContentProfileTestSupport;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** PostgreSQL composition and metadata discovery only. No network/native launch permission. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class StrictPublicFetchProfileIT extends RunApiFixtures {
    Map<String,ObjectNode> profiles;
    @Override void profile(String id,boolean content,String source){
        if(!content){super.profile(id,false,source);return;}
        profiles=PublicContentProfileTestSupport.registerMetadataChain(jdbc,id,RunFixtures.profile(true,source),CONTENT);
    }
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        var c=SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:9000").put("region","local").put("bucket","bronze").put("credentialsFile","/not-installed/credentials");
        byte[] raw=SourceJson.canonical(c);jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,raw,SourceJson.sha(raw),INSTALL);
    }
    @BeforeEach void registerPublic(){
        insert("fetch","public-fetch",profiles.get("fetch"));insert("bronze","public-bronze",profiles.get("bronze"));insert("verifier","public-verifier",profiles.get("verifier"));insert("pipeline","public-pipeline",profiles.get("pipeline"));
    }
    void insert(String family,String id,ObjectNode c){
        String table=switch(family){case "fetch"->"content_fetch_launch_profiles";case "bronze"->"content_bronze_launch_profiles";case "verifier"->"content_bronze_verification_profiles";default->"content_pipeline_profiles";};
        String key=family.equals("pipeline")?"profile_id":family.equals("verifier")?"verification_profile_id":"launch_profile_id";
        byte[] raw=SourceJson.canonical(c);jdbc.update("INSERT INTO ddip_management."+table+"("+key+",configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,raw,SourceJson.sha(raw));
    }
    @Test void completePublicCompositionIsDiscoverableOnlyAsRegistrationMetadata(){
        var before=jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class);
        var discovery=new DiscoveryRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY));
        var rows=discovery.profiles("CONTENT_RSS_V1",100,null).data();assertThat(rows).hasSize(1);
        var row=rows.get(0);assertThat(row.get("executionProfileId").stringValue()).isEqualTo(CONTENT_PROFILE);
        assertThat(row.get("dispatchDisabled").booleanValue()).isTrue();assertThat(row.get("runtimeQualification").stringValue()).isEqualTo("NOT_CHECKED");
        assertThat(row.get("qualificationScope").stringValue()).isEqualTo("REGISTRATION_METADATA_ONLY");
        assertThat(row.toString()).doesNotContain("endpoint","javaHome","credentials","networkPolicy","classpath");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(before);
        assertThat(count("content_native_operations")).isZero();
    }
    @ParameterizedTest @ValueSource(strings={"policy","policyDigest","fetchDigest","endpoint","extra","providerMissing","providerReordered","version"})
    void rejectsPublicFetchSubstitutionEvenWhenConfigurationDigestIsRecomputed(String changed){
        var c=profiles.get("fetch").deepCopy();
        switch(changed){case "policy"->{((ObjectNode)c.get("networkPolicy")).put("proxies",true);c.put("networkPolicySha256",SourceJson.sha(SourceJson.canonical(c.get("networkPolicy"))));}
            case "policyDigest"->c.put("networkPolicySha256","0".repeat(64));case "fetchDigest"->c.put("fetchRuntimeSha256","0".repeat(64));
            case "endpoint"->c.put("testEndpoint",true);case "extra"->c.put("extra",true);case "version"->c.put("strictFetchLaunchVersion",1);
            case "providerMissing"->((tools.jackson.databind.node.ArrayNode)c.get("manifest").get("classpath")).remove(1);
            case "providerReordered"->{var a=(tools.jackson.databind.node.ArrayNode)c.get("manifest").get("classpath");var first=a.get(0).deepCopy();a.set(0,a.get(1));a.set(1,first);}}
        c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(c.get("manifest"))));
        assertThatThrownBy(()->insert("fetch","rejected-fetch",c)).hasStackTraceContaining("CONTENT_FETCH_LAUNCH_PROFILE_INVALID");
    }
    @ParameterizedTest @ValueSource(strings={"fetchLaunchProfileId","fetchLaunchProfileSha256","sourceTestEndpoint","storageEndpointPolicy","providerClasspath","version","missingDigest"})
    void bronzeAndVerifierRejectMixedOrDisconnectedPublicComposition(String changed){
        for(String family:List.of("bronze","verifier")){
            var c=profiles.get(family).deepCopy();
            switch(changed){case "fetchLaunchProfileId"->c.put(changed,"missing-fetch");case "fetchLaunchProfileSha256"->c.put(changed,"0".repeat(64));
                case "sourceTestEndpoint"->c.put(changed,true);case "storageEndpointPolicy"->c.put(changed,"PUBLIC_HTTP");case "missingDigest"->c.remove("fetchLaunchProfileSha256");
                case "providerClasspath"->{((ObjectNode)c.get("manifest")).set("classpath",profiles.get("fetch").get("manifest").get("classpath"));c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(c.get("manifest"))));}
                case "version"->c.put(family.equals("bronze")?"strictBronzeLaunchVersion":"strictBronzeVerificationVersion",1);}
            assertThatThrownBy(()->insert(family,"rejected-"+family,c)).isInstanceOf(RuntimeException.class);
        }
    }
    @ParameterizedTest @ValueSource(strings={"fetchLaunchProfileSha256","bronzeLaunchProfileSha256","bronzeVerificationProfileSha256","fetchLaunchProfileId","bronzeLaunchProfileId","bronzeVerificationProfileId","version"})
    void pipelineRequiresExactThreeHashesAndIds(String changed){
        var c=profiles.get("pipeline").deepCopy();if(changed.equals("version"))c.put("strictContentPipelineVersion",1);else c.put(changed,changed.endsWith("Sha256")?"0".repeat(64):"missing");
        assertThatThrownBy(()->insert("pipeline","rejected-pipeline",c)).hasStackTraceContaining("CONTENT_PIPELINE_");
    }
    @Test void differentIndividuallyValidPublicFetchLinkCannotCompose(){
        insert("fetch","other-fetch",profiles.get("fetch"));
        var verifier=profiles.get("verifier").deepCopy().put("fetchLaunchProfileId","other-fetch");insert("verifier","other-verifier",verifier);
        var c=profiles.get("pipeline").deepCopy().put("bronzeVerificationProfileId","other-verifier").put("bronzeVerificationProfileSha256",SourceJson.sha(SourceJson.canonical(verifier)));
        assertThatThrownBy(()->insert("pipeline","rejected-pipeline",c)).hasStackTraceContaining("CONTENT_PIPELINE_");
    }
    @ParameterizedTest @ValueSource(strings={"/nonexistent/provider.zip","/nonexistent/../provider.jar","/nonexistent/./provider.jar","/nonexistent//provider.jar","relative.jar"})
    void providerPathMustHaveExactAbsoluteJarSpelling(String path){
        var c=profiles.get("fetch").deepCopy();((ObjectNode)c.get("manifest").get("classpath").get(1)).put("path",path);
        c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(c.get("manifest"))));
        assertThatThrownBy(()->insert("fetch","rejected-path",c)).hasStackTraceContaining("CONTENT_FETCH_LAUNCH_PROFILE_INVALID");
    }
    @Test void newCompositionRegistrationRejectsNoncanonicalOrDuplicateWireBytes(){
        for(String family:List.of("fetch","bronze","verifier","pipeline")){
            String table=switch(family){case "fetch"->"content_fetch_launch_profiles";case "bronze"->"content_bronze_launch_profiles";case "verifier"->"content_bronze_verification_profiles";default->"content_pipeline_profiles";};
            String key=family.equals("pipeline")?"profile_id":family.equals("verifier")?"verification_profile_id":"launch_profile_id";
            String canonical=new String(SourceJson.canonical(profiles.get(family)),java.nio.charset.StandardCharsets.UTF_8);
            byte[] raw=(" "+canonical).getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management."+table+"("+key+",configuration_bytes,configuration_sha256) VALUES (?,?,?)","bad-wire",raw,SourceJson.sha(raw))).isInstanceOf(RuntimeException.class);
        }
        String canonical=new String(SourceJson.canonical(profiles.get("fetch")),java.nio.charset.StandardCharsets.UTF_8);
        for(String wire:List.of(canonical.replace("\"strictFetchLaunchVersion\":2","\"strictFetchLaunchVersion\":2.0"),
                canonical.replace("\"strictFetchLaunchVersion\":2","\"strictFetchLaunchVersion\":2,\"strictFetchLaunchVersion\":2"))){
            byte[] raw=wire.getBytes(java.nio.charset.StandardCharsets.UTF_8);
            assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.content_fetch_launch_profiles(launch_profile_id,configuration_bytes,configuration_sha256) VALUES ('bad-wire',?,?)",raw,SourceJson.sha(raw))).isInstanceOf(RuntimeException.class);
        }
    }
    @Test void ambiguousCompleteCompositionsAreNotAdvertised(){
        insert("pipeline","other-pipeline",profiles.get("pipeline"));
        var rows=new DiscoveryRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY)).profiles("CONTENT_RSS_V1",100,null).data();assertThat(rows.isEmpty()).isTrue();
    }
}
