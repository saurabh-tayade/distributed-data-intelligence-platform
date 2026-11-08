package platform.contentapi.execution;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.zip.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

class StrictPublicFetchProfileTest {
    private static final String POLICY="{\"addressFamily\":\"IPV4_ONLY\",\"addressPolicySha256\":\"85959d534b478bf1c8194e99f49a2c860f9fe9864978ed7118496913767cefcd\",\"addressPolicyVersion\":1,\"httpVersion\":\"HTTP_1_1\",\"maxApplicationInvocations\":1,\"maxResolvedAddresses\":64,\"policyId\":\"ddip-public-feed-pinned-v1\",\"policyVersion\":1,\"proxies\":false,\"redirects\":false,\"resolverProvider\":\"platform.ingestion.managed.PublicFetchResolverProvider\",\"transportRetryPolicy\":\"JDK_BOUNDED_PINNED_V1\"}";
    final NarrowContentRuntimeQualificationTest.Fixture f=new NarrowContentRuntimeQualificationTest.Fixture();
    StrictPublicFetchProfileTest(){PublicContentProfileTestSupport.publicSource(f);}
    ObjectNode fetch(){return PublicContentProfileTestSupport.fetchLaunch(f);}
    ContentFetchLaunchRuntime.Profile profile(ObjectNode c){return new ContentFetchLaunchRuntime.Profile("public-fetch",SourceJson.sha(SourceJson.canonical(c)),c);}
    @Test void policyBytesMatchTransportAndReviewedAddressSnapshot()throws Exception{
        assertThat(SourceJson.canonical(StrictPublicFetchLaunchRuntime.networkPolicy())).isEqualTo(POLICY.getBytes(StandardCharsets.UTF_8));
        assertThat(StrictPublicFetchLaunchRuntime.networkPolicyDigest()).isEqualTo("86e5e8ef709e0b1a7472515ca2569914668565c4a39785b52b6c4c36e4f8b808");
        assertThat(SourceJson.sha(Files.readAllBytes(Path.of("../../docs/architecture/0046-public-fetch-ipv4-deny-snapshot.json"))))
                .isEqualTo(StrictPublicFetchLaunchRuntime.ADDRESS_POLICY_SHA256);
    }
    @Test void publicMetadataQualifiesWithoutInstalledFilesAndDoesNotWidenV1(){
        var c=fetch();byte[] old=SourceJson.canonical(f.configs.get("fetch"));
        StrictPublicFetchLaunchRuntime.validate(c,f.inspect(),f.host);
        ContentFetchLaunchRuntime.validateSelection(profile(c),f.inspect(),f.host);
        assertThat(SourceJson.canonical(f.configs.get("fetch"))).isEqualTo(old);
        assertThatThrownBy(()->StrictFetchLaunchRuntime.validate(c,f.inspect(),f.host)).isInstanceOf(RuntimeException.class);
        assertThat(c.get("manifest").get("classpath")).hasSize(2);
        assertThat(Files.exists(f.host.root())).isFalse();
        var immutable=profile(c);((ObjectNode)immutable.configuration()).put("testEndpoint",true);
        assertThat(immutable.configuration().get("testEndpoint").booleanValue()).isFalse();
        assertThatThrownBy(()->new ContentFetchLaunchRuntime.Profile("public-fetch","0".repeat(64),c)).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest @ValueSource(strings={"version","testEndpoint","runtime","policy","policyDigest","deployment","adapter","classpath","duplicatePath","missingProvider","extraProvider","unknown"})
    void rejectsAnySelfConsistentPublicRegistrationSubstitution(String mutation){
        var c=fetch();var manifest=(ObjectNode)c.get("manifest");var cp=(tools.jackson.databind.node.ArrayNode)manifest.get("classpath");
        switch(mutation){
            case "version"->c.put("strictFetchLaunchVersion","2");case "testEndpoint"->c.put("testEndpoint",true);case "runtime"->c.put("fetchRuntimeSha256","0".repeat(64));
            case "policy"->{((ObjectNode)c.get("networkPolicy")).put("maxResolvedAddresses",65);c.put("networkPolicySha256",SourceJson.sha(SourceJson.canonical(c.get("networkPolicy"))));}
            case "policyDigest"->c.put("networkPolicySha256","0".repeat(64));case "deployment"->c.put("deploymentSha256","0".repeat(64));
            case "adapter"->manifest.put("adapter",StrictFetchLaunchRuntime.MAIN);case "classpath"->((ObjectNode)cp.get(0)).put("sha256","0".repeat(64));
            case "duplicatePath"->((ObjectNode)cp.get(1)).put("path",cp.get(0).get("path").stringValue());case "missingProvider"->cp.remove(1);case "extraProvider"->cp.add(cp.get(1).deepCopy());
            case "unknown"->c.put("unknown",true);
        }
        if(!mutation.equals("deployment"))c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)));
        assertThatThrownBy(()->StrictPublicFetchLaunchRuntime.validateRegistration(c,f.inspect().registration(),f.host)).isInstanceOf(RuntimeException.class);
    }
    @ParameterizedTest @ValueSource(strings={"/nonexistent/provider.zip","/nonexistent/../provider.jar","/nonexistent/./provider.jar","/nonexistent//provider.jar","relative.jar"})
    void providerPathsHaveIdenticalSqlAndJavaSpelling(String path){
        var c=fetch();((ObjectNode)c.get("manifest").get("classpath").get(1)).put("path",path);
        c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(c.get("manifest"))));
        assertThatThrownBy(()->StrictPublicFetchLaunchRuntime.validateRegistration(c,f.inspect().registration(),f.host)).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest @ValueSource(strings={"http://127.0.0.1/feed","http://[::1]/feed","http://localhost/feed","http://foo.local/feed","http://foo.internal/feed","http://foo.localhost/feed","http://example.com:0/feed","http://example.com:65536/feed","http://user@example.com/feed","http://example.com/feed?q=1","http://example.com/feed#x","file:///feed"})
    void publicSelectionDoesNotResolveOrAdmitLocalLiteralCredentialsOrAmbiguity(String uri){
        var selected=(ObjectNode)f.frozen.get("sources").get(0);((ObjectNode)selected.get("endpoint")).put("uri",uri);
        selected.put("endpointSha256",SourceJson.sha(SourceJson.canonical(selected.get("endpoint"))));
        assertThatThrownBy(()->StrictPublicFetchLaunchRuntime.validate(fetch(),f.inspect(),f.host)).isInstanceOf(RuntimeException.class);
    }
    @ParameterizedTest @ValueSource(strings={"endpointId","sourceId","endpointDigest","sourceDigest"})
    void frozenTupleAndConfigurationDigestsAreAuthority(String changed){
        var selected=(ObjectNode)f.frozen.get("sources").get(0);var endpoint=(ObjectNode)selected.get("endpoint");
        if(changed.equals("endpointId") || changed.equals("sourceId"))endpoint.put(changed,"different");
        else selected.put(changed.equals("endpointDigest")?"endpointSha256":"configurationSha256","0".repeat(64));
        assertThatThrownBy(()->StrictPublicFetchLaunchRuntime.validate(fetch(),f.inspect(),f.host)).isInstanceOf(RuntimeException.class);
    }
    @Test void bronzePublicCompositionKeepsLocalStorageAndHistoricalClasspath(){
        var fetch=fetch();var r=f.inspect().registration();
        for(boolean verifier:List.of(false,true)){
            var c=PublicContentProfileTestSupport.bronze(f,fetch,verifier);
            if(verifier)StrictBronzeVerificationRuntime.validatePublic(c,r,f.host,profile(fetch));
            else StrictBronzeLaunchRuntime.validatePublic(c,r,f.host,profile(fetch));
            assertThat(c.get("manifest").get("classpath")).isEqualTo(f.configs.get("fetch").get("manifest").get("classpath"));
            assertThat(c.has("testEndpoint")).isFalse();
            assertThat(c.get("storageEndpointPolicy").stringValue()).isEqualTo("LITERAL_LOOPBACK_HTTP_V1");
        }
    }
    @ParameterizedTest @ValueSource(strings={"version","digest","id","source","storagePolicy","providerClasspath","unknown"})
    void bronzeAndVerifierRejectWrongPublicLinkOrExpandedAuthority(String changed){
        var fetch=fetch();var r=f.inspect().registration();
        for(boolean verifier:List.of(false,true)){
            var c=PublicContentProfileTestSupport.bronze(f,fetch,verifier);
            switch(changed){case "version"->c.put(verifier?"strictBronzeVerificationVersion":"strictBronzeLaunchVersion",1);
                case "digest"->c.put("fetchLaunchProfileSha256","0".repeat(64));case "id"->c.put("fetchLaunchProfileId","other");
                case "source"->c.put("sourceTestEndpoint",true);case "storagePolicy"->c.put("storageEndpointPolicy","PUBLIC_HTTP");
                case "providerClasspath"->{((ObjectNode)c.get("manifest")).set("classpath",fetch.get("manifest").get("classpath"));c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(c.get("manifest"))));}
                case "unknown"->c.put("testEndpoint",true);}
            assertThatThrownBy(()->{if(verifier)StrictBronzeVerificationRuntime.validatePublic(c,r,f.host,profile(fetch));else StrictBronzeLaunchRuntime.validatePublic(c,r,f.host,profile(fetch));}).hasMessage("RUNTIME_MISMATCH");
        }
    }
    @Test void completeCompositionReopensPureMetadataBeforeAnyLaunchPhase(){
        var fetch=fetch();var bronze=PublicContentProfileTestSupport.bronze(f,fetch,false);var verifier=PublicContentProfileTestSupport.bronze(f,fetch,true);
        var pipeline=PublicContentProfileTestSupport.pipeline(f,fetch,bronze,verifier);var jdbc=rows(fetch,bronze,verifier,pipeline);
        var actual=ContentFetchLaunchRuntime.forRunProfile(jdbc,f.tx,f.inspect().registration(),f.host);
        assertThat(actual.configuration()).isEqualTo(fetch);assertThat(actual.version()).isEqualTo(2);
        StrictContentPipelineRuntime.validateComposition(jdbc,f.tx,pipeline,f.inspect().registration(),f.host);
    }
    @ParameterizedTest @ValueSource(strings={"fetchLaunchProfileSha256","bronzeLaunchProfileSha256","bronzeVerificationProfileSha256","fetchLaunchProfileId","bronzeLaunchProfileId","bronzeVerificationProfileId","mixed"})
    void pipelineRequiresAllThreeExactIdsHashesAndVersions(String changed){
        var fetch=fetch();var bronze=PublicContentProfileTestSupport.bronze(f,fetch,false);var verifier=PublicContentProfileTestSupport.bronze(f,fetch,true);
        var pipeline=PublicContentProfileTestSupport.pipeline(f,fetch,bronze,verifier);
        if(changed.equals("mixed"))pipeline.put("strictContentPipelineVersion",1);else pipeline.put(changed,changed.endsWith("Sha256")?"0".repeat(64):"other");
        var jdbc=rows(fetch,bronze,verifier,pipeline);
        assertThatThrownBy(()->StrictContentPipelineRuntime.validateComposition(jdbc,f.tx,pipeline,f.inspect().registration(),f.host)).isInstanceOf(RuntimeException.class);
    }
    JdbcTemplate rows(ObjectNode fetch,ObjectNode bronze,ObjectNode verifier,ObjectNode pipeline){
        var entries=Map.of("public-fetch",fetch,"public-bronze",bronze,"public-verifier",verifier,"public-pipeline",pipeline);
        return new JdbcTemplate(){@Override public List<Map<String,Object>> queryForList(String sql,Object... args){
            if(sql.contains("SELECT profile_id FROM"))return List.of(Map.of("profile_id","public-pipeline"));
            var c=entries.get((String)args[0]);if(c==null)return List.of();byte[] raw=SourceJson.canonical(c);
            return List.of(Map.of("configuration_bytes",raw,"configuration_sha256",SourceJson.sha(raw)));
        }};
    }
    @Test void providerServiceIsForbiddenInHistoricalClasspathAndMustBeExact()throws Exception{
        var provider=new LinkedHashMap<String,String>();
        for(String name:List.of("PublicFetchResolverProvider","PublicFetchResolverProvider$State","PublicFetchResolverProvider$1","PublicFetchAddressPolicy"))provider.put("platform/ingestion/managed/"+name+".class","class");
        provider.put("META-INF/services/java.net.spi.InetAddressResolverProvider",StrictPublicFetchLaunchRuntime.PROVIDER+"\n");
        StrictPublicFetchLaunchRuntime.checkJar(jar(provider),true,true);
        assertThatThrownBy(()->StrictPublicFetchLaunchRuntime.checkJar(jar(provider),false,false)).hasMessage("RUNTIME_MISMATCH");
        provider.put("foreign.class","foreign");assertThatThrownBy(()->StrictPublicFetchLaunchRuntime.checkJar(jar(provider),true,true)).hasMessage("RUNTIME_MISMATCH");provider.remove("foreign.class");
        provider.put("META-INF/services/java.net.spi.InetAddressResolverProvider",StrictPublicFetchLaunchRuntime.PROVIDER+"\nforeign.Provider\n");
        assertThatThrownBy(()->StrictPublicFetchLaunchRuntime.checkJar(jar(provider),true,true)).hasMessage("RUNTIME_MISMATCH");
        assertThatThrownBy(()->StrictPublicFetchLaunchRuntime.checkJar(jar(Map.of("META-INF/MANIFEST.MF","Manifest-Version: 1.0\nClass-Path: external.jar\n\n")),false,true)).hasMessage("RUNTIME_MISMATCH");
    }
    byte[] jar(Map<String,String> entries)throws IOException{
        var out=new ByteArrayOutputStream();try(var zip=new ZipOutputStream(out)){
            for(var e:entries.entrySet()){zip.putNextEntry(new ZipEntry(e.getKey()));zip.write(e.getValue().getBytes(StandardCharsets.UTF_8));zip.closeEntry();}
        }return out.toByteArray();
    }
}
