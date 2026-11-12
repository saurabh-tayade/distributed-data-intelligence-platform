package platform.contentapi.execution;

import java.util.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** Metadata-only public composition fixtures. No provider delegate or network bypass. */
public final class PublicContentProfileTestSupport {
    static void publicSource(NarrowContentRuntimeQualificationTest.Fixture f){
        f.configs.get("fetch").put("testEndpoint",false);f.relinkAll();
        var selected=(ObjectNode)f.frozen.get("sources").get(0);
        var config=(ObjectNode)selected.get("configuration");config.put("feedEndpointId","00000000-0000-0000-0000-000000000001");
        selected.put("configurationSha256",SourceJson.sha(SourceJson.canonical(config)));
        var endpoint=selected.putObject("endpoint").put("endpointId",config.get("feedEndpointId").stringValue())
                .put("sourceId",selected.get("sourceId").stringValue()).put("uri","https://example.com/feed");
        selected.put("endpointSha256",SourceJson.sha(SourceJson.canonical(endpoint)));
    }
    static ObjectNode fetchLaunch(NarrowContentRuntimeQualificationTest.Fixture f){
        var registration=f.inspect().registration();
        return fetchLaunch(registration.configuration("FETCH"),registration.id(NarrowContentRuntimeQualification.Family.FETCH),registration.digest("FETCH"),
                SourceJson.object().put("path","/nonexistent/public-provider.jar").put("sha256","b".repeat(64)));
    }
    public static ObjectNode fetchLaunch(JsonNode fetch,String fetchId,String fetchDigest,JsonNode provider){
        var c=SourceJson.object().put("strictFetchLaunchVersion",2).put("fetchRuntimeId",fetchId).put("fetchRuntimeSha256",fetchDigest).put("testEndpoint",false);
        for(String key:List.of("runProfileId","runProfileSha256","controlBindingId","javaHome"))c.set(key,fetch.get(key));
        var manifest=((ObjectNode)fetch.get("manifest").deepCopy()).put("adapter",StrictPublicFetchLaunchRuntime.MAIN);
        ((tools.jackson.databind.node.ArrayNode)manifest.get("classpath")).add(provider.deepCopy());c.set("manifest",manifest);
        c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest))).put("networkPolicySha256",StrictPublicFetchLaunchRuntime.networkPolicyDigest());
        c.set("networkPolicy",StrictPublicFetchLaunchRuntime.networkPolicy());return c;
    }
    public static ObjectNode bronze(JsonNode historicalBronze,String bronzeId,String bronzeDigest,JsonNode historicalFetch,
            String fetchLaunchId,String fetchLaunchDigest,boolean verifier){
        String main=verifier?StrictBronzeVerificationRuntime.MAIN:StrictBronzeLaunchRuntime.MAIN;
        var c=SourceJson.object().put(verifier?"strictBronzeVerificationVersion":"strictBronzeLaunchVersion",2)
                .put("bronzeRuntimeId",bronzeId).put("bronzeRuntimeSha256",bronzeDigest)
                .put("sourceTestEndpoint",false).put("storageEndpointPolicy","LITERAL_LOOPBACK_HTTP_V1")
                .put("fetchLaunchProfileId",fetchLaunchId).put("fetchLaunchProfileSha256",fetchLaunchDigest);
        for(String key:List.of("runProfileId","runProfileSha256","controlBindingId","storageBindingId","storageBindingSha256"))c.set(key,historicalBronze.get(key));
        c.set("javaHome",historicalFetch.get("javaHome"));
        var manifest=((ObjectNode)historicalFetch.get("manifest").deepCopy()).put("adapter",main);c.set("manifest",manifest);
        c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest))).put("strictRuntimeSha256",verifier?
                StrictBronzeVerificationRuntime.protocolDigest(manifest.get("javaRuntime").stringValue()):StrictBronzeLaunchRuntime.protocolDigest(manifest.get("javaRuntime").stringValue()));
        return c;
    }
    static ObjectNode bronze(NarrowContentRuntimeQualificationTest.Fixture f,ObjectNode fetch,boolean verifier){
        var r=f.inspect().registration();return bronze(r.configuration("BRONZE"),r.id(NarrowContentRuntimeQualification.Family.BRONZE),r.digest("BRONZE"),r.configuration("FETCH"),"public-fetch",SourceJson.sha(SourceJson.canonical(fetch)),verifier);
    }
    static ObjectNode pipeline(NarrowContentRuntimeQualificationTest.Fixture f,ObjectNode fetch,ObjectNode bronze,ObjectNode verifier){
        var r=f.inspect().registration();var c=SourceJson.object().put("strictContentPipelineVersion",2).put("runProfileId",r.profileId()).put("runProfileSha256",r.profileDigest())
                .put("contentRuntimeId",r.id(NarrowContentRuntimeQualification.Family.CONTENT)).put("contentRuntimeSha256",r.digest("CONTENT"))
                .put("controlBindingId",f.host.id()).put("strictRuntimeSha256",StrictContentPipelineRuntime.protocolDigest())
                .put("fetchLaunchProfileId","public-fetch").put("fetchLaunchProfileSha256",SourceJson.sha(SourceJson.canonical(fetch)))
                .put("bronzeLaunchProfileId","public-bronze").put("bronzeLaunchProfileSha256",SourceJson.sha(SourceJson.canonical(bronze)))
                .put("bronzeVerificationProfileId","public-verifier").put("bronzeVerificationProfileSha256",SourceJson.sha(SourceJson.canonical(verifier)));
        for(String key:List.of("python","processingRoot","javaHome","modules"))c.set(key,r.configuration("SILVER").get(key));
        c.set("versions",StrictContentPipelineRuntime.versions());return c;
    }
    /** Real SQL registration, synthetic uninstalled bytes, and no network/process readiness claim. */
    public static Map<String,ObjectNode> registerMetadataChain(org.springframework.jdbc.core.JdbcTemplate jdbc,String profileId,ObjectNode profile,String product){
        var f=new NarrowContentRuntimeQualificationTest.Fixture();
        var bindings=new HashMap<String,String>();
        for(var row:jdbc.queryForList("SELECT binding_id,role FROM ddip_management.storage_bindings WHERE product=?",product))bindings.put((String)row.get("role"),(String)row.get("binding_id"));
        profile.put("runProfileVersion",2).put("contentRuntimeId","content");((ObjectNode)profile.get("runtime")).put("deploymentSha256",f.deployment);
        String profileDigest=SourceJson.sha(SourceJson.canonical(profile));
        for(var config:f.configs.values())if(config.has("runProfileSha256"))config.put("runProfileSha256",profileDigest);
        var fetch=f.configs.get("fetch");fetch.put("runProfileId",profileId).put("controlBindingId",bindings.get("CONTROL")).put("testEndpoint",false);
        var bronze=f.configs.get("bronze");bronze.put("runProfileId",profileId).put("controlBindingId",bindings.get("CONTROL"))
                .put("storageBindingId",bindings.get("BRONZE")).put("storageBindingSha256",jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.storage_bindings WHERE binding_id=?",String.class,bindings.get("BRONZE")));
        f.configs.get("silver").put("outputBindingId",bindings.get("SILVER")).put("tempBindingId",bindings.get("TEMP"));
        f.configs.get("gold").put("goldBindingId",bindings.get("GOLD"));f.configs.get("content").put("referenceBindingId",bindings.get("REFERENCE")).put("contentBindingId",bindings.get("CONTENT"));
        f.relinkAll();
        for(String family:List.of("fetch","bronze","silver","gold","content"))insertMetadata(jdbc,family,f.configs.get(family),product);
        insertMetadata(jdbc,profileId,profile,product);
        var launch=fetchLaunch(fetch,"fetch",SourceJson.sha(SourceJson.canonical(fetch)),SourceJson.object().put("path","/nonexistent/provider.jar").put("sha256","b".repeat(64)));
        var bronzeLaunch=bronze(bronze,"bronze",SourceJson.sha(SourceJson.canonical(bronze)),fetch,"public-fetch",SourceJson.sha(SourceJson.canonical(launch)),false);
        var verifier=bronze(bronze,"bronze",SourceJson.sha(SourceJson.canonical(bronze)),fetch,"public-fetch",SourceJson.sha(SourceJson.canonical(launch)),true);
        var pipeline=SourceJson.object().put("strictContentPipelineVersion",2).put("runProfileId",profileId).put("runProfileSha256",profileDigest)
                .put("contentRuntimeId","content").put("contentRuntimeSha256",SourceJson.sha(SourceJson.canonical(f.configs.get("content"))))
                .put("controlBindingId",bindings.get("CONTROL")).put("strictRuntimeSha256",StrictContentPipelineRuntime.protocolDigest())
                .put("fetchLaunchProfileId","public-fetch").put("fetchLaunchProfileSha256",SourceJson.sha(SourceJson.canonical(launch)))
                .put("bronzeLaunchProfileId","public-bronze").put("bronzeLaunchProfileSha256",SourceJson.sha(SourceJson.canonical(bronzeLaunch)))
                .put("bronzeVerificationProfileId","public-verifier").put("bronzeVerificationProfileSha256",SourceJson.sha(SourceJson.canonical(verifier)));
        for(String key:List.of("python","processingRoot","javaHome"))pipeline.set(key,f.configs.get("silver").get(key));
        pipeline.set("versions",StrictContentPipelineRuntime.versions());var modules=pipeline.putObject("modules");
        for(String name:List.of("strict_native_content.py","strict_native_runtime.py","strict_native_sink.py"))modules.put(name,"a".repeat(64));
        return Map.of("fetch",launch,"bronze",bronzeLaunch,"verifier",verifier,"pipeline",pipeline);
    }
    private static void insertMetadata(org.springframework.jdbc.core.JdbcTemplate jdbc,String id,ObjectNode c,String product){
        byte[] raw=SourceJson.canonical(c);jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,raw,SourceJson.sha(raw));
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",id,product);
    }
    private PublicContentProfileTestSupport(){}
}
