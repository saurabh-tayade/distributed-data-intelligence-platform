package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;

/** Test-only actual installed runtime registration; no metadata-only stage fabrication. */
public final class NarrowContentPipelineTestSupport {
    public static void registerChain(JdbcTemplate jdbc,String profileId,ObjectNode profile,String product,tools.jackson.databind.JsonNode fetchManifest,java.nio.file.Path processing,java.nio.file.Path python)throws Exception{
        registerChain(jdbc,profileId,profile,product,fetchManifest,processing,python,true);
    }
    /** Distinct source descriptor flag; the old overload retains literal-loopback semantics. */
    public static void registerChain(JdbcTemplate jdbc,String profileId,ObjectNode profile,String product,tools.jackson.databind.JsonNode fetchManifest,java.nio.file.Path processing,java.nio.file.Path python,boolean testEndpoint)throws Exception{
        var fixture=new NarrowContentRuntimeQualificationTest.Fixture();
        fixture.configs.get("fetch").put("testEndpoint",testEndpoint);
        String deployment=fetchManifest==null?fixture.deployment:SourceJson.sha(SourceJson.canonical(fetchManifest));
        if(fetchManifest!=null){
            fixture.configs.get("fetch").set("manifest",fetchManifest);
            fixture.configs.get("fetch").put("javaHome",java.nio.file.Path.of(System.getProperty("java.home")).toString()).put("httpTimeoutMillis",30000);
            for(var config:fixture.configs.values())if(config.has("deploymentSha256"))config.put("deploymentSha256",deployment);
        }
        profile.put("runProfileVersion",2).put("contentRuntimeId","content");
        ((ObjectNode)profile.get("runtime")).put("deploymentSha256",deployment);
        String hash=NarrowContentRuntimeQualificationTest.sha(profile);
        var bindings=new HashMap<String,String>();
        for(var row:jdbc.queryForList("SELECT binding_id,role FROM ddip_management.storage_bindings WHERE product=?",product))bindings.put((String)row.get("role"),(String)row.get("binding_id"));
        var fetch=fixture.configs.get("fetch");fetch.put("runProfileId",profileId).put("runProfileSha256",hash).put("controlBindingId",bindings.get("CONTROL"));
        var bronze=fixture.configs.get("bronze");bronze.put("runProfileId",profileId).put("runProfileSha256",hash).put("controlBindingId",bindings.get("CONTROL")).put("storageBindingId",bindings.get("BRONZE")).put("storageBindingSha256",jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.storage_bindings WHERE binding_id=?",String.class,bindings.get("BRONZE")));
        fixture.configs.get("silver").put("runProfileSha256",hash).put("outputBindingId",bindings.get("SILVER")).put("tempBindingId",bindings.get("TEMP"));
        fixture.configs.get("gold").put("goldBindingId",bindings.get("GOLD"));
        fixture.configs.get("content").put("referenceBindingId",bindings.get("REFERENCE")).put("contentBindingId",bindings.get("CONTENT"));
        for(String family:List.of("silver","gold","content")){
            var config=fixture.configs.get(family);config.put("seconds",300);
            var modules=config.putObject("modules");
            for(String name:NarrowContentInstalledRuntimeTestSupport.modules(family))modules.put(name,SourceJson.sha(java.nio.file.Files.readAllBytes(processing.resolve(name))));
        }
        fixture.configs.get("silver").put("python",python.toString()).put("processingRoot",processing.toString()).put("javaHome",System.getProperty("java.home"));
        fixture.configs.get("gold").put("python",python.toString());
        fixture.relinkAll();
        for(String family:List.of("fetch","bronze","silver","gold","content")){
            insert(jdbc,family,fixture.configs.get(family));
            jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",family,product);
        }
        insert(jdbc,profileId,profile);
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",profileId,product);
    }
    private static void insert(JdbcTemplate jdbc,String id,ObjectNode node){byte[] bytes=SourceJson.canonical(node);jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,bytes,SourceJson.sha(bytes));}

    public record Completed(UUID run,UUID attempt,UUID artifact,String proofSha256,WorkerRepository.Session worker){}
    public record Event(String operation,String boundary){}
    public static Completed execute(JdbcTemplate j,ManagementTransactions t,WorkerRepository w,InstallationHostGuard g,WorkerRepository.Owner o,String profile,java.util.function.Consumer<Event> fault){
        var result=new NarrowContentPipeline(j,t,w).executeOriginal(g,o,profile,e->fault.accept(new Event(e.operation().name(),e.boundary().name())));
        return new Completed(result.run(),result.attempt(),result.artifact(),result.proofSha256(),result.worker());
    }
    /** Historical Reliability/NONE capabilities only; not the qualified Content capability. */
    public static boolean legacyCapabilitiesDispatchContent(){return WorkerExecutionCapability.RELIABILITY_V2.supports("CONTENT_RSS_V1") || WorkerExecutionCapability.NONE.supports("CONTENT_RSS_V1");}
    public static List<Path> spareDirectories(JdbcTemplate j,ManagementTransactions t,InstallationHostGuard g,WorkerRepository.Owner o){
        var ready=NarrowContentAdmission.inspectReady(j,t,g,o.runId(),o.attemptId());var context=new NarrowContentEvidenceContext(ready);
        return List.of(context.originalDirectory("INSPECT_BRONZE",4),context.originalDirectory("CHECK_INPUTS",3));
    }
    public static String protocolDigest(){return StrictContentPipelineRuntime.protocolDigest();}
    public static tools.jackson.databind.JsonNode versions(){return StrictContentPipelineRuntime.versions();}
    private NarrowContentPipelineTestSupport(){}
}
