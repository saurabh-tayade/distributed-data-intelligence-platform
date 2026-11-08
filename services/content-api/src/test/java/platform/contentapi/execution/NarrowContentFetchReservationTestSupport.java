package platform.contentapi.execution;

import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.node.ObjectNode;

/** Test-only access; never installs an active launcher or dispatcher. */
public final class NarrowContentFetchReservationTestSupport {
    public record Facts(UUID operation,UUID attempt,UUID checkpoint,String commandSha256,String bootstrapSha256,boolean nativeLaunchCompatible) {}
    public static Facts reserve(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,Consumer<String> boundary){
        return facts(new NarrowContentFetchReservation(jdbc,tx,workers).reserve(guard,owner,operation,b->boundary.accept(b.name())));
    }
    public static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,UUID operation){return facts(new NarrowContentFetchReservation(jdbc,tx,workers).inspect(operation));}
    private static Facts facts(NarrowContentFetchReservation.Facts f){return new Facts(f.operation(),f.attempt(),f.checkpoint(),f.commandSha256(),f.bootstrapSha256(),f.nativeLaunchCompatible());}

    public record Payload(UUID checkpoint,byte[] commandBytes,byte[] bootstrapBytes) {}
    public static Payload payload(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation){var p=new NarrowContentFetchReservation(jdbc,tx,workers).prepare(guard,owner,operation);return new Payload(p.checkpoint(),p.commandBytes(),p.bootstrapBytes());}

    public static String deploymentDigest(JdbcTemplate jdbc,String profileId){
        byte[] bytes=jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id=?",byte[].class,profileId);
        return SourceJson.parse(bytes).get("runtime").get("deploymentSha256").stringValue();
    }

    /** Fully hash-linked runtime registrations, with deliberately nonexistent executable/JAR paths. */
    public static void registerChain(JdbcTemplate jdbc,String profileId,ObjectNode profile,String product){
        registerChain(jdbc,profileId,profile,product,null);
    }
    public static void registerChain(JdbcTemplate jdbc,String profileId,ObjectNode profile,String product,tools.jackson.databind.JsonNode fetchManifest){
        registerChain(jdbc,profileId,profile,product,fetchManifest,true);
    }
    public static void registerChain(JdbcTemplate jdbc,String profileId,ObjectNode profile,String product,tools.jackson.databind.JsonNode fetchManifest,boolean testEndpoint){
        var fixture=new NarrowContentRuntimeQualificationTest.Fixture();
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
        var fetch=fixture.configs.get("fetch");fetch.put("testEndpoint",testEndpoint).put("runProfileId",profileId).put("runProfileSha256",hash).put("controlBindingId",bindings.get("CONTROL"));
        var bronze=fixture.configs.get("bronze");bronze.put("runProfileId",profileId).put("runProfileSha256",hash).put("controlBindingId",bindings.get("CONTROL")).put("storageBindingId",bindings.get("BRONZE")).put("storageBindingSha256",jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.storage_bindings WHERE binding_id=?",String.class,bindings.get("BRONZE")));
        fixture.configs.get("silver").put("runProfileSha256",hash).put("outputBindingId",bindings.get("SILVER")).put("tempBindingId",bindings.get("TEMP"));
        fixture.configs.get("gold").put("goldBindingId",bindings.get("GOLD"));
        fixture.configs.get("content").put("referenceBindingId",bindings.get("REFERENCE")).put("contentBindingId",bindings.get("CONTENT"));
        fixture.relinkAll();
        for(String family:List.of("fetch","bronze","silver","gold","content")){
            insert(jdbc,family,fixture.configs.get(family));
            jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",family,product);
        }
        insert(jdbc,profileId,profile);
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",profileId,product);
    }
    private static void insert(JdbcTemplate jdbc,String id,ObjectNode node){byte[] bytes=SourceJson.canonical(node);jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,bytes,SourceJson.sha(bytes));}
    private NarrowContentFetchReservationTestSupport(){}
}
