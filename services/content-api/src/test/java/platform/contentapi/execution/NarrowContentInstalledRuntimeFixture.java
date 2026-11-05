package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** Real files and connectionless registered metadata. No READY provenance is claimed by this seam. */
final class NarrowContentInstalledRuntimeFixture implements AutoCloseable {
    final Path base,processing,jar,silverPython,goldPython;
    final InstallationHostGuard guard;
    final Map<String,ObjectNode> configs=new HashMap<>();
    final ObjectNode frozen=SourceJson.object();
    final Map<String,Path> roots=new TreeMap<>();
    final UUID run=UUID.randomUUID(),attempt=UUID.randomUUID();
    final ManagementTransactions tx=new ManagementTransactions(new org.springframework.transaction.support.TransactionTemplate(){
        @Override public <T> T execute(org.springframework.transaction.support.TransactionCallback<T> callback){return callback.doInTransaction(new org.springframework.transaction.support.SimpleTransactionStatus());}
    });
    final JdbcTemplate jdbc=new JdbcTemplate(new org.springframework.jdbc.datasource.AbstractDataSource(){
        @Override public java.sql.Connection getConnection(){throw new AssertionError("Unexpected SQL or mutation");}
        @Override public java.sql.Connection getConnection(String u,String p){throw new AssertionError("Unexpected SQL or mutation");}
    }){
        @Override public List<Map<String,Object>> queryForList(String sql,Object... args){
            if(sql.equals("SELECT captured_selection,selection_sha256,execution_profile_id FROM ddip_management.runs WHERE run_id=?") && args.length==1 && run.equals(args[0]))return List.of(Map.of("captured_selection",SourceJson.canonical(frozen),"selection_sha256",sha(frozen),"execution_profile_id","profile"));
            if(sql.equals("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?") && args.length==1){var c=configs.get(args[0]);return c==null?List.of():List.of(Map.of("configuration_bytes",SourceJson.canonical(c),"configuration_sha256",sha(c)));}
            throw new AssertionError("Unexpected SQL");
        }
        @Override public <T> T queryForObject(String sql,Class<T> type,Object... args){
            if(sql.equals("SELECT EXISTS(SELECT 1 FROM ddip_management.execution_profile_storage p JOIN ddip_management.storage_bindings b USING(binding_id) WHERE p.profile_id=? AND b.binding_id=? AND b.installation_id=? AND b.role=? AND b.product=? AND b.configuration_sha256=? AND b.configuration_bytes=?)") && type==Boolean.class && args.length==7)return type.cast(true);
            throw new AssertionError("Unexpected SQL");
        }
    };
    NarrowContentInstalledRuntimeFixture(Path parent) throws Exception {
        base=Files.createTempDirectory(parent,"installed-").toRealPath();processing=Files.createDirectory(base.resolve("processing"));
        jar=Files.writeString(base.resolve("code.jar"),"registered fixture bytes, never loaded");
        silverPython=executable(base.resolve("silver-python"));goldPython=executable(base.resolve("gold-python"));
        UUID installation=UUID.randomUUID();Path control=Files.createDirectory(base.resolve("control"));privateMode(control,true);
        Files.writeString(control.resolve("installation.id"),installation+"\n");Files.createFile(control.resolve("host.guard"));privateMode(control.resolve("installation.id"),false);privateMode(control.resolve("host.guard"),false);
        var host=new InstallationHostGuard.Binding("control",installation,control,InstallationHostGuard.identity(control),InstallationHostGuard.identity(control.resolve("host.guard")),InstallationHostGuard.identity(control.resolve("installation.id")));
        guard=InstallationHostGuard.acquire(host);
        frozen.put("pipeline","CONTENT_RSS_V1");var source=frozen.putArray("sources").addObject().put("sourceId","source-one").put("revision",1);
        var sc=source.putObject("configuration").put("inputStorageProfileId","bronze");source.put("configurationSha256",sha(sc));
        var endpoint=source.putObject("endpoint").put("uri","http://127.0.0.1:1234/feed");source.put("endpointSha256",sha(endpoint));
        frozen.putArray("inputs").addObject().put("role","PUBLISHER_REFERENCE").put("ordinal",0).put("storageBindingId","reference");frozen.putArray("storageBindings");
        binding("control","CONTROL",SourceJson.parse(host.descriptor()));roots.put("CONTROL",control);
        binding("bronze","BRONZE",SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:9000").put("region","local").put("bucket","bronze").put("credentialsFile",base.resolve("never-read-credentials").toString()));
        for(String role:List.of("SILVER","TEMP","GOLD","REFERENCE","CONTENT","STAGING")){
            Path root=Files.createDirectory(base.resolve(role.toLowerCase()));privateMode(root,true);roots.put(role,root);var i=InstallationHostGuard.identity(root);
            binding(role.toLowerCase(),role,SourceJson.object().put("importStorageVersion",1).put("root",root.toString()).put("rootDevice",Long.toString(i.device())).put("rootInode",Long.toString(i.inode())));
        }
        var manifest=SourceJson.object().put("adapter","platform.ingestion.managed.BoundedManagedRssFetch").put("javaRuntime",Runtime.version().toString());manifest.putArray("classpath").addObject().put("path",jar.toString()).put("sha256",SourceJson.sha(Files.readAllBytes(jar)));
        var profile=SourceJson.object().put("runProfileVersion",2).put("pipeline","CONTENT_RSS_V1").put("pipelineVersion",1).put("sourceProfile","rss-atom-v1").put("contentRuntimeId","content");profile.putObject("runtime").put("javaVersion","21").put("pythonVersion","3.12.15").put("pysparkVersion","4.0.2").put("deploymentSha256",sha(manifest));configs.put("profile",profile);
        String javaHome=Path.of(System.getProperty("java.home")).toRealPath().toString();
        var fetch=SourceJson.object().put("rssFetchRuntimeVersion",2).put("pipeline","CONTENT_RSS_V1").put("runProfileId","profile").put("javaHome",javaHome).put("controlBindingId","control").put("testEndpoint",true).put("httpTimeoutMillis",1000);fetch.set("manifest",manifest);configs.put("fetch",fetch);
        configs.put("bronze",SourceJson.object().put("bronzeRuntimeVersion",2).put("adapter","platform.ingestion.managed.BoundedManagedBronze").put("fetchRuntimeId","fetch").put("runProfileId","profile").put("controlBindingId","control").put("storageBindingId","bronze").put("storageBindingSha256",sha(frozen.get("storageBindings").get(1).get("configuration"))));
        var silver=SourceJson.object().put("silverRuntimeVersion",2).put("bronzeRuntimeId","bronze").put("python",silverPython.toString()).put("javaHome",javaHome).put("processingRoot",processing.toString()).put("outputBindingId","silver").put("tempBindingId","temp").put("seconds",30);modules(silver,RssSilverRuntime.MODULES);silver.set("versions",SourceJson.object().put("python","3.12.15").put("pyspark","4.0.2").put("boto3","1.43.114").put("java","21"));configs.put("silver",silver);
        var gold=SourceJson.object().put("goldRuntimeVersion",2).put("silverRuntimeId","silver").put("goldBindingId","gold").put("duckdb","1.4.5").put("seconds",30).put("python",goldPython.toString());modules(gold,RssGoldRuntime.MODULES);configs.put("gold",gold);
        var content=SourceJson.object().put("contentRuntimeVersion",2).put("goldRuntimeId","gold").put("referenceBindingId","reference").put("contentBindingId","content").put("seconds",30);modules(content,RssContentRuntime.MODULES);configs.put("content",content);relink();
    }
    void relink(){
        String deployment=sha(configs.get("fetch").get("manifest"));((ObjectNode)configs.get("profile").get("runtime")).put("deploymentSha256",deployment);String profile=sha(configs.get("profile"));
        frozen.putObject("executionProfile").put("profileId","profile").put("configurationSha256",profile).set("configuration",configs.get("profile"));
        for(String family:List.of("silver","bronze","fetch"))configs.get(family).put("runProfileSha256",profile).put("deploymentSha256",deployment);
        for(String[] pair:new String[][]{{"bronze","fetch"},{"silver","bronze"},{"gold","silver"},{"content","gold"}})configs.get(pair[0]).put(pair[1]+"RuntimeSha256",sha(configs.get(pair[1])));
    }
    NarrowContentRuntimeQualification.Snapshot correspondence(){
        relink();var i=new InstallationHostGuard.Identity(1,2);
        var e=new NarrowContentEvidenceContext.Expected(NarrowContentProfile.POLICY,run,attempt,NarrowContentEvidenceContext.PlanKind.ORIGINAL,"source-one",guard.binding(),i,i,new NarrowContentEvidenceContext.FrozenIdentities(sha(frozen),sha(configs.get("profile")),sha(configs.get("content"))),"d".repeat(64));
        return NarrowContentRuntimeQualification.inspect(jdbc,tx,e);
    }
    NarrowContentInstalledRuntimeQualification.Installed observe(){return NarrowContentInstalledRuntimeQualification.observe(correspondence(),guard,p->NarrowContentInstalledRuntimeQualification.FREE_BYTES);}
    ObjectNode binding(String role){for(var b:frozen.get("storageBindings"))if(b.get("role").stringValue().equals(role))return (ObjectNode)b;throw new AssertionError(role);}
    void binding(String id,String role,JsonNode c){frozen.withArray("storageBindings").addObject().put("bindingId",id).put("productId","content-intelligence").put("role",role).put("configurationSha256",sha(c)).set("configuration",c);}
    void modules(ObjectNode config,List<String> names) throws Exception {var modules=config.putObject("modules");for(String name:ContentEvidenceProfile.modules(names,2)){Path p=processing.resolve(name);if(!Files.exists(p))Files.writeString(p,"raise RuntimeError('NEVER IMPORT')\n");modules.put(name,SourceJson.sha(Files.readAllBytes(p)));}}
    static String sha(JsonNode n){return SourceJson.sha(SourceJson.canonical(n));}
    static void privateMode(Path p,boolean directory)throws Exception{Files.setPosixFilePermissions(p,PosixFilePermissions.fromString(directory?"rwx------":"rw-------"));}
    static Path executable(Path p)throws Exception{Files.writeString(p,"#!/bin/sh\nexit 97\n");privateMode(p,true);return p;}
    @Override public void close(){guard.close();}
}
