package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.jar.JarOutputStream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Real PostgreSQL/READY and installed-file observations. Stubs are never launched or imported. */
class NarrowContentInstalledRuntimeIT extends NarrowContentAdmissionFixtures {
    Path installed, processing, jar, python, javaHome;
    final Map<String,ObjectNode> configurations=new LinkedHashMap<>();

    @Override void storage(String id,String product,String role) {
        if (!role.equals("BRONZE")) {super.storage(id,product,role);return;}
        // The nonexistent credentials path is deliberately not materialized or read.
        var config=SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:9000")
                .put("region","local").put("bucket","bronze")
                .put("credentialsFile",temp.resolve("never-read-credentials").toString());
        byte[] bytes=SourceJson.canonical(config);
        jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",
                id,product,role,bytes,SourceJson.sha(bytes),INSTALL);
    }

    @Override void profile(String id,boolean content,String source) {
        if (!content) {super.profile(id,false,source);return;}
        try {
            configurations.clear();
            installed=Files.createTempDirectory(temp,"installed-runtime-").toRealPath();
            roots.put("installed-runtime-fixture",installed);
            processing=Files.createDirectory(installed.resolve("processing"));
            javaHome=Path.of(System.getProperty("java.home")).toRealPath();
            python=stub(installed.resolve("python"));
            jar=installed.resolve("adapter.jar");try(var out=new JarOutputStream(Files.newOutputStream(jar))) { }
            var manifest=SourceJson.object().put("adapter","platform.ingestion.managed.BoundedManagedRssFetch").put("javaRuntime",Runtime.version().toString());
            manifest.putArray("classpath").addObject().put("path",jar.toString()).put("sha256",SourceJson.sha(Files.readAllBytes(jar)));
            String deployment=sha(manifest);
            var profile=RunFixtures.profile(true,source).put("runProfileVersion",2).put("contentRuntimeId",RUNTIME);
            ((ObjectNode)profile.get("runtime")).put("deploymentSha256",deployment);
            register(id,profile);String profileHash=sha(profile);
            var fetch=common(profileHash,deployment).put("rssFetchRuntimeVersion",2).put("pipeline","CONTENT_RSS_V1")
                    .put("runProfileId",id).put("javaHome",javaHome.toString()).put("controlBindingId",binding.id())
                    .put("testEndpoint",true).put("httpTimeoutMillis",1000);fetch.set("manifest",manifest);register("installed-fetch",fetch);
            var bronze=common(profileHash,deployment).put("bronzeRuntimeVersion",2).put("adapter","platform.ingestion.managed.BoundedManagedBronze")
                    .put("fetchRuntimeId","installed-fetch").put("fetchRuntimeSha256",sha(fetch)).put("runProfileId",id)
                    .put("controlBindingId",binding.id()).put("storageBindingId",BRONZE)
                    .put("storageBindingSha256",jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.storage_bindings WHERE binding_id=?",String.class,BRONZE));
            register("installed-bronze",bronze);
            var silver=common(profileHash,deployment).put("silverRuntimeVersion",2).put("bronzeRuntimeId","installed-bronze")
                    .put("bronzeRuntimeSha256",sha(bronze)).put("python",python.toString()).put("javaHome",javaHome.toString())
                    .put("processingRoot",processing.toString()).put("outputBindingId",SILVER_BINDING).put("tempBindingId","content-TEMP").put("seconds",30);
            modules(silver,"silver");silver.set("versions",SourceJson.object().put("python","3.12.15").put("pyspark","4.0.2").put("boto3","1.43.114").put("java","21"));register("installed-silver",silver);
            var gold=SourceJson.object().put("goldRuntimeVersion",2).put("silverRuntimeId","installed-silver").put("silverRuntimeSha256",sha(silver))
                    .put("goldBindingId","content-GOLD").put("duckdb","1.4.5").put("seconds",30).put("python",stub(installed.resolve("gold-python")).toString());
            modules(gold,"gold");register("installed-gold",gold);
            var runtime=SourceJson.object().put("contentRuntimeVersion",2).put("goldRuntimeId","installed-gold").put("goldRuntimeSha256",sha(gold))
                    .put("referenceBindingId",REFS).put("contentBindingId","content-CONTENT").put("seconds",30);
            modules(runtime,"content");register(RUNTIME,runtime);
        } catch(Exception ex) {throw new IllegalStateException(ex);}
    }
    @Override void ready() {
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        String deployment=configurations.get(CONTENT_PROFILE).get("runtime").get("deploymentSha256").stringValue();
        session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),deployment);
    }
    private void register(String id,ObjectNode config) {
        configurations.put(id,config);byte[] bytes=SourceJson.canonical(config);
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,bytes,SourceJson.sha(bytes));
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",id,CONTENT);
    }
    private void modules(ObjectNode config,String family) throws Exception {
        var modules=config.putObject("modules");
        for(String name:NarrowContentInstalledRuntimeTestSupport.modules(family)) {
            Path module=processing.resolve(name);
            if(!Files.exists(module))Files.writeString(module,"raise RuntimeError('TEST STUB MUST NEVER BE IMPORTED')\n");
            modules.put(name,SourceJson.sha(Files.readAllBytes(module)));
        }
    }
    private static Path stub(Path path) throws Exception {
        Files.writeString(path,"#!/bin/sh\nexit 97\n");Files.setPosixFilePermissions(path,PosixFilePermissions.fromString("rwx------"));return path;
    }
    private static String sha(JsonNode value) {return SourceJson.sha(SourceJson.canonical(value));}
    private static ObjectNode common(String profile,String deployment) {return SourceJson.object().put("runProfileSha256",profile).put("deploymentSha256",deployment);}
    private NarrowContentInstalledRuntimeTestSupport.Facts installedObservation() {
        return NarrowContentInstalledRuntimeTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId());
    }
    private Map<String,List<String>> allMetadata() {
        var result=new TreeMap<String,List<String>>();
        for(String table:jdbc.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='ddip_management' ORDER BY tablename",String.class))
            result.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.\""+table+"\" t ORDER BY to_jsonb(t)::text",String.class));
        return result;
    }
    private void rejectsWithoutMutation() throws Exception {
        var db=allMetadata();var fs=filesystem();
        assertThatThrownBy(this::installedObservation).isInstanceOf(RuntimeException.class);
        assertThat(allMetadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }

    @Test void reconstructsReadyAndInspectsActualFilesWithoutGrantingLaunchOrMutatingAnything() throws Exception {
        claimed();var ready=prepare();var db=allMetadata();var fs=filesystem();
        var first=installedObservation();
        var freshJdbc=new org.springframework.jdbc.core.JdbcTemplate(jdbc.getDataSource());
        var second=NarrowContentInstalledRuntimeTestSupport.inspect(freshJdbc,transactions(freshJdbc),guard,owner.runId(),owner.attemptId());
        assertThat(first).isEqualTo(second);assertThat(first.run()).isEqualTo(ready.run());assertThat(first.attempt()).isEqualTo(ready.attempt());
        assertThat(first.installedArtifactsVerified()).isTrue();assertThat(first.nativeLaunchCompatible()).isFalse();
        assertThat(first.runtimeVersionsVerified()).isFalse();assertThat(first.strictSinkCompatible()).isFalse();
        assertThat(first.artifacts()).isGreaterThan(1);assertThat(first.roots()).isPositive();assertThat(first.executables()).isPositive();
        assertThat(allMetadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
        assertThat(temp.resolve("never-read-credentials")).doesNotExist();
    }
    @Test void missingReadyDoesNotPrepareOrRepair() throws Exception {claimed();rejectsWithoutMutation();assertThat(phases()).isEmpty();}
    @Test void preparingOnlyDoesNotPromoteToReady() throws Exception {claimed();stopAt("PREPARING_COMMITTED");rejectsWithoutMutation();assertThat(phases()).containsExactly("PREPARING");}
    @Test void closedHostGuardFailsWithoutMutation() throws Exception {claimed();prepare();guard.close();rejectsWithoutMutation();}
    @ParameterizedTest @ValueSource(strings={"installed-fetch","installed-bronze","installed-silver","installed-gold",RUNTIME})
    void registeredRuntimeDriftFailsWithoutMutation(String id) throws Exception {
        claimed();prepare();var altered=configurations.get(id).deepCopy();altered.put("unapprovedReplacement",true);byte[] bytes=SourceJson.canonical(altered);
        // Simulate out-of-band corruption; ordinary application writes remain guarded.
        jdbc.execute("ALTER TABLE ddip_management.execution_profiles DISABLE TRIGGER immutable_evidence");
        try {jdbc.update("UPDATE ddip_management.execution_profiles SET configuration_bytes=?,configuration_sha256=? WHERE profile_id=?",bytes,SourceJson.sha(bytes),id);}
        finally {jdbc.execute("ALTER TABLE ddip_management.execution_profiles ENABLE TRIGGER immutable_evidence");}
        rejectsWithoutMutation();
    }
    @ParameterizedTest @ValueSource(strings={"membership","module","lock","ledger","guard"})
    void driftBetweenObservationPassesFailsWithoutWritingEvidence(String kind) throws Exception {
        claimed();prepare();
        var afterDb=new java.util.concurrent.atomic.AtomicReference<Map<String,List<String>>>();
        var afterFs=new java.util.concurrent.atomic.AtomicReference<Map<String,String>>();
        assertThatThrownBy(()->NarrowContentInstalledRuntimeTestSupport.inspectWithBoundary(jdbc,tx,guard,
                owner.runId(),owner.attemptId(),boundary->{
                    assertThat(boundary).isEqualTo("INSTALLED_INSPECTED");
                    try {
                        switch(kind) {
                            case "membership" -> {
                                jdbc.execute("ALTER TABLE ddip_management.execution_profile_storage DISABLE TRIGGER immutable_evidence");
                                try {jdbc.update("DELETE FROM ddip_management.execution_profile_storage WHERE profile_id=? AND binding_id=?",RUNTIME,REFS);}
                                finally {jdbc.execute("ALTER TABLE ddip_management.execution_profile_storage ENABLE TRIGGER immutable_evidence");}
                            }
                            case "module" -> Files.writeString(processing.resolve("bounded_evidence.py"),"changed during inspection");
                            case "lock" -> {
                                Path lock=attemptRoot().resolve(NarrowContentAdmissionTestSupport.LOCK);
                                Files.move(lock,lock.resolveSibling("replaced-lock"));
                                Files.write(lock,new byte[0]);Files.setPosixFilePermissions(lock,PosixFilePermissions.fromString("rw-------"));
                            }
                            case "ledger" -> Files.writeString(attemptRoot().resolve(NarrowContentAdmissionTestSupport.MANIFEST),"{}");
                            case "guard" -> guard.close();
                            default -> throw new AssertionError(kind);
                        }
                        afterDb.set(allMetadata());afterFs.set(filesystem());
                    } catch(Exception ex) {throw new AssertionError(ex);}
                })).isInstanceOf(RuntimeException.class);
        assertThat(afterFs.get()).isNotNull();assertThat(filesystem()).isEqualTo(afterFs.get());
        assertThat(allMetadata()).isEqualTo(afterDb.get());noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"jar","module","root","executable"})
    void installedArtifactRootOrExecutableDriftFailsWithoutMutation(String kind) throws Exception {
        claimed();prepare();
        switch(kind) {
            case "jar" -> Files.writeString(jar,"changed artifact");
            case "module" -> Files.writeString(processing.resolve("bounded_evidence.py"),"changed module");
            case "root" -> {Path root=roots.get("content-GOLD");Files.move(root,root.resolveSibling(root.getFileName()+"-replaced"));Files.createDirectory(root);}
            case "executable" -> Files.setPosixFilePermissions(python,PosixFilePermissions.fromString("rw-------"));
            default -> throw new AssertionError(kind);
        }
        rejectsWithoutMutation();
    }
}
