package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.mock.env.MockEnvironment;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Actual installed public composition and production catalog loader; no source fetch in setup. */
abstract class OrdinaryContentWorkerFixtures extends NarrowContentAdmissionFixtures {
    static final String FETCH_PROFILE="ordinary-public-fetch", BRONZE_PROFILE="ordinary-public-bronze";
    static final String VERIFY_PROFILE="ordinary-public-verifier", PIPELINE_PROFILE="ordinary-public-pipeline";
    Path processing,python,installedFetchJar,installedProvider,credentials;
    final List<WorkerIdleLifecycle> lifecycles=new ArrayList<>();
    final Map<String,ObjectNode> strictProfiles=new LinkedHashMap<>();

    boolean publicComposition(){return true;}
    String sourceUri(){return "https://public-fixture.example.com/feed.xml";}
    String bronzeEndpoint(){return "http://127.0.0.1:9000";}

    @Override void createSource(String product,String source,JsonNode configuration){
        try {
            // Both source admission and frozen run selection use the actual bounded file loader.
            var reviewed=SourceFixtures.catalog();
            for(var endpoint:reviewed.get("endpoints"))((ObjectNode)endpoint).put("uri",sourceUri());
            Files.write(catalogFile,SourceJson.canonical(reviewed));
            catalog=ReviewedSourceCatalog.load(catalogFile.toString());
            sources=new SourceRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY));
            runs=new RunRepository(jdbc,tx,INSTALL,catalog);
            super.createSource(product,source,configuration);
        } catch(java.io.IOException ex){throw new java.io.UncheckedIOException(ex);}
    }
    @Override void profile(String id,boolean content,String source){
        if(!content){super.profile(id,false,source);return;}
        try {
            Path sourceProcessing=Path.of("../../processing").toRealPath();
            processing=Files.createTempDirectory(temp,"ordinary-processing-").toRealPath();
            try(var files=Files.list(sourceProcessing)){
                for(var file:files.filter(p->p.getFileName().toString().endsWith(".py")).toList()){
                    String name=file.getFileName().toString();
                    if(!Set.of("reliability_execution.py","reliability_execution_preflight.py").contains(name))
                        Files.copy(file,processing.resolve(name));
                }
            }
            python=Path.of(Objects.requireNonNull(System.getenv("DDIP_TEST_PYTHON"),"DDIP_TEST_PYTHON")).toAbsolutePath();
            var manifest=RssFetchTestSupport.manifest(true);
            Path installation=Files.createTempDirectory(temp,"ordinary-runtime-");
            var entry=(ObjectNode)manifest.get("classpath").get(0);
            installedFetchJar=Files.copy(Path.of(entry.get("path").stringValue()),installation.resolve("ingestion.jar")).toRealPath();
            entry.put("path",installedFetchJar.toString());
            installedProvider=Files.copy(Path.of("../../ingestion/target/ingestion-0.1.0-SNAPSHOT-public-fetch-provider.jar"),installation.resolve("public-fetch-provider.jar")).toRealPath();
            NarrowContentPipelineTestSupport.registerChain(jdbc,id,RunFixtures.profile(true,source),CONTENT,manifest,processing,python,!publicComposition());
        } catch(Exception ex){throw new IllegalStateException(ex);}
    }
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        try {
            credentials=Files.createTempFile(temp,"ordinary-credentials-",".json",PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.write(credentials,SourceJson.canonical(SourceJson.object().put("accessKey","fixture-access").put("secretKey","fixture-secret")));
            var c=SourceJson.object().put("importBronzeVersion",1).put("endpoint",bronzeEndpoint()).put("region","local").put("bucket","bronze").put("credentialsFile",credentials.toString());
            byte[] raw=SourceJson.canonical(c);
            jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,raw,SourceJson.sha(raw),INSTALL);
        } catch(Exception ex){throw new IllegalStateException(ex);}
    }
    @Override void artifact(String id,ObjectNode evidence){
        if(id.equals(PUB))try {
            Path output=Files.createTempFile(temp,"ordinary-reference-",".json");
            var process=new ProcessBuilder(python.toString(),"-I","-B",Path.of("../../processing/tests/managed_content_fixture.py").toRealPath().toString(),"reference",roots.get(REFS).toString(),REFS)
                    .redirectError(ProcessBuilder.Redirect.INHERIT).redirectOutput(output.toFile()).start();
            try {
                assertThat(process.waitFor(30,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isZero();
                assertThat(Files.size(output)).isLessThanOrEqualTo(512*1024);evidence=(ObjectNode)SourceJson.parse(Files.readAllBytes(output),512*1024);
            } finally {if(process.isAlive()){process.destroyForcibly();process.waitFor(10,TimeUnit.SECONDS);}}
        } catch(Exception ex){throw new IllegalStateException(ex);}
        super.artifact(id,evidence);
    }
    JsonNode config(String id){return SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id=?",byte[].class,id));}
    @BeforeEach void registerComposition(TestInfo test)throws Exception {
        registerComposition(!test.getTestMethod().orElseThrow().getName().equals("missingCompositionCannotRegisterOrClaim"));
    }
    private void registerComposition(boolean includePipeline)throws Exception {
        strictProfiles.clear();var fetch=config("fetch");var bronze=config("bronze");
        ObjectNode launch,bronzeLaunch,verifier;
        if(publicComposition()){
            launch=PublicContentProfileTestSupport.fetchLaunch(fetch,"fetch",digest("fetch"),SourceJson.object().put("path",installedProvider.toString()).put("sha256",SourceJson.sha(Files.readAllBytes(installedProvider))));
            String launchHash=SourceJson.sha(SourceJson.canonical(launch));
            bronzeLaunch=PublicContentProfileTestSupport.bronze(bronze,"bronze",digest("bronze"),fetch,FETCH_PROFILE,launchHash,false);
            verifier=PublicContentProfileTestSupport.bronze(bronze,"bronze",digest("bronze"),fetch,FETCH_PROFILE,launchHash,true);
        }else{
            var manifest=((ObjectNode)fetch.get("manifest").deepCopy()).put("adapter","platform.ingestion.managed.StrictManagedRssFetch");
            launch=SourceJson.object().put("strictFetchLaunchVersion",1).put("fetchRuntimeId","fetch").put("fetchRuntimeSha256",digest("fetch"))
                    .put("runProfileId",CONTENT_PROFILE).put("runProfileSha256",digest(CONTENT_PROFILE)).put("controlBindingId",binding.id())
                    .put("javaHome",System.getProperty("java.home")).put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest))).put("testEndpoint",true);
            launch.set("manifest",manifest);
            bronzeLaunch=legacyBronze(bronze,fetch,false);verifier=legacyBronze(bronze,fetch,true);
        }
        insertStrict("content_fetch_launch_profiles","launch_profile_id",FETCH_PROFILE,launch);
        insertStrict("content_bronze_launch_profiles","launch_profile_id",BRONZE_PROFILE,bronzeLaunch);
        insertStrict("content_bronze_verification_profiles","verification_profile_id",VERIFY_PROFILE,verifier);
        var pipeline=SourceJson.object().put("strictContentPipelineVersion",publicComposition()?2:1)
                .put("runProfileId",CONTENT_PROFILE).put("runProfileSha256",digest(CONTENT_PROFILE)).put("contentRuntimeId","content").put("contentRuntimeSha256",digest("content"))
                .put("controlBindingId",binding.id()).put("strictRuntimeSha256",NarrowContentPipelineTestSupport.protocolDigest())
                .put("python",python.toString()).put("processingRoot",processing.toString()).put("javaHome",System.getProperty("java.home"))
                .put("fetchLaunchProfileId",FETCH_PROFILE).put("bronzeLaunchProfileId",BRONZE_PROFILE).put("bronzeVerificationProfileId",VERIFY_PROFILE);
        if(publicComposition())pipeline.put("fetchLaunchProfileSha256",sha(launch)).put("bronzeLaunchProfileSha256",sha(bronzeLaunch)).put("bronzeVerificationProfileSha256",sha(verifier));
        pipeline.set("versions",NarrowContentPipelineTestSupport.versions());var modules=pipeline.putObject("modules");
        var process=new ProcessBuilder(python.toString(),"-I","-B","-c","import sys,json;sys.path.insert(0,sys.argv[1]);from strict_native_content import MODULES;print(json.dumps(list(MODULES)))",processing.toString()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try {
            byte[] result=process.getInputStream().readNBytes(65537);assertThat(process.waitFor(30,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isZero();
            for(var name:SourceJson.parse(result)){String file=name.stringValue();modules.put(file,SourceJson.sha(Files.readAllBytes(processing.resolve(file))));}
        } finally {if(process.isAlive()){process.destroyForcibly();process.waitFor(10,TimeUnit.SECONDS);}}
        strictProfiles.put("pipeline",pipeline);
        if(includePipeline)
            insertStrict("content_pipeline_profiles","profile_id",PIPELINE_PROFILE,pipeline);
    }
    ObjectNode legacyBronze(JsonNode bronze,JsonNode fetch,boolean verifier){
        String main=verifier?"platform.ingestion.managed.StrictBronzeVerifier":"platform.ingestion.managed.StrictManagedBronze";
        var manifest=((ObjectNode)fetch.get("manifest").deepCopy()).put("adapter",main);
        var protocol=SourceJson.object().put(verifier?"strictBronzeVerifierRuntimeVersion":"strictBronzeRuntimeVersion",1).put("adapter",main).put("javaRuntime",Runtime.version().toString());
        var c=SourceJson.object().put(verifier?"strictBronzeVerificationVersion":"strictBronzeLaunchVersion",1).put("bronzeRuntimeId","bronze").put("bronzeRuntimeSha256",digest("bronze"))
                .put("javaHome",System.getProperty("java.home")).put("deploymentSha256",sha(manifest)).put("strictRuntimeSha256",sha(protocol)).put("testEndpoint",true);
        for(String key:List.of("runProfileId","runProfileSha256","controlBindingId","storageBindingId","storageBindingSha256"))c.set(key,bronze.get(key));
        c.set("manifest",manifest);return c;
    }
    static String sha(JsonNode value){return SourceJson.sha(SourceJson.canonical(value));}
    void insertStrict(String table,String key,String id,ObjectNode c){byte[] raw=SourceJson.canonical(c);jdbc.update("INSERT INTO ddip_management."+table+"("+key+",configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,raw,SourceJson.sha(raw));}
    MockEnvironment workerEnvironment(){
        return new MockEnvironment().withProperty("ddip.worker.execution-profile-id",CONTENT_PROFILE).withProperty("ddip.worker.control-binding-id",binding.id())
                .withProperty("ddip.worker.deployment-sha256",NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE))
                .withProperty("ddip.worker.runtime-profile-id",PIPELINE_PROFILE).withProperty("ddip.worker.poll-millis","250")
                .withProperty("ddip.worker.python",python.toString()).withProperty("ddip.worker.java-home",System.getProperty("java.home"))
                .withProperty("ddip.worker.processing-root",processing.toString());
    }
    WorkerIdleLifecycle lifecycle(JdbcTemplate data,MockEnvironment environment){
        var worker=new WorkerIdleLifecycle(data,transactions(data),new DatabaseSettings(url,user,password,INSTALL),environment);lifecycles.add(worker);return worker;
    }
    /** Actual packaged role with read-only startup only; no queued source or bootstrap. */
    Process packagedWorker(Path log)throws Exception {
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx256m","-jar",
                Path.of("target/content-api-0.1.0-SNAPSHOT.jar").toRealPath().toString(),
                "--spring.main.web-application-type=none","--spring.main.banner-mode=off",
                "--spring.main.log-startup-info=true","--logging.level.root=INFO",
                "--logging.level.platform.contentapi.execution.WorkerIdleLifecycle=DEBUG")
                .redirectErrorStream(true).redirectOutput(log.toFile());
        var values=builder.environment();values.put("DDIP_ROLE","worker");values.put("DDIP_DB_URL",url);
        values.put("DDIP_DB_USER",user);values.put("DDIP_DB_PASSWORD",password);values.put("DDIP_INSTALLATION_ID",INSTALL.toString());
        for(String key:List.of("execution-profile-id","control-binding-id","deployment-sha256","runtime-profile-id","poll-millis","python","java-home","processing-root"))
            values.put("DDIP_WORKER_"+key.toUpperCase(Locale.ROOT).replace('-','_'),workerEnvironment().getProperty("ddip.worker."+key));
        values.put("DDIP_WORKER_POLL_MILLIS","5000");
        return builder.start();
    }
    @AfterEach void stopLifecycles(){for(var worker:lifecycles)worker.stop();lifecycles.clear();}
    static void await(BooleanSupplier done,int seconds)throws InterruptedException {
        long limit=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);
        while(!done.getAsBoolean() && System.nanoTime()<limit)Thread.sleep(25);
        assertThat(done.getAsBoolean()).as("Bounded lifecycle condition").isTrue();
    }
    String state(UUID run){return jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run);}
    void noNativeWork(){
        for(String table:List.of("content_native_operations","content_bronze_reservations","content_pipeline_operations","stage_invocations","stage_artifacts","rss_deliveries","content_pipeline_completions"))assertThat(count(table)).as(table).isZero();
    }
}
