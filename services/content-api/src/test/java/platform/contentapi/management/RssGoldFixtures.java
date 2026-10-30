package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Shared real D1-D4 fixture, with no inherited test cases. */
abstract class RssGoldFixtures extends RssFetchFixtures {
    static final String SECOND="60000000-0000-4000-8000-000000000009";
    Path credentials;BronzeFaultProxy proxy;boolean multi,retainedImported;String bronzeRuntime="bronze-d4",silverRuntime="silver-d4",goldRuntime="gold-d4";
    RssGoldTestSupport.Prepared prepared;
    final RssGoldTimings timing=new RssGoldTimings();
    @Override byte[] feedPayload(){return "<rss version=\"2.0\"><channel><item><guid>one</guid><title>One</title></item><item><guid>one</guid><title>One again</title></item><item><title>Missing identifier</title></item></channel></rss>".getBytes(java.nio.charset.StandardCharsets.UTF_8);}
    @BeforeEach @Override void setup(){timing.reset();try(var measured=timing.phase("fixture-setup")){
        multi=false;retainedImported=false;prepared=null;goldRuntime="gold-d4";
        proxy=new BronzeFaultProxy(Objects.requireNonNull(System.getenv("BRONZE_S3_ENDPOINT")));credentials=Files.createTempFile(temp,"d4-credentials-",".json").toRealPath();Files.setPosixFilePermissions(credentials,PosixFilePermissions.fromString("rw-------"));Files.write(credentials,SourceJson.canonical(SourceJson.object().put("accessKey",System.getenv("BRONZE_S3_ACCESS_KEY")).put("secretKey",System.getenv("BRONZE_S3_SECRET_KEY"))));super.setup();
        var ctor=ReviewedSourceCatalog.class.getDeclaredConstructor(Map.class,Set.class,Map.class);ctor.setAccessible(true);var profiles=ReviewedSourceCatalog.class.getDeclaredField("profiles");var storage=ReviewedSourceCatalog.class.getDeclaredField("storage");profiles.setAccessible(true);storage.setAccessible(true);
        catalog=ctor.newInstance(Map.of(TEST_ENDPOINT,new ReviewedSourceCatalog.Endpoint(TEST_ENDPOINT,"demo-feed",endpoint),ENDPOINT2,new ReviewedSourceCatalog.Endpoint(ENDPOINT2,"demo-feed",endpoint+"-changed"),SECOND,new ReviewedSourceCatalog.Endpoint(SECOND,"second-feed",endpoint)),profiles.get(catalog),storage.get(catalog));sources=new SourceRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY));runs=new RunRepository(jdbc,tx,INSTALL,catalog);createSource(CONTENT,"second-feed",rss().put("feedEndpointId",SECOND));
    }catch(Exception ex){throw new IllegalStateException(ex);}}
    @AfterEach void reportTimings(TestInfo info){timing.emit(info.getTestMethod().orElseThrow().getName());}
    @AfterEach void closeProxy(){if(proxy!=null)proxy.close();}
    @Override void storage(String id,String product,String role){if(!role.equals("BRONZE")){super.storage(id,product,role);return;}byte[] raw=SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint",proxy.endpoint()).put("region",System.getenv("BRONZE_S3_REGION")).put("bucket",System.getenv("BRONZE_S3_BUCKET")).put("credentialsFile",credentials.toString()));jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,raw,SourceJson.sha(raw),INSTALL);}
    @Override tools.jackson.databind.node.ObjectNode content(){var n=super.content();if(!retainedImported)n.withArray("retainedSilverArtifactIds").removeAll();if(multi)n.withArray("fetchSources").addObject().put("sourceId","second-feed").put("revision",1);return n;}
    String digest(String id){return jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,id);}
    void register(String id,JsonNode n){byte[] raw=SourceJson.canonical(n);jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,raw,SourceJson.sha(raw));jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",id,CONTENT);}
    void silver()throws Exception {silver(multi?2:1);}
    void silver(int completed)throws Exception {try(var measured=timing.phase("d1-d3")){
        ready();register(bronzeRuntime,SourceJson.object().put("bronzeRuntimeVersion",1).put("adapter","platform.ingestion.managed.ManagedBronze").put("fetchRuntimeId",runtime).put("fetchRuntimeSha256",digest(runtime)).put("runProfileId",CONTENT_PROFILE).put("runProfileSha256",digest(CONTENT_PROFILE)).put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest))).put("controlBindingId",guard.binding().id()).put("storageBindingId",BRONZE).put("storageBindingSha256",jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.storage_bindings WHERE binding_id=?",String.class,BRONZE)));
        Path processing=Path.of("../../processing").toRealPath();var s=SourceJson.object().put("silverRuntimeVersion",1).put("bronzeRuntimeId",bronzeRuntime).put("bronzeRuntimeSha256",digest(bronzeRuntime)).put("runProfileSha256",digest(CONTENT_PROFILE)).put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest))).put("python",System.getenv("DDIP_TEST_PYTHON")).put("javaHome",Path.of(System.getProperty("java.home")).toRealPath().toString()).put("processingRoot",processing.toString()).put("outputBindingId",SILVER_BINDING).put("tempBindingId","content-TEMP").put("seconds",300);s.set("modules",RssSilverTestSupport.modules(processing));s.set("versions",SourceJson.object().put("python","3.12.15").put("pyspark","4.0.2").put("boto3","1.43.114").put("java","21"));register(silverRuntime,s);
        var g=SourceJson.object().put("goldRuntimeVersion",1).put("silverRuntimeId",silverRuntime).put("silverRuntimeSha256",digest(silverRuntime)).put("goldBindingId","content-GOLD").put("duckdb","1.4.5").put("python",System.getenv("DDIP_TEST_PYTHON")).put("seconds",300);g.set("modules",RssGoldTestSupport.modules(processing));register(goldRuntime,g);
        owner=RssGoldTestSupport.silver(jdbc,tx,workers,guard,owner,runtime,bronzeRuntime,silverRuntime,multi?2:1,completed);
    }}
    RssGoldTestSupport.Prepared prepare(){return prepare((p,b,o)->{});}
    RssGoldTestSupport.Prepared prepare(RssGoldTestSupport.Fault f){try(var measured=timing.phase("prepare")){prepared=RssGoldTestSupport.prepare(jdbc,tx,workers,guard,owner,goldRuntime,(p,b,o)->{timing.boundary(p,b);f.at(p,b,o);});owner=prepared.owner();return prepared;}}
    RssGoldTestSupport.Published publish(){return publish((p,b,o)->{});}
    RssGoldTestSupport.Published publish(RssGoldTestSupport.Fault f){try(var measured=timing.phase("publish")){var r=RssGoldTestSupport.publish(jdbc,tx,workers,guard,prepared,goldRuntime,(p,b,o)->{timing.boundary(p,b);f.at(p,b,o);});owner=r.owner();return r;}}
    UUID inspectGold(){try(var measured=timing.phase("inspect")){return RssGoldTestSupport.inspect(jdbc,tx,workers,guard,owner.runId(),goldRuntime);}}
    Path gold(){return roots.get("content-GOLD").resolve("v1/processor_revision=1/input_set="+SourceJson.parse(prepared.bytes(),16*1024*1024).get("prepared").get("inputSetFingerprint").stringValue());}
    Map<String,String> snapshot(Path root)throws Exception {var r=new TreeMap<String,String>();try(var stream=Files.walk(root)){for(Path p:stream.filter(Files::isRegularFile).toList())r.put(root.relativize(p).toString(),SourceJson.sha(Files.readAllBytes(p))+":"+Files.getLastModifiedTime(p)+":"+Files.getAttribute(p,"unix:ino"));}return r;}
    void occupied(){assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE stage IN ('PUBLISH_CONTENT','VERIFY_RESULT')",Integer.class)).isZero();assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='GOLD'",Integer.class)).isZero();}
}
