package platform.contentapi.management;

import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFixtures.*;

/** Real ingestion JAR plus a synthetic loopback server. Other runtime families remain unexecuted. */
abstract class NarrowContentFetchLaunchFixtures extends NarrowContentAdmissionFixtures {
    static final String LAUNCH_PROFILE="strict-fetch-test";
    static final String TEST_ENDPOINT="60000000-0000-4000-8000-000000000001";
    final AtomicInteger requests=new AtomicInteger();
    final byte[] payload="<feed xmlns=\"http://www.w3.org/2005/Atom\"><title>test</title></feed>".getBytes(java.nio.charset.StandardCharsets.UTF_8);
    HttpServer server;ExecutorService serverThreads;String endpoint;UUID operation;
    volatile CountDownLatch responseGate;
    Path installedFetchJar;
    @BeforeEach @Override void setup(){
        requests.set(0);operation=UUID.randomUUID();responseGate=new CountDownLatch(0);
        try{
            server=HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"),0),0);
            server.createContext("/feed",exchange->{requests.incrementAndGet();try{
                if(!exchange.getRequestMethod().equals("GET"))throw new AssertionError("Unexpected HTTP method");
                responseGate.await(15,TimeUnit.SECONDS);
                byte[] body=responsePayload();exchange.getResponseHeaders().add("Content-Type","application/atom+xml");exchange.sendResponseHeaders(200,body.length);exchange.getResponseBody().write(body);
            }catch(java.io.IOException ignored){}catch(InterruptedException ex){Thread.currentThread().interrupt();}finally{exchange.close();}});
            serverThreads=Executors.newVirtualThreadPerTaskExecutor();server.setExecutor(serverThreads);server.start();endpoint="http://127.0.0.1:"+server.getAddress().getPort()+"/feed";
        }catch(Exception ex){throw new IllegalStateException(ex);}
        super.setup();
        try{
            var constructor=ReviewedSourceCatalog.class.getDeclaredConstructor(Map.class,Set.class,Map.class);constructor.setAccessible(true);
            var profiles=ReviewedSourceCatalog.class.getDeclaredField("profiles");var storage=ReviewedSourceCatalog.class.getDeclaredField("storage");profiles.setAccessible(true);storage.setAccessible(true);
            catalog=constructor.newInstance(Map.of(TEST_ENDPOINT,new ReviewedSourceCatalog.Endpoint(TEST_ENDPOINT,"demo-feed",endpoint)),profiles.get(catalog),storage.get(catalog));
            sources=new SourceRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY));runs=new RunRepository(jdbc,tx,INSTALL,catalog);
            sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("strict fetch test",rss().put("feedEndpointId",TEST_ENDPOINT)),"loopback","\"0\""),"test");
            var fetch=SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id='fetch'",byte[].class));
            var manifest=(ObjectNode)fetch.get("manifest").deepCopy();manifest.put("adapter","platform.ingestion.managed.StrictManagedRssFetch");
            var config=SourceJson.object().put("strictFetchLaunchVersion",1).put("fetchRuntimeId","fetch").put("fetchRuntimeSha256",digest("fetch"))
                    .put("runProfileId",CONTENT_PROFILE).put("runProfileSha256",digest(CONTENT_PROFILE)).put("controlBindingId",binding.id())
                    .put("javaHome",System.getProperty("java.home")).put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest))).put("testEndpoint",true);
            config.set("manifest",manifest);byte[] raw=SourceJson.canonical(config);
            jdbc.update("INSERT INTO ddip_management.content_fetch_launch_profiles(launch_profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",LAUNCH_PROFILE,raw,SourceJson.sha(raw));
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
    byte[] responsePayload(){return payload;}
    @Override void profile(String id,boolean content,String source){
        if(!content){super.profile(id,false,source);return;}
        try{
            var manifest=RssFetchTestSupport.manifest(true);
            var entry=(ObjectNode)manifest.get("classpath").get(0);
            installedFetchJar=Files.copy(Path.of(entry.get("path").stringValue()),Files.createTempDirectory(temp,"strict-fetch-runtime-").resolve("installed.jar")).toRealPath();
            entry.put("path",installedFetchJar.toString());
            NarrowContentFetchReservationTestSupport.registerChain(jdbc,id,RunFixtures.profile(true,source),CONTENT,manifest);
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        byte[] raw=SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:9000").put("region","local").put("bucket","bronze").put("credentialsFile","/never/read/credentials"));
        jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,raw,SourceJson.sha(raw),INSTALL);
    }
    @Override ObjectNode content(){var request=super.content();((ObjectNode)request.get("fetchSources").get(0)).put("revision",2);return request;}
    @Override void ready(){guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE));}
    void prepared(){claimed();prepare();}
    NarrowContentFetchLaunchTestSupport.Facts launch(java.util.function.Consumer<String> boundary){return NarrowContentFetchLaunchTestSupport.launch(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,boundary);}
    NarrowContentFetchLaunchTestSupport.Facts facts(){return NarrowContentFetchLaunchTestSupport.inspect(jdbc,tx,workers,operation);}
    Path fetchDirectory(){UUID checkpoint=jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_native_operations WHERE operation_id=?",UUID.class,operation);return attemptRoot().resolve("fetch-source/demo-feed/"+checkpoint);}
    void retained(){noExecution();org.assertj.core.api.Assertions.assertThat(count("content_native_operations")).isEqualTo(1);org.assertj.core.api.Assertions.assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());}
    @AfterEach void closeServer(){responseGate.countDown();if(server!=null)server.stop(0);if(serverThreads!=null)serverThreads.shutdownNow();}
}
