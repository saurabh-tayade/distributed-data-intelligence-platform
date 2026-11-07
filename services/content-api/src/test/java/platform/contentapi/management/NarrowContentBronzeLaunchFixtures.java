package platform.contentapi.management;

import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Real strict children, isolated PostgreSQL and deliberately unavailable loopback object storage. */
abstract class NarrowContentBronzeLaunchFixtures extends NarrowContentFetchLaunchFixtures {
    static final String BRONZE_PROFILE="strict-bronze-test";
    UUID bronzeOperation;Path credentials;HttpServer objectServer;ExecutorService objectThreads;
    final AtomicInteger objectRequests=new AtomicInteger();final AtomicInteger objectWrites=new AtomicInteger();
    final Map<String,byte[]> objects=new ConcurrentHashMap<>();final Map<String,String> objectTypes=new ConcurrentHashMap<>();
    volatile String objectError="AccessDenied";
    volatile CountDownLatch objectGate=new CountDownLatch(0);
    @BeforeEach @Override void setup(){
        bronzeOperation=UUID.randomUUID();objectRequests.set(0);objectWrites.set(0);objectGate=new CountDownLatch(0);objects.clear();objectTypes.clear();objectError="AccessDenied";
        try{
            objectServer=HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"),0),0);
            objectServer.createContext("/",e->{objectRequests.incrementAndGet();try{
                objectGate.await(20,TimeUnit.SECONDS);String method=e.getRequestMethod(),path=e.getRequestURI().getPath();
                if(method.equals("PUT")){
                    objectWrites.incrementAndGet();byte[] body=e.getRequestBody().readAllBytes();
                    if(objectError==null){objects.put(path,body);objectTypes.put(path,e.getRequestHeaders().getFirst("Content-Type"));e.sendResponseHeaders(200,-1);}
                    else objectError(e,objectError);
                }else if(method.equals("GET")||method.equals("HEAD")){
                    byte[] body=objects.get(path);
                    if(objectError!=null||body==null)objectError(e,objectError==null?"NoSuchKey":objectError);
                    else{e.getResponseHeaders().set("Content-Type",objectTypes.get(path));e.getResponseHeaders().set("Content-Length",Integer.toString(body.length));e.sendResponseHeaders(200,method.equals("HEAD")?-1:body.length);if(!method.equals("HEAD"))e.getResponseBody().write(body);}
                }else e.sendResponseHeaders(400,-1);
            }catch(java.io.IOException ignored){}catch(InterruptedException ex){Thread.currentThread().interrupt();}finally{e.close();}});
            objectThreads=Executors.newVirtualThreadPerTaskExecutor();objectServer.setExecutor(objectThreads);objectServer.start();
            super.setup();
            var bronze=SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id='bronze'",byte[].class));
            var fetch=SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id='fetch'",byte[].class));
            var manifest=(ObjectNode)fetch.get("manifest").deepCopy();manifest.put("adapter","platform.ingestion.managed.StrictManagedBronze");
            var strict=SourceJson.object().put("strictBronzeRuntimeVersion",1).put("adapter","platform.ingestion.managed.StrictManagedBronze").put("javaRuntime",Runtime.version().toString());
            var config=SourceJson.object().put("strictBronzeLaunchVersion",1).put("bronzeRuntimeId","bronze").put("bronzeRuntimeSha256",digest("bronze"))
                    .put("runProfileId",CONTENT_PROFILE).put("runProfileSha256",digest(CONTENT_PROFILE)).put("controlBindingId",binding.id())
                    .put("storageBindingId",bronze.get("storageBindingId").stringValue()).put("storageBindingSha256",bronze.get("storageBindingSha256").stringValue())
                    .put("javaHome",System.getProperty("java.home")).put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)))
                    .put("strictRuntimeSha256",SourceJson.sha(SourceJson.canonical(strict))).put("testEndpoint",true);
            config.set("manifest",manifest);byte[] raw=SourceJson.canonical(config);
            jdbc.update("INSERT INTO ddip_management.content_bronze_launch_profiles(launch_profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",BRONZE_PROFILE,raw,SourceJson.sha(raw));
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
    private static void objectError(com.sun.net.httpserver.HttpExchange exchange,String code)throws java.io.IOException{
        byte[] body=("<Error><Code>"+code+"</Code><Message>Synthetic</Message></Error>").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        exchange.getResponseHeaders().set("Content-Type","application/xml");exchange.sendResponseHeaders(code.equals("AccessDenied")?403:404,exchange.getRequestMethod().equals("HEAD")?-1:body.length);
        if(!exchange.getRequestMethod().equals("HEAD"))exchange.getResponseBody().write(body);
    }
    String objectPath(String kind)throws Exception{return "/bronze/"+SourceJson.parse(Files.readAllBytes(fetchDirectory().resolve("descriptor.json"))).get("expected").get(kind+"Key").stringValue();}
    void seedObject(String kind,byte[] bytes)throws Exception{String path=objectPath(kind);objects.put(path,bytes.clone());objectTypes.put(path,kind.equals("payload")?"application/octet-stream":"application/json");}
    void seedPair()throws Exception{seedObject("payload",payload);seedObject("metadata",Files.readAllBytes(fetchDirectory().resolve("metadata.json")));}
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        try{
            Path file=Files.createTempFile(temp,"bronze-credentials-",".json",PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            if(product.equals(CONTENT))credentials=file;
            Files.write(file,SourceJson.canonical(SourceJson.object().put("accessKey","fixture-access").put("secretKey","fixture-secret")));
            byte[] raw=SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:"+objectServer.getAddress().getPort()).put("region","local").put("bucket","bronze").put("credentialsFile",file.toString()));
            jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,raw,SourceJson.sha(raw),INSTALL);
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
    void checkpoint(){prepared();NarrowContentFetchCompletionTestSupport.launchAndCheckpoint(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,b->{});}
    NarrowContentBronzeLaunchTestSupport.Facts bronzeLaunch(Consumer<String> boundary){return NarrowContentBronzeLaunchTestSupport.launch(jdbc,tx,workers,guard,owner,operation,bronzeOperation,BRONZE_PROFILE,boundary);}
    NarrowContentBronzeLaunchTestSupport.Facts bronzeFacts(){return NarrowContentBronzeLaunchTestSupport.inspect(jdbc,tx,workers,bronzeOperation);}
    Path bronzeDirectory(){UUID id=jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_bronze_reservations WHERE operation_id=?",UUID.class,bronzeOperation);return attemptRoot().resolve("store-bronze/"+id);}
    void bronzeRetained(){bronzeRetained(0);}
    void bronzeRetained(int writes){retained();assertThat(count("content_bronze_reservations")).isEqualTo(1);assertThat(objectWrites).hasValue(writes);assertThat(Files.exists(bronzeDirectory().resolve("complete.json"))).isFalse();}
    void privateFile(Path path){try{Files.createFile(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));}catch(Exception ex){throw new IllegalStateException(ex);}}
    @AfterEach void closeObjectServer(){objectGate.countDown();if(objectServer!=null)objectServer.stop(0);if(objectThreads!=null)objectThreads.shutdownNow();}
}
