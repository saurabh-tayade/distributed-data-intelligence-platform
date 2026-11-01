package platform.contentapi.management;

import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Shared synthetic HTTP, immutable D1 profile and private-root fixtures; no test methods. */
abstract class RssFetchFixtures extends RunApiFixtures {
    static final String TEST_ENDPOINT="60000000-0000-4000-8000-000000000001";
    final Map<String,InstallationHostGuard.Binding> controls=new HashMap<>();
    final Map<String,Path> roots=new HashMap<>();
    final AtomicInteger requests=new AtomicInteger();
    final AtomicInteger nonGet=new AtomicInteger();
    final byte[] payload={'<','f','e','e','d','>',0,(byte)0xff,'\r','\n'};
    byte[] feedPayload(){return payload;}
    HttpServer http;java.util.concurrent.ExecutorService httpExecutor;String endpoint,behavior;InstallationHostGuard guard;WorkerRepository workers;
    WorkerRepository.Session session;WorkerRepository.Owner owner;JsonNode manifest;
    String runtime="rss-fetch-test";int timeout=1000;boolean boundedEvidence;
    final AtomicBoolean loseAck=new AtomicBoolean();
    @Override void storage(String id,String product,String role){
        try {
            Path base=Files.createTempDirectory(temp,"d1-").toRealPath();Files.setPosixFilePermissions(base,PosixFilePermissions.fromString("rwx------"));JsonNode descriptor;
            if(role.equals("CONTROL")){var b=OwnershipFixtures.host(base);var named=new InstallationHostGuard.Binding(id,INSTALL,b.root(),b.rootIdentity(),b.guardIdentity(),b.installationIdentity());controls.put(id,named);descriptor=SourceJson.parse(named.descriptor());roots.put(id,named.root());}
            else {var identity=InstallationHostGuard.identity(base);descriptor=SourceJson.object().put("importStorageVersion",1).put("root",base.toString()).put("rootDevice",Long.toString(identity.device())).put("rootInode",Long.toString(identity.inode()));roots.put(id,base);Files.writeString(base.resolve("untouched.native"),"retained sentinel");}
            byte[] raw=SourceJson.canonical(descriptor);jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,raw,SourceJson.sha(raw),INSTALL);
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
    @Override void profile(String id,boolean content,String source){
        try {if(manifest==null)manifest=RssFetchTestSupport.manifest(boundedEvidence);}catch(Exception ex){throw new IllegalStateException(ex);}
        var p=RunFixtures.profile(content,source);if(content && boundedEvidence)p.put("runProfileVersion",2).put("contentRuntimeId","content-d5a");if(content)((tools.jackson.databind.node.ObjectNode)p.get("runtime")).put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)));
        byte[] raw=SourceJson.canonical(p);jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,raw,SourceJson.sha(raw));jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",id,content?CONTENT:RELIABILITY);
    }
    @BeforeEach @Override void setup(){
        manifest=null;requests.set(0);nonGet.set(0);behavior="normal";timeout=1000;loseAck.set(false);runtime="rss-fetch-test";
        // Every fault proxy belongs to one case; never re-arm a previous case's commit matcher.
        jdbc=new JdbcTemplate(new org.springframework.jdbc.datasource.DriverManagerDataSource(url,user,password));tx=transactions(jdbc);
        try {
            http=HttpServer.create(new InetSocketAddress(InetAddress.getByName("127.0.0.1"),0),0);
            http.createContext("/feed",exchange->{requests.incrementAndGet();if(!exchange.getRequestMethod().equals("GET"))nonGet.incrementAndGet();try{
                if(behavior.equals("timeout")){try{Thread.sleep(3000);}catch(InterruptedException ex){Thread.currentThread().interrupt();}}
                if(behavior.equals("redirect")){exchange.getResponseHeaders().add("Location","/feed");exchange.sendResponseHeaders(302,-1);return;}
                exchange.getResponseHeaders().add("Content-Type","application/atom+xml; charset=UTF-8");
                if(behavior.equals("malformed"))exchange.getResponseHeaders().add("Transfer-Encoding","chunked");
                if(behavior.equals("oversize")){exchange.sendResponseHeaders(200,0);byte[] chunk=new byte[8192];for(int i=0;i<641;i++)exchange.getResponseBody().write(chunk);}
                else {byte[] bytes=feedPayload();exchange.sendResponseHeaders(behavior.equals("http-error")?503:200,behavior.equals("chunked")?0:bytes.length);exchange.getResponseBody().write(bytes);}
            }catch(java.io.IOException ignored){ }finally{exchange.close();}});
            httpExecutor=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor();http.setExecutor(httpExecutor);http.start();endpoint="http://127.0.0.1:"+http.getAddress().getPort()+"/feed";
        }catch(Exception ex){throw new IllegalStateException(ex);}
        super.setup();
        // The production reviewed catalog never admits loopback. Only this test class constructs it.
        try {
            var constructor=ReviewedSourceCatalog.class.getDeclaredConstructor(Map.class,Set.class,Map.class);constructor.setAccessible(true);
            Field profiles=ReviewedSourceCatalog.class.getDeclaredField("profiles"),storage=ReviewedSourceCatalog.class.getDeclaredField("storage");profiles.setAccessible(true);storage.setAccessible(true);
            catalog=constructor.newInstance(Map.of(TEST_ENDPOINT,new ReviewedSourceCatalog.Endpoint(TEST_ENDPOINT,"demo-feed",endpoint),ENDPOINT2,new ReviewedSourceCatalog.Endpoint(ENDPOINT2,"demo-feed",endpoint+"-changed")),profiles.get(catalog),storage.get(catalog));
            sources=new SourceRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY));runs=new RunRepository(jdbc,tx,INSTALL,catalog);
            sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("D1 test",rss().put("feedEndpointId",TEST_ENDPOINT)),"loopback","\"0\""),"test");
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
    void ready(){
        var request=content();((tools.jackson.databind.node.ObjectNode)request.get("fetchSources").get(0)).put("revision",2);submit(request,"d1");
        var binding=controls.get("content-CONTROL");guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        String digest=jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,CONTENT_PROFILE);
        String deployment=SourceJson.sha(SourceJson.canonical(manifest));session=workers.register(guard,CONTENT_PROFILE,digest,deployment);owner=workers.claim(guard,session,UUID.randomUUID()).owner();
        var c=SourceJson.object().put("rssFetchRuntimeVersion",boundedEvidence?2:1).put("pipeline","CONTENT_RSS_V1").put("runProfileId",CONTENT_PROFILE).put("runProfileSha256",digest).put("deploymentSha256",deployment).put("javaHome",Path.of(System.getProperty("java.home")).toString()).put("controlBindingId",binding.id()).put("testEndpoint",true).put("httpTimeoutMillis",timeout);c.set("manifest",manifest);
        byte[] raw=SourceJson.canonical(c);jdbc.update("INSERT INTO ddip_management.execution_profiles VALUES (?,?,?,clock_timestamp())",runtime,raw,SourceJson.sha(raw));jdbc.update("INSERT INTO ddip_management.execution_profile_storage VALUES (?,?)",runtime,binding.id());
    }
    RssFetchTestSupport.Observed fetch(){return fetch((p,s)->{});}
    RssFetchTestSupport.Observed fetch(java.util.function.BiConsumer<String,WorkerRepository.FetchStage> fault){return RssFetchTestSupport.fetch(jdbc,tx,workers,guard,owner,runtime,fault);}
    Path directory(WorkerRepository.FetchStage stage){return guard.binding().root().resolve(stage.namespace());}
    boolean inspect(RssFetchTestSupport.Observed observed){return RssFetchTestSupport.inspect(jdbc,tx,workers,guard,observed);}
    boolean committed(){return RssFetchTestSupport.committed(jdbc,tx,workers,guard,owner.runId(),"demo-feed");}
    Map<Path,String> nativeSnapshot()throws Exception {var result=new HashMap<Path,String>();for(var entry:roots.entrySet())if(!entry.getKey().endsWith("CONTROL"))try(var paths=Files.walk(entry.getValue())){for(Path p:paths.toList())result.put(p,(Files.isDirectory(p)?"directory":SourceJson.sha(Files.readAllBytes(p)))+":"+Files.getLastModifiedTime(p)+":"+Files.getAttribute(p,"unix:ino"));}return result;}
    @AfterEach void release(){if(guard!=null){guard.close();guard=null;}if(http!=null){http.stop(0);http=null;}if(httpExecutor!=null)httpExecutor.shutdownNow();assertThat(nonGet.get()).isZero();}
    void installLostCommit(String statement){
        var real=new org.springframework.jdbc.datasource.DriverManagerDataSource(url,user,password);var wrapped=new AbstractDataSource(){
            public Connection getConnection()throws SQLException {Connection c=real.getConnection();var matched=new AtomicBoolean();return(Connection)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(p,m,args)->{try{if(m.getName().equals("prepareStatement") && args[0] instanceof String sql && sql.contains(statement))matched.set(true);Object result=m.invoke(c,args);if(m.getName().equals("commit") && matched.get() && loseAck.compareAndSet(true,false))throw new SQLException("PRIVATE","08006");return result;}catch(InvocationTargetException ex){throw ex.getCause();}});}
            public Connection getConnection(String u,String p)throws SQLException{return getConnection();}};
        jdbc=new JdbcTemplate(wrapped);tx=transactions(jdbc);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
    }
    void outage(boolean down){admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS "+!down);if(down)admin.queryForList("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=? AND pid<>pg_backend_pid()",database);}
}
