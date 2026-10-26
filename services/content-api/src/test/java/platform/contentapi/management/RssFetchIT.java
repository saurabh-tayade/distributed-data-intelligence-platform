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

/** Real PostgreSQL plus synthetic loopback HTTP and the actual prebuilt ingestion child. No Spark/S3. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RssFetchIT extends RunApiFixtures {
    static final String TEST_ENDPOINT="60000000-0000-4000-8000-000000000001";
    final Map<String,InstallationHostGuard.Binding> controls=new HashMap<>();
    final Map<String,Path> roots=new HashMap<>();
    final AtomicInteger requests=new AtomicInteger();
    final AtomicInteger nonGet=new AtomicInteger();
    final byte[] payload={'<','f','e','e','d','>',0,(byte)0xff,'\r','\n'};
    HttpServer http;java.util.concurrent.ExecutorService httpExecutor;String endpoint,behavior;InstallationHostGuard guard;WorkerRepository workers;
    WorkerRepository.Session session;WorkerRepository.Owner owner;JsonNode manifest;
    String runtime="rss-fetch-test";int timeout=1000;
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
        try {if(manifest==null)manifest=RssFetchTestSupport.manifest();}catch(Exception ex){throw new IllegalStateException(ex);}
        var p=RunFixtures.profile(content,source);if(content)((tools.jackson.databind.node.ObjectNode)p.get("runtime")).put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)));
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
                else {exchange.sendResponseHeaders(behavior.equals("http-error")?503:200,behavior.equals("chunked")?0:payload.length);exchange.getResponseBody().write(payload);}
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
        var c=SourceJson.object().put("rssFetchRuntimeVersion",1).put("pipeline","CONTENT_RSS_V1").put("runProfileId",CONTENT_PROFILE).put("runProfileSha256",digest).put("deploymentSha256",deployment).put("javaHome",Path.of(System.getProperty("java.home")).toString()).put("controlBindingId",binding.id()).put("testEndpoint",true).put("httpTimeoutMillis",timeout);c.set("manifest",manifest);
        byte[] raw=SourceJson.canonical(c);jdbc.update("INSERT INTO ddip_management.execution_profiles VALUES (?,?,?,clock_timestamp())",runtime,raw,SourceJson.sha(raw));jdbc.update("INSERT INTO ddip_management.execution_profile_storage VALUES (?,?)",runtime,binding.id());
    }
    RssFetchTestSupport.Observed fetch(){return fetch((p,s)->{});}
    RssFetchTestSupport.Observed fetch(java.util.function.BiConsumer<String,WorkerRepository.FetchStage> fault){return RssFetchTestSupport.fetch(jdbc,tx,workers,guard,owner,runtime,fault);}
    Path directory(WorkerRepository.FetchStage stage){return guard.binding().root().resolve(stage.namespace());}
    boolean inspect(RssFetchTestSupport.Observed observed){return RssFetchTestSupport.inspect(jdbc,tx,workers,guard,observed);}
    boolean committed(){return RssFetchTestSupport.committed(jdbc,tx,workers,guard,owner.runId(),"demo-feed");}
    Map<Path,String> nativeSnapshot()throws Exception {var result=new HashMap<Path,String>();for(var entry:roots.entrySet())if(!entry.getKey().endsWith("CONTROL"))try(var paths=Files.walk(entry.getValue())){for(Path p:paths.toList())result.put(p,(Files.isDirectory(p)?"directory":SourceJson.sha(Files.readAllBytes(p)))+":"+Files.getLastModifiedTime(p)+":"+Files.getAttribute(p,"unix:ino"));}return result;}
    @AfterEach void release(){if(guard!=null){guard.close();guard=null;}if(http!=null){http.stop(0);http=null;}if(httpExecutor!=null)httpExecutor.shutdownNow();assertThat(nonGet.get()).isZero();}
    @Test void exactPayloadMetadataProvenanceAndImmutableReadOnlyReplay()throws Exception {
        ready();var before=nativeSnapshot();var observed=fetch();var stage=observed.stage();Path directory=directory(stage);
        assertThat(requests.get()).isEqualTo(1);assertThat(Files.readAllBytes(directory.resolve("payload.xml"))).isEqualTo(payload);
        var d=SourceJson.parse(observed.descriptor());assertThat(d.get("revision").intValue()).isEqualTo(2);assertThat(d.get("sourceId").stringValue()).isEqualTo("demo-feed");assertThat(d.get("payloadSha256").stringValue()).isEqualTo(SourceJson.sha(payload));assertThat(d.get("contentType").stringValue()).isEqualTo("application/atom+xml; charset=UTF-8");
        var row=jdbc.queryForMap("SELECT * FROM ddip_management.rss_deliveries");assertThat(row.get("journal_descriptor")).isEqualTo(observed.descriptor());assertThat(row.get("original_ingested_at_text")).isEqualTo(d.get("ingestedAt").stringValue());assertThat(row.get("ingestion_id")).isEqualTo(UUID.fromString(d.get("ingestionId").stringValue()));assertThat(row.get("state")).isEqualTo("FETCH_JOURNALED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations",String.class)).isEqualTo("VERIFIED");assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs",String.class)).isEqualTo("RUNNING");assertThat(count("stage_artifacts")).isZero();
        var stamp=Files.getLastModifiedTime(directory.resolve("payload.xml"));assertThat(inspect(observed)).isTrue();assertThat(inspect(observed)).isTrue();assertThat(Files.getLastModifiedTime(directory.resolve("payload.xml"))).isEqualTo(stamp);assertThat(nativeSnapshot()).isEqualTo(before);assertThat(requests.get()).isEqualTo(1);
        StringBuilder audit=new StringBuilder();for(var event:jdbc.queryForList("SELECT details_bytes FROM ddip_management.audit_events"))audit.append(new String((byte[])event.get("details_bytes"),java.nio.charset.StandardCharsets.UTF_8));assertThat(audit.toString()).doesNotContain(endpoint,"application/atom+xml","payload.xml");
        var history=new RunHistoryRepository(jdbc,tx,new SourceCursor(KEY));assertThat(history.events(owner.runId().toString(),100,null).data().toString()).contains("VERIFIED").doesNotContain(endpoint,"payload.xml");
        assertThatThrownBy(this::fetch).hasMessage("OWNERSHIP_LOST");assertThat(requests.get()).isEqualTo(1);
    }
    @ParameterizedTest @ValueSource(strings={"chunked","http-error","oversize","redirect","timeout","malformed"}) void boundedHttpFailuresNeverRefetch(String mode){
        behavior=mode;if(mode.equals("timeout"))timeout=100;ready();
        if(mode.equals("chunked")){var observed=fetch();assertThat(SourceJson.parse(observed.descriptor()).get("declaredContentLength").isNull()).isTrue();assertThat(inspect(observed)).isTrue();}
        else {String expected=mode.equals("oversize")?"FETCH_PAYLOAD_TOO_LARGE":Set.of("redirect","http-error").contains(mode)?"FETCH_HTTP":"FETCH_TRANSPORT";assertThatThrownBy(this::fetch).hasMessage(expected);assertThat(count("rss_deliveries")).isZero();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs",String.class)).isEqualTo("RECOVERY_REQUIRED");assertThatThrownBy(this::fetch).isInstanceOf(RuntimeException.class);}
        assertThat(requests.get()).isEqualTo(1);
    }
    @ParameterizedTest @ValueSource(strings={"INTENT","BEFORE_AUTHORIZATION","AFTER_CHILD","BEFORE_COMPLETION","COMPLETED","BEFORE_CHECKPOINT"}) void crashesAtDurableBoundariesNeverAuthorizeAnotherFetch(String boundary)throws Exception {
        ready();AtomicReference<WorkerRepository.FetchStage> stage=new AtomicReference<>();
        assertThatThrownBy(()->fetch((point,s)->{stage.set(s);if(point.equals(boundary))throw new AssertionError("simulated crash");})).isInstanceOf(AssertionError.class);
        int expected=Set.of("INTENT","BEFORE_AUTHORIZATION").contains(boundary)?0:1;assertThat(requests.get()).isEqualTo(expected);assertThat(count("rss_deliveries")).isZero();
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);owner=stage.get().owner();assertThatThrownBy(this::fetch).hasMessage("OWNERSHIP_LOST");assertThat(requests.get()).isEqualTo(expected);
        if(Set.of("COMPLETED","BEFORE_CHECKPOINT").contains(boundary)){var observed=RssFetchTestSupport.observe(guard,stage.get());assertThat(inspect(observed)).isFalse();assertThat(committed()).isFalse();assertThat(Files.exists(directory(stage.get()).resolve("complete.json"))).isTrue();}
    }
    @Test void workerInterruptedBeforeRetrievalRetainsIntentAndNoNetwork(){ready();try{assertThatThrownBy(()->fetch((point,s)->{if(point.equals("BEFORE_AUTHORIZATION"))Thread.currentThread().interrupt();})).isInstanceOf(RuntimeException.class);}finally{Thread.interrupted();}assertThat(requests.get()).isZero();assertThat(count("rss_deliveries")).isZero();assertThat(count("stage_invocations")).isEqualTo(1);}
    @ParameterizedTest @ValueSource(strings={"BEFORE_AUTHORIZATION","BEFORE_COMPLETION","BEFORE_CHECKPOINT"}) void staleCoordinatorTokenCannotLaunchPublishOrCheckpoint(String boundary){ready();assertThatThrownBy(()->fetch((point,s)->{if(point.equals(boundary))jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");})).hasMessage("OWNERSHIP_LOST");assertThat(count("rss_deliveries")).isZero();assertThat(requests.get()).isEqualTo(boundary.equals("BEFORE_AUTHORIZATION")?0:1);if(boundary.equals("BEFORE_COMPLETION"))assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations",String.class)).isEqualTo("RUNNING");}
    @Test void lostHostGuardCannotPublish(){ready();AtomicReference<Path> path=new AtomicReference<>();assertThatThrownBy(()->fetch((point,s)->{if(point.equals("BEFORE_COMPLETION")){path.set(directory(s));guard.close();}})).isInstanceOf(RuntimeException.class);assertThat(Files.exists(path.get().resolve("complete.json"))).isFalse();assertThat(count("rss_deliveries")).isZero();assertThat(requests.get()).isEqualTo(1);}
    @Test void competingInvocationsHaveOneFetchAndOneIntent()throws Exception {ready();var barrier=new java.util.concurrent.CyclicBarrier(2);try(var pool=java.util.concurrent.Executors.newFixedThreadPool(2)){var results=new ArrayList<java.util.concurrent.Future<String>>();for(int i=0;i<2;i++)results.add(pool.submit(()->{barrier.await(5,java.util.concurrent.TimeUnit.SECONDS);var repo=WorkerTestSupport.enabled(jdbc,tx,INSTALL);try{RssFetchTestSupport.fetch(jdbc,tx,repo,guard,owner,runtime,(p,s)->{});return "FETCH_JOURNALED";}catch(WorkerFailure ex){return ex.code().name();}}));assertThat(List.of(results.get(0).get(15,java.util.concurrent.TimeUnit.SECONDS),results.get(1).get(15,java.util.concurrent.TimeUnit.SECONDS))).containsExactlyInAnyOrder("FETCH_JOURNALED","OWNERSHIP_LOST");}assertThat(requests.get()).isEqualTo(1);assertThat(count("stage_invocations")).isEqualTo(1);assertThat(count("rss_deliveries")).isEqualTo(1);}
    @Test void staleEpochCannotCreateIntent(){ready();var w=owner.worker();var stale=new WorkerRepository.Session(w.installationId(),w.instanceId(),w.profileId(),w.profileDigest(),w.deploymentDigest(),w.hostDigest(),w.status(),w.epoch()-1,w.coordinatorVersion(),w.workerVersion());owner=new WorkerRepository.Owner(stale,owner.runId(),owner.attemptId(),owner.runVersion(),owner.attemptVersion(),owner.attemptState());assertThatThrownBy(this::fetch).hasMessage("OWNERSHIP_LOST");assertThat(requests.get()).isZero();assertThat(count("stage_invocations")).isZero();}
    @ParameterizedTest @ValueSource(strings={"runProfileSha256","controlBindingId","deploymentSha256","testEndpoint"}) void changedRuntimeBindingNeverAuthorizesFetch(String field){ready();var c=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id=?",byte[].class,runtime));if(field.equals("testEndpoint"))c.put(field,false);else c.put(field,"0".repeat(64));byte[] raw=SourceJson.canonical(c);runtime="changed-runtime";jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",runtime,raw,SourceJson.sha(raw));jdbc.update("INSERT INTO ddip_management.execution_profile_storage VALUES (?,?)",runtime,guard.binding().id());assertThatThrownBy(this::fetch).hasMessage("RUNTIME_MISMATCH");assertThat(requests.get()).isZero();assertThat(count("stage_invocations")).isZero();}
    @ParameterizedTest @ValueSource(strings={"delete","change","unknown","permissions","symlink","replace","replace-completion"}) void journalMutationNeverVerifiesOrRepairs(String mutation)throws Exception {
        ready();var observed=fetch();Path directory=directory(observed.stage()),p=directory.resolve("payload.xml");
        switch(mutation){case "delete"->Files.delete(p);case "change"->Files.write(p,new byte[]{9});case "unknown"->Files.writeString(directory.resolve("unexpected"),"x");case "permissions"->Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rw-r--r--"));case "symlink"->{Files.delete(p);Files.createSymbolicLink(p,directory.resolve("metadata.json"));}case "replace-completion"->{Path marker=directory.resolve("complete.json");byte[] raw=Files.readAllBytes(marker);Files.delete(marker);Files.write(marker,raw);Files.setPosixFilePermissions(marker,PosixFilePermissions.fromString("rw-------"));}case "replace"->{byte[] raw=Files.readAllBytes(p);Files.delete(p);Files.write(p,raw);Files.setPosixFilePermissions(p,PosixFilePermissions.fromString("rw-------"));}}
        assertThatThrownBy(()->inspect(observed)).isInstanceOf(RuntimeException.class);assertThatThrownBy(this::committed).isInstanceOf(RuntimeException.class);assertThat(requests.get()).isEqualTo(1);assertThat(count("rss_deliveries")).isEqualTo(1);assertThatThrownBy(this::fetch).isInstanceOf(RuntimeException.class);assertThat(requests.get()).isEqualTo(1);
    }
    @ParameterizedTest @ValueSource(strings={"unexpected","permissions","io"}) void failedJournalCreationCannotBecomeVerified(String mode){ready();assertThatThrownBy(()->fetch((point,s)->{if(point.equals("BEFORE_COMPLETION"))try{Path d=directory(s);if(mode.equals("unexpected"))Files.writeString(d.resolve("extra"),"x");else if(mode.equals("permissions"))Files.setPosixFilePermissions(d.resolve("payload.xml"),PosixFilePermissions.fromString("rw-r--r--"));else Files.createDirectory(d.resolve("complete.json"));}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);assertThat(count("rss_deliveries")).isZero();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations",String.class)).isNotEqualTo("VERIFIED");assertThat(requests.get()).isEqualTo(1);}
    @Test void newEndpointRevisionCannotReplaceFrozenEndpoint(){ready();var changed=rss().put("feedEndpointId",ENDPOINT2);sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("new endpoint",changed),"endpoint","\"1\""),"test");var observed=fetch();assertThat(SourceJson.parse(observed.descriptor()).get("revision").intValue()).isEqualTo(2);assertThat(requests.get()).isEqualTo(1);}
    @Test void dbOutageBeforeAuthorizationLeavesNoNetworkAndNoRetry(){ready();assertThatThrownBy(()->fetch((point,s)->{if(point.equals("BEFORE_AUTHORIZATION"))outage(true);})).hasMessage("MANAGEMENT_UNAVAILABLE");outage(false);assertThat(count("rss_deliveries")).isZero();assertThat(requests.get()).isZero();assertThatThrownBy(this::fetch).hasMessage("OWNERSHIP_LOST");}
    @ParameterizedTest @ValueSource(strings={"BEFORE_COMPLETION","BEFORE_CHECKPOINT"}) void databaseLossAfterFetchNeverCheckpointsOrRefetches(String boundary){ready();assertThatThrownBy(()->fetch((point,s)->{if(point.equals(boundary))outage(true);})).hasMessage("MANAGEMENT_UNAVAILABLE");outage(false);assertThat(count("rss_deliveries")).isZero();assertThat(requests.get()).isEqualTo(1);assertThatThrownBy(this::fetch).hasMessage("OWNERSHIP_LOST");}
    @ParameterizedTest @ValueSource(strings={"payload.xml","descriptor.json","complete.json"}) void changedJournalImmediatelyBeforeCheckpointCannotVerify(String name){ready();assertThatThrownBy(()->fetch((point,s)->{if(point.equals("BEFORE_CHECKPOINT"))try{Files.write(directory(s).resolve(name),new byte[]{1});}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);assertThat(count("rss_deliveries")).isZero();assertThat(requests.get()).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations",String.class)).isNotEqualTo("VERIFIED");}
    @ParameterizedTest @ValueSource(strings={"journalVersion","httpStatus"}) void overflowingDescriptorCannotQualify(String field){ready();assertThatThrownBy(()->fetch((point,s)->{if(point.equals("BEFORE_COMPLETION"))try{Path p=directory(s).resolve("descriptor.json");var d=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(Files.readAllBytes(p));d.put(field,4294967296L+(field.equals("journalVersion")?1:200));Files.write(p,SourceJson.canonical(d));}catch(Exception ex){throw new IllegalStateException(ex);}})).hasMessage("INPUT_INVALID");assertThat(count("rss_deliveries")).isZero();assertThat(requests.get()).isEqualTo(1);}
    @ParameterizedTest @ValueSource(strings={"rss_deliveries","stage_invocations","audit_events"}) void checkpointAndAuditRollbackTogether(String table){ready();assertThatThrownBy(()->fetch((point,s)->{if(point.equals("BEFORE_CHECKPOINT")){jdbc.execute("CREATE FUNCTION ddip_management.d1_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");jdbc.execute("CREATE TRIGGER d1_fault BEFORE "+(table.equals("stage_invocations")?"UPDATE":"INSERT")+" ON ddip_management."+table+" FOR EACH ROW EXECUTE FUNCTION ddip_management.d1_fault()");}})).hasMessage("STATE_CONFLICT");assertThat(count("rss_deliveries")).isZero();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations",String.class)).isEqualTo("RUNNING");assertThat(requests.get()).isEqualTo(1);}
    void outage(boolean down){admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS "+!down);if(down)admin.queryForList("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=? AND pid<>pg_backend_pid()",database);}
    @Test void lostCheckpointAcknowledgmentIsInspectedWithoutJournalRewrite(){ready();installLostCommit("INSERT INTO ddip_management.rss_deliveries");AtomicReference<RssFetchTestSupport.Observed> observed=new AtomicReference<>();
        assertThatThrownBy(()->fetch((point,s)->{if(point.equals("BEFORE_CHECKPOINT")){observed.set(RssFetchTestSupport.observe(guard,s));loseAck.set(true);}})).hasMessage("MANAGEMENT_UNAVAILABLE");
        // Receipt inspection is read-only and does not restore the stopped authorization channel.
        assertThat(inspect(observed.get())).isTrue();assertThat(count("rss_deliveries")).isEqualTo(1);assertThat(requests.get()).isEqualTo(1);assertThatThrownBy(this::fetch).hasMessage("OWNERSHIP_LOST");
        assertThat(committed()).isTrue();
    }
    void installLostCommit(String statement){
        var real=new org.springframework.jdbc.datasource.DriverManagerDataSource(url,user,password);var wrapped=new AbstractDataSource(){
            public Connection getConnection()throws SQLException {Connection c=real.getConnection();var matched=new AtomicBoolean();return(Connection)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(p,m,args)->{try{if(m.getName().equals("prepareStatement") && args[0] instanceof String sql && sql.contains(statement))matched.set(true);Object result=m.invoke(c,args);if(m.getName().equals("commit") && matched.get() && loseAck.compareAndSet(true,false))throw new SQLException("PRIVATE","08006");return result;}catch(InvocationTargetException ex){throw ex.getCause();}});}
            public Connection getConnection(String u,String p)throws SQLException{return getConnection();}};
        jdbc=new JdbcTemplate(wrapped);tx=transactions(jdbc);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
    }
    @ParameterizedTest @ValueSource(strings={"INSERT INTO ddip_management.stage_invocations","SET command_address="}) void lostIntentOrAuthorizationAcknowledgmentNeverLaunches(String statement){ready();installLostCommit(statement);loseAck.set(true);assertThatThrownBy(this::fetch).hasMessage("MANAGEMENT_UNAVAILABLE");assertThat(requests.get()).isZero();assertThat(count("stage_invocations")).isEqualTo(1);assertThat(count("rss_deliveries")).isZero();assertThatThrownBy(this::fetch).hasMessage("OWNERSHIP_LOST");}
    @Test void unknownCheckpointAckNeedsNoInMemoryReceiptOrOwnerRefresh(){ready();installLostCommit("INSERT INTO ddip_management.rss_deliveries");loseAck.set(true);assertThatThrownBy(this::fetch).hasMessage("MANAGEMENT_UNAVAILABLE");assertThat(count("rss_deliveries")).as("checkpoint row committed").isEqualTo(1);assertThat(committed()).as("receipt on original guard").isTrue();var before=jdbc.queryForMap("SELECT * FROM ddip_management.coordinator");var binding=guard.binding();guard.close();guard=InstallationHostGuard.acquire(binding);workers=new WorkerRepository(jdbc,tx,INSTALL);assertThat(committed()).as("receipt on newly held guard").isTrue();assertThat(jdbc.queryForMap("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);assertThat(requests.get()).isEqualTo(1);assertThat(count("stage_invocations")).isEqualTo(1);assertThatThrownBy(this::fetch).hasMessage("OWNERSHIP_LOST");}
    @Test void checkpointSurvivesActualDatabaseRestart()throws Exception {ready();var observed=fetch();String signal=System.getProperty("ddip.test.rss-restart-signal");if(signal!=null){Path file=Path.of(signal);Files.writeString(file,"RESTART\n");long until=System.nanoTime()+java.time.Duration.ofSeconds(60).toNanos();while(!Files.exists(file.resolveSibling(file.getFileName()+".done")) && System.nanoTime()<until)Thread.sleep(50);assertThat(Files.exists(file.resolveSibling(file.getFileName()+".done"))).isTrue();}assertThat(inspect(observed)).isTrue();assertThat(committed()).isTrue();assertThat(requests.get()).isEqualTo(1);}
}
