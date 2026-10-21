package platform.contentapi.management;

import java.net.*;
import java.nio.file.*;
import java.sql.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.test.web.servlet.*;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import platform.contentapi.ContentApiApplication;
import platform.contentapi.execution.*;
import platform.contentapi.importing.RegisteredImportEvidence;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFixtures.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

/** Real PostgreSQL metadata/HTTP acceptance. Registered qualification evidence is synthetic setup; no native I/O. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RunSubmissionIT {
    static final UUID INSTALL=UUID.fromString("10000000-0000-4000-8000-000000000001");
    static final String CONTENT_PROFILE=RunCommandsTest.PROFILE, JSON_PROFILE="10000000-0000-4000-8000-000000000002", PARQUET_PROFILE="10000000-0000-4000-8000-000000000003";
    static final String PUB=RunCommandsTest.REF, SERVICE="20000000-0000-4000-8000-000000000002", JSON_BATCH=RunCommandsTest.INPUT, PARQUET_BATCH="30000000-0000-4000-8000-000000000002", SILVER="40000000-0000-4000-8000-000000000001";
    static final String RUNS="/api/management/v1/runs", REFS="50000000-0000-4000-8000-000000000001", SERVICE_BINDING="50000000-0000-4000-8000-000000000002", SILVER_BINDING="50000000-0000-4000-8000-000000000003";
    @TempDir static Path temp;
    JdbcTemplate jdbc,admin;ManagementTransactions tx;ReviewedSourceCatalog catalog;RunRepository runs;SourceRepository sources;
    String database,url,user,password,host,origin;int port;Path catalogFile;ConfigurableApplicationContext context;MockMvc mvc;
    @BeforeAll void database() throws Exception {
        String base=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_URL"));user=System.getenv("DDIP_TEST_DB_USER");password=System.getenv("DDIP_TEST_DB_PASSWORD");
        admin=new JdbcTemplate(new DriverManagerDataSource(base,user,password));database="ddip_submit_"+UUID.randomUUID().toString().replace("-","");admin.execute("CREATE DATABASE "+database);
        url=base.substring(0,base.lastIndexOf('/')+1)+database;jdbc=new JdbcTemplate(new DriverManagerDataSource(url,user,password));tx=transactions(jdbc);
        try(var socket=new ServerSocket(0,50,InetAddress.getByName("127.0.0.1"))){port=socket.getLocalPort();}host="127.0.0.1:"+port;origin="http://"+host;
        catalogFile=temp.resolve("catalog.json");Files.write(catalogFile,SourceJson.canonical(catalog()));
    }
    @AfterAll void cleanup(){close();if(admin!=null)admin.execute("DROP DATABASE "+database+" WITH (FORCE)");}
    @BeforeEach void setup(){
        jdbc.execute("DROP SCHEMA IF EXISTS ddip_management CASCADE");var flyway=Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        new SchemaLifecycle(flyway,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);
        catalog=ReviewedSourceCatalog.from(catalog());sources=new SourceRepository(jdbc,tx,INSTALL,catalog,new SourceCursor(KEY));runs=new RunRepository(jdbc,tx,INSTALL,catalog);
        storage(BRONZE,CONTENT,"BRONZE");storage(BATCH,RELIABILITY,"BATCH");storage(REFS,CONTENT,"REFERENCE");storage(SERVICE_BINDING,RELIABILITY,"REFERENCE");storage(SILVER_BINDING,CONTENT,"SILVER");
        for(String role:List.of("GOLD","CONTENT","CONTROL","STAGING","TEMP"))storage("content-"+role,CONTENT,role);
        for(String role:List.of("RELIABILITY","CONTROL","STAGING","TEMP"))storage("reliability-"+role,RELIABILITY,role);
        profile(CONTENT_PROFILE,true,"rss-atom-v1");profile(JSON_PROFILE,false,"request-jsonl-v2");profile(PARQUET_PROFILE,false,"request-parquet-v1");
        createSource(CONTENT,"demo-feed",rss());createSource(RELIABILITY,"demo-requests",requests("request-jsonl-v2"));createSource(RELIABILITY,"second-requests",requests("request-parquet-v1"));
        artifact(PUB,RunFixtures.evidence("PUBLISHER_REFERENCE",REFS,"demo-feed",null));artifact(SERVICE,RunFixtures.evidence("SERVICE_REFERENCE",SERVICE_BINDING,"demo-requests",null));artifact(JSON_BATCH,RunFixtures.evidence("RELIABILITY_BATCH_V2",BATCH,"demo-requests","JSONL"));artifact(PARQUET_BATCH,RunFixtures.evidence("RELIABILITY_BATCH_V2",BATCH,"second-requests","PARQUET"));artifact(SILVER,RunFixtures.silver(SILVER_BINDING,BRONZE));
    }
    static ManagementTransactions transactions(JdbcTemplate j){return new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(j.getDataSource())));}
    void storage(String id,String product,String role){byte[] b=bytes("{}");jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,b,SourceJson.sha(b),INSTALL);}
    void profile(String id,boolean content,String source){byte[] b=SourceJson.canonical(RunFixtures.profile(content,source));jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,b,SourceJson.sha(b));jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product=?",id,content?CONTENT:RELIABILITY);}
    void artifact(String id,ObjectNode evidence){byte[] bytes=SourceJson.canonical(evidence);var e=RegisteredImportEvidence.validate(bytes,SourceJson.sha(bytes)).selection();jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,product_id,kind,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at,native_format) VALUES (?,?,?,?,?,?,?,?,?,?,clock_timestamp(),?)",UUID.fromString(id),e.product(),e.kind(),e.contract(),e.processor(),e.nativeIdentity(),e.bindingId(),e.address(),bytes,SourceJson.sha(bytes),e.format());}
    void createSource(String product,String source,JsonNode c){sources.execute(SourceCommands.command(SourceCommands.Action.CREATE,null,null,create(product,source,c),"create-"+source,null),UUID.randomUUID().toString());}
    ObjectNode content(){var n=RunCommandsTest.request(true);((ObjectNode)n.get("fetchSources").get(0)).put("sourceId","demo-feed");n.withArray("retainedSilverArtifactIds").add(SILVER);return n;}
    ObjectNode reliability(boolean parquet){var n=RunCommandsTest.request(false).put("executionProfileId",parquet?PARQUET_PROFILE:JSON_PROFILE).put("serviceReferenceArtifactId",SERVICE);((ObjectNode)n.get("sources").get(0)).put("sourceId",parquet?"second-requests":"demo-requests");n.withArray("batchArtifactIds").removeAll().add(parquet?PARQUET_BATCH:JSON_BATCH);return n;}
    RunCommands.Command command(JsonNode n,String key){return RunCommands.parse(SourceJson.canonical(n),key);}
    JsonNode submit(JsonNode n,String key){return runs.submit(command(n,key),UUID.randomUUID().toString());}
    long count(String table){return jdbc.queryForObject("SELECT count(*) FROM ddip_management."+table,Long.class);}
    void start(){if(context!=null)return;context=new SpringApplicationBuilder(ContentApiApplication.class).run("--ddip.role=api","--server.address=127.0.0.1","--server.port="+port,"--ddip.database.url="+url,"--ddip.database.username="+user,"--ddip.database.password="+password,"--ddip.installation-id="+INSTALL,"--ddip.management.catalog="+catalogFile,"--ddip.management.cursor-key="+KEY,"--ddip.management.origins="+origin,"--logging.level.root=OFF","--spring.main.banner-mode=off");mvc=MockMvcBuilders.webAppContextSetup((WebApplicationContext)context).addFilters(context.getBean(ManagementBoundary.class)).build();}
    void close(){if(context!=null){context.close();context=null;}}
    @AfterEach void stop(){close();}
    MvcResult http(MockHttpServletRequestBuilder b)throws Exception{start();return mvc.perform(b.header("Host",host)).andReturn();}
    MockHttpServletRequestBuilder postRun(JsonNode n,String key){var b=post(RUNS).contentType("application/json").header("Origin",origin).header("X-DDIP-Local-Request","1").content(SourceJson.canonical(n));if(key!=null)b.header("Idempotency-Key",key);return b;}
    JsonNode body(MvcResult r){return SourceJson.parse(r.getResponse().getContentAsByteArray());}
    void code(MvcResult r,int status,String code){assertThat(r.getResponse().getStatus()).isEqualTo(status);assertThat(body(r).get("code").stringValue()).isEqualTo(code);assertThat(new String(r.getResponse().getContentAsByteArray(),java.nio.charset.StandardCharsets.UTF_8)).doesNotContain(password,url,"/private/","metadata.json","feeds.example", "PRIVATE");}
    @Test void contentAtomicSelectionAndSafeHttpDetail()throws Exception{
        var accepted=http(postRun(content(),"first"));assertThat(accepted.getResponse().getStatus()).isEqualTo(202);var data=body(accepted).get("data");assertThat(data.get("state").stringValue()).isEqualTo("QUEUED");assertThat(data.get("sourceRevisions").size()).isEqualTo(1);assertThat(data.get("inputArtifactIds").size()).isEqualTo(2);String location=accepted.getResponse().getHeader("Location");assertThat(location).isEqualTo(RUNS+"/"+data.get("runId").stringValue());assertThat(body(http(get(location))).get("data")).isEqualTo(data);
        assertThat(count("runs")).isEqualTo(1);assertThat(count("run_inputs")).isEqualTo(2);assertThat(count("run_sources")).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RUN_SUBMITTED'",Long.class)).isEqualTo(1);
        var snapshot=SourceJson.parse(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class));assertThat(snapshot.get("sources").get(0).get("endpoint").get("uri").stringValue()).isEqualTo("https://feeds.example.org/one");assertThat(snapshot.get("inputs").get(0).get("qualification").get("verification").get("inventoryIdentity").get("kind").stringValue()).isEqualTo("E5_SILVER_INVENTORY_V1");
        for(String table:List.of("attempts","stage_invocations","worker_instances","rss_deliveries","controls"))assertThat(count(table)).isZero();assertThat(accepted.getResponse().getContentAsString()).doesNotContain("Sha256","configuration","binding","fingerprint","feeds.example","parquet");
    }
    @ParameterizedTest @ValueSource(booleans={false,true}) void acceptsBothReliabilityFormats(boolean parquet){var n=submit(reliability(parquet),"first");assertThat(n.get("state").stringValue()).isEqualTo("QUEUED");assertThat(n.get("dataOrigin").stringValue()).isEqualTo("synthetic");assertThat(count("run_inputs")).isEqualTo(2);}
    @Test void contentEmptyHistoryAllowed(){var n=content();n.withArray("retainedSilverArtifactIds").removeAll();submit(n,"empty");assertThat(count("run_inputs")).isEqualTo(1);}
    @Test void replayBeforeSourceRevisionDisablementCapacityAndCatalogChecks(){var request=content();var initial=submit(request,"same");byte[] frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);
        sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("new",rss().put("feedEndpointId",ENDPOINT2)),"revise","\"0\""),"test");sources.execute(SourceCommands.command(SourceCommands.Action.ENABLE,CONTENT,"demo-feed",SourceJson.object().put("enabled",false),"disable","\"1\""),"test");
        var empty=SourceJson.object().put("catalogVersion",1);for(String name:List.of("endpoints","reliabilityProfiles","storageProfiles"))empty.putArray(name);
        runs=new RunRepository(jdbc,tx,INSTALL,ReviewedSourceCatalog.from(empty));
        assertThat(submit(request,"same")).isEqualTo(initial);assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(frozen);
        runs=new RunRepository(jdbc,tx,INSTALL,catalog);assertThatThrownBy(()->submit(request,"new")).hasMessage("STATE_CONFLICT");
        ((ObjectNode)request.get("fetchSources").get(0)).put("revision",2);assertThatThrownBy(()->submit(request,"newer")).hasMessage("SOURCE_DISABLED");
    }
    @Test void replayAfterTerminalStateAndChangedRequestConflict(){var initial=submit(content(),"same");jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=clock_timestamp(),row_version=row_version+1,outcome_code='TEST_CLOSED' WHERE run_id=?",UUID.fromString(initial.get("runId").stringValue()));var replay=submit(content(),"same");assertThat(replay.get("runId")).isEqualTo(initial.get("runId"));assertThat(replay.get("state").stringValue()).isEqualTo("FAILED");var different=content();different.withArray("retainedSilverArtifactIds").removeAll();assertThatThrownBy(()->submit(different,"same")).hasMessage("IDEMPOTENCY_CONFLICT");assertThat(count("runs")).isEqualTo(1);}
    @Test void newDisabledSourceRejected(){sources.execute(SourceCommands.command(SourceCommands.Action.ENABLE,CONTENT,"demo-feed",SourceJson.object().put("enabled",false),"disable","\"0\""),"test");assertThatThrownBy(()->submit(content(),"new")).hasMessage("SOURCE_DISABLED");assertThat(count("runs")).isZero();}
    @Test void concurrentIdenticalAndDifferentScopedKeys()throws Exception{race(content(),content(),"same",false);assertThat(count("runs")).isEqualTo(1);var different=content();different.withArray("retainedSilverArtifactIds").removeAll();race(content(),different,"race",true);assertThat(count("runs")).isEqualTo(2);}
    void race(JsonNode a,JsonNode b,String key,boolean conflict)throws Exception{var ready=new CountDownLatch(2);var go=new CountDownLatch(1);try(var pool=Executors.newFixedThreadPool(2)){var tasks=new ArrayList<Future<String>>();for(var body:List.of(a,b))tasks.add(pool.submit(()->{ready.countDown();go.await();try{return submit(body,key).get("runId").stringValue();}catch(SourceFailure f){return f.code.name();}}));assertThat(ready.await(5,TimeUnit.SECONDS)).isTrue();go.countDown();var results=List.of(tasks.get(0).get(10,TimeUnit.SECONDS),tasks.get(1).get(10,TimeUnit.SECONDS));if(conflict)assertThat(results).contains("IDEMPOTENCY_CONFLICT");else assertThat(results.get(0)).isEqualTo(results.get(1));}}
    @Test void fullQueueAndReplayNoAdditionalCapacity()throws Exception{JsonNode first=null;for(int i=0;i<20;i++){var n=submit(content(),"queue"+i);if(i==0)first=n;}code(http(postRun(content(),"overflow")),429,"MANAGEMENT_CAPACITY_EXCEEDED");assertThat(submit(content(),"queue0")).isEqualTo(first);assertThat(count("runs")).isEqualTo(20);assertThat(count("run_inputs")).isEqualTo(40);}
    @Test void retainedHistoryCapRejectsNewWithoutDeletingOrBlockingReplay() {
        var original=submit(content(),"original");byte[] empty=bytes("{}");String hash=SourceJson.sha(empty);
        jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state,finished_at) SELECT gen_random_uuid(),'content-intelligence','CONTENT_RSS_V1',1,'local-operator','history-'||n,?::bytea,?::text,?::bytea,?::text,?::text,'FAILED',clock_timestamp() FROM generate_series(1,9999) n",empty,hash,empty,hash,CONTENT_PROFILE);
        assertThatThrownBy(()->submit(content(),"overflow-history")).hasMessage("MANAGEMENT_CAPACITY_EXCEEDED");assertThat(submit(content(),"original")).isEqualTo(original);assertThat(count("runs")).isEqualTo(10000);assertThat(count("run_inputs")).isEqualTo(2);
    }
    @ParameterizedTest @ValueSource(strings={"run_inputs","audit_events"}) void rollbackOfAnyLinkOrAudit(String table){jdbc.execute("CREATE FUNCTION ddip_management.inject_failure() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");jdbc.execute("CREATE TRIGGER injected BEFORE INSERT ON ddip_management."+table+" FOR EACH ROW EXECUTE FUNCTION ddip_management.inject_failure()");assertThatThrownBy(()->submit(content(),"rollback")).hasMessage("STATE_CONFLICT").hasNoCause();for(String t:List.of("runs","run_sources","run_inputs"))assertThat(count(t)).isZero();assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE run_id IS NOT NULL",Long.class)).isZero();jdbc.execute("DROP TRIGGER injected ON ddip_management."+table);submit(content(),"rollback");}
    @ParameterizedTest @ValueSource(strings={"STOPPING","RECOVERY_ONLY"}) void coordinatorModeBlocksNewButNotReplay(String mode)throws Exception{var first=submit(content(),"old");jdbc.update("UPDATE ddip_management.coordinator SET mode=?,row_version=row_version+1",mode);code(http(postRun(content(),"new")),503,"COORDINATOR_RECOVERY_REQUIRED");assertThat(submit(content(),"old")).isEqualTo(first);}
    @Test void activeAndAbandonedImportOwnershipRemainsUntouched()throws Exception {
        var initial=submit(content(),"replay");var directory=Files.createDirectory(temp.resolve(UUID.randomUUID().toString()));var binding=OwnershipFixtures.host(directory);OwnershipFixtures.provision(jdbc,binding);
        var ownership=new ImportOwnershipRepository(jdbc,tx,INSTALL);UUID operation=UUID.randomUUID();
        try(var guard=InstallationHostGuard.acquire(binding)) {ownership.reserve(guard,OwnershipFixtures.reservation(operation));var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");
            assertThatThrownBy(()->submit(content(),"new")).hasMessage("COORDINATOR_RECOVERY_REQUIRED");assertThat(submit(content(),"replay")).isEqualTo(initial);assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);
        }
        assertThatThrownBy(()->submit(content(),"abandoned")).hasMessage("COORDINATOR_RECOVERY_REQUIRED");assertThat(jdbc.queryForObject("SELECT active_import_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(operation);assertThat(count("attempts")).isZero();
    }
    UUID activeExecution() {
        UUID run=UUID.fromString(submit(content(),"active").get("runId").stringValue()),attempt=UUID.randomUUID(),instance=UUID.randomUUID();
        tx.required(()->{jdbc.queryForList("SELECT * FROM ddip_management.coordinator FOR UPDATE");
            String hash=jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,CONTENT_PROFILE);
            jdbc.update("INSERT INTO ddip_management.worker_instances(instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,heartbeat_at,status) VALUES (?,?,?,?,?,1,now(),now(),'READY')",instance,INSTALL,CONTENT_PROFILE,hash,HASH);
            jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state) VALUES (?,?,1,'EXECUTE',?,1,'RUNNING')",attempt,run,instance);
            jdbc.update("UPDATE ddip_management.runs SET state='RUNNING',current_attempt_id=?,row_version=row_version+1 WHERE run_id=?",attempt,run);
            jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=?,owner_instance_id=?,epoch=1,heartbeat_at=clock_timestamp(),row_version=row_version+1",attempt,instance);return null;});return run;
    }
    @Test void oneHealthyActiveSlotAllowsQueueWithoutChangingOwnership() {activeExecution();var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");assertThat(submit(content(),"queued").get("state").stringValue()).isEqualTo("QUEUED");assertThat(count("attempts")).isEqualTo(1);assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);}
    @Test void recoveryRequiredAndStaleOwnershipBlockAdmissions() {UUID run=activeExecution();jdbc.update("UPDATE ddip_management.coordinator SET heartbeat_at=clock_timestamp()-interval '1 hour',row_version=row_version+1");assertThatThrownBy(()->submit(content(),"stale")).hasMessage("COORDINATOR_RECOVERY_REQUIRED");jdbc.update("UPDATE ddip_management.runs SET state='RECOVERY_REQUIRED',row_version=row_version+1 WHERE run_id=?",run);assertThatThrownBy(()->submit(content(),"recovery")).hasMessage("COORDINATOR_RECOVERY_REQUIRED");assertThat(count("runs")).isEqualTo(1);}
    @Test void sourceUpdateWinsBeforeAdmissionCapturesNoStaleRevision()throws Exception {
        var entered=new CountDownLatch(1);var ready=new CountDownLatch(1);try(var pool=Executors.newFixedThreadPool(2)) {
            var update=pool.submit(()->tx.required(()->{jdbc.queryForList("SELECT * FROM ddip_management.coordinator FOR UPDATE");entered.countDown();await(ready);sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("updated",rss().put("feedEndpointId",ENDPOINT2)),"r2","\"0\""),"test");return true;}));
            assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();var request=pool.submit(()->{ready.countDown();return catchThrowable(()->submit(content(),"race"));});assertThat(update.get(10,TimeUnit.SECONDS)).isTrue();assertThat(request.get(10,TimeUnit.SECONDS)).hasMessage("STATE_CONFLICT");
        }
        assertThat(count("runs")).isZero();var latest=content();((ObjectNode)latest.get("fetchSources").get(0)).put("revision",2);submit(latest,"new");var frozen=SourceJson.parse(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class));assertThat(frozen.get("sources").get(0).get("endpoint").get("endpointId").stringValue()).isEqualTo(ENDPOINT2);
    }
    @Test void admissionWinsAndFreezesRevisionBeforeConcurrentUpdate()throws Exception {
        var captured=new CountDownLatch(1);var resume=new CountDownLatch(1);JdbcTemplate delayed=wrapped((c,m,args,value)->{if(m.getName().equals("prepareStatement") && args[0].toString().startsWith("INSERT INTO ddip_management.runs")){captured.countDown();await(resume);}});
        var repository=new RunRepository(delayed,transactions(delayed),INSTALL,catalog);
        try(var pool=Executors.newFixedThreadPool(2)){
            var accept=pool.submit(()->repository.submit(command(content(),"race"),"test"));assertThat(captured.await(5,TimeUnit.SECONDS)).isTrue();
            var update=pool.submit(()->sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,CONTENT,"demo-feed",revise("updated",rss().put("feedEndpointId",ENDPOINT2)),"r2","\"0\""),"test"));
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);boolean blocked=false;while(System.nanoTime()<deadline){if(Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=? AND wait_event_type='Lock' AND query LIKE 'SELECT installation_id%')",Boolean.class,database))){blocked=true;break;}Thread.sleep(10);}
            assertThat(blocked).isTrue();resume.countDown();assertThat(accept.get(10,TimeUnit.SECONDS).get("state").stringValue()).isEqualTo("QUEUED");assertThat(update.get(10,TimeUnit.SECONDS).status()).isEqualTo(200);
        }finally{resume.countDown();}
        var frozen=SourceJson.parse(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class));assertThat(frozen.get("sources").get(0).get("revision").intValue()).isEqualTo(1);assertThat(frozen.get("sources").get(0).get("endpoint").get("endpointId").stringValue()).isEqualTo(ENDPOINT);
    }
    static void await(CountDownLatch latch){try{if(!latch.await(10,TimeUnit.SECONDS))throw new AssertionError("barrier timed out");}catch(InterruptedException ex){Thread.currentThread().interrupt();throw new AssertionError(ex);}}
    @Test void immutableSelectionAndRelationalConstraintsHold() {var run=submit(content(),"immutable");UUID id=UUID.fromString(run.get("runId").stringValue());assertThatThrownBy(()->jdbc.update("UPDATE ddip_management.runs SET captured_selection=convert_to('{}','UTF8'),row_version=row_version+1 WHERE run_id=?",id)).hasMessageContaining("IMMUTABLE_IDENTITY");assertThatThrownBy(()->jdbc.update("DELETE FROM ddip_management.run_inputs WHERE run_id=?",id)).hasMessageContaining("IMMUTABLE_EVIDENCE");assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.run_inputs SELECT run_id,'EXTRA',0,?,evidence_bytes,evidence_sha256 FROM ddip_management.run_inputs WHERE run_id=? LIMIT 1",UUID.randomUUID(),id)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);}
    @Test void restartDurabilityUsesFreshDatabaseServerWhenRequested()throws Exception {
        var a=submit(content(),"first");submit(reliability(false),"second");var identities=jdbc.queryForList("SELECT run_id,submission_seq,request_sha256,selection_sha256 FROM ddip_management.runs ORDER BY submission_seq");byte[] frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE idempotency_key='first'",byte[].class);byte[] input=jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.run_inputs WHERE role='RETAINED_SILVER'",byte[].class);
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);String signal=System.getProperty("ddip.test.submission-restart-signal");
        if(signal!=null){Files.writeString(Path.of(signal),"READY");long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<deadline)Thread.sleep(100);assertThat(Files.exists(Path.of(signal+".done"))).isTrue();}
        var fresh=new JdbcTemplate(new DriverManagerDataSource(url,user,password));if(signal!=null)assertThat(fresh.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class)).isAfter(started);
        assertThat(fresh.queryForList("SELECT run_id,submission_seq,request_sha256,selection_sha256 FROM ddip_management.runs ORDER BY submission_seq")).isEqualTo(identities);assertThat(fresh.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE idempotency_key='first'",byte[].class)).isEqualTo(frozen);assertThat(fresh.queryForObject("SELECT evidence_bytes FROM ddip_management.run_inputs WHERE role='RETAINED_SILVER'",byte[].class)).isEqualTo(input);assertThat(new RunRepository(fresh,transactions(fresh),INSTALL,catalog).get(a.get("runId").stringValue())).isEqualTo(a);
    }
    @Test void invalidProfileAndProductMembership(){assertThatThrownBy(()->submit(content().put("executionProfileId",JSON_PROFILE),"wrong")).hasMessage("INVALID_ARGUMENT");assertThatThrownBy(()->submit(content().put("executionProfileId",UUID.randomUUID().toString()),"missing")).hasMessage("INVALID_ARGUMENT");assertThatThrownBy(()->submit(content().put("publisherReferenceArtifactId",SERVICE),"wrong-ref")).hasMessage("INVALID_ARGUMENT");}
    @Test void mixedFormatsWrongSourceAndUnknownArtifacts(){var mixed=reliability(false);mixed.withArray("batchArtifactIds").add(PARQUET_BATCH);assertThatThrownBy(()->submit(mixed,"mixed")).hasMessage("INVALID_ARGUMENT");var wrong=reliability(false);((ObjectNode)wrong.get("sources").get(0)).put("sourceId","second-requests");assertThatThrownBy(()->submit(wrong,"wrong")).hasMessage("INVALID_ARGUMENT");var missing=content().put("publisherReferenceArtifactId",UUID.randomUUID().toString());assertThatThrownBy(()->submit(missing,"missing")).hasMessage("INVALID_ARGUMENT");}
    @Test void malformedRegistryEvidenceFailsClosed(){var corrupt=RunFixtures.evidence("PUBLISHER_REFERENCE",REFS,"demo",null);((ObjectNode)corrupt.get("verification").get("native")).put("fingerprint","b".repeat(64));var q=SourceJson.canonical(corrupt);String id=UUID.randomUUID().toString(); // different native namespace for test setup
        ((ObjectNode)corrupt.get("selection").get("identity")).put("revision","r2");((ObjectNode)corrupt.get("selection")).put("relativeAddress","reference_id=demo/revision=r2");q=SourceJson.canonical(corrupt);
        jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,product_id,kind,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at) VALUES (?,'content-intelligence','PUBLISHER_REFERENCE',1,1,?,?,'reference_id=demo/revision=r2',?,?,clock_timestamp())",UUID.fromString(id),"{\"referenceId\":\"demo\",\"revision\":\"r2\"}",REFS,q,SourceJson.sha(q));assertThatThrownBy(()->submit(content().put("publisherReferenceArtifactId",id),"corrupt")).hasMessage("STATE_CONFLICT");assertThat(count("runs")).isZero();}
    @Test void lostCommitAcknowledgmentResolvesExactBinding(){var armed=new AtomicBoolean(true);JdbcTemplate lost=wrapped((c,m,args,result)->{if(m.getName().equals("commit") && armed.compareAndSet(true,false))throw new SQLException("PRIVATE COMMIT","08006");});var repository=new RunRepository(lost,transactions(lost),INSTALL,catalog);assertThatThrownBy(()->repository.submit(command(content(),"lost"),"test")).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();assertThat(armed).isFalse();assertThat(count("runs")).isEqualTo(1);var replay=submit(content(),"lost");assertThat(replay.get("runId").stringValue()).isEqualTo(jdbc.queryForObject("SELECT run_id FROM ddip_management.runs",UUID.class).toString());assertThat(count("runs")).isEqualTo(1);}
    interface Hook {void call(Connection c,Method method,Object[] args,Object result)throws Exception;}
    JdbcTemplate wrapped(Hook hook){var ds=new AbstractDataSource(){public Connection getConnection()throws SQLException{Connection real=jdbc.getDataSource().getConnection();return(Connection)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,m,args)->{try{var value=m.invoke(real,args);hook.call(real,m,args,value);return value;}catch(InvocationTargetException ex){throw ex.getCause();}});}public Connection getConnection(String u,String p)throws SQLException{return getConnection();}};return new JdbcTemplate(ds);}
    @Test void outageIsSafeAndLeavesNoAcceptedState()throws Exception{start();admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS false");try{assertThatThrownBy(()->submit(content(),"outage")).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();}finally{admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS true");}assertThat(count("runs")).isZero();submit(content(),"outage");}
    @Test void persistedSelectionAndOrderSurviveApplicationRestart()throws Exception{var first=submit(content(),"first");var before=jdbc.queryForList("SELECT run_id,submission_seq,request_sha256,selection_sha256 FROM ddip_management.runs ORDER BY submission_seq");byte[] frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);start();close();start();assertThat(body(http(get(RUNS+"/"+first.get("runId").stringValue()))).get("data")).isEqualTo(first);submit(reliability(false),"second");assertThat(jdbc.queryForList("SELECT run_id,submission_seq,request_sha256,selection_sha256 FROM ddip_management.runs ORDER BY submission_seq").get(0)).isEqualTo(before.get(0));assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,UUID.fromString(first.get("runId").stringValue()))).isEqualTo(frozen);}
    @ParameterizedTest @ValueSource(strings={"missing-key","bad-key","unknown","duplicate","large","type","query","if-match"}) void strictHttpValidation(String reason)throws Exception{var n=content();var b=postRun(n,reason.equals("missing-key")?null:reason.equals("bad-key")?"space key":"test");switch(reason){case "unknown"->b.content(SourceJson.canonical(n.put("path","/private/input")));case "duplicate"->b.content("{\"pipeline\":1,\"pipeline\":2}");case "large"->b.content(new byte[65537]);case "type"->b.content(SourceJson.canonical(n.put("executionProfileId",1)));case "query"->b.queryParam("latest","true");case "if-match"->b.header("If-Match","\"0\"");default->{}}code(http(b),400,"INVALID_ARGUMENT");assertThat(count("runs")).isZero();}
    @ParameterizedTest @ValueSource(strings={"origin","missing-origin","header","host","content-type"}) void localBoundary(String reason)throws Exception {
        start();var b=postRun(content(),"key").header("Host",host).with(r->{switch(reason){case "origin"->{r.removeHeader("Origin");r.addHeader("Origin","https://evil.example");}case "missing-origin"->r.removeHeader("Origin");case "header"->{r.removeHeader("X-DDIP-Local-Request");r.addHeader("X-DDIP-Local-Request","0");}case "host"->{r.removeHeader("Host");r.addHeader("Host","evil.example");}default->{}}return r;});if(reason.equals("content-type"))b.contentType("text/plain");code(mvc.perform(b).andReturn(),403,"LOCAL_BOUNDARY_REJECTED");assertThat(count("runs")).isZero();
    }
    @Test void httpDatabaseOutageHasFixed503Envelope()throws Exception {start();admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS false");admin.queryForList("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=? AND pid<>pg_backend_pid()",database);try{code(http(postRun(content(),"outage")),503,"MANAGEMENT_UNAVAILABLE");}finally{admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS true");}assertThat(count("runs")).isZero();}
    @Test void invalidSourceRevisionAndMissingProfileStorage() {
        var request=content();((ObjectNode)request.get("fetchSources").get(0)).put("revision",2);assertThatThrownBy(()->submit(request,"revision")).hasMessage("STATE_CONFLICT");
        byte[] config=SourceJson.canonical(RunFixtures.profile(true,"rss-atom-v1"));String id=UUID.randomUUID().toString();jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,config,SourceJson.sha(config));assertThatThrownBy(()->submit(content().put("executionProfileId",id),"storage")).hasMessage("INVALID_ARGUMENT");
    }
    @Test void acceptedRunsNeverModifyRegisteredEvidence() {
        var before=jdbc.queryForList("SELECT artifact_id,native_identity,evidence_sha256,verified_at FROM ddip_management.artifacts ORDER BY artifact_id");var bytesBefore=jdbc.queryForList("SELECT encode(evidence_bytes,'hex') AS bytes FROM ddip_management.artifacts ORDER BY artifact_id");submit(content(),"content");submit(reliability(false),"jsonl");submit(reliability(true),"parquet");assertThat(jdbc.queryForList("SELECT artifact_id,native_identity,evidence_sha256,verified_at FROM ddip_management.artifacts ORDER BY artifact_id")).isEqualTo(before);assertThat(jdbc.queryForList("SELECT encode(evidence_bytes,'hex') AS bytes FROM ddip_management.artifacts ORDER BY artifact_id")).isEqualTo(bytesBefore);
    }
    @Test void noFutureRoutesOrExecutionSideEffects()throws Exception{code(http(get(RUNS)),404,"NOT_FOUND");code(http(postRun(content(),"x").with(r->{r.setRequestURI(RUNS+"/"+UUID.randomUUID()+"/controls");return r;})),404,"NOT_FOUND");code(http(get(RUNS+"/"+UUID.randomUUID())),404,"NOT_FOUND");assertThat(count("attempts")).isZero();}
}
