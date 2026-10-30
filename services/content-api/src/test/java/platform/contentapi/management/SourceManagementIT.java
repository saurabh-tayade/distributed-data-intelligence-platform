package platform.contentapi.management;

import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.test.web.servlet.*;
import org.springframework.test.web.servlet.request.MockHttpServletRequestBuilder;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
import platform.contentapi.ContentApiApplication;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.management.SourceFixtures.*;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SourceManagementIT {
    static final UUID INSTALL=UUID.fromString("10000000-0000-4000-8000-000000000001");
    static final String BASE="/api/management/v1", PATH=BASE+"/sources/"+CONTENT+"/demo-feed";
    @TempDir Path temp;
    JdbcTemplate admin,jdbc;String database,url,user,password,origin,host;int port;
    ConfigurableApplicationContext context;MockMvc mvc;Path catalogFile;
    @BeforeAll void database() {
        String base=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_URL"),"Dedicated PostgreSQL service required");
        user=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_USER"));password=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_PASSWORD"));
        admin=new JdbcTemplate(new DriverManagerDataSource(base,user,password));database="ddip_b2a_"+UUID.randomUUID().toString().replace("-","");admin.execute("CREATE DATABASE "+database);
        url=base.substring(0,base.lastIndexOf('/')+1)+database;jdbc=new JdbcTemplate(new DriverManagerDataSource(url,user,password));
    }
    @AfterAll void cleanupDatabase() {if(admin!=null && database!=null)admin.execute("DROP DATABASE "+database+" WITH (FORCE)");}
    @BeforeEach void setup() throws Exception {
        jdbc.execute("DROP SCHEMA IF EXISTS ddip_management CASCADE");
        var flyway=Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        var tx=new ManagementTransactions(new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.support.JdbcTransactionManager(jdbc.getDataSource())));
        new SchemaLifecycle(flyway,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);
        for(String id:List.of(BRONZE,BATCH)) {
            byte[] bytes=bytes("{}");jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,id.equals(BRONZE)?CONTENT:RELIABILITY,id.equals(BRONZE)?"BRONZE":"BATCH",bytes,SourceJson.sha(bytes),INSTALL);
        }
        catalogFile=temp.resolve("catalog.json");Files.write(catalogFile,SourceJson.canonical(catalog()));
        try(var socket=new ServerSocket(0,50,InetAddress.getByName("127.0.0.1"))) {port=socket.getLocalPort();}
        host="127.0.0.1:"+port;origin="http://"+host;start();
    }
    void start() {
        context=new SpringApplicationBuilder(ContentApiApplication.class).run("--ddip.role=api","--server.port="+port,"--server.address=127.0.0.1","--ddip.database.url="+url,"--ddip.database.username="+user,"--ddip.database.password="+password,"--ddip.installation-id="+INSTALL,
                "--ddip.management.catalog="+catalogFile,"--ddip.management.cursor-key="+KEY,"--ddip.management.origins="+origin+",http://127.0.0.1:5173","--spring.main.banner-mode=off","--logging.level.root=OFF");
        mvc=MockMvcBuilders.webAppContextSetup((WebApplicationContext)context).addFilters(context.getBean(ManagementBoundary.class)).build();
    }
    @AfterEach void close() {if(context!=null) {context.close();context=null;}}
    MvcResult perform(MockHttpServletRequestBuilder b) throws Exception {return mvc.perform(b.header("Host",host)).andReturn();}
    MvcResult write(MockHttpServletRequestBuilder b,JsonNode data,String key,String etag) throws Exception {
        b.contentType("application/json").header("Origin",origin).header("X-DDIP-Local-Request","1").header("Idempotency-Key",key).content(SourceJson.canonical(data));
        if(etag!=null)b.header("If-Match",etag);return perform(b);
    }
    JsonNode body(MvcResult r) {return SourceJson.JSON.readTree(r.getResponse().getContentAsByteArray());}
    JsonNode data(MvcResult r) {return body(r).get("data");}
    MvcResult created() throws Exception {return write(post(BASE+"/sources"),create(CONTENT,"demo-feed",rss()),"create",null);}
    long count(String table) {return jdbc.queryForObject("SELECT count(*) FROM ddip_management."+table,Long.class);}
    void code(MvcResult result,int status,String code) {
        assertThat(result.getResponse().getStatus()).isEqualTo(status);var n=body(result);assertThat(n.get("code").stringValue()).isEqualTo(code);
        assertThat(n.propertyNames()).containsExactlyInAnyOrder("apiVersion","code","message","correlationId","retryable");assertThat(UUID.fromString(n.get("correlationId").stringValue())).isNotNull();
    }
    @Test void createGetCapabilitiesAndPersistAcrossApplicationRestart() throws Exception {
        var r=created();assertThat(r.getResponse().getStatus()).isEqualTo(201);assertThat(r.getResponse().getHeader("ETag")).isEqualTo("\"0\"");
        assertThat(data(r).get("currentRevision").intValue()).isEqualTo(1);assertThat(data(r).get("configSha256").stringValue()).isEqualTo("621880a6619c9d7dc570a9fd3bfba86909d6628872503d7d7cba3d30cc489f86");
        assertThat(data(r).get("createdAt").stringValue()).matches(".*\\.[0-9]{6}Z");
        close();start();assertThat(data(perform(get(PATH)))).isEqualTo(data(r));
        var capabilities=perform(get(BASE+"/capabilities"));assertThat(capabilities.getResponse().getStatus()).isEqualTo(200);
        String text=capabilities.getResponse().getContentAsString();assertThat(text).contains("SOURCE_MANAGEMENT_ONLY").doesNotContain("https://feeds",url,"runSubmission","private",password);
    }
    @ParameterizedTest @ValueSource(strings={"request-jsonl-v2","request-parquet-v1"})
    void syntheticReliabilitySources(String profile) throws Exception {
        var r=write(post(BASE+"/sources"),create(RELIABILITY,"demo-requests",requests(profile)),"create",null);
        assertThat(r.getResponse().getStatus()).isEqualTo(201);assertThat(data(r).get("config").get("dataOrigin").stringValue()).isEqualTo("synthetic");
        assertThat(count("runs")).isZero();assertThat(count("attempts")).isZero();assertThat(count("worker_instances")).isZero();
    }
    @Test void revisionsNoopRevertAndGateHaveIndependentHistory() throws Exception {
        created();var rev=write(post(PATH+"/revisions"),revise("Corrected",rss().put("feedEndpointId",ENDPOINT2)),"revise","\"0\"");assertThat(rev.getResponse().getStatus()).isEqualTo(200);
        assertThat(data(rev).get("currentRevision").intValue()).isEqualTo(2);
        var noop=write(post(PATH+"/revisions"),revise("Corrected",rss().put("feedEndpointId",ENDPOINT2)),"noop","\"1\"");assertThat(data(noop)).isEqualTo(data(rev));
        var revert=write(post(PATH+"/revisions"),revise("Reviewed source",rss()),"revert","\"1\"");assertThat(data(revert).get("currentRevision").intValue()).isEqualTo(3);
        var gate=write(put(PATH+"/enabled"),SourceJson.object().put("enabled",false),"disable","\"2\"");assertThat(data(gate).get("rowVersion").stringValue()).isEqualTo("3");assertThat(data(gate).get("currentRevision").intValue()).isEqualTo(3);
        var sameGate=write(put(PATH+"/enabled"),SourceJson.object().put("enabled",false),"disable-noop","\"3\"");assertThat(data(sameGate)).isEqualTo(data(gate));
        var rows=data(perform(get(PATH+"/revisions")));assertThat(rows.size()).isEqualTo(3);assertThat(rows.get(0).get("config")).isEqualTo(rows.get(2).get("config"));assertThat(rows.get(1).get("displayName").stringValue()).isEqualTo("Corrected");
        assertThat(count("source_revisions")).isEqualTo(3);assertThat(count("audit_events")).isEqualTo(6);
    }
    @Test void newStaleAndMissingPreconditionsDifferFromReplay() throws Exception {
        var initial=created();var revised=write(post(PATH+"/revisions"),revise("Second",rss()),"update","\"0\"");
        code(write(put(PATH+"/enabled"),SourceJson.object().put("enabled",false),"stale","\"0\""),412,"PRECONDITION_FAILED");
        code(write(put(PATH+"/enabled"),SourceJson.object().put("enabled",false),"missing",null),428,"PRECONDITION_REQUIRED");
        assertThat(data(write(post(BASE+"/sources"),create(CONTENT,"demo-feed",rss()),"create",null))).isEqualTo(data(initial));
        assertThat(data(write(post(PATH+"/revisions"),revise("Second",rss()),"update","\"0\""))).isEqualTo(data(revised));
        code(write(post(PATH+"/revisions"),revise("Different",rss()),"update","\"0\""),409,"IDEMPOTENCY_CONFLICT");
        assertThat(count("command_bindings")).isEqualTo(2);
    }
    @Test void duplicateSourceAndFamilyChangesCannotRecycleIdentity() throws Exception {
        created();code(write(post(BASE+"/sources"),create(CONTENT,"demo-feed",rss()),"other",null),409,"STATE_CONFLICT");
        write(post(BASE+"/sources"),create(RELIABILITY,"demo-requests",requests("request-jsonl-v2")),"create",null);
        code(write(post(BASE+"/sources/"+RELIABILITY+"/demo-requests/revisions"),revise("Parquet",requests("request-parquet-v1")),"change","\"0\""),409,"STATE_CONFLICT");
        assertThat(count("sources")).isEqualTo(2);
    }
    @Test void concurrentIdenticalCommandsCommitOnce() throws Exception {
        var start=new CountDownLatch(1);
        try(var pool=Executors.newFixedThreadPool(2)) {
            Callable<MvcResult> call=() -> {start.await();return write(post(BASE+"/sources"),create(CONTENT,"demo-feed",rss()),"same",null);};
            var a=pool.submit(call);var b=pool.submit(call);start.countDown();var first=a.get(10,TimeUnit.SECONDS);var second=b.get(10,TimeUnit.SECONDS);
            assertThat(first.getResponse().getStatus()).isEqualTo(201);assertThat(second.getResponse().getStatus()).isEqualTo(201);assertThat(data(first)).isEqualTo(data(second));
        }
        assertThat(count("sources")).isEqualTo(1);assertThat(count("source_revisions")).isEqualTo(1);assertThat(count("command_bindings")).isEqualTo(1);assertThat(count("audit_events")).isEqualTo(1);
    }
    @Test void concurrentRevisionsHaveExactlyOneWinner() throws Exception {
        created();var start=new CountDownLatch(1);
        try(var pool=Executors.newFixedThreadPool(2)) {
            var a=pool.submit(() -> {start.await();return write(post(PATH+"/revisions"),revise("A",rss()),"a","\"0\"");});
            var b=pool.submit(() -> {start.await();return write(post(PATH+"/revisions"),revise("B",rss()),"b","\"0\"");});start.countDown();
            assertThat(List.of(a.get(10,TimeUnit.SECONDS).getResponse().getStatus(),b.get(10,TimeUnit.SECONDS).getResponse().getStatus())).containsExactlyInAnyOrder(200,412);
        }
        assertThat(count("source_revisions")).isEqualTo(2);assertThat(count("command_bindings")).isEqualTo(2);assertThat(count("audit_events")).isEqualTo(2);
    }
    @Test void canonicalReplayLostAcknowledgmentAndCatalogRemoval() throws Exception {
        var request=create(CONTENT,"demo-feed",rss());var command=SourceCommands.command(SourceCommands.Action.CREATE,null,null,request,"lost",null);
        var committed=context.getBean(SourceRepository.class).execute(command,UUID.randomUUID().toString()); // response deliberately discarded
        var replay=write(post(BASE+"/sources"),SourceJson.parse(bytes("{\"config\":"+new String(SourceJson.canonical(rss()),StandardCharsets.UTF_8)+",\"enabled\":true,\"sourceId\":\"demo-feed\",\"productId\":\"content-intelligence\",\"displayName\":\"Reviewed source\"}")),"lost",null);
        assertThat(data(replay)).isEqualTo(committed.data());
        close();Files.write(catalogFile,bytes("{\"catalogVersion\":1,\"endpoints\":[],\"reliabilityProfiles\":[],\"storageProfiles\":[]}"));start();
        assertThat(data(write(post(BASE+"/sources"),request,"lost",null))).isEqualTo(committed.data());
        assertThat(data(perform(get(PATH)))).isEqualTo(committed.data());
        code(write(post(PATH+"/revisions"),revise("New",rss()),"new","\"0\""),400,"INVALID_ARGUMENT");
    }
    @Test void auditFailureRollsBackSourceRevisionAndBindingTogether() throws Exception {
        jdbc.execute("CREATE FUNCTION ddip_management.fail_test_audit() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE-SQL-DETAIL',ERRCODE='23514'; END $$");
        jdbc.execute("CREATE TRIGGER fail_test_audit BEFORE INSERT ON ddip_management.audit_events FOR EACH ROW EXECUTE FUNCTION ddip_management.fail_test_audit()");
        var failed=created();code(failed,409,"STATE_CONFLICT");assertThat(failed.getResponse().getContentAsString()).doesNotContain("PRIVATE-SQL-DETAIL",url,password);
        for(String table:List.of("sources","source_revisions","command_bindings","audit_events"))assertThat(count(table)).isZero();
        jdbc.execute("DROP TRIGGER fail_test_audit ON ddip_management.audit_events");assertThat(created().getResponse().getStatus()).isEqualTo(201);
    }
    @Test void databaseGuardsProtectOldRevisionsAndCapturedResponses() throws Exception {
        created();assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.source_revisions SET display_name='bad'")).hasMessageContaining("IMMUTABLE_EVIDENCE");
        assertThatThrownBy(() -> jdbc.update("DELETE FROM ddip_management.source_revisions")).hasMessageContaining("IMMUTABLE_EVIDENCE");
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.command_bindings SET response_data=convert_to('{}','UTF8')")).hasMessageContaining("IMMUTABLE_EVIDENCE");
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.sources SET source_seq=DEFAULT,row_version=row_version+1")).hasMessageContaining("IMMUTABLE_IDENTITY");
    }
    @Test void sourceAndRevisionPaginationPinHighWaterAndValidateCursors() throws Exception {
        created();write(post(BASE+"/sources"),create(RELIABILITY,"demo-requests",requests("request-jsonl-v2")),"second",null);
        var first=perform(get(BASE+"/sources").param("limit","1"));assertThat(data(first).size()).isEqualTo(1);String cursor=body(first).get("meta").get("nextCursor").stringValue();
        write(post(BASE+"/sources"),create(RELIABILITY,"second-requests",requests("request-jsonl-v2")),"third",null);
        var next=perform(get(BASE+"/sources").param("limit","100").param("cursor",cursor));close();start();assertThat(data(perform(get(BASE+"/sources").param("limit","100").param("cursor",cursor)))).isEqualTo(data(next));assertThat(data(next).size()).isEqualTo(1);assertThat(data(next).get(0).get("productId").stringValue()).isEqualTo(CONTENT);
        code(perform(get(BASE+"/sources").param("cursor",cursor).param("enabled","true")),400,"INVALID_ARGUMENT");
        code(perform(get(BASE+"/sources").param("cursor",cursor+"x")),400,"INVALID_ARGUMENT");
        write(post(PATH+"/revisions"),revise("Two",rss()),"r2","\"0\"");var page=perform(get(PATH+"/revisions").param("limit","1"));String revisionCursor=body(page).get("meta").get("nextCursor").stringValue();
        write(post(PATH+"/revisions"),revise("Three",rss()),"r3","\"1\"");var r2=perform(get(PATH+"/revisions").param("cursor",revisionCursor));assertThat(data(r2).size()).isEqualTo(1);assertThat(data(r2).get(0).get("revision").intValue()).isEqualTo(2);
        code(perform(get(BASE+"/sources/"+RELIABILITY+"/demo-requests/revisions").param("cursor",revisionCursor)),400,"INVALID_ARGUMENT");
    }
    @ParameterizedTest @ValueSource(strings={"0","101","-1","1.0","","0001"})
    void invalidPageBounds(String limit) throws Exception {code(perform(get(BASE+"/sources").param("limit",limit)),400,"INVALID_ARGUMENT");}
    @Test void strictQueryAllowlistsAndMissingSources() throws Exception {
        code(perform(get(BASE+"/sources").param("limit","1","2")),400,"INVALID_ARGUMENT");code(perform(get(BASE+"/sources").param("offset","0")),400,"INVALID_ARGUMENT");
        code(perform(get(BASE+"/sources").param("enabled","yes")),400,"INVALID_ARGUMENT");code(perform(get(PATH)),404,"NOT_FOUND");assertThat(perform(get(BASE+"/runs")).getResponse().getStatus()).isEqualTo(200);
        assertThat(body(perform(get(BASE+"/sources"))).get("meta").get("limit").intValue()).isEqualTo(20);
    }
    @Test void rejectedPrivateFieldsUnknownReferencesAndDuplicateJsonLeaveNoEvidence() throws Exception {
        for(var config:List.of(rss().put("feedEndpointId",UUID.randomUUID().toString()),rss().put("credentials","PRIVATE"),rss().put("inputStorageProfileId",BATCH)))
            code(write(post(BASE+"/sources"),create(CONTENT,"demo-feed",config),"bad",null),400,"INVALID_ARGUMENT");
        code(perform(post(BASE+"/sources").header("Origin",origin).header("X-DDIP-Local-Request","1").header("Idempotency-Key","bad").contentType("application/json").content("{\"productId\":\"x\",\"productId\":\"y\"}")),400,"INVALID_ARGUMENT");
        assertThat(count("sources")).isZero();assertThat(count("command_bindings")).isZero();assertThat(count("audit_events")).isZero();
    }
    @Test void securityBoundaryEnforcesHostOriginHeaderAndJsonWithoutCors() throws Exception {
        var request=create(CONTENT,"demo-feed",rss());
        code(mvc.perform(get(BASE+"/sources").header("Host","evil.example:8080")).andReturn(),403,"LOCAL_BOUNDARY_REJECTED");
        code(perform(get(BASE+"/sources").header("Origin","https://evil.example")),403,"LOCAL_BOUNDARY_REJECTED");
        code(perform(post(BASE+"/sources").contentType("application/json").content(SourceJson.canonical(request))),403,"LOCAL_BOUNDARY_REJECTED");
        code(perform(post(BASE+"/sources").header("Origin",origin).contentType("application/json").content(SourceJson.canonical(request))),403,"LOCAL_BOUNDARY_REJECTED");
        code(perform(post(BASE+"/sources").header("Origin",origin).header("X-DDIP-Local-Request","1").contentType("text/plain").content("PRIVATE")),403,"LOCAL_BOUNDARY_REJECTED");
        var options=perform(options(BASE+"/sources").header("Origin","https://evil.example"));code(options,403,"LOCAL_BOUNDARY_REJECTED");assertThat(options.getResponse().getHeader("Access-Control-Allow-Origin")).isNull();
        assertThat(count("sources")).isZero();
    }
    @Test void viteProxyHostIsAcceptedWithoutEnablingCrossOriginCors() throws Exception {
        var result=mvc.perform(post(BASE+"/sources").header("Host","127.0.0.1:5173").header("Origin","http://127.0.0.1:5173").header("X-DDIP-Local-Request","1").header("Idempotency-Key","proxy").contentType("application/json").content(SourceJson.canonical(create(CONTENT,"demo-feed",rss())))).andReturn();
        assertThat(result.getResponse().getStatus()).isEqualTo(201);assertThat(result.getResponse().getHeader("Access-Control-Allow-Origin")).isNull();
    }
    @Test void realHttpServerExposesSourcesAndPreservesLegacyAnalyticsEnvelope() throws Exception {
        try(var client=HttpClient.newHttpClient()) {
            var response=client.send(HttpRequest.newBuilder(URI.create(origin+BASE+"/sources")).GET().build(),HttpResponse.BodyHandlers.ofString());assertThat(response.statusCode()).isEqualTo(200);
            var legacy=client.send(HttpRequest.newBuilder(URI.create(origin+"/api/v1/publishers")).GET().build(),HttpResponse.BodyHandlers.ofString());assertThat(legacy.statusCode()).isEqualTo(503);assertThat(legacy.body()).isEqualTo("{\"code\":\"PUBLICATION_UNAVAILABLE\"}");
        }
    }
    @Test void databaseOutageReturnsSafeFailuresWithoutPartialMutation() throws Exception {
        admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS false");
        admin.queryForList("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=?",database);
        try {
            var read=perform(get(BASE+"/sources"));code(read,503,"MANAGEMENT_UNAVAILABLE");
            var write=created();code(write,503,"MANAGEMENT_UNAVAILABLE");
            assertThat(read.getResponse().getContentAsString()+write.getResponse().getContentAsString()).doesNotContain(url,password,"PSQLException","SQL","does not accept connections");
        } finally {admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS true");}
        assertThat(count("sources")).isZero();assertThat(count("command_bindings")).isZero();
    }
    @Test void operationScopesAndConditionalContentAreUnambiguous() throws Exception {
        created();
        var revision=write(post(PATH+"/revisions"),revise("Second",rss()),"same-key","\"0\"");assertThat(revision.getResponse().getStatus()).isEqualTo(200);
        var gate=write(put(PATH+"/enabled"),SourceJson.object().put("enabled",false),"same-key","\"1\"");assertThat(gate.getResponse().getStatus()).isEqualTo(200);
        code(write(post(PATH+"/revisions"),revise("Second",rss()),"same-key","\"2\""),409,"IDEMPOTENCY_CONFLICT");
    }
    @Test void approvedStorageStillRequiresTheCorrectPersistedInstallationBinding() throws Exception {
        String absent=UUID.randomUUID().toString();var c=catalog();((tools.jackson.databind.node.ArrayNode)c.get("storageProfiles")).addObject().put("profileId",absent).put("productId",CONTENT).put("role","BRONZE");
        close();Files.write(catalogFile,SourceJson.canonical(c));start();
        code(write(post(BASE+"/sources"),create(CONTENT,"demo-feed",rss().put("inputStorageProfileId",absent)),"missing-binding",null),400,"INVALID_ARGUMENT");
        assertThat(count("sources")).isZero();
    }
    @Test void missingIdempotencyAndDuplicateHeadersFailSafely() throws Exception {
        code(perform(post(BASE+"/sources").header("Origin",origin).header("X-DDIP-Local-Request","1").contentType("application/json").content(SourceJson.canonical(create(CONTENT,"demo-feed",rss())))),400,"INVALID_ARGUMENT");
        code(perform(post(BASE+"/sources").header("Origin",origin).header("X-DDIP-Local-Request","1").header("Idempotency-Key","one","two").contentType("application/json").content(SourceJson.canonical(create(CONTENT,"demo-feed",rss())))),400,"INVALID_ARGUMENT");
        assertThat(count("command_bindings")).isZero();
    }
    @Test void forwardV5UpgradePreservesEvidenceAndCannotRecreateALostCoordinator() {
        close();jdbc.execute("DROP SCHEMA ddip_management CASCADE");
        var old=Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").target("5").load();old.migrate();
        var tx=new ManagementTransactions(new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.support.JdbcTransactionManager(jdbc.getDataSource())));
        // Build the historical V5 fixture using its schema, not a current repository against old tables.
        jdbc.update("INSERT INTO ddip_management.coordinator(singleton_id,installation_id,mode) VALUES (1,?,'READY')",INSTALL);
        jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=1");
        var coordinator=new JdbcCoordinatorRepository(jdbc,tx,INSTALL);
        var before=jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE type='SQL' ORDER BY installed_rank");
        var current=Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        new SchemaLifecycle(current,jdbc,coordinator).verify(true);
        assertThat(coordinator.read().rowVersion()).isEqualTo(1);assertThat(coordinator.read().mode()).isEqualTo(CoordinatorRepository.Mode.STOPPING);
        assertThat(jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE type='SQL' AND version IN ('1','2','3','4','5') ORDER BY installed_rank")).isEqualTo(before);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.flyway_schema_history WHERE type='SQL'",Integer.class)).isEqualTo(8);
        jdbc.execute("DROP SCHEMA ddip_management CASCADE");old.migrate();
        assertThatThrownBy(() -> new SchemaLifecycle(current,jdbc,coordinator).verify(true)).hasMessage("MANAGEMENT_INCOMPATIBLE");
        assertThat(count("coordinator")).isZero();
    }
    @Test void sourceMutationsAndReplayDoNotReleaseOrChangeMaintenanceOwnership() throws Exception {
        var binding=OwnershipFixtures.host(Files.createTempDirectory(temp,"ownership-"));OwnershipFixtures.provision(jdbc,binding);
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var repository=context.getBean(ImportOwnershipRepository.class);
            var owner=repository.reserve(guard,OwnershipFixtures.reservation(UUID.randomUUID()));
            var coordinator=context.getBean(CoordinatorRepository.class);var before=coordinator.read();
            var initial=created();assertThat(initial.getResponse().getStatus()).isEqualTo(201);
            var revised=write(post(PATH+"/revisions"),revise("Second",rss()),"revise","\"0\"");assertThat(revised.getResponse().getStatus()).isEqualTo(200);
            assertThat(data(write(post(BASE+"/sources"),create(CONTENT,"demo-feed",rss()),"create",null))).isEqualTo(data(initial));
            assertThat(coordinator.read()).isEqualTo(before);assertThat(before.activeImportId()).isEqualTo(owner.importId());
            assertThat(count("runs")).isZero();assertThat(count("attempts")).isZero();
            repository.finish(guard,owner,ImportOwnershipRepository.Outcome.SAFE_COMPLETION,OwnershipFixtures.RESULT,true);
        }
    }
    @Test void sourceCapAndHundredRowPageAreEnforcedWithoutPartialRegistration() throws Exception {
        byte[] config=SourceJson.canonical(requests("request-jsonl-v2"));
        var tx=new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.support.JdbcTransactionManager(jdbc.getDataSource()));
        tx.execute(status -> {
            jdbc.update("INSERT INTO ddip_management.sources(product_id,source_id,profile_family,current_revision,enabled) SELECT 'application-reliability','existing-'||lpad(i::text,3,'0'),'request-jsonl-v2',1,true FROM generate_series(0,127) i");
            jdbc.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor) SELECT 'application-reliability','existing-'||lpad(i::text,3,'0'),1,'Reviewed',1,?,?,'local-operator' FROM generate_series(0,127) i",config,SourceJson.sha(config));return null;
        });
        var page=perform(get(BASE+"/sources").param("limit","100"));assertThat(data(page).size()).isEqualTo(100);assertThat(data(page).get(0).get("sourceId").stringValue()).isEqualTo("existing-000");
        var remainder=perform(get(BASE+"/sources").param("limit","100").param("cursor",body(page).get("meta").get("nextCursor").stringValue()));assertThat(data(remainder).size()).isEqualTo(28);assertThat(data(remainder).get(27).get("sourceId").stringValue()).isEqualTo("existing-127");
        code(created(),429,"MANAGEMENT_CAPACITY_EXCEEDED");assertThat(count("sources")).isEqualTo(128);assertThat(count("command_bindings")).isZero();assertThat(count("audit_events")).isZero();
    }
    @Test void thousandRevisionCapStillPermitsAnUnchangedCurrentCommand() throws Exception {
        created();byte[] config=SourceJson.canonical(rss());
        jdbc.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor) SELECT 'content-intelligence','demo-feed',i,'Last',1,?,?,'local-operator' FROM generate_series(2,1000) i",config,SourceJson.sha(config));
        jdbc.update("UPDATE ddip_management.sources SET current_revision=1000,row_version=row_version+1 WHERE source_id='demo-feed'");
        code(write(post(PATH+"/revisions"),revise("Changed",rss()),"over-limit","\"1\""),429,"MANAGEMENT_CAPACITY_EXCEEDED");
        var noop=write(post(PATH+"/revisions"),revise("Last",rss()),"allowed-noop","\"1\"");assertThat(noop.getResponse().getStatus()).isEqualTo(200);assertThat(data(noop).get("currentRevision").intValue()).isEqualTo(1000);assertThat(count("source_revisions")).isEqualTo(1000);
    }
    @Test void malformedStoredConfigurationFailsWithoutLeakingPrivateValues() throws Exception {
        created();byte[] bad=SourceJson.canonical(rss().put("credentials","PRIVATE-STORED-VALUE"));
        jdbc.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor) VALUES ('content-intelligence','demo-feed',2,'Reviewed',1,?,?,'local-operator')",bad,SourceJson.sha(bad));
        jdbc.update("UPDATE ddip_management.sources SET current_revision=2,row_version=row_version+1 WHERE source_id='demo-feed'");
        var result=perform(get(PATH));code(result,500,"INTERNAL_FAILURE");assertThat(result.getResponse().getContentAsString()).doesNotContain("PRIVATE-STORED-VALUE","credentials",url,password);
    }
    @Test void malformedCommandSnapshotCannotBecomeAPublicResponse() throws Exception {
        created();jdbc.execute("ALTER TABLE ddip_management.command_bindings DISABLE TRIGGER immutable_evidence");
        jdbc.update("UPDATE ddip_management.command_bindings SET response_data=convert_to('{\"credentials\":\"PRIVATE-COMMAND-VALUE\"}','UTF8')");
        jdbc.execute("ALTER TABLE ddip_management.command_bindings ENABLE TRIGGER immutable_evidence");
        var result=created();code(result,500,"INTERNAL_FAILURE");assertThat(result.getResponse().getContentAsString()).doesNotContain("PRIVATE-COMMAND-VALUE","credentials");
    }
    @Test void reviewedEndpointIdentityCannotBeReboundToAnotherUri() throws Exception {
        created();close();var changed=catalog();((tools.jackson.databind.node.ObjectNode)changed.get("endpoints").get(0)).put("uri","https://feeds.example.org/changed");Files.write(catalogFile,SourceJson.canonical(changed));start();
        code(write(post(PATH+"/revisions"),revise("Rebound",rss()),"rebind","\"0\""),409,"STATE_CONFLICT");
        assertThat(count("source_revisions")).isEqualTo(1);assertThat(count("command_bindings")).isEqualTo(1);
        assertThat(created().getResponse().getStatus()).isEqualTo(201); // historical replay is not fresh admission
        var corrected=write(post(PATH+"/revisions"),revise("Corrected",rss().put("feedEndpointId",ENDPOINT2)),"new-endpoint","\"0\"");assertThat(corrected.getResponse().getStatus()).isEqualTo(200);
    }
    @Test void auditContainsOnlyFixedLifecycleAndCommandEvidence() throws Exception {
        var first=created();write(put(PATH+"/enabled"),SourceJson.object().put("enabled",false),"disable","\"0\"");
        var rows=jdbc.queryForList("SELECT * FROM ddip_management.audit_events ORDER BY event_seq");
        assertThat(rows).hasSize(2);var created=rows.get(0);assertThat(created.get("actor_scope")).isEqualTo("local-operator");assertThat(created.get("action")).isEqualTo("SOURCE_CREATED");assertThat(created.get("safe_code")).isEqualTo("CREATED");
        assertThat(created.get("before_version")).isNull();assertThat(((Number)created.get("after_version")).longValue()).isZero();
        var details=SourceJson.parse((byte[])created.get("details_bytes"));assertThat(details.propertyNames()).containsExactlyInAnyOrder("commandId","correlationId","changed","previousRevision","resultingRevision");
        assertThat(details.get("correlationId").stringValue()).isEqualTo(body(first).get("meta").get("correlationId").stringValue());
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.command_bindings WHERE response_object_id=?",Integer.class,details.get("commandId").stringValue())).isEqualTo(1);
        assertThat(rows.get(1).get("action")).isEqualTo("SOURCE_DISABLED");assertThat(((Number)rows.get(1).get("after_version")).longValue()).isEqualTo(1);
    }
    @Test void bodyAndActualEncodedResponseBounds() throws Exception {
        code(perform(post(BASE+"/sources").header("Origin",origin).header("X-DDIP-Local-Request","1").header("Idempotency-Key","large").contentType("application/json").content(new byte[65537])),400,"INVALID_ARGUMENT");
        var request=new org.springframework.mock.web.MockHttpServletRequest();request.setAttribute(ManagementBoundary.CORRELATION,UUID.randomUUID().toString());
        assertThatThrownBy(() -> SourceResponses.response(SourceJson.object().put("large","é".repeat(524288)),200,null,null,request)).hasMessage("INTERNAL_FAILURE");
    }
}
