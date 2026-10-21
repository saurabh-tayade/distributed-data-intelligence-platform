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
abstract class RunApiFixtures {

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
    void start(){start(new Class<?>[0]);}
    void start(Class<?>... extra){if(context!=null)return;context=new SpringApplicationBuilder(ContentApiApplication.class).sources(extra).run("--ddip.role=api","--server.address=127.0.0.1","--server.port="+port,"--ddip.database.url="+url,"--ddip.database.username="+user,"--ddip.database.password="+password,"--ddip.installation-id="+INSTALL,"--ddip.management.catalog="+catalogFile,"--ddip.management.cursor-key="+KEY,"--ddip.management.origins="+origin,"--logging.level.root=OFF","--spring.main.banner-mode=off");mvc=MockMvcBuilders.webAppContextSetup((WebApplicationContext)context).addFilters(context.getBean(ManagementBoundary.class)).build();}
    void close(){if(context!=null){context.close();context=null;}}
    @AfterEach void stop(){close();}
    MvcResult http(MockHttpServletRequestBuilder b)throws Exception{start();return mvc.perform(b.header("Host",host)).andReturn();}
    MockHttpServletRequestBuilder postRun(JsonNode n,String key){var b=post(RUNS).contentType("application/json").header("Origin",origin).header("X-DDIP-Local-Request","1").content(SourceJson.canonical(n));if(key!=null)b.header("Idempotency-Key",key);return b;}
    JsonNode body(MvcResult r){return SourceJson.parse(r.getResponse().getContentAsByteArray());}
    void code(MvcResult r,int status,String code){assertThat(r.getResponse().getStatus()).isEqualTo(status);assertThat(body(r).get("code").stringValue()).isEqualTo(code);assertThat(new String(r.getResponse().getContentAsByteArray(),java.nio.charset.StandardCharsets.UTF_8)).doesNotContain(password,url,"/private/","metadata.json","feeds.example", "PRIVATE");}
}
