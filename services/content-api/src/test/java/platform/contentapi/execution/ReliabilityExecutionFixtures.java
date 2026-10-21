package platform.contentapi.execution;

import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.*;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.*;
import platform.contentapi.importing.RegisteredImportEvidence;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Shared isolated PostgreSQL/native setup; no inherited test cases. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
abstract class ReliabilityExecutionFixtures {
    static final UUID INSTALL=OwnershipFixtures.INSTALL;
    static final String PROFILE="10000000-0000-4000-8000-000000000002",RUNTIME="reliability-preflight-v1";
    @TempDir Path temp;
    JdbcTemplate admin,jdbc;ManagementTransactions tx;String database,url,user,password;Path repo,python,work;
    JsonNode fixture;InstallationHostGuard.Binding binding;InstallationHostGuard guard;WorkerRepository workers;WorkerRepository.Owner owner;UUID run,checkpoint;
    @BeforeAll void database() {
        repo=Path.of("../..").toAbsolutePath().normalize();python=Path.of(Objects.requireNonNull(System.getenv("DDIP_TEST_PYTHON")));
        String base=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_URL"));user=System.getenv("DDIP_TEST_DB_USER");password=System.getenv("DDIP_TEST_DB_PASSWORD");admin=new JdbcTemplate(new DriverManagerDataSource(base,user,password));database="ddip_c2a_"+UUID.randomUUID().toString().replace("-","");admin.execute("CREATE DATABASE "+database);url=base.substring(0,base.lastIndexOf('/')+1)+database;jdbc=new JdbcTemplate(new DriverManagerDataSource(url,user,password));jdbc.setQueryTimeout(3);tx=transactions(jdbc);
    }
    static ManagementTransactions transactions(JdbcTemplate jdbc){var t=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));t.setTimeout(3);return new ManagementTransactions(t);}
    @AfterAll void cleanup(){if(admin!=null)admin.execute("DROP DATABASE "+database+" WITH (FORCE)");}
    @BeforeEach void setup()throws Exception {
        jdbc.execute("DROP SCHEMA IF EXISTS ddip_management CASCADE");new SchemaLifecycle(Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load(),jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);
        provision("JSONL");
    }
    @AfterEach void stop(){if(guard!=null){guard.close();guard=null;}}
    void provision(String format)throws Exception {provision(format,false);}
    void provision(String format,boolean multi)throws Exception {
        work=Files.createTempDirectory(temp,"case-").toRealPath();var process=new ProcessBuilder(python.toString(),"-B",repo.resolve("processing/tests/reliability_preflight_fixtures.py").toString(),work.toString(),format).redirectError(ProcessBuilder.Redirect.DISCARD);process.environment().put("PYTHONPATH",repo.resolve("processing")+":"+repo.resolve("processing/tests"));
        if(multi)process.command().add("multi");
        var p=process.start();byte[] raw=p.getInputStream().readNBytes(PreflightFiles.LARGE+1);assertThat(p.waitFor(20,TimeUnit.SECONDS)).isTrue();assertThat(p.exitValue()).isZero();fixture=SourceJson.parse(raw,PreflightFiles.LARGE);var frozen=fixture.get("bundle").get("capturedSelection");
        var ep=frozen.get("executionProfile");insertProfile(PROFILE,ep.get("configuration"));insertProfile(RUNTIME,fixture.get("command").get("runtime").get("configuration"));
        for(var b:frozen.get("storageBindings")) {
            byte[] bytes=SourceJson.canonical(b.get("configuration"));jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",b.get("bindingId").stringValue(),b.get("productId").stringValue(),b.get("role").stringValue(),bytes,SourceJson.sha(bytes),INSTALL);
            for(String id:List.of(PROFILE,RUNTIME))jdbc.update("INSERT INTO ddip_management.execution_profile_storage VALUES (?,?)",id,b.get("bindingId").stringValue());
            if(b.get("role").stringValue().equals("CONTROL")){Path file=Files.write(work.resolve("host.json"),bytes);binding=OwnershipFixtures.readBinding(file);}
        }
        var catalog=SourceJson.object().put("catalogVersion",1);catalog.putArray("endpoints");var profiles=catalog.putArray("reliabilityProfiles");var storage=catalog.putArray("storageProfiles");
        for(var s:frozen.get("sources")) {
            var c=s.get("configuration");String source=s.get("sourceId").stringValue();byte[] bytes=SourceJson.canonical(c);
            tx.required(()->{jdbc.queryForList("SELECT singleton_id FROM ddip_management.coordinator FOR UPDATE");jdbc.update("INSERT INTO ddip_management.sources(product_id,source_id,profile_family,current_revision,enabled) VALUES ('application-reliability',?,?,1,true)",source,c.get("profile").stringValue());jdbc.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor) VALUES ('application-reliability',?,1,'fixture',1,?,?,'local-operator')",source,bytes,SourceJson.sha(bytes));return null;});
            profiles.addObject().put("sourceId",source).put("profile",c.get("profile").stringValue()).put("dataOrigin","synthetic").put("registeredProfileSha256",c.get("registeredProfileSha256").stringValue());storage.addObject().put("profileId",c.get("inputStorageProfileId").stringValue()).put("productId","application-reliability").put("role","BATCH");
        }
        var request=SourceJson.object().put("pipeline","RELIABILITY_V2").put("executionProfileId",PROFILE);var selected=request.putArray("sources");for(var s:frozen.get("sources"))selected.addObject().put("sourceId",s.get("sourceId").stringValue()).put("revision",1);var ids=request.putArray("batchArtifactIds");
        for(var input:frozen.get("inputs")) {
            byte[] evidence=SourceJson.canonical(input.get("qualification"));var e=RegisteredImportEvidence.validate(evidence,SourceJson.sha(evidence)).selection();UUID artifact=UUID.fromString(input.get("artifactId").stringValue());
            jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,product_id,kind,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at,native_format) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)",artifact,e.product(),e.kind(),e.contract(),e.processor(),e.nativeIdentity(),e.bindingId(),e.address(),evidence,SourceJson.sha(evidence),Timestamp.from(java.time.Instant.parse(input.get("verifiedAt").stringValue())),e.format());
            if(e.format()!=null)ids.add(artifact.toString());else request.put("serviceReferenceArtifactId",artifact.toString());
        }
        Files.write(work.resolve("source-catalog.json"),SourceJson.canonical(catalog));
        var runs=new RunRepository(jdbc,tx,INSTALL,PreflightCatalogFixture.from(catalog));run=UUID.fromString(runs.submit(RunCommands.parse(SourceJson.canonical(request),"c2a-fixture"),UUID.randomUUID().toString()).get("runId").stringValue());
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);var session=workers.register(guard,PROFILE,ep.get("configurationSha256").stringValue(),ep.get("configuration").get("runtime").get("deploymentSha256").stringValue());owner=workers.claim(guard,session,UUID.randomUUID()).owner();assertThat(owner).isNotNull();checkpoint=UUID.randomUUID();
    }
    void insertProfile(String id,JsonNode config){byte[] bytes=SourceJson.canonical(config);jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,bytes,SourceJson.sha(bytes));}
    ReliabilityPreflight service(){return new ReliabilityPreflight(jdbc,tx,workers);}
    ReliabilityPreflight service(ReadOnlyVerifierProcess process){return new ReliabilityPreflight(jdbc,tx,workers,process);}
    ReliabilityPreflight.Verified verify(){return service().verify(guard,owner,RUNTIME,checkpoint);}
    String state(){return jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run);}
    Map<String,Object> stage(){return jdbc.queryForMap("SELECT * FROM ddip_management.stage_invocations WHERE attempt_id=?",owner.attemptId());}
    Path descriptor(){return binding.root().resolve("runs/"+run+"/attempts/"+owner.attemptId()+"/check-inputs/"+checkpoint+"/command.json");}
    void occupied(){assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_artifacts",Long.class)).isZero();assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run)).isNull();assertThat(state()).isNotEqualTo("SUCCEEDED");}
    void outage(boolean down){admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS "+!down);if(down)admin.queryForList("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=? AND pid<>pg_backend_pid()",database);}
}
