package platform.contentapi.execution;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import javax.sql.DataSource;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.system.OutputCaptureExtension;
import org.springframework.boot.test.system.CapturedOutput;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.WebApplicationType;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import platform.contentapi.*;
import static org.assertj.core.api.Assertions.*;

/** Dedicated service-backed tests. No H2, repository mocks or silently skipped PostgreSQL assertions. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class PersistenceIT {
    private static final UUID INSTALLATION=UUID.fromString("10000000-0000-4000-8000-000000000001");
    private String url,user,password,database;
    private JdbcTemplate admin,jdbc;
    private ManagementTransactions tx;
    private JdbcCoordinatorRepository coordinator;
    private Flyway flyway;
    private SchemaLifecycle schema;
    private static final byte[] OBJECT="{}".getBytes(StandardCharsets.UTF_8);
    private static final String HASH="44136fa355b3678a1146ad16f7e8649e94fb4fc21fe77e8310c060f61caaff8a";

    @BeforeAll void database() {
        String base=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_URL"),"Set DDIP_TEST_DB_URL to a dedicated PostgreSQL 17 test service");
        user=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_USER")); password=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_PASSWORD"));
        admin=new JdbcTemplate(new DriverManagerDataSource(base,user,password));
        database="ddip_b1_"+UUID.randomUUID().toString().replace("-","");
        admin.execute("CREATE DATABASE "+database);
        url=base.substring(0,base.lastIndexOf('/')+1)+database;
        jdbc=new JdbcTemplate(new DriverManagerDataSource(url,user,password)); jdbc.setQueryTimeout(3);
        var template=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())); template.setTimeout(3);
        tx=new ManagementTransactions(template); coordinator=new JdbcCoordinatorRepository(jdbc,tx,INSTALLATION);
    }
    @AfterAll void removeOnlyThisTestDatabase() {
        if(admin!=null && database!=null) admin.execute("DROP DATABASE "+database+" WITH (FORCE)");
    }
    private Flyway migrations() {
        return Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management")
                .locations("classpath:db/migration").cleanDisabled(true).baselineOnMigrate(false).ignoreMigrationPatterns(new String[0]).load();
    }
    @BeforeEach void cleanTestSchema() {
        jdbc.execute("DROP SCHEMA IF EXISTS ddip_management CASCADE");
        flyway=migrations(); schema=new SchemaLifecycle(flyway,jdbc,coordinator);
    }
    private void migrate() { schema.verify(true); }
    private void profile() {
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES ('local',?,?)",OBJECT,HASH);
    }
    private UUID run(String key) {
        UUID id=UUID.randomUUID();
        jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state) " +
                "VALUES (?,'application-reliability','RELIABILITY_V2',1,'local-operator',?,?,?,?,?,'local','QUEUED')",id,key,OBJECT,HASH,OBJECT,HASH);
        return id;
    }
    private void source(String id) {
        tx.required(() -> {
            jdbc.update("INSERT INTO ddip_management.sources(product_id,source_id,profile_family,current_revision,enabled) VALUES ('application-reliability',?,'request-jsonl-v2',1,true)",id);
            jdbc.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor) VALUES ('application-reliability',?,1,'Synthetic',1,?,?,'local-operator')",id,OBJECT,HASH);
            return null;
        });
    }
    @Test void cleanMigrationAndRepeatPreserveEvidence() {
        migrate(); profile();
        var before=jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history ORDER BY installed_rank");
        schema.verify(true); schema.verify(false);
        assertThat(jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history ORDER BY installed_rank")).isEqualTo(before);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.execution_profiles",Integer.class)).isEqualTo(1);
        assertThat(coordinator.read().installationId()).isEqualTo(INSTALLATION);
    }
    @Test void checksumMismatchIsRejectedWithoutRepair() {
        migrate(); jdbc.update("UPDATE ddip_management.flyway_schema_history SET checksum=checksum+1 WHERE version='2'");
        assertThatThrownBy(() -> schema.verify(false)).isInstanceOf(ManagementFailure.class).hasMessage("MANAGEMENT_INCOMPATIBLE");
        assertThatThrownBy(() -> schema.verify(true)).isInstanceOf(ManagementFailure.class).hasMessage("MANAGEMENT_INCOMPATIBLE");
    }
    @Test void futureVersionIsRejected() {
        migrate(); jdbc.update("INSERT INTO ddip_management.flyway_schema_history(installed_rank,version,description,type,script,checksum,installed_by,execution_time,success) VALUES (99,'99','future','SQL','V99__future.sql',1,current_user,0,true)");
        assertThatThrownBy(() -> schema.verify(false)).hasMessage("MANAGEMENT_INCOMPATIBLE");
    }
    @Test void pendingMigrationIsNotAutomaticallyAppliedByApi() {
        Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").target("4").load().migrate();
        assertThatThrownBy(() -> context("api")).hasMessage("MANAGEMENT_INCOMPATIBLE");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.flyway_schema_history WHERE version='5'",Integer.class)).isZero();
    }
    @Test void requiredForeignKeysAndRequestIdempotencyAreEnforced() {
        migrate(); profile(); UUID first=run("same-key");
        assertThatThrownBy(() -> run("same-key")).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("INSERT INTO ddip_management.run_inputs(run_id,role,ordinal,artifact_id,evidence_bytes,evidence_sha256) VALUES (?,'BATCH',0,?,?,?)", first,UUID.randomUUID(),OBJECT,HASH)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("INSERT INTO ddip_management.artifact_dependencies VALUES (?,'BASE',0,?)",UUID.randomUUID(),UUID.randomUUID())).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }
    @Test void revisionBytesAndSourceIdentityAreImmutable() {
        migrate(); source("synthetic");
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.source_revisions SET display_name='changed'")).hasMessageContaining("IMMUTABLE_EVIDENCE");
        assertThatThrownBy(() -> jdbc.update("DELETE FROM ddip_management.source_revisions")).hasMessageContaining("IMMUTABLE_EVIDENCE");
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.sources SET source_id='changed',row_version=row_version+1")).hasMessageContaining("IMMUTABLE_IDENTITY");
        assertThatThrownBy(() -> jdbc.update("TRUNCATE ddip_management.audit_events")).hasMessageContaining("IMMUTABLE_EVIDENCE");
    }
    @Test void badDigestAndOversizedBytesFail() {
        migrate();
        assertThatThrownBy(() -> jdbc.update("INSERT INTO ddip_management.execution_profiles VALUES ('bad',?,?,now())",OBJECT,"0".repeat(64))).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("INSERT INTO ddip_management.execution_profiles VALUES ('big',?,encode(sha256(?),'hex'),now())",new byte[65537],new byte[65537])).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }
    @Test void singletonAndInstallationMismatchCannotReplaceCoordinator() {
        migrate();
        assertThatThrownBy(() -> jdbc.update("INSERT INTO ddip_management.coordinator(singleton_id,installation_id,mode) VALUES (2,?,'READY')",UUID.randomUUID())).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThatThrownBy(() -> new JdbcCoordinatorRepository(jdbc,tx,UUID.randomUUID()).initialize()).hasMessage("MANAGEMENT_INCOMPATIBLE");
        assertThat(coordinator.read().installationId()).isEqualTo(INSTALLATION);
    }
    @Test void concurrentConditionalUpdateHasExactlyOneWinner() throws Exception {
        migrate(); var start=new CountDownLatch(1);
        try(var pool=Executors.newFixedThreadPool(2)) {
            Callable<Boolean> operation=() -> { start.await(); return coordinator.compareAndSetIdleMode(0,CoordinatorRepository.Mode.STOPPING); };
            var a=pool.submit(operation); var b=pool.submit(operation); start.countDown();
            assertThat(List.of(a.get(5,TimeUnit.SECONDS),b.get(5,TimeUnit.SECONDS))).containsExactlyInAnyOrder(true,false);
        }
        assertThat(coordinator.read().rowVersion()).isEqualTo(1);
        assertThat(coordinator.isCurrentOwner(UUID.randomUUID(),UUID.randomUUID(),1)).isFalse();
    }
    @Test void transactionRollsBackRelatedRowsAndDeferredForeignKeys() {
        migrate(); profile();
        assertThatThrownBy(() -> tx.required(() -> { run("rollback"); jdbc.update("INSERT INTO ddip_management.sources VALUES ('application-reliability','orphan','request-jsonl-v2',1,true,0,now(),now())"); return null; })).isInstanceOf(ManagementFailure.class).hasMessage("STATE_CONFLICT");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs",Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.sources",Integer.class)).isZero();
    }
    @ParameterizedTest @ValueSource(strings={"PAUSED","PAUSE_REQUESTED","UNKNOWN"})
    void unsupportedRunStateIsRejected(String state) {
        migrate(); profile(); UUID id=run("state");
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.runs SET state=?,row_version=row_version+1 WHERE run_id=?",state,id)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }
    @ParameterizedTest @ValueSource(strings={"PAUSE","RESUME"})
    void unsupportedControlsAreRejected(String type) {
        migrate(); profile(); UUID id=run("control");
        assertThatThrownBy(() -> jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,?,'key',?,?,0,'REQUESTED','local-operator')",UUID.randomUUID(),id,type,OBJECT,HASH)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }
    @Test void activeOwnershipIsCompositeAndStaleTokensDoNotMatch() {
        migrate(); profile(); UUID run=run("owner"); UUID instance=UUID.randomUUID(); UUID attempt=UUID.randomUUID();
        jdbc.update("INSERT INTO ddip_management.worker_instances(instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,heartbeat_at,status) VALUES (?,?,'local',?,?,1,now(),now(),'READY')",instance,INSTALLATION,HASH,HASH);
        tx.required(() -> {
            jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state) VALUES (?,?,1,'EXECUTE',?,1,'CLAIMED')",attempt,run,instance);
            jdbc.update("UPDATE ddip_management.runs SET state='RUNNING',current_attempt_id=?,row_version=row_version+1 WHERE run_id=?",attempt,run);
            jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=?,owner_instance_id=?,epoch=1,heartbeat_at=now(),row_version=row_version+1",attempt,instance);
            return null;
        });
        assertThat(coordinator.isCurrentOwner(attempt,instance,1)).isTrue();
        assertThat(coordinator.isCurrentOwner(attempt,instance,0)).isFalse();
        assertThat(coordinator.isCurrentOwner(attempt,UUID.randomUUID(),1)).isFalse();
        assertThat(coordinator.compareAndSetIdleMode(1,CoordinatorRepository.Mode.READY)).isFalse();
        assertThatThrownBy(() -> tx.required(() -> { jdbc.update("UPDATE ddip_management.coordinator SET epoch=2,row_version=row_version+1"); return null; })).isInstanceOf(ManagementFailure.class);
        assertThat(coordinator.read().epoch()).isEqualTo(1);
        UUID second=run("second");
        assertThatThrownBy(() -> jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state) VALUES (?,?,1,'EXECUTE',?,2,'CLAIMED')",UUID.randomUUID(),second,instance)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.runs SET state='RECOVERY_REQUIRED',current_attempt_id=?,row_version=row_version+1 WHERE run_id=?",attempt,second)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
    }
    @Test void unsafePrivateAddressesFailDatabaseAdmission() {
        migrate();
        for(String address:List.of("../outside","/absolute","C:/outside","a/../b","a//b","a\\b")) {
            assertThatThrownBy(() -> jdbc.queryForObject("SELECT CAST(? AS ddip_management.relative_address)",String.class,address))
                    .isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        }
        assertThat(jdbc.queryForObject("SELECT CAST(? AS ddip_management.relative_address)",String.class,"v2/processor_revision=2/publication=abc/complete.json")).contains("complete.json");
    }
    @Test void terminalSuccessRequiresArtifactAndVersionsCannotBeSkipped() {
        migrate(); profile(); UUID run=run("success");
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.runs SET state='SUCCEEDED',row_version=row_version+1 WHERE run_id=?",run)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING'")).hasMessageContaining("ROW_VERSION_REQUIRED");
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.runs SET normalized_request=convert_to('{\"changed\":true}','UTF8'),row_version=row_version+1 WHERE run_id=?",run)).hasMessageContaining("IMMUTABLE_IDENTITY");
    }
    @Test void runtimeConnectionsHaveBoundedUtcConfiguration() {
        migrate();
        try(var context=context("api")) {
            var pool=context.getBean(com.zaxxer.hikari.HikariDataSource.class);
            assertThat(pool.getMaximumPoolSize()).isEqualTo(4);
            assertThat(pool.getConnectionTimeout()).isEqualTo(3000);
            var connected=context.getBean(JdbcTemplate.class);
            assertThat(connected.queryForObject("SHOW timezone",String.class)).isEqualTo("UTC");
            assertThat(connected.queryForObject("SHOW statement_timeout",String.class)).isEqualTo("3s");
            assertThat(connected.queryForObject("SHOW lock_timeout",String.class)).isEqualTo("3s");
        }
    }
    @Test void migrationRoleIsExplicitNonWebAndDoesNotStartQueryOrWorkerBeans() {
        try(var context=context("migrate")) {
            assertThat(context.getBeansOfType(DataSource.class)).hasSize(1);
            assertThat(context.getBeansOfType(AnalyticsController.class)).isEmpty();
            assertThat(context.getBeansOfType(BoundedProcessRunner.class)).isEmpty();
            assertThat(context.getBeansOfType(WorkerIdleLifecycle.class)).isEmpty();
            assertThat(context).isNotInstanceOf(org.springframework.boot.web.server.context.WebServerApplicationContext.class);
        }
        schema.verify(false);
    }
    @Test void workerWithoutApprovedProfileAndGuardFailsClosed() {
        migrate();
        assertThatThrownBy(()->context("worker","--spring.main.web-application-type=servlet"))
                .hasRootCauseMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Integer.class)).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Integer.class)).isZero();
    }
    @Test void workerMissingRuntimePathsFailsClosed() {
        migrate(); assertThatThrownBy(() -> context("worker","--ddip.worker.python=")).hasMessage("MANAGEMENT_CONFIGURATION_INVALID");
    }
    @Test void applicationRestartsRetainStateWithoutMigrations() {
        migrate(); profile();
        try(var context=context("api")) {
            assertThat(context.getBean(CoordinatorRepository.class).compareAndSetIdleMode(0,CoordinatorRepository.Mode.STOPPING)).isTrue();
        }
        try(var context=context("api")) {
            assertThat(context.getBean(CoordinatorRepository.class).read().mode()).isEqualTo(CoordinatorRepository.Mode.STOPPING);
            assertThat(context.getBeansOfType(AnalyticsController.class)).hasSize(1);
        }
    }
    @Test void unknownDatabaseHasNoAutomaticSchemaCreation() {
        assertThatThrownBy(() -> context("api")).hasMessage("MANAGEMENT_INCOMPATIBLE");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM pg_namespace WHERE nspname='ddip_management'",Integer.class)).isZero();
    }
    @Test void missingOperationalTableIsRejectedWithoutRecreation() {
        migrate(); jdbc.execute("DROP TABLE ddip_management.stage_artifacts");
        assertThatThrownBy(() -> context("api")).hasMessage("MANAGEMENT_INCOMPATIBLE");
        assertThat(jdbc.queryForObject("SELECT to_regclass('ddip_management.stage_artifacts')",String.class)).isNull();
    }
    @Test void missingCoordinatorIsNotReinitializedOnRepeatedMigration() {
        migrate();
        // A test administrator simulates lost/corrupted singleton evidence. Ordinary DELETE is forbidden.
        jdbc.execute("ALTER TABLE ddip_management.coordinator DISABLE TRIGGER coordinator_guard");
        jdbc.update("DELETE FROM ddip_management.coordinator");
        jdbc.execute("ALTER TABLE ddip_management.coordinator ENABLE TRIGGER coordinator_guard");
        assertThatThrownBy(() -> schema.verify(true)).hasMessage("MANAGEMENT_INCOMPATIBLE");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.coordinator",Integer.class)).isZero();
    }
    @Test void terminalAttemptAndAuditEvidenceCannotBeChanged() {
        migrate(); profile(); UUID run=run("terminal"); UUID instance=UUID.randomUUID(); UUID attempt=UUID.randomUUID();
        jdbc.update("INSERT INTO ddip_management.worker_instances(instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,heartbeat_at,status) VALUES (?,?,'local',?,?,1,now(),now(),'READY')",instance,INSTALLATION,HASH,HASH);
        jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at) VALUES (?,?,1,'EXECUTE',?,1,'INTERRUPTED',now())",attempt,run,instance);
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.attempts SET state='SUCCEEDED',row_version=row_version+1 WHERE attempt_id=?",attempt)).hasMessageContaining("TERMINAL_OUTCOME_IMMUTABLE");
        jdbc.update("INSERT INTO ddip_management.audit_events(actor_scope,run_id,action,details_bytes) VALUES ('local-operator',?,'B1_TEST',?)",run,OBJECT);
        assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.audit_events SET action='CHANGED'")).hasMessageContaining("IMMUTABLE_EVIDENCE");
        assertThatThrownBy(() -> jdbc.update("DELETE FROM ddip_management.audit_events")).hasMessageContaining("IMMUTABLE_EVIDENCE");
    }
    @Test @ExtendWith(OutputCaptureExtension.class)
    void closedPortReturnsFixedUnavailableWithoutDriverSecrets(CapturedOutput output) {
        assertThatThrownBy(() -> context("api","--ddip.database.url=jdbc:postgresql://127.0.0.1:1/unavailable","--logging.level.root=WARN"))
                .hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();
        assertThat(output.getAll()).doesNotContain(password,"jdbc:postgresql:","PSQLException","Connection refused");
    }
    private ConfigurableApplicationContext context(String role,String... extra) {
        var args=new ArrayList<>(List.of("--ddip.role="+role,"--ddip.database.url="+url,"--ddip.database.username="+user,
                "--ddip.database.password="+password,"--ddip.installation-id="+INSTALLATION,"--spring.main.banner-mode=off","--logging.level.root=OFF",
                "--ddip.worker.python="+Path.of("../../outputs/silver-python312/.venv/bin/python").toAbsolutePath().normalize(),
                "--ddip.worker.java-home="+System.getProperty("java.home"),"--ddip.worker.processing-root="+Path.of("../../processing").toAbsolutePath().normalize()));
        // CI supplies its already installed Python, never installs or probes another runtime here.
        String python=System.getenv("DDIP_TEST_PYTHON"); if(python!=null) { args.removeIf(arg -> arg.startsWith("--ddip.worker.python=")); args.add("--ddip.worker.python="+python); }
        for(String replacement:extra) {
            String prefix=replacement.substring(0,replacement.indexOf('=')+1);
            args.removeIf(arg -> arg.startsWith(prefix)); args.add(replacement);
        }
        return new SpringApplicationBuilder(ContentApiApplication.class).web(WebApplicationType.NONE).run(args.toArray(String[]::new));
    }
}
