package platform.contentapi.execution;

import java.io.*;
import java.nio.file.*;
import java.sql.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import platform.contentapi.management.SourceJson;
import static platform.contentapi.execution.OwnershipFixtures.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL 17 transactions/processes. All databases and native-free evidence are synthetic. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ImportOwnershipIT {
    @TempDir Path temp;
    JdbcTemplate admin,jdbc;String database,url,user,password;
    ManagementTransactions tx;JdbcCoordinatorRepository coordinator;ImportOwnershipRepository repository;
    InstallationHostGuard.Binding binding;Path descriptor;
    List<Process> children=new ArrayList<>();
    @BeforeAll void database() {
        String base=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_URL"),"Dedicated PostgreSQL 17 test service required");
        user=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_USER"));password=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_PASSWORD"));
        admin=new JdbcTemplate(new DriverManagerDataSource(base,user,password));database="ddip_b2b1a_"+UUID.randomUUID().toString().replace("-","");
        admin.execute("CREATE DATABASE "+database);url=base.substring(0,base.lastIndexOf('/')+1)+database;
        jdbc=new JdbcTemplate(new DriverManagerDataSource(url,user,password));jdbc.setQueryTimeout(3);
        var template=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));template.setTimeout(3);
        tx=new ManagementTransactions(template);coordinator=new JdbcCoordinatorRepository(jdbc,tx,INSTALL);repository=new ImportOwnershipRepository(jdbc,tx,INSTALL);
    }
    @AfterAll void dropOnlyTestDatabase() {
        if(admin!=null && database!=null)admin.execute("DROP DATABASE "+database+" WITH (FORCE)");
    }
    Flyway flyway(String version) {
        return Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management")
                .cleanDisabled(true).ignoreMigrationPatterns(new String[0]).target(version).load();
    }
    void reset(String version) {
        jdbc.execute("DROP SCHEMA IF EXISTS ddip_management CASCADE");flyway(version).migrate();
        jdbc.update("INSERT INTO ddip_management.coordinator(singleton_id,installation_id,mode) VALUES (1,?,'READY')",INSTALL);
    }
    @BeforeEach void setup() throws Exception {
        reset("8");binding=host(Files.createTempDirectory(temp,"host-"));provision(jdbc,binding);
        descriptor=Files.write(temp.resolve(UUID.randomUUID()+".json"),binding.descriptor());
    }
    @AfterEach void cleanupChildren() throws Exception {
        for(Process p:children) {p.destroyForcibly();assertThat(p.waitFor(5,TimeUnit.SECONDS)).isTrue();}children.clear();
    }
    long count(String table) {return jdbc.queryForObject("SELECT count(*) FROM ddip_management."+table,Long.class);}
    ImportOwnershipRepository.Operation lookup(UUID id) {return repository.lookup(id,REQUEST).orElseThrow();}
    ImportOwnershipRepository.Token token(UUID id) {
        var op=lookup(id);return new ImportOwnershipRepository.Token(id,op.processInstanceId(),op.epoch(),op.rowVersion(),coordinator.read().rowVersion());
    }
    ImportOwnershipRepository.Token reserve(InstallationHostGuard guard) {return repository.reserve(guard,reservation(UUID.randomUUID()));}
    void finish(InstallationHostGuard guard,ImportOwnershipRepository.Token token) {repository.finish(guard,token,ImportOwnershipRepository.Outcome.SAFE_COMPLETION,RESULT,true);}
    void fixedConflict(org.assertj.core.api.ThrowableAssert.ThrowingCallable action,String code) {assertThatThrownBy(action).hasMessage(code).hasNoCause();}
    Process child(String mode,UUID id,UUID predecessor) throws Exception {
        var builder=OwnershipFixtures.child(descriptor,mode,id);builder.environment().put("DDIP_CHILD_DB_URL",url);
        if(predecessor!=null)builder.environment().put("DDIP_CHILD_PREDECESSOR",predecessor.toString());
        Process p=builder.start();children.add(p);
        var line=CompletableFuture.supplyAsync(() -> {try {
            var reader=new BufferedReader(new InputStreamReader(p.getInputStream()));String first=reader.readLine();
            if(mode.equals("released"))assertThat(reader.readLine()).isEqualTo("GUARD_RELEASED");return first;
        } catch(IOException ex){throw new UncheckedIOException(ex);}});
        assertThat(line.get(10,TimeUnit.SECONDS)).isEqualTo("ACQUIRED");return p;
    }
    void stop(Process p) throws Exception {p.destroyForcibly();assertThat(p.waitFor(5,TimeUnit.SECONDS)).isTrue();}
    UUID run() {
        UUID id=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state) " +
                "VALUES (?,'content-intelligence','CONTENT_RSS_V1',1,'local-operator',?,?,?,?,?,'import-test','QUEUED')",id,id.toString(),OBJECT,HASH,OBJECT,HASH);return id;
    }
    UUID worker() {
        UUID id=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.worker_instances(instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,heartbeat_at,status) " +
                "VALUES (?,?,'import-test',?,?,1,now(),now(),'READY')",id,INSTALL,HASH,binding.digest());return id;
    }
    UUID attempt(UUID run,UUID worker,String state,long epoch) {
        UUID id=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at) VALUES (?,?,1,'EXECUTE',?,?,?,"+
                (state.equals("CLAIMED")?"NULL":"clock_timestamp()")+")",id,run,worker,epoch,state);return id;
    }
    void execution(String state) {
        tx.required(() -> {
            UUID run=run(),worker=worker();long epoch=coordinator.read().epoch()+1;
            UUID attempt=attempt(run,worker,state.equals("RECOVERY_REQUIRED")?"INTERRUPTED":"CLAIMED",epoch);
            jdbc.update("UPDATE ddip_management.runs SET state=?,current_attempt_id=?,row_version=row_version+1 WHERE run_id=?",state,attempt,run);
            jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=?,owner_instance_id=?,epoch=?,heartbeat_at=clock_timestamp()-interval '1 day',row_version=row_version+1",attempt,worker,epoch);
            return null;
        });
    }
    void artifact(String product,String kind,int contract,int processor,String format,String identity,String address,boolean v7) {
        String storage="s-"+product;
        jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,'GOLD',?,?,?) ON CONFLICT DO NOTHING",storage,product,OBJECT,HASH,INSTALL);
        jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at"+(v7?",native_format":"")+") VALUES (?,?,?,?,?,?,?,?,?,?,clock_timestamp()"+(v7?",?":"")+")",
                v7?new Object[]{UUID.randomUUID(),kind,product,contract,processor,identity,storage,address,OBJECT,HASH,format}:
                        new Object[]{UUID.randomUUID(),kind,product,contract,processor,identity,storage,address,OBJECT,HASH});
    }
    @Test void cleanMigrationAndRepeatedValidationRecognizeEightVersionsAndOneNewTable() {
        var schema=new SchemaLifecycle(flyway("8"),jdbc,coordinator);schema.verify(false);schema.verify(true);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.flyway_schema_history WHERE success AND version IS NOT NULL",Integer.class)).isEqualTo(8);
        assertThat(count("artifact_import_operations")).isZero();assertThat(coordinator.read().activeImportId()).isNull();
    }
    @Test void populatedV6UpgradePreservesCoordinatorAndOldChecksumsAndEvidence() {
        reset("6");provision(jdbc,binding);
        tx.required(() -> {
            jdbc.update("INSERT INTO ddip_management.sources(product_id,source_id,profile_family,current_revision,enabled) VALUES ('content-intelligence','old','rss-atom-v1',1,true)");
            jdbc.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor) VALUES ('content-intelligence','old',1,'Old',1,?,?,'local-operator')",OBJECT,HASH);return null;
        });
        artifact("content-intelligence","GOLD",1,1,null,"native","old-gold",false);
        jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',epoch=8,row_version=1");
        var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");
        var hashes=jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IS NOT NULL ORDER BY installed_rank");
        var artifact=jdbc.queryForList("SELECT artifact_id,evidence_bytes,evidence_sha256,verified_at FROM ddip_management.artifacts");
        new SchemaLifecycle(flyway("8"),jdbc,coordinator).verify(true);
        var after=jdbc.queryForList("SELECT singleton_id,installation_id,mode,epoch,active_attempt_id,owner_instance_id,heartbeat_at,row_version FROM ddip_management.coordinator");
        assertThat(after).isEqualTo(before);
        assertThat(jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IN ('1','2','3','4','5','6') ORDER BY installed_rank")).isEqualTo(hashes);
        assertThat(jdbc.queryForList("SELECT artifact_id,evidence_bytes,evidence_sha256,verified_at FROM ddip_management.artifacts")).usingRecursiveComparison().isEqualTo(artifact);
        assertThat(count("sources")).isEqualTo(1);assertThat(coordinator.read().mode()).isEqualTo(CoordinatorRepository.Mode.STOPPING);
    }
    @Test void nativeIdentityConflictAbortsV7WithoutPartialSchemaOrDeletedRows() {
        reset("6");artifact("content-intelligence","GOLD",1,1,null,"same","one",false);artifact("content-intelligence","GOLD",1,1,null,"same","two",false);
        assertThatThrownBy(() -> flyway("7").migrate()).hasMessageContaining("ARTIFACT_NATIVE_IDENTITY_CONFLICT");
        assertThat(count("artifacts")).isEqualTo(2);assertThat(jdbc.queryForObject("SELECT to_regclass('ddip_management.artifact_import_operations')",String.class)).isNull();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM information_schema.columns WHERE table_schema='ddip_management' AND table_name='artifacts' AND column_name='native_format'",Integer.class)).isZero();
        assertThat(flyway("6").info().current().getVersion().toString()).isEqualTo("6");
    }
    @Test void unresolvedLegacyReliabilityFormatAbortsRatherThanGuessing() {
        reset("6");artifact("application-reliability","RELIABILITY_BATCH_V2",2,2,null,"same","one",false);
        assertThatThrownBy(() -> flyway("7").migrate()).hasMessageContaining("ARTIFACT_NATIVE_FORMAT_UNRESOLVED");
        assertThat(count("artifacts")).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT to_regclass('ddip_management.artifact_import_operations')",String.class)).isNull();
    }
    @Test void legitimateVersionedLegacyIdentitiesCanUpgrade() {
        reset("6");artifact("content-intelligence","GOLD",1,1,null,"same","one",false);artifact("content-intelligence","GOLD",2,1,null,"same","two",false);
        flyway("7").migrate();assertThat(count("artifacts")).isEqualTo(2);
    }
    @Test void upgradeNeverReinitializesAMissingOldCoordinator() {
        reset("6");jdbc.execute("ALTER TABLE ddip_management.coordinator DISABLE TRIGGER coordinator_guard");jdbc.update("DELETE FROM ddip_management.coordinator");
        jdbc.execute("ALTER TABLE ddip_management.coordinator ENABLE TRIGGER coordinator_guard");
        fixedConflict(() -> new SchemaLifecycle(flyway("7"),jdbc,coordinator).verify(true),"MANAGEMENT_INCOMPATIBLE");assertThat(count("coordinator")).isZero();
    }
    @Test void V6OccupiedRecoveryOwnerSurvivesUpgradeUnchanged() {
        reset("6");provision(jdbc,binding);
        UUID run=run(),worker=worker(),attempt=UUID.randomUUID();
        tx.required(() -> {
            jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at) VALUES (?,?,1,'EXECUTE',?,4,'INTERRUPTED',now())",attempt,run,worker);
            jdbc.update("UPDATE ddip_management.runs SET state='RECOVERY_REQUIRED',current_attempt_id=?,row_version=1 WHERE run_id=?",attempt,run);
            jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=?,owner_instance_id=?,mode='RECOVERY_ONLY',epoch=4,heartbeat_at=now()-interval '1 day',row_version=1",attempt,worker);return null;
        });
        var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");flyway("7").migrate();
        assertThat(jdbc.queryForList("SELECT singleton_id,installation_id,mode,epoch,active_attempt_id,owner_instance_id,heartbeat_at,row_version FROM ddip_management.coordinator")).isEqualTo(before);
        try(var guard=InstallationHostGuard.acquire(binding)) {fixedConflict(() -> reserve(guard),"OWNERSHIP_STALE");}
        assertThat(coordinator.isCurrentOwner(attempt,worker,4)).isTrue();
    }
    @Test void concurrentAttemptAndImportAdmissionCannotBothCommit() throws Exception {
        UUID run=run(),worker=worker();
        try(var guard=InstallationHostGuard.acquire(binding);var pool=Executors.newFixedThreadPool(2)) {
            var start=new CountDownLatch(1);
            var importer=pool.submit(() -> {start.await();try {reserve(guard);return true;}catch(ImportOwnershipFailure e){assertThat(e).hasMessage("OWNERSHIP_STALE");return false;}});
            // Direct SQL intentionally omits the pointer claim to exercise the database guard itself.
            var executor=pool.submit(() -> {start.await();try {attempt(run,worker,"CLAIMED",1);return true;}catch(org.springframework.dao.DataIntegrityViolationException e){assertThat(e).hasMessageContaining("IMPORT_EXECUTION_EXCLUSION");return false;}});
            start.countDown();assertThat(List.of(importer.get(10,TimeUnit.SECONDS),executor.get(10,TimeUnit.SECONDS))).containsExactlyInAnyOrder(true,false);
            assertThat(count("artifact_import_operations")+count("attempts")).isEqualTo(1);
        }
    }
    @Test void reservationAndFinalizationPersistOnlyCoordinationEvidenceAndSafeAudit() {
        UUID id;
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var owner=reserve(guard);id=owner.importId();var slot=coordinator.read();
            assertThat(slot.activeImportId()).isEqualTo(id);assertThat(slot.activeAttemptId()).isNull();assertThat(slot.mode()).isEqualTo(CoordinatorRepository.Mode.READY);
            assertThat(slot.epoch()).isEqualTo(1);assertThat(lookup(id).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);
            finish(guard,owner);assertThat(coordinator.read().activeImportId()).isNull();assertThat(coordinator.read().epoch()).isEqualTo(1);
            assertThat(lookup(id).state()).isEqualTo(ImportOwnershipRepository.State.SUCCEEDED);assertThat(lookup(id).resultBytes()).isEqualTo(RESULT);
        }
        assertThat(count("runs")).isZero();assertThat(count("attempts")).isZero();assertThat(count("artifacts")).isZero();assertThat(count("artifact_dependencies")).isZero();
        assertThat(count("audit_events")).isEqualTo(2);
        var audit=jdbc.queryForList("SELECT action,safe_code,details_bytes FROM ddip_management.audit_events ORDER BY event_seq");
        assertThat(audit.stream().map(m -> m.get("action"))).containsExactly("IMPORT_RESERVED","IMPORT_FINISHED");
        assertThat(new String((byte[])audit.getFirst().get("details_bytes"))).isEqualTo("{\"protocolVersion\":1}");
    }
    @Test void knownFailureCanReleaseButUncertainCleanupCannot() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var owner=reserve(guard);
            fixedConflict(() -> repository.finish(guard,owner,ImportOwnershipRepository.Outcome.SAFE_FAILURE,RESULT,false),"QUIESCENCE_REQUIRED");
            assertThat(coordinator.read().activeImportId()).isEqualTo(owner.importId());
            repository.finish(guard,owner,ImportOwnershipRepository.Outcome.SAFE_FAILURE,RESULT,true);
            assertThat(lookup(owner.importId()).state()).isEqualTo(ImportOwnershipRepository.State.FAILED);assertThat(coordinator.read().activeImportId()).isNull();
        }
    }
    @Test void doubleAcquisitionRejectsWithoutAnotherIntentOrAudit() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var owner=reserve(guard);fixedConflict(() -> reserve(guard),"OWNERSHIP_STALE");
            assertThat(count("artifact_import_operations")).isEqualTo(1);assertThat(count("audit_events")).isEqualTo(1);assertThat(coordinator.read().activeImportId()).isEqualTo(owner.importId());
        }
    }
    @Test void concurrentReservationHasExactlyOneWinner() throws Exception {
        try(var guard=InstallationHostGuard.acquire(binding);var pool=Executors.newFixedThreadPool(2)) {
            var start=new CountDownLatch(1);Callable<Boolean> call=() -> {start.await();try {reserve(guard);return true;}catch(ImportOwnershipFailure e){assertThat(e).hasMessage("OWNERSHIP_STALE");return false;}};
            var a=pool.submit(call);var b=pool.submit(call);start.countDown();assertThat(List.of(a.get(10,TimeUnit.SECONDS),b.get(10,TimeUnit.SECONDS))).containsExactlyInAnyOrder(true,false);
            assertThat(count("artifact_import_operations")).isEqualTo(1);assertThat(count("audit_events")).isEqualTo(1);
        }
    }
    @ParameterizedTest @ValueSource(strings={"RUNNING","CANCEL_REQUESTED","RECOVERY_REQUIRED"})
    void occupiedExecutionOrRecoveryBlocksImportDespiteOldHeartbeat(String state) {
        execution(state);var original=coordinator.read();
        try(var guard=InstallationHostGuard.acquire(binding)) {fixedConflict(() -> reserve(guard),"OWNERSHIP_STALE");}
        assertThat(coordinator.read()).isEqualTo(original);assertThat(count("artifact_import_operations")).isZero();
    }
    @Test void importBlocksDirectAttemptAndRunAdmissionInBothTables() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            reserve(guard);UUID run=run(),worker=worker();
            assertThatThrownBy(() -> attempt(run,worker,"CLAIMED",2)).hasMessageContaining("IMPORT_EXECUTION_EXCLUSION");
            UUID previous=attempt(run,worker,"FAILED",2);
            assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.runs SET state='RUNNING',current_attempt_id=?,row_version=row_version+1 WHERE run_id=?",previous,run)).hasMessageContaining("IMPORT_EXECUTION_EXCLUSION");
            assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("QUEUED");
        }
    }
    @Test void idleModeCASAndDirectPointerClearCannotReleaseActiveImport() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var owner=reserve(guard);assertThat(coordinator.compareAndSetIdleMode(owner.coordinatorVersion(),CoordinatorRepository.Mode.STOPPING)).isFalse();
            assertThatThrownBy(() -> tx.required(() -> {jdbc.update("UPDATE ddip_management.coordinator SET active_import_id=NULL,row_version=row_version+1");return null;})).hasMessage("STATE_CONFLICT");
            assertThat(coordinator.read().activeImportId()).isEqualTo(owner.importId());assertThat(coordinator.read().mode()).isEqualTo(CoordinatorRepository.Mode.READY);
        }
    }
    @Test void mutableIdentityAndInvalidStatesAreRejectedByPostgreSQL() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var owner=reserve(guard);
            for(String assignment:List.of("owner_epoch=owner_epoch+1","owner_pid=owner_pid+1","process_instance_id=gen_random_uuid()","request_bytes=convert_to('{}','UTF8'),request_sha256=encode(sha256(convert_to('{}','UTF8')),'hex')"))
                assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.artifact_import_operations SET "+assignment+",row_version=row_version+1")).hasMessageContaining("IMMUTABLE_IDENTITY");
            assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.artifact_import_operations SET row_version=row_version+1")).hasMessageContaining("IMPORT_TRANSITION_INVALID");
            assertThatThrownBy(() -> jdbc.update("DELETE FROM ddip_management.artifact_import_operations")).hasMessageContaining("HISTORY_DELETE_FORBIDDEN");
            assertThatThrownBy(() -> jdbc.execute("TRUNCATE ddip_management.artifact_import_operations CASCADE")).hasMessageContaining("IMMUTABLE_EVIDENCE");
            finish(guard,owner);
            assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.artifact_import_operations SET state='ACTIVE',finished_at=NULL,outcome_code=NULL,result_bytes=NULL,result_sha256=NULL,row_version=row_version+1")).hasMessageContaining("IMPORT_TRANSITION_INVALID");
        }
    }
    @Test void resultDigestVersionsAndOutcomeKindsAreProtectedIndependently() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var owner=reserve(guard);
            fixedConflict(() -> repository.finish(guard,owner,ImportOwnershipRepository.Outcome.RECOVERY_CLOSED,RESULT,true),"INVALID_REQUEST");
            assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.artifact_import_operations SET state='SUCCEEDED',outcome_code='SAFE_COMPLETION',result_bytes=?,result_sha256=?,finished_at=clock_timestamp(),row_version=2",RESULT,SourceJson.sha(RESULT))).hasMessageContaining("ROW_VERSION_REQUIRED");
            assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.artifact_import_operations SET state='SUCCEEDED',outcome_code='SAFE_COMPLETION',result_bytes=?,result_sha256=?,finished_at=clock_timestamp(),row_version=1",RESULT,"0".repeat(64))).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
            assertThatThrownBy(() -> jdbc.update("UPDATE ddip_management.artifact_import_operations SET state='PAUSED',row_version=1")).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
            assertThat(lookup(owner.importId()).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(lookup(owner.importId()).resultBytes()).isNull();
        }
    }
    @Test void exhaustedEpochFailsClosedWithoutOverflowOrReservation() {
        jdbc.update("UPDATE ddip_management.coordinator SET epoch=?,row_version=1",Long.MAX_VALUE);
        try(var guard=InstallationHostGuard.acquire(binding)) {fixedConflict(() -> reserve(guard),"OWNERSHIP_STALE");}
        assertThat(count("artifact_import_operations")).isZero();assertThat(coordinator.read().epoch()).isEqualTo(Long.MAX_VALUE);
    }
    @ParameterizedTest @ValueSource(ints={0,1,2,3,4})
    void staleOwnerEpochProcessOrVersionCannotFinalize(int variant) {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var t=reserve(guard);
            var stale=new ImportOwnershipRepository.Token(variant==4?UUID.randomUUID():t.importId(),variant==3?UUID.randomUUID():t.processInstanceId(),t.epoch()-(variant==0?1:0),t.operationVersion()+(variant==1?1:0),t.coordinatorVersion()+(variant==2?1:0));
            fixedConflict(() -> finish(guard,stale),"OWNERSHIP_STALE");assertThat(lookup(t.importId()).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(count("audit_events")).isEqualTo(1);
        }
    }
    @Test void reservationAuditFailureRollsBackIntentEpochAndPointer() {
        jdbc.execute("ALTER TABLE ddip_management.audit_events ADD CONSTRAINT injected_audit CHECK(action<>'IMPORT_RESERVED')");
        try(var guard=InstallationHostGuard.acquire(binding)) {fixedConflict(() -> reserve(guard),"STATE_CONFLICT");}
        assertThat(count("artifact_import_operations")).isZero();assertThat(count("audit_events")).isZero();assertThat(coordinator.read().epoch()).isZero();assertThat(coordinator.read().activeImportId()).isNull();
    }
    @Test void finalizationAuditFailureRollsBackResultAndRelease() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var owner=reserve(guard);jdbc.execute("ALTER TABLE ddip_management.audit_events ADD CONSTRAINT injected_audit CHECK(action<>'IMPORT_FINISHED')");
            fixedConflict(() -> finish(guard,owner),"STATE_CONFLICT");assertThat(lookup(owner.importId()).resultBytes()).isNull();assertThat(coordinator.read().activeImportId()).isEqualTo(owner.importId());assertThat(count("audit_events")).isEqualTo(1);
        }
    }
    @Test void lostAcknowledgmentsResolveByExactOperationRequestWithoutDuplicateWrites() {
        UUID id=UUID.randomUUID();
        try(var guard=InstallationHostGuard.acquire(binding)) {
            repository.reserve(guard,reservation(id)); // Deliberately discard the reservation acknowledgment.
            var observed=lookup(id);assertThat(observed.state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);
            fixedConflict(() -> repository.lookup(id,OBJECT),"INVALID_REQUEST");
            finish(guard,token(id)); // Discard final acknowledgment too.
        }
        var fresh=new ImportOwnershipRepository(jdbc,tx,INSTALL);var result=fresh.lookup(id,REQUEST).orElseThrow();
        assertThat(result.state()).isEqualTo(ImportOwnershipRepository.State.SUCCEEDED);assertThat(result.resultBytes()).isEqualTo(RESULT);
        result.resultBytes()[0]=0;assertThat(lookup(id).resultBytes()).isEqualTo(RESULT);assertThat(count("audit_events")).isEqualTo(2);assertThat(result.toString()).isEqualTo("ImportOperation[redacted]");
    }
    /** Inject only the network acknowledgment failure, after an actual PostgreSQL write commit. */
    ImportOwnershipRepository lostCommitRepository(AtomicBoolean armed) {
        var real=new DriverManagerDataSource(url,user,password);
        var dataSource=new org.springframework.jdbc.datasource.AbstractDataSource() {
            @Override public Connection getConnection() throws SQLException {
                Connection connection=real.getConnection();AtomicBoolean wrote=new AtomicBoolean();
                return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args) -> {
                    try {
                        if(method.getName().equals("prepareStatement") && args!=null && args[0] instanceof String sql && (sql.startsWith("INSERT") || sql.startsWith("UPDATE")))wrote.set(true);
                        Object result=method.invoke(connection,args);
                        if(method.getName().equals("commit") && wrote.get() && armed.compareAndSet(true,false))throw new SQLException("PRIVATE injected commit acknowledgment failure","08006");
                        return result;
                    } catch(InvocationTargetException ex) {throw ex.getCause();}
                });
            }
            @Override public Connection getConnection(String u,String p) throws SQLException {return getConnection();}
        };
        var template=new TransactionTemplate(new JdbcTransactionManager(dataSource));template.setTimeout(3);
        var queries=new JdbcTemplate(dataSource);queries.setQueryTimeout(3);
        return new ImportOwnershipRepository(queries,new ManagementTransactions(template),INSTALL);
    }
    @ParameterizedTest @ValueSource(strings={"RESERVE","FINISH","HANDOFF"})
    void actualCommittedWriteWithLostAckIsResolvedByLookup(String phase) throws Exception {
        AtomicBoolean armed=new AtomicBoolean(true);var uncertain=lostCommitRepository(armed);UUID id=UUID.randomUUID();
        if(phase.equals("HANDOFF")) {
            Process p=child("hold",id,null);var old=token(id);stop(p);UUID recovery=UUID.randomUUID();
            try(var guard=InstallationHostGuard.acquire(binding)) {
                fixedConflict(() -> uncertain.handoff(guard,old,recovery,true),"MANAGEMENT_UNAVAILABLE");
                assertThat(armed).isFalse();assertThat(lookup(recovery).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);
                assertThat(lookup(id).state()).isEqualTo(ImportOwnershipRepository.State.INTERRUPTED);assertThat(coordinator.read().activeImportId()).isEqualTo(recovery);
                fixedConflict(() -> repository.handoff(guard,old,UUID.randomUUID(),true),"OWNERSHIP_STALE");
                assertThat(count("artifact_import_operations")).isEqualTo(2);assertThat(count("audit_events")).isEqualTo(3);
            }
        } else try(var guard=InstallationHostGuard.acquire(binding)) {
            if(phase.equals("RESERVE")) {
                fixedConflict(() -> uncertain.reserve(guard,reservation(id)),"MANAGEMENT_UNAVAILABLE");
                assertThat(lookup(id).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(coordinator.read().activeImportId()).isEqualTo(id);
                fixedConflict(() -> reserve(guard),"OWNERSHIP_STALE");assertThat(count("audit_events")).isEqualTo(1);
            } else {
                var owner=repository.reserve(guard,reservation(id));
                fixedConflict(() -> uncertain.finish(guard,owner,ImportOwnershipRepository.Outcome.SAFE_COMPLETION,RESULT,true),"MANAGEMENT_UNAVAILABLE");
                assertThat(lookup(id).state()).isEqualTo(ImportOwnershipRepository.State.SUCCEEDED);assertThat(lookup(id).resultBytes()).isEqualTo(RESULT);
                assertThat(coordinator.read().activeImportId()).isNull();assertThat(count("audit_events")).isEqualTo(2);
            }
            assertThat(armed).isFalse();assertThat(count("artifact_import_operations")).isEqualTo(1);
        }
    }
    @Test void completedOperationIdCannotBeReboundToANewReservation() {
        UUID id;
        try(var guard=InstallationHostGuard.acquire(binding)) {var t=reserve(guard);id=t.importId();finish(guard,t);}
        var before=coordinator.read();
        try(var guard=InstallationHostGuard.acquire(binding)) {fixedConflict(() -> repository.reserve(guard,reservation(id)),"STATE_CONFLICT");}
        assertThat(coordinator.read()).isEqualTo(before);assertThat(count("artifact_import_operations")).isEqualTo(1);assertThat(count("audit_events")).isEqualTo(2);
    }
    @Test void releasedGuardDoesNotProveLiveOwnerQuiescent() throws Exception {
        UUID id=UUID.randomUUID();Process p=child("released",id,null);var old=token(id);
        try(var guard=InstallationHostGuard.acquire(binding)) {
            fixedConflict(() -> reserve(guard),"OWNERSHIP_STALE");fixedConflict(() -> repository.handoff(guard,old,UUID.randomUUID(),false),"QUIESCENCE_REQUIRED");
            fixedConflict(() -> repository.handoff(guard,old,UUID.randomUUID(),true),"OWNER_PROCESS_UNRESOLVED");assertThat(lookup(id).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);
        }
        stop(p);
    }
    @Test void crashLeavesDurableOwnerAndRecoveryTransfersOccupiedSlotAtomically() throws Exception {
        UUID id=UUID.randomUUID();Process p=child("hold",id,null);var old=token(id);stop(p);
        assertThat(coordinator.read().activeImportId()).isEqualTo(id);
        try(var guard=InstallationHostGuard.acquire(binding);var pool=Executors.newFixedThreadPool(1)) {
            fixedConflict(() -> reserve(guard),"OWNERSHIP_STALE");
            fixedConflict(() -> repository.handoff(guard,old,UUID.randomUUID(),false),"QUIESCENCE_REQUIRED");
            UUID recovery=UUID.randomUUID();var future=pool.submit(() -> repository.handoff(guard,old,recovery,true));
            int observations=0;
            do {
                var seen=jdbc.queryForMap("SELECT c.active_import_id,c.epoch,o.state,o.owner_epoch FROM ddip_management.coordinator c JOIN ddip_management.artifact_import_operations o ON o.import_id=c.active_import_id WHERE c.singleton_id=1");
                assertThat(seen.get("active_import_id")).isIn(id,recovery);assertThat(seen.get("state")).isEqualTo("ACTIVE");assertThat(seen.get("epoch")).isEqualTo(seen.get("owner_epoch"));observations++;
            } while(!future.isDone() && observations<100);
            var next=future.get(10,TimeUnit.SECONDS);assertThat(observations).isPositive();assertThat(next.epoch()).isEqualTo(old.epoch()+1);
            assertThat(coordinator.read().activeImportId()).isEqualTo(recovery);assertThat(lookup(id).state()).isEqualTo(ImportOwnershipRepository.State.INTERRUPTED);
            assertThat(lookup(recovery).predecessorId()).isEqualTo(id);assertThat(lookup(recovery).kind()).isEqualTo(ImportOwnershipRepository.Kind.RECOVERY);
            fixedConflict(() -> finish(guard,next),"INVALID_REQUEST");
            assertThat(lookup(recovery).requestBytes()).isEqualTo(lookup(id).requestBytes());fixedConflict(() -> finish(guard,old),"OWNERSHIP_STALE");
            assertThat(jdbc.update("UPDATE ddip_management.artifact_import_operations SET state='FAILED',row_version=row_version+1 WHERE import_id=? AND owner_epoch=? AND row_version=? AND state='ACTIVE'",id,old.epoch(),old.operationVersion())).isZero();
            repository.finish(guard,next,ImportOwnershipRepository.Outcome.RECOVERY_CLOSED,RESULT,true);
            assertThat(coordinator.read().activeImportId()).isNull();assertThat(count("audit_events")).isEqualTo(4);assertThat(count("runs")).isZero();assertThat(count("artifacts")).isZero();
        }
    }
    @Test void handoffRollbackAndLostAcknowledgmentPreserveExactHistory() throws Exception {
        UUID id=UUID.randomUUID();Process p=child("hold",id,null);var old=token(id);stop(p);UUID recovery=UUID.randomUUID();
        try(var guard=InstallationHostGuard.acquire(binding)) {
            jdbc.execute("ALTER TABLE ddip_management.audit_events ADD CONSTRAINT injected_audit CHECK(action<>'IMPORT_INTERRUPTED')");
            fixedConflict(() -> repository.handoff(guard,old,recovery,true),"STATE_CONFLICT");
            assertThat(count("artifact_import_operations")).isEqualTo(1);assertThat(lookup(id).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(coordinator.read().epoch()).isEqualTo(old.epoch());
            jdbc.execute("ALTER TABLE ddip_management.audit_events DROP CONSTRAINT injected_audit");
            repository.handoff(guard,old,recovery,true); // Deliberately discard commit acknowledgment.
            assertThat(lookup(recovery).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(lookup(id).state()).isEqualTo(ImportOwnershipRepository.State.INTERRUPTED);
            fixedConflict(() -> repository.handoff(guard,old,UUID.randomUUID(),true),"OWNERSHIP_STALE");assertThat(count("artifact_import_operations")).isEqualTo(2);
        }
    }
    @Test void abandonedRecoveryCanOnlyTransferToAnotherFencedRecovery() throws Exception {
        UUID id=UUID.randomUUID();Process original=child("hold",id,null);stop(original);
        UUID first=UUID.randomUUID();Process recovery=child("recover",first,id);var old=token(first);stop(recovery);
        try(var guard=InstallationHostGuard.acquire(binding)) {
            fixedConflict(() -> reserve(guard),"OWNERSHIP_STALE");UUID second=UUID.randomUUID();var current=repository.handoff(guard,old,second,true);
            assertThat(current.epoch()).isEqualTo(3);assertThat(lookup(first).state()).isEqualTo(ImportOwnershipRepository.State.INTERRUPTED);assertThat(lookup(second).predecessorId()).isEqualTo(first);
            repository.finish(guard,current,ImportOwnershipRepository.Outcome.RECOVERY_CLOSED,RESULT,true);assertThat(count("artifact_import_operations")).isEqualTo(3);
        }
    }
    @Test void staleRecoveryVersionCannotCloseOrReplaceOldOwner() throws Exception {
        UUID id=UUID.randomUUID();Process p=child("hold",id,null);var old=token(id);stop(p);
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var stale=new ImportOwnershipRepository.Token(id,old.processInstanceId(),old.epoch(),old.operationVersion(),old.coordinatorVersion()+1);
            fixedConflict(() -> repository.handoff(guard,stale,UUID.randomUUID(),true),"OWNERSHIP_STALE");assertThat(count("artifact_import_operations")).isEqualTo(1);assertThat(coordinator.read().activeImportId()).isEqualTo(id);
        }
    }
    @Test void databaseOutageBeforeFinalizationLeavesOwnershipAndSafeDiagnostics() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            var owner=reserve(guard);
            admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS false");
            try {fixedConflict(() -> finish(guard,owner),"MANAGEMENT_UNAVAILABLE");}
            finally {admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS true");}
            assertThat(lookup(owner.importId()).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(lookup(owner.importId()).resultBytes()).isNull();assertThat(count("audit_events")).isEqualTo(1);
            finish(guard,owner);
        }
    }
    @Test void missingProfileOrDifferentControlBindingRejectsBeforeReservation() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            fixedConflict(() -> repository.reserve(guard,new ImportOwnershipRepository.Reservation(UUID.randomUUID(),"absent",HASH,REQUEST)),"HOST_BINDING_INVALID");
            assertThat(count("artifact_import_operations")).isZero();assertThat(coordinator.read().epoch()).isZero();
        }
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES ('wrong',?,?)",OBJECT,HASH);
        try(var guard=InstallationHostGuard.acquire(binding)) {fixedConflict(() -> repository.reserve(guard,new ImportOwnershipRepository.Reservation(UUID.randomUUID(),"wrong",HASH,REQUEST)),"HOST_BINDING_INVALID");}
    }
    @Test void externalTransactionCannotEncloseOwnershipOperations() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            fixedConflict(() -> tx.required(() -> reserve(guard)),"INVALID_REQUEST");assertThat(count("artifact_import_operations")).isZero();
        }
    }
    @Test void scopedUniquenessPermitsProductsKindsFormatsAndVersionsButRejectsSameScope() {
        artifact("content-intelligence","GOLD",1,1,null,"same","gold",true);
        artifact("application-reliability","SERVICE_REFERENCE",1,1,null,"same","service-other-product",true);
        artifact("content-intelligence","SILVER",1,1,null,"same","silver",true);
        artifact("content-intelligence","GOLD",2,1,null,"same","contract-two",true);
        artifact("content-intelligence","GOLD",1,2,null,"same","processor-two",true);
        artifact("application-reliability","RELIABILITY_BATCH_V2",2,2,"JSONL","same","jsonl",true);
        artifact("application-reliability","RELIABILITY_BATCH_V2",2,2,"PARQUET","same","parquet",true);
        assertThat(count("artifacts")).isEqualTo(7);
        assertThatThrownBy(() -> artifact("application-reliability","RELIABILITY_BATCH_V2",2,2,"JSONL","same","another-jsonl",true)).hasMessageContaining("artifact_native_scope_unique");
        assertThatThrownBy(() -> artifact("content-intelligence","GOLD",1,1,null,"same","another-gold",true)).hasMessageContaining("artifact_native_scope_unique");
        assertThatThrownBy(() -> artifact("content-intelligence","GOLD",1,1,null,"different","gold",true)).hasMessageContaining("artifacts_storage_binding_id_relative_address_key");
        assertThatThrownBy(() -> artifact("application-reliability","RELIABILITY_BATCH_V2",2,2,null,"unscoped","unscoped",true)).hasMessageContaining("artifact_format_scope");
        assertThatThrownBy(() -> artifact("content-intelligence","GOLD",1,1,"JSONL","bad-format","bad-format",true)).hasMessageContaining("artifact_format_scope");
        assertThat(count("artifacts")).isEqualTo(7);
    }
    @Test void orphanActiveOperationCannotCommitWithoutCoordinatorBinding() {
        try(var guard=InstallationHostGuard.acquire(binding)) {
            fixedConflict(() -> tx.required(() -> {
                jdbc.update("INSERT INTO ddip_management.artifact_import_operations(import_id,installation_id,kind,quiescence_confirmed,process_instance_id,owner_pid,owner_started_at,execution_profile_id,profile_sha256,host_binding_id,host_binding_sha256,owner_epoch,request_bytes,request_sha256,state) " +
                        "VALUES (?,?,'IMPORT',false,?,1,now(),'import-test',?,?,?,1,?,?,'ACTIVE')",UUID.randomUUID(),INSTALL,guard.process().instanceId(),HASH,binding.id(),binding.digest(),REQUEST,SourceJson.sha(REQUEST));return null;
            }),"STATE_CONFLICT");
            assertThat(count("artifact_import_operations")).isZero();assertThat(coordinator.read().activeImportId()).isNull();
        }
    }
}
