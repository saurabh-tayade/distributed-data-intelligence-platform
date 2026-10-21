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

/** Gate B: actual PostgreSQL, actual native readers plus controlled process/transaction faults. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReliabilityPreflightIT {
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
    void provision(String format)throws Exception {
        work=Files.createTempDirectory(temp,"case-").toRealPath();var process=new ProcessBuilder(python.toString(),"-B",repo.resolve("processing/tests/reliability_preflight_fixtures.py").toString(),work.toString(),format).redirectError(ProcessBuilder.Redirect.DISCARD);process.environment().put("PYTHONPATH",repo.resolve("processing")+":"+repo.resolve("processing/tests"));
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
    @Test void actualNativePreflightAndHistoryHaveNoPublication() {
        var before=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);long artifacts=jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts",Long.class);var verified=verify();assertThat(verified.nativeDependencyFingerprint()).isEqualTo("3b9354d578f017cefea8e9ace2556c6ed474afec606722ee01e9fba8e4a39a7a");assertThat(stage().get("state")).isEqualTo("VERIFIED");assertThat(stage().get("child_pid")).isInstanceOf(Long.class);assertThat(stage().get("command_sha256")).isEqualTo(SourceJson.sha(PreflightFiles.read(descriptor(),65536,true)));assertThat(stage().get("result_sha256")).isEqualTo(SourceJson.sha(PreflightFiles.read(descriptor().resolveSibling("verified.json"),65536,true)));assertThat(state()).isEqualTo("RUNNING");assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(before);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts",Long.class)).isEqualTo(artifacts);occupied();assertThat(workers.inspectCheckInputs(owner,checkpoint).phase()).isEqualTo("HISTORICAL_VERIFIED");
        var catalog=SourceJson.object().put("catalogVersion",1);for(String k:List.of("endpoints","reliabilityProfiles","storageProfiles"))catalog.putArray(k);var detail=new RunRepository(jdbc,tx,INSTALL,PreflightCatalogFixture.from(catalog)).get(run.toString());assertThat(detail.get("currentStage").stringValue()).isEqualTo("CHECK_INPUTS");assertThat(detail.get("result").isNull()).isTrue();assertThat(detail.get("counts").isNull()).isTrue();assertThat(detail.get("attempts").get("count").stringValue()).isEqualTo("1");
    }
    @Test void actualParquetPreflight()throws Exception{stop();jdbc.execute("DROP SCHEMA ddip_management CASCADE");new SchemaLifecycle(Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load(),jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);provision("PARQUET");assertThat(verify().stage().state()).isEqualTo("VERIFIED");occupied();}
    @Test void intentCommitsBeforeAnyLaunchAndChildIdentityIsObserved() {
        var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();assertThat(stage().get("state")).isEqualTo("INTENT");assertThat(stage().get("command_address")).isNull();return super.run(c,e,d,t,new Observer(){public void beforeStart(){observer.beforeStart();assertThat(stage().get("command_address")).isNotNull();assertThat(stage().get("child_pid")).isNull();}public void started(InstallationHostGuard.ProcessIdentity child){observer.started(child);assertThat(stage().get("child_pid")).isEqualTo(child.pid());}public void heartbeat(){observer.heartbeat();}});}};
        service(process).verify(guard,owner,RUNTIME,checkpoint);assertThat(stage().get("state")).isEqualTo("VERIFIED");
    }
    @Test void intentRollbackPreventsLaunchAndAllLifecyclePartialWrites(){jdbc.execute("ALTER TABLE ddip_management.stage_invocations ADD CONSTRAINT injected CHECK(stage<>'CHECK_INPUTS')");assertThatThrownBy(this::verify).hasMessage("STATE_CONFLICT");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts",String.class)).isEqualTo("CLAIMED");assertThat(jdbc.queryForObject("SELECT current_stage FROM ddip_management.runs",String.class)).isNull();assertThat(workers.inspectCheckInputs(owner,checkpoint).phase()).isEqualTo("CONFIRMED_ROLLBACK");occupied();}
    @Test void cancellationBeforeAuthorizationPreventsLaunch(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){cancel();return super.run(c,e,d,t,observer);}};assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("WORKER_NOT_READY");assertThat(stage().get("child_pid")).isNull();assertThat(stage().get("command_address")).isNull();occupied();}
    void cancel(){byte[] b=SourceJson.canonical(SourceJson.object());jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','cancel',?,?,?,'REQUESTED','local-operator')",UUID.randomUUID(),run,b,SourceJson.sha(b),jdbc.queryForObject("SELECT row_version FROM ddip_management.runs",Long.class));}
    @Test void stageVersionChangeFencesLaunch(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){jdbc.update("UPDATE ddip_management.stage_invocations SET row_version=row_version+1");return super.run(c,e,d,t,observer);}};assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("OWNERSHIP_LOST");assertThat(stage().get("child_pid")).isNull();occupied();}
    @ParameterizedTest @ValueSource(strings={"epoch","instance","attempt","run-version","attempt-version","coordinator-version","worker-version"}) void staleOwnerCannotEnterStage(String field){var w=owner.worker();var changed=new WorkerRepository.Session(w.installationId(),field.equals("instance")?UUID.randomUUID():w.instanceId(),w.profileId(),w.profileDigest(),w.deploymentDigest(),w.hostDigest(),w.status(),w.epoch()+(field.equals("epoch")?1:0),w.coordinatorVersion()+(field.equals("coordinator-version")?1:0),w.workerVersion()+(field.equals("worker-version")?1:0));var stale=new WorkerRepository.Owner(changed,run,field.equals("attempt")?UUID.randomUUID():owner.attemptId(),owner.runVersion()+(field.equals("run-version")?1:0),owner.attemptVersion()+(field.equals("attempt-version")?1:0),owner.attemptState());assertThatThrownBy(()->service().verify(guard,stale,RUNTIME,checkpoint)).hasMessage("OWNERSHIP_LOST");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();occupied();}
    JdbcTemplate commitFault(String phase) {
        var once=new AtomicBoolean(true);var data=new AbstractDataSource(){
            public Connection getConnection()throws SQLException {
                var real=jdbc.getDataSource().getConnection();var matching=new AtomicBoolean();
                return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,method,args)->{
                    try {
                        if(method.getName().equals("prepareStatement") && args!=null && args[0] instanceof String sql && switch(phase){case "intent"->sql.startsWith("INSERT INTO ddip_management.stage_invocations");case "authorization"->sql.startsWith("UPDATE ddip_management.stage_invocations SET command_address");default->sql.startsWith("UPDATE ddip_management.stage_invocations SET state=?,result_address");})matching.set(true);
                        Object result=method.invoke(real,args);
                        if(method.getName().equals("commit") && matching.get() && once.compareAndSet(true,false))throw new SQLException("PRIVATE","08006");return result;
                    }catch(InvocationTargetException ex){throw ex.getCause();}
                });
            }
            public Connection getConnection(String u,String p)throws SQLException{return getConnection();}
        };return new JdbcTemplate(data);
    }
    @ParameterizedTest @ValueSource(strings={"intent","authorization","finalization"}) void lostCommitAcknowledgmentNeverDuplicatesLaunch(String phase) {
        var wrapped=commitFault(phase);workers=WorkerTestSupport.enabled(wrapped,transactions(wrapped),INSTALL);
        assertThatThrownBy(this::verify).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();
        var receipt=workers.inspectCheckInputs(owner,checkpoint);assertThat(receipt.phase()).isEqualTo(switch(phase){case "intent"->"INTENT_COMMITTED";case "authorization"->"LAUNCH_UNCERTAIN";default->"HISTORICAL_VERIFIED";});
        if(!phase.equals("finalization"))assertThat(stage().get("child_pid")).isNull();
        assertThatThrownBy(this::verify).hasMessage("OWNERSHIP_LOST");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isEqualTo(1);occupied();
    }
    @Test void finalAuditRollbackCannotTrustPartialStage(){jdbc.execute("ALTER TABLE ddip_management.audit_events ADD CONSTRAINT fail_verified CHECK(action<>'STAGE_VERIFIED')");assertThatThrownBy(this::verify).hasMessage("STATE_CONFLICT");assertThat(stage().get("state")).isEqualTo("RUNNING");assertThat(stage().get("result_address")).isNull();assertThat(stage().get("result_sha256")).isNull();occupied();}
    ReadOnlyVerifierProcess controlled(String mode,Duration timeout) {
        return new ReadOnlyVerifierProcess(128,Duration.ofMillis(500),Duration.ofMillis(500),Duration.ofMillis(500)){
            Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer) {
                return super.run(VerifierProcessTest.command(mode,descriptor().toString()),e,d,timeout,observer);
            }
        };
    }
    @Test void childExitZeroWithoutValidEvidenceIsUnknownAndBlocked(){assertThatThrownBy(()->service(controlled("invalid-result",Duration.ofSeconds(5))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("RESULT_INVALID");assertThat(stage().get("state")).isEqualTo("UNKNOWN");assertThat(state()).isEqualTo("RECOVERY_REQUIRED");occupied();}
    @Test void childExitZeroWithMalformedJsonIsUnknownAndBlocked(){assertThatThrownBy(()->service(controlled("malformed-result",Duration.ofSeconds(5))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("RESULT_INVALID");assertThat(stage().get("state")).isEqualTo("UNKNOWN");occupied();}
    @Test void nonzeroFixedFailureIsNotAVerifiedStage(){assertThatThrownBy(()->service(controlled("native-failed",Duration.ofSeconds(5))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("INPUT_UNAVAILABLE");assertThat(stage().get("state")).isEqualTo("FAILED");assertThat(stage().get("outcome_code")).isEqualTo("INPUT_UNAVAILABLE");occupied();}
    @Test void processStartupFailureClosesOnlyFailedStage(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){var changed=new ArrayList<>(c);changed.set(0,work.resolve("missing").toString());return super.run(changed,e,d,t,observer);}};assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("PROCESS_START_FAILED");assertThat(stage().get("state")).isEqualTo("FAILED");assertThat(stage().get("child_pid")).isNull();occupied();}
    @Test void boundedChildTimeoutCannotVerify(){assertThatThrownBy(()->service(controlled("hang",Duration.ofMillis(250))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("STAGE_TIMEOUT");assertThat(stage().get("state")).isEqualTo("FAILED");occupied();}
    @ParameterizedTest @ValueSource(strings={"stdout","stderr"}) void outputOverflowCannotVerify(String stream){assertThatThrownBy(()->service(controlled(stream,Duration.ofSeconds(5))).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("STAGE_OUTPUT_LIMIT");assertThat(stage().get("state")).isEqualTo("FAILED");occupied();}
    @Test void databaseOutageBeforeAuthorizationPreventsChild(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){outage(true);return super.run(c,e,d,t,observer);}};try{assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("MANAGEMENT_UNAVAILABLE");}finally{outage(false);}assertThat(stage().get("child_pid")).isNull();assertThat(stage().get("command_address")).isNull();occupied();}
    @Test void databaseOutageAfterChildIdentityStopsAuthorizationAndCleans() {
        var child=new AtomicReference<InstallationHostGuard.ProcessIdentity>();
        var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){return super.run(c,e,d,t,new Observer(){public void beforeStart(){observer.beforeStart();}public void started(InstallationHostGuard.ProcessIdentity p){child.set(p);observer.started(p);outage(true);observer.heartbeat();}public void heartbeat(){observer.heartbeat();}});}};
        try{assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("MANAGEMENT_UNAVAILABLE");}finally{outage(false);}assertThat(ProcessHandle.of(child.get().pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();assertThat(stage().get("state")).isEqualTo("RUNNING");occupied();
    }
    @Test void databaseOutageAfterNativeVerificationDoesNotRegisterStage(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){var result=super.run(c,e,d,t,observer);outage(true);return result;}};try{assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("MANAGEMENT_UNAVAILABLE");}finally{outage(false);}assertThat(stage().get("state")).isEqualTo("RUNNING");assertThat(stage().get("result_address")).isNull();occupied();}
    @Test void importerCannotAcquireWorkerGuardOrOccupiedAttempt(){assertThatThrownBy(()->InstallationHostGuard.acquire(binding)).hasMessage("HOST_GUARD_OCCUPIED");guard.close();guard=InstallationHostGuard.acquire(binding);String digest=fixture.get("bundle").get("capturedSelection").get("executionProfile").get("configurationSha256").stringValue();assertThatThrownBy(()->new ImportOwnershipRepository(jdbc,tx,INSTALL).reserve(guard,new ImportOwnershipRepository.Reservation(UUID.randomUUID(),PROFILE,digest,OwnershipFixtures.REQUEST))).hasMessage("OWNERSHIP_STALE");occupied();}
    @Test void changedRawEvidenceAfterImportFailsFreshQualification()throws Exception {Path raw=nativeRaw();byte[] bytes=Files.readAllBytes(raw);bytes[0]='!';Files.write(raw,bytes);assertThatThrownBy(this::verify).hasMessage("INPUT_CHANGED");assertThat(stage().get("state")).isEqualTo("FAILED");occupied();}
    Path nativeRaw(){var f=fixture.get("bundle");var input=f.get("capturedSelection").get("inputs").get(0);return Path.of(f.get("bindings").get("batch").get("root").stringValue()).resolve(input.get("relativeAddress").stringValue()).resolve("data/events.jsonl");}
    @Test void laterSourceDisablementDoesNotReplaceAlreadyClaimedFrozenRevision(){byte[] before=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);jdbc.update("UPDATE ddip_management.sources SET enabled=false,row_version=row_version+1");assertThat(verify().stage().state()).isEqualTo("VERIFIED");assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(before);occupied();}
    @Test void invalidRuntimeModuleFailsBeforeIntent()throws Exception{Path module=work.resolve("deployment/service_reference.py");Files.writeString(module,"# changed",StandardOpenOption.APPEND);assertThatThrownBy(this::verify).hasMessage("RUNTIME_MISMATCH");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();occupied();}
    @Test void productionNoneCannotEnterPreflight(){workers=new WorkerRepository(jdbc,tx,INSTALL);assertThat(workers.executionAvailable()).isFalse();assertThatThrownBy(this::verify).hasMessage("WORKER_NOT_READY");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();assertThat(Files.exists(descriptor())).isFalse();occupied();}
    @Test void currentOwnerCanHeartbeatStageButStaleTokenCannotAuthorize(){var frozen=workers.frozenReliability(guard,owner);var initial=workers.beginCheckInputs(guard,owner,checkpoint,"a".repeat(64),SourceJson.sha(SourceJson.canonical(frozen)));long events=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class);var fresh=workers.heartbeatCheckInputs(guard,initial);assertThat(fresh.owner().worker().epoch()).isEqualTo(initial.owner().worker().epoch());assertThat(fresh.owner().worker().coordinatorVersion()).isEqualTo(initial.owner().worker().coordinatorVersion()+1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class)).isEqualTo(events);assertThatThrownBy(()->workers.authorizeCheckInputs(guard,initial)).hasMessage("OWNERSHIP_LOST");occupied();}
    @Test void authorizationCannotBeConsumedTwice(){var frozen=workers.frozenReliability(guard,owner);var initial=workers.beginCheckInputs(guard,owner,checkpoint,"a".repeat(64),SourceJson.sha(SourceJson.canonical(frozen)));var authorized=workers.authorizeCheckInputs(guard,initial);assertThat(authorized.authorized()).isTrue();assertThatThrownBy(()->workers.authorizeCheckInputs(guard,initial)).hasMessage("OWNERSHIP_LOST");assertThat(stage().get("child_pid")).isNull();occupied();}
    @Test void processInterruptionPreservesRecoveryOwnership(){var process=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){return super.run(c,e,d,t,new Observer(){public void beforeStart(){observer.beforeStart();}public void started(InstallationHostGuard.ProcessIdentity p){observer.started(p);Thread.currentThread().interrupt();}public void heartbeat(){observer.heartbeat();}});}};try{assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("INTERRUPTED");}finally{Thread.interrupted();}assertThat(stage().get("state")).isEqualTo("UNKNOWN");assertThat(state()).isEqualTo("RECOVERY_REQUIRED");occupied();}
    @Test void observedDescendantUncertaintyRemainsOccupied() {
        Path marker=work.resolve("descendant-started");
        var process=new ReadOnlyVerifierProcess(128,Duration.ofMillis(500),Duration.ofMillis(500),Duration.ofMillis(500)){
            Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer observer){return super.run(VerifierProcessTest.command("descendant",marker.toString()),e,d,Duration.ofSeconds(5),new Observer(){public void beforeStart(){observer.beforeStart();}public void started(InstallationHostGuard.ProcessIdentity p){observer.started(p);long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!Files.exists(marker) && System.nanoTime()<until)java.util.concurrent.locks.LockSupport.parkNanos(1000000);assertThat(Files.exists(marker)).isTrue();}public void heartbeat(){observer.heartbeat();}});}
        };
        assertThatThrownBy(()->service(process).verify(guard,owner,RUNTIME,checkpoint)).hasMessage("TERMINATION_UNCONFIRMED");assertThat(stage().get("state")).isEqualTo("UNKNOWN");assertThat(state()).isEqualTo("RECOVERY_REQUIRED");occupied();
    }
}
