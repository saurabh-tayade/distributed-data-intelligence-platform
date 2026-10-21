package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import platform.contentapi.ContentApiApplication;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;

/** C1 gates: actual PostgreSQL + test-only claim capability. No native job/publication is simulated. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class WorkerCoordinationIT extends RunApiFixtures {
    final Map<String,InstallationHostGuard.Binding> controls=new HashMap<>();
    final Map<String,Path> roots=new HashMap<>();
    InstallationHostGuard guard;
    WorkerRepository repository;
    WorkerRepository.Session session;
    @Override void storage(String id,String product,String role) {
        try {
            Path base=Files.createTempDirectory(temp,"c1-").toRealPath();JsonNode n;
            if(role.equals("CONTROL")) {
                var b=OwnershipFixtures.host(base);var named=new InstallationHostGuard.Binding(id,INSTALL,b.root(),b.rootIdentity(),b.guardIdentity(),b.installationIdentity());controls.put(id,named);n=SourceJson.parse(named.descriptor());roots.put(id,named.root());
            } else if(role.equals("BRONZE")) {
                Path credentials=Files.writeString(base.resolve("credentials.json"),"{\"accessKey\":\"synthetic\",\"secretKey\":\"synthetic\"}");Files.setPosixFilePermissions(credentials,PosixFilePermissions.fromString("rw-------"));
                n=SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:9").put("region","us-east-1").put("bucket","synthetic").put("credentialsFile",credentials.toString());
            } else {
                var identity=InstallationHostGuard.identity(base);n=SourceJson.object().put("importStorageVersion",1).put("root",base.toString()).put("rootDevice",Long.toString(identity.device())).put("rootInode",Long.toString(identity.inode()));roots.put(id,base);
            }
            byte[] bytes=SourceJson.canonical(n);jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,bytes,SourceJson.sha(bytes),INSTALL);
        }catch(Exception ex){throw new IllegalStateException(ex);}
    }
    String digest(String profile){return jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,profile);}
    void ready(){ready(JSON_PROFILE);}
    void ready(String profile){guard=InstallationHostGuard.acquire(controls.get(profile.equals(CONTENT_PROFILE)?"content-CONTROL":"reliability-CONTROL"));repository=WorkerTestSupport.enabled(jdbc,tx,INSTALL);session=repository.register(guard,profile,digest(profile),"a".repeat(64));}
    UUID queued(String key){return UUID.fromString(submit(reliability(false),key).get("runId").stringValue());}
    WorkerRepository.Claim claim(){return repository.claim(guard,session,UUID.randomUUID());}
    String state(UUID run){return jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run);}
    void closeGuard(){if(guard!=null){guard.close();guard=null;}}
    @AfterEach void release(){close();closeGuard();}
    void noEffects(){assertThat(count("stage_invocations")).isZero();assertThat(count("stage_artifacts")).isZero();assertThat(count("rss_deliveries")).isZero();assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.runs WHERE state='SUCCEEDED' OR result_artifact_id IS NOT NULL",Long.class)).isZero();}

    @Tag("C1_GATE_A") @Test void emptyQueueCreatesNoAttempt(){ready();assertThat(claim().status()).isEqualTo("EMPTY");assertThat(count("attempts")).isZero();noEffects();}
    @Tag("C1_GATE_A") @Test void atomicClaimHasDurableIdentityAndSafeInspection()throws Exception {
        UUID run=queued("first");var nativeBefore=namespaceSnapshot();ready();var c=claim();var o=c.owner();assertThat(c.runId()).isEqualTo(run);assertThat(state(run)).isEqualTo("RUNNING");assertThat(o.attemptState()).isEqualTo("CLAIMED");
        var a=jdbc.queryForMap("SELECT * FROM ddip_management.attempts");assertThat(a.get("attempt_no")).isEqualTo(1);assertThat(a.get("kind")).isEqualTo("EXECUTE");assertThat(a.get("instance_id")).isEqualTo(session.instanceId());assertThat(a.get("owner_epoch")).isEqualTo(1L);
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(o.attemptId());
        assertThat(repository.lookup(o.attemptId()).orElseThrow().runId()).isEqualTo(run);
        var detail=body(http(get(RUNS+"/"+run))).get("data");assertThat(detail.get("currentStage").isNull()).isTrue();assertThat(detail.get("result").isNull()).isTrue();assertThat(detail.get("counts").isNull()).isTrue();assertThat(detail.get("attempts").get("count").stringValue()).isEqualTo("1");
        var attempts=body(http(get(RUNS+"/"+run+"/attempts"))).get("data");assertThat(attempts.get(0).get("state").stringValue()).isEqualTo("CLAIMED");
        var events=body(http(get(RUNS+"/"+run+"/events"))).get("data");assertThat(events.size()).isEqualTo(3);assertThat(events.get(1).get("action").stringValue()).isEqualTo("RUN_CLAIMED");assertThat(events.get(2).get("action").stringValue()).isEqualTo("ATTEMPT_CLAIMED");assertThat(namespaceSnapshot()).isEqualTo(nativeBefore);noEffects();
    }
    @Tag("C1_GATE_A") @Test void fifoAcrossKnownPrelaunchFailures(){UUID first=queued("first"),second=queued("second");ready();var a=claim();assertThat(a.runId()).isEqualTo(first);session=repository.failBeforeLaunch(guard,a.owner(),WorkerRepository.PrelaunchFailure.PROCESS_START_FAILED);assertThat(claim().runId()).isEqualTo(second);assertThat(state(first)).isEqualTo("FAILED");noEffects();}
    @Tag("C1_GATE_A") @Test void competingTransactionsHaveExactlyOneWinner()throws Exception {
        queued("one");ready();var barrier=new CyclicBarrier(2);var other=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        try(var pool=Executors.newFixedThreadPool(2)) {
            List<Future<String>> tasks=new ArrayList<>();for(var repo:List.of(repository,other))tasks.add(pool.submit(()->{barrier.await(5,TimeUnit.SECONDS);try{return repo.claim(guard,session,UUID.randomUUID()).status();}catch(WorkerFailure ex){return ex.code().name();}}));
            assertThat(List.of(tasks.get(0).get(10,TimeUnit.SECONDS),tasks.get(1).get(10,TimeUnit.SECONDS))).containsExactlyInAnyOrder("CLAIMED","OWNERSHIP_LOST");
        }assertThat(count("attempts")).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RUN_CLAIMED'",Long.class)).isEqualTo(1);
    }
    @Tag("C1_GATE_A") @ParameterizedTest @ValueSource(strings={"attempts","runs","coordinator","audit_events"})
    void rollbackAtEachClaimMutation(String table) {
        UUID run=queued("rollback");ready();var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");long audits=count("audit_events");
        jdbc.execute("CREATE FUNCTION ddip_management.c1_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");
        String action=Set.of("runs","coordinator").contains(table)?"UPDATE":"INSERT";
        jdbc.execute("CREATE TRIGGER c1_fault BEFORE "+action+" ON ddip_management."+table+" FOR EACH ROW EXECUTE FUNCTION ddip_management.c1_fault()");
        assertThatThrownBy(this::claim).hasMessage("STATE_CONFLICT").hasNoCause();assertThat(state(run)).isEqualTo("QUEUED");assertThat(count("attempts")).isZero();assertThat(count("audit_events")).isEqualTo(audits);assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);noEffects();
    }
    @Tag("C1_GATE_A") @ParameterizedTest @ValueSource(strings={"STOPPING","RECOVERY_ONLY"}) void blockedModesNeverClaim(String mode){queued("one");jdbc.update("UPDATE ddip_management.coordinator SET mode=?,row_version=row_version+1",mode);ready();assertThat(session.status()).isEqualTo(mode);assertThatThrownBy(this::claim).hasMessage("WORKER_NOT_READY");assertThat(count("attempts")).isZero();}
    @Tag("C1_GATE_B") @Test void importOwnershipAndWorkerAreMutuallyExclusive() {
        queued("one");var b=controls.get("reliability-CONTROL");var owner=new ImportOwnershipRepository(jdbc,tx,INSTALL);
        try(var importing=InstallationHostGuard.acquire(b)) {
            var r=new ImportOwnershipRepository.Reservation(UUID.randomUUID(),JSON_PROFILE,digest(JSON_PROFILE),OwnershipFixtures.REQUEST);owner.reserve(importing,r);
            assertThatThrownBy(()->InstallationHostGuard.acquire(b)).hasMessage("HOST_GUARD_OCCUPIED");
        }
        ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");assertThatThrownBy(this::claim).hasMessage("WORKER_NOT_READY");assertThat(count("attempts")).isZero();assertThat(count("artifact_import_operations")).isEqualTo(1);
    }
    @Tag("C1_GATE_A") @Test void disabledSourceFailsWithoutAttemptAndKeepsFrozenRevision(){UUID run=queued("one");byte[] frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);disable();ready();var c=claim();assertThat(c.status()).isEqualTo("REJECTED");assertThat(c.code()).isEqualTo("SOURCE_DISABLED");assertThat(state(run)).isEqualTo("FAILED");assertThat(count("attempts")).isZero();assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(frozen);assertThat(jdbc.queryForObject("SELECT action FROM ddip_management.audit_events WHERE safe_code='SOURCE_DISABLED'",String.class)).isEqualTo("SOURCE_DISABLED_AT_DISPATCH");}
    void disable(){sources.execute(SourceCommands.command(SourceCommands.Action.ENABLE,RELIABILITY,"demo-requests",SourceJson.object().put("enabled",false),"disable","\"0\""),"test");}
    @Tag("C1_GATE_A") @Test void sourceRevisionEditsDoNotReplaceCapturedRevision(){UUID run=queued("one");byte[] frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);sources.execute(SourceCommands.command(SourceCommands.Action.REVISE,RELIABILITY,"demo-requests",revise("revised",requests("request-jsonl-v2")),"revise","\"0\""),"test");ready();assertThat(claim().runId()).isEqualTo(run);assertThat(jdbc.queryForObject("SELECT revision FROM ddip_management.run_sources",Integer.class)).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(frozen);}
    @Tag("C1_GATE_A") @Test void sourceDisableWinsSerializedRace()throws Exception {
        UUID run=queued("one");ready();var locked=new CountDownLatch(1);var release=new CountDownLatch(1);var entered=new CountDownLatch(1);
        try(var pool=Executors.newFixedThreadPool(2)) {
            var update=pool.submit(()->tx.required(()->{jdbc.queryForList("SELECT singleton_id FROM ddip_management.coordinator FOR UPDATE");locked.countDown();try{assertThat(release.await(5,TimeUnit.SECONDS)).isTrue();}catch(InterruptedException ex){throw new RuntimeException(ex);}jdbc.update("UPDATE ddip_management.sources SET enabled=false,row_version=row_version+1 WHERE product_id=? AND source_id='demo-requests'",RELIABILITY);return null;}));
            assertThat(locked.await(5,TimeUnit.SECONDS)).isTrue();var claiming=pool.submit(()->{entered.countDown();return claim();});assertThat(entered.await(5,TimeUnit.SECONDS)).isTrue();awaitCoordinatorWait();release.countDown();update.get(10,TimeUnit.SECONDS);assertThat(claiming.get(10,TimeUnit.SECONDS).code()).isEqualTo("SOURCE_DISABLED");
        }assertThat(state(run)).isEqualTo("FAILED");assertThat(count("attempts")).isZero();
    }
    @Tag("C1_GATE_A") @Test void incompatibleRuntimeProfileIsObservableAndDoesNotSkipForever(){UUID run=UUID.fromString(submit(content(),"content-first").get("runId").stringValue());UUID next=queued("rel-second");ready();var rejected=claim();assertThat(rejected.runId()).isEqualTo(run);assertThat(rejected.code()).isEqualTo("RUNTIME_MISMATCH");assertThat(count("attempts")).isZero();assertThat(claim().runId()).isEqualTo(next);}
    @Tag("C1_GATE_A") @Test void attemptNumbersRemainMonotonic(){UUID run=queued("one");ready();jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code) VALUES (?,?,1,'EXECUTE',?,1,'FAILED',now(),'PROCESS_START_FAILED')",UUID.randomUUID(),run,session.instanceId());assertThat(claim().owner().attemptId()).isNotNull();assertThat(jdbc.queryForObject("SELECT max(attempt_no) FROM ddip_management.attempts",Integer.class)).isEqualTo(2);}
    @Tag("C1_GATE_B") @Test void staleEpochCannotMutateOrRelease(){queued("one");ready();var o=claim().owner();var s=o.worker();var stale=new WorkerRepository.Session(s.installationId(),s.instanceId(),s.profileId(),s.profileDigest(),s.deploymentDigest(),s.hostDigest(),s.status(),s.epoch()-1,s.coordinatorVersion(),s.workerVersion());assertThatThrownBy(()->repository.heartbeat(guard,new WorkerRepository.Owner(stale,o.runId(),o.attemptId(),o.runVersion(),o.attemptVersion(),o.attemptState()))).hasMessage("OWNERSHIP_LOST");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(o.attemptId());}
    @Tag("C1_GATE_B") @Test void correctHeartbeatIsBoundedVersionedAndNotAudited(){queued("one");ready();var o=claim().owner();long audits=count("audit_events");var next=repository.heartbeat(guard,o);assertThat(next.worker().workerVersion()).isEqualTo(o.worker().workerVersion()+1);assertThat(next.worker().coordinatorVersion()).isEqualTo(o.worker().coordinatorVersion()+1);assertThat(next.attemptVersion()).isEqualTo(o.attemptVersion());assertThat(count("audit_events")).isEqualTo(audits);assertThatThrownBy(()->repository.heartbeat(guard,o)).hasMessage("OWNERSHIP_LOST");assertThat(count("attempts")).isEqualTo(1);}
    @Tag("C1_GATE_B") @Test void runningAndUncertainInterruptionRetainOccupiedSlot()throws Exception {UUID run=queued("one");ready();var o=repository.markRunning(guard,claim().owner());assertThat(o.attemptState()).isEqualTo("RUNNING");session=repository.interrupt(guard,o);assertThat(state(run)).isEqualTo("RECOVERY_REQUIRED");assertThat(repository.lookup(o.attemptId()).orElseThrow().state()).isEqualTo("INTERRUPTED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(o.attemptId());assertThat(session.status()).isEqualTo("RECOVERY_ONLY");assertThat(body(http(get(RUNS+"/"+run))).get("data").get("state").stringValue()).isEqualTo("RECOVERY_REQUIRED");noEffects();}
    @Tag("C1_GATE_A") @Test void prelaunchFailureCannotCloseAfterRunning(){queued("one");ready();var o=repository.markRunning(guard,claim().owner());assertThatThrownBy(()->repository.failBeforeLaunch(guard,o,WorkerRepository.PrelaunchFailure.PROCESS_START_FAILED)).hasMessage("OWNERSHIP_LOST");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(o.attemptId());}
    @Tag("C1_GATE_A") @Test void prelaunchFailureRollsBackIfAuditFails(){queued("one");ready();var o=claim().owner();jdbc.execute("CREATE FUNCTION ddip_management.fail_final() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");jdbc.execute("CREATE TRIGGER fail_final BEFORE INSERT ON ddip_management.audit_events FOR EACH ROW EXECUTE FUNCTION ddip_management.fail_final()");assertThatThrownBy(()->repository.failBeforeLaunch(guard,o,WorkerRepository.PrelaunchFailure.PROCESS_START_FAILED)).hasMessage("STATE_CONFLICT");assertThat(repository.lookup(o.attemptId()).orElseThrow().state()).isEqualTo("CLAIMED");assertThat(state(o.runId())).isEqualTo("RUNNING");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(o.attemptId());}
    JdbcTemplate lostAck() {
        var once=new AtomicBoolean(true);var ds=new AbstractDataSource(){
            public Connection getConnection()throws SQLException {
                Connection real=jdbc.getDataSource().getConnection();return(Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(p,m,args)->{
                    try{Object result=m.invoke(real,args);if(m.getName().equals("commit") && once.compareAndSet(true,false))throw new SQLException("PRIVATE","08006");return result;}catch(InvocationTargetException ex){throw ex.getCause();}
                });
            }
            public Connection getConnection(String u,String p)throws SQLException{return getConnection();}
        };return new JdbcTemplate(ds);
    }
    @Tag("C1_GATE_A") @Test void lostClaimAcknowledgmentUsesExactReceiptAndNeverBlindReclaims(){UUID run=queued("one");ready();UUID id=UUID.randomUUID();var wrapped=lostAck();var repo=WorkerTestSupport.enabled(wrapped,transactions(wrapped),INSTALL);assertThatThrownBy(()->repo.claim(guard,session,id)).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();assertThat(repository.lookup(id).orElseThrow().runId()).isEqualTo(run);assertThat(count("attempts")).isEqualTo(1);assertThatThrownBy(()->repo.claim(guard,session,id)).hasMessage("OWNERSHIP_LOST");assertThat(state(run)).isEqualTo("RUNNING");noEffects();}
    @Tag("C1_GATE_B") @Test void databaseFailureAfterClaimStopsFurtherAuthorization(){queued("one");ready();var o=claim().owner();outage(true);try{assertThatThrownBy(()->repository.heartbeat(guard,o)).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();}finally{outage(false);}assertThatThrownBy(()->repository.markRunning(guard,o)).hasMessage("OWNERSHIP_LOST");assertThat(repository.lookup(o.attemptId()).orElseThrow().state()).isEqualTo("CLAIMED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(o.attemptId());}
    @Tag("C1_GATE_A") @Test void databaseUnavailableBeforeClaimCannotCreateAttempt(){UUID run=queued("one");ready();outage(true);try{assertThatThrownBy(this::claim).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();}finally{outage(false);}assertThat(count("attempts")).isZero();assertThat(state(run)).isEqualTo("QUEUED");}
    void outage(boolean down){admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS "+!down);if(down)admin.queryForList("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname=? AND pid<>pg_backend_pid()",database);}
    @Tag("C1_GATE_B") @Test void abruptOwnerLossAndStaleHeartbeatNeverPermitTakeover(){queued("one");ready();var o=claim().owner();jdbc.update("UPDATE ddip_management.coordinator SET heartbeat_at=clock_timestamp()-interval '1 hour',row_version=row_version+1");closeGuard();ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");assertThatThrownBy(this::claim).hasMessage("WORKER_NOT_READY");assertThat(count("attempts")).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(o.attemptId());}
    @Tag("C1_GATE_B") @Test void claimReceiptSurvivesFreshConnectionsAndRepositoryRestart()throws Exception {
        queued("one");ready();var o=claim().owner();closeGuard();
        var receipt=repository.lookup(o.attemptId());var identities=jdbc.queryForList("SELECT run_id,submission_seq,request_sha256,selection_sha256,current_attempt_id FROM ddip_management.runs");byte[] frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);String signal=System.getProperty("ddip.test.worker-restart-signal");
        if(signal!=null){Files.writeString(Path.of(signal),"READY");long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<deadline)Thread.sleep(100);assertThat(Files.exists(Path.of(signal+".done"))).isTrue();}
        var fresh=new JdbcTemplate(new org.springframework.jdbc.datasource.DriverManagerDataSource(url,user,password));if(signal!=null)assertThat(fresh.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class)).isAfter(started);
        var restarted=new WorkerRepository(fresh,transactions(fresh),INSTALL);assertThat(restarted.lookup(o.attemptId())).isEqualTo(receipt);assertThat(restarted.lookup(UUID.randomUUID())).isEmpty();assertThat(fresh.queryForList("SELECT run_id,submission_seq,request_sha256,selection_sha256,current_attempt_id FROM ddip_management.runs")).isEqualTo(identities);assertThat(fresh.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(frozen);
        ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");assertThatThrownBy(this::claim).hasMessage("WORKER_NOT_READY");
    }
    @Tag("C1_GATE_B") @Test void productionRepositoryNeverClaimsEvenWhenRequested(){UUID run=queued("one");ready();var production=new WorkerRepository(jdbc,tx,INSTALL);assertThat(production.executionAvailable()).isFalse();for(int i=0;i<3;i++)assertThat(production.claim(guard,session,UUID.randomUUID()).status()).isEqualTo("NO_EXECUTOR");assertThat(state(run)).isEqualTo("QUEUED");assertThat(count("attempts")).isZero();noEffects();}
    void workerContext(String...extra) {
        var args=new ArrayList<>(List.of("--ddip.role=worker","--spring.main.web-application-type=servlet","--ddip.database.url="+url,"--ddip.database.username="+user,"--ddip.database.password="+password,"--ddip.installation-id="+INSTALL,"--ddip.worker.execution-profile-id="+JSON_PROFILE,"--ddip.worker.control-binding-id=reliability-CONTROL","--ddip.worker.deployment-sha256="+"a".repeat(64),"--ddip.worker.python="+System.getenv("DDIP_TEST_PYTHON"),"--ddip.worker.java-home="+System.getProperty("java.home"),"--ddip.worker.processing-root="+Path.of("../../processing").toAbsolutePath().normalize(),"--logging.level.root=OFF","--spring.main.banner-mode=off"));
        // Last occurrence wins is not assumed; replace exact property in this test helper.
        for(String value:extra){String key=value.substring(0,value.indexOf('=')+1);args.removeIf(a->a.startsWith(key));args.add(value);}
        context=new SpringApplicationBuilder(ContentApiApplication.class).run(args.toArray(String[]::new));
    }
    @Tag("C1_GATE_B") @Test void productionWorkerIsNonWebPassiveAndCleanlyStops()throws Exception {
        UUID run=queued("one");Path sentinel=Files.writeString(roots.get(BATCH).resolve("synthetic.txt"),"unchanged");var before=Files.readAttributes(sentinel,java.nio.file.attribute.BasicFileAttributes.class);byte[] selection=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);
        workerContext("--ddip.worker.dispatch-enabled=true","--ddip.worker.executor=fake");assertThat(context).isNotInstanceOf(org.springframework.boot.web.server.context.WebServerApplicationContext.class);
        assertThat(context.getBeansOfType(platform.contentapi.AnalyticsController.class)).isEmpty();assertThat(context.getBeansOfType(platform.contentapi.BoundedProcessRunner.class)).isEmpty();
        var worker=context.getBean(WorkerIdleLifecycle.class);assertThat(worker.isRunning()).isTrue();assertThat(worker.condition()).isEqualTo("READY");assertThat(worker.executionAvailable()).isFalse();long audit=count("audit_events");long version=worker.session().workerVersion();worker.tick();assertThat(worker.session().workerVersion()).isGreaterThan(version);assertThat(count("audit_events")).isEqualTo(audit);assertThat(state(run)).isEqualTo("QUEUED");
        UUID instance=worker.session().instanceId();close();assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances WHERE instance_id=?",String.class,instance)).isEqualTo("STOPPED");try(var acquired=InstallationHostGuard.acquire(controls.get("reliability-CONTROL"))){acquired.checkHeld(INSTALL);}assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(selection);assertThat(Files.readString(sentinel)).isEqualTo("unchanged");assertThat(Files.getLastModifiedTime(sentinel)).isEqualTo(before.lastModifiedTime());noEffects();
    }
    @Tag("C1_GATE_B") @Test void cleanIdleStopLeavesQueueUnchanged(){UUID run=queued("one");ready();session=repository.heartbeatIdle(guard,session);repository.stopIdle(guard,session);assertThat(state(run)).isEqualTo("QUEUED");assertThat(count("attempts")).isZero();assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances",String.class)).isEqualTo("STOPPED");}
    @Tag("C1_GATE_B") @Test void workerStartupRefusesOccupiedGuard(){ready();assertThatThrownBy(this::workerContext).hasRootCauseMessage("HOST_GUARD_OCCUPIED");assertThat(count("worker_instances")).isEqualTo(1);}
    @Tag("C1_GATE_B") @Test void productionRestartReportsRecoveryWithoutAdoptingAttempt(){queued("one");ready();var o=claim().owner();closeGuard();workerContext();var worker=context.getBean(WorkerIdleLifecycle.class);assertThat(worker.condition()).isEqualTo("RECOVERY_ONLY");worker.tick();assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(o.attemptId());close();assertThat(repository.lookup(o.attemptId()).orElseThrow().state()).isEqualTo("CLAIMED");}
    @Tag("C1_GATE_B") @Test void lifecycleHeartbeatDatabaseFailureBecomesNonDispatchingUncertainty(){queued("one");workerContext();var worker=context.getBean(WorkerIdleLifecycle.class);outage(true);try{worker.tick();assertThat(worker.condition()).isEqualTo("RECOVERY_REQUIRED");assertThat(worker.executionAvailable()).isFalse();}finally{outage(false);}worker.tick();assertThat(worker.condition()).isEqualTo("RECOVERY_REQUIRED");close();assertThat(count("attempts")).isZero();}
    @Tag("C1_GATE_B") @Test void startupDatabaseUnavailableFailsSafely(){outage(true);try{assertThatThrownBy(this::workerContext).hasMessage("MANAGEMENT_UNAVAILABLE");}finally{outage(false);}assertThat(count("worker_instances")).isZero();}
    @Tag("C1_GATE_B") @Test void schemaMismatchFailsBeforeRegistration(){jdbc.update("UPDATE ddip_management.flyway_schema_history SET checksum=checksum+1 WHERE version='7'");assertThatThrownBy(this::workerContext).hasMessage("MANAGEMENT_INCOMPATIBLE");assertThat(count("worker_instances")).isZero();}
    @Tag("C1_GATE_B") @Test void runtimeMismatchFailsBeforeRegistration(){assertThatThrownBy(()->workerContext("--ddip.worker.deployment-sha256="+"b".repeat(64))).hasRootCauseMessage("WORKER_CONFIGURATION_INVALID");assertThat(count("worker_instances")).isZero();try(var g=InstallationHostGuard.acquire(controls.get("reliability-CONTROL"))){g.checkHeld(INSTALL);}}
    @Tag("C1_GATE_B") @Test void installationMismatchFailsBeforeRegistration(){assertThatThrownBy(()->workerContext("--ddip.installation-id="+UUID.randomUUID())).hasMessage("MANAGEMENT_INCOMPATIBLE");assertThat(count("worker_instances")).isZero();}
    @Tag("C1_GATE_B") @Test void staleWorkerVersionCannotHeartbeatIdle(){ready();session=repository.heartbeatIdle(guard,session);var s=session;var stale=new WorkerRepository.Session(s.installationId(),s.instanceId(),s.profileId(),s.profileDigest(),s.deploymentDigest(),s.hostDigest(),s.status(),s.epoch(),s.coordinatorVersion(),s.workerVersion()-1);assertThatThrownBy(()->repository.heartbeatIdle(guard,stale)).hasMessage("OWNERSHIP_LOST");assertThat(count("attempts")).isZero();}
    @Tag("C1_GATE_B") @Test void ownerCannotUseIdleStopToClearActiveWork(){queued("one");ready();var o=claim().owner();assertThatThrownBy(()->repository.stopIdle(guard,o.worker())).hasMessage("WORKER_NOT_READY");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(o.attemptId());}
    @Tag("C1_GATE_B") @Test void rejectedRegistrationLeavesNoWorker(){guard=InstallationHostGuard.acquire(controls.get("reliability-CONTROL"));repository=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->repository.register(guard,JSON_PROFILE,"b".repeat(64),"a".repeat(64))).hasMessage("RUNTIME_MISMATCH");assertThat(count("worker_instances")).isZero();}

    Process child(String mode)throws Exception {
        Path descriptor=Files.createTempFile(temp,"worker-child-",".json");Files.write(descriptor,controls.get("reliability-CONTROL").descriptor());Files.setPosixFilePermissions(descriptor,PosixFilePermissions.fromString("rw-------"));
        String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));var b=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",cp,WorkerTestChild.class.getName(),descriptor.toString(),mode,JSON_PROFILE).redirectError(ProcessBuilder.Redirect.DISCARD);
        b.environment().put("DDIP_TEST_DB_URL",url);b.environment().put("DDIP_TEST_DB_USER",user);b.environment().put("DDIP_TEST_DB_PASSWORD",password);return b.start();
    }
    String readyLine(Process child)throws Exception {
        var future=CompletableFuture.supplyAsync(()->{try{var reader=new java.io.BufferedReader(new java.io.InputStreamReader(child.getInputStream()));String line;while((line=reader.readLine())!=null)if(line.startsWith("C1_"))return line;return "EOF";}catch(Exception ex){throw new CompletionException(ex);}});
        try{return future.get(20,TimeUnit.SECONDS);}catch(Exception ex){child.destroyForcibly();throw ex;}
    }
    void kill(Process p)throws Exception{p.destroyForcibly();assertThat(p.waitFor(10,TimeUnit.SECONDS)).isTrue();}
    @Tag("C1_GATE_B") @Test void separateProductionWorkersAndImporterCompeteForOneGuard()throws Exception {
        UUID run=queued("one");Process first=child("passive");try{assertThat(readyLine(first)).startsWith("C1_READY");Process second=child("passive");try{assertThat(readyLine(second)).isEqualTo("C1_HOST_GUARD_OCCUPIED");assertThat(second.waitFor(10,TimeUnit.SECONDS)).isTrue();}finally{if(second.isAlive())kill(second);}assertThatThrownBy(()->InstallationHostGuard.acquire(controls.get("reliability-CONTROL"))).hasMessage("HOST_GUARD_OCCUPIED");assertThat(state(run)).isEqualTo("QUEUED");assertThat(count("attempts")).isZero();first.getOutputStream().write('\n');first.getOutputStream().flush();assertThat(first.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances ORDER BY started_at LIMIT 1",String.class)).isEqualTo("STOPPED");}finally{if(first.isAlive())kill(first);}noEffects();
    }
    @Tag("C1_GATE_B") @Test void actualTestWorkerDeathRetainsCommittedClaimAndBlocksRestart()throws Exception {
        UUID run=queued("one");Process child=child("claim");try{assertThat(readyLine(child)).startsWith("C1_CLAIMED");assertThat(state(run)).isEqualTo("RUNNING");kill(child);}finally{if(child.isAlive())kill(child);}assertThat(count("attempts")).isEqualTo(1);ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");assertThatThrownBy(this::claim).hasMessage("WORKER_NOT_READY");noEffects();
    }
    @Tag("C1_GATE_B") @Test void unresolvedAttemptWithoutCoordinatorPointerStillBlocksStartup() {
        UUID run=queued("one");ready();jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state) VALUES (?,?,1,'EXECUTE',?,1,'CLAIMED')",UUID.randomUUID(),run,session.instanceId());closeGuard();ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");assertThatThrownBy(this::claim).hasMessage("WORKER_NOT_READY");assertThat(state(run)).isEqualTo("QUEUED");assertThat(count("attempts")).isEqualTo(1);
    }
    @Tag("C1_GATE_A") @Test void rollbackAfterFirstClaimAuditRemovesAllClaimEffects() {
        UUID run=queued("one");ready();long audits=count("audit_events");jdbc.execute("CREATE FUNCTION ddip_management.fail_second() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='ATTEMPT_CLAIMED' THEN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END IF; RETURN NEW; END $$");jdbc.execute("CREATE TRIGGER fail_second BEFORE INSERT ON ddip_management.audit_events FOR EACH ROW EXECUTE FUNCTION ddip_management.fail_second()");assertThatThrownBy(this::claim).hasMessage("STATE_CONFLICT");assertThat(count("attempts")).isZero();assertThat(count("audit_events")).isEqualTo(audits);assertThat(state(run)).isEqualTo("QUEUED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
    }
    @Tag("C1_GATE_A") @Test void claimWinsConcurrentDisableWithoutReplacingFrozenSelection()throws Exception {
        UUID run=queued("one");ready();byte[] frozen=jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class);var atCommit=new CountDownLatch(1);var release=new CountDownLatch(1);var updating=new CountDownLatch(1);var once=new AtomicBoolean(true);
        var ds=new AbstractDataSource(){public Connection getConnection()throws SQLException{var real=jdbc.getDataSource().getConnection();return(Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(p,m,args)->{try{if(m.getName().equals("commit") && once.compareAndSet(true,false)){atCommit.countDown();assertThat(release.await(5,TimeUnit.SECONDS)).isTrue();}return m.invoke(real,args);}catch(InvocationTargetException ex){throw ex.getCause();}});}public Connection getConnection(String u,String p)throws SQLException{return getConnection();}};
        var wrapped=new JdbcTemplate(ds);var claiming=WorkerTestSupport.enabled(wrapped,transactions(wrapped),INSTALL);
        try(var pool=Executors.newFixedThreadPool(2)){var claim=pool.submit(()->claiming.claim(guard,session,UUID.randomUUID()));assertThat(atCommit.await(5,TimeUnit.SECONDS)).isTrue();var update=pool.submit(()->{updating.countDown();disable();});assertThat(updating.await(5,TimeUnit.SECONDS)).isTrue();awaitCoordinatorWait();release.countDown();assertThat(claim.get(10,TimeUnit.SECONDS).status()).isEqualTo("CLAIMED");update.get(10,TimeUnit.SECONDS);}
        assertThat(state(run)).isEqualTo("RUNNING");assertThat(jdbc.queryForObject("SELECT enabled FROM ddip_management.sources WHERE source_id='demo-requests'",Boolean.class)).isFalse();assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs",byte[].class)).isEqualTo(frozen);
    }
    @Tag("C1_GATE_A") @ParameterizedTest @ValueSource(strings={"snapshot","request","input","storage"})
    void invalidCapturedMetadataFailsWithoutNativeVerification(String mutation) {
        UUID original=queued("original");var row=jdbc.queryForMap("SELECT * FROM ddip_management.runs WHERE run_id=?",original);jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=now(),row_version=row_version+1 WHERE run_id=?",original);
        var captured=(tools.jackson.databind.node.ObjectNode)SourceJson.parse((byte[])row.get("captured_selection"),16*1024*1024);var request=(tools.jackson.databind.node.ObjectNode)SourceJson.parse((byte[])row.get("normalized_request"));
        switch(mutation){case "snapshot"->captured.put("selectionVersion",2);case "request"->((tools.jackson.databind.node.ObjectNode)request.get("sources").get(0)).put("revision",2);case "input"->((tools.jackson.databind.node.ObjectNode)captured.get("inputs").get(0)).put("evidenceSha256","b".repeat(64));case "storage"->((tools.jackson.databind.node.ObjectNode)captured.get("storageBindings").get(0)).put("configurationSha256","b".repeat(64));default->throw new AssertionError();}
        byte[] cb=SourceJson.canonical(captured),rb=SourceJson.canonical(request);UUID run=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state) VALUES (?,?,'RELIABILITY_V2',1,'local-operator','malformed',?,?,?,?,?,'QUEUED')",run,RELIABILITY,rb,SourceJson.sha(rb),cb,SourceJson.sha(cb),JSON_PROFILE);
        jdbc.update("INSERT INTO ddip_management.run_sources SELECT ?,product_id,source_id,revision,captured_gate_version,captured_enabled FROM ddip_management.run_sources WHERE run_id=?",run,original);jdbc.update("INSERT INTO ddip_management.run_inputs SELECT ?,role,ordinal,artifact_id,evidence_bytes,evidence_sha256 FROM ddip_management.run_inputs WHERE run_id=?",run,original);
        ready();assertThat(claim().code()).isEqualTo("RUNTIME_MISMATCH");assertThat(state(run)).isEqualTo("FAILED");assertThat(count("attempts")).isZero();noEffects();
    }
    @Tag("C1_GATE_A") @Test void stageIntentMetadataIsFencedAndRollsBackAtomically() {
        queued("one");ready();var owner=repository.markRunning(guard,claim().owner());assertThatThrownBy(()->repository.stageIntentTransaction(guard,owner,()->{jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?)",owner.attemptId(),UUID.randomUUID(),"a".repeat(64));throw new IllegalStateException("TEST_ROLLBACK");})).hasMessage("TEST_ROLLBACK");assertThat(count("stage_invocations")).isZero();
        var next=repository.heartbeat(guard,owner);assertThatThrownBy(()->repository.stageIntentTransaction(guard,owner,()->{throw new AssertionError("STALE_CALLBACK");})).hasMessage("OWNERSHIP_LOST");assertThat(next.attemptState()).isEqualTo("RUNNING");
    }
    @Tag("C1_GATE_A") @Test void committedCancellationPreventsAttemptStart() {
        queued("one");ready();var owner=claim().owner();byte[] b=SourceJson.canonical(SourceJson.object());jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','cancel',?,?,?,'REQUESTED','local-operator')",UUID.randomUUID(),owner.runId(),b,SourceJson.sha(b),owner.runVersion());assertThatThrownBy(()->repository.markRunning(guard,owner)).hasMessage("WORKER_NOT_READY");assertThat(repository.lookup(owner.attemptId()).orElseThrow().state()).isEqualTo("CLAIMED");noEffects();
    }
    @Tag("C1_GATE_B") @Test void productionWorkerRespectsStoppingCoordinator() {jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");workerContext();assertThat(context.getBean(WorkerIdleLifecycle.class).condition()).isEqualTo("STOPPING");assertThat(context.getBean(WorkerIdleLifecycle.class).isRunning()).isFalse();assertThat(count("attempts")).isZero();close();assertThat(jdbc.queryForObject("SELECT mode FROM ddip_management.coordinator",String.class)).isEqualTo("STOPPING");}

    @Tag("C1_GATE_B") @Test void historicalHostBindingCannotBeSilentlyReplaced(){ready();repository.stopIdle(guard,session);closeGuard();assertThatThrownBy(()->ready(CONTENT_PROFILE)).hasMessage("WORKER_CONFIGURATION_INVALID");assertThat(count("worker_instances")).isEqualTo(1);}
    @Tag("C1_GATE_B") @Test void ownerOperationsRejectAnOuterTransaction(){ready();assertThatThrownBy(()->tx.required(()->claim())).hasMessage("WORKER_CONFIGURATION_INVALID");assertThat(count("attempts")).isZero();}
    @Tag("C1_GATE_A") @Test void validContentMetadataCanBeClaimedOnlyByTestCapability(){UUID run=UUID.fromString(submit(content(),"content").get("runId").stringValue());ready(CONTENT_PROFILE);assertThat(claim().runId()).isEqualTo(run);noEffects();}
    @Tag("C1_GATE_A") @Test void validParquetMetadataCanBeClaimedOnlyByTestCapability(){UUID run=UUID.fromString(submit(reliability(true),"parquet").get("runId").stringValue());ready(PARQUET_PROFILE);assertThat(claim().runId()).isEqualTo(run);noEffects();}

    void awaitCoordinatorWait()throws Exception {
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
        while(System.nanoTime()<deadline){if(Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=? AND wait_event_type='Lock' AND query LIKE 'SELECT installation_id%')",Boolean.class,database)))return;Thread.sleep(10);}
        throw new AssertionError("EXPECTED_COORDINATOR_LOCK_WAIT");
    }
    Map<String,String> namespaceSnapshot()throws Exception {
        Map<String,String> out=new TreeMap<>();
        for(var entry:roots.entrySet())try(var paths=Files.walk(entry.getValue())) {
            for(Path p:paths.toList())out.put(entry.getKey()+"/"+entry.getValue().relativize(p),Files.getLastModifiedTime(p).toString()+":"+(Files.isRegularFile(p,LinkOption.NOFOLLOW_LINKS)?SourceJson.sha(Files.readAllBytes(p)):"DIRECTORY"));
        }
        return out;
    }

}
