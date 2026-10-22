package platform.contentapi.execution;

import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.function.BooleanSupplier;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.boot.builder.SpringApplicationBuilder;
import org.springframework.context.ConfigurableApplicationContext;
import platform.contentapi.ContentApiApplication;
import platform.contentapi.management.*;
import platform.contentapi.importing.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Actual packaged lifecycle/capability, loopback HTTP, native Spark and disposable PostgreSQL. */
class ReliabilityWorkerIT extends ReliabilityExecutionFixtures {
    ConfigurableApplicationContext api,workerContext;HttpClient client;String base;JsonNode request;
    @Override boolean productionSetup(){return true;}
    @Override void finishSetup(JsonNode request,JsonNode frozen)throws Exception {
        this.request=request;startApi();
        for(var source:frozen.get("sources")) {
            var body=SourceJson.object().put("productId","application-reliability").put("sourceId",source.get("sourceId").stringValue()).put("displayName","Reviewed synthetic fixture").put("enabled",true);body.set("config",source.get("configuration"));
            assertThat(write("/sources",body,"source-"+source.get("sourceId").stringValue(),null).statusCode()).isEqualTo(201);
        }
        // Exercise the real fenced importer before starting the production worker.
        String importProfile="worker-fixture-import-v1";Path processing=repo.resolve("processing").toRealPath();
        var runtime=SourceJson.object().put("importProtocolVersion",1).put("python",python.toString()).put("processingRoot",processing.toString()).put("pythonVersion","3.12.15").put("duckdbVersion","1.4.5").put("timeoutSeconds",30);var modules=runtime.putObject("modules");
        for(var name:ImportBindings.MODULES)modules.put(name,SourceJson.sha(Files.readAllBytes(processing.resolve(name))));insertProfile(importProfile,runtime);
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.execution_profile_storage WHERE profile_id=?",importProfile,PROFILE);
        var catalog=SourceJson.object().put("catalogVersion",1).put("operationId",UUID.randomUUID().toString()).put("executionProfileId",importProfile).put("controlBindingId",binding.id());var entries=catalog.putArray("artifacts");for(var input:frozen.get("inputs"))entries.add(input.get("qualification").get("selection"));
        var imported=new OperatorImportService(new ImportBindings(jdbc,tx,INSTALL),new ImportOwnershipRepository(jdbc,tx,INSTALL),new JdbcCoordinatorRepository(jdbc,tx,INSTALL),new NativeImportVerifier(new NativeImportProcess()),INSTALL).execute(ImportCatalog.parse(SourceJson.canonical(catalog)));
        assertThat(imported.get("status").stringValue()).isEqualTo("VERIFIED");var batches=SourceJson.object().putArray("batches");
        for(var row:jdbc.queryForList("SELECT artifact_id,kind FROM ddip_management.artifacts ORDER BY artifact_id"))if(row.get("kind").equals("RELIABILITY_BATCH_V2"))batches.add(row.get("artifact_id").toString());else ((tools.jackson.databind.node.ObjectNode)request).put("serviceReferenceArtifactId",row.get("artifact_id").toString());
        ((tools.jackson.databind.node.ObjectNode)request).set("batchArtifactIds",batches);run=submit("first");
    }
    void startApi()throws Exception {
        int port;try(var socket=new java.net.ServerSocket(0,1,InetAddress.getLoopbackAddress())){port=socket.getLocalPort();}
        base="http://127.0.0.1:"+port;
        var args=common("api");args.addAll(List.of("--server.address=127.0.0.1","--server.port="+port,"--ddip.management.catalog="+work.resolve("source-catalog.json"),"--ddip.management.cursor-key="+Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]),"--ddip.management.origins="+base));
        api=new SpringApplicationBuilder(ContentApiApplication.class).run(args.toArray(String[]::new));client=HttpClient.newHttpClient();
    }
    ArrayList<String> common(String role){return new ArrayList<>(List.of("--ddip.role="+role,"--ddip.database.url="+url,"--ddip.database.username="+user,"--ddip.database.password="+password,"--ddip.installation-id="+INSTALL,"--logging.level.root=OFF","--spring.main.banner-mode=off"));}
    ArrayList<String> workerArguments(String... extra) {
        var args=common("worker");String digest=fixture.get("command").get("runtime").get("configuration").get("deploymentSha256").stringValue();
        args.addAll(List.of("--ddip.worker.execution-profile-id="+PROFILE,"--ddip.worker.runtime-profile-id="+RUNTIME,"--ddip.worker.control-binding-id="+binding.id(),"--ddip.worker.deployment-sha256="+digest,"--ddip.worker.poll-millis=250","--ddip.worker.python="+python,"--ddip.worker.java-home="+System.getProperty("java.home"),"--ddip.worker.processing-root="+fixture.get("command").get("runtime").get("configuration").get("processingRoot").stringValue()));
        for(var value:extra){var key=value.substring(0,value.indexOf('=')+1);args.removeIf(v->v.startsWith(key));args.add(value);}
        return args;
    }
    WorkerIdleLifecycle startWorker(String... extra) {
        workerContext=new SpringApplicationBuilder(ContentApiApplication.class).run(workerArguments(extra).toArray(String[]::new));return workerContext.getBean(WorkerIdleLifecycle.class);
    }
    HttpResponse<String> write(String suffix,JsonNode body,String key,String version)throws Exception {
        var b=HttpRequest.newBuilder(URI.create(base+"/api/management/v1"+suffix)).timeout(Duration.ofSeconds(10)).header("Content-Type","application/json").header("Origin",base).header("X-DDIP-Local-Request","1").header("Idempotency-Key",key);
        if(version!=null)b.header("If-Match",version);
        return client.send(b.POST(HttpRequest.BodyPublishers.ofByteArray(SourceJson.canonical(body))).build(),HttpResponse.BodyHandlers.ofString());
    }
    UUID submit(String key)throws Exception {var r=write("/runs",request,key,null);assertThat(r.statusCode()).withFailMessage(r.body()).isEqualTo(202);return UUID.fromString(SourceJson.parse(r.body().getBytes()).get("data").get("runId").stringValue());}
    JsonNode get(UUID id,String suffix)throws Exception {var r=client.send(HttpRequest.newBuilder(URI.create(base+"/api/management/v1/runs/"+id+suffix)).GET().build(),HttpResponse.BodyHandlers.ofString());assertThat(r.statusCode()).isEqualTo(200);assertThat(r.body()).doesNotContain(work.toString(),"command.json","retained.json");return SourceJson.parse(r.body().getBytes()).get("data");}
    void await(BooleanSupplier condition,int seconds)throws Exception {long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(seconds);while(!condition.getAsBoolean() && System.nanoTime()<until)Thread.sleep(20);assertThat(condition.getAsBoolean()).isTrue();}
    void finished(UUID id)throws Exception {await(()->Set.of("SUCCEEDED","FAILED","RECOVERY_REQUIRED").contains(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,id)),100);}
    void idle(WorkerIdleLifecycle w)throws Exception {await(()->w.session().coordinatorVersion()==jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class) && jdbc.queryForObject("SELECT active_attempt_id IS NULL FROM ddip_management.coordinator",Boolean.class),10);}
    @AfterEach void closeContexts(){if(workerContext!=null){workerContext.close();workerContext=null;}if(api!=null){api.close();api=null;}if(client!=null)client.close();}
    Path output(){return Path.of(fixture.get("bundle").get("bindings").get("output").get("root").stringValue());}
    Map<String,String> snapshot(Path root)throws Exception {var m=new TreeMap<String,String>();try(var paths=Files.walk(root)){for(var p:paths.toList())m.put(p.toString(),Files.getLastModifiedTime(p)+":"+(Files.isRegularFile(p)?SourceJson.sha(Files.readAllBytes(p)):"directory"));}return m;}
    @ParameterizedTest @ValueSource(strings={"JSONL","PARQUET"}) void realHttpToWorkerSuccessReplayAndIdleShutdown(String format)throws Exception {
        if(format.equals("PARQUET")){closeContexts();jdbc.execute("DROP SCHEMA ddip_management CASCADE");new SchemaLifecycle(Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load(),jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);provision(format);}
        var nativeBefore=snapshot(Path.of(fixture.get("bundle").get("bindings").get("batch").get("root").stringValue()));var referenceBefore=snapshot(Path.of(fixture.get("bundle").get("bindings").get("reference").get("root").stringValue()));
        assertThat(state()).isEqualTo("QUEUED");var w=startWorker();assertThat(workerContext).isNotInstanceOf(org.springframework.boot.web.server.context.WebServerApplicationContext.class);assertThat(workerContext.getBeansOfType(platform.contentapi.AnalyticsController.class)).isEmpty();assertThat(workerContext.getBeansOfType(platform.contentapi.BoundedProcessRunner.class)).isEmpty();
        // Many concurrent wake requests cannot perform an idle heartbeat with an active stale token.
        try(var pool=Executors.newFixedThreadPool(3)){for(int i=0;i<30;i++)pool.submit(w::tick);}
        finished(run);assertThat(state()).isEqualTo("SUCCEEDED");idle(w);UUID artifact=jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run);
        assertThat(get(run,"").get("result").get("verificationStatus").stringValue()).isEqualTo("VERIFIED_AT_EXECUTION");assertThat(get(run,"").get("counts").get("occurrences").stringValue()).isEqualTo("10");assertThat(get(run,"/attempts").get(0).get("stageCount").stringValue()).isEqualTo("3");assertThat(new String(SourceJson.canonical(get(run,"/events")))).contains("RUN_SUCCEEDED","STAGE_VERIFIED");
        assertThat(jdbc.queryForList("SELECT stage FROM ddip_management.stage_invocations WHERE state='VERIFIED' ORDER BY started_at",String.class)).containsExactly("CHECK_INPUTS","PUBLISH_RELIABILITY","VERIFY_RESULT");
        var before=snapshot(output());UUID replay=submit("replay");finished(replay);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,replay)).isEqualTo("SUCCEEDED");idle(w);
        assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,replay)).isEqualTo(artifact);assertThat(snapshot(output())).isEqualTo(before);assertThat(snapshot(Path.of(fixture.get("bundle").get("bindings").get("batch").get("root").stringValue()))).isEqualTo(nativeBefore);assertThat(snapshot(Path.of(fixture.get("bundle").get("bindings").get("reference").get("root").stringValue()))).isEqualTo(referenceBefore);
        UUID instance=w.session().instanceId();workerContext.close();workerContext=null;assertThat(w.condition()).isEqualTo("STOPPED");assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances WHERE instance_id=?",String.class,instance)).isEqualTo("STOPPED");try(var g=InstallationHostGuard.acquire(binding)){g.checkHeld(INSTALL);}
        api.close();api=null;client.close();
        var identity=jdbc.queryForList("SELECT run_id,submission_seq,selection_sha256,result_artifact_id FROM ddip_management.runs ORDER BY submission_seq");
        String signal=System.getProperty("ddip.test.production-restart-signal");if(signal!=null && format.equals("JSONL")){var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);Files.writeString(Path.of(signal),"READY");await(()->Files.exists(Path.of(signal+".done")),45);assertThat(jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class)).isAfter(started);}
        startApi();assertThat(get(run,"").get("result").get("artifactId").stringValue()).isEqualTo(artifact.toString());assertThat(jdbc.queryForList("SELECT run_id,submission_seq,selection_sha256,result_artifact_id FROM ddip_management.runs ORDER BY submission_seq")).isEqualTo(identity);
    }
    @Test void cleanPreflightFailureReleasesAndWorkerContinues()throws Exception {
        closeContexts();jdbc.execute("DROP SCHEMA ddip_management CASCADE");new SchemaLifecycle(Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load(),jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);provision("JSONL",true);
        Path batch=Path.of(fixture.get("bundle").get("bindings").get("batch").get("root").stringValue());Path raw;
        try(var p=Files.walk(batch)){raw=p.filter(v->v.toString().contains("batch_id=b00/") && v.toString().endsWith(".jsonl")).findFirst().orElseThrow();}Files.writeString(raw,"corrupt");
        var w=startWorker();finished(run);assertThat(state()).isEqualTo("FAILED");idle(w);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations",String.class)).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT retry_eligible FROM ddip_management.runs",Boolean.class)).isFalse();
        // An unrelated retained batch is still valid. The failed native bytes are never repaired.
        var ids=SourceJson.object().putArray("ids");for(var input:fixture.get("bundle").get("capturedSelection").get("inputs"))if(input.get("role").stringValue().equals("RELIABILITY_BATCH") && input.get("qualification").get("selection").get("identity").get("batchId").stringValue().equals("b01"))ids.add(jdbc.queryForObject("SELECT artifact_id FROM ddip_management.artifacts WHERE kind='RELIABILITY_BATCH_V2' AND native_identity=?",UUID.class,input.get("nativeIdentity").stringValue()).toString());
        ((tools.jackson.databind.node.ObjectNode)request).set("batchArtifactIds",ids);UUID next=submit("unrelated-selection-new-run");finished(next);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,next)).isEqualTo("SUCCEEDED");idle(w);assertThat(w.condition()).isEqualTo("READY");assertThat(Files.readString(raw)).isEqualTo("corrupt");
    }
    @Test void activeShutdownRetainsOccupiedRecoveryAndRestartRefuses()throws Exception {
        var w=startWorker();await(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.stage_invocations WHERE stage='PUBLISH_RELIABILITY' AND child_pid IS NOT NULL AND state='RUNNING')",Boolean.class),30);
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class);await(()->jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class)>version,15);
        w.stop();assertThat(w.isRunning()).isFalse();assertThat(w.condition()).isEqualTo("RECOVERY_REQUIRED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNotNull();assertThat(state()).isNotEqualTo("SUCCEEDED");workerContext.close();workerContext=null;
        var next=startWorker();assertThat(next.isRunning()).isFalse();assertThat(next.condition()).isEqualTo("RECOVERY_ONLY");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(1);
    }
    @ParameterizedTest @ValueSource(strings={"STOPPING","RECOVERY_ONLY"}) void blockedCoordinatorNeverDispatches(String mode)throws Exception {
        jdbc.update("UPDATE ddip_management.coordinator SET mode=?,row_version=row_version+1",mode);var w=startWorker();assertThat(w.isRunning()).isFalse();assertThat(w.condition()).isEqualTo(mode);assertThat(state()).isEqualTo("QUEUED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isZero();
    }
    @Test void idleHeartbeatAndWakeupsStayOnOneOwnerThread()throws Exception {
        jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=clock_timestamp(),row_version=row_version+1 WHERE run_id=?",run);var w=startWorker();long version=w.session().workerVersion();
        try(var pool=Executors.newFixedThreadPool(4)){for(int i=0;i<100;i++)pool.submit(w::tick);}await(()->w.session().workerVersion()>version,10);assertThat(w.condition()).isEqualTo("READY");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isZero();w.stop();assertThat(w.condition()).isEqualTo("STOPPED");
    }
    @Test void databaseFailureStopsPollingWithoutRefreshingToken()throws Exception {
        jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=clock_timestamp(),row_version=row_version+1 WHERE run_id=?",run);var w=startWorker();outage(true);try{w.tick();await(()->!w.isRunning(),10);assertThat(w.condition()).isEqualTo("RECOVERY_REQUIRED");}finally{outage(false);}api.close();client.close();startApi();UUID next=submit("after-outage");w.tick();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,next)).isEqualTo("QUEUED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isZero();
    }
    @Test void otherApprovedProfileRemainsQueuedAheadOfEligibleWork()throws Exception {
        String other="10000000-0000-4000-8000-000000000099";jdbc.update("INSERT INTO ddip_management.execution_profiles SELECT ?,configuration_bytes,configuration_sha256,created_at FROM ddip_management.execution_profiles WHERE profile_id=?",other,PROFILE);jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.execution_profile_storage WHERE profile_id=?",other,PROFILE);
        jdbc.update("UPDATE ddip_management.runs SET state='FAILED',finished_at=clock_timestamp(),row_version=row_version+1 WHERE run_id=?",run);((tools.jackson.databind.node.ObjectNode)request).put("executionProfileId",other);UUID waiting=submit("other-profile");((tools.jackson.databind.node.ObjectNode)request).put("executionProfileId",PROFILE);UUID eligible=submit("eligible");var w=startWorker();finished(eligible);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,eligible)).isEqualTo("SUCCEEDED");idle(w);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,waiting)).isEqualTo("QUEUED");
    }
    @Test void actualProductionJvmDeathBeforeLaunchLeavesOccupiedSlot()throws Exception {
        jdbc.execute("CREATE FUNCTION ddip_management.hold_intent() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN PERFORM pg_advisory_xact_lock(1298374); RETURN NEW; END $$");jdbc.execute("CREATE TRIGGER hold_intent BEFORE INSERT ON ddip_management.stage_invocations FOR EACH ROW EXECUTE FUNCTION ddip_management.hold_intent()");
        try(var connection=jdbc.getDataSource().getConnection()) {
            connection.createStatement().execute("SELECT pg_advisory_lock(1298374)");
            var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path"),ProductionWorkerChild.class.getName()));
            var args=workerArguments();args.removeIf(a->a.startsWith("--ddip.database."));command.addAll(args);
            var builder=new ProcessBuilder(command).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD);builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);
            var child=builder.start();try {
                await(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=? AND wait_event='advisory')",Boolean.class,database),20);
                assertThat(state()).isEqualTo("RUNNING");child.destroyForcibly();assertThat(child.waitFor(10,TimeUnit.SECONDS)).isTrue();
            }finally{if(child.isAlive()){child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);}}
        }
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isZero();assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNotNull();var w=startWorker();assertThat(w.condition()).isEqualTo("RECOVERY_ONLY");assertThat(w.isRunning()).isFalse();assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(1);
    }
    @Test void lostFinalCommitAcknowledgmentStopsProductionLoopAfterRealSuccess()throws Exception {
        UUID waiting=submit("must-remain-queued");var once=new java.util.concurrent.atomic.AtomicBoolean();
        var data=new org.springframework.jdbc.datasource.AbstractDataSource(){public java.sql.Connection getConnection()throws java.sql.SQLException{var real=jdbc.getDataSource().getConnection();var match=new java.util.concurrent.atomic.AtomicBoolean();return(java.sql.Connection)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{java.sql.Connection.class},(p,m,a)->{try{if(m.getName().equals("prepareStatement") && a[0] instanceof String sql && sql.contains("UPDATE ddip_management.runs SET state='SUCCEEDED'"))match.set(true);var result=m.invoke(real,a);if(m.getName().equals("commit") && match.get() && once.compareAndSet(false,true))throw new java.sql.SQLException("PRIVATE","08006");return result;}catch(java.lang.reflect.InvocationTargetException ex){throw ex.getCause();}});}public java.sql.Connection getConnection(String u,String p)throws java.sql.SQLException{return getConnection();}};
        var fault=new org.springframework.jdbc.core.JdbcTemplate(data);var env=new org.springframework.mock.env.MockEnvironment();for(String arg:workerArguments()){int split=arg.indexOf('=');env.setProperty(arg.substring(2,split),arg.substring(split+1));}
        // The real production lifecycle/capability/adapters; only the JDBC acknowledgment is faulted.
        var w=new WorkerIdleLifecycle(fault,transactions(fault),new DatabaseSettings(url,user,password,INSTALL),env);try{w.start();await(()->!w.isRunning(),100);assertThat(w.condition()).isEqualTo("RECOVERY_REQUIRED");assertThat(once.get()).isTrue();assertThat(state()).isEqualTo("SUCCEEDED");assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,waiting)).isEqualTo("QUEUED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(1);}finally{w.stop();}
    }
    @Test void databaseOutageDuringNativeWorkBlocksAnotherClaim()throws Exception {
        UUID waiting=submit("waiting-after-outage");var w=startWorker();await(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM ddip_management.stage_invocations WHERE stage='PUBLISH_RELIABILITY' AND child_pid IS NOT NULL)",Boolean.class),30);
        outage(true);try{await(()->!w.isRunning(),25);}finally{outage(false);}assertThat(w.condition()).isEqualTo("RECOVERY_REQUIRED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNotNull();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,waiting)).isEqualTo("QUEUED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(1);
    }
    @Test void incompleteOutputIsRetainedAndFailureReleasesConfirmedToken()throws Exception {
        Path namespace=output().resolve("v2/processor_revision=2/publication=3b9354d578f017cefea8e9ace2556c6ed474afec606722ee01e9fba8e4a39a7a");Files.createDirectories(namespace);Path retained=Files.writeString(namespace.resolve("retain-for-owner"),"incomplete");var w=startWorker();finished(run);assertThat(state()).isEqualTo("FAILED");idle(w);assertThat(jdbc.queryForObject("SELECT outcome_code FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("INCOMPLETE_OUTPUT");assertThat(jdbc.queryForObject("SELECT retry_eligible FROM ddip_management.runs WHERE run_id=?",Boolean.class,run)).isFalse();assertThat(Files.readString(retained)).isEqualTo("incomplete");assertThat(w.condition()).isEqualTo("READY");
    }
    @Test void publisherSuccessWithInvalidIndependentOutputStopsDispatch()throws Exception {
        UUID waiting=submit("must-not-follow-invalid-output");jdbc.execute("CREATE FUNCTION ddip_management.hold_verify() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.stage='VERIFY_RESULT' THEN PERFORM pg_advisory_xact_lock(1298375); END IF; RETURN NEW; END $$");jdbc.execute("CREATE TRIGGER hold_verify BEFORE INSERT ON ddip_management.stage_invocations FOR EACH ROW EXECUTE FUNCTION ddip_management.hold_verify()");
        try(var connection=jdbc.getDataSource().getConnection()) {
            connection.createStatement().execute("SELECT pg_advisory_lock(1298375)");var w=startWorker();await(()->jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=? AND wait_event='advisory')",Boolean.class,database),40);
            assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations WHERE stage='PUBLISH_RELIABILITY'",String.class)).isEqualTo("VERIFIED");Path marker;try(var paths=Files.walk(output())){marker=paths.filter(p->p.getFileName().toString().equals("complete.json")).findFirst().orElseThrow();}Files.writeString(marker,"{}");connection.createStatement().execute("SELECT pg_advisory_unlock(1298375)");await(()->!w.isRunning(),20);assertThat(state()).isNotEqualTo("SUCCEEDED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNotNull();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,waiting)).isEqualTo("QUEUED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isZero();
        }
    }
    @ParameterizedTest @ValueSource(strings={"249","5001","invalid"}) void pollingBoundsFailClosed(String value){assertThatThrownBy(()->startWorker("--ddip.worker.poll-millis="+value)).hasRootCauseMessage("WORKER_CONFIGURATION_INVALID");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class)).isZero();assertThat(state()).isEqualTo("QUEUED");}
    @Test void invalidRuntimeCannotRegisterOrClaim(){assertThatThrownBy(()->startWorker("--ddip.worker.runtime-profile-id=missing")).hasRootCauseMessage("WORKER_CONFIGURATION_INVALID");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class)).isZero();assertThat(state()).isEqualTo("QUEUED");}
    @Test void secondWorkerJvmCannotEnterEvenWithDifferentApprovedProfile()throws Exception {
        String other="10000000-0000-4000-8000-000000000098",runtime="second-approved-runtime";
        jdbc.update("INSERT INTO ddip_management.execution_profiles SELECT ?,configuration_bytes,configuration_sha256,created_at FROM ddip_management.execution_profiles WHERE profile_id=?",other,PROFILE);jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.execution_profile_storage WHERE profile_id=?",other,PROFILE);
        var config=fixture.get("command").get("runtime").get("configuration").deepCopy();((tools.jackson.databind.node.ObjectNode)config).put("runProfileId",other);insertProfile(runtime,config);jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.execution_profile_storage WHERE profile_id=?",runtime,PROFILE);
        startWorker();var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path"),ProductionWorkerChild.class.getName()));var args=workerArguments("--ddip.worker.execution-profile-id="+other,"--ddip.worker.runtime-profile-id="+runtime);args.removeIf(a->a.startsWith("--ddip.database."));command.addAll(args);
        var builder=new ProcessBuilder(command).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(work.resolve("competing-worker.log").toFile());builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);var child=builder.start();try{assertThat(child.waitFor(20,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isNotZero();assertThat(Files.readString(work.resolve("competing-worker.log"))).contains("HOST_GUARD_OCCUPIED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class)).isEqualTo(1);}finally{if(child.isAlive()){child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);}}
    }
    @Test void zeroExitFromWrongInterpreterCannotPassRuntimeReadiness()throws Exception {
        var config=fixture.get("command").get("runtime").get("configuration").deepCopy();((tools.jackson.databind.node.ObjectNode)config).put("python","/usr/bin/true");insertProfile("wrong-interpreter",config);jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT 'wrong-interpreter',binding_id FROM ddip_management.execution_profile_storage WHERE profile_id=?",PROFILE);
        assertThatThrownBy(()->startWorker("--ddip.worker.runtime-profile-id=wrong-interpreter","--ddip.worker.python=/usr/bin/true")).hasRootCauseMessage("WORKER_CONFIGURATION_INVALID");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class)).isZero();assertThat(state()).isEqualTo("QUEUED");
    }
}
