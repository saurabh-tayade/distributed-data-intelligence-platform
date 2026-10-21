package platform.contentapi.execution;

import java.lang.reflect.*;
import java.nio.file.*;
import java.sql.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Gate B: actual Spark success + independent child + atomic PostgreSQL result; no production capability. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ReliabilityExecutionIT extends ReliabilityExecutionFixtures {
    ReliabilityPreflight.Verified prepared;
    @BeforeEach void preflight(){prepared=verify();}
    ReliabilityExecution adapter(){return new ReliabilityExecution(jdbc,tx,workers);}
    ReliabilityExecution adapter(ReadOnlyVerifierProcess p,ReadOnlyVerifierProcess v){return new ReliabilityExecution(jdbc,tx,workers,p,v);}
    ReliabilityExecution.Completed execute(){return adapter().run(guard,prepared,RUNTIME);}
    void blocked(){assertThat(state()).isEqualTo("RECOVERY_REQUIRED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isZero();assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs",UUID.class)).isNull();}
    Path nativeOutput(){return Path.of(fixture.get("bundle").get("bindings").get("output").get("root").stringValue());}
    boolean hasNativeFiles(){try(var p=Files.list(nativeOutput())){return p.findAny().isPresent();}catch(Exception e){throw new AssertionError(e);}}
    @Test void realJsonlSuccessAndImmutableReplayReuseOneRegisteredNativeResult()throws Exception {
        var completed=execute();assertSuccess(completed.artifactId());
        var before=new TreeMap<String,String>();try(var paths=Files.walk(nativeOutput())){for(var p:paths.filter(Files::isRegularFile).toList())before.put(p.toString(),Files.getLastModifiedTime(p)+":"+SourceJson.sha(Files.readAllBytes(p)));}
        // New durable submission, exact same frozen input IDs; native replay must not rewrite output.
        var request=jdbc.queryForObject("SELECT normalized_request FROM ddip_management.runs WHERE run_id=?",byte[].class,run);
        var catalog=SourceJson.object().put("catalogVersion",1);catalog.putArray("endpoints");var profiles=catalog.putArray("reliabilityProfiles");var stores=catalog.putArray("storageProfiles");
        for(var source:fixture.get("bundle").get("capturedSelection").get("sources")){var c=source.get("configuration");profiles.addObject().put("sourceId",source.get("sourceId").stringValue()).put("profile",c.get("profile").stringValue()).put("dataOrigin","synthetic").put("registeredProfileSha256",c.get("registeredProfileSha256").stringValue());stores.addObject().put("profileId",c.get("inputStorageProfileId").stringValue()).put("productId","application-reliability").put("role","BATCH");}
        run=UUID.fromString(new RunRepository(jdbc,tx,INSTALL,PreflightCatalogFixture.from(catalog)).submit(RunCommands.parse(request,"replay"),UUID.randomUUID().toString()).get("runId").stringValue());
        owner=workers.claim(guard,completed.worker(),UUID.randomUUID()).owner();checkpoint=UUID.randomUUID();prepared=verify();
        var replay=execute();assertThat(replay.artifactId()).isEqualTo(completed.artifactId());assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isEqualTo(1);
        for(var entry:before.entrySet()){Path p=Path.of(entry.getKey());assertThat(Files.getLastModifiedTime(p)+":"+SourceJson.sha(Files.readAllBytes(p))).isEqualTo(entry.getValue());}
        assertSuccess(replay.artifactId());
    }
    void assertSuccess(UUID artifact) {
        assertThat(state()).isEqualTo("SUCCEEDED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForList("SELECT stage FROM ddip_management.stage_invocations WHERE attempt_id=? AND state='VERIFIED' ORDER BY CASE stage WHEN 'CHECK_INPUTS' THEN 0 WHEN 'PUBLISH_RELIABILITY' THEN 1 ELSE 2 END",String.class,owner.attemptId())).containsExactly("CHECK_INPUTS","PUBLISH_RELIABILITY","VERIFY_RESULT");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifact_dependencies WHERE artifact_id=?",Long.class,artifact)).isEqualTo(2);
        var catalog=SourceJson.object().put("catalogVersion",1);for(String key:List.of("endpoints","reliabilityProfiles","storageProfiles"))catalog.putArray(key);
        var detail=new RunRepository(jdbc,tx,INSTALL,PreflightCatalogFixture.from(catalog)).get(run.toString());assertThat(detail.get("result").get("verificationStatus").stringValue()).isEqualTo("VERIFIED_AT_EXECUTION");assertThat(detail.get("result").get("nativeFingerprint").stringValue()).isEqualTo(prepared.nativeDependencyFingerprint());assertThat(detail.get("counts").get("occurrences").stringValue()).isEqualTo("10");
        var history=new RunHistoryRepository(jdbc,tx,PreflightCatalogFixture.cursor());assertThat(history.attempts(run.toString(),20,null).data().get(0).get("stageCount").stringValue()).isEqualTo("3");
        assertThat(new String(SourceJson.canonical(history.events(run.toString(),100,null).data()),java.nio.charset.StandardCharsets.UTF_8)).contains("RUN_SUCCEEDED","STAGE_VERIFIED").doesNotContain("command.json","retained.json",nativeOutput().toString());
    }
    @Test void realParquetSuccess()throws Exception {stop();jdbc.execute("DROP SCHEMA ddip_management CASCADE");new SchemaLifecycle(org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load(),jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);provision("PARQUET");prepared=verify();assertSuccess(execute().artifactId());}
    @Test void intentAndAuthorizationAreCommittedBeforeActualPublisherLaunch() {
        var p=new ReadOnlyVerifierProcess(true){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer o){assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations WHERE stage='PUBLISH_RELIABILITY'",String.class)).isEqualTo("INTENT");return super.run(c,e,d,t,new Observer(){public void beforeStart(){o.beforeStart();assertThat(jdbc.queryForObject("SELECT command_address FROM ddip_management.stage_invocations WHERE stage='PUBLISH_RELIABILITY'",String.class)).isNotNull();}public void started(InstallationHostGuard.ProcessIdentity child){o.started(child);}public void heartbeat(){o.heartbeat();}});}};
        assertSuccess(adapter(p,new ReadOnlyVerifierProcess()).run(guard,prepared,RUNTIME).artifactId());
    }
    @Test void retainedEvidenceChangedBeforePublicationFailsAndReleasesKnownOwner()throws Exception {Path e=descriptor().resolveSibling("evidence.json");Files.writeString(e,"{}");assertThatThrownBy(this::execute).hasMessage("INPUT_CHANGED");assertThat(state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE stage='PUBLISH_RELIABILITY'",Long.class)).isZero();}
    @Test void changedNativeInputAfterPreflightCannotPublish()throws Exception {Path raw=Files.walk(Path.of(fixture.get("bundle").get("bindings").get("batch").get("root").stringValue())).filter(p->p.toString().endsWith(".jsonl")).findFirst().orElseThrow();Files.writeString(raw,"changed");assertThatThrownBy(this::execute).hasMessage("INPUT_CHANGED");assertThat(state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();assertThat(hasNativeFiles()).isFalse();}
    @Test void stageIntentRollbackPreventsNativeLaunch(){jdbc.execute("ALTER TABLE ddip_management.stage_invocations ADD CONSTRAINT reject_publish CHECK(stage<>'PUBLISH_RELIABILITY')");assertThatThrownBy(this::execute).hasMessage("STATE_CONFLICT");assertThat(state()).isEqualTo("RUNNING");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isEqualTo(1);assertThat(hasNativeFiles()).isFalse();}
    ReadOnlyVerifierProcess intercept(Runnable before,boolean spark){return new ReadOnlyVerifierProcess(spark){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer o){before.run();return super.run(c,e,d,t,o);}};}
    @Test void cancellationCommittedBeforeAuthorizationPreventsLaunch(){var p=intercept(()->{byte[] b=SourceJson.canonical(SourceJson.object());jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','cancel',?,?,?,'REQUESTED','local-operator')",UUID.randomUUID(),run,b,SourceJson.sha(b),prepared.stage().owner().runVersion()+1);},true);assertThatThrownBy(()->adapter(p,new ReadOnlyVerifierProcess()).run(guard,prepared,RUNTIME)).hasMessage("WORKER_NOT_READY");assertThat(hasNativeFiles()).isFalse();}
    @Test void staleStageVersionCannotAuthorize(){var p=intercept(()->jdbc.update("UPDATE ddip_management.stage_invocations SET row_version=row_version+1 WHERE stage='PUBLISH_RELIABILITY'"),true);assertThatThrownBy(()->adapter(p,new ReadOnlyVerifierProcess()).run(guard,prepared,RUNTIME)).hasMessage("OWNERSHIP_LOST");assertThat(hasNativeFiles()).isFalse();}
    @Test void startupFailureIsKnownFailedAndReleased(){var p=new ReadOnlyVerifierProcess(true){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer o){return super.run(List.of(work.resolve("missing").toString()),e,d,t,o);}};assertThatThrownBy(()->adapter(p,new ReadOnlyVerifierProcess()).run(guard,prepared,RUNTIME)).hasMessage("PROCESS_START_FAILED");assertThat(state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();}
    @Test void publisherSuccessWithoutIndependentVerifierCannotSucceed(){var verifier=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer o){return super.run(VerifierProcessTest.command("invalid-result",c.getLast()),e,d,t,o);}};assertThatThrownBy(()->adapter(new ReadOnlyVerifierProcess(true),verifier).run(guard,prepared,RUNTIME)).hasMessage("RESULT_INVALID");blocked();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations WHERE stage='PUBLISH_RELIABILITY'",String.class)).isEqualTo("VERIFIED");}
    @Test void wrongNativeSchemaWithRefreshedInventoryBlocksIndependentCompletion(){
        var verifier=intercept(()->{try{
            String script="""
                import sys,json
                from pathlib import Path
                from hashlib import sha256
                import duckdb
                root=Path(sys.argv[1]);marker=next(root.rglob('complete.json'));p=next(root.rglob('*.parquet'))
                with duckdb.connect() as db:db.execute("COPY (SELECT 1 AS wrong_column) TO ? (FORMAT PARQUET)",[str(p)])
                obj=json.loads(marker.read_bytes())
                for files in obj['files'].values():
                    for f in files:
                        if f['path']==p.relative_to(marker.parent).as_posix():f.update(byte_length=p.stat().st_size,sha256=sha256(p.read_bytes()).hexdigest())
                marker.write_text(json.dumps(obj,sort_keys=True,separators=(',',':')))
                """;
            var child=new ProcessBuilder(python.toString(),"-I","-B","-c",script,nativeOutput().toString()).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.DISCARD).start();assertThat(child.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isZero();
        }catch(Exception e){throw new AssertionError(e);}},false);
        assertThatThrownBy(()->adapter(new ReadOnlyVerifierProcess(true),verifier).run(guard,prepared,RUNTIME)).hasMessage("RESULT_INVALID");blocked();
    }
    @Test void wrongFingerprintInNativeResultDescriptorCannotComplete(){var v=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer o){var r=super.run(c,e,d,t,o);try{Path evidence=Path.of(c.getLast()).resolveSibling("evidence.json");var body=SourceJson.parse(Files.readAllBytes(evidence),PreflightFiles.LARGE);((tools.jackson.databind.node.ObjectNode)body.get("publication")).put("nativeFingerprint","0".repeat(64));Files.write(evidence,SourceJson.canonical(body));}catch(Exception ex){throw new AssertionError(ex);}return r;}};assertThatThrownBy(()->adapter(new ReadOnlyVerifierProcess(true),v).run(guard,prepared,RUNTIME)).hasMessage("RESULT_INVALID");blocked();}
    @ParameterizedTest @ValueSource(strings={"artifact","stage-link","audit"}) void finalTransactionFailureRollsBackAllTrustedState(String failure) {
        switch(failure){case "artifact"->jdbc.execute("ALTER TABLE ddip_management.artifacts ADD CONSTRAINT reject_result CHECK(kind<>'RELIABILITY_V2')");case "stage-link"->jdbc.execute("ALTER TABLE ddip_management.stage_artifacts ADD CONSTRAINT reject_result CHECK(stage<>'VERIFY_RESULT')");default->jdbc.execute("ALTER TABLE ddip_management.audit_events ADD CONSTRAINT reject_result CHECK(action<>'RUN_SUCCEEDED')");}
        assertThatThrownBy(this::execute).hasMessage("STATE_CONFLICT");assertThat(state()).isEqualTo("RUNNING");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isZero();assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_artifacts",Long.class)).isZero();assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations WHERE stage='VERIFY_RESULT'",String.class)).isEqualTo("RUNNING");
    }
    @Test void databaseOutageBeforeLaunchNeverWrites(){var p=intercept(()->outage(true),true);try{assertThatThrownBy(()->adapter(p,new ReadOnlyVerifierProcess()).run(guard,prepared,RUNTIME)).hasMessage("MANAGEMENT_UNAVAILABLE");}finally{outage(false);}assertThat(hasNativeFiles()).isFalse();}
    @Test void outageAfterPublicationRetainsOccupiedOwner(){var v=intercept(()->outage(true),false);try{assertThatThrownBy(()->adapter(new ReadOnlyVerifierProcess(true),v).run(guard,prepared,RUNTIME)).hasMessage("MANAGEMENT_UNAVAILABLE");}finally{outage(false);}assertThat(state()).isEqualTo("RUNNING");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());}
    @Test void productionNoneCannotRunAdapter(){workers=new WorkerRepository(jdbc,tx,INSTALL);assertThatThrownBy(this::execute).hasMessage("WORKER_NOT_READY");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations",Long.class)).isEqualTo(1);}
    @Test void knownIncompleteNamespaceIsRetainedAndFailureReleasesSlot()throws Exception {
        Path directory=nativeOutput().resolve("v2/processor_revision=2/publication="+prepared.nativeDependencyFingerprint());Files.createDirectories(directory);Path sentinel=Files.writeString(directory.resolve("owner-investigation"),"retained");
        assertThatThrownBy(this::execute).isInstanceOf(ReliabilityExecution.DefinitiveFailure.class).hasMessage("INCOMPLETE_OUTPUT").satisfies(ex->assertThat(workers.heartbeatIdle(guard,((ReliabilityExecution.DefinitiveFailure)ex).worker()).status()).isEqualTo("READY"));assertThat(state()).isEqualTo("FAILED");assertThat(Files.readString(sentinel)).isEqualTo("retained");assertThat(jdbc.queryForObject("SELECT retry_eligible FROM ddip_management.runs",Boolean.class)).isFalse();assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
    }
    @Test void realNativeIdentityConflictCanCloseAfterProvenQuiescence()throws Exception {
        stop();jdbc.execute("DROP SCHEMA ddip_management CASCADE");new SchemaLifecycle(org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load(),jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);provision("JSONL",true);prepared=verify();
        assertThatThrownBy(this::execute).hasMessage("INPUT_INVALID");assertThat(state()).isEqualTo("FAILED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();assertThat(hasNativeFiles()).isFalse();
    }
    @Test void staleFinalOwnershipCannotInsertArtifact(){var v=new ReadOnlyVerifierProcess(){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer o){var r=super.run(c,e,d,t,o);jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");return r;}};assertThatThrownBy(()->adapter(new ReadOnlyVerifierProcess(true),v).run(guard,prepared,RUNTIME)).hasMessage("OWNERSHIP_LOST");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isZero();assertThat(state()).isEqualTo("RUNNING");}
    @ParameterizedTest @ValueSource(strings={"intent","authorization"}) void ambiguousPublicationCommitCannotLaunchTwice(String phase){var q=commitFault(phase);workers=WorkerTestSupport.enabled(q,transactions(q),INSTALL);assertThatThrownBy(this::execute).hasMessage("MANAGEMENT_UNAVAILABLE");assertThat(hasNativeFiles()).isFalse();assertThat(jdbc.queryForObject("SELECT child_pid FROM ddip_management.stage_invocations WHERE stage='PUBLISH_RELIABILITY'",Long.class)).isNull();assertThatThrownBy(this::execute).hasMessage("OWNERSHIP_LOST");}
    @Test void uncertainSparkTerminationRetainsRecoveryOwnership(){var p=new ReadOnlyVerifierProcess(true){Result run(List<String> c,Map<String,String> e,Path d,Duration t,Observer o){o.beforeStart();throw new PreflightFailure(PreflightFailure.Code.TERMINATION_UNCONFIRMED,false);}};assertThatThrownBy(()->adapter(p,new ReadOnlyVerifierProcess()).run(guard,prepared,RUNTIME)).hasMessage("TERMINATION_UNCONFIRMED");blocked();}
    @Test void resultLinkageSurvivesApiAndOptionalDatabaseRestart()throws Exception {
        UUID artifact=execute().artifactId();byte[] evidence=jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?",byte[].class,artifact);
        var before=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);String signal=System.getProperty("ddip.test.reliability-restart-signal");if(signal!=null){Files.writeString(Path.of(signal),"READY");long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<end)java.util.concurrent.locks.LockSupport.parkNanos(10000000);assertThat(Files.exists(Path.of(signal+".done"))).isTrue();assertThat(jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class)).isAfter(before);}
        assertThat(jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?",byte[].class,artifact)).isEqualTo(evidence);assertSuccess(artifact);
        try(var socket=new java.net.ServerSocket(0,1,java.net.InetAddress.getLoopbackAddress())){int port=socket.getLocalPort();socket.close();String base="http://127.0.0.1:"+port;
            try(var api=new org.springframework.boot.builder.SpringApplicationBuilder(platform.contentapi.ContentApiApplication.class).run("--ddip.role=api","--server.address=127.0.0.1","--server.port="+port,"--ddip.database.url="+url,"--ddip.database.username="+user,"--ddip.database.password="+password,"--ddip.installation-id="+INSTALL,"--ddip.management.catalog="+work.resolve("source-catalog.json"),"--ddip.management.cursor-key="+Base64.getUrlEncoder().withoutPadding().encodeToString(new byte[32]),"--ddip.management.origins="+base,"--logging.level.root=OFF","--spring.main.banner-mode=off");var client=java.net.http.HttpClient.newHttpClient()){
                for(String suffix:List.of("","/attempts","/events")){var response=client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(base+"/api/management/v1/runs/"+run+suffix)).GET().build(),java.net.http.HttpResponse.BodyHandlers.ofString());assertThat(response.statusCode()).isEqualTo(200);assertThat(response.body()).doesNotContain("command.json","retained.json",nativeOutput().toString());if(suffix.isEmpty())assertThat(response.body()).contains("VERIFIED_AT_EXECUTION",artifact.toString(),"SUCCEEDED");}
            }
        }
    }
    @Test void lostFinalAcknowledgmentResolvesExactlyPersistedResultWithoutSecondWriter(){var wrapped=commitFault();workers=WorkerTestSupport.enabled(wrapped,transactions(wrapped),INSTALL);assertThatThrownBy(this::execute).hasMessage("MANAGEMENT_UNAVAILABLE");UUID artifact=jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs",UUID.class);assertSuccess(artifact);assertThat(workers.lookup(owner.attemptId()).orElseThrow().runState()).isEqualTo("SUCCEEDED");assertThatThrownBy(this::execute).hasMessage("OWNERSHIP_LOST");}
    JdbcTemplate commitFault(){return commitFault("final");}
    JdbcTemplate commitFault(String phase){var once=new AtomicBoolean();var data=new AbstractDataSource(){public Connection getConnection()throws SQLException {var real=jdbc.getDataSource().getConnection();var matching=new AtomicBoolean();return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(p,m,a)->{try{if(m.getName().equals("prepareStatement") && a[0] instanceof String sql && (phase.equals("intent")?sql.startsWith("INSERT INTO ddip_management.stage_invocations"):phase.equals("authorization")?sql.startsWith("UPDATE ddip_management.stage_invocations SET command_address"):sql.startsWith("UPDATE ddip_management.runs SET state='SUCCEEDED'")))matching.set(true);Object r=m.invoke(real,a);if(m.getName().equals("commit") && matching.get() && once.compareAndSet(false,true))throw new SQLException("PRIVATE","08006");return r;}catch(InvocationTargetException e){throw e.getCause();}});}public Connection getConnection(String u,String p)throws SQLException{return getConnection();}};return new JdbcTemplate(data);}
}
