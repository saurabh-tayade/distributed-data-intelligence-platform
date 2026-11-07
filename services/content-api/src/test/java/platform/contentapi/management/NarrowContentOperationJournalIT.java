package platform.contentapi.management;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;
import static platform.contentapi.execution.NarrowContentAdmissionTestSupport.MANIFEST;
import static platform.contentapi.execution.NarrowContentAdmissionTestSupport.LOCK;

/** Actual PostgreSQL plus fixed Python read-only probe. No native publisher or activated dispatch. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentOperationJournalIT extends NarrowContentAdmissionFixtures {
    UUID operation;boolean versionTen;
    @Override void migrateFixtureSchema(){
        if(!versionTen){super.migrateFixtureSchema();return;}
        org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").target("10").load().migrate();
        new JdbcCoordinatorRepository(jdbc,tx,INSTALL).initialize();
    }
    @BeforeEach void operationId(){operation=UUID.randomUUID();}
    @AfterEach void currentSchema(){versionTen=false;}
    NarrowContentOperationTestSupport.Facts probe(java.util.function.Consumer<String> fault){return NarrowContentOperationTestSupport.probe(jdbc,tx,workers,guard,owner,operation,fault);}
    NarrowContentOperationTestSupport.Facts facts(){return NarrowContentOperationTestSupport.inspect(jdbc,tx,workers,operation);}
    void prepared(){claimed();prepare();}
    List<String> operationPhases(){return facts().phases();}
    void retained(){
        assertThat(count("content_operations")).isEqualTo(1);assertThat(count("stage_invocations")).isZero();assertThat(count("rss_deliveries")).isZero();
        assertThat(workers.lookup(owner.attemptId()).orElseThrow().state()).isEqualTo("CLAIMED");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
    }
    void stopAt(String point){assertThatThrownBy(()->probe(b->{if(b.equals(point))throw new IllegalStateException("TEST_BOUNDARY");})).hasMessage("TEST_BOUNDARY");}
    @ParameterizedTest @ValueSource(strings={"RESERVED","PROBE_COMPLETED"})
    void nativeFetchAssessmentRejectsIncompatibleProbeHistoryWithoutMutation(String prefix)throws Exception{
        prepared();if(prefix.equals("RESERVED"))stopAt(prefix);else probe(b->{});
        var before=metadata();var files=filesystem();var facts=facts();
        assertThatThrownBy(()->NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId())).hasMessage("WORKER_NOT_READY");
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(facts()).isEqualTo(facts);retained();noExecution();
    }
    @Test void reservationPrecedesEveryFileAndChildAndCompletionPreservesTheRunModel()throws Exception{
        prepared();var events=new ArrayList<String>();var before=filesystem();
        var result=probe(b->{
            events.add(b);
            if(b.equals("RESERVED")){assertThat(operationPhases()).isEmpty();try{assertThat(filesystem()).isEqualTo(before);}catch(Exception ex){throw new AssertionError(ex);}}
            if(b.equals("CHILD_RECORDED")){assertThat(facts().child().pid()).isPositive();assertThat(facts().child().startedAt()).isNotNull();assertThat(operationPhases()).doesNotContain("RELEASE_AUTHORIZED");}
        });
        assertThat(result.phases()).containsExactly("FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","PROBE_COMPLETED");
        assertThat(events).containsExactly("RESERVED","DIRECTORY_CREATED","DIRECTORY_CREATED","COMMAND_WRITTEN","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","BEFORE_RELEASE","RELEASE_AUTHORIZED","BEFORE_DELIVERY","PROBE_COMPLETED");
        try(var files=Files.walk(attemptRoot())){assertThat(files.filter(Files::isRegularFile).map(p->p.getFileName().toString()).toList()).containsExactlyInAnyOrder(MANIFEST,LOCK,"command.json");}
        assertThat(ProcessHandle.of(result.child().pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();retained();noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"RESERVED","DIRECTORY_CREATED","COMMAND_WRITTEN","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","BEFORE_RELEASE","RELEASE_AUTHORIZED","BEFORE_DELIVERY","PROBE_COMPLETED"})
    void everyInterruptedBoundaryIsPermanentlyConsumed(String point)throws Exception{
        prepared();stopAt(point);var initial=facts();var fs=filesystem();
        var fresh=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        assertThatThrownBy(()->NarrowContentOperationTestSupport.probe(jdbc,tx,fresh,guard,owner,operation,b->{})).hasMessage("STATE_CONFLICT");
        assertThat(facts()).isEqualTo(initial);assertThat(filesystem()).isEqualTo(fs);retained();
        if(initial.child()!=null)assertThat(ProcessHandle.of(initial.child().pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }
    @Test void newOperationIdCannotRepeatTheSameFixedSlot()throws Exception{
        prepared();stopAt("RESERVED");var fs=filesystem();var original=operation;operation=UUID.randomUUID();
        assertThatThrownBy(()->probe(b->{})).hasMessage("STATE_CONFLICT");operation=original;
        assertThat(operationPhases()).isEmpty();assertThat(filesystem()).isEqualTo(fs);retained();
    }
    @ParameterizedTest @ValueSource(strings={"RESERVED","DIRECTORY_CREATED","COMMAND_WRITTEN","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","BEFORE_RELEASE","RELEASE_AUTHORIZED","BEFORE_DELIVERY"})
    void cancellationAtEachBoundaryPreventsTheFollowingEffect(String point)throws Exception{
        prepared();var fs=new AtomicReference<Map<String,String>>();
        assertThatThrownBy(()->probe(b->{if(b.equals(point)){cancel();try{fs.set(filesystem());}catch(Exception ex){throw new AssertionError(ex);}}})).isInstanceOf(RuntimeException.class);
        assertThat(fs.get()).isNotNull();assertThat(filesystem()).isEqualTo(fs.get());assertThat(operationPhases()).doesNotContain("PROBE_COMPLETED");retained();
        if(facts().child()!=null)assertThat(ProcessHandle.of(facts().child().pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }
    @ParameterizedTest @ValueSource(strings={"run","attempt","coordinator","worker","stop"})
    void exactOwnerVersionChangeAfterReservationAllowsNoDirectory(String field)throws Exception{
        prepared();var fs=filesystem();
        assertThatThrownBy(()->probe(b->{if(b.equals("RESERVED")){
            switch(field){
                case "run"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());
                case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
                case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",owner.worker().instanceId());
                case "coordinator"->jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");
                case "stop"->jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
            }
        }})).hasMessage("OWNERSHIP_LOST");
        assertThat(filesystem()).isEqualTo(fs);assertThat(operationPhases()).isEmpty();
    }
    @Test void noReadyRecordMeansNoReservationOrFilesystemEffect()throws Exception{
        claimed();var fs=filesystem();assertThatThrownBy(()->probe(b->{})).isInstanceOf(RuntimeException.class);
        assertThat(count("content_operations")).isZero();assertThat(filesystem()).isEqualTo(fs);
    }
    @ParameterizedTest @ValueSource(strings={"content_operations","content_operation_phases"})
    void journalRowsCannotBeChangedDeletedOrTruncated(String table){
        prepared();stopAt("FILES_READY");
        for(String sql:List.of("UPDATE ddip_management."+table+" SET operation_id=operation_id","DELETE FROM ddip_management."+table,"TRUNCATE ddip_management."+table+" CASCADE"))
            assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(RuntimeException.class);
        assertThat(operationPhases()).containsExactly("FILES_READY");retained();
    }
    @ParameterizedTest @ValueSource(strings={"START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","PROBE_COMPLETED"})
    void sqlCannotSkipPhases(String phase){
        prepared();stopAt("RESERVED");
        assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.content_operation_phases(operation_id,phase) VALUES (?,?)",operation,phase)).isInstanceOf(RuntimeException.class);
        assertThat(operationPhases()).isEmpty();
    }
    @ParameterizedTest @ValueSource(strings={"installationId","policy","planKind","runId","attemptId","sourceId","controlRoot","controlDevice","controlInode","rootDevice","rootInode","lockDevice","lockInode","ledgerSha256","selectionSha256","profileSha256","runtimeSha256","workerInstanceId","ownerEpoch","checkpointId","commandSha256"})
    void sqlBindsEverySuppliedIdentityToReadyAndTheOperation(String key){
        prepared();ObjectNode n=NarrowContentOperationTestSupport.reservation(jdbc,tx,guard,owner,operation);
        UUID checkpoint=UUID.fromString(n.get("checkpointId").stringValue());n.put(key,"other");byte[] bytes=SourceJson.canonical(n);
        assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.content_operations(operation_id,attempt_id,stage,item_ordinal,checkpoint_id,bootstrap_bytes,bootstrap_sha256) VALUES (?,?,'CHECK_INPUTS',0,?,?,?)",operation,owner.attemptId(),checkpoint,bytes,SourceJson.sha(bytes))).isInstanceOf(RuntimeException.class);
        assertThat(count("content_operations")).isZero();
    }
    @Test void concurrentReservationsHaveOneWinnerBeforeFiles(){
        prepared();var barrier=new CyclicBarrier(2);var winners=new AtomicInteger();
        try(var pool=Executors.newFixedThreadPool(2)){
            var futures=new ArrayList<Future<?>>();
            for(int i=0;i<2;i++)futures.add(pool.submit(()->{
                try{barrier.await(5,TimeUnit.SECONDS);NarrowContentOperationTestSupport.probe(jdbc,tx,WorkerTestSupport.enabled(jdbc,tx,INSTALL),guard,owner,UUID.randomUUID(),b->{if(b.equals("RESERVED")){winners.incrementAndGet();throw new IllegalStateException("TEST_BOUNDARY");}});}catch(Exception expected){}
            }));
            for(var f:futures)try{f.get(10,TimeUnit.SECONDS);}catch(Exception ex){throw new AssertionError(ex);}
        }
        assertThat(winners).hasValue(1);assertThat(count("content_operations")).isEqualTo(1);assertThat(count("content_operation_phases")).isZero();
    }
    org.springframework.jdbc.core.JdbcTemplate loseOperationCommit(String phase){
        var source=new org.springframework.jdbc.datasource.AbstractDataSource(){
            public java.sql.Connection getConnection()throws java.sql.SQLException{
                var real=jdbc.getDataSource().getConnection();var lose=new AtomicBoolean();
                return (java.sql.Connection)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{java.sql.Connection.class},(proxy,method,args)->{
                    try{
                        Object result=method.invoke(real,args);
                        if(method.getName().equals("prepareStatement") && args[0] instanceof String sql){
                            if(sql.startsWith("INSERT INTO ddip_management.content_operations("))lose.set(phase.equals("RESERVED"));
                            if(sql.startsWith("INSERT INTO ddip_management.content_operation_phases(")){
                                var statement=(java.sql.PreparedStatement)result;
                                return java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{java.sql.PreparedStatement.class},(p,m,a)->{
                                    try{if(m.getName().equals("setString") && a[0].equals(2) && phase.equals(a[1]))lose.set(true);return m.invoke(statement,a);}
                                    catch(java.lang.reflect.InvocationTargetException ex){throw ex.getCause();}
                                });
                            }
                        }
                        if(method.getName().equals("commit") && lose.compareAndSet(true,false))throw new java.sql.SQLException("PRIVATE_ACK","08006");
                        return result;
                    }catch(java.lang.reflect.InvocationTargetException ex){throw ex.getCause();}
                });
            }
            public java.sql.Connection getConnection(String u,String p)throws java.sql.SQLException{return getConnection();}
        };
        return new org.springframework.jdbc.core.JdbcTemplate(source);
    }
    @ParameterizedTest @ValueSource(strings={"RESERVED","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","PROBE_COMPLETED"})
    void lostAcknowledgmentNeverContinuesOrRepeatsTheConsumedOperation(String phase)throws Exception{
        prepared();var wrapped=loseOperationCommit(phase);var wrappedTx=transactions(wrapped);var uncertain=WorkerTestSupport.enabled(wrapped,wrappedTx,INSTALL);
        assertThatThrownBy(()->NarrowContentOperationTestSupport.probe(wrapped,wrappedTx,uncertain,guard,owner,operation,b->{})).hasMessage("MANAGEMENT_UNAVAILABLE");
        var historical=facts();if(phase.equals("RESERVED"))assertThat(historical.phases()).isEmpty();else assertThat(historical.phases()).endsWith(phase);
        var fs=filesystem();assertThatThrownBy(()->uncertain.heartbeat(guard,owner)).hasMessage("OWNERSHIP_LOST");
        assertThatThrownBy(()->probe(b->{})).hasMessage("STATE_CONFLICT");assertThat(facts()).isEqualTo(historical);assertThat(filesystem()).isEqualTo(fs);retained();
        if(historical.child()!=null)assertThat(ProcessHandle.of(historical.child().pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }
    @Test void factsAfterInterruptionGrantNoNewOwnerOrLegacyExecution(){
        prepared();stopAt("CHILD_RECORDED");workers.interrupt(guard,owner);
        var historical=facts();assertThat(historical.phases()).containsExactly("FILES_READY","START_AUTHORIZED","CHILD_RECORDED");
        assertThat(workers.lookup(owner.attemptId()).orElseThrow().runState()).isEqualTo("RECOVERY_REQUIRED");
        assertThatThrownBy(()->probe(b->{})).isInstanceOf(RuntimeException.class);
        assertThat(facts()).isEqualTo(historical);assertThat(count("stage_invocations")).isZero();
    }
    Process operationChild(String mode,UUID run,UUID attempt)throws Exception{
        Path descriptor=Files.createTempFile(temp,"operation-child-",".json");Files.write(descriptor,binding.descriptor());
        Files.setPosixFilePermissions(descriptor,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                NarrowContentOperationTestChild.class.getName(),descriptor.toString(),mode,CONTENT_PROFILE,run.toString(),attempt.toString(),operation.toString())
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);
        return builder.start();
    }
    String operationLine(Process process)throws Exception{
        var result=CompletableFuture.supplyAsync(()->{try{var reader=new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()));String line;
            while((line=reader.readLine())!=null)if(line.startsWith("NARROW_"))return line;return "EOF";}catch(Exception ex){throw new CompletionException(ex);}});
        return result.get(20,TimeUnit.SECONDS);
    }
    @ParameterizedTest @ValueSource(strings={"RESERVED","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","PROBE_COMPLETED"})
    void actualWriterDeathAndFreshJvmHistoryDoNotResumeTheOperation(String boundary)throws Exception{
        UUID run=queue("hard-crash"),attempt=UUID.randomUUID();var child=operationChild(boundary,run,attempt);
        try{assertThat(operationLine(child)).isEqualTo("NARROW_BOUNDARY "+child.pid()+" "+boundary);}
        finally{child.destroyForcibly();assertThat(child.waitFor(10,TimeUnit.SECONDS)).isTrue();}
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);var historical=facts();var before=filesystem();
        if(historical.child()!=null){
            long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
            while(sameLiveChild(historical.child()) && System.nanoTime()<until)Thread.sleep(25);
            assertThat(sameLiveChild(historical.child())).as("Recorded child must exit after parent pipe closes").isFalse();
        }
        var lookup=operationChild("inspect",run,attempt);
        try{assertThat(operationLine(lookup)).isEqualTo("NARROW_OPERATION "+lookup.pid()+" "+String.join(",",historical.phases()));assertThat(lookup.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(lookup.exitValue()).isZero();}
        finally{if(lookup.isAlive()){lookup.destroyForcibly();lookup.waitFor(5,TimeUnit.SECONDS);}}
        assertThat(facts()).isEqualTo(historical);assertThat(filesystem()).isEqualTo(before);
        ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),run)).hasMessage("WORKER_NOT_READY");
        assertThat(count("stage_invocations")).isZero();assertThat(count("content_operations")).isEqualTo(1);
    }

    static boolean sameLiveChild(InstallationHostGuard.ProcessIdentity identity){
        return ProcessHandle.of(identity.pid()).map(p->p.isAlive() && p.info().startInstant().map(t->t.truncatedTo(java.time.temporal.ChronoUnit.MICROS).equals(identity.startedAt())).orElse(true)).orElse(false);
    }
    @ParameterizedTest @ValueSource(strings={"PREPARING","READY"})
    void populatedVersionTenUpgradePreservesNarrowHistoryWithoutOperationBackfill(String phase)throws Exception{
        versionTen=true;setup();claimed();
        if(phase.equals("READY"))prepare();
        else assertThatThrownBy(()->NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,owner,b->{if(b.equals("PREPARING_COMMITTED"))throw new IllegalStateException("TEST_BOUNDARY");})).hasMessage("TEST_BOUNDARY");
        var metadata=metadata();var files=filesystem();
        var migration=org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        assertThat(migration.migrate().migrationsExecuted).isEqualTo(5);
        new SchemaLifecycle(migration,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(false);
        assertThat(metadataBeforeRecoveryPolicyUpgrade()).isEqualTo(metadata);assertThat(filesystem()).isEqualTo(files);
        assertThat(count("content_operations")).isZero();assertThat(count("content_operation_phases")).isZero();
        if(phase.equals("READY"))assertThat(probe(b->{}).phases()).endsWith("PROBE_COMPLETED");
    }

}
