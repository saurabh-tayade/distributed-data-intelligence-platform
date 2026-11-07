package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Native completion consumes one immutable intent without starting legacy execution. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchCompletionIT extends NarrowContentFetchLaunchFixtures {
    byte[] body;boolean versionThirteen,versionFourteen;
    @Override void migrateFixtureSchema(){
        if(!versionThirteen && !versionFourteen){super.migrateFixtureSchema();return;}
        org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").target(versionFourteen?"14":"13").load().migrate();
        new JdbcCoordinatorRepository(jdbc,tx,INSTALL).initialize();
    }
    @AfterEach void resetSchemaTarget(){versionThirteen=false;versionFourteen=false;}

    @Override byte[] responsePayload(){return body==null?super.responsePayload():body;}
    @BeforeEach void resetBody(){body=null;}
    void candidate(){prepared();launch(b->{});}
    NarrowContentFetchCompletionTestSupport.Observation complete(){return complete(b->{});}
    NarrowContentFetchCompletionTestSupport.Observation complete(Consumer<String> hook){return NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,hook);}
    Optional<NarrowContentFetchCompletionTestSupport.Observation> checkpoint(){return NarrowContentFetchCompletionTestSupport.inspect(jdbc,tx,guard,operation);}
    Map<String,List<String>> history(){var result=new TreeMap<>(metadata());for(String table:List.of("content_native_operations","content_native_launch_phases","content_fetch_completion_intents","content_fetch_checkpoints"))result.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));return result;}
    void rejectedWithoutWrites()throws Exception{var before=history();var files=filesystem();assertThatThrownBy(this::complete).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();}
    @Test void populatedVersionThirteenUpgradePreservesCandidateWithoutCompletionBackfill()throws Exception{
        versionThirteen=true;closeServer();setup();candidate();var before=metadata();var launch=facts();var files=filesystem();
        var migration=org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        assertThat(migration.migrate().migrationsExecuted).isEqualTo(4);new SchemaLifecycle(migration,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(false);
        assertThat(metadataBeforeRecoveryPolicyUpgrade()).isEqualTo(before);assertThat(facts()).isEqualTo(launch);assertThat(filesystem()).isEqualTo(files);
        assertThat(count("content_fetch_completion_intents")).isZero();assertThat(count("content_fetch_checkpoints")).isZero();assertThat(checkpoint()).isEmpty();retained();
    }
    @Test void populatedVersionFourteenUpgradePreservesCheckpointWithoutRecoveryBackfill()throws Exception{
        versionFourteen=true;closeServer();setup();candidate();complete();
        var before=metadata();var launch=facts();var durable=checkpoint();var files=filesystem();
        var checkpoints=jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_fetch_checkpoints t",String.class);
        var intents=jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_fetch_completion_intents t",String.class);
        var migrations=jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IS NOT NULL ORDER BY installed_rank");
        var migration=org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        assertThat(migration.migrate().migrationsExecuted).isEqualTo(3);
        new SchemaLifecycle(migration,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(false);
        assertThat(migration.info().current().getVersion().getVersion()).isEqualTo("17");
        assertThat(metadataBeforeRecoveryPolicyUpgrade()).isEqualTo(before);assertThat(facts()).isEqualTo(launch);
        assertThat(checkpoint()).isEqualTo(durable);assertThat(filesystem()).isEqualTo(files);
        assertThat(jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_fetch_checkpoints t",String.class)).isEqualTo(checkpoints);
        assertThat(jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_fetch_completion_intents t",String.class)).isEqualTo(intents);
        assertThat(jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IS NOT NULL AND version NOT IN ('15','16','17') ORDER BY installed_rank")).isEqualTo(migrations);
        assertThat(count("content_fetch_recovery_handoffs")).isZero();assertThat(requests).hasValue(1);retained();
    }
    @Test void freshCompletionWritesOnlyNativeMarkerAndImmutableReceipt()throws Exception{
        candidate();var before=metadata();var phases=facts();var files=filesystem();var boundaries=new ArrayList<String>();
        var result=complete(b->{boundaries.add(b);if(!Set.of("INTENT_INSERTED","CHECKPOINT_INSERTED").contains(b))assertThat(TransactionSynchronizationManager.isActualTransactionActive()).as(b).isFalse();});
        assertThat(result.operation()).isEqualTo(operation);assertThat(result.run()).isEqualTo(owner.runId());assertThat(result.attempt()).isEqualTo(owner.attemptId());assertThat(result.candidateSha256()).isEqualTo(phases.candidateSha256());assertThat(result.checkpointed()).isTrue();assertThat(result.nativeLaunchCompatible()).isFalse();
        assertThat(boundaries).containsSubsequence("FRESH_VERIFIED","INTENT_INSERTED","INTENT_COMMITTED","MARKER_WRITTEN","REOPENED","CHECKPOINT_INSERTED","CHECKPOINT_COMMITTED");
        byte[] marker=Files.readAllBytes(fetchDirectory().resolve("complete.json"));assertThat(result.completionSha256()).isEqualTo(SourceJson.sha(marker));
        assertThat(jdbc.queryForObject("SELECT completion_bytes FROM ddip_management.content_fetch_completion_intents",byte[].class)).isEqualTo(marker);
        assertThat(count("content_fetch_completion_intents")).isEqualTo(1);assertThat(count("content_fetch_checkpoints")).isEqualTo(1);
        assertThat(metadata()).isEqualTo(before);assertThat(facts()).isEqualTo(phases);assertThat(checkpoint()).contains(result);assertThat(requests).hasValue(1);retained();
        var after=filesystem();for(var entry:files.entrySet())if(!entry.getKey().endsWith("/"+result.checkpoint()))assertThat(after.get(entry.getKey())).as(entry.getKey()).isEqualTo(entry.getValue());
        assertThat(after.size()).isEqualTo(files.size()+1);rejectedWithoutWrites();
    }
    @Test void sameLiveLaunchHandsRefreshedOwnerToCompletionAfterHeartbeat()throws Exception{
        prepared();responseGate=new CountDownLatch(1);
        try(var pool=Executors.newSingleThreadScheduledExecutor()){
            pool.schedule(()->responseGate.countDown(),7,TimeUnit.SECONDS);
            var result=NarrowContentFetchCompletionTestSupport.launchAndCheckpoint(jdbc,tx,workers,guard,owner,operation,LAUNCH_PROFILE,b->{});
            assertThat(result.checkpointed()).isTrue();assertThat(checkpoint()).contains(result);
            assertThat(jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class)).isGreaterThan(owner.worker().coordinatorVersion());
            assertThat(requests).hasValue(1);retained();
        }finally{responseGate.countDown();}
    }
    @ParameterizedTest @ValueSource(strings={"","not XML","<feed><broken>"}) void rawHttpPayloadIsCheckpointable(String raw){body=raw.getBytes(java.nio.charset.StandardCharsets.UTF_8);candidate();assertThat(complete().checkpointed()).isTrue();assertThat(requests).hasValue(1);retained();}
    @ParameterizedTest @ValueSource(strings={"FRESH_VERIFIED","BEFORE_INTENT","INTENT_INSERTED","INTENT_COMMITTED","BEFORE_MARKER","PREFIX_CREATED","PREFIX_WRITTEN","PREFIX_FORCED","MARKER_RENAMED","DIRECTORY_FORCED","MARKER_WRITTEN","JOURNAL_REOPENED","REOPENED","BEFORE_CHECKPOINT","CHECKPOINT_INSERTED","CHECKPOINT_COMMITTED"})
    void failuresAtEveryBoundaryNeverResumeConsumedIntent(String boundary)throws Exception{
        candidate();assertThatThrownBy(()->complete(b->{if(b.equals(boundary))throw new IllegalStateException("TEST_BOUNDARY");})).isInstanceOf(RuntimeException.class);
        boolean consumed=!Set.of("FRESH_VERIFIED","BEFORE_INTENT","INTENT_INSERTED").contains(boundary);assertThat(count("content_fetch_completion_intents")).isEqualTo(consumed?1:0);
        assertThat(count("content_fetch_checkpoints")).isEqualTo(boundary.equals("CHECKPOINT_COMMITTED")?1:0);
        if(consumed)rejectedWithoutWrites();assertThat(requests).hasValue(1);retained();
    }
    @ParameterizedTest @ValueSource(strings={"FRESH_VERIFIED","BEFORE_INTENT","INTENT_COMMITTED","BEFORE_MARKER","MARKER_WRITTEN","REOPENED","BEFORE_CHECKPOINT"})
    void changedCandidateAtBoundaryCannotCheckpoint(String boundary)throws Exception{
        candidate();assertThatThrownBy(()->complete(b->{if(b.equals(boundary))try{Files.writeString(fetchDirectory().resolve("payload.xml"),"changed");}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);
        assertThat(count("content_fetch_checkpoints")).isZero();assertThat(requests).hasValue(1);retained();
    }
    @ParameterizedTest @ValueSource(strings={"FRESH_VERIFIED","BEFORE_INTENT","INTENT_COMMITTED","BEFORE_MARKER","MARKER_WRITTEN","REOPENED","BEFORE_CHECKPOINT"})
    void cancellationAtBoundaryCannotCheckpoint(String boundary){candidate();assertThatThrownBy(()->complete(b->{if(b.equals(boundary))cancel();})).isInstanceOf(RuntimeException.class);assertThat(count("content_fetch_checkpoints")).isZero();assertThat(requests).hasValue(1);retained();}
    @ParameterizedTest @ValueSource(strings={"FRESH_VERIFIED","BEFORE_INTENT","INTENT_COMMITTED","BEFORE_MARKER","MARKER_WRITTEN","REOPENED","BEFORE_CHECKPOINT"})
    void staleOwnerAtBoundaryCannotCheckpoint(String boundary){candidate();assertThatThrownBy(()->complete(b->{if(b.equals(boundary))workers.heartbeat(guard,owner);})).isInstanceOf(RuntimeException.class);assertThat(count("content_fetch_checkpoints")).isZero();assertThat(requests).hasValue(1);retained();}
    @Test void foreignOwnerCannotConsumeIntent()throws Exception{candidate();owner=new WorkerRepository.Owner(owner.worker(),UUID.randomUUID(),owner.attemptId(),owner.runVersion(),owner.attemptVersion(),owner.attemptState());var before=history();var files=filesystem();assertThatThrownBy(this::complete).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);}
    @ParameterizedTest @ValueSource(strings={"complete.json","complete.json.preparing","unexpected"}) void existingMarkerOrPrefixIsNeverAdopted(String name)throws Exception{candidate();Files.createFile(fetchDirectory().resolve(name),PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));rejectedWithoutWrites();}
    @ParameterizedTest @ValueSource(strings={"command.json","payload.xml","metadata.json","descriptor.json","result.json"}) void changedOriginalFileCannotConsumeIntent(String name)throws Exception{candidate();Files.writeString(fetchDirectory().resolve(name),"changed");rejectedWithoutWrites();assertThat(count("content_fetch_completion_intents")).isZero();}
    @ParameterizedTest @ValueSource(strings={"FRESH_VERIFIED","JOURNAL_REOPENED"})
    void identicalBytesReplacementAfterVerificationFailsClosed(String boundary){candidate();assertThatThrownBy(()->complete(b->{if(b.equals(boundary))try{Path p=fetchDirectory().resolve("payload.xml");byte[] raw=Files.readAllBytes(p);var time=Files.getLastModifiedTime(p);Files.move(p,temp.resolve("old-payload"));Files.createFile(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));Files.write(p,raw);Files.setLastModifiedTime(p,time);}catch(Exception ex){throw new IllegalStateException(ex);}})).isInstanceOf(RuntimeException.class);assertThat(count("content_fetch_checkpoints")).isZero();retained();}
    @ParameterizedTest @ValueSource(strings={"MARKER_RENAMED","SECOND_JOURNAL_REOPENED"})
    void identicalCompletionMarkerAtNewInodeCannotBeCheckpointed(String boundary)throws Exception{
        candidate();var reopened=new AtomicInteger();var replaced=new AtomicBoolean();
        assertThatThrownBy(()->complete(b->{
            int occurrence=b.equals("JOURNAL_REOPENED")?reopened.incrementAndGet():0;
            if(b.equals(boundary) || boundary.equals("SECOND_JOURNAL_REOPENED") && occurrence==2)try{
                Path path=fetchDirectory().resolve("complete.json");byte[] bytes=Files.readAllBytes(path);var modified=Files.getLastModifiedTime(path);var identity=InstallationHostGuard.identity(path);
                Files.move(path,temp.resolve("original-completion-"+UUID.randomUUID()));
                Files.createFile(path,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));Files.write(path,bytes);Files.setLastModifiedTime(path,modified);
                assertThat(InstallationHostGuard.identity(path)).isNotEqualTo(identity);assertThat(Files.readAllBytes(path)).isEqualTo(bytes);assertThat(Files.getLastModifiedTime(path)).isEqualTo(modified);replaced.set(true);
            }catch(Exception ex){throw new IllegalStateException(ex);}
        })).isInstanceOf(RuntimeException.class);
        assertThat(replaced).isTrue();assertThat(reopened).hasValue(boundary.equals("SECOND_JOURNAL_REOPENED")?2:0);
        assertThat(count("content_fetch_completion_intents")).isEqualTo(1);assertThat(count("content_fetch_checkpoints")).isZero();assertThat(checkpoint()).isEmpty();
        assertThat(Files.readAllBytes(fetchDirectory().resolve("complete.json"))).isEqualTo(jdbc.queryForObject("SELECT completion_bytes FROM ddip_management.content_fetch_completion_intents",byte[].class));
        rejectedWithoutWrites();assertThat(requests).hasValue(1);retained();
    }
    @Test void nativeCheckpointDoesNotAuthorizeLegacyFetchOrAnotherLaunch()throws Exception{
        candidate();var receipt=complete();var before=history();var files=filesystem();
        byte[] command=jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_native_operations WHERE operation_id=?",byte[].class,operation);var json=SourceJson.parse(command);
        assertThatThrownBy(()->workers.beginFetch(guard,owner,0,receipt.checkpoint(),json.get("sourceId").stringValue(),json.get("capturedConfigSha256").stringValue(),SourceJson.sha(command))).hasMessage("WORKER_NOT_READY");
        assertThatThrownBy(()->launch(b->{})).isInstanceOf(RuntimeException.class);
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(checkpoint()).contains(receipt);assertThat(requests).hasValue(1);retained();
    }
    @Test void concurrentCallersConsumeExactlyOneIntent()throws Exception{
        candidate();var gate=new CyclicBarrier(2);var ok=new AtomicInteger();var failed=new AtomicInteger();
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()){
            var tasks=new ArrayList<Future<?>>();for(int i=0;i<2;i++)tasks.add(pool.submit(()->{try{NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,WorkerTestSupport.enabled(jdbc,tx,INSTALL),guard,owner,operation,b->{if(b.equals("BEFORE_INTENT"))try{gate.await(15,TimeUnit.SECONDS);}catch(Exception ex){throw new IllegalStateException(ex);}});ok.incrementAndGet();}catch(RuntimeException ex){failed.incrementAndGet();}}));for(var task:tasks)task.get(30,TimeUnit.SECONDS);
        }
        assertThat(count("content_fetch_completion_intents")).isEqualTo(1);assertThat(ok.get()+failed.get()).isEqualTo(2);assertThat(ok.get()).isEqualTo(1);assertThat(failed.get()).isEqualTo(1);assertThat(count("content_fetch_checkpoints")).isEqualTo(1);assertThat(requests).hasValue(1);retained();
    }
    @ParameterizedTest @ValueSource(strings={"BEFORE_INTENT","CHECKPOINT_INSERTED"}) void lostCommitAcknowledgmentOnlyAllowsReadOnlyConfirmation(String boundary)throws Exception{
        candidate();var armed=new AtomicBoolean();armed.set(true);var uncertain=loseWriteAcknowledgment(armed,boundary.equals("BEFORE_INTENT")?"content_fetch_completion_intents":"content_fetch_checkpoints");var transaction=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(uncertain.getDataSource())));var repository=WorkerTestSupport.enabled(uncertain,transaction,INSTALL);
        if(boundary.equals("BEFORE_INTENT"))assertThatThrownBy(()->NarrowContentFetchCompletionTestSupport.complete(uncertain,transaction,repository,guard,owner,operation,b->{})).isInstanceOf(RuntimeException.class);
        else assertThat(NarrowContentFetchCompletionTestSupport.complete(uncertain,transaction,repository,guard,owner,operation,b->{}).checkpointed()).isTrue();
        assertThat(armed).isFalse();assertThat(count("content_fetch_completion_intents")).isEqualTo(1);assertThat(count("content_fetch_checkpoints")).isEqualTo(boundary.equals("CHECKPOINT_INSERTED")?1:0);
        var before=history();var files=filesystem();assertThat(checkpoint().isPresent()).isEqualTo(boundary.equals("CHECKPOINT_INSERTED"));assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);rejectedWithoutWrites();
    }
    org.springframework.jdbc.core.JdbcTemplate loseWriteAcknowledgment(AtomicBoolean armed,String table){
        var source=new org.springframework.jdbc.datasource.AbstractDataSource(){
            @Override public java.sql.Connection getConnection()throws java.sql.SQLException{
                var real=jdbc.getDataSource().getConnection();var matching=new AtomicBoolean();
                return (java.sql.Connection)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{java.sql.Connection.class},(proxy,method,args)->{
                    try{
                        if(method.getName().equals("prepareStatement") && args!=null && args[0] instanceof String sql && sql.startsWith("INSERT INTO ddip_management."+table+"("))matching.set(true);
                        Object result=method.invoke(real,args);
                        if(method.getName().equals("commit") && matching.get() && armed.compareAndSet(true,false))throw new java.sql.SQLException("PRIVATE_COMPLETION_ACK","08006");
                        return result;
                    }catch(java.lang.reflect.InvocationTargetException ex){throw ex.getCause();}
                });
            }
            @Override public java.sql.Connection getConnection(String user,String password)throws java.sql.SQLException{return getConnection();}
        };
        return new org.springframework.jdbc.core.JdbcTemplate(source);
    }
    @ParameterizedTest @ValueSource(strings={"content_fetch_completion_intents","content_fetch_checkpoints"}) void receiptsAreImmutableAndCannotBeTruncated(String table){candidate();complete();for(String sql:List.of("UPDATE ddip_management."+table+" SET completion_sha256=completion_sha256","DELETE FROM ddip_management."+table,"TRUNCATE ddip_management."+table+" CASCADE"))assertThatThrownBy(()->jdbc.update(sql)).isInstanceOf(org.springframework.dao.DataAccessException.class);assertThat(count(table)).isEqualTo(1);retained();}
    @Test void expiredAttemptCannotConsumeIntent()throws Exception{
        candidate();
        // Test-only clock fixture: started_at is immutable to application writers.
        jdbc.execute("ALTER TABLE ddip_management.attempts DISABLE TRIGGER USER");
        try{jdbc.update("UPDATE ddip_management.attempts SET started_at=clock_timestamp()-interval '2 hours 1 second' WHERE attempt_id=?",owner.attemptId());}
        finally{jdbc.execute("ALTER TABLE ddip_management.attempts ENABLE TRIGGER USER");}
        rejectedWithoutWrites();assertThat(count("content_fetch_completion_intents")).isZero();
    }
    void insertIntent(Map<String,Object> row){jdbc.update("INSERT INTO ddip_management.content_fetch_completion_intents(operation_id,candidate_sha256,completion_bytes,completion_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?)",row.get("operation_id"),row.get("candidate_sha256"),row.get("completion_bytes"),row.get("completion_sha256"),row.get("owner_coordinator_version"),row.get("owner_worker_version"),row.get("owner_run_version"),row.get("owner_attempt_version"));}
    void insertCheckpoint(Map<String,Object> row){jdbc.update("INSERT INTO ddip_management.content_fetch_checkpoints(operation_id,checkpoint_id,journal_address,journal_descriptor,journal_sha256,expected_evidence,expected_sha256,completion_sha256,payload_length,payload_sha256) VALUES (?,?,?,?,?,?,?,?,?,?)",row.get("operation_id"),row.get("checkpoint_id"),row.get("journal_address"),row.get("journal_descriptor"),row.get("journal_sha256"),row.get("expected_evidence"),row.get("expected_sha256"),row.get("completion_sha256"),row.get("payload_length"),row.get("payload_sha256"));}
    @ParameterizedTest @ValueSource(strings={"candidate","operation","owner_coordinator_version","owner_worker_version","owner_run_version","owner_attempt_version","extra-field","version-string","version-float","stamp-size-float","missing-file","duplicate-file","file-length","file-hash","stamp-size","duplicate-key"})
    void sqlIntentGuardRejectsSubstitutionBeforeDuplicateConstraint(String change){
        candidate();assertThatThrownBy(()->complete(b->{if(b.equals("INTENT_COMMITTED"))throw new IllegalStateException("STOP");})).isInstanceOf(RuntimeException.class);
        var row=new HashMap<>(jdbc.queryForMap("SELECT * FROM ddip_management.content_fetch_completion_intents"));
        var marker=(tools.jackson.databind.node.ObjectNode)SourceJson.parse((byte[])row.get("completion_bytes"));
        var files=(tools.jackson.databind.node.ArrayNode)marker.get("files");var first=(tools.jackson.databind.node.ObjectNode)files.get(0);
        switch(change){
            case "candidate"->row.put("candidate_sha256","0".repeat(64));
            case "operation"->row.put("operation_id",UUID.randomUUID());
            case "owner_coordinator_version","owner_worker_version","owner_run_version","owner_attempt_version"->row.put(change,((Number)row.get(change)).longValue()+1);
            case "extra-field"->marker.put("unexpected",true);
            case "version-string"->marker.put("journalCompletionVersion","1");
            case "missing-file"->files.remove(0);
            case "duplicate-file"->files.set(1,files.get(0));
            case "file-length"->first.put("byteLength",65537);
            case "file-hash"->first.put("sha256","0".repeat(64));
            case "stamp-size"->((tools.jackson.databind.node.ObjectNode)first.get("stamp")).put("size",-1);
        }
        byte[] raw=SourceJson.canonical(marker);
        if(change.equals("duplicate-key"))raw=new String(raw,java.nio.charset.StandardCharsets.UTF_8).replace("\"journalCompletionVersion\":1","\"journalCompletionVersion\":1,\"journalCompletionVersion\":1").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if(change.equals("version-float"))raw=new String(raw,java.nio.charset.StandardCharsets.UTF_8).replace("\"journalCompletionVersion\":1","\"journalCompletionVersion\":1.0").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if(change.equals("stamp-size-float"))raw=new String(raw,java.nio.charset.StandardCharsets.UTF_8).replaceFirst("(\"size\":)([0-9]+)","$1$2.0").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        row.put("completion_bytes",raw);row.put("completion_sha256",SourceJson.sha(raw));
        assertThatThrownBy(()->insertIntent(row)).isInstanceOf(org.springframework.dao.DataAccessException.class).hasStackTraceContaining("CONTENT_FETCH_COMPLETION_");assertThat(count("content_fetch_completion_intents")).isEqualTo(1);assertThat(count("content_fetch_checkpoints")).isZero();retained();
    }
    @ParameterizedTest @ValueSource(strings={"operation","checkpoint","address","completion","payload-length","payload-hash","descriptor-run","descriptor-field","reference-hash","reference-field","reference-stamp","nested-reference-float","descriptor-duplicate"})
    void sqlCheckpointGuardRejectsSubstitutionBeforeDuplicateConstraint(String change){
        candidate();complete();var row=new HashMap<>(jdbc.queryForMap("SELECT * FROM ddip_management.content_fetch_checkpoints"));
        var descriptor=(tools.jackson.databind.node.ObjectNode)SourceJson.parse((byte[])row.get("journal_descriptor"));var expected=(tools.jackson.databind.node.ObjectNode)SourceJson.parse((byte[])row.get("expected_evidence"));
        switch(change){
            case "operation"->row.put("operation_id",UUID.randomUUID());
            case "checkpoint"->row.put("checkpoint_id",UUID.randomUUID());
            case "address"->row.put("journal_address","runs/foreign");
            case "completion"->row.put("completion_sha256","0".repeat(64));
            case "payload-length"->row.put("payload_length",((Number)row.get("payload_length")).longValue()+1);
            case "payload-hash"->row.put("payload_sha256","0".repeat(64));
            case "descriptor-run"->descriptor.put("runId",UUID.randomUUID().toString());
            case "descriptor-field"->descriptor.put("unexpected",true);
            case "reference-hash"->expected.put("completionSha256","0".repeat(64));
            case "reference-field"->expected.put("unexpected",true);
            case "reference-stamp"->((tools.jackson.databind.node.ObjectNode)expected.get("completionStamp")).put("size",-1);
        }
        byte[] raw=SourceJson.canonical(descriptor),reference=SourceJson.canonical(expected);
        if(change.equals("descriptor-duplicate"))raw=new String(raw,java.nio.charset.StandardCharsets.UTF_8).replace("\"journalVersion\":1","\"journalVersion\":1,\"journalVersion\":1").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        if(change.equals("nested-reference-float")){
            String integer="\"metadataByteLength\":"+expected.get("bronze").get("metadataByteLength").longValue();
            String text=new String(reference,java.nio.charset.StandardCharsets.UTF_8);assertThat(text).contains(integer);
            reference=text.replace(integer,integer+".0").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        }
        row.put("journal_descriptor",raw);row.put("journal_sha256",SourceJson.sha(raw));row.put("expected_evidence",reference);row.put("expected_sha256",SourceJson.sha(reference));
        assertThatThrownBy(()->insertCheckpoint(row)).isInstanceOf(org.springframework.dao.DataAccessException.class).hasStackTraceContaining("CONTENT_FETCH_");assertThat(count("content_fetch_checkpoints")).isEqualTo(1);retained();
    }
    @ParameterizedTest @ValueSource(strings={"REPEATABLE_READ","SERIALIZABLE"}) void sqlCompletionGuardsRequireReadCommitted(String isolation){
        candidate();complete();var intent=jdbc.queryForMap("SELECT * FROM ddip_management.content_fetch_completion_intents");var receipt=jdbc.queryForMap("SELECT * FROM ddip_management.content_fetch_checkpoints");
        for(Runnable write:List.<Runnable>of(()->insertIntent(intent),()->insertCheckpoint(receipt))){
            var transaction=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));transaction.setIsolationLevelName("ISOLATION_"+isolation);
            assertThatThrownBy(()->transaction.execute(status->{write.run();return null;})).isInstanceOf(org.springframework.dao.DataAccessException.class).hasStackTraceContaining("CONTENT_FETCH_COMPLETION_ISOLATION_REQUIRED");
        }
        retained();
    }

}
