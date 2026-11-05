package platform.contentapi.management;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Real PostgreSQL reservation-only acceptance. No native child, credentials or endpoint is used. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchReservationIT extends NarrowContentAdmissionFixtures {
    UUID operation;boolean versionEleven;
    @Override void migrateFixtureSchema(){
        if(!versionEleven){super.migrateFixtureSchema();return;}
        org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").target("11").load().migrate();
        new JdbcCoordinatorRepository(jdbc,tx,INSTALL).initialize();
    }
    @Override void profile(String id,boolean content,String source){
        if(content)NarrowContentFetchReservationTestSupport.registerChain(jdbc,id,RunFixtures.profile(true,source),CONTENT);
        else super.profile(id,false,source);
    }
    @Override void ready(){
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE));
    }
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        byte[] bytes=SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:9000").put("region","local").put("bucket","bronze").put("credentialsFile","/never/read/credentials"));
        jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,bytes,SourceJson.sha(bytes),INSTALL);
    }
    @BeforeEach void identity(){operation=UUID.randomUUID();}
    @AfterEach void resetVersion(){versionEleven=false;}
    void prepared(){claimed();prepare();}
    NarrowContentFetchReservationTestSupport.Facts reserve(java.util.function.Consumer<String> boundary){return NarrowContentFetchReservationTestSupport.reserve(jdbc,tx,workers,guard,owner,operation,boundary);}
    NarrowContentFetchReservationTestSupport.Facts facts(){return NarrowContentFetchReservationTestSupport.inspect(jdbc,tx,workers,operation);}
    void retained(){
        noExecution();assertThat(count("content_native_operations")).isEqualTo(1);assertThat(count("content_operations")).isZero();assertThat(count("content_operation_phases")).isZero();
        assertThat(workers.lookup(owner.attemptId()).orElseThrow().state()).isEqualTo("CLAIMED");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
    }
    @Test void reservationContainsOnlyHistoricalFactsAndChangesNeitherFilesNorLegacyState()throws Exception{
        prepared();var files=filesystem();var metadata=metadata();var boundaries=new ArrayList<String>();var result=reserve(boundaries::add);
        assertThat(boundaries).containsExactly("INSPECTED","RESERVED");assertThat(result.nativeLaunchCompatible()).isFalse();assertThat(result.operation()).isEqualTo(operation);assertThat(result.attempt()).isEqualTo(owner.attemptId());
        assertThat(result.commandSha256()).matches("[a-f0-9]{64}");assertThat(result.bootstrapSha256()).matches("[a-f0-9]{64}");assertThat(facts()).isEqualTo(result);
        assertThat(filesystem()).isEqualTo(files);assertThat(metadata()).isEqualTo(metadata);retained();
    }
    @Test void inspectionNeedsNoLiveOwnerOrAttemptFilesystem()throws Exception{
        prepared();var history=reserve(b->{});workers.interrupt(guard,owner);
        Files.move(attemptRoot(),attemptRoot().resolveSibling("removed-attempt"));
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);guard.close();guard=null;
        assertThat(facts()).isEqualTo(history);assertThat(facts().nativeLaunchCompatible()).isFalse();noExecution();
    }
    @Test void missingReadyDoesNotReserve()throws Exception{
        claimed();var files=filesystem();assertThatThrownBy(()->reserve(b->{})).isInstanceOf(RuntimeException.class);assertThat(count("content_native_operations")).isZero();assertThat(filesystem()).isEqualTo(files);
    }
    @ParameterizedTest @ValueSource(strings={"run","attempt","worker","coordinator","stop","cancel"})
    void changedOwnershipOrControlsAfterInspectionPreventReservation(String field)throws Exception{
        prepared();var files=filesystem();
        assertThatThrownBy(()->reserve(b->{if(b.equals("INSPECTED"))switch(field){
            case "run"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());
            case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
            case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",owner.worker().instanceId());
            case "coordinator"->jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");
            case "stop"->jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
            case "cancel"->cancel();
        }})).isInstanceOf(RuntimeException.class);
        assertThat(count("content_native_operations")).isZero();assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void reservationIsImmutableAndCannotBeRetriedWithFreshRepositoryOrOperation(){
        prepared();var original=reserve(b->{});workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        assertThatThrownBy(()->reserve(b->{})).hasMessage("STATE_CONFLICT");
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);operation=UUID.randomUUID();
        assertThatThrownBy(()->reserve(b->{})).hasMessage("STATE_CONFLICT");operation=original.operation();
        for(String sql:List.of("UPDATE ddip_management.content_native_operations SET operation_id=operation_id","DELETE FROM ddip_management.content_native_operations","TRUNCATE ddip_management.content_native_operations CASCADE"))assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(RuntimeException.class);
        assertThat(facts()).isEqualTo(original);retained();
    }
    @Test void probeConsumptionExcludesNativeReservation()throws Exception{
        prepared();assertThatThrownBy(()->NarrowContentOperationTestSupport.probe(jdbc,tx,workers,guard,owner,UUID.randomUUID(),b->{if(b.equals("RESERVED"))throw new IllegalStateException("TEST_BOUNDARY");})).hasMessage("TEST_BOUNDARY");
        var files=filesystem();assertThatThrownBy(()->reserve(b->{})).hasMessage("STATE_CONFLICT");assertThat(count("content_native_operations")).isZero();assertThat(filesystem()).isEqualTo(files);
    }
    @Test void nativeConsumptionExcludesProbe()throws Exception{
        prepared();reserve(b->{});var files=filesystem();assertThatThrownBy(()->NarrowContentOperationTestSupport.probe(jdbc,tx,workers,guard,owner,UUID.randomUUID(),b->{})).hasMessage("STATE_CONFLICT");assertThat(filesystem()).isEqualTo(files);retained();
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void simultaneousReservationsHaveExactlyOneWinnerIncludingProbeRace(boolean probeRace)throws Exception{
        prepared();var files=filesystem();var barrier=new CyclicBarrier(2);var winners=new AtomicInteger();var conflicts=new AtomicInteger();
        try(var pool=Executors.newFixedThreadPool(2)){
            var futures=new ArrayList<Future<?>>();for(int i=0;i<2;i++){final boolean probe=probeRace && i==1;futures.add(pool.submit(()->{
                try{barrier.await(5,TimeUnit.SECONDS);}catch(Exception ex){throw new CompletionException(ex);}
                var fresh=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
                java.util.function.Consumer<String> stop=b->{if(b.equals("RESERVED"))throw new IllegalStateException("TEST_BOUNDARY");};
                try{
                    if(probe)NarrowContentOperationTestSupport.probe(jdbc,tx,fresh,guard,owner,UUID.randomUUID(),stop);else NarrowContentFetchReservationTestSupport.reserve(jdbc,tx,fresh,guard,owner,UUID.randomUUID(),stop);
                    throw new AssertionError("Reservation returned without reaching RESERVED");
                }catch(ManagementFailure ex){
                    if(ex.code()!=ManagementFailure.Code.STATE_CONFLICT)throw ex;
                    conflicts.incrementAndGet();
                }catch(IllegalStateException ex){
                    if(!"TEST_BOUNDARY".equals(ex.getMessage()))throw ex;
                    winners.incrementAndGet();
                }
            }));}for(var future:futures)future.get(15,TimeUnit.SECONDS);
        }
        assertThat(winners).hasValue(1);assertThat(conflicts).hasValue(1);assertThat(count("content_native_operations")+count("content_operations")).isEqualTo(1);assertThat(count("content_operation_phases")).isZero();assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void lostCommitAcknowledgementConsumesReservationAndInvalidatesOwner()throws Exception{
        prepared();var wrapped=loseReservationCommit();var wrappedTx=transactions(wrapped);var uncertain=WorkerTestSupport.enabled(wrapped,wrappedTx,INSTALL);var files=filesystem();
        assertThatThrownBy(()->NarrowContentFetchReservationTestSupport.reserve(wrapped,wrappedTx,uncertain,guard,owner,operation,b->{})).hasMessage("MANAGEMENT_UNAVAILABLE");
        var history=facts();assertThatThrownBy(()->uncertain.heartbeat(guard,owner)).hasMessage("OWNERSHIP_LOST");workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->reserve(b->{})).hasMessage("STATE_CONFLICT");assertThat(facts()).isEqualTo(history);assertThat(filesystem()).isEqualTo(files);retained();
    }
    @Test void populatedVersionElevenUpgradePreservesProbeWithoutNativeBackfill()throws Exception{
        versionEleven=true;setup();prepared();assertThatThrownBy(()->NarrowContentOperationTestSupport.probe(jdbc,tx,workers,guard,owner,UUID.randomUUID(),b->{if(b.equals("RESERVED"))throw new IllegalStateException("TEST_BOUNDARY");})).hasMessage("TEST_BOUNDARY");
        var metadata=metadata();var files=filesystem();var probes=jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_operations t",String.class);
        var migration=org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();assertThat(migration.migrate().migrationsExecuted).isEqualTo(1);
        new SchemaLifecycle(migration,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(false);
        assertThat(metadata()).isEqualTo(metadata);assertThat(filesystem()).isEqualTo(files);assertThat(jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_operations t",String.class)).isEqualTo(probes);assertThat(count("content_native_operations")).isZero();assertThatThrownBy(()->reserve(b->{})).hasMessage("STATE_CONFLICT");
    }
    Process operationChild(String mode,UUID run,UUID attempt)throws Exception{
        Path descriptor=Files.createTempFile(temp,"operation-child-",".json");Files.write(descriptor,binding.descriptor());
        Files.setPosixFilePermissions(descriptor,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                NarrowContentFetchReservationTestChild.class.getName(),descriptor.toString(),mode,CONTENT_PROFILE,run.toString(),attempt.toString(),operation.toString())
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);
        return builder.start();
    }
    String operationLine(Process process)throws Exception{
        var result=CompletableFuture.supplyAsync(()->{try{var reader=new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()));String line;
            while((line=reader.readLine())!=null)if(line.startsWith("NARROW_"))return line;return "EOF";}catch(Exception ex){throw new CompletionException(ex);}});
        return result.get(20,TimeUnit.SECONDS);
    }
    @Test void writerDeathAndFreshJvmInspectionNeverResume()throws Exception{
        UUID run=queue("native-hard-crash"),attempt=UUID.randomUUID();var child=operationChild("RESERVED",run,attempt);
        try{assertThat(operationLine(child)).isEqualTo("NARROW_BOUNDARY "+child.pid()+" RESERVED");}
        finally{child.destroyForcibly();assertThat(child.waitFor(10,TimeUnit.SECONDS)).isTrue();}
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);var history=facts();var files=filesystem();
        var lookup=operationChild("inspect",run,attempt);
        try{assertThat(operationLine(lookup)).isEqualTo("NARROW_OPERATION "+lookup.pid()+" "+history.commandSha256());assertThat(lookup.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(lookup.exitValue()).isZero();}
        finally{if(lookup.isAlive()){lookup.destroyForcibly();lookup.waitFor(5,TimeUnit.SECONDS);}}
        assertThat(facts()).isEqualTo(history);assertThat(filesystem()).isEqualTo(files);ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),run)).hasMessage("WORKER_NOT_READY");noExecution();assertThat(count("content_native_operations")).isEqualTo(1);
    }
    @Test void actualPostgresRestartAndFreshJvmInspectionPreserveOnlyReservationFacts()throws Exception{
        prepared();var history=reserve(b->{});guard.close();guard=null;
        var before=metadata();var files=filesystem();
        var nativeRows=jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_native_operations t ORDER BY operation_id",String.class);
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);
        // The CI harness names its disposable PostgreSQL service. Never discover or choose a host database.
        String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).matches("[0-9a-f]{12,64}");
        var restart=new ProcessBuilder("docker","restart",container).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try{assertThat(restart.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(restart.exitValue()).isZero();}
        finally{if(restart.isAlive()){restart.destroyForcibly();restart.waitFor(5,TimeUnit.SECONDS);}}
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
        while(true){
            try{
                var after=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);assertThat(after).isAfter(started);
                System.out.println("NARROW_FETCH_RESERVATION_POSTGRES_RESTART before="+started+" after="+after+" operation="+operation);break;
            }catch(org.springframework.dao.DataAccessException ex){if(System.nanoTime()>deadline)throw ex;Thread.sleep(100);}
        }
        var lookup=operationChild("inspect",owner.runId(),owner.attemptId());
        try{
            assertThat(lookup.pid()).isNotEqualTo(ProcessHandle.current().pid());
            assertThat(operationLine(lookup)).isEqualTo("NARROW_OPERATION "+lookup.pid()+" "+history.commandSha256());
            assertThat(lookup.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(lookup.exitValue()).isZero();
        }finally{if(lookup.isAlive()){lookup.destroyForcibly();lookup.waitFor(5,TimeUnit.SECONDS);}}
        assertThat(facts()).isEqualTo(history);assertThat(facts().nativeLaunchCompatible()).isFalse();
        assertThat(jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_native_operations t ORDER BY operation_id",String.class)).isEqualTo(nativeRows);
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);retained();
    }
    @Test void preexistingEmptyFetchDirectoryIsNotAdopted()throws Exception{
        prepared();var payload=NarrowContentFetchReservationTestSupport.payload(jdbc,tx,workers,guard,owner,operation);
        Files.createDirectories(attemptRoot().resolve("fetch-source/demo-feed/"+payload.checkpoint()));
        var files=filesystem();assertThatThrownBy(()->reserve(b->{})).isInstanceOf(RuntimeException.class);
        assertThat(count("content_native_operations")).isZero();assertThat(filesystem()).isEqualTo(files);
    }
    @ParameterizedTest @ValueSource(strings={"operation_id","attempt_id","checkpoint_id","command","bootstrap","stage","ordinal"})
    void directSqlCannotReplaceIndependentlyBoundIdentitiesOrCanonicalBytes(String field){
        prepared();var payload=NarrowContentFetchReservationTestSupport.payload(jdbc,tx,workers,guard,owner,operation);
        byte[] command=payload.commandBytes(),bootstrap=payload.bootstrapBytes();
        if(field.equals("command")){var n=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(command);n.put("unexpected","malicious");command=SourceJson.canonical(n);}
        if(field.equals("bootstrap")){var n=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(bootstrap);n.put("unexpected","malicious");bootstrap=SourceJson.canonical(n);}
        final byte[] c=command,b=bootstrap;
        assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.content_native_operations(operation_id,attempt_id,stage,item_ordinal,checkpoint_id,command_bytes,command_sha256,bootstrap_bytes,bootstrap_sha256) VALUES (?,?,?,?,?,?,?,?,?)",
                field.equals("operation_id")?UUID.randomUUID():operation,field.equals("attempt_id")?UUID.randomUUID():owner.attemptId(),field.equals("stage")?"CHECK_INPUTS":"FETCH_SOURCE",field.equals("ordinal")?1:0,field.equals("checkpoint_id")?UUID.randomUUID():payload.checkpoint(),c,SourceJson.sha(c),b,SourceJson.sha(b))).isInstanceOf(RuntimeException.class);
        assertThat(count("content_native_operations")).isZero();noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"protocolVersion","runId","attemptId","ownerEpoch","stage","itemOrdinal","checkpointId","sourceId","revision","capturedConfigSha256","endpointSha256","feedUri","runtimeSha256","javaRuntime","testEndpoint","httpTimeoutMillis"})
    void exactHistoricalCommandCannotSupplyItsOwnIndependentFacts(String field){
        prepared();var p=NarrowContentFetchReservationTestSupport.payload(jdbc,tx,workers,guard,owner,operation);
        var node=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(p.commandBytes());
        if(node.get(field).isBoolean())node.put(field,!node.get(field).booleanValue());
        else if(node.get(field).isIntegralNumber())node.put(field,node.get(field).longValue()+1);
        else node.put(field,"different");
        byte[] command=SourceJson.canonical(node);var bootstrap=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(p.bootstrapBytes());
        ((tools.jackson.databind.node.ObjectNode)bootstrap.get("expected").get("writer")).put("commandSha256",SourceJson.sha(command));
        ((tools.jackson.databind.node.ObjectNode)bootstrap.get("current").get("writer")).put("commandSha256",SourceJson.sha(command));
        byte[] bytes=SourceJson.canonical(bootstrap);
        assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.content_native_operations(operation_id,attempt_id,stage,item_ordinal,checkpoint_id,command_bytes,command_sha256,bootstrap_bytes,bootstrap_sha256) VALUES (?,?,'FETCH_SOURCE',0,?,?,?,?,?)",operation,owner.attemptId(),p.checkpoint(),command,SourceJson.sha(command),bytes,SourceJson.sha(bytes))).isInstanceOf(RuntimeException.class);
        assertThat(count("content_native_operations")).isZero();
    }
    @Test void everyNestedBootstrapIdentityIsIndependentlyBound()throws Exception{
        prepared();var p=NarrowContentFetchReservationTestSupport.payload(jdbc,tx,workers,guard,owner,operation);
        var before=metadata();var files=filesystem();
        var paths=new ArrayList<String>(List.of("reservationVersion","mode","operationId",
                "expected.installationId","expected.policy","expected.planKind","expected.runId","expected.originalAttemptId","expected.sourceId","expected.controlRoot",
                "expected.controlIdentity.device","expected.controlIdentity.inode","expected.rootIdentity.device","expected.rootIdentity.inode","expected.lockIdentity.device","expected.lockIdentity.inode","expected.ledgerSha256","current.policy"));
        for(String section:List.of("expected","current")){
            for(String field:List.of("selectionSha256","profileSha256","runtimeSha256"))paths.add(section+".frozen."+field);
            for(String field:List.of("installationId","workerInstanceId","epoch","runId","attemptId","stage","ordinal","checkpointId","commandSha256"))paths.add(section+".writer."+field);
        }
        for(String field:List.of("revision","capturedConfigSha256","endpointSha256","feedUri","runtimeSha256","javaRuntime","testEndpoint","httpTimeoutMillis"))paths.add("fetch."+field);
        for(String path:paths){
            var bootstrap=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(p.bootstrapBytes());
            var parts=path.split("\\.");var parent=bootstrap;for(int i=0;i<parts.length-1;i++)parent=(tools.jackson.databind.node.ObjectNode)parent.get(parts[i]);
            String field=parts[parts.length-1];var value=parent.get(field);assertThat(value).as(path).isNotNull();
            if(value.isBoolean())parent.put(field,!value.booleanValue());else if(value.isIntegralNumber())parent.put(field,value.longValue()+1);else parent.put(field,"different");
            rejectPayload(p,p.commandBytes(),SourceJson.canonical(bootstrap),path);
        }
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void duplicateKeysAndTypeSubstitutionsCannotHideBehindRecomputedHashes()throws Exception{
        prepared();var p=NarrowContentFetchReservationTestSupport.payload(jdbc,tx,workers,guard,owner,operation);
        var before=metadata();var files=filesystem();
        String command=new String(p.commandBytes(),java.nio.charset.StandardCharsets.UTF_8);
        String bootstrap=new String(p.bootstrapBytes(),java.nio.charset.StandardCharsets.UTF_8);
        // Preserve the same values: jsonb conversion alone would silently collapse these duplicates.
        rejectPayload(p,p.commandBytes(),utf8("{\"reservationVersion\":1,"+bootstrap.substring(1)),"duplicate top-level bootstrap");
        rejectPayload(p,p.commandBytes(),utf8(bootstrap.replace("\"expected\":{","\"expected\":{\"planKind\":\"ORIGINAL\",")),"duplicate expected identity");
        rejectPayload(p,p.commandBytes(),utf8(bootstrap.replace("\"writer\":{","\"writer\":{\"stage\":\"FETCH_SOURCE\",")),"duplicate nested writer identity");
        rejectPayload(p,p.commandBytes(),utf8(bootstrap.replace("\"fetch\":{","\"fetch\":{\"javaRuntime\":\"21\",")),"duplicate fetch fact");
        byte[] duplicateCommand=utf8("{\"protocolVersion\":1,"+command.substring(1));
        rejectPayload(p,duplicateCommand,linkedCommandHash(p.bootstrapBytes(),duplicateCommand),"duplicate command field");
        for(String path:List.of("reservationVersion","expected.controlIdentity.device","expected.writer.epoch","current.writer.ordinal","fetch.revision","fetch.testEndpoint","fetch.httpTimeoutMillis")){
            var node=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(p.bootstrapBytes());
            var parts=path.split("\\.");var parent=node;for(int i=0;i<parts.length-1;i++)parent=(tools.jackson.databind.node.ObjectNode)parent.get(parts[i]);
            String field=parts[parts.length-1];var old=parent.get(field);
            if(old.isString())parent.put(field,Long.parseLong(old.stringValue()));else parent.put(field,old.toString());
            rejectPayload(p,p.commandBytes(),SourceJson.canonical(node),"type substitution "+path);
        }
        for(String field:List.of("protocolVersion","ownerEpoch","itemOrdinal","revision","testEndpoint","httpTimeoutMillis")){
            var node=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(p.commandBytes());node.put(field,node.get(field).toString());byte[] bytes=SourceJson.canonical(node);
            rejectPayload(p,bytes,linkedCommandHash(p.bootstrapBytes(),bytes),"command type "+field);
        }
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void startupRejectsMissingNativeTableWithoutRecreatingIt(){
        jdbc.execute("DROP TABLE ddip_management.content_native_operations CASCADE");
        var migration=org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        var lifecycle=new SchemaLifecycle(migration,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL));
        assertThatThrownBy(()->lifecycle.verify(false)).hasMessage("MANAGEMENT_INCOMPATIBLE");
        assertThatThrownBy(()->lifecycle.verify(true)).hasMessage("MANAGEMENT_INCOMPATIBLE");
        assertThat(jdbc.queryForObject("SELECT to_regclass(?)",String.class,"ddip_management.content_native_operations")).isNull();
    }
    private void rejectPayload(NarrowContentFetchReservationTestSupport.Payload p,byte[] command,byte[] bootstrap,String label){
        assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.content_native_operations(operation_id,attempt_id,stage,item_ordinal,checkpoint_id,command_bytes,command_sha256,bootstrap_bytes,bootstrap_sha256) VALUES (?,?,'FETCH_SOURCE',0,?,?,?,?,?)",operation,owner.attemptId(),p.checkpoint(),command,SourceJson.sha(command),bootstrap,SourceJson.sha(bootstrap))).as(label).isInstanceOf(RuntimeException.class);
        assertThat(count("content_native_operations")).as(label).isZero();
    }
    private static byte[] linkedCommandHash(byte[] bootstrap,byte[] command){
        var node=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(bootstrap);
        for(String section:List.of("expected","current"))((tools.jackson.databind.node.ObjectNode)node.get(section).get("writer")).put("commandSha256",SourceJson.sha(command));
        return SourceJson.canonical(node);
    }
    private static byte[] utf8(String value){return value.getBytes(java.nio.charset.StandardCharsets.UTF_8);}
    @ParameterizedTest @ValueSource(ints={java.sql.Connection.TRANSACTION_REPEATABLE_READ,java.sql.Connection.TRANSACTION_SERIALIZABLE})
    void staleSnapshotIsolationCannotReserveEitherJournal(int isolation)throws Exception{
        prepared();var nativePayload=NarrowContentFetchReservationTestSupport.payload(jdbc,tx,workers,guard,owner,operation);
        UUID probeId=UUID.randomUUID();var probe=NarrowContentOperationTestSupport.reservation(jdbc,tx,guard,owner,probeId);
        var before=metadata();var files=filesystem();
        for(boolean nativeJournal:List.of(true,false)){
            var transaction=isolated(isolation);
            assertThatThrownBy(()->transaction.execute(status->{
                assertThat(jdbc.queryForObject("SHOW transaction_isolation",String.class)).isEqualTo(isolation==java.sql.Connection.TRANSACTION_REPEATABLE_READ?"repeatable read":"serializable");
                if(nativeJournal)insertNative(nativePayload);else insertProbe(probeId,probe);
                return null;
            })).isInstanceOf(org.springframework.dao.DataAccessException.class).hasStackTraceContaining(nativeJournal?"CONTENT_NATIVE_OPERATION_ISOLATION_REQUIRED":"CONTENT_OPERATION_ISOLATION_REQUIRED");
        }
        assertThat(count("content_native_operations")).isZero();assertThat(count("content_operations")).isZero();
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void overlappingEstablishedRepeatableReadSnapshotsCannotBypassCrossJournalExclusion()throws Exception{
        prepared();var nativePayload=NarrowContentFetchReservationTestSupport.payload(jdbc,tx,workers,guard,owner,operation);
        UUID probeId=UUID.randomUUID();var probe=NarrowContentOperationTestSupport.reservation(jdbc,tx,guard,owner,probeId);
        var snapshots=new CyclicBarrier(2);var rejected=new AtomicInteger();var observed=new AtomicInteger();var files=filesystem();
        try(var pool=Executors.newFixedThreadPool(2)){
            var futures=new ArrayList<Future<?>>();for(boolean nativeJournal:List.of(true,false))futures.add(pool.submit(()->{
                try{
                    isolated(java.sql.Connection.TRANSACTION_REPEATABLE_READ).execute(status->{
                        // Both sessions freeze an empty view before either journal's INSERT is attempted.
                        assertThat(count("content_native_operations")+count("content_operations")).isZero();observed.incrementAndGet();
                        try{snapshots.await(5,TimeUnit.SECONDS);}catch(Exception ex){throw new AssertionError(ex);}
                        if(nativeJournal)insertNative(nativePayload);else insertProbe(probeId,probe);
                        return null;
                    });
                }catch(org.springframework.dao.DataAccessException expected){
                    assertThat(expected).hasStackTraceContaining(nativeJournal?"CONTENT_NATIVE_OPERATION_ISOLATION_REQUIRED":"CONTENT_OPERATION_ISOLATION_REQUIRED");rejected.incrementAndGet();
                }
            }));
            for(var future:futures)future.get(15,TimeUnit.SECONDS);
        }
        assertThat(observed).hasValue(2);assertThat(rejected).hasValue(2);
        assertThat(count("content_native_operations")).isZero();assertThat(count("content_operations")).isZero();
        assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void oversizedCommandAndBootstrapCannotReserveDespiteMatchingDigests()throws Exception{
        prepared();var p=NarrowContentFetchReservationTestSupport.payload(jdbc,tx,workers,guard,owner,operation);
        var before=metadata();var files=filesystem();
        // Whitespace preserves JSON semantics and the command-to-bootstrap linkage, isolating the byte ceiling.
        byte[] command=utf8(" ".repeat(65536)+new String(p.commandBytes(),java.nio.charset.StandardCharsets.UTF_8));
        rejectPayload(p,command,linkedCommandHash(p.bootstrapBytes(),command),"oversized exact command");
        byte[] bootstrap=utf8(" ".repeat(65536)+new String(p.bootstrapBytes(),java.nio.charset.StandardCharsets.UTF_8));
        rejectPayload(p,p.commandBytes(),bootstrap,"oversized exact bootstrap");
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    private org.springframework.transaction.support.TransactionTemplate isolated(int isolation){
        var transaction=new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.support.JdbcTransactionManager(jdbc.getDataSource()));
        transaction.setIsolationLevel(isolation);transaction.setTimeout(10);return transaction;
    }
    private void insertNative(NarrowContentFetchReservationTestSupport.Payload p){
        byte[] c=p.commandBytes(),b=p.bootstrapBytes();
        jdbc.update("INSERT INTO ddip_management.content_native_operations(operation_id,attempt_id,stage,item_ordinal,checkpoint_id,command_bytes,command_sha256,bootstrap_bytes,bootstrap_sha256) VALUES (?,?,'FETCH_SOURCE',0,?,?,?,?,?)",operation,owner.attemptId(),p.checkpoint(),c,SourceJson.sha(c),b,SourceJson.sha(b));
    }
    private void insertProbe(UUID probeId,tools.jackson.databind.node.ObjectNode probe){
        byte[] bytes=SourceJson.canonical(probe);
        jdbc.update("INSERT INTO ddip_management.content_operations(operation_id,attempt_id,stage,item_ordinal,checkpoint_id,bootstrap_bytes,bootstrap_sha256) VALUES (?,?,'CHECK_INPUTS',0,?,?,?)",probeId,owner.attemptId(),UUID.fromString(probe.get("checkpointId").stringValue()),bytes,SourceJson.sha(bytes));
    }
    org.springframework.jdbc.core.JdbcTemplate loseReservationCommit(){
        var source=new org.springframework.jdbc.datasource.AbstractDataSource(){
            public java.sql.Connection getConnection()throws java.sql.SQLException{
                var real=jdbc.getDataSource().getConnection();var armed=new AtomicBoolean();
                return (java.sql.Connection)java.lang.reflect.Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{java.sql.Connection.class},(proxy,method,args)->{
                    try{Object result=method.invoke(real,args);
                        if(method.getName().equals("prepareStatement") && args[0] instanceof String sql && sql.startsWith("INSERT INTO ddip_management.content_native_operations("))armed.set(true);
                        if(method.getName().equals("commit") && armed.compareAndSet(true,false))throw new java.sql.SQLException("PRIVATE_ACK","08006");return result;
                    }catch(java.lang.reflect.InvocationTargetException ex){throw ex.getCause();}
                });
            }
            public java.sql.Connection getConnection(String u,String p)throws java.sql.SQLException{return getConnection();}
        };return new org.springframework.jdbc.core.JdbcTemplate(source);
    }
}
