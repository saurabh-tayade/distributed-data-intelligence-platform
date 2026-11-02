package platform.contentapi.importing;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.*;
import platform.contentapi.execution.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.OwnershipFixtures.*;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL, real Python public readers, synthetic namespaces only. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class ImportWorkflowIT {
    @TempDir Path temp;Path repo,python,work;JdbcTemplate admin,jdbc;String database,url,user,password;
    ManagementTransactions tx;JdbcCoordinatorRepository coordinator;ImportOwnershipRepository ownership;
    InstallationHostGuard.Binding host;ImportBindings bindings;ImportBindings.RuntimeProfile runtime;JsonNode fixture;ImportCatalog catalog;
    @BeforeAll void database() {
        repo=Path.of("../..").toAbsolutePath().normalize();python=Path.of(Objects.requireNonNull(System.getenv("DDIP_TEST_PYTHON"))).toAbsolutePath();
        String base=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_URL"));user=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_USER"));password=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_PASSWORD"));
        admin=new JdbcTemplate(new DriverManagerDataSource(base,user,password));database="ddip_b2b1ba_"+UUID.randomUUID().toString().replace("-","");admin.execute("CREATE DATABASE "+database);url=base.substring(0,base.lastIndexOf('/')+1)+database;
        jdbc=new JdbcTemplate(new DriverManagerDataSource(url,user,password));jdbc.setQueryTimeout(3);
        tx=transactions(jdbc);coordinator=new JdbcCoordinatorRepository(jdbc,tx,INSTALL);ownership=new ImportOwnershipRepository(jdbc,tx,INSTALL);bindings=new ImportBindings(jdbc,tx,INSTALL);
    }
    ManagementTransactions transactions(JdbcTemplate queries){var t=new TransactionTemplate(new JdbcTransactionManager(queries.getDataSource()));t.setTimeout(3);return new ManagementTransactions(t);}
    @AfterAll void cleanup(){if(admin!=null && database!=null)admin.execute("DROP DATABASE "+database+" WITH (FORCE)");}
    @BeforeEach void setup() throws Exception {
        jdbc.execute("DROP SCHEMA IF EXISTS ddip_management CASCADE");Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load().migrate();coordinator.initialize();
        work=Files.createTempDirectory(temp,"case-").toRealPath();host=host(work);
        var process=new ProcessBuilder(python.toString(),"-B",repo.resolve("processing/tests/artifact_import_fixtures.py").toString(),work.toString()).redirectError(ProcessBuilder.Redirect.DISCARD);
        process.environment().put("PYTHONPATH",repo.resolve("processing")+":"+repo.resolve("processing/tests"));
        var p=process.start();byte[] raw=p.getInputStream().readNBytes(65537);assertThat(p.waitFor(15,TimeUnit.SECONDS)).isTrue();assertThat(p.exitValue()).isZero();fixture=SourceJson.parse(raw);
        var config=SourceJson.object().put("importProtocolVersion",1).put("python",python.toString()).put("processingRoot",repo.resolve("processing").toRealPath().toString()).put("pythonVersion","3.12.15").put("duckdbVersion","1.4.5").put("timeoutSeconds",10);
        var hashes=config.putObject("modules");for(String name:ImportBindings.MODULES)hashes.put(name,SourceJson.sha(Files.readAllBytes(repo.resolve("processing").resolve(name))));
        byte[] bytes=SourceJson.canonical(config);jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES ('native-import-v1',?,?)",bytes,SourceJson.sha(bytes));
        storage(host.id(),"content-intelligence","CONTROL",host.descriptor());
        var names=fixture.get("roots").properties();for(var pair:names){Path root=Path.of(pair.getValue().stringValue());var identity=InstallationHostGuard.identity(root);byte[] descriptor=SourceJson.canonical(SourceJson.object().put("importStorageVersion",1).put("root",root.toString()).put("rootDevice",Long.toString(identity.device())).put("rootInode",Long.toString(identity.inode())));
            String id=pair.getKey();storage(id,id.equals("publisher-refs")?"content-intelligence":"application-reliability",id.endsWith("refs")?"REFERENCE":"BATCH",descriptor);}
        for(JsonNode e:fixture.get("artifacts"))if(e.get("kind").stringValue().equals("RELIABILITY_BATCH_V2")) {
            var i=e.get("identity");String source=i.get("sourceId").stringValue(),profile=i.get("inputFormat").stringValue().equals("JSONL")?"request-jsonl-v2":"request-parquet-v1";
            var c=SourceJson.object().put("configVersion",1).put("profile",profile).put("dataOrigin","synthetic").putNull("feedEndpointId").put("registeredProfileSha256",i.get("registeredProfileSha256").stringValue()).put("inputStorageProfileId",e.get("storageBindingId").stringValue());byte[] cb=SourceJson.canonical(c);
            tx.required(()->{jdbc.update("INSERT INTO ddip_management.sources(product_id,source_id,profile_family,current_revision,enabled) VALUES ('application-reliability',?,?,1,false)",source,profile);
                jdbc.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor) VALUES ('application-reliability',?,1,'Synthetic import fixture',1,?,?,'local-operator')",source,cb,SourceJson.sha(cb));return null;});
        }
        runtime=bindings.runtime("native-import-v1");catalog=catalog(UUID.randomUUID(),0,1,2,3);
    }
    void storage(String id,String product,String role,byte[] bytes){jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,bytes,SourceJson.sha(bytes),INSTALL);jdbc.update("INSERT INTO ddip_management.execution_profile_storage VALUES ('native-import-v1',?)",id);}
    ImportCatalog catalog(UUID id,int... indexes){var n=SourceJson.object().put("catalogVersion",1).put("operationId",id.toString()).put("executionProfileId","native-import-v1").put("controlBindingId",host.id());var a=n.putArray("artifacts");for(int i:indexes)a.add(fixture.get("artifacts").get(i));return ImportCatalog.parse(SourceJson.canonical(n));}
    OperatorImportService service(NativeImportProcess process){return new OperatorImportService(bindings,ownership,coordinator,new NativeImportVerifier(process),INSTALL);}
    JsonNode execute(ImportCatalog c){return service(new NativeImportProcess()).execute(c);}
    long count(String table){return jdbc.queryForObject("SELECT count(*) FROM ddip_management."+table,Long.class);}
    ImportOwnershipRepository.Operation op(ImportCatalog c){return ownership.lookup(c.operationId(),c.requestBytes()).orElseThrow();}
    ImportOwnershipRepository.Token reserve(InstallationHostGuard guard,ImportCatalog c){return ownership.reserve(guard,new ImportOwnershipRepository.Reservation(c.operationId(),runtime.id(),runtime.digest(),c.requestBytes()));}
    List<QualifiedArtifact> qualify(ImportCatalog c){var v=new NativeImportVerifier(new NativeImportProcess());return c.entries().stream().map(e->v.verify(c.operationId(),e,bindings.storage(e,c.profileId()),runtime)).toList();}
    Path selected(int index,String name){var a=fixture.get("artifacts").get(index);return Path.of(fixture.get("roots").get(a.get("storageBindingId").stringValue()).stringValue()).resolve(a.get("relativeAddress").stringValue()).resolve(name);}
    @Test void successfulNativeImportSupportsSafeReliabilityDiscovery()throws Exception {
        var imported=execute(catalog);assertThat(imported.get("status").stringValue()).isEqualTo("VERIFIED");
        var sourceCatalog=SourceJson.object().put("catalogVersion",1);sourceCatalog.putArray("endpoints");var approved=sourceCatalog.putArray("reliabilityProfiles");var storageIds=sourceCatalog.putArray("storageProfiles");
        for(var entry:fixture.get("artifacts"))if(entry.get("kind").stringValue().equals("RELIABILITY_BATCH_V2")) {
            var identity=entry.get("identity");String profile=identity.get("inputFormat").stringValue().equals("JSONL")?"request-jsonl-v2":"request-parquet-v1";
            approved.addObject().put("sourceId",identity.get("sourceId").stringValue()).put("profile",profile).put("dataOrigin","synthetic").put("registeredProfileSha256",identity.get("registeredProfileSha256").stringValue());
            storageIds.addObject().put("profileId",entry.get("storageBindingId").stringValue()).put("productId","application-reliability").put("role","BATCH");
        }
        var reviewed=platform.contentapi.management.PreflightCatalogFixture.from(sourceCatalog);
        var sources=new platform.contentapi.management.SourceRepository(jdbc,tx,INSTALL,reviewed,platform.contentapi.management.PreflightCatalogFixture.cursor());
        for(var entry:approved)sources.execute(platform.contentapi.management.SourceCommands.command(platform.contentapi.management.SourceCommands.Action.ENABLE,"application-reliability",entry.get("sourceId").stringValue(),SourceJson.object().put("enabled",true),"discovery-enable-"+entry.get("sourceId").stringValue(),"\"0\""),"test");
        String profileId="10000000-0000-4000-8000-000000000002";var config=SourceJson.object().put("runProfileVersion",1).put("pipeline","RELIABILITY_V2").put("pipelineVersion",1).put("sourceProfile","request-jsonl-v2");
        config.putObject("runtime").put("javaVersion","21").put("pythonVersion","3.12.15").put("pysparkVersion","4.0.2").put("deploymentSha256","a".repeat(64));byte[] bytes=SourceJson.canonical(config);
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",profileId,bytes,SourceJson.sha(bytes));
        for(String role:List.of("RELIABILITY","STAGING","TEMP"))storage("discovery-"+role,"application-reliability",role,OBJECT);
        jdbc.update("INSERT INTO ddip_management.execution_profile_storage SELECT ?,binding_id FROM ddip_management.storage_bindings WHERE product='application-reliability' OR role='CONTROL'",profileId);
        Path sourceFile=Files.write(work.resolve("discovery-source-catalog.json"),SourceJson.canonical(sourceCatalog));
        int port;try(var socket=new java.net.ServerSocket(0,1,java.net.InetAddress.getLoopbackAddress())){port=socket.getLocalPort();}String origin="http://127.0.0.1:"+port;
        var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");long events=count("audit_events");
        // Missing native completion does not make historical registration metadata a fresh verification.
        Files.delete(selected(0,"complete.json"));
        try(var api=new org.springframework.boot.builder.SpringApplicationBuilder(platform.contentapi.ContentApiApplication.class).run("--ddip.role=api","--server.address=127.0.0.1","--server.port="+port,"--ddip.database.url="+url,"--ddip.database.username="+user,"--ddip.database.password="+password,"--ddip.installation-id="+INSTALL,"--ddip.management.catalog="+sourceFile,"--ddip.management.origins="+origin,"--logging.level.root=OFF","--spring.main.banner-mode=off");var client=java.net.http.HttpClient.newHttpClient()) {
            for(String path:List.of("/execution-profiles","/artifacts?executionProfileId="+profileId+"&role=RELIABILITY_BATCH","/artifacts?executionProfileId="+profileId+"&role=SERVICE_REFERENCE")) {
                var response=client.send(java.net.http.HttpRequest.newBuilder(java.net.URI.create(origin+"/api/management/v1"+path)).GET().build(),java.net.http.HttpResponse.BodyHandlers.ofByteArray());
                assertThat(response.statusCode()).isEqualTo(200);assertThat(response.headers().firstValue("Cache-Control")).contains("no-store");var envelope=SourceJson.parse(response.body());assertThat(envelope.get("apiVersion").intValue()).isEqualTo(1);assertThat(envelope.get("data").size()).isEqualTo(1);
                String text=new String(response.body(),java.nio.charset.StandardCharsets.UTF_8);assertThat(text.contains(work.toString())).isFalse();assertThat(text.contains(password)).isFalse();assertThat(text.contains(url)).isFalse();assertThat(text).doesNotContain("relativeAddress","evidence_bytes","verification","capture","command.json");
                if(path.contains("role=RELIABILITY_BATCH"))assertThat(envelope.get("data").get(0).get("nativeFingerprint").stringValue()).isEqualTo(fixture.get("artifacts").get(0).get("fingerprint").stringValue());
            }
        }
        assertThat(Files.exists(selected(0,"complete.json"))).isFalse();assertThat(count("artifacts")).isEqualTo(4);assertThat(count("audit_events")).isEqualTo(events);assertThat(count("runs")).isZero();assertThat(count("attempts")).isZero();assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);
    }
    @Test void allFourRegisterAtomicallyAndReimportsReverifyAndReuseIdentity() throws Exception {
        Map<Path,String> before=new HashMap<>();for(JsonNode root:fixture.get("roots")){try(var files=Files.walk(Path.of(root.stringValue()))){for(Path f:files.filter(Files::isRegularFile).toList())before.put(f,SourceJson.sha(Files.readAllBytes(f))+Files.getLastModifiedTime(f));}}
        var first=execute(catalog);assertThat(first.get("status").stringValue()).isEqualTo("VERIFIED");assertThat(count("artifacts")).isEqualTo(4);assertThat(count("artifact_dependencies")).isZero();assertThat(count("audit_events")).isEqualTo(2);
        var again=execute(catalog(UUID.randomUUID(),0,1,2,3));assertThat(again.get("artifactIds")).isEqualTo(first.get("artifactIds"));assertThat(count("artifacts")).isEqualTo(4);assertThat(coordinator.read().activeImportId()).isNull();
        for(byte[] raw:jdbc.query("SELECT evidence_bytes FROM ddip_management.artifacts",(r,n)->r.getBytes(1))){var e=ImportJson.parse(raw,512*1024);assertThat(e.get("verification").get("native").get("dependencies").size()).isPositive();assertThat(e.get("verification").get("capture").get("completionBase64").stringValue()).isNotBlank();}
        for(var e:before.entrySet())assertThat(SourceJson.sha(Files.readAllBytes(e.getKey()))+Files.getLastModifiedTime(e.getKey())).isEqualTo(e.getValue());assertThat(count("runs")).isZero();assertThat(count("attempts")).isZero();
    }
    @Test void sameCommandIsHistoricalLookupAndDoesNotClaimCurrentReadability() throws Exception {
        execute(catalog);Files.delete(selected(0,"complete.json"));var result=execute(catalog);assertThat(result.get("status").stringValue()).isEqualTo("HISTORICAL_RESULT");assertThat(result.get("revalidated").booleanValue()).isFalse();
        var fresh=execute(catalog(UUID.randomUUID(),0));assertThat(fresh.get("status").stringValue()).isEqualTo("FAILED");assertThat(count("artifacts")).isEqualTo(4);
    }
    @Test void changedEvidenceSameNativeIdentityConflicts() throws Exception {execute(catalog(UUID.randomUUID(),3));Path p=selected(3,"complete.json");var changed=(tools.jackson.databind.node.ObjectNode)SourceJson.parse(Files.readAllBytes(p));changed.put("created_at","2026-10-21T00:00:00.000000Z");Files.write(p,SourceJson.canonical(changed));var result=execute(catalog(UUID.randomUUID(),3));assertThat(result.get("code").stringValue()).isEqualTo("IDENTITY_CONFLICT");assertThat(count("artifacts")).isEqualTo(1);assertThat(coordinator.read().activeImportId()).isNull();}
    @Test void locationConflictRollsBackEveryArtifactAndAudit() {
        var c=catalog(UUID.randomUUID(),0,1,2,3);var e=c.entries().getLast();jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at) VALUES (?,'PUBLISHER_REFERENCE',?,1,1,'test-fixture-conflicting-identity',?,?,?, ?,now())",UUID.randomUUID(),e.product(),e.bindingId(),e.address(),OBJECT,HASH);
        var result=execute(c);assertThat(result.get("code").stringValue()).isEqualTo("IDENTITY_CONFLICT");assertThat(count("artifacts")).isEqualTo(1);assertThat(count("audit_events")).isEqualTo(2);assertThat(op(c).state()).isEqualTo(ImportOwnershipRepository.State.FAILED);
    }
    @Test void finalAuditFailureRollsBackRegistryAndRetainsOwnership() {jdbc.execute("ALTER TABLE ddip_management.audit_events ADD CONSTRAINT injected CHECK(action<>'IMPORT_FINISHED')");assertThatThrownBy(()->execute(catalog)).hasMessage("STATE_CONFLICT").hasNoCause();assertThat(count("artifacts")).isZero();assertThat(count("audit_events")).isEqualTo(1);assertThat(op(catalog).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(coordinator.read().activeImportId()).isEqualTo(catalog.operationId());}
    @Test void exclusiveSecondImporterCannotQualify() {try(var guard=InstallationHostGuard.acquire(host)){reserve(guard,catalog);assertThatThrownBy(()->execute(catalog(UUID.randomUUID(),0))).hasMessage("HOST_GUARD_OCCUPIED");}assertThat(execute(catalog).get("status").stringValue()).isEqualTo("RECOVERY_REQUIRED");assertThatThrownBy(()->execute(catalog(UUID.randomUUID(),0))).hasMessage("OWNERSHIP_STALE");assertThat(count("artifacts")).isZero();}
    @Test void staleTokenCannotInsertArtifacts() {try(var guard=InstallationHostGuard.acquire(host)){var t=reserve(guard,catalog);var q=qualify(catalog);var stale=new ImportOwnershipRepository.Token(t.importId(),t.processInstanceId(),t.epoch()-1,t.operationVersion(),t.coordinatorVersion());assertThatThrownBy(()->ownership.finishVerified(guard,stale,q)).hasMessage("OWNERSHIP_STALE");assertThat(count("artifacts")).isZero();}}
    @Test void concurrentFinalizationHasOneLogicalRegistration() throws Exception {try(var guard=InstallationHostGuard.acquire(host)){var t=reserve(guard,catalog);var q=qualify(catalog);try(var pool=Executors.newFixedThreadPool(2)){var gate=new CountDownLatch(1);var futures=new ArrayList<Future<Boolean>>();for(int i=0;i<2;i++)futures.add(pool.submit(()->{gate.await();try{ownership.finishVerified(guard,t,q);return true;}catch(ImportOwnershipFailure f){return false;}}));gate.countDown();int wins=0;for(var f:futures)if(f.get(10,TimeUnit.SECONDS))wins++;assertThat(wins).isEqualTo(1);}}assertThat(count("artifacts")).isEqualTo(4);assertThat(count("audit_events")).isEqualTo(2);}
    @ParameterizedTest @ValueSource(strings={"START_FAILED","TIMEOUT","RESULT_INVALID","NATIVE_UNAVAILABLE","NATIVE_CHANGED"})
    void knownQuiescentFailureFinalizesWithoutArtifacts(String code){var process=new NativeImportProcess(){public Result run(List<String> c,Path d,int t,byte[] r){assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();throw new ImportFailure(ImportFailure.Code.valueOf(code));}};var result=service(process).execute(catalog);assertThat(result.get("code").stringValue()).isEqualTo(code);assertThat(op(catalog).state()).isEqualTo(ImportOwnershipRepository.State.FAILED);assertThat(count("artifacts")).isZero();assertThat(coordinator.read().activeImportId()).isNull();}
    @Test void uncertainCleanupRetainsActiveOwner(){var p=new NativeImportProcess(){public Result run(List<String> c,Path d,int t,byte[] r){throw new ImportFailure(ImportFailure.Code.CLEANUP_UNCERTAIN,false);}};assertThat(service(p).execute(catalog).get("status").stringValue()).isEqualTo("RECOVERY_REQUIRED");assertThat(op(catalog).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(coordinator.read().activeImportId()).isEqualTo(catalog.operationId());}
    @Test void unknownFailureAfterNativeVerificationRetainsOwner(){var p=new NativeImportProcess(){int calls;public Result run(List<String> c,Path d,int t,byte[] r){var result=super.run(c,d,t,r);if(++calls==4)throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_UNAVAILABLE);return result;}};assertThatThrownBy(()->service(p).execute(catalog)).hasMessage("MANAGEMENT_UNAVAILABLE");assertThat(count("artifacts")).isZero();assertThat(op(catalog).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);}
    @Test void liveOwnerCannotBeRecoveredEvenWithConfirmation(){try(var guard=InstallationHostGuard.acquire(host)){reserve(guard,catalog);}assertThatThrownBy(()->service(new NativeImportProcess()).recover(catalog,catalog.operationId(),UUID.randomUUID(),true)).hasMessage("OWNER_PROCESS_UNRESOLVED");assertThat(op(catalog).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);}
    @Test void missingBindingsFailBeforeTrust(){var n=SourceJson.parse(catalog.requestBytes());((tools.jackson.databind.node.ObjectNode)n).put("executionProfileId","absent");var c=ImportCatalog.parse(SourceJson.canonical(n));assertThatThrownBy(()->execute(c)).hasMessage("BINDING_INVALID");assertThat(count("artifact_import_operations")).isZero();}
    @Test void malformedNativeResponseCannotRegister(){var p=new NativeImportProcess(){public Result run(List<String> c,Path d,int t,byte[] r){return new Result(0,"{}".getBytes());}};var result=service(p).execute(catalog);assertThat(result.get("code").stringValue()).isEqualTo("RESULT_INVALID");assertThat(count("artifacts")).isZero();}
    @Test void actualCommandRoleIsNonWebAndNoAnalyticsPool() throws Exception {
        Path file=Files.write(work.resolve("catalog.json"),catalog.requestBytes());Files.setPosixFilePermissions(file,PosixFilePermissions.fromString("rw-------"));
        try(var context=new org.springframework.boot.builder.SpringApplicationBuilder(platform.contentapi.ContentApiApplication.class).run("--spring.main.banner-mode=off","--logging.level.root=OFF","--ddip.role=operator-import","--ddip.database.url="+url,"--ddip.database.username="+user,"--ddip.database.password="+password,"--ddip.installation-id="+INSTALL,"--ddip.import.catalog="+file)) {
            assertThat(context).isNotInstanceOf(org.springframework.boot.web.server.servlet.context.ServletWebServerApplicationContext.class);
            assertThat(context.getBeansOfType(platform.contentapi.AnalyticsController.class)).isEmpty();assertThat(context.getBeansOfType(platform.contentapi.PublisherController.class)).isEmpty();assertThat(context.getBeansOfType(WorkerIdleLifecycle.class)).isEmpty();assertThat(context.getBeansOfType(platform.contentapi.BoundedProcessRunner.class)).isEmpty();
            assertThat(context.getBean(OperatorImportConfiguration.OperatorCommand.class).getExitCode()).isZero();
        }
        assertThat(count("artifacts")).isEqualTo(4);assertThat(count("runs")).isZero();
    }
    ImportOwnershipRepository lostAckRepository(AtomicBoolean armed) {
        var real=new DriverManagerDataSource(url,user,password);
        var ds=new org.springframework.jdbc.datasource.AbstractDataSource() {
            public Connection getConnection() throws SQLException {
                Connection c=real.getConnection();AtomicBoolean wrote=new AtomicBoolean();
                return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,m,args)->{
                    try {
                        if(m.getName().equals("prepareStatement") && args!=null && args[0] instanceof String sql && (sql.startsWith("INSERT") || sql.startsWith("UPDATE")))wrote.set(true);
                        Object value=m.invoke(c,args);
                        if(m.getName().equals("commit") && wrote.get() && armed.compareAndSet(true,false))throw new SQLException("PRIVATE lost response","08006");
                        return value;
                    } catch(InvocationTargetException e){throw e.getCause();}
                });
            }
            public Connection getConnection(String u,String p) throws SQLException{return getConnection();}
        };
        var queries=new JdbcTemplate(ds);return new ImportOwnershipRepository(queries,transactions(queries),INSTALL);
    }
    @Test void realCommittedRegistrationWithLostAckResolvesByExactLookup() {
        try(var guard=InstallationHostGuard.acquire(host)) {
            var t=reserve(guard,catalog);var q=qualify(catalog);var armed=new AtomicBoolean(true);
            assertThatThrownBy(()->lostAckRepository(armed).finishVerified(guard,t,q)).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();
            assertThat(armed).isFalse();assertThat(op(catalog).state()).isEqualTo(ImportOwnershipRepository.State.SUCCEEDED);
            assertThat(count("artifacts")).isEqualTo(4);assertThat(count("audit_events")).isEqualTo(2);assertThat(coordinator.read().activeImportId()).isNull();
            assertThatThrownBy(()->ownership.finishVerified(guard,t,q)).hasMessage("OWNERSHIP_STALE");
        }
        var result=service(new NativeImportProcess()).inspect(catalog,catalog.operationId());assertThat(result.get("status").stringValue()).isEqualTo("HISTORICAL_RESULT");
        assertThat(result.get("result").get("artifactIds").size()).isEqualTo(4);
    }
    @ParameterizedTest @ValueSource(strings={"BEFORE","AFTER"})
    void realDatabaseConnectionOutageNeverCommitsPartialTrust(String phase) {
        var process=new NativeImportProcess(){int calls;public Result run(List<String> c,Path d,int t,byte[] r){var result=super.run(c,d,t,r);if(++calls==8)admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS false");return result;}};
        if(phase.equals("BEFORE"))admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS false");
        try {assertThatThrownBy(()->service(process).execute(catalog)).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();}
        finally {admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS true");}
        assertThat(count("artifacts")).isZero();
        if(phase.equals("BEFORE"))assertThat(count("artifact_import_operations")).isZero();
        else {assertThat(op(catalog).state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(coordinator.read().activeImportId()).isEqualTo(catalog.operationId());}
    }
    @ParameterizedTest @ValueSource(strings={"reserved","qualified"})
    void killedImporterRemainsBlockedUntilExplicitFencedRecovery(String phase) throws Exception {
        Path file=Files.write(work.resolve("child-catalog.json"),catalog.requestBytes());
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),ImportWorkflowChild.class.getName(),file.toString(),phase).redirectError(ProcessBuilder.Redirect.DISCARD);
        builder.environment().put("DDIP_CHILD_DB_URL",url);Process child=builder.start();
        try {
            var ready=CompletableFuture.supplyAsync(()->{try{return new BufferedReader(new InputStreamReader(child.getInputStream())).readLine();}catch(IOException e){throw new UncheckedIOException(e);}});
            assertThat(ready.get(15,TimeUnit.SECONDS)).isEqualTo("CHECKPOINT");assertThat(coordinator.read().activeImportId()).isEqualTo(catalog.operationId());
            child.destroyForcibly();assertThat(child.waitFor(5,TimeUnit.SECONDS)).isTrue();
            assertThat(execute(catalog).get("status").stringValue()).isEqualTo("RECOVERY_REQUIRED");
            assertThatThrownBy(()->execute(catalog(UUID.randomUUID(),0))).hasMessage("OWNERSHIP_STALE");
            UUID recovery=UUID.randomUUID();var service=service(new NativeImportProcess());
            assertThatThrownBy(()->service.recover(catalog,catalog.operationId(),recovery,false)).hasMessage("QUIESCENCE_REQUIRED");
            assertThat(service.recover(catalog,catalog.operationId(),recovery,true).get("status").stringValue()).isEqualTo("RECOVERY_CLOSED");
            assertThat(op(catalog).state()).isEqualTo(ImportOwnershipRepository.State.INTERRUPTED);assertThat(coordinator.read().activeImportId()).isNull();assertThat(count("artifacts")).isZero();
            assertThat(service.recover(catalog,catalog.operationId(),recovery,true).get("status").stringValue()).isEqualTo("HISTORICAL_RESULT");
        } finally {child.destroyForcibly();child.waitFor(5,TimeUnit.SECONDS);}
    }
    @Test void evidenceCannotBeReusedForAnotherOperation() {
        var q=qualify(catalog);var other=catalog(UUID.randomUUID(),0,1,2,3);
        try(var guard=InstallationHostGuard.acquire(host)){var token=reserve(guard,other);assertThatThrownBy(()->ownership.finishVerified(guard,token,q)).hasMessage("INVALID_REQUEST");}
        assertThat(count("artifacts")).isZero();
    }

    @ParameterizedTest @ValueSource(strings={"identity","mode","exit","verifier","inventory","completion","unknown","oversized"})
    void inconsistentNativeProtocolFailsClosed(String variant) {
        var p=new NativeImportProcess(){public Result run(List<String> c,Path d,int t,byte[] r) {
            var actual=super.run(c,d,t,r);var n=(tools.jackson.databind.node.ObjectNode)ImportJson.parse(actual.stdout(),512*1024);
            int exit=actual.exitCode();
            switch(variant){
                case "identity"->n.put("operationId",UUID.randomUUID().toString());case "mode"->n.put("mode","RECHECK");case "exit"->exit=1;
                case "verifier"->((tools.jackson.databind.node.ObjectNode)n.get("evidence").get("native")).put("verifier","fake.open");
                case "inventory"->((tools.jackson.databind.node.ObjectNode)n.get("evidence").get("capture")).set("files",SourceJson.object().putArray("empty"));
                case "completion"->((tools.jackson.databind.node.ObjectNode)n.get("evidence").get("capture")).put("completionBase64","e30=");
                case "unknown"->((tools.jackson.databind.node.ObjectNode)n.get("evidence")).put("unreviewed","x");
                case "oversized"->{return new Result(0,new byte[512*1024+1]);}
                default->throw new IllegalArgumentException();
            }
            return new Result(exit,SourceJson.canonical(n));
        }};
        var result=service(p).execute(catalog);assertThat(result.get("code").stringValue()).isEqualTo("RESULT_INVALID");assertThat(count("artifacts")).isZero();assertThat(op(catalog).state()).isEqualTo(ImportOwnershipRepository.State.FAILED);
    }
    @Test void missingStorageBindingAfterReservationSafelyFails() {
        var n=SourceJson.parse(catalog.requestBytes());((tools.jackson.databind.node.ObjectNode)n.get("artifacts").get(0)).put("storageBindingId","absent");var c=ImportCatalog.parse(SourceJson.canonical(n));
        assertThat(execute(c).get("code").stringValue()).isEqualTo("BINDING_INVALID");assertThat(op(c).state()).isEqualTo(ImportOwnershipRepository.State.FAILED);assertThat(count("artifacts")).isZero();
    }
    @Test void realSymlinkEscapeCannotRegister() throws Exception {
        Path f=selected(0,"data/events.jsonl"),outside=work.resolve("outside.jsonl");Files.move(f,outside);Files.createSymbolicLink(f,outside);
        var c=catalog(UUID.randomUUID(),0);var result=execute(c);assertThat(result.get("code").stringValue()).isEqualTo("NATIVE_INVALID");assertThat(count("artifacts")).isZero();assertThat(coordinator.read().activeImportId()).isNull();
    }
    @Test void registeredIdentitySurvivesFreshConnectionAndOptionalActualRestart() throws Exception {
        execute(catalog);var before=jdbc.queryForList("SELECT artifact_id,evidence_sha256,verified_at FROM ddip_management.artifacts ORDER BY artifact_id");
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);
        String signal=System.getProperty("ddip.test.restart-signal");
        if(signal!=null) {
            Path marker=Path.of(signal);Files.writeString(marker,"READY");long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
            while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<deadline)Thread.sleep(100);
            assertThat(Files.exists(Path.of(signal+".done"))).isTrue();
        }
        var freshJdbc=new JdbcTemplate(new DriverManagerDataSource(url,user,password));
        if(signal!=null)assertThat(freshJdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class)).isAfter(started);
        assertThat(freshJdbc.queryForList("SELECT artifact_id,evidence_sha256,verified_at FROM ddip_management.artifacts ORDER BY artifact_id")).isEqualTo(before);
        var freshRepository=new ImportOwnershipRepository(freshJdbc,transactions(freshJdbc),INSTALL);
        assertThat(freshRepository.lookup(catalog.operationId(),catalog.requestBytes()).orElseThrow().state()).isEqualTo(ImportOwnershipRepository.State.SUCCEEDED);
    }

    @Test void packagedOperatorCommandImportsInspectsAndExitsWithSafeStatuses() throws Exception {
        Path file=Files.write(work.resolve("command-catalog.json"),catalog.requestBytes());Files.setPosixFilePermissions(file,PosixFilePermissions.fromString("rw-------"));
        for(String action:List.of("import","inspect")) {
            var b=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-jar",repo.resolve("services/content-api/target/content-api-0.1.0-SNAPSHOT.jar").toString(),"--spring.main.banner-mode=off","--logging.level.root=OFF");
            var env=b.environment();env.put("DDIP_ROLE","operator-import");env.put("DDIP_DB_URL",url);env.put("DDIP_DB_USER",user);env.put("DDIP_DB_PASSWORD",password);env.put("DDIP_INSTALLATION_ID",INSTALL.toString());env.put("DDIP_IMPORT_CATALOG",file.toString());env.put("DDIP_IMPORT_ACTION",action);
            Process p=b.redirectError(ProcessBuilder.Redirect.DISCARD).start();
            try {
                var output=CompletableFuture.supplyAsync(()->{try{return p.getInputStream().readNBytes(65537);}catch(IOException e){throw new UncheckedIOException(e);}});
                assertThat(p.waitFor(15,TimeUnit.SECONDS)).isTrue();assertThat(p.exitValue()).isZero();
                var result=ImportJson.parse(output.get(2,TimeUnit.SECONDS),65536);
                assertThat(result.get("status").stringValue()).isEqualTo(action.equals("import")?"VERIFIED":"HISTORICAL_RESULT");
            } finally {p.destroyForcibly();p.waitFor(5,TimeUnit.SECONDS);}
        }
        assertThat(count("artifacts")).isEqualTo(4);assertThat(count("runs")).isZero();
    }

    @Test void concurrentIdenticalCatalogsCannotDuplicateRegistry() throws Exception {
        var other=catalog(UUID.randomUUID(),0,1,2,3);var gate=new CountDownLatch(1);
        try(var pool=Executors.newFixedThreadPool(2)) {
            var results=new ArrayList<Future<JsonNode>>();
            for(var c:List.of(catalog,other))results.add(pool.submit(()->{gate.await();try{return execute(c);}catch(ImportOwnershipFailure f){return SourceJson.object().put("status",f.getMessage());}}));
            gate.countDown();int verified=0;JsonNode ids=null;
            for(var future:results){var result=future.get(15,TimeUnit.SECONDS);if(result.get("status").stringValue().equals("VERIFIED")){verified++;if(ids!=null)assertThat(result.get("artifactIds")).isEqualTo(ids);ids=result.get("artifactIds");}else assertThat(result.get("status").stringValue()).isIn("HOST_GUARD_OCCUPIED","OWNERSHIP_STALE");}
            assertThat(verified).isPositive();
        }
        assertThat(count("artifacts")).isEqualTo(4);assertThat(coordinator.read().activeImportId()).isNull();
    }

}
