package platform.contentapi.importing;

import java.io.*;
import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.*;
import java.lang.reflect.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import com.sun.net.httpserver.HttpServer;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import platform.contentapi.execution.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static platform.contentapi.execution.OwnershipFixtures.*;
import static org.assertj.core.api.Assertions.*;

/** Gate B: actual Silver/PySpark, boto3/loopback HTTP and PostgreSQL. Never a retained dataset. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class SilverImportIT {
    @TempDir static Path temp;Path repo,python,fixtures,work,silver,scratch;JsonNode fixture;
    JdbcTemplate admin,jdbc;String database,url,user,password;ManagementTransactions tx;
    JdbcCoordinatorRepository coordinator;ImportOwnershipRepository ownership;ImportBindings bindings;InstallationHostGuard.Binding host;ImportBindings.RuntimeProfile runtime;ImportCatalog catalog;
    HttpServer server;Map<String,byte[]> objects=new ConcurrentHashMap<>();List<String> requests=new CopyOnWriteArrayList<>();AtomicInteger status=new AtomicInteger(200);ExecutorService http;
    @BeforeAll void initialize() throws Exception {
        repo=Path.of("../..").toAbsolutePath().normalize();python=Path.of(Objects.requireNonNull(System.getenv("DDIP_TEST_PYTHON"))).toAbsolutePath();fixtures=Files.createDirectory(temp.resolve("fixtures")).toRealPath();
        var builder=new ProcessBuilder(python.toString(),"-B",repo.resolve("processing/tests/silver_import_fixtures.py").toString(),fixtures.toString()).redirectError(ProcessBuilder.Redirect.DISCARD);
        builder.environment().clear();builder.environment().putAll(Map.of("PATH",Path.of(System.getProperty("java.home"),"bin")+":/usr/bin:/bin","JAVA_HOME",System.getProperty("java.home"),"PYTHONPATH",repo.resolve("processing")+":"+repo.resolve("processing/tests"),"PYSPARK_PYTHON",python.toString(),"SPARK_LOCAL_IP","127.0.0.1","TZ","UTC"));
        Process p=builder.start();try{var output=CompletableFuture.supplyAsync(()->{try{return p.getInputStream().readNBytes(65537);}catch(IOException e){throw new UncheckedIOException(e);}});assertThat(p.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(p.exitValue()).isZero();fixture=ImportJson.parse(output.get(2,TimeUnit.SECONDS),65536);}finally{p.destroyForcibly();p.waitFor(5,TimeUnit.SECONDS);}
        String base=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_URL"));user=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_USER"));password=Objects.requireNonNull(System.getenv("DDIP_TEST_DB_PASSWORD"));admin=new JdbcTemplate(new DriverManagerDataSource(base,user,password));database="ddip_silver_"+UUID.randomUUID().toString().replace("-","");admin.execute("CREATE DATABASE "+database);url=base.substring(0,base.lastIndexOf('/')+1)+database;
        jdbc=new JdbcTemplate(new DriverManagerDataSource(url,user,password));jdbc.setQueryTimeout(3);tx=transactions(jdbc);coordinator=new JdbcCoordinatorRepository(jdbc,tx,INSTALL);ownership=new ImportOwnershipRepository(jdbc,tx,INSTALL);bindings=new ImportBindings(jdbc,tx,INSTALL);
        server=HttpServer.create(new java.net.InetSocketAddress("127.0.0.1",0),8);http=Executors.newVirtualThreadPerTaskExecutor();server.setExecutor(http);
        server.createContext("/",exchange->{try{
            requests.add(exchange.getRequestMethod());assertThat(exchange.getRequestMethod()).isEqualTo("GET");assertThat(exchange.getRequestHeaders().getFirst("Authorization")).contains("Credential=synthetic-access/");
            byte[] data=objects.get(exchange.getRequestURI().getPath());int code=status.get();if(data==null && code==200)code=404;if(code!=200)data="<Error><Code>AccessDenied</Code></Error>".getBytes();exchange.sendResponseHeaders(code,data.length);exchange.getResponseBody().write(data);
        }finally{exchange.close();}});server.start();
    }
    ManagementTransactions transactions(JdbcTemplate queries){var template=new TransactionTemplate(new JdbcTransactionManager(queries.getDataSource()));template.setTimeout(3);return new ManagementTransactions(template);}
    @AfterAll void close(){if(server!=null)server.stop(0);if(http!=null)http.close();if(admin!=null && database!=null)admin.execute("DROP DATABASE "+database+" WITH (FORCE)");}
    void copy(Path from,Path to) throws Exception {try(var files=Files.walk(from)){for(Path f:files.toList()){Path target=to.resolve(from.relativize(f));if(Files.isDirectory(f))Files.createDirectories(target);else Files.copy(f,target,StandardCopyOption.COPY_ATTRIBUTES);}}}
    @BeforeEach void setup() throws Exception {
        jdbc.execute("DROP SCHEMA IF EXISTS ddip_management CASCADE");Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load().migrate();coordinator.initialize();
        objects.clear();requests.clear();status.set(200);for(var object:fixture.get("objects"))objects.put("/"+object.get("bucket").stringValue()+"/"+object.get("key").stringValue(),Files.readAllBytes(fixtures.resolve(object.get("file").stringValue())));
        work=Files.createTempDirectory(temp,"case-").toRealPath();silver=work.resolve("silver");copy(Path.of(fixture.get("root").stringValue()),silver);scratch=Files.createDirectory(work.resolve("scratch"));Files.setPosixFilePermissions(scratch,PosixFilePermissions.fromString("rwx------"));host=host(work);
        Path credentials=Files.write(work.resolve("credentials.json"),SourceJson.canonical(SourceJson.object().put("accessKey","synthetic-access").put("secretKey","synthetic-secret")));Files.setPosixFilePermissions(credentials,PosixFilePermissions.fromString("rw-------"));
        var config=SourceJson.object().put("importProtocolVersion",2).put("python",python.toString()).put("processingRoot",repo.resolve("processing").toRealPath().toString()).put("pythonVersion","3.12.15").put("duckdbVersion","1.4.5").put("timeoutSeconds",120);
        config.set("spark",SourceJson.object().put("javaHome",Path.of(System.getProperty("java.home")).toRealPath().toString()).put("javaVersion","21").put("pysparkVersion","4.0.2").put("scratchBindingId","silver-temp"));
        var hashes=config.putObject("modules");for(String name:ImportBindings.SILVER_MODULES)hashes.put(name,SourceJson.sha(Files.readAllBytes(repo.resolve("processing").resolve(name))));byte[] raw=SourceJson.canonical(config);
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES ('silver-import-v1',?,?)",raw,SourceJson.sha(raw));
        storage(host.id(),"CONTROL",host.descriptor());storage("silver-test","SILVER",local(silver));storage("silver-temp","TEMP",local(scratch));
        storage("bronze-test","BRONZE",SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:"+server.getAddress().getPort()).put("region","us-east-1").put("bucket","local").put("credentialsFile",credentials.toString())));
        runtime=bindings.runtime("silver-import-v1");catalog=catalog(UUID.randomUUID());
    }
    byte[] local(Path path)throws Exception{var id=InstallationHostGuard.identity(path);return SourceJson.canonical(SourceJson.object().put("importStorageVersion",1).put("root",path.toString()).put("rootDevice",Long.toString(id.device())).put("rootInode",Long.toString(id.inode())));}
    void storage(String id,String role,byte[] raw){jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,'content-intelligence',?,?,?,?)",id,role,raw,SourceJson.sha(raw),INSTALL);jdbc.update("INSERT INTO ddip_management.execution_profile_storage VALUES ('silver-import-v1',?)",id);}
    ImportCatalog catalog(UUID id){var n=SourceJson.object().put("catalogVersion",1).put("operationId",id.toString()).put("executionProfileId","silver-import-v1").put("controlBindingId",host.id());n.putArray("artifacts").add(fixture.get("artifacts").get(0));return ImportCatalog.parse(SourceJson.canonical(n));}
    OperatorImportService service(NativeImportProcess process){return new OperatorImportService(bindings,ownership,coordinator,new NativeImportVerifier(process),INSTALL);}
    JsonNode execute(ImportCatalog c){return service(new NativeImportProcess()).execute(c);}
    long count(String table){return jdbc.queryForObject("SELECT count(*) FROM ddip_management."+table,Long.class);}
    ImportOwnershipRepository.Operation op(){return ownership.lookup(catalog.operationId(),catalog.requestBytes()).orElseThrow();}
    ImportOwnershipRepository.Token reserve(InstallationHostGuard guard){return ownership.reserve(guard,new ImportOwnershipRepository.Reservation(catalog.operationId(),runtime.id(),runtime.digest(),catalog.requestBytes()));}
    List<QualifiedArtifact> qualify(){var verifier=new NativeImportVerifier(new NativeImportProcess());return catalog.entries().stream().map(e->verifier.verify(catalog.operationId(),e,bindings.storage(e,catalog.profileId()),runtime)).toList();}
    Path marker(){return silver.resolve(catalog.entries().getFirst().address());}
    @Test void successfulFreshReimportIsIdenticalAndReadonly() throws Exception {
        Path bookkeeping=Files.createDirectories(marker().getParent().resolve("accepted/_temporary/0"));Files.writeString(bookkeeping.resolve("retained.tmp"),"native-ignored-bookkeeping");
        Map<Path,String> before=new HashMap<>();try(var files=Files.walk(silver)){for(Path f:files.filter(Files::isRegularFile).toList())before.put(f,SourceJson.sha(Files.readAllBytes(f))+Files.getLastModifiedTime(f));}
        try(var files=Files.walk(fixtures.resolve("objects"))){for(Path f:files.filter(Files::isRegularFile).toList())before.put(f,SourceJson.sha(Files.readAllBytes(f))+Files.getLastModifiedTime(f));}
        var result=execute(catalog);assertThat(result.get("status").stringValue()).isEqualTo("VERIFIED");int gets=requests.size();assertThat(gets).isGreaterThanOrEqualTo(6);
        var again=execute(catalog(UUID.randomUUID()));assertThat(again.get("artifactIds")).isEqualTo(result.get("artifactIds"));assertThat(requests.size()).isGreaterThan(gets);
        assertThat(count("artifacts")).isEqualTo(1);assertThat(count("artifact_dependencies")).isZero();assertThat(count("audit_events")).isEqualTo(4);assertThat(count("runs")).isZero();assertThat(count("attempts")).isZero();assertThat(coordinator.read().activeImportId()).isNull();
        var evidence=ImportJson.parse(jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts",byte[].class),512*1024);assertThat(evidence.get("verification").get("inventoryIdentity").get("kind").stringValue()).isEqualTo("E5_SILVER_INVENTORY_V1");assertThat(evidence.get("selection").has("fingerprint")).isFalse();
        for(var f:before.entrySet())assertThat(SourceJson.sha(Files.readAllBytes(f.getKey()))+Files.getLastModifiedTime(f.getKey())).isEqualTo(f.getValue());assertThat(requests).allMatch("GET"::equals);
    }
    @Test void changedEvidenceConflictsAndHistoricalResultDoesNotRevalidate() throws Exception {execute(catalog);Files.write(marker()," ".getBytes(),StandardOpenOption.APPEND);var result=execute(catalog(UUID.randomUUID()));assertThat(result.get("code").stringValue()).isEqualTo("IDENTITY_CONFLICT");assertThat(execute(catalog).get("status").stringValue()).isEqualTo("HISTORICAL_RESULT");assertThat(count("artifacts")).isEqualTo(1);}
    @Test void corruptedRegisteredFileFailsFreshVerification() throws Exception {execute(catalog);try(var paths=Files.list(marker().getParent().resolve("accepted"))){var file=paths.filter(p->p.toString().endsWith(".parquet")).findFirst().orElseThrow();Files.write(file,"corrupt".getBytes());}assertThat(execute(catalog(UUID.randomUUID())).get("status").stringValue()).isEqualTo("FAILED");assertThat(count("artifacts")).isEqualTo(1);}
    @Test void changedBronzeDependencyInvalidatesReimport(){execute(catalog);String key="/local/"+catalog.entries().getFirst().identity().get("bronzePayloadKey").stringValue();objects.put(key,"changed".getBytes());assertThat(execute(catalog(UUID.randomUUID())).get("code").stringValue()).isEqualTo("NATIVE_INVALID");assertThat(count("artifacts")).isEqualTo(1);}
    @Test void auditFailureRollsBackArtifactResultAndRelease(){jdbc.execute("ALTER TABLE ddip_management.audit_events ADD CONSTRAINT injected CHECK(action<>'IMPORT_FINISHED')");assertThatThrownBy(()->execute(catalog)).hasMessage("STATE_CONFLICT").hasNoCause();assertThat(count("artifacts")).isZero();assertThat(count("audit_events")).isEqualTo(1);assertThat(op().state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(coordinator.read().activeImportId()).isEqualTo(catalog.operationId());}
    @Test void concurrentImportsHaveOneLogicalArtifact() throws Exception {var other=catalog(UUID.randomUUID());var gate=new CountDownLatch(1);try(var pool=Executors.newFixedThreadPool(2)){var tasks=new ArrayList<Future<Boolean>>();for(var c:List.of(catalog,other))tasks.add(pool.submit(()->{gate.await();try{return execute(c).get("status").stringValue().equals("VERIFIED");}catch(ImportOwnershipFailure failure){assertThat(failure.getMessage()).isIn("HOST_GUARD_OCCUPIED","OWNERSHIP_STALE");return false;}}));gate.countDown();int success=0;for(var task:tasks)if(task.get(150,TimeUnit.SECONDS))success++;assertThat(success).isPositive();}assertThat(count("artifacts")).isEqualTo(1);assertThat(coordinator.read().activeImportId()).isNull();}
    @Test void staleTokenCannotInsertActualQualifiedSilver(){try(var guard=InstallationHostGuard.acquire(host)){var token=reserve(guard);var qualified=qualify();var stale=new ImportOwnershipRepository.Token(token.importId(),token.processInstanceId(),token.epoch()-1,token.operationVersion(),token.coordinatorVersion());assertThatThrownBy(()->ownership.finishVerified(guard,stale,qualified)).hasMessage("OWNERSHIP_STALE");assertThat(count("artifacts")).isZero();}}
    @ParameterizedTest @ValueSource(ints={403,404,500}) void failedBronzeGetCannotMeanAbsence(int code){status.set(code);var result=execute(catalog);assertThat(result.get("code").stringValue()).isEqualTo("NATIVE_UNAVAILABLE");assertThat(op().state()).isEqualTo(ImportOwnershipRepository.State.FAILED);assertThat(count("artifacts")).isZero();assertThat(coordinator.read().activeImportId()).isNull();}
    @Test void uncertaintyDuringSparkRetainsOwnership(){var process=new NativeImportProcess(){@Override public Result runSpark(List<String> c,Path d,int t,byte[] request,Path java){throw new ImportFailure(ImportFailure.Code.TIMEOUT,false);}};assertThat(service(process).execute(catalog).get("status").stringValue()).isEqualTo("RECOVERY_REQUIRED");assertThat(op().state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(count("artifacts")).isZero();}
    @Test void databaseOutageAfterActualSparkBlocksFinalization(){var process=new NativeImportProcess(){@Override public Result run(List<String> c,Path d,int t,byte[] r){var result=super.run(c,d,t,r);admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS false");return result;}};
        try{assertThatThrownBy(()->service(process).execute(catalog)).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();}finally{admin.execute("ALTER DATABASE "+database+" ALLOW_CONNECTIONS true");}assertThat(op().state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(count("artifacts")).isZero();}
    ImportOwnershipRepository lostAck(AtomicBoolean armed) {
        var real=new DriverManagerDataSource(url,user,password);
        var ds=new org.springframework.jdbc.datasource.AbstractDataSource(){
            public Connection getConnection()throws SQLException{Connection c=real.getConnection();AtomicBoolean wrote=new AtomicBoolean();return(Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(proxy,m,args)->{try{
                if(m.getName().equals("prepareStatement") && args!=null && args[0] instanceof String sql && (sql.startsWith("INSERT") || sql.startsWith("UPDATE")))wrote.set(true);
                Object value=m.invoke(c,args);if(m.getName().equals("commit") && wrote.get() && armed.compareAndSet(true,false))throw new SQLException("PRIVATE lost response","08006");return value;
            }catch(InvocationTargetException e){throw e.getCause();}});}
            public Connection getConnection(String u,String p)throws SQLException{return getConnection();}
        };
        var queries=new JdbcTemplate(ds);return new ImportOwnershipRepository(queries,transactions(queries),INSTALL);
    }
    @Test void actualSilverCommitWithLostAcknowledgmentIsHistorical() {
        AtomicBoolean armed=new AtomicBoolean();var process=new NativeImportProcess(){@Override public Result run(List<String> c,Path d,int t,byte[] r){var result=super.run(c,d,t,r);armed.set(true);return result;}};
        var service=new OperatorImportService(bindings,lostAck(armed),coordinator,new NativeImportVerifier(process),INSTALL);
        assertThatThrownBy(()->service.execute(catalog)).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();assertThat(armed).isFalse();assertThat(op().state()).isEqualTo(ImportOwnershipRepository.State.SUCCEEDED);assertThat(count("artifacts")).isEqualTo(1);assertThat(count("audit_events")).isEqualTo(2);assertThat(coordinator.read().activeImportId()).isNull();
        assertThat(execute(catalog).get("status").stringValue()).isEqualTo("HISTORICAL_RESULT");
    }
    @Test void changedBronzeAfterSparkIsDetectedByFinalRecheck() {
        var process=new NativeImportProcess(){@Override public Result runSpark(List<String> c,Path d,int t,byte[] r,Path java){var result=super.runSpark(c,d,t,r,java);objects.put("/local/"+catalog.entries().getFirst().identity().get("bronzePayloadKey").stringValue(),"changed".getBytes());return result;}};
        var result=service(process).execute(catalog);assertThat(result.get("code").stringValue()).isEqualTo("NATIVE_CHANGED");assertThat(result.get("nativeCode").stringValue()).isEqualTo("BRONZE_CHANGED");assertThat(count("artifacts")).isZero();assertThat(coordinator.read().activeImportId()).isNull();
    }
    @ParameterizedTest @ValueSource(strings={"inventory","shutdown"})
    void ambiguousSilverProtocolKeepsOwnership(String variant) {
        var process=new NativeImportProcess(){@Override public Result runSpark(List<String> c,Path d,int t,byte[] r,Path java){var result=super.runSpark(c,d,t,r,java);var n=ImportJson.parse(result.stdout(),512*1024);if(variant.equals("inventory"))((tools.jackson.databind.node.ObjectNode)n.get("evidence").get("inventoryIdentity")).put("sha256","0".repeat(64));else((tools.jackson.databind.node.ObjectNode)n.get("evidence").get("native")).put("sparkShutdown","UNKNOWN");return new Result(0,SourceJson.canonical(n));}};
        var result=service(process).execute(catalog);assertThat(result.get("status").stringValue()).isEqualTo("RECOVERY_REQUIRED");assertThat(op().state()).isEqualTo(ImportOwnershipRepository.State.ACTIVE);assertThat(count("artifacts")).isZero();
    }
    @ParameterizedTest @ValueSource(strings={"reserved","qualified"})
    void crashedSilverImporterRequiresExplicitFencedRecovery(String phase)throws Exception {
        Path file=Files.write(work.resolve("catalog.json"),catalog.requestBytes());
        var b=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),ImportWorkflowChild.class.getName(),file.toString(),phase).redirectError(ProcessBuilder.Redirect.DISCARD);b.environment().put("DDIP_CHILD_DB_URL",url);Process child=b.start();
        try{var ready=CompletableFuture.supplyAsync(()->{try{return new BufferedReader(new InputStreamReader(child.getInputStream())).readLine();}catch(IOException e){throw new UncheckedIOException(e);}});assertThat(ready.get(150,TimeUnit.SECONDS)).isEqualTo("CHECKPOINT");child.destroyForcibly();assertThat(child.waitFor(5,TimeUnit.SECONDS)).isTrue();
            assertThat(execute(catalog).get("status").stringValue()).isEqualTo("RECOVERY_REQUIRED");var service=service(new NativeImportProcess());UUID recovery=UUID.randomUUID();
            assertThatThrownBy(()->service.recover(catalog,catalog.operationId(),recovery,false)).hasMessage("QUIESCENCE_REQUIRED");assertThat(service.recover(catalog,catalog.operationId(),recovery,true).get("status").stringValue()).isEqualTo("RECOVERY_CLOSED");
            assertThat(op().state()).isEqualTo(ImportOwnershipRepository.State.INTERRUPTED);assertThat(coordinator.read().activeImportId()).isNull();assertThat(count("artifacts")).isZero();
            assertThat(service.recover(catalog,catalog.operationId(),recovery,true).get("status").stringValue()).isEqualTo("HISTORICAL_RESULT");
        }finally{child.destroyForcibly();child.waitFor(5,TimeUnit.SECONDS);}
    }
    @Test void silverRegistrationSurvivesFreshConnectionAndOptionalRestart()throws Exception {
        execute(catalog);var rows=jdbc.queryForList("SELECT artifact_id,native_identity,evidence_sha256,verified_at FROM ddip_management.artifacts");var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);String signal=System.getProperty("ddip.test.silver-restart-signal");
        if(signal!=null){Files.writeString(Path.of(signal),"READY");long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<deadline)Thread.sleep(100);assertThat(Files.exists(Path.of(signal+".done"))).isTrue();}
        var fresh=new JdbcTemplate(new DriverManagerDataSource(url,user,password));if(signal!=null)assertThat(fresh.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class)).isAfter(started);
        assertThat(fresh.queryForList("SELECT artifact_id,native_identity,evidence_sha256,verified_at FROM ddip_management.artifacts")).isEqualTo(rows);assertThat(new ImportOwnershipRepository(fresh,transactions(fresh),INSTALL).lookup(catalog.operationId(),catalog.requestBytes()).orElseThrow().state()).isEqualTo(ImportOwnershipRepository.State.SUCCEEDED);
    }

    @Test void unapprovedBronzeBindingFailsBeforeSpark(){var n=ImportJson.parse(catalog.requestBytes(),65536);((tools.jackson.databind.node.ObjectNode)n.get("artifacts").get(0).get("identity")).put("bronzeStorageBindingId","unapproved");var c=ImportCatalog.parse(SourceJson.canonical(n));assertThat(execute(c).get("code").stringValue()).isEqualTo("BINDING_INVALID");assertThat(count("artifacts")).isZero();assertThat(coordinator.read().activeImportId()).isNull();}
    @Test void missingScratchBindingBlocksBeforeReservation(){var config=SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id='silver-import-v1'",byte[].class));((tools.jackson.databind.node.ObjectNode)config.get("spark")).put("scratchBindingId","unregistered-scratch");byte[] raw=SourceJson.canonical(config);jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES ('missing-scratch',?,?)",raw,SourceJson.sha(raw));var request=SourceJson.parse(catalog.requestBytes());((tools.jackson.databind.node.ObjectNode)request).put("executionProfileId","missing-scratch");var c=ImportCatalog.parse(SourceJson.canonical(request));assertThatThrownBy(()->execute(c)).hasMessage("BINDING_INVALID");assertThat(count("artifact_import_operations")).isZero();}
    @Test void privateCredentialSymlinkIsRejected()throws Exception {Path file=work.resolve("credentials.json"),moved=work.resolve("other-credentials");Files.move(file,moved);Files.createSymbolicLink(file,moved);assertThat(execute(catalog).get("code").stringValue()).isEqualTo("BINDING_INVALID");assertThat(count("artifacts")).isZero();}
    @Test void packagedRoleImportsSilverWithoutHttpOrProcessingRun()throws Exception {
        Path file=Files.write(work.resolve("catalog.json"),catalog.requestBytes());Files.setPosixFilePermissions(file,PosixFilePermissions.fromString("rw-------"));
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-jar",repo.resolve("services/content-api/target/content-api-0.1.0-SNAPSHOT.jar").toString(),"--spring.main.banner-mode=off","--logging.level.root=OFF");builder.environment().clear();builder.environment().putAll(Map.of("DDIP_ROLE","operator-import","DDIP_DB_URL",url,"DDIP_DB_USER",user,"DDIP_DB_PASSWORD",password,"DDIP_INSTALLATION_ID",INSTALL.toString(),"DDIP_IMPORT_CATALOG",file.toString()));
        Process p=builder.redirectError(ProcessBuilder.Redirect.DISCARD).start();try{var output=CompletableFuture.supplyAsync(()->{try{return p.getInputStream().readNBytes(65537);}catch(IOException e){throw new UncheckedIOException(e);}});assertThat(p.waitFor(150,TimeUnit.SECONDS)).isTrue();assertThat(p.exitValue()).isZero();assertThat(ImportJson.parse(output.get(2,TimeUnit.SECONDS),65536).get("status").stringValue()).isEqualTo("VERIFIED");}finally{p.destroyForcibly();p.waitFor(5,TimeUnit.SECONDS);}
        assertThat(count("artifacts")).isEqualTo(1);assertThat(count("runs")).isZero();assertThat(count("attempts")).isZero();
    }

}
