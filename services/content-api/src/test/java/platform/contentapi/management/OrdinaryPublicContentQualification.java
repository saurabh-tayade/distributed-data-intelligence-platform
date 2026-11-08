package platform.contentapi.management;

import com.sun.net.httpserver.HttpServer;
import java.net.*;
import java.net.http.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/**
 * Explicit public-network qualification only. Deliberately matches neither normal Surefire nor
 * Failsafe naming. Invoke scripts/qualify-ordinary-public-content.sh with reviewed immutable inputs.
 * No assume/skip, source prefetch, application retry, synthetic public resolver, or test capability.
 */
class OrdinaryPublicContentQualification extends OrdinaryContentWorkerFixtures {
    String fixtureUri,fixtureSha,expectedTitle;
    Path evidence,packagedJar,objectRoot;
    HttpServer objectServer;ExecutorService objectThreads;
    Process apiProcess,workerProcess;
    final List<Process> processes=new ArrayList<>();
    final AtomicInteger objectWrites=new AtomicInteger();
    final Map<String,String> objectTypes=new ConcurrentHashMap<>();
    final List<UUID> submitted=new ArrayList<>();
    boolean passed;
    volatile CountDownLatch objectGate=new CountDownLatch(0),objectEntered=new CountDownLatch(0);
    volatile String liveStop="NOT_RUN";
    HttpClient client;

    static String required(String name){String value=System.getenv(name);if(value==null || value.isBlank())throw new IllegalArgumentException("Required qualification input: "+name);return value;}
    static void digestInput(String name){assertThat(required(name)).as(name).matches("[0-9a-f]{64}");}
    @Override String sourceUri(){return fixtureUri;}
    @Override String bronzeEndpoint(){return "http://127.0.0.1:"+objectServer.getAddress().getPort();}
    @Override void createSource(String product,String source,JsonNode c){
        // The real packaged API creates the selected public source, not a reflective fixture row.
        if(!product.equals(CONTENT))super.createSource(product,source,c);
    }
    public static void main(String[] args)throws Exception {
        if(args.length!=0)throw new IllegalArgumentException("Configure exact qualification inputs through the documented environment");
        var qualification=new OrdinaryPublicContentQualification();
        try {
            qualification.database();qualification.setup();qualification.registerComposition();
            qualification.packagedPublicApiWorkerCompletesOriginalAndUsesAcknowledgedSessionForNextClaim();
        } finally {qualification.cleanup();}
    }
    @Override void database()throws Exception {
        fixtureUri=required("DDIP_PUBLIC_FIXTURE_URI");fixtureSha=required("DDIP_PUBLIC_FIXTURE_SHA256");expectedTitle=required("DDIP_PUBLIC_FIXTURE_TITLE");
        URI uri=URI.create(fixtureUri);
        assertThat(uri.getScheme()).isEqualTo("https");assertThat(uri.getHost()).isEqualTo("raw.githubusercontent.com");
        assertThat(uri.getRawPath()).matches("/[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+/[0-9a-f]{40}/[A-Za-z0-9_./-]+");
        assertThat(uri.getRawPath()).doesNotContain("/../","/./");assertThat(uri.getPort()).isEqualTo(-1);
        assertThat(uri.getUserInfo()).isNull();assertThat(uri.getQuery()).isNull();assertThat(uri.getFragment()).isNull();
        digestInput("DDIP_PUBLIC_FIXTURE_SHA256");digestInput("DDIP_PUBLIC_INGESTION_SHA256");digestInput("DDIP_PUBLIC_PROVIDER_SHA256");
        assertThat(expectedTitle.length()).isBetween(1,256);
        evidence=Path.of(required("DDIP_PUBLIC_QUALIFICATION_OUTPUT")).toAbsolutePath().normalize();Files.createDirectories(evidence);
        // Not JUnit's temporary directory: failed and successful evidence is retained for review.
        temp=Files.createTempDirectory(evidence,"installation-").toRealPath();
        packagedJar=Path.of("target/content-api-0.1.0-SNAPSHOT.jar").toRealPath();
        assertThat(SourceJson.sha(Files.readAllBytes(Path.of("../../ingestion/target/ingestion-0.1.0-SNAPSHOT.jar")))).isEqualTo(required("DDIP_PUBLIC_INGESTION_SHA256"));
        assertThat(SourceJson.sha(Files.readAllBytes(Path.of("../../ingestion/target/ingestion-0.1.0-SNAPSHOT-public-fetch-provider.jar")))).isEqualTo(required("DDIP_PUBLIC_PROVIDER_SHA256"));
        objectRoot=Files.createDirectory(temp.resolve("bronze-objects"));
        objectServer=HttpServer.create(new InetSocketAddress("127.0.0.1",0),0);
        objectServer.createContext("/",exchange->{try {
            String key=exchange.getRequestURI().getPath();Path path=objectRoot.resolve(key.substring(1)).normalize();
            if(!path.startsWith(objectRoot) || !key.startsWith("/bronze/"))throw new IllegalArgumentException("Object path");
            String method=exchange.getRequestMethod();objectEntered.countDown();
            if(!objectGate.await(90,TimeUnit.SECONDS))throw new IllegalStateException("Held object response deadline expired");
            if(method.equals("PUT")){
                byte[] bytes=exchange.getRequestBody().readNBytes(5*1024*1024+1);
                if(bytes.length>5*1024*1024){exchange.sendResponseHeaders(413,-1);return;}
                Files.createDirectories(path.getParent());Files.write(path,bytes);objectTypes.put(key,exchange.getRequestHeaders().getFirst("Content-Type"));objectWrites.incrementAndGet();exchange.sendResponseHeaders(200,-1);
            }else if(Set.of("GET","HEAD").contains(method) && Files.isRegularFile(path)){
                byte[] bytes=Files.readAllBytes(path);exchange.getResponseHeaders().set("Content-Type",objectTypes.get(key));exchange.getResponseHeaders().set("Content-Length",Integer.toString(bytes.length));
                exchange.sendResponseHeaders(200,method.equals("HEAD")?-1:bytes.length);if(method.equals("GET"))exchange.getResponseBody().write(bytes);
            }else{
                byte[] bytes="<Error><Code>NoSuchKey</Code></Error>".getBytes(java.nio.charset.StandardCharsets.UTF_8);
                exchange.getResponseHeaders().set("Content-Type","application/xml");exchange.sendResponseHeaders(404,method.equals("HEAD")?-1:bytes.length);if(!method.equals("HEAD"))exchange.getResponseBody().write(bytes);
            }
        }catch(java.io.IOException ignored){}catch(InterruptedException ex){Thread.currentThread().interrupt();}finally{exchange.close();}});
        objectThreads=Executors.newVirtualThreadPerTaskExecutor();objectServer.setExecutor(objectThreads);objectServer.start();
        client=HttpClient.newBuilder().connectTimeout(Duration.ofSeconds(3)).followRedirects(HttpClient.Redirect.NEVER)
                .proxy(new ProxySelector(){public List<Proxy> select(URI u){return List.of(Proxy.NO_PROXY);}public void connectFailed(URI u,SocketAddress a,java.io.IOException e){}}).build();
        super.database();
        writeReport("STARTED");
    }
    Process packaged(String role,String publication)throws Exception {
        var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-jar",packagedJar.toString(),"--ddip.role="+role,
                "--logging.level.root=WARN","--spring.main.banner-mode=off"));
        if(role.equals("worker")){
            command.add("--spring.main.web-application-type=none");
            for(var entry:Map.of("execution-profile-id",CONTENT_PROFILE,"control-binding-id",binding.id(),"deployment-sha256",config(CONTENT_PROFILE).get("runtime").get("deploymentSha256").stringValue(),
                    "runtime-profile-id",PIPELINE_PROFILE,"poll-millis","250","python",python.toString(),"java-home",System.getProperty("java.home"),"processing-root",processing.toString()).entrySet())
                command.add("--ddip.worker."+entry.getKey()+"="+entry.getValue());
        }else{
            command.addAll(List.of("--server.address=127.0.0.1","--server.port="+port,"--ddip.management.catalog="+catalogFile,"--ddip.management.cursor-key="+KEY,"--ddip.management.origins="+origin,
                    "--content.analytics.python-executable="+python,"--content.analytics.processing-root="+processing,"--content.analytics.root="+roots.get("content-CONTENT"),
                    "--content.analytics.gold-root="+roots.get("content-GOLD"),"--content.analytics.reference-root="+roots.get(REFS)));
            if(publication!=null)command.add("--content.analytics.publication="+publication);
        }
        var builder=new ProcessBuilder(command).redirectOutput(ProcessBuilder.Redirect.appendTo(evidence.resolve(role+".log").toFile())).redirectErrorStream(true);
        builder.environment().put("DDIP_DB_URL",url);builder.environment().put("DDIP_DB_USER",user);builder.environment().put("DDIP_DB_PASSWORD",password);builder.environment().put("DDIP_INSTALLATION_ID",INSTALL.toString());
        for(String key:List.of("JAVA_TOOL_OPTIONS","JDK_JAVA_OPTIONS","_JAVA_OPTIONS","CLASSPATH"))builder.environment().remove(key);
        Process result=builder.start();processes.add(result);return result;
    }
    HttpResponse<byte[]> request(String method,String path,JsonNode body,String key,String etag)throws Exception {
        var builder=HttpRequest.newBuilder(URI.create(origin+path)).timeout(Duration.ofSeconds(60));
        if(body==null)builder.GET();else{
            builder.header("Origin",origin).header("X-DDIP-Local-Request","1").header("Content-Type","application/json").header("Idempotency-Key",key);
            if(etag!=null)builder.header("If-Match",etag);builder.method(method,HttpRequest.BodyPublishers.ofByteArray(SourceJson.canonical(body)));
        }
        return client.send(builder.build(),HttpResponse.BodyHandlers.ofByteArray());
    }
    JsonNode getData(String path)throws Exception {var result=request("GET",path,null,null,null);assertThat(result.statusCode()).isEqualTo(200);return SourceJson.parse(result.body()).get("data");}
    void awaitApi()throws Exception {
        long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(30);boolean ready=false;
        while(System.nanoTime()<until && apiProcess.isAlive())try{if(request("GET","/actuator/health",null,null,null).statusCode()==200){ready=true;break;}}catch(java.io.IOException ex){Thread.sleep(100);}
        assertThat(ready).as("Packaged API must become ready; see retained api.log").isTrue();
    }
    UUID submitApi(String key)throws Exception {
        var request=content();request.withArray("retainedSilverArtifactIds").removeAll();var response=request("POST",RUNS,request,key,null);
        assertThat(response.statusCode()).isEqualTo(202);UUID id=UUID.fromString(SourceJson.parse(response.body()).get("data").get("runId").stringValue());submitted.add(id);return id;
    }
    void awaitSuccess(UUID run)throws Exception {
        long until=System.nanoTime()+TimeUnit.MINUTES.toNanos(12);
        while(System.nanoTime()<until){
            String state=state(run);if(state.equals("SUCCEEDED"))return;
            assertThat(state).as("A failed/uncertain public attempt is retained, never retried or skipped").isIn("QUEUED","RUNNING");
            assertThat(workerProcess.isAlive()).as("Packaged worker exited; see retained worker.log").isTrue();
            // A stopped dispatcher can leave the Spring process alive; retain and report that failure.
            if(Files.exists(evidence.resolve("worker.log")))assertThat(Files.readString(evidence.resolve("worker.log"))).doesNotContain("WORKER_OWNERSHIP_UNCERTAIN","WORKER_STOP_UNCERTAIN");
            Thread.sleep(250);
        }
        fail("Public qualification deadline exceeded; installation and attempt evidence retained");
    }
    void packagedPublicApiWorkerCompletesOriginalAndUsesAcknowledgedSessionForNextClaim()throws Exception {
        apiProcess=packaged("api",null);awaitApi();
        var created=request("POST","/api/management/v1/sources",create(CONTENT,"demo-feed",rss()),"qualification-source",null);assertThat(created.statusCode()).isEqualTo(201);
        UUID first=submitApi("public-first"),second=submitApi("public-next");
        var frozen=new LinkedHashMap<UUID,byte[]>();for(UUID run:submitted)frozen.put(run,jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run));
        var revised=request("POST","/api/management/v1/sources/"+CONTENT+"/demo-feed/revisions",revise("Changed after submission",rss().put("feedEndpointId",ENDPOINT2)),"qualification-revision","\"0\"");
        assertThat(revised.statusCode()).isEqualTo(200);
        workerProcess=packaged("worker",null);awaitSuccess(first);awaitSuccess(second);
        var attempts=jdbc.queryForList("SELECT a.* FROM ddip_management.attempts a JOIN ddip_management.runs r USING(run_id) ORDER BY r.submission_seq");
        assertThat(attempts).hasSize(2);assertThat(attempts.get(0).get("instance_id")).isEqualTo(attempts.get(1).get("instance_id"));
        assertThat(((Number)attempts.get(1).get("owner_epoch")).longValue()).isEqualTo(((Number)attempts.get(0).get("owner_epoch")).longValue()+1);
        assertThat(count("worker_instances")).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        String publication=null;
        for(UUID run:submitted){
            assertThat(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run)).isEqualTo(frozen.get(run));
            var selection=SourceJson.parse(frozen.get(run));assertThat(selection.get("sources").get(0).get("revision").intValue()).isEqualTo(1);
            assertThat(selection.get("sources").get(0).get("endpoint").get("uri").stringValue()).isEqualTo(fixtureUri);
            UUID attempt=jdbc.queryForObject("SELECT current_attempt_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run);
            UUID artifact=jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run);
            NarrowContentPipelineSqlIT.assertCompletedGraphAndCounts(jdbc,run,attempt,artifact);
            var detail=getData(RUNS+"/"+run);assertThat(detail.get("counts").get("articles").stringValue()).isEqualTo("1");
            assertThat(detail.get("result").get("artifactId").stringValue()).isEqualTo(artifact.toString());assertThat(detail.get("result").get("verificationStatus").stringValue()).isEqualTo("VERIFIED_AT_EXECUTION");
            var history=getData(RUNS+"/"+run+"/attempts/"+attempt+"/native-operations?limit=100");assertThat(history).hasSize(17);
            var steps=new ArrayList<Integer>();for(var step:history){steps.add(step.get("stepIndex").intValue());assertThat(step.get("checkpointedAt").isNull()).isFalse();}assertThat(steps).containsExactly(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16);
            UUID checkpoint=jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_native_operations WHERE attempt_id=?",UUID.class,attempt);
            Path payload=binding.root().resolve("runs/"+run+"/attempts/"+attempt+"/fetch-source/demo-feed/"+checkpoint+"/payload.xml");assertThat(SourceJson.sha(Files.readAllBytes(payload))).isEqualTo(fixtureSha);
            var evidence=SourceJson.parse(jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?",byte[].class,artifact));
            assertThat(evidence.get("verification").get("status").stringValue()).isEqualTo("VERIFIED");publication=evidence.get("nativeFingerprint").stringValue();
        }
        assertThat(count("stage_invocations")).isZero();assertThat(count("stage_artifacts")).isZero();assertThat(count("rss_deliveries")).isZero();assertThat(objectWrites).hasValue(4);
        // An idempotent same-key API replay returns the original result without another fetch,
        // attempt, registry insertion, or rewrite of retained native publication bytes/timestamps.
        UUID firstArtifact=jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,first);
        var firstEvidence=SourceJson.parse(jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?",byte[].class,firstArtifact));
        Path marker=roots.get("content-CONTENT").resolve(firstEvidence.get("relativeAddress").stringValue()).resolve("complete.json");
        byte[] markerBytes=Files.readAllBytes(marker);var markerTime=Files.getLastModifiedTime(marker);
        var tableCounts=new TreeMap<String,Long>();for(String table:List.of("runs","attempts","artifacts","content_native_operations","content_bronze_reservations","content_pipeline_operations","content_pipeline_completions"))tableCounts.put(table,count(table));
        var replayRequest=content();replayRequest.withArray("retainedSilverArtifactIds").removeAll();
        var replay=request("POST",RUNS,replayRequest,"public-first",null);assertThat(replay.statusCode()).isEqualTo(202);
        var replayed=SourceJson.parse(replay.body()).get("data");assertThat(replayed.get("runId").stringValue()).isEqualTo(first.toString());
        assertThat(replayed.get("result").get("artifactId").stringValue()).isEqualTo(firstArtifact.toString());
        for(var entry:tableCounts.entrySet())assertThat(count(entry.getKey())).as(entry.getKey()).isEqualTo(entry.getValue());
        assertThat(objectWrites).hasValue(4);assertThat(Files.readAllBytes(marker)).isEqualTo(markerBytes);assertThat(Files.getLastModifiedTime(marker)).isEqualTo(markerTime);
        stopProcess(workerProcess);stopProcess(apiProcess);apiProcess=packaged("api",publication);awaitApi();
        var analytics=request("GET","/api/v1/content/catalog?limit=10",null,null,null);assertThat(analytics.statusCode()).isEqualTo(200);
        var result=SourceJson.parse(analytics.body());assertThat(result.get("publication").stringValue()).isEqualTo(publication);assertThat(result.get("data")).hasSize(1);
        assertThat(result.get("data").get(0).get("title").stringValue()).isEqualTo(expectedTitle);Files.write(evidence.resolve("content-read-api.json"),analytics.body());
        liveChildStopRetainsOccupancy();
        passed=true; // Final PASSED report is emitted only after bounded cleanup succeeds.
    }
    void liveChildStopRetainsOccupancy()throws Exception {
        var stoppedRequest=content();stoppedRequest.withArray("retainedSilverArtifactIds").removeAll();
        ((ObjectNode)stoppedRequest.get("fetchSources").get(0)).put("revision",2);
        var stoppedResponse=request("POST",RUNS,stoppedRequest,"public-live-stop",null);assertThat(stoppedResponse.statusCode()).isEqualTo(202);
        UUID stopped=UUID.fromString(SourceJson.parse(stoppedResponse.body()).get("data").get("runId").stringValue());submitted.add(stopped);
        var waitingResponse=request("POST",RUNS,stoppedRequest,"public-after-stop",null);assertThat(waitingResponse.statusCode()).isEqualTo(202);
        UUID waiting=UUID.fromString(SourceJson.parse(waitingResponse.body()).get("data").get("runId").stringValue());submitted.add(waiting);
        objectGate=new CountDownLatch(1);objectEntered=new CountDownLatch(1);
        workerProcess=packaged("worker",null);
        try {
            assertThat(objectEntered.await(75,TimeUnit.SECONDS)).as("Actual third public FETCH must reach the live Bronze child before stop").isTrue();
            UUID attempt=jdbc.queryForObject("SELECT current_attempt_id FROM ddip_management.runs WHERE run_id=?",UUID.class,stopped);
            var child=jdbc.queryForMap("SELECT p.child_pid,p.child_started_at FROM ddip_management.content_bronze_launch_phases p JOIN ddip_management.content_bronze_reservations r USING(operation_id) WHERE r.attempt_id=? AND p.phase='CHILD_RECORDED'",attempt);
            long pid=((Number)child.get("child_pid")).longValue();ProcessHandle process=ProcessHandle.of(pid).orElseThrow();
            assertThat(process.isAlive()).isTrue();assertThat(process.info().startInstant().orElseThrow().truncatedTo(java.time.temporal.ChronoUnit.MICROS))
                    .isEqualTo(((java.sql.Timestamp)child.get("child_started_at")).toInstant());
            stopProcess(workerProcess);await(()->!process.isAlive(),10);
            assertThat(state(stopped)).isEqualTo("RUNNING");assertThat(state(waiting)).isEqualTo("QUEUED");
            assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,attempt)).isEqualTo("CLAIMED");
            assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(attempt);
            assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,stopped)).isNull();
            assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.content_pipeline_completions WHERE attempt_id=?",Long.class,attempt)).isZero();
            assertThat(count("attempts")).isEqualTo(3);assertThat(count("content_pipeline_completions")).isEqualTo(2);
            assertThat(count("stage_invocations")).isZero();assertThat(count("rss_deliveries")).isZero();
            objectGate.countDown();
            // Inspect the real retained prefix only. Local child death is not a substitute for the
            // separate operator review of remote storage effects required by ADR 0044.
            var before=jdbc.queryForList("SELECT * FROM ddip_management.coordinator");
            try(var guard=InstallationHostGuard.acquire(binding)){
                var assessment=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,stopped,attempt);
                assertThat(assessment.prefix()).isEqualTo("BRONZE_INCOMPLETE");assertThat(assessment.authorization()).isFalse();
                assertThat(assessment.quiescenceEstablished()).isFalse();assertThat(assessment.nativeLaunchCompatible()).isFalse();
                assertThat(assessment.checkpointSteps()).containsExactly(0);
            }
            assertThat(jdbc.queryForList("SELECT * FROM ddip_management.coordinator")).isEqualTo(before);
            liveStop="PACKAGED_LIVE_BRONZE_CHILD_STOPPED_OCCUPANCY_RETAINED";
        } finally {objectGate.countDown();}
    }
    void stopProcess(Process process)throws InterruptedException {
        if(process==null || !process.isAlive())return;process.destroy();
        if(!process.waitFor(60,TimeUnit.SECONDS)){process.destroyForcibly();assertThat(process.waitFor(10,TimeUnit.SECONDS)).isTrue();fail("Packaged process required forced termination; evidence retained");}
    }
    void writeReport(String status)throws Exception {
        var report=SourceJson.object().put("status",status).put("scope","OPT_IN_PACKAGED_PUBLIC_ORIGINAL_CHAIN")
                .put("fixtureUri",fixtureUri).put("fixtureSha256",fixtureSha).put("packagedApiSha256",SourceJson.sha(Files.readAllBytes(packagedJar)))
                .put("ingestionSha256",required("DDIP_PUBLIC_INGESTION_SHA256")).put("providerSha256",required("DDIP_PUBLIC_PROVIDER_SHA256"))
                .put("database",database).put("installationDirectory",temp.toString()).put("automaticRetry",false).put("evidenceRetained",true)
                .put("liveStop",liveStop).put("safeEndScope","DETACHED_ASSESSMENT_ONLY_OPERATOR_REVIEW_REQUIRED");
        var runs=report.putArray("runs");for(UUID run:submitted)runs.addObject().put("runId",run.toString()).put("state",state(run));
        Files.write(evidence.resolve("qualification.json"),SourceJson.canonical(report));
    }
    @Override void cleanup(){
        Throwable failure=null;
        // Attempt every bounded process shutdown even if another shutdown failed. A cleanup failure
        // can never leave a PASSED report behind or be reclassified as an availability skip.
        for(Process process:processes)try{stopProcess(process);}catch(Throwable ex){passed=false;if(failure==null)failure=ex;else failure.addSuppressed(ex);}
        objectGate.countDown();
        try{if(client!=null)client.close();}catch(Throwable ex){passed=false;if(failure==null)failure=ex;else failure.addSuppressed(ex);}
        if(objectServer!=null)objectServer.stop(0);if(objectThreads!=null)objectThreads.shutdownNow();
        try{if(evidence!=null && database!=null)writeReport(passed?"PASSED":"FAILED_EVIDENCE_RETAINED");}
        catch(Exception ex){if(failure==null)failure=ex;else failure.addSuppressed(ex);}
        if(failure!=null)throw new IllegalStateException("Qualification cleanup or report failed; retain installation",failure);
        // Deliberately do not drop this disposable database or delete native evidence on failure.
    }
}
