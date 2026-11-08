package platform.contentapi.management;

import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Packaged operator, real FETCH evidence and local process observation. Old ownership metadata is
 * an isolated fixture containing a captured disposable process identity, not a packaged-worker crash. */
class NarrowContentTerminalOperatorIT extends NarrowContentFetchLaunchFixtures {
    final List<Process> processes=new ArrayList<>();
    UUID control;
    Process originalProcess;
    @AfterEach void stopProcesses() throws Exception {
        for(var p:processes){p.destroyForcibly();assertThat(p.waitFor(10,TimeUnit.SECONDS)).isTrue();}
        processes.clear();
    }
    Process disposable() throws Exception {
        var p=new ProcessBuilder("sleep","120").start();processes.add(p);return p;
    }
    @Override void ready() {
        guard=InstallationHostGuard.acquire(binding);
        try{originalProcess=disposable();}catch(java.io.IOException ex){throw new java.io.UncheckedIOException(ex);}catch(Exception ex){throw new IllegalStateException(ex);}
        var started=Timestamp.from(originalProcess.info().startInstant().orElseThrow());
        // Fixture-only identity injection at the ORIGINAL INSERT, before any immutable history exists.
        // The guard/capability remain test fixtures; recovery always uses the production LOCAL observer.
        var insertions=new java.util.concurrent.atomic.AtomicInteger();
        var registration=new org.springframework.jdbc.core.JdbcTemplate(jdbc.getDataSource()) {
            @Override public int update(String sql,Object... args) {
                if(sql.startsWith("INSERT INTO ddip_management.worker_instances(")){
                    insertions.incrementAndGet();return super.update(sql,NativeFetchOperatorFixtureIdentity.atInsert(sql,args,originalProcess.pid(),started));
                }
                return super.update(sql,args);
            }
        };
        // Admission/launch/completion require the exact same JdbcTemplate, not merely its DataSource.
        // Publish the wrapper to every inherited fixture helper before constructing the worker.
        jdbc=registration;
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE));
        assertThat(insertions).hasValue(1);
    }
    void admitted() throws Exception {
        prepared();originalProcess.destroy();assertThat(originalProcess.waitFor(10,TimeUnit.SECONDS)).isTrue();
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,owner.runId());
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);
        control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(ControlCommands.parse(owner.runId().toString(),SourceJson.canonical(request),"content-operator","\""+version+"\"")).get("controlId").stringValue());guard.close();guard=null;
    }
    Child start(String action,JsonNode plan) throws Exception {
        Path jar=Path.of(NarrowContentTerminalTestSupport.class.getProtectionDomain().getCodeSource().getLocation().toURI()).getParent().resolve("content-api-0.1.0-SNAPSHOT.jar");
        var args=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx256m","-jar",jar.toString(),"--ddip.role=operator-recovery","--ddip.recovery.action="+action));
        if(plan==null)args.addAll(List.of("--ddip.recovery.control-id="+control,"--ddip.worker.execution-profile-id="+CONTENT_PROFILE,"--ddip.worker.control-binding-id="+binding.id(),"--ddip.worker.deployment-sha256="+owner.worker().deploymentDigest(),"--ddip.recovery.same-host-process-namespace=true"));
        else args.add("--ddip.recovery.terminal-id="+plan.get("terminalId").stringValue());
        var builder=new ProcessBuilder(args).redirectErrorStream(true);builder.environment().putAll(Map.of("DDIP_DB_URL",url,"DDIP_DB_USER",user,"DDIP_DB_PASSWORD",password,"DDIP_INSTALLATION_ID",INSTALL.toString()));var p=builder.start();processes.add(p);return new Child(p);
    }
    ObjectNode answer(JsonNode prompt){
        var r=SourceJson.object().put("approval","AUTHORIZE_NATIVE_CONTENT_TERMINAL").put("reviewMethod","LOCAL_PROCESS_TREE_AND_ALL_WRITERS_REVIEWED");
        for(String key:List.of("challenge","inspectionSha256","targetSha256","reviewedCheckpoints","recordedProcesses"))r.set(key,prompt.get(key));
        for(String key:List.of("handlersStopped","unmanagedWritersStopped","orphanReviewComplete","sameHostAndProcessNamespace","descendantsReviewed","launchGapsReviewed","sparkGatewaysReviewed","remoteWritersStopped"))r.put(key,true);r.putArray("descendants");return r;
    }
    @Test void packagedSingleReviewEndsAndNewProcessInspectsWithoutGuard()throws Exception{
        admitted();var files=filesystem();var child=start("native-content-end",null);var plan=child.await("NATIVE_CONTENT_PLAN");var prompt=child.await("CONFIRMATION_REQUIRED");child.send(answer(prompt));var done=child.await("COMPLETED");child.exited(0);
        assertThat(done.get("code").stringValue()).isEqualTo("NATIVE_CONTENT_INTERRUPTED");assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(0);assertThat(count("content_fetch_recovery_handoffs")).isZero();
        var before=metadata();guard=InstallationHostGuard.acquire(binding);var inspect=start("native-content-inspect",plan);assertThat(inspect.await("NATIVE_CONTENT_INSPECTED").get("terminalStatus").stringValue()).isEqualTo("CONFIRMED_TERMINAL");inspect.exited(0);assertThat(metadata()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"false","nonce","target","eof"})
    void invalidSingleReviewLeavesOriginalOccupied(String mode)throws Exception{
        admitted();var child=start("native-content-end",null);child.await("NATIVE_CONTENT_PLAN");var prompt=child.await("CONFIRMATION_REQUIRED");var reply=answer(prompt);
        switch(mode){case "false"->reply.put("remoteWritersStopped",false);case "nonce"->reply.put("challenge",UUID.randomUUID().toString());case "target"->reply.put("targetSha256","0".repeat(64));default->child.process.getOutputStream().close();}
        if(!mode.equals("eof"))child.send(reply);child.await("RECOVERY_REQUIRED");child.exited(2);assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());assertThat(count("content_original_terminal_receipts")).isZero();assertThat(count("attempts")).isEqualTo(1);assertThat(requests).hasValue(0);
    }
    static final class Child {
        final Process process;final BlockingQueue<String> lines=new LinkedBlockingQueue<>();final List<String> output=new CopyOnWriteArrayList<>();
        Child(Process process){this.process=process;Thread.ofPlatform().daemon().start(()->{try(var reader=process.inputReader()){String line;while((line=reader.readLine())!=null){output.add(line);lines.add(line);}}catch(java.io.IOException ignored){}});}
        JsonNode await(String... statuses)throws Exception {
            var wanted=Set.of(statuses);long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
            while(System.nanoTime()<deadline){var line=lines.poll(100,TimeUnit.MILLISECONDS);if(line==null){if(!process.isAlive() && lines.isEmpty())break;continue;}
                assertThat(line).startsWith("{").doesNotContain("jdbc:","Exception","command.json","retained.json");var event=SourceJson.parse(line.getBytes(java.nio.charset.StandardCharsets.UTF_8),1024*1024);
                if(wanted.contains(event.get("status").stringValue()))return event;
                if(Set.of("COMPLETED","RECOVERY_REQUIRED").contains(event.get("status").stringValue()))break;
            }
            throw new AssertionError("Expected "+wanted+"; output="+output);
        }
        void send(JsonNode reply)throws Exception {process.getOutputStream().write(SourceJson.canonical(reply));process.getOutputStream().write('\n');process.getOutputStream().flush();}
        void exited(int expected)throws Exception {assertThat(process.waitFor(15,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).withFailMessage(output.toString()).isEqualTo(expected);}
    }
}
