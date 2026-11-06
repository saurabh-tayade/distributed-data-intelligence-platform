package platform.contentapi.management;

import java.nio.file.*;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Real launcher death and disposable PostgreSQL restart, never a recovery launch route. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchLaunchRecoveryIT extends NarrowContentFetchLaunchFixtures {
    Process launcher(String mode,UUID run,UUID attempt)throws Exception{
        Path descriptor=Files.createTempFile(temp,"launch-binding-",".json");
        Files.write(descriptor,binding.descriptor());
        Files.setPosixFilePermissions(descriptor,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                NarrowContentFetchLaunchTestChild.class.getName(),descriptor.toString(),mode,CONTENT_PROFILE,run.toString(),attempt.toString(),operation.toString(),LAUNCH_PROFILE)
                .redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);
        return builder.start();
    }
    String line(Process child)throws Exception{
        return CompletableFuture.supplyAsync(()->{
            try{var reader=child.inputReader();String value;while((value=reader.readLine())!=null)if(value.startsWith("NARROW_LAUNCH_"))return value;return "EOF";}
            catch(Exception ex){throw new CompletionException(ex);}
        }).get(25,TimeUnit.SECONDS);
    }
    void inspectInFreshJvm(UUID run,UUID attempt,NarrowContentFetchLaunchTestSupport.Facts history)throws Exception{
        var reader=launcher("inspect",run,attempt);
        try{
            assertThat(reader.pid()).isNotEqualTo(ProcessHandle.current().pid());
            assertThat(line(reader)).isEqualTo("NARROW_LAUNCH_HISTORY "+reader.pid()+" "+String.join(",",history.phases())+" "+history.candidateSha256());
            assertThat(reader.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(reader.exitValue()).isZero();
        }finally{stop(reader);}
    }
    static void stop(Process process)throws Exception{
        if(process.isAlive())process.destroyForcibly();
        assertThat(process.waitFor(10,TimeUnit.SECONDS)).isTrue();
    }
    /** A PID alone must never authorize signaling an orphan. Unknown identity fails the test. */
    static void stopOrphan(InstallationHostGuard.ProcessIdentity identity)throws Exception{
        assertThat(identity).isNotNull();var observed=ProcessHandle.of(identity.pid());
        if(observed.isEmpty() || !observed.get().isAlive())return;
        var process=observed.get();var start=process.info().startInstant();
        assertThat(start).isPresent();
        if(!start.orElseThrow().truncatedTo(ChronoUnit.MICROS).equals(identity.startedAt()))return;
        process.destroyForcibly();
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
        while(process.isAlive() && System.nanoTime()<deadline)Thread.sleep(20);
        assertThat(process.isAlive()).as("Exact orphan child must terminate").isFalse();
    }
    @ParameterizedTest @ValueSource(strings={"CHILD_RECORDED","HTTP"})
    void killedLauncherNeverReleasesOrRefetchesFromHistory(String point)throws Exception{
        UUID run=queue("launch-hard-crash"),attempt=UUID.randomUUID();
        responseGate=new CountDownLatch(1);var process=launcher(point,run,attempt);
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        try{
            assertThat(line(process)).isEqualTo("NARROW_LAUNCH_BOUNDARY "+process.pid()+" "+(point.equals("HTTP")?"RELEASE_AUTHORIZED":point));
            if(point.equals("HTTP")){
                long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
                while(requests.get()==0 && System.nanoTime()<deadline)Thread.sleep(10);
                assertThat(requests).hasValue(1);
            }else assertThat(requests).hasValue(0);
        }finally{
            stop(process);
            // The native child is no longer supervised after SIGKILL. Clean up only its exact recorded identity.
            var rows=jdbc.queryForList("SELECT child_instance_id,child_pid,child_started_at FROM ddip_management.content_native_launch_phases WHERE operation_id=? AND phase='CHILD_RECORDED'",operation);
            if(!rows.isEmpty()){
                var row=rows.getFirst();stopOrphan(new InstallationHostGuard.ProcessIdentity((UUID)row.get("child_instance_id"),((Number)row.get("child_pid")).longValue(),((java.sql.Timestamp)row.get("child_started_at")).toInstant()));
            }
            responseGate.countDown();
        }
        var history=facts();assertThat(history.candidateForVerification()).isFalse();
        assertThat(history.phases()).containsExactlyElementsOf(point.equals("HTTP")
                ?List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED")
                :List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED"));
        var files=filesystem();var before=metadata();
        inspectInFreshJvm(run,attempt,history);
        assertThat(facts()).isEqualTo(history);assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),run)).hasMessage("WORKER_NOT_READY");
        assertThat(requests).hasValue(point.equals("HTTP")?1:0);assertThat(count("content_native_operations")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(attempt);
        assertThat(Files.exists(binding.root().resolve("runs/"+run+"/attempts/"+attempt+"/fetch-source/demo-feed/"+
                jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_native_operations WHERE operation_id=?",UUID.class,operation)+"/complete.json"))).isFalse();
        assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void actualPostgresRestartPreservesCandidateFactsWithoutReexecution()throws Exception{
        prepared();var history=launch(boundary->{});assertThat(requests).hasValue(1);
        guard.close();guard=null;var before=metadata();var files=filesystem();
        var phases=jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_native_launch_phases t ORDER BY phase",String.class);
        var nativeRows=jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_native_operations t ORDER BY operation_id",String.class);
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);
        String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).matches("[0-9a-f]{12,64}");
        var restart=new ProcessBuilder("docker","restart",container).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try{assertThat(restart.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(restart.exitValue()).isZero();}finally{stop(restart);}
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
        while(true){
            try{
                var after=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);assertThat(after).isAfter(started);
                System.out.println("NARROW_FETCH_LAUNCH_POSTGRES_RESTART before="+started+" after="+after+" operation="+operation);break;
            }catch(org.springframework.dao.DataAccessException ex){if(System.nanoTime()>deadline)throw ex;Thread.sleep(100);}
        }
        inspectInFreshJvm(owner.runId(),owner.attemptId(),history);
        assertThat(facts()).isEqualTo(history);assertThat(history.candidateForVerification()).isTrue();assertThat(history.completed()).isFalse();
        assertThat(jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_native_launch_phases t ORDER BY phase",String.class)).isEqualTo(phases);
        assertThat(jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_native_operations t ORDER BY operation_id",String.class)).isEqualTo(nativeRows);
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);retained();
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        assertThatThrownBy(()->launch(boundary->{})).isInstanceOf(RuntimeException.class);
        assertThat(requests).hasValue(1);assertThat(facts()).isEqualTo(history);assertThat(filesystem()).isEqualTo(files);retained();
    }
}
