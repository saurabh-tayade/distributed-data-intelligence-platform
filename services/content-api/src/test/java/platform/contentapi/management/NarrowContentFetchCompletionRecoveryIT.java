package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** SIGKILL, fresh SQL-only JVM readers and an actual disposable PostgreSQL restart. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchCompletionRecoveryIT extends NarrowContentFetchLaunchFixtures {
    Map<String,List<String>> history(){var result=new TreeMap<>(metadata());for(String table:List.of("content_native_operations","content_native_launch_phases","content_fetch_completion_intents","content_fetch_checkpoints"))result.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));return result;}
    Process child(String mode,UUID run,UUID attempt)throws Exception{
        Path descriptor=Files.createTempFile(temp,"completion-binding-",".json");Files.write(descriptor,binding.descriptor());Files.setPosixFilePermissions(descriptor,PosixFilePermissions.fromString("rw-------"));
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),NarrowContentFetchCompletionTestChild.class.getName(),descriptor.toString(),mode,CONTENT_PROFILE,run.toString(),attempt.toString(),operation.toString(),LAUNCH_PROFILE).redirectError(ProcessBuilder.Redirect.INHERIT);
        builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);return builder.start();
    }
    String line(Process process)throws Exception{return CompletableFuture.supplyAsync(()->{try{var reader=process.inputReader();String s;while((s=reader.readLine())!=null)if(s.startsWith("NARROW_COMPLETION_"))return s;return "EOF";}catch(Exception ex){throw new CompletionException(ex);}}).get(45,TimeUnit.SECONDS);}
    static void stop(Process process)throws Exception{if(process.isAlive())process.destroyForcibly();assertThat(process.waitFor(10,TimeUnit.SECONDS)).isTrue();}
    void freshJvm(UUID run,UUID attempt,boolean present)throws Exception{var process=child("inspect",run,attempt);try{assertThat(line(process)).isEqualTo("NARROW_COMPLETION_HISTORY "+present);assertThat(process.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isZero();}finally{stop(process);}}
    @ParameterizedTest @ValueSource(strings={"FRESH_VERIFIED","BEFORE_INTENT","INTENT_INSERTED","INTENT_COMMITTED","BEFORE_MARKER","PREFIX_CREATED","PREFIX_WRITTEN","PREFIX_FORCED","MARKER_RENAMED","DIRECTORY_FORCED","MARKER_WRITTEN","JOURNAL_REOPENED","REOPENED","BEFORE_CHECKPOINT","CHECKPOINT_INSERTED","CHECKPOINT_COMMITTED"})
    void killedOwnerNeverResumesOrRefetches(String boundary)throws Exception{
        UUID run=queue("completion-crash"),attempt=UUID.randomUUID();var process=child(boundary,run,attempt);
        try{assertThat(line(process)).isEqualTo("NARROW_COMPLETION_BOUNDARY "+boundary);}finally{stop(process);}
        boolean consumed=!Set.of("FRESH_VERIFIED","BEFORE_INTENT","INTENT_INSERTED").contains(boundary),checkpointed=boundary.equals("CHECKPOINT_COMMITTED");
        assertThat(count("content_fetch_completion_intents")).isEqualTo(consumed?1:0);assertThat(count("content_fetch_checkpoints")).isEqualTo(checkpointed?1:0);assertThat(requests).hasValue(1);noExecution();
        var before=history();var files=filesystem();freshJvm(run,attempt,checkpointed);freshJvm(run,attempt,checkpointed);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");assertThatThrownBy(()->NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),run)).hasMessage("WORKER_NOT_READY");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(attempt);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);noExecution();
    }
    @Test void postgresRestartAndMissingFilesystemPreserveDetachedReceipt()throws Exception{
        prepared();launch(b->{});var expected=NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});guard.close();guard=null;
        // Historical inspection must not reopen or repair native files or depend on installed JARs.
        Files.move(attemptRoot(),temp.resolve("detached-completed-attempt"));Files.delete(installedFetchJar);var before=history();var files=filesystem();
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).matches("[0-9a-f]{12,64}");
        Process restart=new ProcessBuilder("docker","restart",container).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();try{assertThat(restart.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(restart.exitValue()).isZero();}finally{stop(restart);}
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(true)try{var after=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);assertThat(after).isAfter(started);System.out.println("NARROW_FETCH_COMPLETION_POSTGRES_RESTART before="+started+" after="+after+" operation="+operation);break;}catch(org.springframework.dao.DataAccessException ex){if(System.nanoTime()>deadline)throw ex;Thread.sleep(100);}
        freshJvm(owner.runId(),owner.attemptId(),true);
        try(var inspectionGuard=InstallationHostGuard.acquire(binding)){
            var assessment=NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,inspectionGuard,owner.runId(),owner.attemptId());
            assertThat(assessment.durable().prefix()).isEqualTo("CHECKPOINT");assertThat(assessment.integrity()).isEqualTo("UNAVAILABLE_OR_CHANGED");
            assertThat(assessment.authorization()).isFalse();assertThat(assessment.quiescenceEstablished()).isFalse();assertThat(assessment.nativeLaunchCompatible()).isFalse();
        }
        assertThat(NarrowContentFetchCompletionTestSupport.inspect(jdbc,tx,null,operation)).contains(expected);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();
    }
}
