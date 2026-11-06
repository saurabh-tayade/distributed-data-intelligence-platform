package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;

/** Fresh JVM and actual disposable PostgreSQL restart; inspection never continues execution. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchCandidateVerificationRecoveryIT extends NarrowContentFetchLaunchFixtures {
    Map<String,List<String>> history(){
        var result=new TreeMap<>(metadata());
        for(String table:List.of("content_native_operations","content_native_launch_phases"))
            result.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));
        return result;
    }
    void freshJvm()throws Exception{
        Path descriptor=Files.createTempFile(temp,"candidate-binding-",".json"),output=Files.createTempFile(temp,"candidate-inspection-",".log");
        Files.write(descriptor,binding.descriptor());Files.setPosixFilePermissions(descriptor,PosixFilePermissions.fromString("rw-------"));
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                NarrowContentFetchCandidateVerificationTestChild.class.getName(),descriptor.toString(),operation.toString())
                .redirectError(ProcessBuilder.Redirect.INHERIT).redirectOutput(output.toFile());
        builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);
        Process child=builder.start();
        try{
            assertThat(child.pid()).isNotEqualTo(ProcessHandle.current().pid());assertThat(child.waitFor(30,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isZero();
            assertThat(Files.readString(output)).contains("NARROW_CANDIDATE_VERIFIED "+operation+" "+facts().candidateSha256()+" "+payload.length);
        }finally{if(child.isAlive()){child.destroyForcibly();assertThat(child.waitFor(10,TimeUnit.SECONDS)).isTrue();}}
    }
    @Test void freshJvmReconstructsCandidateWithoutWorkerRegistrationOrOwner()throws Exception{
        prepared();launch(b->{});var before=history();var files=filesystem();guard.close();guard=null;
        freshJvm();freshJvm();assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();
        guard=InstallationHostGuard.acquire(binding);
        assertThat(NarrowContentFetchCandidateVerificationTestSupport.inspect(jdbc,tx,guard,operation).rawHttpJournalVerified()).isTrue();
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
    }
    @Test void actualPostgresRestartPreservesReadOnlyVerification()throws Exception{
        prepared();launch(b->{});var before=history();var files=filesystem();guard.close();guard=null;
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);
        String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).matches("[0-9a-f]{12,64}");
        Process restart=new ProcessBuilder("docker","restart",container).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try{assertThat(restart.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(restart.exitValue()).isZero();}
        finally{if(restart.isAlive()){restart.destroyForcibly();assertThat(restart.waitFor(10,TimeUnit.SECONDS)).isTrue();}}
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
        while(true){
            try{
                var after=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);assertThat(after).isAfter(started);
                System.out.println("NARROW_FETCH_CANDIDATE_POSTGRES_RESTART before="+started+" after="+after+" operation="+operation);break;
            }catch(org.springframework.dao.DataAccessException ex){if(System.nanoTime()>deadline)throw ex;Thread.sleep(100);}
        }
        freshJvm();assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);retained();
        guard=InstallationHostGuard.acquire(binding);var result=NarrowContentFetchCandidateVerificationTestSupport.inspect(jdbc,tx,guard,operation);
        assertThat(result.rawHttpJournalVerified()).isTrue();assertThat(result.completed()).isFalse();assertThat(result.nativeLaunchCompatible()).isFalse();
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(Files.exists(fetchDirectory().resolve("complete.json"))).isFalse();
    }
}
