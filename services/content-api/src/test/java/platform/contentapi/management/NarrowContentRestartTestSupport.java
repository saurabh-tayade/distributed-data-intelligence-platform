package platform.contentapi.management;

import java.nio.file.*;
import java.time.OffsetDateTime;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.*;

/** Explicit disposable-database restart only, shared by native qualification fixtures. */
final class NarrowContentRestartTestSupport {
    static void restart(JdbcTemplate jdbc,UUID attempt)throws Exception {
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);
        String signal=System.getProperty("ddip.test.narrow-pipeline-restart-signal");
        if(signal!=null) {
            Path request=Path.of(signal),done=Path.of(signal+".done");
            Files.deleteIfExists(done);Files.writeString(request,"READY "+attempt);
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);
            while(!Files.exists(done) && System.nanoTime()<deadline)Thread.sleep(50);
            assertThat(Files.exists(done)).as("isolated PostgreSQL restart acknowledgment").isTrue();
            Files.deleteIfExists(request);
        } else {
            // Only an explicitly supplied disposable CI service, never an inferred host database.
            String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).matches("[0-9a-f]{12,64}");
            var process=new ProcessBuilder("docker","restart",container).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try{assertThat(process.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isZero();}
            finally{if(process.isAlive()){process.destroyForcibly();process.waitFor(5,TimeUnit.SECONDS);}}
        }
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
        while(true) {
            try {
                var after=jdbc.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);assertThat(after).isAfter(started);
                System.out.println("NARROW_PIPELINE_POSTGRES_RESTART before="+started+" after="+after+" attempt="+attempt);return;
            } catch(org.springframework.dao.DataAccessException ex){if(System.nanoTime()>deadline)throw ex;Thread.sleep(100);}
        }
    }
    private NarrowContentRestartTestSupport(){}
}
