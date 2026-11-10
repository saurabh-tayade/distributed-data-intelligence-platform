package platform.contentapi.execution;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import platform.contentapi.importing.ImportBindings;

/** Test child really owns the retry; halt leaves an authorized launch gap, not a fabricated PID. */
public final class ReliabilityRetryCrashProcess {
    public static void main(String[] args){
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(args[0],System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var tx=ReliabilityExecutionFixtures.transactions(jdbc);UUID installation=UUID.fromString(args[1]);
        try(var guard=InstallationHostGuard.acquire(new ImportBindings(jdbc,tx,installation).control(args[2],args[3]))){
            new ReliabilityRetry(jdbc,tx,installation).execute(guard,UUID.fromString(args[4]),args[5],new ReadOnlyVerifierProcess(true){
                @Override Result run(List<String> command,Map<String,String> environment,Path cwd,Duration deadline,Observer observer){observer.beforeStart();Runtime.getRuntime().halt(23);throw new AssertionError("halt returned");}
            });
        }
        throw new AssertionError("crash boundary not reached");
    }
}
