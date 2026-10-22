package platform.contentapi.execution;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test classpath only. Qualifies metadata claims, never launches or simulates a publication. */
public final class WorkerTestSupport {
    private WorkerTestSupport() { }
    /** Exercise the exact packaged capability without starting native execution. */
    public static WorkerRepository reliability(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation) {
        return new WorkerRepository(jdbc,tx,installation,WorkerExecutionCapability.RELIABILITY_V2);
    }
    public static WorkerRepository enabled(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation) {
        return new WorkerRepository(jdbc,tx,installation,new WorkerExecutionCapability(){
            public boolean available(){return true;}
            public boolean supports(String pipeline){return java.util.Set.of("CONTENT_RSS_V1","RELIABILITY_V2").contains(pipeline);}
        });
    }
}
