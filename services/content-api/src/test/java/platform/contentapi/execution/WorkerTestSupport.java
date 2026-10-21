package platform.contentapi.execution;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test classpath only. Qualifies metadata claims, never launches or simulates a publication. */
public final class WorkerTestSupport {
    private WorkerTestSupport() { }
    public static WorkerRepository enabled(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation) {
        return new WorkerRepository(jdbc,tx,installation,new WorkerExecutionCapability(){
            public boolean available(){return true;}
            public boolean supports(String pipeline){return java.util.Set.of("CONTENT_RSS_V1","RELIABILITY_V2").contains(pipeline);}
        });
    }
}
