package platform.contentapi.execution;

import java.nio.file.*;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Disposable real worker; the test kills this JVM without Java cleanup at an exact boundary. */
public final class NarrowContentPipelineCrashTestProcess {
    public static void main(String[] args)throws Exception {
        var binding=OwnershipFixtures.readBinding(Path.of(args[0]));
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_TEST_DB_URL"),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var tx=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())));
        var workers=WorkerTestSupport.enabled(jdbc,tx,binding.installationId());
        try(var guard=InstallationHostGuard.acquire(binding)) {
            String digest=jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,args[1]);
            var session=workers.register(guard,args[1],digest,NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,args[1]));
            var claim=NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.fromString(args[3]),UUID.fromString(args[2]));
            NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,claim.owner());
            NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,claim.owner(),args[4],event->{
                if(!event.boundary().equals("HEARTBEAT"))System.out.println("PIPELINE_CRASH_PROGRESS "+event);
                if(event.operation().equals(args[5]) && event.boundary().equals(args[6])) {
                    if(event.boundary().equals("HEARTBEAT") && ProcessHandle.current().descendants().noneMatch(p->p.info().commandLine().orElse("").contains("org.apache.spark.deploy.SparkSubmit")))return;
                    try {
                        Path marker=Path.of(args[7]),preparing=Path.of(args[7]+".preparing");
                        Files.writeString(preparing,event.operation()+":"+event.boundary(),StandardOpenOption.CREATE_NEW);
                        Files.move(preparing,marker,StandardCopyOption.ATOMIC_MOVE);
                        while(true)Thread.sleep(1000);
                    }catch(Exception ex){throw new IllegalStateException(ex);}
                }
            });
            throw new AssertionError("EXPECTED_CRASH_BOUNDARY_NOT_REACHED");
        }
    }
    private NarrowContentPipelineCrashTestProcess(){}
}
