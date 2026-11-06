package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** A real owner JVM is killed at completion boundaries; inspection never registers a worker. */
public final class NarrowContentFetchCompletionTestChild {
    public static void main(String[] args)throws Exception{
        var binding=OwnershipFixtures.readBinding(Path.of(args[0]));
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_TEST_DB_URL"),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var tx=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())));
        UUID operation=UUID.fromString(args[5]);
        if(args[1].equals("inspect")){
            var result=NarrowContentFetchCompletionTestSupport.inspect(jdbc,tx,null,operation);
            if(result.isPresent() && (!result.get().checkpointed() || result.get().nativeLaunchCompatible()))throw new AssertionError("INVALID_RECEIPT");
            System.out.println("NARROW_COMPLETION_HISTORY "+result.isPresent());return;
        }
        var workers=WorkerTestSupport.enabled(jdbc,tx,binding.installationId());
        try(var guard=InstallationHostGuard.acquire(binding)){
            String digest=jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,args[2]);
            var session=workers.register(guard,args[2],digest,NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,args[2]));
            var claim=NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.fromString(args[4]),UUID.fromString(args[3]));
            NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,claim.owner());
            NarrowContentFetchCompletionTestSupport.launchAndCheckpoint(jdbc,tx,workers,guard,claim.owner(),operation,args[6],b->{if(b.equals(args[1])){
                System.out.println("NARROW_COMPLETION_BOUNDARY "+b);System.out.flush();
                try{System.in.read();}catch(java.io.IOException ex){throw new IllegalStateException(ex);}
            }});
            throw new AssertionError("OWNER_WAS_NOT_KILLED");
        }
    }
    private NarrowContentFetchCompletionTestChild(){}
}
