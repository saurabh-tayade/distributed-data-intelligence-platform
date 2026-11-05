package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Separate JVM crash fixture. Claims its own authority; history mode obtains no token or guard. */
public final class NarrowContentFetchReservationTestChild {
    public static void main(String[] args)throws Exception{
        var binding=OwnershipFixtures.readBinding(Path.of(args[0]));
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_TEST_DB_URL"),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var tx=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())));
        var workers=WorkerTestSupport.enabled(jdbc,tx,binding.installationId());UUID operation=UUID.fromString(args[5]);
        if(args[1].equals("inspect")){
            var facts=NarrowContentFetchReservationTestSupport.inspect(jdbc,tx,workers,operation);
            System.out.println("NARROW_OPERATION "+ProcessHandle.current().pid()+" "+facts.commandSha256());return;
        }
        try(var guard=InstallationHostGuard.acquire(binding)){
            String digest=jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,args[2]);
            var session=workers.register(guard,args[2],digest,NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,args[2]));
            var claim=NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.fromString(args[4]),UUID.fromString(args[3]));
            NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,claim.owner());
            NarrowContentFetchReservationTestSupport.reserve(jdbc,tx,workers,guard,claim.owner(),operation,b->{
                if(b.equals(args[1])){
                    System.out.println("NARROW_BOUNDARY "+ProcessHandle.current().pid()+" "+b);System.out.flush();
                    try{System.in.read();}catch(java.io.IOException ex){throw new IllegalStateException("TEST_INPUT_CLOSED");}
                }
            });
            throw new AssertionError("TEST_BOUNDARY_NOT_REACHED");
        }
    }
    private NarrowContentFetchReservationTestChild(){}
}
