package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** A real separate JVM using durable SQL facts. Never copies an Owner from the parent process. */
public final class NarrowContentAdmissionTestChild {
    public static void main(String[] args) throws Exception {
        var binding=OwnershipFixtures.readBinding(Path.of(args[0]));
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_TEST_DB_URL"),
                System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var tx=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())));
        UUID run=UUID.fromString(args[3]),attempt=UUID.fromString(args[4]);
        try(var guard=InstallationHostGuard.acquire(binding)) {
            if(args[1].equals("inspect")) {
                var facts=NarrowContentAdmissionTestSupport.inspect(jdbc,tx,guard,run,attempt);
                System.out.println("NARROW_READY "+ProcessHandle.current().pid()+" "+facts.ledgerSha256());
            } else {
                var workers=WorkerTestSupport.enabled(jdbc,tx,binding.installationId());
                String digest=jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,args[2]);
                var session=workers.register(guard,args[2],digest,"a".repeat(64));
                var claim=NarrowContentAdmissionTestSupport.claim(workers,guard,session,attempt,run);
                if(!claim.status().equals("CLAIMED"))throw new AssertionError("CLAIM_REQUIRED");
                NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,claim.owner(),boundary->{
                    if(boundary.equals(args[1])) {
                        System.out.println("NARROW_BOUNDARY "+ProcessHandle.current().pid()+" "+boundary);System.out.flush();
                        try {System.in.read();} catch(java.io.IOException ex) {throw new IllegalStateException("TEST_INPUT_CLOSED");}
                    }
                });
                System.out.println("NARROW_UNEXPECTED_RETURN");
            }
        } catch(ManagementFailure|WorkerFailure|PreflightFailure|ImportOwnershipFailure ex) {
            System.out.println("NARROW_REJECTED "+ProcessHandle.current().pid()+" "+ex.getMessage());
        }
        System.out.flush();
    }
    private NarrowContentAdmissionTestChild() { }
}
