package platform.contentapi.execution;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.file.Path;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Test JVM only. No application role or operational import entry point. */
public final class OwnershipTestChild {
    public static void main(String[] args) throws Exception {
        var binding=OwnershipFixtures.readBinding(Path.of(args[0]));
        String mode=args[1];UUID id=UUID.fromString(args[2]);
        try(var guard=InstallationHostGuard.acquire(binding)) {
            if(!mode.equals("guard")) {
                var jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_CHILD_DB_URL"),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
                var template=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));template.setTimeout(3);
                var tx=new ManagementTransactions(template);
                var repository=new ImportOwnershipRepository(jdbc,tx,OwnershipFixtures.INSTALL);
                if(mode.equals("recover")) {
                    UUID oldId=UUID.fromString(System.getenv("DDIP_CHILD_PREDECESSOR"));
                    var op=repository.lookup(oldId,OwnershipFixtures.REQUEST).orElseThrow();
                    var slot=new JdbcCoordinatorRepository(jdbc,tx,OwnershipFixtures.INSTALL).read();
                    repository.handoff(guard,new ImportOwnershipRepository.Token(oldId,op.processInstanceId(),op.epoch(),op.rowVersion(),slot.rowVersion()),id,true);
                } else repository.reserve(guard,OwnershipFixtures.reservation(id));
            }
            System.out.println("ACQUIRED");System.out.flush();
            if(!mode.equals("released"))new BufferedReader(new InputStreamReader(System.in)).readLine();
        } catch(ImportOwnershipFailure failure) {
            System.out.println(failure.getMessage());System.out.flush();return;
        }
        if(mode.equals("released")) {
            System.out.println("GUARD_RELEASED");System.out.flush();
            new BufferedReader(new InputStreamReader(System.in)).readLine();
        }
    }
}
