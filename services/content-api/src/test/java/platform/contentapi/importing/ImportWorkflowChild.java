package platform.contentapi.importing;
import java.nio.file.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import platform.contentapi.execution.*;
import static platform.contentapi.execution.OwnershipFixtures.INSTALL;
/** Disposable JVM for actual abrupt-parent-death tests. */
public final class ImportWorkflowChild {
    public static void main(String[] args) throws Exception {
        var c=ImportCatalog.load(Path.of(args[0]));
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_CHILD_DB_URL"),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var template=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));template.setTimeout(3);
        var tx=new ManagementTransactions(template);var bindings=new ImportBindings(jdbc,tx,INSTALL);var runtime=bindings.runtime(c.profileId());
        try(var guard=InstallationHostGuard.acquire(bindings.control(c.controlBindingId(),c.profileId()))) {
            var repository=new ImportOwnershipRepository(jdbc,tx,INSTALL);
            repository.reserve(guard,new ImportOwnershipRepository.Reservation(c.operationId(),runtime.id(),runtime.digest(),c.requestBytes()));
            if(args[1].equals("qualified")) {
                var verifier=new NativeImportVerifier(new NativeImportProcess());
                for(var e:c.entries()){var s=bindings.storage(e,c.profileId());var q=verifier.verify(c.operationId(),e,s,runtime);verifier.recheck(c.operationId(),q,s,runtime);}
            }
            System.out.println("CHECKPOINT");System.out.flush();Thread.sleep(60000);
        }
    }
}
