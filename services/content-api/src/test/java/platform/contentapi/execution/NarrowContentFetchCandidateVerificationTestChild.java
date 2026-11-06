package platform.contentapi.execution;

import java.nio.file.Path;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Fresh verifier process: host guard only, no worker registration, owner or launch. */
public final class NarrowContentFetchCandidateVerificationTestChild {
    public static void main(String[] args)throws Exception{
        var binding=OwnershipFixtures.readBinding(Path.of(args[0]));
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_TEST_DB_URL"),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var tx=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())));
        try(var guard=InstallationHostGuard.acquire(binding)){
            var value=NarrowContentFetchCandidateVerificationTestSupport.inspect(jdbc,tx,guard,UUID.fromString(args[1]));
            if(!value.rawHttpJournalVerified() || value.completed() || value.nativeLaunchCompatible())throw new AssertionError("INVALID_OBSERVATION");
            System.out.println("NARROW_CANDIDATE_VERIFIED "+value.operation()+" "+value.candidateSha256()+" "+value.payloadBytes());
        }
    }
    private NarrowContentFetchCandidateVerificationTestChild(){}
}
