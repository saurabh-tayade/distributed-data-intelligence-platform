package platform.contentapi.execution;

import java.time.Clock;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Fresh process has no owner, guard, review or qualified token. */
public final class NarrowContentFetchTerminalLookupTestProcess {
    public static void main(String[] args){
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(args[0],args[1],args[2]));jdbc.setQueryTimeout(3);
        var tx=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())));
        var repo=new NarrowContentFetchRecoveryFinalization(jdbc,tx,UUID.fromString(args[3]),Clock.systemUTC(),
                identity->{throw new AssertionError("LOOKUP_PROCESS_IO");},boundary->{throw new AssertionError("LOOKUP_MUTATION");});
        var receipt=repo.lookup(UUID.fromString(args[4]));
        if(receipt.status()!=NarrowContentFetchRecoveryFinalization.Status.CONFIRMED_TERMINAL)throw new AssertionError("LOOKUP_UNRESOLVED");
        System.out.println("NATIVE_FETCH_TERMINAL_LOOKUP "+receipt.status());
    }
    private NarrowContentFetchTerminalLookupTestProcess(){}
}
