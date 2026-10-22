package platform.contentapi.execution;

import java.util.function.Supplier;
import java.sql.SQLException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.transaction.TransactionException;
import org.springframework.transaction.support.TransactionTemplate;

public final class ManagementTransactions {
    private final TransactionTemplate transactions;
    public ManagementTransactions(TransactionTemplate transactions) { this.transactions = transactions; }
    public <T> T required(Supplier<T> operation) {
        try { return transactions.execute(status -> operation.get()); }
        catch (DataIntegrityViolationException ex) {
            throw new ManagementFailure(ManagementFailure.Code.STATE_CONFLICT);
        }
        catch (DataAccessException | TransactionException ex) {
            for(Throwable cause=ex; cause!=null; cause=cause.getCause()) {
                if(cause instanceof SQLException sql && sql.getSQLState()!=null && sql.getSQLState().startsWith("23"))
                    throw new ManagementFailure(ManagementFailure.Code.STATE_CONFLICT);
            }
            throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_UNAVAILABLE);
        }
    }
}
