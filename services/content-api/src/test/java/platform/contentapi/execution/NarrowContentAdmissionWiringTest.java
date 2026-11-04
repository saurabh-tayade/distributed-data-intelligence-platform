package platform.contentapi.execution;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.*;

/** No database connection: wiring must fail before claiming, committing or touching files. */
class NarrowContentAdmissionWiringTest {
    final JdbcTemplate jdbc=new JdbcTemplate();
    final ManagementTransactions tx=new ManagementTransactions(new TransactionTemplate());
    final WorkerRepository workers=new WorkerRepository(jdbc,tx,UUID.randomUUID());

    @Test void identicalDatabaseAndTransactionObjectsAreAccepted(){
        assertThatCode(()->new NarrowContentAdmission(jdbc,tx,workers)).doesNotThrowAnyException();
    }
    @Test void differentDatabaseObjectIsRejectedBeforeAnyOperation(){
        assertThatThrownBy(()->new NarrowContentAdmission(new JdbcTemplate(),tx,workers))
                .isInstanceOf(WorkerFailure.class).hasMessage("WORKER_CONFIGURATION_INVALID").hasNoCause();
    }
    @Test void differentTransactionObjectIsRejectedBeforeAnyOperation(){
        assertThatThrownBy(()->new NarrowContentAdmission(jdbc,new ManagementTransactions(new TransactionTemplate()),workers))
                .isInstanceOf(WorkerFailure.class).hasMessage("WORKER_CONFIGURATION_INVALID").hasNoCause();
    }
}
