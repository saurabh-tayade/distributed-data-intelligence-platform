package platform.contentapi.execution;

import java.util.UUID;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.*;

/** Fast no-I/O regression for fixture JDBC wrappers and production admission identity fencing. */
class WorkerRepositoryInfrastructureIdentityTest {
    @Test void persistentStopVetoesNarrowFencesBeforeAnyDatabaseOrGuardAccess(){
        var stopping=new java.util.concurrent.atomic.AtomicBoolean();
        var worker=new WorkerRepository(new JdbcTemplate(new DriverManagerDataSource()),new ManagementTransactions(new TransactionTemplate()),UUID.randomUUID(),WorkerExecutionCapability.NONE,stopping::get);
        assertThatCode(worker::checkNarrowActive).doesNotThrowAnyException();
        stopping.set(true);Thread.interrupted();
        assertThatThrownBy(()->worker.narrowAdmissionTransaction(null,null,()->{throw new AssertionError("must not mutate");})).hasMessage("WORKER_NOT_READY");
        assertThat(worker.stopping()).isTrue();
    }
    @Test void sharedWrapperPassesButSameDataSourceDifferentTemplateFails() {
        var source=new DriverManagerDataSource();
        var original=new JdbcTemplate(source);
        var registration=new JdbcTemplate(source);
        var tx=new ManagementTransactions(new TransactionTemplate());
        var worker=WorkerTestSupport.enabled(registration,tx,UUID.randomUUID());
        assertThatCode(()->new NarrowContentAdmission(registration,tx,worker)).doesNotThrowAnyException();
        assertThatThrownBy(()->new NarrowContentAdmission(original,tx,worker))
                .isInstanceOf(WorkerFailure.class).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThatThrownBy(()->new NarrowContentAdmission(registration,new ManagementTransactions(new TransactionTemplate()),worker))
                .isInstanceOf(WorkerFailure.class).hasMessage("WORKER_CONFIGURATION_INVALID");
    }
}
