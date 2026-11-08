package platform.contentapi.execution;

import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.*;

class NarrowContentSilverReservationTest {
    @Test void checkpointBridgeIsPrivateAndConsumedExactlyOnce()throws Exception{
        var constructor=NarrowContentBronzeCompletion.LiveCheckpoint.class.getDeclaredConstructor(WorkerRepository.Owner.class,UUID.class,Runnable.class);
        assertThat(java.lang.reflect.Modifier.isPrivate(constructor.getModifiers())).isTrue();constructor.setAccessible(true);
        UUID operation=UUID.randomUUID();var owner=new WorkerRepository.Owner(null,UUID.randomUUID(),UUID.randomUUID(),0,0,"CLAIMED");var checks=new AtomicInteger();
        var live=constructor.newInstance(owner,operation,(Runnable)checks::incrementAndGet);
        assertThatThrownBy(live::check).hasMessage("WORKER_NOT_READY");assertThatThrownBy(()->live.consume(UUID.randomUUID())).hasMessage("WORKER_NOT_READY");
        assertThat(live.consume(operation)).isSameAs(owner);live.check();assertThat(checks).hasValue(1);
        assertThatThrownBy(()->live.consume(operation)).hasMessage("WORKER_NOT_READY");assertThatThrownBy(()->live.consume(UUID.randomUUID())).hasMessage("WORKER_NOT_READY");
    }
    @Test void nullDirectReservationInputsFailBeforeAnyDatabaseAccess(){
        var jdbc=new JdbcTemplate();var tx=new ManagementTransactions(new TransactionTemplate());var worker=new WorkerRepository(jdbc,tx,UUID.randomUUID());
        var reservation=new NarrowContentSilverReservation(jdbc,tx,worker);
        assertThatThrownBy(()->reservation.reserve(null,null,UUID.randomUUID(),b->{})).hasMessage("WORKER_CONFIGURATION_INVALID");
        var completion=new NarrowContentBronzeCompletion(jdbc,tx,worker);
        assertThatThrownBy(()->completion.completeAndReserveSilver(null,null,"profile",UUID.randomUUID(),b->{},b->{})).hasMessage("WORKER_CONFIGURATION_INVALID");
    }
    @Test void repositoryResourcesCannotBeMixed(){
        var jdbc=new JdbcTemplate();var other=new JdbcTemplate();var tx=new ManagementTransactions(new TransactionTemplate());
        assertThatThrownBy(()->new NarrowContentSilverReservation(other,tx,new WorkerRepository(jdbc,tx,UUID.randomUUID()))).hasMessage("WORKER_CONFIGURATION_INVALID");
    }
}
