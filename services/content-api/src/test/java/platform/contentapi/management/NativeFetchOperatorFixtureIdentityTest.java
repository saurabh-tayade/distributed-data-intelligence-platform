package platform.contentapi.management;

import java.sql.Timestamp;
import java.time.Instant;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class NativeFetchOperatorFixtureIdentityTest {
    private static Object[] arguments(){return new Object[]{"instance","installation","profile","profile-sha","host-sha",1L,Timestamp.from(Instant.EPOCH),"READY"};}
    @Test void replacesOnlyOriginalInsertIdentityWithoutMutatingCaller() {
        var original=arguments();var started=Timestamp.from(Instant.ofEpochSecond(10));
        var shaped=NativeFetchOperatorFixtureIdentity.atInsert(NativeFetchOperatorFixtureIdentity.INSERT,original,2L,started);
        assertThat(shaped).containsExactly("instance","installation","profile","profile-sha","host-sha",2L,started,"READY");
        assertThat(original).containsExactly(arguments());
    }
    @Test void registrationSqlOrParameterShapeChangesFailClearly() {
        assertThatThrownBy(()->NativeFetchOperatorFixtureIdentity.atInsert("UPDATE ddip_management.worker_instances",arguments(),2L,Timestamp.from(Instant.EPOCH)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("INSERT shape changed");
        assertThatThrownBy(()->NativeFetchOperatorFixtureIdentity.atInsert(NativeFetchOperatorFixtureIdentity.INSERT,new Object[9],2L,Timestamp.from(Instant.EPOCH)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("INSERT shape changed");
        var reordered=arguments();reordered[5]="not-the-pid-slot";
        assertThatThrownBy(()->NativeFetchOperatorFixtureIdentity.atInsert(NativeFetchOperatorFixtureIdentity.INSERT,reordered,2L,Timestamp.from(Instant.EPOCH)))
                .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("INSERT shape changed");
    }
}
