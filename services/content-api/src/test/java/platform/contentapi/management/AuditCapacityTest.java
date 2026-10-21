package platform.contentapi.management;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class AuditCapacityTest {
    @Test void reservesEntireRunLifecycleAndImportClosureBeforeNewAdmission(){assertThat(AuditCapacity.allows(995903,1,1,0)).isTrue();assertThat(AuditCapacity.allows(995904,1,1,0)).isFalse();assertThat(AuditCapacity.allows(999996,1,0,1)).isTrue();assertThat(AuditCapacity.allows(999997,1,0,1)).isFalse();}
    @Test void twentyRunsAndHistoryAreBoundedWithoutPurging(){assertThat(AuditCapacity.allows(918079,1,20,0)).isTrue();assertThat(AuditCapacity.allows(918080,1,20,0)).isFalse();assertThat(AuditCapacity.allows(1000000,1,0,0)).isFalse();}
}
