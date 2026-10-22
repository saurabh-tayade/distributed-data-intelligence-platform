package platform.contentapi.execution;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class WorkerCapabilityTest {
    @Test void packagedCapabilityHasOnlyTheFixedReliabilityAdapter(){
        assertThat(WorkerExecutionCapability.RELIABILITY_V2.available()).isTrue();
        assertThat(WorkerExecutionCapability.RELIABILITY_V2.supports("RELIABILITY_V2")).isTrue();
        assertThat(WorkerExecutionCapability.RELIABILITY_V2.supports("CONTENT_RSS_V1")).isFalse();
        assertThat(WorkerExecutionCapability.RELIABILITY_V2.supports("fake")).isFalse();
        assertThat(WorkerExecutionCapability.NONE.available()).isFalse();
    }
}
