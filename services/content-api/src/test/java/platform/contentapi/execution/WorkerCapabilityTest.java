package platform.contentapi.execution;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

class WorkerCapabilityTest {
    @Test void legacyCapabilitiesNeverDispatchContent(){
        assertThat(WorkerExecutionCapability.RELIABILITY_V2.available()).isTrue();
        assertThat(WorkerExecutionCapability.RELIABILITY_V2.supports("RELIABILITY_V2")).isTrue();
        assertThat(WorkerExecutionCapability.RELIABILITY_V2.supports("CONTENT_RSS_V1")).isFalse();
        assertThat(WorkerExecutionCapability.RELIABILITY_V2.supports("fake")).isFalse();
        assertThat(WorkerExecutionCapability.NONE.available()).isFalse();
        assertThat(WorkerExecutionCapability.NONE.supports("RELIABILITY_V2")).isFalse();
        assertThat(WorkerExecutionCapability.NONE.supports("CONTENT_RSS_V1")).isFalse();
        assertThat(NarrowContentPipelineTestSupport.legacyCapabilitiesDispatchContent()).isFalse();
    }
    @Test void qualifiedOriginalContentCapabilitySupportsOnlyContent(){
        assertThat(WorkerExecutionCapability.CONTENT_NARROW_ORIGINAL.available()).isTrue();
        assertThat(WorkerExecutionCapability.CONTENT_NARROW_ORIGINAL.supports("CONTENT_RSS_V1")).isTrue();
        assertThat(WorkerExecutionCapability.CONTENT_NARROW_ORIGINAL.supports("RELIABILITY_V2")).isFalse();
        assertThat(WorkerExecutionCapability.CONTENT_NARROW_ORIGINAL.supports("fake")).isFalse();
        assertThat(WorkerExecutionCapability.CONTENT_NARROW_ORIGINAL.supports(null)).isFalse();
    }
}
