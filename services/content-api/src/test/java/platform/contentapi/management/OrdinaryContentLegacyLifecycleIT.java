package platform.contentapi.management;

import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Historical strict composition 1 remains valid metadata but is never an ordinary source lane. */
class OrdinaryContentLegacyLifecycleIT extends OrdinaryContentWorkerFixtures {
    @Override boolean publicComposition(){return false;}
    @Test void historicalLoopbackCompositionCannotStartOrdinaryDispatcher(){
        var run=queue("historical-composition");var worker=lifecycle(jdbc,workerEnvironment());
        assertThatThrownBy(worker::start).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(count("worker_instances")).isZero();assertThat(count("attempts")).isZero();assertThat(state(run)).isEqualTo("QUEUED");noNativeWork();
    }
}
