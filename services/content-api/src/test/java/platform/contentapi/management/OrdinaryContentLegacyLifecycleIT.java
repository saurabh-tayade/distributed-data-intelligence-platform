package platform.contentapi.management;

import java.nio.file.Files;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;

/** Historical strict composition 1 remains valid metadata but is never an ordinary source lane. */
class OrdinaryContentLegacyLifecycleIT extends OrdinaryContentWorkerFixtures {
    @Override boolean publicComposition(){return false;}
    @Test void packagedWorkerRejectsHistoricalCompositionBeforeRegistering()throws Exception {
        var log=temp.resolve("packaged-legacy-rejection.log");var child=packagedWorker(log);
        try {
            assertThat(child.waitFor(30,java.util.concurrent.TimeUnit.SECONDS)).as(Files.exists(log)?Files.readString(log):"").isTrue();
            assertThat(child.exitValue()).isNotZero();
            assertThat(count("worker_instances")).isZero();assertThat(count("attempts")).isZero();noNativeWork();
        } finally {
            if(child.isAlive()){child.destroyForcibly();child.waitFor(10,java.util.concurrent.TimeUnit.SECONDS);}
        }
    }
    @Test void historicalLoopbackCompositionCannotStartOrdinaryDispatcher(){
        var run=queue("historical-composition");var worker=lifecycle(jdbc,workerEnvironment());
        assertThatThrownBy(worker::start).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(count("worker_instances")).isZero();assertThat(count("attempts")).isZero();assertThat(state(run)).isEqualTo("QUEUED");noNativeWork();
    }
}
