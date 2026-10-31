package platform.contentapi.execution;

import org.junit.jupiter.api.Test;

/** D5b-1 regression: no continuation is performed by reconciliation. */
class ContentRecoveryIT extends ContentRecoveryFixture {
    @Test void actualWorkerExitPostgresRestartAndImmutableFinalRecovery()throws Exception{recover("FINAL");}
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(strings={"BEFORE_FETCH","FETCH_SOURCE","BRONZE","SILVER","GOLD","CHECK_INPUTS"})
    void earlierVerifiedCheckpointPausesWithoutContinuation(String checkpoint)throws Exception{recover(checkpoint);}
}
