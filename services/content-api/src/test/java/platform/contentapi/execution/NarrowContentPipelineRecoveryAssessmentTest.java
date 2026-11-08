package platform.contentapi.execution;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.*;
class NarrowContentPipelineRecoveryAssessmentTest {
    @Test void detachedFactsAreImmutableAndNeverGrantAuthority(){
        var phases=new ArrayList<String>();phases.add("LAUNCH_BOUND");var fact=new NarrowContentPipelineRecoveryAssessment.OperationFact(2,UUID.randomUUID(),"INSPECT_BRONZE",phases,false,"a".repeat(64),null);phases.clear();
        var facts=new ArrayList<NarrowContentPipelineRecoveryAssessment.OperationFact>();facts.add(fact);
        var a=new NarrowContentPipelineRecoveryAssessment.Assessment(UUID.randomUUID(),UUID.randomUUID(),NarrowContentPipelineRecoveryAssessment.Prefix.OPERATION_INCOMPLETE,facts,NarrowContentPipelineRecoveryAssessment.Integrity.NOT_REQUESTED,List.of(),List.of(),List.of());facts.clear();
        assertThat(a.operations()).hasSize(1);assertThat(fact.phases()).containsExactly("LAUNCH_BOUND");assertThat(a.authorization()).isFalse();assertThat(a.quiescenceEstablished()).isFalse();assertThat(a.nativeLaunchCompatible()).isFalse();assertThat(a.historicalFilesystemIdentityEstablished()).isFalse();
    }
}
