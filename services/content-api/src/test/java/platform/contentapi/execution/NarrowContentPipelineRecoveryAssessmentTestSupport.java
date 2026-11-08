package platform.contentapi.execution;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
/** Test bridge exposes detached assessments only, never execution handles. */
public final class NarrowContentPipelineRecoveryAssessmentTestSupport {
    public record Assessment(String prefix,int operationCount,List<Integer> checkpointSteps,String integrity,int recordedProcessCount,
                             boolean authorization,boolean quiescenceEstablished,boolean nativeLaunchCompatible,boolean historicalFilesystemIdentityEstablished) {}
    public static Assessment inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt){
        var a=NarrowContentPipelineRecoveryAssessment.inspect(jdbc,tx,guard,run,attempt);
        return new Assessment(a.prefix().name(),a.operations().size(),a.operations().stream().filter(NarrowContentPipelineRecoveryAssessment.OperationFact::checkpoint).map(NarrowContentPipelineRecoveryAssessment.OperationFact::stepIndex).toList(),a.integrity().name(),a.recordedProcesses().size(),a.authorization(),a.quiescenceEstablished(),a.nativeLaunchCompatible(),a.historicalFilesystemIdentityEstablished());
    }
    private NarrowContentPipelineRecoveryAssessmentTestSupport(){}
}
