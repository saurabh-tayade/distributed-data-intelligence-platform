package platform.contentapi.execution;

import java.time.Instant;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-only detached bridge; it deliberately exposes no continuation or owner. */
public final class NarrowContentFetchRecoveryAssessmentTestSupport {
    public record Durable(UUID run,UUID attempt,UUID operation,UUID checkpoint,String prefix,List<String> admissionPhases,List<String> launchPhases,String admissionSha256,String commandSha256,String candidateSha256,String completionSha256) {}
    public record ProcessFact(String role,long pid,Instant startedAt,String state) {}
    public record Assessment(Durable durable,String integrity,List<ProcessFact> recordedProcesses,List<String> reviewTargets,boolean authorization,boolean quiescenceEstablished,boolean nativeLaunchCompatible) {}
    public static Assessment inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt){
        return copy(NarrowContentFetchRecoveryAssessment.inspect(jdbc,tx,guard,run,attempt));
    }
    public static Assessment inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID run,UUID attempt,String state,Consumer<String> boundary){
        return copy(NarrowContentFetchRecoveryAssessment.inspect(jdbc,tx,guard,run,attempt,p->{if(state.equals("THROWS"))throw new IllegalStateException("PRIVATE_PROCESS_FAILURE");return RecoveryQuiescence.State.valueOf(state);},b->boundary.accept(b.name())));
    }
    private static Assessment copy(NarrowContentFetchRecoveryAssessment.Assessment a){
        var d=a.durable();
        return new Assessment(new Durable(d.run(),d.attempt(),d.operation(),d.checkpoint(),d.prefix().name(),d.admissionPhases(),d.launchPhases(),d.admissionSha256(),d.commandSha256(),d.candidateSha256(),d.completionSha256()),a.integrity().name(),a.recordedProcesses().stream().map(p->new ProcessFact(p.role().name(),p.pid(),p.startedAt(),p.state().name())).toList(),a.reviewTargets().stream().map(Enum::name).toList(),a.authorization(),a.quiescenceEstablished(),a.nativeLaunchCompatible());
    }
    private NarrowContentFetchRecoveryAssessmentTestSupport(){}
}
