package platform.contentapi.execution;

import java.util.UUID;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test bridge only. Verification returns no execution owner or continuation. */
public final class NarrowContentFetchCandidateVerificationTestSupport {
    public record Observation(UUID operation,UUID run,UUID attempt,UUID checkpoint,String candidateSha256,long payloadBytes,
                              boolean rawHttpJournalVerified,boolean completed,boolean nativeLaunchCompatible) {}
    public static Observation inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID operation){
        return inspect(jdbc,tx,guard,operation,b->{});
    }
    public static Observation inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID operation,Consumer<String> fault){
        var o=NarrowContentFetchCandidateVerification.inspect(jdbc,tx,guard,operation,b->fault.accept(b.name()));
        return new Observation(o.operation(),o.run(),o.attempt(),o.checkpoint(),o.candidateSha256(),o.payloadBytes(),o.rawHttpJournalVerified(),o.completed(),o.nativeLaunchCompatible());
    }
    private NarrowContentFetchCandidateVerificationTestSupport(){}
}
