package platform.contentapi.execution;

import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-only bridge. No dispatcher or recovery continuation is installed. */
public final class NarrowContentFetchLaunchTestSupport {
    public record Facts(UUID operation,UUID attempt,List<String> phases,InstallationHostGuard.ProcessIdentity child,
                        String launchProfileId,String launchProfileSha256,String candidateSha256,
                        boolean candidateForVerification,boolean completed,boolean nativeLaunchCompatible) {}
    private static Facts facts(NarrowContentFetchLaunch.Facts f){return new Facts(f.operation(),f.attempt(),f.phases(),f.child(),f.launchProfileId(),f.launchProfileSha256(),f.candidateSha256(),f.candidateForVerification(),f.completed(),f.nativeLaunchCompatible());}
    public static Facts launch(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String profile,Consumer<String> boundary){
        return facts(new NarrowContentFetchLaunch(jdbc,tx,workers).launch(guard,owner,operation,profile,b->boundary.accept(b.name())));
    }
    public static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,UUID operation){return facts(new NarrowContentFetchLaunch(jdbc,tx,workers).inspect(operation));}
    private NarrowContentFetchLaunchTestSupport(){}
}
