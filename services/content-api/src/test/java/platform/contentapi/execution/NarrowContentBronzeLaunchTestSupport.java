package platform.contentapi.execution;

import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-only bridge; no dispatcher or restart continuation. */
public final class NarrowContentBronzeLaunchTestSupport {
    public record Facts(UUID operation,UUID fetchOperation,UUID attempt,List<String> phases,InstallationHostGuard.ProcessIdentity child,
                        String launchProfileId,String launchProfileSha256,String candidateSha256,String storageState,Integer exitCode,
                        boolean candidateForVerification,boolean completed,boolean nativeLaunchCompatible) {}
    private static Facts facts(NarrowContentBronzeLaunch.Facts f){return new Facts(f.operation(),f.fetchOperation(),f.attempt(),f.phases(),f.child(),f.launchProfileId(),f.launchProfileSha256(),f.candidateSha256(),f.storageState(),f.exitCode(),f.candidateForVerification(),f.completed(),f.nativeLaunchCompatible());}
    public static Facts launch(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetch,UUID operation,String profile,Consumer<String> boundary){
        return facts(new NarrowContentBronzeLaunch(jdbc,tx,workers).reserveAndLaunch(guard,owner,fetch,operation,profile,boundary==null?null:b->boundary.accept(b.name())));
    }
    public static Facts chain(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetch,String fetchProfile,UUID operation,String profile,Consumer<String> boundary){
        return facts(new NarrowContentFetchLaunch(jdbc,tx,workers).launchCheckpointAndLaunchBronze(guard,owner,fetch,fetchProfile,operation,profile,boundary==null?null:b->boundary.accept(b.name())));
    }
    public static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,UUID operation){return facts(new NarrowContentBronzeLaunch(jdbc,tx,workers).inspect(operation));}
    private NarrowContentBronzeLaunchTestSupport(){}
}
