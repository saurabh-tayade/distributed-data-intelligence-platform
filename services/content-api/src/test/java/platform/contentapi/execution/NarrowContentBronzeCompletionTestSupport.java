package platform.contentapi.execution;

import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-only access to the uninterrupted launch path and read-only checkpoint inspection. */
public final class NarrowContentBronzeCompletionTestSupport {
    public record Observation(UUID operation,UUID run,UUID attempt,UUID checkpoint,UUID fetchOperation,
                              String candidateSha256,String receiptSha256,String verifiedSha256,
                              boolean checkpointed,boolean nativeLaunchCompatible) {}
    private static Observation observation(NarrowContentBronzeCompletion.Observation o){
        return new Observation(o.operation(),o.run(),o.attempt(),o.checkpoint(),o.fetchOperation(),o.candidateSha256(),o.receiptSha256(),o.verifiedSha256(),o.checkpointed(),o.nativeLaunchCompatible());
    }
    public static Observation launch(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetch,UUID operation,String launchProfile,String verificationProfile,Consumer<String> fault){
        return observation(new NarrowContentBronzeLaunch(jdbc,tx,workers).reserveLaunchAndCheckpoint(guard,owner,fetch,operation,launchProfile,verificationProfile,fault==null?null:b->fault.accept(b.name())));
    }
    public static Observation chain(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetch,String fetchProfile,UUID operation,String launchProfile,String verificationProfile,Consumer<String> fault){
        return observation(new NarrowContentFetchLaunch(jdbc,tx,workers).launchCheckpointAndCompleteBronze(guard,owner,fetch,fetchProfile,operation,launchProfile,verificationProfile,fault==null?null:b->fault.accept(b.name())));
    }
    public static Optional<Observation> inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){return NarrowContentBronzeCompletion.inspect(jdbc,tx,operation).map(NarrowContentBronzeCompletionTestSupport::observation);}
    private NarrowContentBronzeCompletionTestSupport(){}
}
