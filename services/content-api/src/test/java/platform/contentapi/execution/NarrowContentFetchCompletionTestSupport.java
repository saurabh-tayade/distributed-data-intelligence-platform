package platform.contentapi.execution;

import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test bridge only; persisted observations carry no execution authority. */
public final class NarrowContentFetchCompletionTestSupport {
    public record Observation(UUID operation,UUID run,UUID attempt,UUID checkpoint,String candidateSha256,
                              String completionSha256,boolean checkpointed,boolean nativeLaunchCompatible) {}
    private static Observation copy(NarrowContentFetchCompletion.Observation o){return new Observation(o.operation(),o.run(),o.attempt(),o.checkpoint(),o.candidateSha256(),o.completionSha256(),o.checkpointed(),o.nativeLaunchCompatible());}
    public static Observation complete(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,Consumer<String> fault){
        return copy(new NarrowContentFetchCompletion(jdbc,tx,workers).complete(guard,owner,operation,b->fault.accept(b.name())));
    }
    public static Observation launchAndCheckpoint(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,String profile,Consumer<String> fault){
        return copy(new NarrowContentFetchLaunch(jdbc,tx,workers).launchAndCheckpoint(guard,owner,operation,profile,b->fault.accept(b.name())));
    }
    public static Optional<Observation> inspect(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,UUID operation){
        return NarrowContentFetchCompletion.inspect(jdbc,tx,operation).map(NarrowContentFetchCompletionTestSupport::copy);
    }
    private NarrowContentFetchCompletionTestSupport(){}
}
