package platform.contentapi.execution;

import java.nio.file.Path;
import java.time.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;

/** Disposable crash-test JVM. Synthetic process observations are not operational quiescence proof. */
public final class NarrowContentFetchCompositionTestProcess {
    public static void main(String[] args)throws Exception{
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(args[0],args[1],args[2]));jdbc.setQueryTimeout(3);
        var tx=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())));
        var binding=OwnershipFixtures.readBinding(Path.of(args[3]));
        try(var guard=InstallationHostGuard.acquire(binding)){
            var recovery=new WorkerRepository(jdbc,tx,binding.installationId()).register(guard,args[4],args[5],args[6]);
            var composition=new NarrowContentFetchRecoveryComposition(jdbc,tx,binding.installationId(),Clock.systemUTC(),()->"ABSENT",boundary->{
                if(boundary.equals(args[13])){
                    System.out.println("COMPOSITION_CRASH "+boundary);System.out.flush();Runtime.getRuntime().halt(73);
                }
            });
            java.util.function.Function<NarrowContentFetchRecoveryComposition.Target,NarrowContentFetchRecoveryComposition.Review> review=t->
                    new NarrowContentFetchRecoveryComposition.Review(t.digest(),Instant.now(),true,true,true,Set.of(t.checkpoint()),List.of(),true,true);
            composition.start(guard,UUID.fromString(args[7]),UUID.fromString(args[8]),recovery,UUID.fromString(args[9]),UUID.fromString(args[10]),UUID.fromString(args[11]),UUID.fromString(args[12]),review);
            composition.finish(guard,review);
            throw new AssertionError("CRASH_BOUNDARY_NOT_REACHED");
        }
    }
    private NarrowContentFetchCompositionTestProcess(){}
}
