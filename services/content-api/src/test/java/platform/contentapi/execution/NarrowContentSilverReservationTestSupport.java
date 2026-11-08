package platform.contentapi.execution;

import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-only entry to the uninterrupted chain; no historical Owner or checkpoint adoption seam. */
public final class NarrowContentSilverReservationTestSupport {
    public record Facts(UUID operation,UUID bronzeOperation,UUID attempt,UUID checkpoint,UUID requiredInspectionCheckpoint,
                        String commandSha256,String reservationSha256,boolean nativeLaunchCompatible,boolean requiresBronzeInspection) {}
    private static Facts facts(NarrowContentSilverReservation.Facts f){return new Facts(f.operation(),f.bronzeOperation(),f.attempt(),f.checkpoint(),f.requiredInspectionCheckpoint(),f.commandSha256(),f.reservationSha256(),f.nativeLaunchCompatible(),f.requiresBronzeInspection());}
    public static Facts chain(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,
            UUID fetch,String fetchProfile,UUID bronze,String bronzeProfile,String verificationProfile,UUID silver,Consumer<String> completionFault,Consumer<String> fault){
        return facts(new NarrowContentFetchLaunch(jdbc,tx,workers).launchCheckpointAndReserveSilver(guard,owner,fetch,fetchProfile,bronze,bronzeProfile,verificationProfile,silver,
                completionFault==null?null:b->completionFault.accept(b.name()),fault==null?null:b->fault.accept(b.name())));
    }
    public static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){return facts(NarrowContentSilverReservation.inspect(jdbc,tx,operation));}
    public static boolean privateOneUseBridge(){
        var bridge=NarrowContentBronzeCompletion.LiveCheckpoint.class;
        return Arrays.stream(bridge.getDeclaredConstructors()).allMatch(c->java.lang.reflect.Modifier.isPrivate(c.getModifiers()));
    }
    private NarrowContentSilverReservationTestSupport(){}
}
