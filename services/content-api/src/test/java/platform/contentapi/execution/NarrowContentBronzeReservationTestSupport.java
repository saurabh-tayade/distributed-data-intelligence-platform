package platform.contentapi.execution;

import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test bridge only; no production entry point or owner reconstruction. */
public final class NarrowContentBronzeReservationTestSupport {
    public record Facts(UUID operation,UUID fetchOperation,UUID attempt,UUID checkpoint,String commandSha256,String reservationSha256,boolean nativeLaunchCompatible){}
    private static Facts copy(NarrowContentBronzeReservation.Facts f){return new Facts(f.operation(),f.fetchOperation(),f.attempt(),f.checkpoint(),f.commandSha256(),f.reservationSha256(),f.nativeLaunchCompatible());}
    public static Facts reserve(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetch,UUID operation,Consumer<String> fault){return copy(new NarrowContentBronzeReservation(jdbc,tx,workers).reserve(guard,owner,fetch,operation,b->fault.accept(b.name())));}
    public static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,UUID operation){return copy(NarrowContentBronzeReservation.inspect(jdbc,tx,operation));}
    public static Facts launchCheckpointAndReserve(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,UUID fetch,String profile,UUID operation,Consumer<String> fault){return copy(new NarrowContentFetchLaunch(jdbc,tx,workers).launchCheckpointAndReserveBronze(guard,owner,fetch,profile,operation,fault==null?null:b->fault.accept(b.name())));}
    private NarrowContentBronzeReservationTestSupport(){}
}
