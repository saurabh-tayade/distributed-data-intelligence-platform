package platform.contentapi.execution;

import java.time.*;
import java.util.*;
import java.util.function.*;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test bridge only. Production callers cannot deserialize a qualification or an execution owner. */
public final class NarrowContentTerminalTestSupport {
    private final NarrowContentTerminal repository;
    public static final class Target {
        private final NarrowContentTerminal.Target value;
        private Target(NarrowContentTerminal.Target value){this.value=value;}
        public String digest(){return value.digest();}
    }
    public static final class Qualified {
        private final NarrowContentTerminal.Qualified value;
        private Qualified(NarrowContentTerminal.Qualified value){this.value=value;}
    }
    public NarrowContentTerminalTestSupport(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation){repository=new NarrowContentTerminal(jdbc,tx,installation);}
    public NarrowContentTerminalTestSupport(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,Supplier<String> state,Consumer<String> boundary){
        repository=new NarrowContentTerminal(jdbc,tx,installation,clock,p->{if(org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive())throw new AssertionError("PROCESS_IO_IN_TRANSACTION");return RecoveryQuiescence.State.valueOf(state.get());},b->boundary.accept(b.name()));
    }
    public Target inspect(InstallationHostGuard guard,UUID run,UUID attempt,UUID control,UUID terminal,UUID id){return new Target(repository.inspect(guard,run,attempt,control,terminal,id));}
    public Qualified qualify(InstallationHostGuard guard,Target target,Instant at,String omitted){
        var t=target.value;var r=new RecoveryQuiescence.Review(omitted.equals("digest")?"f".repeat(64):t.digest(),at,!omitted.equals("handlers"),!omitted.equals("unmanaged"),!omitted.equals("orphans"),omitted.equals("checkpoints")?Set.of(UUID.randomUUID()):t.checkpoints(),List.of());
        return new Qualified(repository.qualify(guard,t,new NarrowContentTerminal.Review(r,omitted.equals("nonce")?UUID.randomUUID():t.nonce(),!omitted.equals("namespace"),!omitted.equals("descendants"),!omitted.equals("gaps"),!omitted.equals("gateways"),!omitted.equals("remote"))));
    }
    public Qualified qualifyWithOldReview(InstallationHostGuard guard,Target target,Target old,Instant at){
        var r=new RecoveryQuiescence.Review(old.value.digest(),at,true,true,true,old.value.checkpoints(),List.of());
        return new Qualified(repository.qualify(guard,target.value,new NarrowContentTerminal.Review(r,old.value.nonce(),true,true,true,true,true)));
    }
    public String finish(InstallationHostGuard guard,Qualified q){return repository.finish(guard,q.value).status().name();}
    public String resolve(Qualified q){return repository.resolve(q.value).status().name();}
    public String lookup(UUID id){return repository.lookup(id).status().name();}
}
