package platform.contentapi.execution;

import java.time.*;
import java.util.*;
import java.util.function.*;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-only bridge. Production tokens stay opaque and cannot be reconstructed from receipts. */
public final class NarrowContentFetchRecoveryHandoffTestSupport {
    private final NarrowContentFetchRecoveryHandoff repository;
    public static final class Target {
        private final NarrowContentFetchRecoveryHandoff.Target value;
        private Target(NarrowContentFetchRecoveryHandoff.Target value){this.value=value;}
        public String digest(){return value.digest();}
        public UUID checkpoint(){return value.checkpoint();}
    }
    public static final class Qualified {
        private final NarrowContentFetchRecoveryHandoff.Qualified value;
        private Qualified(NarrowContentFetchRecoveryHandoff.Qualified value){this.value=value;}
    }
    public record Result(String status,boolean ownerPresent) {}
    public record ProcessIdentity(long pid,Instant startedAt) {}
    public NarrowContentFetchRecoveryHandoffTestSupport(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,Supplier<String> state,Consumer<String> boundary){
        this(jdbc,tx,installation,clock,p->state.get(),boundary);
    }
    public NarrowContentFetchRecoveryHandoffTestSupport(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,Function<ProcessIdentity,String> state,Consumer<String> boundary){
        repository=new NarrowContentFetchRecoveryHandoff(jdbc,tx,installation,clock,p->{String s=state.apply(new ProcessIdentity(p.pid(),p.startedAt()));if(s.equals("THROWS"))throw new IllegalStateException("PRIVATE_PROCESS_FAILURE");return RecoveryQuiescence.State.valueOf(s);},b->boundary.accept(b.name()));
    }
    public Target inspect(InstallationHostGuard guard,WorkerRepository.Session recovery,UUID control,UUID successor,UUID handoff){return new Target(repository.inspect(guard,recovery,control,successor,handoff));}
    public Qualified qualify(InstallationHostGuard guard,Target t,Instant at,String omitted){
        return qualify(guard,t,at,omitted,List.of());
    }
    public Qualified qualify(InstallationHostGuard guard,Target t,Instant at,String omitted,List<ProcessIdentity> descendants){
        var review=new RecoveryQuiescence.Review(omitted.equals("digest")?"f".repeat(64):t.digest(),at,!omitted.equals("handlers"),!omitted.equals("unmanaged"),!omitted.equals("orphans"),omitted.equals("checkpoint")?Set.of():Set.of(t.checkpoint()),descendants.stream().map(p->new RecoveryQuiescence.Identity(p.pid(),p.startedAt())).toList());
        return new Qualified(repository.qualify(guard,t.value,new NarrowContentFetchRecoveryHandoff.Review(review,!omitted.equals("descendants"),!omitted.equals("gaps"))));
    }
    public Result transfer(InstallationHostGuard guard,Qualified q){var r=repository.transfer(guard,q.value);return new Result(r.receipt().status().name(),r.owner()!=null);}
    public String resolve(Qualified q){return repository.resolve(q.value).status().name();}
    public String lookup(UUID handoff){return repository.lookup(handoff).status().name();}
    public static List<String> publicSeams(){return Arrays.stream(NarrowContentFetchRecoveryHandoff.class.getDeclaredMethods()).filter(m->!java.lang.reflect.Modifier.isPrivate(m.getModifiers())).map(java.lang.reflect.Method::getName).distinct().sorted().toList();}
}
