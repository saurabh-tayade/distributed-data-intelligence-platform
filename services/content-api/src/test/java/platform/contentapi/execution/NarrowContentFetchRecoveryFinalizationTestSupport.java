package platform.contentapi.execution;

import java.time.*;
import java.util.*;
import java.util.function.*;
import org.springframework.jdbc.core.JdbcTemplate;

/** Test-only bridge to the inactive terminal seam. No owner constructor or restart reauthorization. */
public final class NarrowContentFetchRecoveryFinalizationTestSupport {
    private final NarrowContentFetchRecoveryFinalization repository;
    public static final class Target {
        final NarrowContentFetchRecoveryFinalization.Target value;
        Target(NarrowContentFetchRecoveryFinalization.Target value){this.value=value;}
        public String digest(){return value.digest();}
        public UUID checkpoint(){return value.checkpoint();}
    }
    public static final class Qualified {
        final NarrowContentFetchRecoveryFinalization.Qualified value;
        Qualified(NarrowContentFetchRecoveryFinalization.Qualified value){this.value=value;}
    }
    public NarrowContentFetchRecoveryFinalizationTestSupport(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,Supplier<String> state,Consumer<String> boundary){this(jdbc,tx,installation,clock,p->state.get(),boundary);}
    public NarrowContentFetchRecoveryFinalizationTestSupport(JdbcTemplate jdbc,ManagementTransactions tx,UUID installation,Clock clock,Function<NarrowContentFetchRecoveryHandoffTestSupport.ProcessIdentity,String> state,Consumer<String> boundary){
        repository=new NarrowContentFetchRecoveryFinalization(jdbc,tx,installation,clock,p->{String s=state.apply(new NarrowContentFetchRecoveryHandoffTestSupport.ProcessIdentity(p.pid(),p.startedAt()));if(s.equals("THROWS"))throw new IllegalStateException("PRIVATE_PROCESS_FAILURE");return RecoveryQuiescence.State.valueOf(s);},b->boundary.accept(b.name()));
    }
    public Target inspect(InstallationHostGuard guard,NarrowContentFetchRecoveryHandoffTestSupport.Owner owner,UUID id){return new Target(repository.inspect(guard,owner==null?null:owner.value,id));}
    public Qualified qualify(InstallationHostGuard guard,Target t,Instant at,String omitted){return qualify(guard,t,at,omitted,List.of());}
    public Qualified qualify(InstallationHostGuard guard,Target t,Instant at,String omitted,List<NarrowContentFetchRecoveryHandoffTestSupport.ProcessIdentity> descendants){
        var review=new RecoveryQuiescence.Review(omitted.equals("digest")?"f".repeat(64):t.digest(),at,!omitted.equals("handlers"),!omitted.equals("unmanaged"),!omitted.equals("orphans"),omitted.equals("checkpoint")?Set.of():Set.of(t.checkpoint()),descendants.stream().map(p->new RecoveryQuiescence.Identity(p.pid(),p.startedAt())).toList());
        return new Qualified(repository.qualify(guard,t.value,new NarrowContentFetchRecoveryHandoff.Review(review,!omitted.equals("descendants"),!omitted.equals("gaps"))));
    }
    public String finish(InstallationHostGuard guard,Qualified q){return repository.finish(guard,q.value).status().name();}
    public String resolve(Qualified q){return repository.resolve(q.value).status().name();}
    public String lookup(UUID id){return repository.lookup(id).status().name();}
    public static List<String> publicSeams(){return Arrays.stream(NarrowContentFetchRecoveryFinalization.class.getDeclaredMethods()).filter(m->!java.lang.reflect.Modifier.isPrivate(m.getModifiers())).map(java.lang.reflect.Method::getName).distinct().sorted().toList();}
}
