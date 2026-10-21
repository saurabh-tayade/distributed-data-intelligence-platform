package platform.contentapi.execution;

import java.util.UUID;
import java.util.concurrent.*;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.*;
import org.springframework.core.env.Environment;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;
import platform.contentapi.importing.ImportBindings;
import platform.contentapi.management.*;
import static platform.contentapi.execution.WorkerFailure.Code.*;
import static platform.contentapi.execution.WorkerFailure.require;

/** Passive C1 worker. Retains the installation guard; has NO production claim/dispatch loop. */
@Component
@Conditional(WorkerIdleLifecycle.Enabled.class)
public final class WorkerIdleLifecycle implements SmartLifecycle {
    public static final int HEARTBEAT_SECONDS=5, STALE_SECONDS=30;
    static final class Enabled implements Condition {
        @Override public boolean matches(ConditionContext c,AnnotatedTypeMetadata m){return RuntimeRole.from(c.getEnvironment())==RuntimeRole.WORKER;}
    }
    private static final System.Logger LOG=System.getLogger(WorkerIdleLifecycle.class.getName());
    private final WorkerRepository repository;private final ImportBindings bindings;private final Environment environment;
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    private volatile boolean running;private volatile String condition="NOT_STARTED";
    private InstallationHostGuard guard;private WorkerRepository.Session session;private ScheduledExecutorService heartbeat;
    public WorkerIdleLifecycle(JdbcTemplate jdbc,ManagementTransactions tx,DatabaseSettings settings,Environment environment) {
        this.jdbc=jdbc;this.tx=tx;this.installation=settings.installationId();this.environment=environment;
        repository=new WorkerRepository(jdbc,tx,installation);bindings=new ImportBindings(jdbc,tx,installation);
    }
    @Override public synchronized void start() {
        if(running)return;
        try {
            String profile=environment.getProperty("ddip.worker.execution-profile-id","");
            String control=environment.getProperty("ddip.worker.control-binding-id","");
            String deployment=environment.getProperty("ddip.worker.deployment-sha256","");
            require(profile.matches("[A-Za-z0-9_-]{1,128}") && control.matches("[A-Za-z0-9_-]{1,128}") && deployment.matches("[0-9a-f]{64}") && Runtime.version().feature()==21,WORKER_CONFIGURATION_INVALID);
            // Resolve the immutable binding, then hold its existing guard for the entire lifetime.
            guard=InstallationHostGuard.acquire(bindings.control(control,profile));
            var p=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",profile));
            require(p.size()==1,WORKER_CONFIGURATION_INVALID);
            String digest=(String)p.getFirst().get("configuration_sha256");
            var config=FrozenRunSelection.profile((byte[])p.getFirst().get("configuration_bytes"),digest,deployment);
            bindings.checkWorkerStorage(profile,config.get("pipeline").stringValue().equals("CONTENT_RSS_V1")?"content-intelligence":"application-reliability");
            session=repository.register(guard,profile,digest,deployment);condition=session.status();
            if(condition.equals("STOPPING")) {
                repository.stopIdle(guard,session);guard.close();guard=null;
                LOG.log(System.Logger.Level.INFO,"WORKER_STOPPING");return;
            }
            heartbeat=Executors.newSingleThreadScheduledExecutor(r->Thread.ofPlatform().name("worker-c1-heartbeat").unstarted(r));
            running=true;
            heartbeat.scheduleWithFixedDelay(this::tick,HEARTBEAT_SECONDS,HEARTBEAT_SECONDS,TimeUnit.SECONDS);
            LOG.log(System.Logger.Level.INFO,"WORKER_PASSIVE_NO_EXECUTOR");
        } catch(RuntimeException ex) {
            if(guard!=null){guard.close();guard=null;}
            // Never leak a configuration path, source descriptor, JDBC URL or native exception.
            if(ex instanceof ManagementFailure || ex instanceof WorkerFailure || ex instanceof ImportOwnershipFailure)throw ex;
            throw new WorkerFailure(WORKER_CONFIGURATION_INVALID);
        }
    }
    /** Also used by deterministic lifecycle tests; no sleep-based heartbeat assertions. */
    public synchronized void tick() {
        if(!running || !condition.equals(session.status()))return;
        try{session=repository.heartbeatIdle(guard,session);}
        catch(RuntimeException ex){condition="RECOVERY_REQUIRED";LOG.log(System.Logger.Level.WARNING,"WORKER_HEARTBEAT_UNCERTAIN");}
    }
    @Override public synchronized void stop() {
        if(!running)return;running=false;
        heartbeat.shutdownNow(); // Only the short heartbeat operation runs here; no processing children exist in C1.
        try {
            if(condition.equals(session.status()))repository.stopIdle(guard,session);
            else LOG.log(System.Logger.Level.WARNING,"WORKER_STOP_UNCERTAIN");
        } catch(RuntimeException ex){condition="RECOVERY_REQUIRED";LOG.log(System.Logger.Level.WARNING,"WORKER_STOP_UNCERTAIN");}
        finally {guard.close();guard=null;}
    }
    public String condition(){return condition;}
    public boolean executionAvailable(){return repository.executionAvailable();}
    public synchronized WorkerRepository.Session session(){return session;}
    @Override public boolean isRunning(){return running;}
}
