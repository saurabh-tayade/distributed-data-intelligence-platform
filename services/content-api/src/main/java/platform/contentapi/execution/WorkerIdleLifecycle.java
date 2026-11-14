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

/** One profile, one dispatcher thread, one token writer. Native supervisors heartbeat on that thread. */
@Component
@Conditional(WorkerIdleLifecycle.Enabled.class)
public final class WorkerIdleLifecycle implements SmartLifecycle {
    public static final int HEARTBEAT_SECONDS=5, STALE_SECONDS=30;
    static final class Enabled implements Condition {
        @Override public boolean matches(ConditionContext c,AnnotatedTypeMetadata m){return RuntimeRole.from(c.getEnvironment())==RuntimeRole.WORKER;}
    }
    private static final System.Logger LOG=System.getLogger(WorkerIdleLifecycle.class.getName());
    private WorkerRepository repository;private final ImportBindings bindings;private final Environment environment;
    private final JdbcTemplate jdbc;private final ManagementTransactions tx;private final UUID installation;
    private final Semaphore wake=new Semaphore(0);
    private volatile boolean running,stopping;private volatile String condition="NOT_STARTED";
    // Published snapshots are inspection only. Only dispatch() writes the live session after startup.
    private volatile WorkerRepository.Session session;
    private InstallationHostGuard guard;private volatile Thread dispatcher;
    private String runtimeId;private int pollMillis;private boolean content,startupUncertain;
    private NarrowContentWorkerStartup.Configuration contentStartup;
    public WorkerIdleLifecycle(JdbcTemplate jdbc,ManagementTransactions tx,DatabaseSettings settings,Environment environment) {
        this.jdbc=jdbc;this.tx=tx;this.installation=settings.installationId();this.environment=environment;
        bindings=new ImportBindings(jdbc,tx,installation);
    }
    @Override public synchronized void start() {
        if(running)return;
        require(dispatcher==null && !stopping && !startupUncertain,WORKER_NOT_READY);
        boolean registrationStarted=false;
        try {
            String profile=environment.getProperty("ddip.worker.execution-profile-id","");
            String control=environment.getProperty("ddip.worker.control-binding-id","");
            String deployment=environment.getProperty("ddip.worker.deployment-sha256","");
            runtimeId=environment.getProperty("ddip.worker.runtime-profile-id","");
            pollMillis=environment.getProperty("ddip.worker.poll-millis",Integer.class,1000);
            require(profile.matches("[A-Za-z0-9_-]{1,128}") && control.matches("[A-Za-z0-9_-]{1,128}") && runtimeId.matches("[A-Za-z0-9_-]{1,128}") && deployment.matches("[0-9a-f]{64}") && Runtime.version().feature()==21 && pollMillis>=250 && pollMillis<=5000,WORKER_CONFIGURATION_INVALID);
            guard=InstallationHostGuard.acquire(bindings.control(control,profile));
            var p=tx.required(()->jdbc.queryForList("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",profile));
            require(p.size()==1,WORKER_CONFIGURATION_INVALID);String digest=(String)p.getFirst().get("configuration_sha256");
            var config=FrozenRunSelection.profile((byte[])p.getFirst().get("configuration_bytes"),digest,deployment);
            String pipeline=config.get("pipeline").stringValue();
            require(java.util.Set.of("RELIABILITY_V2","CONTENT_RSS_V1").contains(pipeline),WORKER_CONFIGURATION_INVALID);
            content=pipeline.equals("CONTENT_RSS_V1");
            if(content){
                require(config.get("runProfileVersion").isInt() && config.get("runProfileVersion").intValue()==2,WORKER_CONFIGURATION_INVALID);
                contentStartup=NarrowContentWorkerStartup.startup(jdbc,tx,guard,profile,digest,deployment,runtimeId,()->stopping);
                // Historical loopback qualification is never an ordinary production source lane.
                require(contentStartup.compositionVersion()==2,WORKER_CONFIGURATION_INVALID);
                require(contentStartup.python().equals(environment.getProperty("ddip.worker.python",""))
                        && contentStartup.javaHome().equals(environment.getProperty("ddip.worker.java-home",""))
                        && contentStartup.processingRoot().equals(environment.getProperty("ddip.worker.processing-root","")),WORKER_CONFIGURATION_INVALID);
                contentStartup.check();
            }else{
                bindings.checkWorkerStorage(profile,"application-reliability");
                var runtime=ReliabilityRuntime.startup(jdbc,tx,guard,profile,digest,deployment,runtimeId);
                for(String key:java.util.List.of("python","javaHome","processingRoot")) {
                    String property=switch(key){case "javaHome"->"java-home";case "processingRoot"->"processing-root";default->key;};
                    require(runtime.json().get("configuration").get(key).stringValue().equals(environment.getProperty("ddip.worker."+property,"")),WORKER_CONFIGURATION_INVALID);
                }
                runtime.readiness();
            }
            require(!stopping,WORKER_NOT_READY);
            repository=new WorkerRepository(jdbc,tx,installation,content?WorkerExecutionCapability.CONTENT_NARROW_ORIGINAL:WorkerExecutionCapability.RELIABILITY_V2,()->stopping);
            registrationStarted=true;
            session=repository.register(guard,profile,digest,deployment);condition=session.status();
            if(!condition.equals("READY")) {
                // Occupied/recovery state is never adopted. Registration is historical evidence only.
                repository.stopIdle(guard,session);guard.close();guard=null;
                LOG.log(System.Logger.Level.WARNING,"WORKER_"+condition);return;
            }
            running=true;dispatcher=Thread.ofPlatform().name(content?"worker-content-dispatch":"worker-reliability-dispatch").unstarted(this::dispatch);
            dispatcher.start();LOG.log(System.Logger.Level.INFO,content?"WORKER_CONTENT_READY":"WORKER_RELIABILITY_READY");
        } catch(RuntimeException ex) {
            // A fresh repository must never erase an unknown registration/stop acknowledgment.
            if(registrationStarted){startupUncertain=true;condition="RECOVERY_REQUIRED";running=false;}
            if(guard!=null){guard.close();guard=null;}
            if(ex instanceof ManagementFailure || ex instanceof WorkerFailure || ex instanceof ImportOwnershipFailure)throw ex;
            throw new WorkerFailure(WORKER_CONFIGURATION_INVALID);
        }
    }
    private void dispatch() {
        boolean released=true;long heartbeatAt=System.nanoTime()+TimeUnit.SECONDS.toNanos(HEARTBEAT_SECONDS);
        try {
            while(!stopping) {
                if(System.nanoTime()>=heartbeatAt){session=repository.heartbeatIdle(guard,session);heartbeatAt=System.nanoTime()+TimeUnit.SECONDS.toNanos(HEARTBEAT_SECONDS);}
                if(stopping)break;
                if(content)try{contentStartup.check();}
                    catch(PreflightFailure ex){
                        // This read-only idle check precedes every claim. Stopping here
                        // can retire the acknowledged idle session, never an active owner.
                        if(stopping)break;
                        throw ex;
                    }
                // The claim acknowledgment itself can be ambiguous: never use the old session afterward.
                released=false;var claim=content?repository.claimNextNarrow(guard,session,UUID.randomUUID()):repository.claim(guard,session,UUID.randomUUID());
                if(claim.owner()==null){session=claim.worker();released=true;LOG.log(System.Logger.Level.DEBUG,"WORKER_QUEUE_IDLE");}
                else {
                    if(content){
                        new NarrowContentAdmission(jdbc,tx,repository).prepare(guard,claim.owner());
                        // CLAIMED stays occupied until the original full-chain success commit.
                        // No Content failure invokes Reliability's definitive-failure release.
                        session=new NarrowContentPipeline(jdbc,tx,repository).executeOriginal(guard,claim.owner(),runtimeId,event->{}).worker();
                        released=true;
                    }else try{
                        var prepared=new ReliabilityPreflight(jdbc,tx,repository,new ReadOnlyVerifierProcess(false,()->stopping)).verifyForExecution(guard,claim.owner(),runtimeId,UUID.randomUUID());
                        session=new ReliabilityExecution(jdbc,tx,repository,new ReadOnlyVerifierProcess(true,()->stopping),new ReadOnlyVerifierProcess(false,()->stopping)).run(guard,prepared,runtimeId).worker();released=true;
                    }catch(ReliabilityPreflight.DefinitiveFailure ex){session=ex.worker();released=true;}
                    catch(ReliabilityExecution.DefinitiveFailure ex){session=ex.worker();released=true;}
                    catch(RuntimeException ex){
                        var cancelled=repository.settleCancellation(guard,ex);
                        if(cancelled==null)throw ex;
                        session=cancelled;released=cancelled.status().equals("READY");
                        if(!released){condition="RECOVERY_REQUIRED";break;}
                    }
                    heartbeatAt=System.nanoTime()+TimeUnit.SECONDS.toNanos(HEARTBEAT_SECONDS);
                }
                if(!stopping){wake.tryAcquire(pollMillis,TimeUnit.MILLISECONDS);wake.drainPermits();}
            }
        }catch(InterruptedException ex){if(!stopping)condition="RECOVERY_REQUIRED";}
        catch(RuntimeException ex){condition="RECOVERY_REQUIRED";LOG.log(System.Logger.Level.WARNING,"WORKER_OWNERSHIP_UNCERTAIN");}
        finally {
            // All child cleanup happens on this path before releasing the cooperative guard.
            // An unconfirmed claim/stage/commit never yields an invented released token.
            Thread.interrupted();
            if(!released)condition="RECOVERY_REQUIRED";
            if(released && condition.equals("READY"))try{repository.stopIdle(guard,session);condition="STOPPED";}
                catch(RuntimeException ex){condition="RECOVERY_REQUIRED";LOG.log(System.Logger.Level.WARNING,"WORKER_STOP_UNCERTAIN");}
            guard.close();guard=null;running=false;
            LOG.log(condition.equals("STOPPED")?System.Logger.Level.INFO:System.Logger.Level.WARNING,"WORKER_"+condition);
        }
    }
    /** Wake the single owner; never perform heartbeat/claim on the caller's thread. */
    public void tick(){if(running && wake.availablePermits()==0)wake.release();}
    @Override public void stop() {
        Thread thread;
        stopping=true;
        synchronized(this){thread=dispatcher;if(thread==null)return;wake.release();thread.interrupt();}
        if(thread==Thread.currentThread())return;
        try{thread.join(TimeUnit.SECONDS.toMillis(45));}
        catch(InterruptedException ex){Thread.currentThread().interrupt();}
        if(thread.isAlive()) {
            // Do not close a guard still used by a supervisor. Its thread owns final cleanup.
            condition="RECOVERY_REQUIRED";LOG.log(System.Logger.Level.WARNING,"WORKER_STOP_UNCERTAIN");
        }
    }
    public String condition(){return condition;}
    public boolean executionAvailable(){return running && !stopping && condition.equals("READY") && repository!=null && repository.executionAvailable();}
    public WorkerRepository.Session session(){return session;}
    @Override public boolean isRunning(){return running;}
}
