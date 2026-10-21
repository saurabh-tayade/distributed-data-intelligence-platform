package platform.contentapi.execution;

import java.util.concurrent.CountDownLatch;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Conditional;
import org.springframework.context.annotation.Condition;
import org.springframework.context.annotation.ConditionContext;
import org.springframework.core.type.AnnotatedTypeMetadata;
import org.springframework.stereotype.Component;

/** B1 keeps a non-web worker alive, but has no polling, claim, host lock or execution loop. */
@Component
@Conditional(WorkerIdleLifecycle.Enabled.class)
public final class WorkerIdleLifecycle implements SmartLifecycle {
    static final class Enabled implements Condition {
        @Override public boolean matches(ConditionContext c,AnnotatedTypeMetadata m) { return RuntimeRole.from(c.getEnvironment())==RuntimeRole.WORKER; }
    }
    private volatile boolean running;
    private CountDownLatch stopped;
    private Thread thread;
    @Override public synchronized void start() {
        stopped=new CountDownLatch(1); running=true;
        thread=Thread.ofPlatform().name("worker-b1-idle").unstarted(() -> {
            try { stopped.await(); } catch(InterruptedException ex) { Thread.currentThread().interrupt(); }
        });
        thread.start();
    }
    @Override public synchronized void stop() {
        running=false; stopped.countDown();
        try { thread.join(1000); } catch(InterruptedException ex) { Thread.currentThread().interrupt(); }
    }
    @Override public boolean isRunning() { return running; }
}
