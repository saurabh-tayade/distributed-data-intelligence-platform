package platform.contentapi.execution;

import java.io.*;
import java.nio.file.Path;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static platform.contentapi.execution.PreflightFailure.Code.*;

/** Fixed native child supervision. Default read-only mode refuses every observed descendant.
 * Explicit Spark mode permits bounded descendants only when all are gone before clean completion;
 * the Reliability adapter also requires the fixed wrapper's reaped-gateway identity.
 * No analytics timeout, persistent server, shell, queue or publication implementation. */
class ReadOnlyVerifierProcess {
    interface Observer {
        void beforeStart();
        void started(InstallationHostGuard.ProcessIdentity child);
        void heartbeat();
    }
    /** Optional bounded startup lane; ordinary native launches still receive immediate stdin EOF. */
    interface BootstrapObserver extends Observer {
        byte[] release(InstallationHostGuard.ProcessIdentity child);
        default void beforeDelivery(){}
    }
    record Result(int exitCode,InstallationHostGuard.ProcessIdentity child,long stdoutBytes,long stderrBytes,Set<String> descendants,String stderrSha256,byte[] stdout) {
        Result {stdout=stdout==null?null:stdout.clone();}
        @Override public byte[] stdout(){return stdout==null?null:stdout.clone();}
        Result(int exitCode,InstallationHostGuard.ProcessIdentity child,long stdoutBytes,long stderrBytes,Set<String> descendants,String stderrSha256){this(exitCode,child,stdoutBytes,stderrBytes,descendants,stderrSha256,null);}
        // Historical synthetic observations retain no invented diagnostic digest.
        Result(int exitCode,InstallationHostGuard.ProcessIdentity child,long stdoutBytes,long stderrBytes,Set<String> descendants){this(exitCode,child,stdoutBytes,stderrBytes,descendants,null);}
    }
    private record Observed(ProcessHandle handle,Instant start) { }
    private java.util.function.BooleanSupplier stopping=()->false;
    private boolean captureStdout;
    /** Opt-in bounded protocol output. Ordinary launchers continue discarding all raw output. */
    static ReadOnlyVerifierProcess receiptReader(){var process=new ReadOnlyVerifierProcess(65536,Duration.ofSeconds(5),Duration.ofSeconds(5),Duration.ofSeconds(5));process.captureStdout=true;return process;}
    private final boolean spark;private final int outputLimit;private final Duration graceful,forced,drain;
    ReadOnlyVerifierProcess(){this(false);}
    ReadOnlyVerifierProcess(boolean spark){this(spark,16*1024*1024,Duration.ofSeconds(5),Duration.ofSeconds(5),Duration.ofSeconds(5));}
    ReadOnlyVerifierProcess(boolean spark,java.util.function.BooleanSupplier stopping){this(spark);this.stopping=Objects.requireNonNull(stopping);}
    /** Narrow test constructor; production caps cannot be raised by a profile or environment. */
    ReadOnlyVerifierProcess(int limit,Duration graceful,Duration forced,Duration drain) {
        this(false,limit,graceful,forced,drain);
    }
    private ReadOnlyVerifierProcess(boolean spark,int limit,Duration graceful,Duration forced,Duration drain) {
        this.spark=spark;
        PreflightFailure.need(limit>0 && limit<=16*1024*1024 && List.of(graceful,forced,drain).stream().allMatch(d->!d.isNegative() && d.compareTo(Duration.ofSeconds(5))<=0),RUNTIME_MISMATCH);
        this.outputLimit=limit;this.graceful=graceful;this.forced=forced;this.drain=drain;
    }
    Result run(List<String> command,Map<String,String> environment,Path directory,Duration timeout,Observer observer) {
        PreflightFailure.need(!timeout.isNegative() && !timeout.isZero() && timeout.compareTo(Duration.ofMinutes(spark?30:15))<=0,RUNTIME_MISMATCH);
        Process p=null;Drain out=null,err=null;InstallationHostGuard.ProcessIdentity identity=null;boolean uncertain=false,interrupted=false;
        RuntimeException failure=null;Result result=null;Map<Long,Observed> observed=new HashMap<>();Thread[] bootstrapWriter={null};
        long deadline=System.nanoTime()+timeout.toNanos();
        try {
            if(stopping.getAsBoolean() || Thread.currentThread().isInterrupted())throw new InterruptedException();
            var builder=new ProcessBuilder(command).directory(directory.toFile());builder.environment().clear();builder.environment().putAll(environment);
            observer.beforeStart(); // Last fenced transaction. Its failure must precede ProcessBuilder.start().
            if(stopping.getAsBoolean() || Thread.currentThread().isInterrupted())throw new InterruptedException();
            if(System.nanoTime()>=deadline)throw new PreflightFailure(STAGE_TIMEOUT);
            p=builder.start();if(!(observer instanceof BootstrapObserver))p.getOutputStream().close();
            out=new Drain(p.getInputStream(),outputLimit,captureStdout);err=new Drain(p.getErrorStream(),outputLimit);out.start();err.start();
            Instant start=p.info().startInstant().orElseThrow(()->new PreflightFailure(TERMINATION_UNCONFIRMED,false));
            identity=new InstallationHostGuard.ProcessIdentity(UUID.randomUUID(),p.pid(),start.truncatedTo(java.time.temporal.ChronoUnit.MICROS));
            observer.started(identity);
            if(observer instanceof BootstrapObserver bootstrap){
                // The observer acknowledges durable child identity and release BEFORE any bytes.
                byte[] packet=bootstrap.release(identity);
                PreflightFailure.need(packet!=null && packet.length>0 && packet.length<65536,RESOURCE_LIMIT);
                sendBootstrap(p,packet,deadline,bootstrap::beforeDelivery,bootstrapWriter);
            }
            long nextHeartbeat=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
            while(true) {
                if(stopping.getAsBoolean() || Thread.currentThread().isInterrupted())throw new InterruptedException();
                var children=descendants(p);
                if((!spark && !children.isEmpty()) || children.size()>32)uncertain=true; // Read-only children may not spawn; Spark observations remain bounded.
                for(var child:children)if(observed.size()<32)observed.putIfAbsent(child.pid(),new Observed(child,child.info().startInstant().orElse(null)));
                if(out.overflow || err.overflow)throw new PreflightFailure(STAGE_OUTPUT_LIMIT);
                if(out.failed || err.failed)throw new PreflightFailure(RESULT_INVALID);
                if(!p.isAlive() && out.done && err.done){result=new Result(p.exitValue(),identity,out.total,err.total,Set.copyOf(observed.keySet().stream().map(Object::toString).toList()),err.sha256,out.captured());break;}
                if(System.nanoTime()>=deadline)throw new PreflightFailure(STAGE_TIMEOUT);
                if(System.nanoTime()>=nextHeartbeat){observer.heartbeat();nextHeartbeat=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);}
                TimeUnit.MILLISECONDS.sleep(5);
            }
        }catch(InterruptedException ex){
            interrupted=true;
            // These checks precede ProcessBuilder.start when p is null. No writer was launched.
            failure=new PreflightFailure(p==null?PROCESS_START_FAILED:INTERRUPTED,p==null);
        }
        catch(IOException ex){failure=new PreflightFailure(p==null?PROCESS_START_FAILED:RESULT_INVALID);}
        catch(RuntimeException ex){failure=ex;}
        finally {
            interrupted|=Thread.interrupted();
            if(p!=null) {
                try {
                    try {
                        var last=descendants(p);if((!spark && !last.isEmpty()) || last.size()>32)uncertain=true;
                        for(var child:last)if(observed.size()<32)observed.putIfAbsent(child.pid(),new Observed(child,child.info().startInstant().orElse(null)));
                    }catch(RuntimeException ex){uncertain=true;} // Enumeration failure must not skip known-parent termination.
                    for(var child:observed.values())if(child.handle().isAlive()) {
                        if(result!=null)uncertain=true;
                        if(child.start()==null || !child.handle().info().startInstant().equals(Optional.of(child.start()))){uncertain=true;continue;}
                        child.handle().destroy();
                    }
                    if(p.isAlive())p.toHandle().destroy();
                    if(!p.waitFor(graceful.toMillis(),TimeUnit.MILLISECONDS)){p.toHandle().destroyForcibly();if(!p.waitFor(forced.toMillis(),TimeUnit.MILLISECONDS))uncertain=true;}
                    for(var child:observed.values())if(child.handle().isAlive() && child.start()!=null && child.handle().info().startInstant().equals(Optional.of(child.start())))child.handle().destroyForcibly();
                    long until=System.nanoTime()+drain.toNanos();
                    for(var child:observed.values())while(child.handle().isAlive() && System.nanoTime()<until)TimeUnit.MILLISECONDS.sleep(5);
                    if(observed.values().stream().anyMatch(c->c.handle().isAlive()))uncertain=true;
                    if(bootstrapWriter[0]!=null){
                        bootstrapWriter[0].join(Duration.ofNanos(Math.max(0,until-System.nanoTime())));
                        if(bootstrapWriter[0].isAlive()){uncertain=true;bootstrapWriter[0].interrupt();}
                    }
                    for(Drain d:List.of(out==null?Drain.EMPTY:out,err==null?Drain.EMPTY:err))if(d.thread!=null){long remaining=Math.max(0,until-System.nanoTime());d.thread.join(Duration.ofNanos(remaining));if(d.thread.isAlive())uncertain=true;}
                }catch(InterruptedException ex){interrupted=true;uncertain=true;}
                catch(RuntimeException ex){uncertain=true;}
                try{p.getInputStream().close();p.getErrorStream().close();
                    // Process.destroy closes stdin synchronously and can deadlock behind a blocked writer.
                    // Signals above use ProcessHandle; never close this stream while that writer may own it.
                    if(bootstrapWriter[0]==null || !bootstrapWriter[0].isAlive())p.getOutputStream().close();}catch(IOException ex){uncertain=true;}
                if(out!=null && out.thread!=null)out.thread.interrupt();if(err!=null && err.thread!=null)err.thread.interrupt();
            }
            if(interrupted)Thread.currentThread().interrupt();
        }
        if(uncertain)throw new PreflightFailure(TERMINATION_UNCONFIRMED,false);
        if(failure!=null)throw failure;
        if(result==null)throw new PreflightFailure(RESULT_INVALID,false);
        return result;
    }
    private void sendBootstrap(Process process,byte[] packet,long deadline,Runnable beforeDelivery,Thread[] bootstrapWriter)throws InterruptedException {
        Thread parent=Thread.currentThread();
        var deliveryAborted=new java.util.concurrent.atomic.AtomicBoolean();
        long startupDeadline=Math.min(deadline,System.nanoTime()+TimeUnit.SECONDS.toNanos(5));
        Runnable gate=()->{
            if(deliveryAborted.get() || stopping.getAsBoolean() || parent.isInterrupted() || Thread.currentThread().isInterrupted())throw new PreflightFailure(INTERRUPTED,false);
            if(System.nanoTime()>=startupDeadline)throw new PreflightFailure(STAGE_TIMEOUT,false);
        };
        gate.run();
        var result=new java.util.concurrent.CompletableFuture<Void>();
        Thread writer=Thread.ofVirtual().name("narrow-bootstrap-writer").unstarted(()->{
            try{
                try(var pipe=process.getOutputStream()){beforeDelivery.run();gate.run();pipe.write(packet);pipe.flush();}
                result.complete(null);
            }catch(IOException|RuntimeException ex){result.completeExceptionally(ex);}
        });
        bootstrapWriter[0]=writer;writer.start();boolean delivered=false;
        try{
            long remaining=startupDeadline-System.nanoTime();
            if(remaining<=0)throw new java.util.concurrent.TimeoutException();
            result.get(remaining,TimeUnit.NANOSECONDS);
            writer.join(Duration.ofNanos(Math.max(0,startupDeadline-System.nanoTime())));
            if(writer.isAlive())throw new java.util.concurrent.TimeoutException();
            delivered=true;
        }catch(java.util.concurrent.TimeoutException ex){throw new PreflightFailure(STAGE_TIMEOUT,false);}
        catch(java.util.concurrent.ExecutionException ex){if(ex.getCause() instanceof RuntimeException failure)throw failure;throw new PreflightFailure(RESULT_INVALID,false);}
        finally{
            // Thread flags can be consumed by callbacks or cleared for cleanup. Revocation cannot be.
            if(!delivered)deliveryAborted.set(true);
            if(writer.isAlive())writer.interrupt();
        }
    }
    List<ProcessHandle> descendants(Process process){return process.descendants().limit(33).toList();}
    private static final class Drain implements Runnable {
        static final Drain EMPTY=new Drain(InputStream.nullInputStream(),1);
        final InputStream input;final int limit;volatile boolean done,overflow,failed;volatile long total;volatile String sha256;Thread thread;
        final ByteArrayOutputStream capture;
        Drain(InputStream input,int limit){this(input,limit,false);}
        Drain(InputStream input,int limit,boolean capture){this.input=input;this.limit=limit;this.capture=capture?new ByteArrayOutputStream():null;}
        byte[] captured(){return capture==null?null:capture.toByteArray();}
        void start(){thread=Thread.ofVirtual().name("reliability-preflight-drain").start(this);}
        public void run(){
            try(input){
                var digest=java.security.MessageDigest.getInstance("SHA-256");byte[] buffer=new byte[8192];int n;
                while((n=input.read(buffer))!=-1){total=Math.min((long)limit+1,total+n);if(total>limit)overflow=true;else if(capture!=null)capture.write(buffer,0,n);digest.update(buffer,0,n);}
                sha256=HexFormat.of().formatHex(digest.digest());
            }catch(IOException|java.security.NoSuchAlgorithmException ex){failed=true;}finally{done=true;}
        }
    }
}
