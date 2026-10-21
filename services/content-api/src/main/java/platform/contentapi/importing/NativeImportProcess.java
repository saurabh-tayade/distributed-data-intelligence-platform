package platform.contentapi.importing;

import java.io.*;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import static platform.contentapi.importing.ImportFailure.Code.*;

/** One fixed, non-forking read-only verifier invocation. Streams/termination are never public diagnostics. */
public class NativeImportProcess {
    public record Result(int exitCode,byte[] stdout) { }
    public Result run(List<String> command,Path directory,int timeoutSeconds,byte[] request) {
        ImportFailure.require(request.length<=ImportJson.PROTOCOL_LIMIT,RESULT_INVALID);
        Process p=null;Drain out=null,err=null;Thread writer=null;boolean uncertain=false,interrupted=false;ImportFailure failure=null;Result result=null;
        Set<ProcessHandle> descendants=new HashSet<>();
        try {
            if(Thread.currentThread().isInterrupted())throw new InterruptedException();
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(timeoutSeconds);
            var builder=new ProcessBuilder(command).directory(directory.toFile());builder.environment().clear();builder.environment().putAll(Map.of("PATH","/usr/bin:/bin","TZ","UTC","LANG","C.UTF-8"));
            p=builder.start();out=new Drain(p.getInputStream(),ImportJson.PROTOCOL_LIMIT,true);err=new Drain(p.getErrorStream(),65536,false);out.start();err.start();
            Process child=p;writer=Thread.ofVirtual().start(() -> {try(var stream=child.getOutputStream()){stream.write(request);}catch(IOException ignored){}});
            while(true) {
                try {var observed=p.descendants().limit(33).toList();if(!observed.isEmpty())uncertain=true;descendants.addAll(observed);}catch(RuntimeException e){uncertain=true;}
                if(out.failed || err.failed)throw new ImportFailure(RESULT_INVALID);
                if(!p.isAlive() && out.done && err.done && !writer.isAlive()){result=new Result(p.exitValue(),out.bytes.toByteArray());break;}
                if(System.nanoTime()>=deadline)throw new ImportFailure(TIMEOUT);
                TimeUnit.MILLISECONDS.sleep(5);
            }
        } catch(InterruptedException e){interrupted=true;failure=new ImportFailure(INTERRUPTED);}
        catch(IOException e){failure=new ImportFailure(START_FAILED);}
        catch(ImportFailure e){failure=e;}
        catch(RuntimeException e){uncertain=true;failure=new ImportFailure(CLEANUP_UNCERTAIN,false);}
        finally {
            interrupted|=Thread.interrupted();
            if(p!=null) {
                try {
                    var last=p.descendants().limit(33).toList();if(!last.isEmpty())uncertain=true;descendants.addAll(last);
                    for(var d:descendants)if(d.isAlive())d.destroyForcibly();
                    if(p.isAlive())p.destroyForcibly();
                    if(!p.waitFor(1,TimeUnit.SECONDS))uncertain=true;
                    long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(1);
                    for(var d:descendants)while(d.isAlive() && System.nanoTime()<deadline)TimeUnit.MILLISECONDS.sleep(5);
                    if(descendants.stream().anyMatch(ProcessHandle::isAlive))uncertain=true;
                } catch(InterruptedException e){interrupted=true;uncertain=true;}catch(RuntimeException e){uncertain=true;}
                try {p.getOutputStream().close();p.getInputStream().close();p.getErrorStream().close();}catch(IOException e){uncertain=true;}
                for(Thread t:new Thread[]{out==null?null:out.thread,err==null?null:err.thread,writer})if(t!=null) {
                    try {t.join(Duration.ofMillis(250));if(t.isAlive())uncertain=true;}catch(InterruptedException e){interrupted=true;uncertain=true;}finally{t.interrupt();}
                }
            }
            if(interrupted)Thread.currentThread().interrupt();
        }
        if(uncertain)throw new ImportFailure(CLEANUP_UNCERTAIN,false);
        if(failure!=null)throw failure;
        return result;
    }
    private static final class Drain implements Runnable {
        final InputStream stream;final int limit;final boolean retain;final ByteArrayOutputStream bytes=new ByteArrayOutputStream();
        volatile boolean done,failed;Thread thread;
        Drain(InputStream stream,int limit,boolean retain){this.stream=stream;this.limit=limit;this.retain=retain;}
        void start(){thread=Thread.ofVirtual().start(this);}
        public void run(){try(stream){byte[] buffer=new byte[8192];int total=0,n;while((n=stream.read(buffer))!=-1){if(n>limit-total){failed=true;return;}total+=n;if(retain)bytes.write(buffer,0,n);}}catch(IOException e){failed=true;}finally{done=true;}}
    }
}
