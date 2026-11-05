package platform.contentapi.execution;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentPreparationFixtures.*;

/** Actual bounded processes and Java-to-Python protocol, without a fake durability claim. */
class NarrowContentBootstrapTest {
    @TempDir Path temp;
    @ParameterizedTest @ValueSource(booleans={false,true})
    void javaBootstrapOpensExactPythonStrictSinkWithoutWritingSidecars(boolean poisonedCache)throws Exception{
        var host=OwnershipFixtures.host(temp.toRealPath());var spec=specification(host);
        try(var guard=InstallationHostGuard.acquire(host)){
            var e=NarrowContentPreparation.prepare(guard,spec).expected();
            Path directory=ContentOperationPlan.original(root(spec),spec.run(),spec.attempt(),spec.source()).directory("CHECK_INPUTS",0);
            for(Path p:List.of(directory.getParent(),directory))Files.createDirectory(p,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));
            byte[] command=SourceJson.canonical(SourceJson.object().put("probeVersion",1));
            PreparedCommand.prepare(guard,directory,command,1024*1024);
            var session=new WorkerRepository.Session(host.installationId(),guard.process().instanceId(),"test","b".repeat(64),"d".repeat(64),host.digest(),"READY",1,1,0);
            var owner=new WorkerRepository.Owner(session,spec.run(),spec.attempt(),1,0,"CLAIMED");
            var base=NarrowContentOperationJournal.bootstrap(e,owner,UUID.randomUUID(),UUID.fromString(directory.getFileName().toString()),SourceJson.sha(command));
            var before=snapshot(host.root());var calls=new ArrayList<String>();var runtime=NarrowContentOperationTestSupport.runtime();
            if(poisonedCache){
                Path processing=Files.createDirectory(temp.resolve("probe-modules"));
                for(String name:List.of("narrow_startup_probe.py","narrow_evidence_sink.py","narrow_transport_json.py"))Files.copy(runtime.processing().resolve(name),processing.resolve(name));
                String poison="import pathlib,py_compile,sys; p=pathlib.Path(sys.argv[1]); original=p.read_bytes(); p.write_text('def main(): return 99\\n'); py_compile.compile(str(p),doraise=True,invalidation_mode=py_compile.PycInvalidationMode.UNCHECKED_HASH); p.write_bytes(original)";
                var compiler=new ProcessBuilder(runtime.python().toString(),"-I","-B","-c",poison,processing.resolve("narrow_startup_probe.py").toString()).inheritIO().start();
                try{assertThat(compiler.waitFor(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();assertThat(compiler.exitValue()).isZero();}
                finally{if(compiler.isAlive())compiler.destroyForcibly();}
                runtime=new NarrowContentOperationJournal.ProbeRuntime(runtime.python(),processing);
                var cachedCommand=new ArrayList<>(runtime.command());int option=cachedCommand.indexOf("-X");cachedCommand.remove(option);cachedCommand.remove(option);
                var cached=new ProcessBuilder(cachedCommand).start();cached.getOutputStream().close();
                try{assertThat(cached.waitFor(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();assertThat(cached.exitValue()).as("Control import must demonstrate poisoned cache execution").isEqualTo(99);}
                finally{if(cached.isAlive())cached.destroyForcibly();}
            }
            var result=new ReadOnlyVerifierProcess().run(runtime.command(),Map.of(),runtime.processing(),Duration.ofSeconds(10),new ReadOnlyVerifierProcess.BootstrapObserver(){
                public void beforeStart(){calls.add("start");}
                public void started(InstallationHostGuard.ProcessIdentity child){calls.add("identity");assertThat(child.pid()).isPositive();assertThat(child.startedAt()).isNotNull();}
                public byte[] release(InstallationHostGuard.ProcessIdentity child){calls.add("release");return SourceJson.canonical(base.deepCopy().put("childPid",Long.toString(child.pid())));}
                public void heartbeat(){}
            });
            assertThat(result.exitCode()).isZero();assertThat(result.stdoutBytes()).isZero();assertThat(result.stderrBytes()).isZero();
            assertThat(calls).containsExactly("start","identity","release");assertThat(snapshot(host.root())).isEqualTo(before);
        }
    }
    static class Observer implements ReadOnlyVerifierProcess.BootstrapObserver{
        InstallationHostGuard.ProcessIdentity child;int releases;
        public void beforeStart(){}
        public void started(InstallationHostGuard.ProcessIdentity identity){child=identity;}
        public byte[] release(InstallationHostGuard.ProcessIdentity identity){releases++;return "READY".getBytes(java.nio.charset.StandardCharsets.US_ASCII);}
        public void heartbeat(){}
    }
    ReadOnlyVerifierProcess.Result run(String mode,Observer observer){return new ReadOnlyVerifierProcess(128,Duration.ofMillis(500),Duration.ofMillis(500),Duration.ofMillis(500))
            .run(VerifierProcessTest.command(mode,temp.resolve("marker").toString()),Map.of(),temp,Duration.ofSeconds(10),observer);}
    @Test void validPacketRequiresIdentityBeforeDeliveryAndEofBeforeEffect()throws Exception{
        var observer=new Observer(){public byte[] release(InstallationHostGuard.ProcessIdentity child){assertThat(Files.exists(temp.resolve("marker"))).isFalse();assertThat(child).isEqualTo(this.child);return super.release(child);}};
        assertThat(run("bootstrap",observer).exitCode()).isZero();assertThat(Files.readString(temp.resolve("marker"))).isEqualTo("READY");assertThat(observer.releases).isEqualTo(1);
    }
    @ParameterizedTest @ValueSource(strings={"start","identity","release","delivery"}) void eachFailedAuthorityBoundaryPreventsChildWork(String boundary){
        var observer=new Observer(){
            void fail(String point){if(point.equals(boundary))throw new WorkerFailure(WorkerFailure.Code.OWNERSHIP_LOST);}
            public void beforeStart(){fail("start");}
            public void started(InstallationHostGuard.ProcessIdentity child){super.started(child);fail("identity");}
            public byte[] release(InstallationHostGuard.ProcessIdentity child){fail("release");return super.release(child);}
            public void beforeDelivery(){fail("delivery");}
        };
        assertThatThrownBy(()->run("bootstrap",observer)).hasMessage("OWNERSHIP_LOST");assertThat(temp.resolve("marker")).doesNotExist();
        if(observer.child!=null)assertThat(ProcessHandle.of(observer.child.pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }
    @ParameterizedTest @ValueSource(ints={0,65536,65537}) void invalidPacketSizeNeverReleasesData(int size){
        var observer=new Observer(){public byte[] release(InstallationHostGuard.ProcessIdentity child){return new byte[size];}};
        assertThatThrownBy(()->run("bootstrap",observer)).hasMessage("RESOURCE_LIMIT");assertThat(temp.resolve("marker")).doesNotExist();
    }
    @Test void startupPacketAndNonreadingChildRemainBounded(){
        var observer=new Observer(){public byte[] release(InstallationHostGuard.ProcessIdentity child){return new byte[65535];}};
        long start=System.nanoTime();assertThatThrownBy(()->new ReadOnlyVerifierProcess(128,Duration.ofMillis(500),Duration.ofMillis(500),Duration.ofMillis(500)).run(VerifierProcessTest.command("hang","unused"),Map.of(),temp,Duration.ofSeconds(1),observer)).hasMessage("STAGE_TIMEOUT");
        assertThat(Duration.ofNanos(System.nanoTime()-start)).isLessThan(Duration.ofSeconds(4));
        assertThat(ProcessHandle.of(observer.child.pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }
    @ParameterizedTest @ValueSource(strings={"release","delivery"}) void expiredDeadlineCannotReleaseWaitingChild(String at){
        var observer=new Observer(){
            void delay(String point){if(point.equals(at))try{Thread.sleep(300);}catch(InterruptedException ex){Thread.currentThread().interrupt();}}
            public byte[] release(InstallationHostGuard.ProcessIdentity child){delay("release");return super.release(child);}
            public void beforeDelivery(){delay("delivery");}
        };
        assertThatThrownBy(()->new ReadOnlyVerifierProcess().run(VerifierProcessTest.command("bootstrap",temp.resolve("marker").toString()),Map.of(),temp,Duration.ofMillis(200),observer)).hasMessage("STAGE_TIMEOUT");
        assertThat(temp.resolve("marker")).doesNotExist();assertThat(ProcessHandle.of(observer.child.pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }
    @ParameterizedTest @ValueSource(strings={"release","delivery"}) void stopAtBootstrapBoundaryCannotReleaseWaitingChild(String at){
        var stopping=new java.util.concurrent.atomic.AtomicBoolean();
        var observer=new Observer(){
            public byte[] release(InstallationHostGuard.ProcessIdentity child){if(at.equals("release"))stopping.set(true);return super.release(child);}
            public void beforeDelivery(){if(at.equals("delivery"))stopping.set(true);}
        };
        assertThatThrownBy(()->new ReadOnlyVerifierProcess(false,stopping::get).run(VerifierProcessTest.command("bootstrap",temp.resolve("marker").toString()),Map.of(),temp,Duration.ofSeconds(3),observer)).hasMessage("INTERRUPTED");
        assertThat(temp.resolve("marker")).doesNotExist();assertThat(ProcessHandle.of(observer.child.pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }

    @Test void fullSmallPipeAndSigtermResistantChildCannotBlockCleanup()throws Exception{
        Path ready=temp.resolve("small-pipe-ready");
        var command=List.of(NarrowContentOperationTestSupport.runtime().python().toString(),"-I","-B","-c",
                "import fcntl,signal,time,sys; fcntl.fcntl(0,fcntl.F_SETPIPE_SZ,4096) if hasattr(fcntl,'F_SETPIPE_SZ') else None; signal.signal(signal.SIGTERM,signal.SIG_IGN); open(sys.argv[1],'w').close(); time.sleep(60)",ready.toString());
        var observer=new Observer(){
            public void started(InstallationHostGuard.ProcessIdentity child){super.started(child);long until=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(2);
                while(!Files.exists(ready) && System.nanoTime()<until)java.util.concurrent.locks.LockSupport.parkNanos(1000000);assertThat(ready).isRegularFile();}
            public byte[] release(InstallationHostGuard.ProcessIdentity child){return new byte[65535];}
        };
        long started=System.nanoTime();
        assertThatThrownBy(()->new ReadOnlyVerifierProcess(128,Duration.ofMillis(100),Duration.ofMillis(100),Duration.ofMillis(100))
                .run(command,Map.of(),temp,Duration.ofMillis(600),observer)).hasMessage("STAGE_TIMEOUT");
        assertThat(Duration.ofNanos(System.nanoTime()-started)).isLessThan(Duration.ofSeconds(3));
        assertThat(ProcessHandle.of(observer.child.pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }

    @Test void consumedInterruptCannotUndoStickyDeliveryRevocation()throws Exception{
        var waiting=new java.util.concurrent.CountDownLatch(1);var outcome=new java.util.concurrent.CompletableFuture<String>();
        var observer=new Observer(){public void beforeDelivery(){waiting.countDown();try{new java.util.concurrent.CountDownLatch(1).await();}catch(InterruptedException consumed){/* Model a callback consuming the flag. */}}};
        var runner=new ReadOnlyVerifierProcess(){List<ProcessHandle> descendants(Process process){
            try{Thread.sleep(400);}catch(InterruptedException consumed){/* Cleanup deliberately clears flags. */}return super.descendants(process);
        }};
        Thread owner=Thread.ofPlatform().start(()->{
            try{runner.run(VerifierProcessTest.command("bootstrap",temp.resolve("marker").toString()),Map.of(),temp,Duration.ofSeconds(5),observer);outcome.complete("UNEXPECTED");}
            catch(PreflightFailure ex){outcome.complete(ex.code()+":"+Thread.currentThread().isInterrupted());}
        });
        assertThat(waiting.await(3,java.util.concurrent.TimeUnit.SECONDS)).isTrue();owner.interrupt();
        assertThat(outcome.get(5,java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("INTERRUPTED:true");owner.join(1000);
        assertThat(temp.resolve("marker")).doesNotExist();assertThat(ProcessHandle.of(observer.child.pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }

}
