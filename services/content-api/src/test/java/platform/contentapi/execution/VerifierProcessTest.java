package platform.contentapi.execution;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import static org.assertj.core.api.Assertions.*;

/** Controlled actual JVM children. No Python prerequisite for the ordinary Java unit suite. */
class VerifierProcessTest {
    @TempDir Path temp;
    static List<String> command(String mode,String marker){return List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),Child.class.getName(),mode,marker);}
    ReadOnlyVerifierProcess process(){return new ReadOnlyVerifierProcess(128,Duration.ofMillis(500),Duration.ofMillis(500),Duration.ofMillis(500));}
    static class Observer implements ReadOnlyVerifierProcess.Observer {
        int authorizations;InstallationHostGuard.ProcessIdentity child;
        public void beforeStart(){authorizations++;}
        public void started(InstallationHostGuard.ProcessIdentity child){this.child=child;}
        public void heartbeat(){ }
    }
    ReadOnlyVerifierProcess.Result run(String mode,Observer observer){return process().run(command(mode,temp.resolve("marker").toString()),Map.of("PATH","/usr/bin:/bin","TZ","UTC","LANG","C.UTF-8"),temp,Duration.ofSeconds(10),observer);}
    @Test void cleanExitHasActualIdentityAndClosedStdinAndIsolatedEnvironment(){var observer=new Observer();var r=run("environment",observer);assertThat(r.exitCode()).isZero();assertThat(r.child().pid()).isPositive();assertThat(r.child().startedAt()).isNotNull();assertThat(r.stdoutBytes()).isZero();assertThat(r.stderrBytes()).isZero();assertThat(observer.authorizations).isEqualTo(1);}
    @Test void nonzeroExitIsNotSuccessEvidence(){assertThat(run("failed",new Observer()).exitCode()).isEqualTo(1);}
    @Test void startupFailureHasSafeClassification(){assertThatThrownBy(()->process().run(List.of(temp.resolve("missing").toString()),Map.of(),temp,Duration.ofSeconds(1),new Observer())).isInstanceOf(PreflightFailure.class).hasMessage("PROCESS_START_FAILED").hasNoCause();}
    @Test void authorizationFailurePreventsLaunch(){var observer=new Observer(){public void beforeStart(){throw new WorkerFailure(WorkerFailure.Code.OWNERSHIP_LOST);}};assertThatThrownBy(()->run("marker",observer)).hasMessage("OWNERSHIP_LOST");assertThat(Files.exists(temp.resolve("marker"))).isFalse();}
    @Test void deadlineTerminatesDirectReadOnlyChild(){var observer=new Observer();assertThatThrownBy(()->process().run(command("hang","unused"),Map.of(),temp,Duration.ofMillis(150),observer)).hasMessage("STAGE_TIMEOUT");assertThat(ProcessHandle.of(observer.child.pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();}
    @ParameterizedTest @ValueSource(strings={"stdout","stderr"}) void eachOutputStreamHasIndependentBound(String mode){assertThatThrownBy(()->run(mode,new Observer())).hasMessage("STAGE_OUTPUT_LIMIT").hasNoCause();}
    @Test void databaseFailureInIdentityCallbackCleansChildAndEscapes(){var observer=new Observer(){public void started(InstallationHostGuard.ProcessIdentity child){super.started(child);throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_UNAVAILABLE);}};assertThatThrownBy(()->run("hang",observer)).hasMessage("MANAGEMENT_UNAVAILABLE");assertThat(ProcessHandle.of(observer.child.pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();}
    @Test void observedDescendantAlwaysRequiresRecoveryEvenIfTerminationSucceeds(){Path marker=temp.resolve("marker");var observer=new Observer(){public void started(InstallationHostGuard.ProcessIdentity child){super.started(child);long until=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);while(!Files.exists(marker) && System.nanoTime()<until)java.util.concurrent.locks.LockSupport.parkNanos(1000000);assertThat(Files.exists(marker)).isTrue();}};assertThatThrownBy(()->process().run(command("descendant",marker.toString()),Map.of(),temp,Duration.ofMillis(200),observer)).hasMessage("TERMINATION_UNCONFIRMED");}
    @Test void interruptionRetainsFlagAndDoesNotLeaveObservedParentAlive()throws Exception {
        var launched=new CountDownLatch(1);var result=new CompletableFuture<String>();var observer=new Observer(){public void started(InstallationHostGuard.ProcessIdentity child){super.started(child);launched.countDown();}};
        Thread t=Thread.ofPlatform().start(()->{try{run("hang",observer);result.complete("UNEXPECTED");}catch(PreflightFailure ex){result.complete(ex.code()+":"+Thread.currentThread().isInterrupted());}});
        assertThat(launched.await(5,TimeUnit.SECONDS)).isTrue();t.interrupt();assertThat(result.get(5,TimeUnit.SECONDS)).isEqualTo("INTERRUPTED:true");t.join(1000);assertThat(ProcessHandle.of(observer.child.pid()).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }
    @Test void deadlineCannotExceedVerificationPolicy(){assertThatThrownBy(()->process().run(command("environment","unused"),Map.of(),temp,Duration.ofMinutes(16),new Observer())).hasMessage("RUNTIME_MISMATCH");}
    public static final class Child {
        public static void main(String[] args)throws Exception {
            switch(args[0]) {
                case "environment"->{if(System.getenv().containsKey("AWS_ACCESS_KEY_ID") || System.getenv().containsKey("HTTPS_PROXY") || System.getenv().containsKey("PYTHONPATH") || System.in.read()!=-1)System.exit(2);}
                case "marker"->Files.writeString(Path.of(args[1]),"STARTED");
                case "failed"->System.exit(1);
                case "invalid-result","malformed-result"->{var file=Path.of(args[1]).resolveSibling("result.json");Files.writeString(file,args[0].equals("invalid-result")?"{}":"{");Files.setPosixFilePermissions(file,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));}
                case "native-failed"->{var command=platform.contentapi.management.SourceJson.parse(Files.readAllBytes(Path.of(args[1])));var n=platform.contentapi.management.SourceJson.object().put("protocolVersion",1).put("runId",command.get("runId").stringValue()).put("attemptId",command.get("attemptId").stringValue()).put("stage","CHECK_INPUTS").put("itemOrdinal",0).put("status","FAILED").put("code","EVIDENCE_UNAVAILABLE").putNull("evidenceId");n.putObject("counts");var file=Path.of(args[1]).resolveSibling("result.json");Files.write(file,platform.contentapi.management.SourceJson.canonical(n));Files.setPosixFilePermissions(file,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));System.exit(1);}
                case "stdout"->System.out.write(new byte[4096]);
                case "stderr"->System.err.write(new byte[4096]);
                case "hang"->new CountDownLatch(1).await();
                case "descendant"->{var child=new ProcessBuilder(command("hang","unused")).redirectError(ProcessBuilder.Redirect.DISCARD).redirectOutput(ProcessBuilder.Redirect.DISCARD).start();Runtime.getRuntime().addShutdownHook(Thread.ofPlatform().unstarted(()->{child.destroyForcibly();try{child.waitFor(1,TimeUnit.SECONDS);}catch(InterruptedException ex){Thread.currentThread().interrupt();}}));Files.writeString(Path.of(args[1]),"STARTED");child.waitFor();}
                default->throw new IllegalArgumentException();
            }
        }
    }
}
