package platform.contentapi.execution;

/** Test-only access to the dispatcher's cleanup-boundary settlement. */
public final class RunningCancellationTestSupport {
    public static WorkerRepository.Session stop(WorkerRepository worker,InstallationHostGuard guard,WorkerRepository.Owner owner,boolean uncertain){
        try{worker.heartbeat(guard,owner);throw new AssertionError("Expected cancellation fence");}
        catch(RunningCancellation signal){return worker.settleCancellation(guard,uncertain?new PreflightFailure(PreflightFailure.Code.TERMINATION_UNCONFIRMED,false):signal);}
    }
    public static WorkerRepository.Session stopWithChild(WorkerRepository worker,InstallationHostGuard guard,WorkerRepository.Owner owner,boolean uncertain){
        var process=uncertain?new ReadOnlyVerifierProcess(){@Override java.util.List<ProcessHandle> descendants(Process child){throw new IllegalStateException("observation unavailable");}}:new ReadOnlyVerifierProcess();
        final long[] pid={0};
        try{
            process.run(java.util.List.of("/bin/sleep","20"),java.util.Map.of(),java.nio.file.Path.of("/tmp"),java.time.Duration.ofSeconds(30),new ReadOnlyVerifierProcess.Observer(){
                public void beforeStart(){}
                public void started(InstallationHostGuard.ProcessIdentity child){pid[0]=child.pid();worker.heartbeat(guard,owner);}
                public void heartbeat(){worker.heartbeat(guard,owner);}
            });throw new AssertionError("Expected cancellation");
        }catch(RunningCancellation signal){
            if(pid[0]==0 || ProcessHandle.of(pid[0]).map(ProcessHandle::isAlive).orElse(false))throw new AssertionError("Child not reaped");
            return worker.settleCancellation(guard,signal);
        }
    }
}
