package platform.contentapi.execution;

import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import platform.contentapi.importing.ImportBindings;
import platform.contentapi.management.*;

/** Genuine retry owner process exits before the parent independently qualifies recovery. */
public final class ReliabilityRetryCancellationProcess {
    public static void main(String[] args){
        var jdbc=new JdbcTemplate(new DriverManagerDataSource(args[0],System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var tx=ReliabilityExecutionFixtures.transactions(jdbc);UUID installation=UUID.fromString(args[1]),run=UUID.fromString(args[6]);String point=args[7];final long[] pid={0};
        Runnable cancel=()->{long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);new ControlRepository(jdbc,tx,installation).request(ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","CANCEL").putNull("attemptId").putNull("quiescenceConfirmed")),"cancel-retry","\""+version+"\""));};
        try(var guard=InstallationHostGuard.acquire(new ImportBindings(jdbc,tx,installation).control(args[2],args[3]))){
            var observed=new java.util.HashMap<Long,java.time.Instant>();
            var publisher=new ReadOnlyVerifierProcess(true){
                @Override List<ProcessHandle> descendants(Process child){
                    var children=super.descendants(child);for(var process:children)process.info().startInstant().ifPresent(start->observed.putIfAbsent(process.pid(),start));return children;
                }
                @Override Result run(List<String> command,Map<String,String> environment,Path cwd,Duration deadline,Observer observer){
                    if(point.equals("pre-authorization")){cancel.run();observer.beforeStart();throw new AssertionError("Cancelled writer authorized");}
                    if(point.equals("publication-complete")){var result=super.run(command,environment,cwd,deadline,observer);
                        // Repeated process-tree scans also see short-lived shell helpers. Review all
                        // observations for absence, then retain the supervisor's bounded identities.
                        observed.forEach((id,start)->{var state=RecoveryQuiescence.LOCAL.inspect(new RecoveryQuiescence.Identity(id,start));if(state!=RecoveryQuiescence.State.ABSENT && state!=RecoveryQuiescence.State.REUSED)throw new AssertionError("Observed descendant still unresolved");});
                        observed.keySet().retainAll(result.descendants().stream().map(Long::valueOf).toList());cancel.run();observer.heartbeat();return result;}
                    return super.run(List.of("/bin/sleep","30"),environment,cwd,deadline,new Observer(){
                        public void beforeStart(){observer.beforeStart();}
                        public void started(InstallationHostGuard.ProcessIdentity child){pid[0]=child.pid();observer.started(child);cancel.run();observer.heartbeat();}
                        public void heartbeat(){observer.heartbeat();}
                    });
                }
            };
            try{new ReliabilityRetry(jdbc,tx,installation).execute(guard,UUID.fromString(args[4]),args[5],publisher);throw new AssertionError("Expected cancellation result");}
            catch(ReliabilityRetry.CancellationRecorded result){
                String expected=point.equals("pre-authorization")?"CANCELLED":"RECOVERY_REQUIRED";
                if(!result.status().equals(expected))throw new AssertionError(result.status());
                if(pid[0]!=0 && ProcessHandle.of(pid[0]).map(ProcessHandle::isAlive).orElse(false))throw new AssertionError("Child not reaped");
                var identities=SourceJson.object().putArray("identities");observed.forEach((id,start)->identities.addObject().put("pid",id.toString()).put("startedAt",start.toString()));
                try{java.nio.file.Files.write(Path.of(args[8]),SourceJson.canonical(identities));}catch(java.io.IOException failure){throw new RuntimeException(failure);}
                System.out.println(result.status());
            }
        }
    }
}
