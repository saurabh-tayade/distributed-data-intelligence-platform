package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import java.util.function.Consumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;

/** Test-classpath-only access. This does not enable the worker or an operator command. */
public final class NarrowContentOperationTestSupport {
    public record Facts(UUID operation,UUID attempt,List<String> phases,InstallationHostGuard.ProcessIdentity child) { }
    public static Facts probe(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,
            InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation,Consumer<String> boundary){
        return facts(new NarrowContentOperationJournal(jdbc,tx,workers).probe(guard,owner,operation,
                SourceJson.canonical(SourceJson.object().put("probeVersion",1)),runtime(),b->boundary.accept(b.name())));
    }
    public static Facts inspect(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,UUID operation){
        return facts(new NarrowContentOperationJournal(jdbc,tx,workers).inspect(operation));
    }
    public static tools.jackson.databind.node.ObjectNode reservation(JdbcTemplate jdbc,ManagementTransactions tx,
            InstallationHostGuard guard,WorkerRepository.Owner owner,UUID operation){
        var e=NarrowContentAdmission.inspectReady(jdbc,tx,guard,owner.runId(),owner.attemptId());
        var path=ContentOperationPlan.original(guard.binding().root().resolve("runs/"+owner.runId()+"/attempts/"+owner.attemptId()),owner.runId(),owner.attemptId(),e.source()).directory("CHECK_INPUTS",0);
        return NarrowContentOperationJournal.bootstrap(e,owner,operation,UUID.fromString(path.getFileName().toString()),SourceJson.sha(SourceJson.canonical(SourceJson.object().put("probeVersion",1))));
    }
    static NarrowContentOperationJournal.ProbeRuntime runtime(){
        Path root=Path.of("").toAbsolutePath();
        while(root!=null && !Files.isRegularFile(root.resolve("processing/narrow_startup_probe.py")))root=root.getParent();
        if(root==null)throw new IllegalStateException("TEST_REPOSITORY_REQUIRED");
        return new NarrowContentOperationJournal.ProbeRuntime(Path.of(System.getProperty("probe.python","/usr/bin/python3")),root.resolve("processing"));
    }
    private static Facts facts(NarrowContentOperationJournal.Facts value){return new Facts(value.operation(),value.attempt(),value.phases(),value.child());}
    private NarrowContentOperationTestSupport(){}
}
