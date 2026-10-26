package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import java.util.function.BiConsumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

/** Test-only access to internal D1 seams; no production Content capability is enabled. */
public final class RssFetchTestSupport {
    private RssFetchTestSupport() { }
    public record Observed(WorkerRepository.FetchStage stage,String completionSha,byte[] descriptor,JsonNode completionStamp) { }
    public static JsonNode manifest() throws Exception {
        Path root=Path.of("../../ingestion/target").toRealPath();var n=SourceJson.object().put("adapter",RssFetchRuntime.MAIN).put("javaRuntime",Runtime.version().toString());var cp=n.putArray("classpath");
        var paths=new ArrayList<Path>();paths.add(root.resolve("ingestion-0.1.0-SNAPSHOT.jar"));try(var jars=Files.list(root.resolve("dependency"))){paths.addAll(jars.filter(p->p.toString().endsWith(".jar")).sorted().toList());}
        for(Path path:paths)cp.addObject().put("path",path.toString()).put("sha256",SourceJson.sha(Files.readAllBytes(path)));return n;
    }
    public static Observed fetch(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,String runtime,BiConsumer<String,WorkerRepository.FetchStage> fault) {
        var result=new RssFetchExecution(jdbc,tx,workers,new ReadOnlyVerifierProcess(),(p,s)->fault.accept(p.name(),s)).fetch(guard,owner,0,runtime);
        return new Observed(result.stage(),result.evidence().completionSha(),result.evidence().descriptorBytes(),result.evidence().completionStamp());
    }
    public static Observed observe(InstallationHostGuard guard,WorkerRepository.FetchStage stage) {
        var files=new RssJournalFiles(guard,stage,false);byte[] complete=files.read("complete.json");var e=files.reopen(files.read("command.json"),SourceJson.sha(complete));return new Observed(stage,e.completionSha(),e.descriptorBytes(),e.completionStamp());
    }
    public static boolean inspect(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,Observed observed) {
        var evidence=new RssJournalFiles.Evidence(observed.stage().namespace(),observed.descriptor(),SourceJson.parse(observed.descriptor()),observed.completionSha(),observed.completionStamp());
        return new RssFetchExecution(jdbc,tx,workers).checkpointPresent(guard,new RssFetchExecution.Journaled(observed.stage(),evidence));
    }
    public static boolean committed(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,UUID run,String source){return new RssFetchExecution(jdbc,tx,workers).committedJournalIntact(guard,run,source);}
}
