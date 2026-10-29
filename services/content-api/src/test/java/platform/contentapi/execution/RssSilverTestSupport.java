package platform.contentapi.execution;

import java.util.*;
import java.util.function.BiConsumer;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

public final class RssSilverTestSupport {
    private RssSilverTestSupport(){}
    public record Stored(WorkerRepository.Owner owner,UUID artifact,byte[] evidence){}
    public static Stored execute(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,String runtime,BiConsumer<String,WorkerRepository.Owner> fault){var r=new RssSilverExecution(jdbc,tx,workers,(p,s)->fault.accept(p.name(),s.owner())).execute(guard,owner,"demo-feed",runtime);return new Stored(r.owner(),r.artifact(),r.evidence());}
    public static UUID inspect(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,UUID run,String runtime){return new RssSilverExecution(jdbc,tx,workers).inspect(guard,run,"demo-feed",runtime);}
    public static JsonNode modules(java.nio.file.Path processing)throws Exception {var n=SourceJson.object();for(String module:RssSilverRuntime.MODULES)n.put(module,SourceJson.sha(java.nio.file.Files.readAllBytes(processing.resolve(module))));return n;}
}
