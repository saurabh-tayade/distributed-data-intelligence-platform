package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;

public final class RssGoldTestSupport {
    private RssGoldTestSupport(){}
    public record Prepared(WorkerRepository.Owner owner,Object token,byte[] bytes){}
    public record Published(WorkerRepository.Owner owner,UUID artifact,byte[] evidence){}
    @FunctionalInterface public interface Fault {void at(String phase,String point,WorkerRepository.Owner owner);}
    public static WorkerRepository.Owner silver(JdbcTemplate j,ManagementTransactions t,WorkerRepository w,InstallationHostGuard g,WorkerRepository.Owner owner,String fetch,String bronze,String silver,int sources,int completed){
        var selected=w.frozenContent(g,owner).get("sources");
        for(int i=0;i<sources;i++){var f=new RssFetchExecution(j,t,w).fetch(g,owner,i,fetch);owner=f.stage().owner();owner=new RssBronzeExecution(j,t,w).store(g,owner,selected.get(i).get("sourceId").stringValue(),bronze).stage().owner();}
        for(int i=0;i<completed;i++)owner=new RssSilverExecution(j,t,w).execute(g,owner,selected.get(i).get("sourceId").stringValue(),silver).owner();return owner;
    }
    public static Prepared prepare(JdbcTemplate j,ManagementTransactions t,WorkerRepository w,InstallationHostGuard g,WorkerRepository.Owner o,String runtime,Fault f){var r=new RssGoldExecution(j,t,w,(phase,point,owner)->f.at(phase,point.name(),owner)).prepare(g,o,runtime);return new Prepared(r.owner(),r,r.bytes());}
    public static Published publish(JdbcTemplate j,ManagementTransactions t,WorkerRepository w,InstallationHostGuard g,Prepared p,String runtime,Fault f){var r=new RssGoldExecution(j,t,w,(phase,point,owner)->f.at(phase,point.name(),owner)).publish(g,(RssGoldExecution.Prepared)p.token(),runtime);return new Published(r.owner(),r.artifact(),r.evidence());}
    public static UUID inspect(JdbcTemplate j,ManagementTransactions t,WorkerRepository w,InstallationHostGuard g,UUID run,String runtime){return new RssGoldExecution(j,t,w).inspect(g,run,runtime);}
    public static Prepared withOwner(Prepared p,WorkerRepository.Owner owner){var original=(RssGoldExecution.Prepared)p.token();return new Prepared(owner,new RssGoldExecution.Prepared(owner,original.checkpoint(),original.inputSha(),original.bytes()),p.bytes());}
    public static JsonNode modules(Path root)throws Exception {var n=SourceJson.object();for(String m:RssGoldRuntime.MODULES)n.put(m,SourceJson.sha(Files.readAllBytes(root.resolve(m))));return n;}
}
