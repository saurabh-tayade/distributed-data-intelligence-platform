package platform.contentapi.execution;

import java.nio.file.*;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import platform.contentapi.management.SourceJson;

/** A real separately guarded resumed executor, stopped at an acknowledged native boundary. */
public final class ContentInheritedRecoveryProcess {
    public static void main(String[] args)throws Exception{
        Path root=Path.of(args[0]);var n=SourceJson.parse(Files.readAllBytes(root.resolve("owner.json")));
        var j=new JdbcTemplate(new DriverManagerDataSource(n.get("url").stringValue(),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
        var tx=ReliabilityExecutionFixtures.transactions(j);var binding=OwnershipFixtures.readBinding(root.resolve("binding.json"));
        try(var guard=InstallationHostGuard.acquire(binding)){
            try{
                new ContentResume(j,tx,binding.installationId()).execute(guard,UUID.fromString(args[1]),n.get("runtime").stringValue(),(phase,boundary,owner)->{
                    if(phase.equals(args[2]) && boundary==(phase.equals("RESUME")?RssContentExecution.Boundary.INTENT:RssContentExecution.Boundary.ACKNOWLEDGED))throw new IllegalStateException("TEST_INTERRUPTION");
                });throw new AssertionError("Interruption not reached");
            }catch(IllegalStateException ex){if(!ex.getMessage().equals("TEST_INTERRUPTION"))throw ex;}
            Files.writeString(root.resolve("resumed-ready"),"READY");System.in.read();
        }
    }
}
