package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import org.springframework.boot.builder.SpringApplicationBuilder;
import platform.contentapi.ContentApiApplication;

/** Separate-process test harness only; never packaged in the application JAR. */
public final class WorkerTestChild {
    public static void main(String[] args)throws Exception {
        var b=OwnershipFixtures.readBinding(Path.of(args[0]));
        try {
            if(args[1].equals("passive")) {
                var e=System.getenv();try(var context=new SpringApplicationBuilder(ContentApiApplication.class).run(
                        "--ddip.role=worker","--ddip.database.url="+e.get("DDIP_TEST_DB_URL"),"--ddip.database.username="+e.get("DDIP_TEST_DB_USER"),"--ddip.database.password="+e.get("DDIP_TEST_DB_PASSWORD"),"--ddip.installation-id="+b.installationId(),"--ddip.worker.execution-profile-id="+args[2],"--ddip.worker.control-binding-id="+b.id(),"--ddip.worker.deployment-sha256="+"a".repeat(64),"--ddip.worker.python="+e.get("DDIP_TEST_PYTHON"),"--ddip.worker.java-home="+System.getProperty("java.home"),"--ddip.worker.processing-root="+Path.of("../../processing").toAbsolutePath().normalize(),"--logging.level.root=OFF","--spring.main.banner-mode=off")) {
                    if(context instanceof org.springframework.boot.web.server.context.WebServerApplicationContext)throw new AssertionError("WEB_WORKER");
                    System.out.println("C1_READY");System.out.flush();System.in.read();
                }
            } else {
                var jdbc=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_TEST_DB_URL"),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));
                var tx=new ManagementTransactions(new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource())));
                try(var guard=InstallationHostGuard.acquire(b)) {
                    var repo=WorkerTestSupport.enabled(jdbc,tx,b.installationId());String hash=jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,args[2]);
                    var session=repo.register(guard,args[2],hash,"a".repeat(64));var claimed=repo.claim(guard,session,UUID.randomUUID());
                    if(!claimed.status().equals("CLAIMED"))throw new AssertionError("NO_CLAIM");
                    System.out.println("C1_CLAIMED");System.out.flush();System.in.read();
                }
            }
        }catch(Exception ex){
            Throwable cause=ex;while(cause.getCause()!=null)cause=cause.getCause();
            System.out.println("C1_"+(cause instanceof ImportOwnershipFailure f?f.getMessage():"START_FAILED"));System.out.flush();System.exit(2);
        }
    }
}
