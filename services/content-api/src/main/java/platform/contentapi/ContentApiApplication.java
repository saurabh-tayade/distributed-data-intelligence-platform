package platform.contentapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration;
import platform.contentapi.execution.RuntimeRole;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class})
public class ContentApiApplication {
    public static void main(String[] args) {
        org.springframework.context.ConfigurableApplicationContext context;
        try { context = SpringApplication.run(ContentApiApplication.class, args); }
        catch (RuntimeException ex) {
            // The operator role suppresses framework diagnostics which may contain private configuration.
            if (java.util.Arrays.asList(args).contains("--ddip.role=operator-recovery") ||
                    "operator-recovery".equals(System.getenv("DDIP_ROLE")) || "operator-recovery".equals(System.getProperty("ddip.role"))) {
                System.out.println("{\"status\":\"RECOVERY_REQUIRED\",\"code\":\"STARTUP_UNAVAILABLE\"}");
                System.exit(2);return;
            }
            throw ex;
        }
        if (RuntimeRole.from(context.getEnvironment()) == RuntimeRole.OPERATOR_IMPORT || RuntimeRole.from(context.getEnvironment()) == RuntimeRole.OPERATOR_RECOVERY) {
            int code=SpringApplication.exit(context);context.close();System.exit(code);
        } else if (RuntimeRole.from(context.getEnvironment()) == RuntimeRole.MIGRATE) context.close();
    }
}
