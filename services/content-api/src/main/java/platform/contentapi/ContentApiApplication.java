package platform.contentapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.jdbc.autoconfigure.DataSourceAutoConfiguration;
import org.springframework.boot.jdbc.autoconfigure.DataSourceTransactionManagerAutoConfiguration;
import platform.contentapi.execution.RuntimeRole;

@SpringBootApplication(exclude = {DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class})
public class ContentApiApplication {
    public static void main(String[] args) {
        var context = SpringApplication.run(ContentApiApplication.class, args);
        if (RuntimeRole.from(context.getEnvironment()) == RuntimeRole.OPERATOR_IMPORT) {
            int code=SpringApplication.exit(context);context.close();System.exit(code);
        } else if (RuntimeRole.from(context.getEnvironment()) == RuntimeRole.MIGRATE) context.close();
    }
}
