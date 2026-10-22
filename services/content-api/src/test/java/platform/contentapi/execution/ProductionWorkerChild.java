package platform.contentapi.execution;

import java.util.*;
import org.springframework.boot.builder.SpringApplicationBuilder;
import platform.contentapi.ContentApiApplication;

/** Separate JVM production-lifecycle harness. No test capability or replacement native adapter. */
public final class ProductionWorkerChild {
    public static void main(String[] args)throws Exception {
        var arguments=new ArrayList<>(List.of(args));
        arguments.add("--ddip.database.url="+System.getenv("DDIP_TEST_DB_URL"));
        arguments.add("--ddip.database.username="+System.getenv("DDIP_TEST_DB_USER"));
        arguments.add("--ddip.database.password="+System.getenv("DDIP_TEST_DB_PASSWORD"));
        try(var context=new SpringApplicationBuilder(ContentApiApplication.class).run(arguments.toArray(String[]::new))) {
            System.out.println("PRODUCTION_WORKER_READY");System.out.flush();System.in.read();
        }
    }
}
