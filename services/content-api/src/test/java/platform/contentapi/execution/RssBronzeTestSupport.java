package platform.contentapi.execution;

import java.util.*;
import java.util.function.BiConsumer;
import org.springframework.jdbc.core.JdbcTemplate;
public final class RssBronzeTestSupport {
    private RssBronzeTestSupport(){ }
    public record Stored(WorkerRepository.Owner owner,byte[] receipt){ }
    public record Inspection(String state,boolean receiptConfirmed,boolean retryClassification,byte[] receipt){ }
    public static Stored store(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,WorkerRepository.Owner owner,String runtime,BiConsumer<String,WorkerRepository.Owner> fault){var s=new RssBronzeExecution(jdbc,tx,workers,(p,stage)->fault.accept(p.name(),stage.owner())).store(guard,owner,"demo-feed",runtime);return new Stored(s.stage().owner(),s.receipt());}
    public static Inspection inspect(JdbcTemplate jdbc,ManagementTransactions tx,WorkerRepository workers,InstallationHostGuard guard,UUID run,String runtime){var r=new RssBronzeExecution(jdbc,tx,workers).inspect(guard,run,"demo-feed",runtime);return new Inspection(r.state(),r.receiptConfirmed(),r.retryClassification(),r.receipt());}
}
