package platform.contentapi.execution;

import java.util.function.BooleanSupplier;
import org.springframework.jdbc.core.JdbcTemplate;

/** Package access only: invokes the production read-only qualifier, with no execution capability. */
public final class NarrowContentWorkerStartupTestSupport {
    private NarrowContentWorkerStartupTestSupport() {}
    public static void qualify(JdbcTemplate jdbc,ManagementTransactions tx,InstallationHostGuard guard,
            String profile,String digest,String deployment,String strict,BooleanSupplier stopping) {
        NarrowContentWorkerStartup.startup(jdbc,tx,guard,profile,digest,deployment,strict,stopping).check();
    }
}
