package platform.contentapi.execution;

import java.util.Arrays;
import java.util.List;
import java.sql.SQLException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.flywaydb.core.Flyway;
import org.flywaydb.core.api.FlywayException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.dao.DataAccessException;

public final class SchemaLifecycle {
    private final Flyway flyway;
    private final JdbcTemplate jdbc;
    private final JdbcCoordinatorRepository coordinator;
    public SchemaLifecycle(Flyway flyway, JdbcTemplate jdbc, JdbcCoordinatorRepository coordinator) {
        this.flyway=flyway; this.jdbc=jdbc; this.coordinator=coordinator;
    }
    public void verify(boolean migrate) {
        try {
            // Connectivity is checked separately so outages are not misreported as schema conflicts.
            jdbc.execute((ConnectionCallback<Void>) connection -> {
                if(connection.getMetaData().getDatabaseMajorVersion()!=17)
                    throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_INCOMPATIBLE);
                return null;
            });
        } catch (DataAccessException ex) {
            throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_UNAVAILABLE);
        }
        try {
            boolean initialSchema = migrate && flyway.info().current()==null;
            boolean appliedMigrations = migrate && flyway.migrate().migrationsExecuted > 0;
            var validation=flyway.validateWithResult();
            if (!validation.validationSuccessful || flyway.info().pending().length!=0 || Arrays.stream(flyway.info().applied()).filter(i -> i.getVersion()!=null).count()!=21)
                throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_INCOMPATIBLE);
            try {
                for(String table: List.of("storage_bindings","execution_profiles","execution_profile_storage","worker_instances",
                        "coordinator","sources","source_revisions","artifacts","artifact_dependencies","runs","run_sources",
                        "run_inputs","attempts","stage_invocations","stage_artifacts","rss_deliveries","controls","command_bindings","audit_events","artifact_import_operations",
                        "content_attempt_admissions","content_admission_milestones","content_operations","content_operation_phases","content_native_operations","content_fetch_launch_profiles","content_native_launch_phases","content_fetch_completion_intents","content_fetch_checkpoints","content_fetch_recovery_handoffs","content_fetch_recovery_finalizations","content_bronze_reservations","content_bronze_launch_profiles","content_bronze_launch_phases","content_bronze_verification_profiles","content_bronze_verification_phases","content_bronze_checkpoints","content_silver_reservations","content_pipeline_profiles","content_pipeline_operations","content_pipeline_phases","content_pipeline_checkpoints","content_pipeline_completions")) {
                    jdbc.execute("SELECT * FROM ddip_management."+table+" LIMIT 0");
                }
            } catch(DataAccessException ex) {
                throw new ManagementFailure(connectionFailure(ex) ? ManagementFailure.Code.MANAGEMENT_UNAVAILABLE : ManagementFailure.Code.MANAGEMENT_INCOMPATIBLE);
            }
            if (initialSchema && appliedMigrations) coordinator.initialize();
            coordinator.read();
        } catch (FlywayException ex) {
            throw new ManagementFailure(connectionFailure(ex) ? ManagementFailure.Code.MANAGEMENT_UNAVAILABLE : ManagementFailure.Code.MANAGEMENT_INCOMPATIBLE);
        }
    }
    private static boolean connectionFailure(Throwable exception) {
        for(Throwable cause=exception; cause!=null; cause=cause.getCause()) {
            if(cause instanceof SQLException sql && sql.getSQLState()!=null && sql.getSQLState().startsWith("08")) return true;
        }
        return false;
    }
}
