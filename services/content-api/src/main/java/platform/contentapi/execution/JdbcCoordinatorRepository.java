package platform.contentapi.execution;

import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;

public final class JdbcCoordinatorRepository implements CoordinatorRepository {
    private final JdbcTemplate jdbc;
    private final ManagementTransactions transactions;
    private final UUID installation;
    public JdbcCoordinatorRepository(JdbcTemplate jdbc, ManagementTransactions transactions, UUID installation) {
        this.jdbc=jdbc; this.transactions=transactions; this.installation=installation;
    }
    /** Explicit migration bootstrap only; never clears/replaces an occupied or foreign installation. */
    public void initialize() {
        transactions.required(() -> {
            jdbc.update("INSERT INTO ddip_management.coordinator(singleton_id,installation_id,mode) VALUES (1,?,'READY') ON CONFLICT DO NOTHING",installation);
            readLocked(); return null;
        });
    }
    private Snapshot map(java.sql.ResultSet rs, int row) throws java.sql.SQLException {
        return new Snapshot(rs.getObject("installation_id", UUID.class),Mode.valueOf(rs.getString("mode")),rs.getLong("epoch"),
                rs.getObject("active_attempt_id",UUID.class),rs.getObject("owner_instance_id",UUID.class),rs.getLong("row_version"));
    }
    private Snapshot checked(Snapshot snapshot) {
        if (!installation.equals(snapshot.installationId())) throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_INCOMPATIBLE);
        return snapshot;
    }
    private Snapshot readLocked() {
        var rows=jdbc.query("SELECT * FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE",this::map);
        if(rows.size()!=1) throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_INCOMPATIBLE);
        return checked(rows.getFirst());
    }
    @Override public Snapshot read() {
        return transactions.required(() -> {
            var rows=jdbc.query("SELECT * FROM ddip_management.coordinator WHERE singleton_id=1",this::map);
            if(rows.size()!=1) throw new ManagementFailure(ManagementFailure.Code.MANAGEMENT_INCOMPATIBLE);
            return checked(rows.getFirst());
        });
    }
    @Override public boolean compareAndSetIdleMode(long expectedVersion, Mode mode) {
        if(expectedVersion < 0 || mode==null) throw new IllegalArgumentException("INVALID_COORDINATOR_UPDATE");
        return transactions.required(() -> jdbc.update("UPDATE ddip_management.coordinator SET mode=?,row_version=row_version+1 " +
                "WHERE singleton_id=1 AND installation_id=? AND row_version=? AND active_attempt_id IS NULL",
                mode.name(),installation,expectedVersion)==1);
    }
    @Override public boolean isCurrentOwner(UUID attemptId, UUID instanceId, long epoch) {
        Snapshot s=read();
        return attemptId!=null && instanceId!=null && attemptId.equals(s.activeAttemptId()) && instanceId.equals(s.ownerInstanceId()) && epoch==s.epoch();
    }
}
