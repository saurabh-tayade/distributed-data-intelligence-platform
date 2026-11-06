package platform.contentapi.execution;

import java.sql.Timestamp;
import java.time.Instant;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.support.TransactionCallback;
import org.springframework.transaction.support.TransactionTemplate;
import static org.assertj.core.api.Assertions.*;

/** Isolates the admission-history veto from process and ownership vetoes. */
class RecoveryOperatorInspectionTest {
    @ParameterizedTest @ValueSource(booleans={false,true})
    void narrowHistoryCannotQualifyEvenWithExactOwnerAndNegativeProcessEvidence(boolean narrowHistory) {
        UUID installation=UUID.randomUUID(),run=UUID.randomUUID(),attempt=UUID.randomUUID(),worker=UUID.randomUUID();
        var coordinator=new HashMap<String,Object>();
        coordinator.putAll(Map.of("installation_id",installation,"mode","RECOVERY_ONLY","active_attempt_id",attempt,
                "owner_instance_id",worker,"epoch",1L,"row_version",1L));
        coordinator.put("active_import_id",null);
        var target=new HashMap<String,Object>();
        target.putAll(Map.of("run_id",run,"target_attempt_id",attempt,"type","RECONCILE","control_state","REQUESTED",
                "control_version",1L,"expected_version",1L,"pipeline","CONTENT_RSS_V1","run_state","RUNNING",
                "run_version",1L,"current_attempt_id",attempt));
        target.putAll(Map.of("kind","EXECUTE","attempt_state","CLAIMED","instance_id",worker,"owner_epoch",1L,
                "pid",ProcessHandle.current().pid(),"process_started_at",Timestamp.from(Instant.EPOCH),"worker_status","READY"));
        var jdbc=new JdbcTemplate() {
            @Override public List<Map<String,Object>> queryForList(String sql) {
                if(sql.contains("FROM ddip_management.coordinator"))return List.of(coordinator);
                throw new AssertionError("Unexpected query: "+sql);
            }
            @Override public List<Map<String,Object>> queryForList(String sql,Object... args) {
                if(sql.contains("FROM ddip_management.controls"))return List.of(target);
                if(sql.contains("FROM ddip_management.stage_invocations"))return List.of();
                throw new AssertionError("Unexpected query: "+sql);
            }
            @Override public <T> T queryForObject(String sql,Class<T> type,Object... args) {
                assertThat(sql).contains("content_admission_policy IS NOT NULL");
                assertThat(args).containsExactly(run);
                return type.cast(narrowHistory);
            }
        };
        var tx=new ManagementTransactions(new TransactionTemplate() {
            @Override public <T> T execute(TransactionCallback<T> action) {return action.doInTransaction(null);}
        });
        var result=new RecoveryOperatorInspection(jdbc,tx,installation).inspect(UUID.randomUUID());
        assertThat(result.get("selectedOwnerCurrent").booleanValue()).isTrue();
        assertThat(result.get("recordedProcesses").get(0).get("state").stringValue()).isEqualTo("REUSED");
        assertThat(result.get("canQualify").booleanValue()).isEqualTo(!narrowHistory);
        assertThat(result.get("authorization").booleanValue()).isFalse();
    }
}
