package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import java.util.UUID;
import org.springframework.jdbc.core.JdbcTemplate;
import static org.assertj.core.api.Assertions.*;

/** SQL-only closure probes against an already committed, independently qualified receipt. */
public final class ContentTerminalSqlTestSupport {
    private ContentTerminalSqlTestSupport() { }
    /** Returns the number of independently derived absent addresses tested; never invents native proofs. */
    public static int assertFrozenBindings(JdbcTemplate jdbc,UUID terminalId) {
        var beforeArtifacts=jdbc.queryForList("SELECT to_jsonb(a)::text FROM ddip_management.artifacts a ORDER BY artifact_id",String.class);
        var beforeEdges=jdbc.queryForList("SELECT to_jsonb(d)::text FROM ddip_management.artifact_dependencies d ORDER BY artifact_id,role,ordinal",String.class);
        var snapshot=SourceJson.parse(jdbc.queryForObject("SELECT snapshot_bytes FROM ddip_management.content_original_terminal_receipts WHERE terminal_id=?",byte[].class,terminalId));
        int absent=0;
        for(var binding:snapshot.get("artifactBindings")) {
            if(binding.get("artifact").isNull()) {
                absent++;byte[] evidence="{}".getBytes(StandardCharsets.UTF_8);
                assertRejected(()->jdbc.update("INSERT INTO ddip_management.artifacts(artifact_id,kind,product_id,contract_version,processor_revision,native_identity,storage_binding_id,relative_address,evidence_bytes,evidence_sha256,verified_at) VALUES (?,?,'content-intelligence',1,1,'closure-probe',?,?,?,?,clock_timestamp())",UUID.randomUUID(),binding.get("kind").stringValue(),binding.get("bindingId").stringValue(),binding.get("relativeAddress").stringValue(),evidence,SourceJson.sha(evidence)),"CONTENT_TERMINAL_ARTIFACT_CLOSED");
            } else {
                UUID id=UUID.fromString(binding.get("artifact").get("artifact_id").stringValue());
                assertRejected(()->jdbc.update("INSERT INTO ddip_management.artifact_dependencies(artifact_id,role,ordinal,dependency_artifact_id) VALUES (?,'TERMINAL_CLOSURE_PROBE',0,?)",id,UUID.randomUUID()),"CONTENT_TERMINAL_DEPENDENCIES_CLOSED");
            }
        }
        assertThat(jdbc.queryForList("SELECT to_jsonb(a)::text FROM ddip_management.artifacts a ORDER BY artifact_id",String.class)).isEqualTo(beforeArtifacts);
        assertThat(jdbc.queryForList("SELECT to_jsonb(d)::text FROM ddip_management.artifact_dependencies d ORDER BY artifact_id,role,ordinal",String.class)).isEqualTo(beforeEdges);
        return absent;
    }
    private static void assertRejected(Runnable action,String code) {
        assertThatThrownBy(action::run).satisfies(error->{Throwable root=error;while(root.getCause()!=null)root=root.getCause();assertThat(root).isInstanceOf(java.sql.SQLException.class);assertThat(((java.sql.SQLException)root).getSQLState()).isEqualTo("23514");assertThat(root.getMessage()).contains(code);});
    }
}
