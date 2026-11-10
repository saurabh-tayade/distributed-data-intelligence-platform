package platform.contentapi.execution;

import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.*;
import platform.contentapi.management.SourceJson;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Native evidence service tests. Terminal metadata is a fixture, not proof of a real recovery flow. */
class ReliabilityRetryEvidenceIT extends ReliabilityExecutionFixtures {
    UUID recovery;
    @BeforeEach void terminalMetadata(){
        recovery=UUID.randomUUID();
        jdbc.update("UPDATE ddip_management.attempts SET state='INTERRUPTED',finished_at=now(),row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
        jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code) VALUES (?,?,2,'RECONCILE',?,2,'SUCCEEDED',now(),'RECONCILED_FAILED')",recovery,run,owner.worker().instanceId());
        jdbc.update("UPDATE ddip_management.runs SET state='FAILED',current_attempt_id=?,finished_at=now(),outcome_code='PUBLICATION_ABSENT',retry_eligible=true,row_version=row_version+1 WHERE run_id=?",recovery,run);
        jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,epoch=2,row_version=row_version+1");
    }
    JsonNode frozen(){return SourceJson.parse(jdbc.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run),PreflightFiles.LARGE);}
    ReliabilityRetryEvidence.Proof proof(){return new ReliabilityRetryEvidence(jdbc,tx).verify(guard,run,UUID.randomUUID(),frozen(),RUNTIME);}
    @Test void realFreshProofDoesNotClaimAttemptOrWriteNativeOutput(){
        var p=proof();p.recheck(guard);assertThat(p.fingerprint()).matches("[0-9a-f]{64}");assertThat(p.sha256()).matches("[0-9a-f]{64}");assertThat(p.address()).startsWith("retry-");
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Integer.class)).isEqualTo(2);assertThat(state()).isEqualTo("FAILED");
        assertThat(Files.exists(Path.of(fixture.get("bundle").get("bindings").get("output").get("root").stringValue()).resolve("v2"))).isFalse();
    }
    @Test void incompleteNamespaceBlocksQualificationAndProofRecheck()throws Exception{
        var p=proof();Path output=Path.of(fixture.get("bundle").get("bindings").get("output").get("root").stringValue());Files.createDirectories(output.resolve("v2/processor_revision=2/publication="+p.fingerprint()));
        assertThatThrownBy(()->p.recheck(guard)).isInstanceOf(RecoveryFailure.class);assertThatThrownBy(this::proof).isInstanceOf(RecoveryFailure.class);
    }
    @Test void modifiedRetainedResultCannotAuthorizeRequeue()throws Exception{
        var p=proof();Files.writeString(binding.root().resolve(p.address()).resolve("result.json"),"{}\n");assertThatThrownBy(()->p.recheck(guard)).isInstanceOf(RecoveryFailure.class);
    }
    @Test void explicitPriorIdentitySupportsQueuedRecheckWithoutInventingAttempt(){
        jdbc.update("UPDATE ddip_management.runs SET state='QUEUED',current_attempt_id=NULL,finished_at=NULL,row_version=row_version+1 WHERE run_id=?",run);
        var p=new ReliabilityRetryEvidence(jdbc,tx).verify(guard,run,UUID.randomUUID(),recovery,frozen(),RUNTIME);p.recheck(guard);assertThat(state()).isEqualTo("QUEUED");assertThatThrownBy(this::proof).isInstanceOf(RecoveryFailure.class);
    }
    @Test void changedDependencyBlocksFreshQualification()throws Exception{
        var p=proof();Path root=Path.of(fixture.get("bundle").get("bindings").get("reference").get("root").stringValue());
        try(var paths=Files.walk(root)){Files.writeString(paths.filter(Files::isRegularFile).findFirst().orElseThrow(),"changed",StandardOpenOption.APPEND);}
        assertThatThrownBy(()->p.recheck(guard)).isInstanceOf(RecoveryFailure.class);assertThatThrownBy(this::proof).isInstanceOf(RecoveryFailure.class);
    }
    @Test void symlinkedOutputParentBlocksFreshQualification()throws Exception{
        Path root=Path.of(fixture.get("bundle").get("bindings").get("output").get("root").stringValue());Files.createSymbolicLink(root.resolve("v2"),Files.createDirectory(work.resolve("other-output")));
        assertThatThrownBy(this::proof).isInstanceOf(RecoveryFailure.class);
    }

}
