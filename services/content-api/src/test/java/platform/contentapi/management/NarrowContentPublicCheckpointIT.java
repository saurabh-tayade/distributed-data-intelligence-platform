package platform.contentapi.management;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/**
 * Real SQL and synthetic native-format journal component evidence only.
 * No public HTTP success, native child, recovered Owner, or follow-on capability is fabricated.
 */
class NarrowContentPublicCheckpointIT extends NarrowContentPublicCheckpointFixtures {
    @Test void syntheticPublicCandidateVerificationAndCheckpointReopenAreReadOnlyFacts()throws Exception{
        candidate();var before=history();var files=filesystem();
        var observation=NarrowContentFetchCandidateVerificationTestSupport.inspect(jdbc,tx,guard,operation);
        assertThat(observation.rawHttpJournalVerified()).isTrue();assertThat(observation.completed()).isFalse();assertThat(observation.nativeLaunchCompatible()).isFalse();
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        var completed=NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});
        assertThat(completed.checkpointed()).isTrue();assertThat(completed.nativeLaunchCompatible()).isFalse();
        assertThat(count("content_fetch_checkpoints")).isEqualTo(1);assertThat(count("content_bronze_reservations")).isZero();
        before=history();files=filesystem();
        assertThat(NarrowContentFetchCompletionTestSupport.inspect(jdbc,tx,null,operation)).contains(completed);
        var assessment=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId());
        assertThat(assessment.prefix()).isEqualTo("FETCH_CHECKPOINT");assertThat(assessment.checkpointSteps()).containsExactly(0);
        assertThat(assessment.integrity()).isEqualTo("CURRENT_BYTES_AND_INVENTORY_MATCH");assertThat(assessment.authorization()).isFalse();assertThat(assessment.nativeLaunchCompatible()).isFalse();
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        Files.move(attemptRoot(),temp.resolve("unavailable-synthetic-public-checkpoint"));guard.close();guard=null;
        assertThat(NarrowContentFetchCompletionTestSupport.inspect(jdbc,tx,null,operation)).contains(completed);
        assertThat(NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,null,owner.runId(),owner.attemptId()).prefix()).isEqualTo("FETCH_CHECKPOINT");
        assertThat(history()).isEqualTo(before);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"payload.xml","metadata.json","descriptor.json","result.json"})
    void changedSyntheticCandidateNeverConsumesCompletionIntent(String name)throws Exception{
        candidate();Files.writeString(candidateDirectory.resolve(name),"changed-component-fixture");var before=history();var files=filesystem();
        assertThatThrownBy(()->NarrowContentFetchCandidateVerificationTestSupport.inspect(jdbc,tx,guard,operation)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(()->NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{})).isInstanceOf(RuntimeException.class);
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(count("content_fetch_completion_intents")).isZero();noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"metadata-source","metadata-version","descriptor-endpoint","descriptor-runtime","descriptor-unknown"})
    void selfConsistentCandidateInventoryDoesNotReplaceNativeSemanticVerification(String change)throws Exception{
        released();candidateDirectory=NarrowContentPublicCheckpointTestSupport.syntheticCandidate(jdbc,guard,operation);
        Path path=candidateDirectory.resolve(change.startsWith("metadata")?"metadata.json":"descriptor.json");var n=(ObjectNode)SourceJson.parse(Files.readAllBytes(path));
        switch(change){
            case "metadata-source"->n.put("sourceUri","https://feeds.example.org/two");
            case "metadata-version"->n.put("schemaVersion",2);
            case "descriptor-endpoint"->n.put("endpointSha256","0".repeat(64));
            case "descriptor-runtime"->n.put("runtimeSha256","0".repeat(64));
            case "descriptor-unknown"->n.put("resolvedAddress","127.0.0.1");
        }
        byte[] changed=SourceJson.canonical(n);Files.write(path,changed);
        if(change.startsWith("metadata")){
            Path descriptor=candidateDirectory.resolve("descriptor.json");var d=(ObjectNode)SourceJson.parse(Files.readAllBytes(descriptor));
            ((ObjectNode)d.get("expected")).put("metadataByteLength",changed.length).put("metadataSha256",SourceJson.sha(changed));Files.write(descriptor,SourceJson.canonical(d));
        }
        phase("CANDIDATE_EXITED");var before=history();var files=filesystem();
        assertThat(launchFacts().candidateSha256()).isEqualTo(NarrowContentPublicCheckpointTestSupport.candidate(candidateDirectory,command()));
        assertThatThrownBy(()->NarrowContentFetchCandidateVerificationTestSupport.inspect(jdbc,tx,guard,operation)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(()->NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{})).isInstanceOf(RuntimeException.class);
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(count("content_fetch_completion_intents")).isZero();noExecution();
    }
    @Test void completePublicCheckpointIsRejectedByOldJavaHandoffBeforeBronze()throws Exception{
        var completed=checkpoint();recover();var before=history();var files=filesystem();
        var clock=Clock.fixed(Instant.now(),ZoneOffset.UTC);
        var handoff=new NarrowContentFetchRecoveryHandoffTestSupport(jdbc,tx,INSTALL,clock,()->"ABSENT",b->{});
        assertThat(NarrowContentFetchCompletionTestSupport.inspect(jdbc,tx,null,operation)).contains(completed);
        assertThatThrownBy(()->handoff.inspect(guard,recovery,control,UUID.randomUUID(),UUID.randomUUID())).hasMessage("EVIDENCE_UNAVAILABLE");
        assertThat(count("content_bronze_reservations")).isZero();assertThat(count("content_fetch_recovery_handoffs")).isZero();
        assertThat(count("attempts")).isEqualTo(1);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());noExecution();
    }
    @Test void directSqlPublicHandoffHitsVersionGateAndRollsBackAllStagedMetadata()throws Exception{
        checkpoint();recover();var before=history();var files=filesystem();
        var raw=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));
        assertThatThrownBy(()->raw.execute(status->{
            // Component-only hostile SQL transaction. No production token/guard bypass is used.
            // It must reach the explicit v1-only handoff gate, then roll every staged row back.
            UUID successor=UUID.randomUUID(),handoff=UUID.randomUUID();
            var c=jdbc.queryForMap("SELECT * FROM ddip_management.coordinator WHERE singleton_id=1 FOR UPDATE");
            var r=jdbc.queryForMap("SELECT * FROM ddip_management.runs WHERE run_id=? FOR UPDATE",owner.runId());
            var a=jdbc.queryForMap("SELECT * FROM ddip_management.attempts WHERE attempt_id=? FOR UPDATE",owner.attemptId());
            var old=jdbc.queryForMap("SELECT * FROM ddip_management.worker_instances WHERE instance_id=?",owner.worker().instanceId());
            var next=jdbc.queryForMap("SELECT * FROM ddip_management.worker_instances WHERE instance_id=?",recovery.instanceId());
            var ctl=jdbc.queryForMap("SELECT * FROM ddip_management.controls WHERE control_id=? FOR UPDATE",control);
            long epoch=((Number)c.get("epoch")).longValue();String policy="ddip-content-fetch-checkpoint-recovery-v1";
            jdbc.update("UPDATE ddip_management.attempts SET state='INTERRUPTED',finished_at=clock_timestamp(),outcome_code='INTERRUPTED',row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
            jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,content_recovery_policy) VALUES (?,?,2,'RECONCILE',?,?,'CLAIMED',?)",successor,owner.runId(),recovery.instanceId(),epoch+1,policy);
            jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=?,owner_instance_id=?,epoch=?,mode='RECOVERY_ONLY',heartbeat_at=clock_timestamp(),row_version=row_version+1 WHERE singleton_id=1",successor,recovery.instanceId(),epoch+1);
            jdbc.update("UPDATE ddip_management.runs SET state='RECOVERY_REQUIRED',current_attempt_id=?,outcome_code='RECOVERY_REQUIRED',retry_eligible=false,row_version=row_version+1 WHERE run_id=?",successor,owner.runId());
            byte[] unused=SourceJson.canonical(SourceJson.object());
            UUID checkpoint=jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_fetch_checkpoints WHERE operation_id=?",UUID.class,operation);
            jdbc.update("INSERT INTO ddip_management.content_fetch_recovery_handoffs(handoff_id,identity_version,policy,run_id,control_id,original_attempt_id,recovery_attempt_id,operation_id,checkpoint_id,original_instance_id,recovery_instance_id,original_epoch,original_coordinator_version,original_run_version,original_attempt_version,original_worker_version,recovery_worker_version,control_version,review_bytes,review_sha256,ancestry_bytes,ancestry_sha256) VALUES (?,1,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    handoff,policy,owner.runId(),control,owner.attemptId(),successor,operation,checkpoint,owner.worker().instanceId(),recovery.instanceId(),epoch,c.get("row_version"),r.get("row_version"),a.get("row_version"),old.get("row_version"),next.get("row_version"),ctl.get("row_version"),unused,SourceJson.sha(unused),unused,SourceJson.sha(unused));
            throw new AssertionError("PUBLIC_HANDOFF_ACCEPTED");
        })).hasStackTraceContaining("CONTENT_FETCH_RECOVERY_V1_REQUIRED");
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(count("content_bronze_reservations")).isZero();
        assertThat(count("content_fetch_recovery_handoffs")).isZero();assertThat(count("attempts")).isEqualTo(1);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"RELEASE_AUTHORIZED","CANDIDATE_EXITED","FETCH_CHECKPOINT"})
    void originalPublicSafeEndRetainsSyntheticPrefixWithoutFollowOnAuthority(String prefix)throws Exception{
        if(prefix.equals("RELEASE_AUTHORIZED")){released();NarrowContentPublicCheckpointTestSupport.syntheticCommand(jdbc,guard,operation);}
        else if(prefix.equals("CANDIDATE_EXITED"))candidate();else checkpoint();
        recover();var clock=Clock.fixed(Instant.now(),ZoneOffset.UTC);var files=filesystem();
        var terminal=new NarrowContentTerminalTestSupport(jdbc,tx,INSTALL,clock,()->"ABSENT",b->{});UUID id=UUID.randomUUID();
        var target=terminal.inspect(guard,owner.runId(),owner.attemptId(),control,UUID.randomUUID(),id);
        var qualified=terminal.qualify(guard,target,clock.instant(),"");assertThat(terminal.finish(guard,qualified)).isEqualTo("COMMITTED");
        assertThat(terminal.lookup(id)).isEqualTo("CONFIRMED_TERMINAL");assertThat(filesystem()).isEqualTo(files);
        assertThat(jdbc.queryForObject("SELECT state||':'||outcome_code FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("FAILED:NATIVE_CONTENT_INTERRUPTED");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(count("content_fetch_checkpoints")).isEqualTo(prefix.equals("FETCH_CHECKPOINT")?1:0);
        assertThat(count("content_bronze_reservations")).isZero();assertThat(count("content_fetch_recovery_handoffs")).isZero();assertThat(count("content_fetch_recovery_finalizations")).isZero();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE kind='EXECUTE'",Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE state IN ('CLAIMED','RUNNING')",Long.class)).isZero();
    }
}
