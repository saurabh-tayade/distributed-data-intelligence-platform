package platform.contentapi.management;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;

/** Real native original-run evidence survives an actual PostgreSQL restart, never authorizing replay. */
class NarrowContentPipelineRestartIT extends NarrowContentPipelineFixtures {
    @ParameterizedTest
    @CsvSource({"BRONZE_BEFORE_SILVER,2", "RECHECK_SILVER,5", "RECHECK_CONTENT,16"})
    void committedCheckpointSurvivesRestartWithoutContinuation(String operation, int step) throws Exception {
        original();var reached=new AtomicBoolean();
        assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,owner,PIPELINE_PROFILE,event->{
            if(event.operation().equals(operation) && event.boundary().equals("CHECKPOINT_COMMITTED")) {
                reached.set(true);throw new IllegalStateException("STOP_AFTER_CHECKPOINT");
            }
        })).hasMessage("STOP_AFTER_CHECKPOINT");
        assertThat(reached).isTrue();
        assertThat(count("content_fetch_checkpoints")).isEqualTo(1);
        assertThat(count("content_bronze_checkpoints")).isEqualTo(1);
        assertThat(count("content_pipeline_operations")).isEqualTo(step-1);
        assertThat(count("content_pipeline_checkpoints")).isEqualTo(step-1);
        assertThat(count("content_pipeline_completions")).isZero();
        assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,owner.runId())).isNull();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
        var before=allRows();var files=filesystem();int reads=objectRequests.get();
        restart();
        assertThat(allRows()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        assertHistory(step+1);
        assertThat(allRows()).isEqualTo(before);
        ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");
        var registered=allRows();var registeredFiles=filesystem();
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),owner.runId())).hasMessage("WORKER_NOT_READY");
        rejectReplay();
        assertThat(allRows()).isEqualTo(registered);assertThat(filesystem()).isEqualTo(registeredFiles);
        assertThat(objectRequests).hasValue(reads);assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
        noLegacyExecution();
    }

    @Test void lostFinalCommitAcknowledgmentRemainsVerifiedReadOnlyAfterRestart() throws Exception {
        original();var armed=new AtomicBoolean();var inserted=new AtomicBoolean();var uncertain=loseCommitAcknowledgment(armed);
        var uncertainTx=transactions(uncertain);var uncertainWorkers=WorkerTestSupport.enabled(uncertain,uncertainTx,INSTALL);
        assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(uncertain,uncertainTx,uncertainWorkers,guard,owner,PIPELINE_PROFILE,event->{
            if(event.boundary().equals("SUCCESS_INSERTED")){inserted.set(true);armed.set(true);}
        })).hasMessage("MANAGEMENT_UNAVAILABLE");
        assertThat(inserted).isTrue();assertThat(armed).isFalse();
        UUID artifact=jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,owner.runId());
        assertThat(artifact).isNotNull();assertThat(count("content_pipeline_completions")).isEqualTo(1);
        var before=allRows();var files=filesystem();int reads=objectRequests.get();
        restart();
        assertThat(allRows()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        NarrowContentPipelineSqlIT.assertCompletedGraphAndCounts(jdbc,owner.runId(),owner.attemptId(),artifact);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        var history=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY));
        for(int i=0;i<2;i++) {
            var detail=history.get(owner.runId().toString());
            assertThat(detail.get("result").get("artifactId").stringValue()).isEqualTo(artifact.toString());
            assertThat(detail.get("result").get("verificationStatus").stringValue()).isEqualTo("VERIFIED_AT_EXECUTION");
            assertThat(detail.get("counts").get("articles").stringValue()).isEqualTo("1");
            assertHistory(17);
        }
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        rejectReplay();
        assertThat(allRows()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        assertThat(objectRequests).hasValue(reads);assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);
        noLegacyExecution();
    }

    private void assertHistory(int expected) {
        var history=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY));
        var facts=history.nativeOperations(owner.runId().toString(),owner.attemptId().toString(),100,null).data();
        assertThat(facts).hasSize(expected);
        for(int i=0;i<facts.size();i++) {
            var fact=facts.get(i);assertThat(fact.get("stepIndex").intValue()).isEqualTo(i);
            assertThat(fact.get("state").stringValue()).isEqualTo("CHECKPOINTED");
            assertThat(fact.get("continuationAuthorized").booleanValue()).isFalse();
            assertThat(fact.get("proofSha256").stringValue()).matches("[a-f0-9]{64}");
            assertThat(fact.get("checkpointedAt").isNull()).isFalse();
        }
    }
    private void rejectReplay() {
        assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,owner,PIPELINE_PROFILE,e->{})).isInstanceOf(RuntimeException.class);
    }
    private void noLegacyExecution() {
        assertThat(count("rss_deliveries")).isZero();assertThat(count("stage_invocations")).isZero();
        assertThat(NarrowContentPipelineTestSupport.ordinaryContentDispatchAvailable()).isFalse();
    }
    private Map<String,List<String>> allRows() {
        var rows=new TreeMap<String,List<String>>();
        // Include proof bytes, profile rows, publication artifacts and every journal, not just counts.
        for(String table:jdbc.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='ddip_management' ORDER BY tablename",String.class))
            rows.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.\""+table.replace("\"","\"\"")+"\" t ORDER BY to_jsonb(t)::text",String.class));
        return rows;
    }
    private void restart() throws Exception {
        guard.close();guard=null;
        var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);
        String signal=System.getProperty("ddip.test.narrow-pipeline-restart-signal");
        if(signal!=null) {
            Path request=Path.of(signal),done=Path.of(signal+".done");
            Files.deleteIfExists(done);Files.writeString(request,"READY "+owner.attemptId());
            long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);
            while(!Files.exists(done) && System.nanoTime()<deadline)Thread.sleep(50);
            assertThat(Files.exists(done)).as("isolated PostgreSQL restart acknowledgment").isTrue();
            Files.deleteIfExists(request);
        } else {
            // Only an explicitly supplied disposable CI service, never an inferred host database.
            String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).matches("[0-9a-f]{12,64}");
            var process=new ProcessBuilder("docker","restart",container).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try{assertThat(process.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isZero();}
            finally{if(process.isAlive()){process.destroyForcibly();process.waitFor(5,TimeUnit.SECONDS);}}
        }
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
        while(true) {
            try {
                var after=jdbc.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);assertThat(after).isAfter(started);
                System.out.println("NARROW_PIPELINE_POSTGRES_RESTART before="+started+" after="+after+" attempt="+owner.attemptId());return;
            } catch(org.springframework.dao.DataAccessException ex){if(System.nanoTime()>deadline)throw ex;Thread.sleep(100);}
        }
    }
}
