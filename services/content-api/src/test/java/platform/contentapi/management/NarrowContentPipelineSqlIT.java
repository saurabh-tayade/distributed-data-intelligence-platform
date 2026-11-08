package platform.contentapi.management;

import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Direct SQL attempts cannot rewrite a rehashed native command or skip a live-chain boundary. */
class NarrowContentPipelineSqlIT extends NarrowContentPipelineFixtures {
    String insertSql;Object[] operation;
    void capture(){
        original();var j=new JdbcTemplate(jdbc.getDataSource()){
            @Override public int update(String sql,Object... args){
                if(sql.startsWith("INSERT INTO ddip_management.content_pipeline_operations(")){
                    insertSql=sql;operation=args.clone();operation[9]=((byte[])args[9]).clone();throw new IllegalStateException("CAPTURED_PIPELINE_OPERATION");
                }return super.update(sql,args);
            }
        };
        var t=transactions(j);var w=WorkerTestSupport.enabled(j,t,INSTALL);
        assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(j,t,w,guard,owner,PIPELINE_PROFILE,e->{})).hasMessage("CAPTURED_PIPELINE_OPERATION");
        assertThat(operation).isNotNull();assertThat(count("content_pipeline_operations")).isZero();
    }
    void insert(){jdbc.update(insertSql,operation);}
    @Test void originalReservationIsOneUseAndImmutable(){
        capture();int calls=objectRequests.get();insert();assertThat(count("content_pipeline_operations")).isEqualTo(1);
        assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);
        for(String sql:List.of("UPDATE ddip_management.content_pipeline_operations SET created_at=created_at",
                "DELETE FROM ddip_management.content_pipeline_operations","TRUNCATE ddip_management.content_pipeline_operations CASCADE"))
            assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(RuntimeException.class);
        assertThat(objectRequests).hasValue(calls);assertThat(count("content_pipeline_phases")).isZero();
    }
    @Test void visibleAttemptStageExclusionStillWaitsForCoordinatorFence()throws Exception{
        capture();var locked=new java.util.concurrent.CountDownLatch(1);
        var release=new java.util.concurrent.CountDownLatch(1);var started=new java.util.concurrent.CountDownLatch(1);
        try(var pool=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()){
            var holder=pool.submit(()->tx.required(()->{
                insert(); // Keep the first pipeline operation uncommitted while the legacy writer starts.
                locked.countDown();try{assertThat(release.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();}
                catch(InterruptedException ex){throw new RuntimeException(ex);}return null;
            }));
            assertThat(locked.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            var stage=pool.submit(()->{
                var other=new JdbcTemplate(jdbc.getDataSource());other.setQueryTimeout(5);started.countDown();
                assertThatThrownBy(()->other.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?)",
                        owner.attemptId(),UUID.randomUUID(),"a".repeat(64)))
                        .hasStackTraceContaining("CONTENT_PIPELINE_LEGACY_HISTORY_FORBIDDEN");
            });
            assertThat(started.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            Thread.sleep(150);assertThat(stage.isDone()).isFalse();release.countDown();
            holder.get(10,java.util.concurrent.TimeUnit.SECONDS);stage.get(10,java.util.concurrent.TimeUnit.SECONDS);
        }finally{release.countDown();}
        assertThat(count("stage_invocations")).isZero();assertThat(count("content_pipeline_operations")).isEqualTo(1);
    }
    @ParameterizedTest @ValueSource(strings={"step","stage","ordinal","slot","native-slot","predecessor","profile","cv","wv","rv","av"})
    void alteredJournalCoordinatesAndOwnerCannotReserve(String key){
        capture();switch(key){case "step"->operation[4]=3;case "stage"->operation[5]="SILVER";case "ordinal"->operation[6]=1;
            case "slot"->operation[7]=UUID.randomUUID();case "native-slot"->operation[8]=UUID.randomUUID();case "predecessor"->operation[13]=UUID.randomUUID();
            case "profile"->operation[12]="0".repeat(64);case "cv"->operation[14]=((Number)operation[14]).longValue()+1;
            case "wv"->operation[15]=((Number)operation[15]).longValue()+1;case "rv"->operation[16]=((Number)operation[16]).longValue()+1;
            case "av"->operation[17]=((Number)operation[17]).longValue()+1;}
        assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);assertThat(count("content_pipeline_operations")).isZero();
    }
    @ParameterizedTest @ValueSource(strings={"mode","runtime","receipt","bronze","unknown","boolean-version","float-version"})
    void rehashedNativeCommandsCannotChangeFrozenMeaning(String key){
        capture();var n=(ObjectNode)SourceJson.parse((byte[])operation[9]);
        switch(key){case "mode"->n.put("mode","PUBLISH");case "runtime"->n.put("runtimeSha256","0".repeat(64));
            case "receipt"->((ObjectNode)n.get("receipt")).put("ingestionId",UUID.randomUUID().toString());
            case "bronze"->((ObjectNode)n.get("bronze")).put("bucket","different-bucket");case "unknown"->n.put("unexpected",true);
            case "boolean-version"->n.put("protocolVersion",true);case "float-version"->{} }
        operation[9]=SourceJson.canonical(n);
        if(key.equals("float-version"))operation[9]=new String((byte[])operation[9],java.nio.charset.StandardCharsets.UTF_8)
                .replace("\"protocolVersion\":2", "\"protocolVersion\":2.0").getBytes(java.nio.charset.StandardCharsets.UTF_8);
        operation[10]=SourceJson.sha((byte[])operation[9]);
        assertThatThrownBy(this::insert).isInstanceOf(RuntimeException.class);assertThat(count("content_pipeline_operations")).isZero();
    }
    static void assertCompletedGraphAndCounts(JdbcTemplate jdbc,UUID run,UUID attempt,UUID completedArtifact){
        var protectedArtifacts=jdbc.queryForList("SELECT artifact_id FROM ddip_management.content_pipeline_completions WHERE attempt_id=? "
                +"UNION SELECT d.dependency_artifact_id FROM ddip_management.artifact_dependencies d JOIN ddip_management.content_pipeline_completions c ON c.artifact_id=d.artifact_id WHERE c.attempt_id=? "
                +"UNION SELECT leaf.dependency_artifact_id FROM ddip_management.artifact_dependencies leaf JOIN ddip_management.artifact_dependencies parent ON parent.dependency_artifact_id=leaf.artifact_id "
                +"JOIN ddip_management.content_pipeline_completions c ON c.artifact_id=parent.artifact_id WHERE c.attempt_id=?",UUID.class,attempt,attempt,attempt);
        assertThat(protectedArtifacts).hasSize(4).contains(completedArtifact);
        for(UUID artifact:protectedArtifacts)assertThatThrownBy(()->jdbc.update(
                "INSERT INTO ddip_management.artifact_dependencies(artifact_id,role,ordinal,dependency_artifact_id) VALUES (?,'FORBIDDEN',0,?)",artifact,UUID.randomUUID()))
                .hasStackTraceContaining("CONTENT_PIPELINE_COMPLETED_DEPENDENCIES_IMMUTABLE");
        var artifact=SourceJson.parse(jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?",byte[].class,completedArtifact));
        var counts=artifact.get("verification").get("completion").get("output_counts");
        var expected=SourceJson.object().put("articles",counts.get("content_catalog_v1").toString())
                .put("rows",counts.get("content_source_discovery_daily_v1").toString());
        assertThat(jdbc.queryForObject("SELECT counts_bytes FROM ddip_management.runs WHERE run_id=?",byte[].class,run))
                .isEqualTo(SourceJson.canonical(expected));
    }
    @Test void deterministicSlotsPreserveOnlyDocumentedAliases(){
        UUID run=UUID.randomUUID(),attempt=UUID.randomUUID();
        var silver=jdbc.queryForObject("SELECT ddip_management.content_pipeline_slot(?,?,'SILVER',0)",UUID.class,run,attempt);
        assertThat(jdbc.queryForObject("SELECT ddip_management.content_pipeline_slot(?,?,'VERIFY_SILVER',0)",UUID.class,run,attempt)).isEqualTo(silver);
        assertThat(jdbc.queryForObject("SELECT ddip_management.content_pipeline_slot(?,?,'RECHECK_SILVER',0)",UUID.class,run,attempt)).isNotEqualTo(silver);
        assertThat(jdbc.queryForList("SELECT i FROM generate_series(2,16) i CROSS JOIN LATERAL ddip_management.content_pipeline_step(i)",Integer.class)).hasSize(15);
    }
}
