package platform.contentapi.management;
import org.junit.jupiter.api.Test;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;
/** Real FETCH/Bronze prefix and actual first native child, not fabricated VERIFIED rows. */
class NarrowContentPipelineRecoveryAssessmentIT extends NarrowContentPipelineFixtures {
    @Test void reservedSuffixIsDetachedAndCannotConveyAuthority(){
        original();assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,owner,PIPELINE_PROFILE,e->{
            if(e.operation().equals("BRONZE_BEFORE_SILVER") && e.boundary().equals("RESERVED"))throw new IllegalStateException("ASSESSMENT_STOP");
        })).hasMessage("ASSESSMENT_STOP");
        var before=metadata();var result=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,null,owner.runId(),owner.attemptId());
        assertThat(result.prefix()).isEqualTo("OPERATION_INCOMPLETE");assertThat(result.operationCount()).isEqualTo(3);assertThat(result.checkpointSteps()).containsExactly(0,1);
        assertThat(result.authorization()).isFalse();assertThat(result.quiescenceEstablished()).isFalse();assertThat(result.nativeLaunchCompatible()).isFalse();assertThat(result.historicalFilesystemIdentityEstablished()).isFalse();assertThat(metadata()).isEqualTo(before);
    }
    @Test void realCheckpointMatchesCurrentInventoryAndMutationFailsClosed()throws Exception{
        original();assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,owner,PIPELINE_PROFILE,e->{
            if(e.operation().equals("BRONZE_BEFORE_SILVER") && e.boundary().equals("CHECKPOINT_COMMITTED"))throw new IllegalStateException("ASSESSMENT_STOP");
        })).hasMessage("ASSESSMENT_STOP");
        var a=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId());assertThat(a.prefix()).isEqualTo("OPERATION_CHECKPOINT");assertThat(a.integrity()).isEqualTo("CURRENT_BYTES_AND_INVENTORY_MATCH");
        var changed=new org.springframework.jdbc.core.JdbcTemplate(jdbc.getDataSource()){
            @Override public java.util.List<java.util.Map<String,Object>> queryForList(String sql,Object... args){
                var rows=super.queryForList(sql,args);
                if(sql.startsWith("SELECT * FROM ddip_management.content_pipeline_checkpoints"))for(var row:rows)row.put("proof_sha256","0".repeat(64));
                return rows;
            }
        };
        assertThatThrownBy(()->NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(changed,transactions(changed),null,owner.runId(),owner.attemptId())).hasMessage("RUNTIME_MISMATCH");
        var checkpoint=jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_pipeline_operations WHERE attempt_id=?",java.util.UUID.class,owner.attemptId());
        var path=binding.root().resolve("runs/"+owner.runId()+"/attempts/"+owner.attemptId()+"/inspect-bronze/"+checkpoint+"/result.json");
        java.nio.file.Files.writeString(path,"{}");
        assertThat(NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId()).integrity()).isEqualTo("UNAVAILABLE_OR_CHANGED");
    }
    @Test void completeRealChainAssessesRegistrationGapsAndTerminalSuccess(){
        original();emptyFeed=true;var seen=new java.util.ArrayList<String>();
        NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,owner,PIPELINE_PROFILE,e->{
            if(e.boundary().equals("RESERVED") && java.util.Set.of("PREPARE_GOLD","PREPARE_CONTENT").contains(e.operation())){
                var a=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,null,owner.runId(),owner.attemptId());
                assertThat(a.prefix()).isEqualTo("OPERATION_INCOMPLETE");assertThat(a.authorization()).isFalse();seen.add(e.operation());
            }
        });
        assertThat(seen).containsExactly("PREPARE_GOLD","PREPARE_CONTENT");var before=metadata();
        var a=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId());
        assertThat(a.prefix()).isEqualTo("TERMINAL_SUCCESS");assertThat(a.operationCount()).isEqualTo(17);assertThat(a.checkpointSteps()).containsExactly(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16);
        assertThat(a.integrity()).isEqualTo("CURRENT_BYTES_AND_INVENTORY_MATCH");assertThat(a.authorization()).isFalse();assertThat(a.quiescenceEstablished()).isFalse();assertThat(metadata()).isEqualTo(before);
    }

}
