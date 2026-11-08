package platform.contentapi.execution;

import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.SourceJson;
import static org.assertj.core.api.Assertions.*;

class StrictContentPipelineRuntimeTest {
    @Test void protocolIdentityIsExplicitAndSeparateFromHistoricalRuntimeTwo(){
        var expected=SourceJson.object().put("strictContentRuntimeVersion",1).put("adapter","strict_native_content").put("bootstrapVersion",2).put("commandProtocolVersion",2).put("ledgerVersion",2);
        assertThat(StrictContentPipelineRuntime.protocolDigest()).isEqualTo(SourceJson.sha(SourceJson.canonical(expected)));
        var versions=StrictContentPipelineRuntime.versions();
        assertThat(versions.propertyNames()).containsExactlyInAnyOrder("python","pyspark","py4j","boto3","botocore","duckdb","java");
        assertThat(versions.get("java").stringValue()).isEqualTo("21");
    }
    @ParameterizedTest @ValueSource(longs={0,2,4294967297L})
    void incompatibleStrictVersionFailsBeforeAnyInstalledRuntimeOrLaunchAccess(long version){
        var fixture=new NarrowContentRuntimeQualificationTest.Fixture();var registered=fixture.inspect();
        var config=SourceJson.object().put("strictContentPipelineVersion",version);
        for(String field:List.of("runProfileId","runProfileSha256","contentRuntimeId","contentRuntimeSha256","controlBindingId","strictRuntimeSha256","python","processingRoot","javaHome","modules","versions","fetchLaunchProfileId","bronzeLaunchProfileId","bronzeVerificationProfileId"))config.putNull(field);
        byte[] bytes=SourceJson.canonical(config);
        var jdbc=new JdbcTemplate(){@Override public List<Map<String,Object>> queryForList(String sql,Object... args){
            assertThat(sql).isEqualTo("SELECT configuration_bytes,configuration_sha256 FROM ddip_management.content_pipeline_profiles WHERE profile_id=?");
            return List.of(Map.of("configuration_bytes",bytes,"configuration_sha256",SourceJson.sha(bytes)));
        }};
        assertThatThrownBy(()->StrictContentPipelineRuntime.load(jdbc,fixture.tx,null,registered,"strict-test")).hasMessage("RUNTIME_MISMATCH");
    }
    @Test void tamperedRegisteredProfileBytesFailBeforeRuntimeOrHostAccess(){
        var fixture=new NarrowContentRuntimeQualificationTest.Fixture();var registered=fixture.inspect();
        var jdbc=new JdbcTemplate(){@Override public List<Map<String,Object>> queryForList(String sql,Object... args){return List.of(Map.of("configuration_bytes",SourceJson.canonical(SourceJson.object()),"configuration_sha256","0".repeat(64)));}};
        assertThatThrownBy(()->StrictContentPipelineRuntime.load(jdbc,fixture.tx,null,registered,"strict-test")).hasMessage("RUNTIME_MISMATCH");
    }
    @Test void absentStrictProfileCannotFallBackToHistoricalRuntime(){
        var fixture=new NarrowContentRuntimeQualificationTest.Fixture();var registered=fixture.inspect();
        var jdbc=new JdbcTemplate(){@Override public List<Map<String,Object>> queryForList(String sql,Object... args){return List.of();}};
        assertThatThrownBy(()->StrictContentPipelineRuntime.load(jdbc,fixture.tx,null,registered,"content")).hasMessage("RUNTIME_MISMATCH");
    }
    @ParameterizedTest @ValueSource(strings={"", "../profile", "with space"})
    void invalidProfileIdentityFailsBeforeSql(String id){assertThatThrownBy(()->StrictContentPipelineRuntime.load(null,null,null,null,id)).hasMessage("RUNTIME_MISMATCH");}
}
