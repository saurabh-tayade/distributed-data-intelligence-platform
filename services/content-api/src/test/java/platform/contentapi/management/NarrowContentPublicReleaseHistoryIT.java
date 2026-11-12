package platform.contentapi.management;

import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Real PostgreSQL, synthetic release claims only. No child process, DNS, or public transport. */
class NarrowContentPublicReleaseHistoryIT extends NarrowContentPublicCheckpointFixtures {
    @Test void exactPublicReleasePacketAndHashReopenWithoutInstalledRuntimeOrExecution()throws Exception{
        released();var before=history();var files=filesystem();
        byte[] expected=expectedPacket();
        assertThat(NarrowContentPublicCheckpointTestSupport.packet(reservation(),SYNTHETIC_PID)).isEqualTo(expected);
        assertThat(SourceJson.parse(expected).get("publicFetch")).isEqualTo(SourceJson.parse(reservation()).get("publicFetch"));
        assertThat(SourceJson.parse(expected).has("operationId")).isFalse();
        assertThat(jdbc.queryForObject("SELECT bootstrap_sha256 FROM ddip_management.content_native_launch_phases WHERE operation_id=? AND phase='RELEASE_AUTHORIZED'",String.class,operation)).isEqualTo(SourceJson.sha(expected));
        var facts=launchFacts();assertThat(facts.phases()).containsExactlyElementsOf(FETCH_PHASES.subList(0,5));
        assertThat(facts.child().pid()).isEqualTo(SYNTHETIC_PID);assertThat(facts.child().startedAt()).isEqualTo(SYNTHETIC_STARTED);
        assertThat(facts.launchProfileId()).isEqualTo("public-fetch");assertThat(facts.launchProfileSha256()).isEqualTo(launchDigest());
        assertThat(facts.candidateForVerification()).isFalse();assertThat(facts.completed()).isFalse();assertThat(facts.nativeLaunchCompatible()).isFalse();
        var digest=historyDigest();assertThat(historyDigest()).isEqualTo(digest);
        var assessment=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,null,owner.runId(),owner.attemptId());
        assertThat(assessment.prefix()).isEqualTo("FETCH_INCOMPLETE");assertThat(assessment.recordedProcessCount()).isEqualTo(2);
        assertThat(assessment.authorization()).isFalse();assertThat(assessment.quiescenceEstablished()).isFalse();
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"policy","policy-digest","launch-id","launch-digest","selected-endpoint","missing-policy","missing-profile","unknown"})
    void changedReservationClaimsCannotServeAsTheirOwnHistoricalAuthority(String change){
        released();
        assertCorruptHistoryRejected("content_native_operations",()->{
            var r=(ObjectNode)SourceJson.parse(reservation());var p=(ObjectNode)r.get("publicFetch");
            switch(change){
                case "policy"->((ObjectNode)p.get("networkPolicy")).put("redirects",true);
                case "policy-digest"->p.put("networkPolicySha256","0".repeat(64));
                case "launch-id"->p.put("launchProfileId","unregistered-public-fetch");
                case "launch-digest"->p.put("launchProfileSha256","0".repeat(64));
                case "selected-endpoint"->((ObjectNode)p.get("selectedEndpoint")).put("uri","https://feeds.example.org/two");
                case "missing-policy"->p.remove("networkPolicy");
                case "missing-profile"->p.remove("launchProfileId");
                case "unknown"->p.put("retry",true);
            }
            byte[] bytes=SourceJson.canonical(r);jdbc.update("UPDATE ddip_management.content_native_operations SET bootstrap_bytes=?,bootstrap_sha256=? WHERE operation_id=?",bytes,SourceJson.sha(bytes),operation);
        },this::launchFacts);
    }
    @ParameterizedTest @ValueSource(strings={"missing-fetch","fetch-policy","fetch-manifest"})
    void publicHistoryRequiresItsIndependentlyRegisteredFetchProfile(String change){
        released();
        String table="content_fetch_launch_profiles",id="public-fetch",column="launch_profile_id";
        assertCorruptHistoryRejected(table,()->{
            if(change.startsWith("missing")){jdbc.update("DELETE FROM ddip_management."+table+" WHERE "+column+"=?",id);return;}
            var n=(ObjectNode)SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management."+table+" WHERE "+column+"=?",byte[].class,id));
            if(change.equals("fetch-policy"))((ObjectNode)n.get("networkPolicy")).put("proxies",true);
            else if(change.equals("fetch-manifest"))((ObjectNode)n.get("manifest")).put("adapter","platform.ingestion.managed.StrictManagedRssFetch");
            replaceConfiguration(table,column,id,n);
        },this::historyDigest);
    }
    @ParameterizedTest @ValueSource(strings={"content_bronze_launch_profiles","content_bronze_verification_profiles","content_pipeline_profiles"})
    void unboundDownstreamProfilesAreNotDependenciesOfRecordedPublicRelease(String table){
        released();var before=history();var digest=historyDigest();
        var raw=new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.support.JdbcTransactionManager(jdbc.getDataSource()));
        raw.execute(status->{
            // Disposable-schema corruption only. These rows have never been bound to this prefix.
            jdbc.execute("ALTER TABLE ddip_management."+table+" DISABLE TRIGGER ALL");jdbc.update("DELETE FROM ddip_management."+table);
            assertThat(historyDigest()).isEqualTo(digest);assertThat(launchFacts().phases()).containsExactlyElementsOf(FETCH_PHASES.subList(0,5));
            status.setRollbackOnly();return null;
        });
        assertThat(history()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED"})
    void skippedPublicReleasePhaseIsRejectedBySqlWithoutWrites(String phase)throws Exception{
        reserved();var before=history();var files=filesystem();
        assertThatThrownBy(()->phase(phase)).isInstanceOf(RuntimeException.class);
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(count("content_fetch_checkpoints")).isZero();noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"missing-phase","release-digest","child-pid","launch-digest"})
    void independentlyCorruptedReleaseHistoryNeverReopens(String change){
        released();assertCorruptHistoryRejected("content_native_launch_phases",()->{
            switch(change){
                case "missing-phase"->jdbc.update("DELETE FROM ddip_management.content_native_launch_phases WHERE operation_id=? AND phase='FILES_READY'",operation);
                case "release-digest"->jdbc.update("UPDATE ddip_management.content_native_launch_phases SET bootstrap_sha256=? WHERE operation_id=? AND phase='RELEASE_AUTHORIZED'","0".repeat(64),operation);
                case "child-pid"->jdbc.update("UPDATE ddip_management.content_native_launch_phases SET child_pid=child_pid+1 WHERE operation_id=? AND phase='CHILD_RECORDED'",operation);
                case "launch-digest"->jdbc.update("UPDATE ddip_management.content_native_launch_phases SET launch_profile_sha256=? WHERE operation_id=? AND phase='LAUNCH_BOUND'","0".repeat(64),operation);
            }
        },this::launchFacts);
    }
}
