package platform.contentapi.management;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Real SQL/public catalog metadata only. No DNS, public child, native candidate or transport claim. */
class NarrowContentPublicHistoryIT extends NarrowContentAdmissionFixtures {
    UUID operation;
    @Override void profile(String id,boolean content,String source){
        if(content)NarrowContentFetchReservationTestSupport.registerChain(jdbc,id,RunFixtures.profile(true,source),CONTENT,null,false);
        else super.profile(id,false,source);
    }
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        byte[] bytes=SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:9000").put("region","local").put("bucket","bronze").put("credentialsFile","/never/read/credentials"));
        jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,bytes,SourceJson.sha(bytes),INSTALL);
    }
    @Override void ready(){
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE));
    }
    @BeforeEach void publicProfiles(){
        operation=UUID.randomUUID();NarrowContentPublicHistoryTestSupport.registerProfiles(jdbc,CONTENT_PROFILE,binding.id());
    }
    void reserved(){claimed();prepare();NarrowContentPublicHistoryTestSupport.reserve(jdbc,tx,workers,guard,owner,operation);}
    void bound(){jdbc.update("INSERT INTO ddip_management.content_native_launch_phases(operation_id,phase,launch_profile_id,launch_profile_sha256) SELECT ?,'LAUNCH_BOUND',launch_profile_id,configuration_sha256 FROM ddip_management.content_fetch_launch_profiles WHERE launch_profile_id='public-fetch'",operation);}
    String history(){return NarrowContentPublicHistoryTestSupport.digest(jdbc,tx,owner.runId(),owner.attemptId());}
    void laterRegistration(){jdbc.update("INSERT INTO ddip_management.content_pipeline_profiles(profile_id,configuration_bytes,configuration_sha256) SELECT 'later-public-pipeline',configuration_bytes,configuration_sha256 FROM ddip_management.content_pipeline_profiles WHERE profile_id='public-pipeline'");}
    @Test void consumedPublicReservationReopensBeforeLaunchWithoutInstalledRuntimeOrNetwork()throws Exception{
        claimed();prepare();var files=filesystem();NarrowContentPublicHistoryTestSupport.reserve(jdbc,tx,workers,guard,owner,operation);
        var reservation=SourceJson.parse(jdbc.queryForObject("SELECT bootstrap_bytes FROM ddip_management.content_native_operations WHERE operation_id=?",byte[].class,operation));
        assertThat(reservation.get("reservationVersion").intValue()).isEqualTo(2);
        assertThat(reservation.get("publicFetch").get("selectedEndpoint").get("uri").stringValue()).isEqualTo("https://feeds.example.org/one");
        var first=history();laterRegistration();assertThat(history()).isEqualTo(first);
        var assessment=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,null,owner.runId(),owner.attemptId());
        assertThat(assessment.prefix()).isEqualTo("FETCH_INCOMPLETE");assertThat(assessment.authorization()).isFalse();
        assertThat(assessment.nativeLaunchCompatible()).isFalse();assertThat(filesystem()).isEqualTo(files);
        Files.move(attemptRoot(),attemptRoot().resolveSibling("unavailable-public-attempt"));
        guard.close();guard=null;assertThat(history()).isEqualTo(first);
        assertThat(count("content_native_launch_phases")).isZero();assertThat(count("content_fetch_checkpoints")).isZero();noExecution();
    }
    @Test void publicLaunchBindingReopensWithExactRegisteredIdentity(){
        reserved();bound();var first=history();assertThat(history()).isEqualTo(first);
        var facts=NarrowContentFetchLaunchTestSupport.inspect(jdbc,tx,workers,operation);
        assertThat(facts.phases()).containsExactly("LAUNCH_BOUND");assertThat(facts.launchProfileId()).isEqualTo("public-fetch");
        assertThat(facts.nativeLaunchCompatible()).isFalse();noExecution();
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void existingOriginalSafeEndRetainsPublicReservedPrefixWithoutContinuation(boolean launchBound)throws Exception{
        reserved();if(launchBound)bound();var beforeRegistration=history();laterRegistration();assertThat(history()).isEqualTo(beforeRegistration);
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,owner.runId());
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);
        UUID control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL,p->{}).request(ControlCommands.parse(owner.runId().toString(),SourceJson.canonical(request),"public-terminal","\""+version+"\"")).get("controlId").stringValue());
        guard.close();guard=InstallationHostGuard.acquire(binding);
        var recovery=new WorkerRepository(jdbc,tx,INSTALL).register(guard,CONTENT_PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());
        assertThat(recovery.status()).isEqualTo("RECOVERY_ONLY");
        var clock=Clock.fixed(Instant.now(),ZoneOffset.UTC);var terminal=new NarrowContentTerminalTestSupport(jdbc,tx,INSTALL,clock,()->"ABSENT",b->{});
        UUID id=UUID.randomUUID();var target=terminal.inspect(guard,owner.runId(),owner.attemptId(),control,UUID.randomUUID(),id);
        var qualified=terminal.qualify(guard,target,clock.instant(),"");var files=filesystem();
        assertThat(terminal.finish(guard,qualified)).isEqualTo("COMMITTED");
        assertThat(terminal.lookup(id)).isEqualTo("CONFIRMED_TERMINAL");assertThat(filesystem()).isEqualTo(files);
        assertThat(jdbc.queryForObject("SELECT state||':'||outcome_code FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("FAILED:NATIVE_CONTENT_INTERRUPTED");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(count("content_native_operations")).isEqualTo(1);assertThat(count("content_fetch_checkpoints")).isZero();
        assertThat(count("content_fetch_recovery_handoffs")).isZero();assertThat(count("attempts")).isEqualTo(2);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE kind='EXECUTE'",Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE kind='RECONCILE' AND state='SUCCEEDED' AND outcome_code='RECONCILED_FAILED' AND content_terminal_policy='ddip-content-original-safe-end-v1'",Long.class)).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE state IN ('CLAIMED','RUNNING')",Long.class)).isZero();
    }
}
