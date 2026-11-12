package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.time.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/**
 * Synthetic FETCH journal plus genuine original-owner Bronze reservation component gates.
 * There are no native Bronze/verifier processes or S3/public HTTP requests. Raw candidate SQL
 * in the verifier binding test is deliberately insufficient for Java historical qualification.
 */
class NarrowContentPublicBronzeHistoryIT extends NarrowContentPublicCheckpointFixtures {
    UUID bronzeOperation;
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        try{
            Path credentials=Files.createTempFile(temp,"public-bronze-component-",".json",PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));
            Files.write(credentials,SourceJson.canonical(SourceJson.object().put("accessKey","synthetic-component-access").put("secretKey","synthetic-component-secret")));
            byte[] bytes=SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:1")
                    .put("region","local").put("bucket","bronze").put("credentialsFile",credentials.toString()));
            jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,bytes,SourceJson.sha(bytes),INSTALL);
        }catch(java.io.IOException ex){throw new IllegalStateException(ex);}
    }
    NarrowContentBronzeReservationTestSupport.Facts reserveBronze()throws Exception{
        checkpoint();bronzeOperation=UUID.randomUUID();
        // This is the original Owner obtained at admission, never reconstructed from a receipt.
        return NarrowContentBronzeReservationTestSupport.reserve(jdbc,tx,workers,guard,owner,operation,bronzeOperation,b->{});
    }
    String profileDigest(String table,String column,String id){return jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management."+table+" WHERE "+column+"=?",String.class,id);}
    void bronzeBound(String id){
        jdbc.update("INSERT INTO ddip_management.content_bronze_launch_phases(operation_id,phase,launch_profile_id,launch_profile_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,'LAUNCH_BOUND',?,?,?,?,?,?)",
                bronzeOperation,id,profileDigest("content_bronze_launch_profiles","launch_profile_id",id),owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
    }
    void alternateLinkedProfiles(){
        byte[] fetch=jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.content_fetch_launch_profiles WHERE launch_profile_id='public-fetch'",byte[].class);
        jdbc.update("INSERT INTO ddip_management.content_fetch_launch_profiles(launch_profile_id,configuration_bytes,configuration_sha256) VALUES ('other-public-fetch',?,?)",fetch,SourceJson.sha(fetch));
        for(var pair:List.of(new String[]{"content_bronze_launch_profiles","launch_profile_id","public-bronze","other-public-bronze"},
                new String[]{"content_bronze_verification_profiles","verification_profile_id","public-verifier","other-public-verifier"})){
            var n=(ObjectNode)SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management."+pair[0]+" WHERE "+pair[1]+"=?",byte[].class,pair[2]));
            n.put("fetchLaunchProfileId","other-public-fetch");byte[] bytes=SourceJson.canonical(n);
            jdbc.update("INSERT INTO ddip_management."+pair[0]+"("+pair[1]+",configuration_bytes,configuration_sha256) VALUES (?,?,?)",pair[3],bytes,SourceJson.sha(bytes));
        }
    }
    @Test void publicSourceKeepsBronzeReservationNativeVersionAndHistoricalReadOnlySemantics()throws Exception{
        var reservation=reserveBronze();var before=history();var files=filesystem();
        var row=jdbc.queryForMap("SELECT command_bytes,reservation_bytes FROM ddip_management.content_bronze_reservations WHERE operation_id=?",bronzeOperation);
        var command=SourceJson.parse((byte[])row.get("command_bytes"));var envelope=SourceJson.parse((byte[])row.get("reservation_bytes"));
        assertThat(command.get("protocolVersion").intValue()).isEqualTo(1);assertThat(command.get("mode").stringValue()).isEqualTo("PUBLISH");
        assertThat(command.get("storage").get("configuration").get("endpoint").stringValue()).isEqualTo("http://127.0.0.1:1");
        assertThat(envelope.get("reservationVersion").intValue()).isEqualTo(1);assertThat(envelope.get("mode").stringValue()).isEqualTo("STORE_BRONZE_RESERVATION");
        assertThat(NarrowContentBronzeReservationTestSupport.inspect(jdbc,tx,bronzeOperation)).isEqualTo(reservation);assertThat(reservation.nativeLaunchCompatible()).isFalse();
        var a=NarrowContentPipelineRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId());
        assertThat(a.prefix()).isEqualTo("BRONZE_INCOMPLETE");assertThat(a.operationCount()).isEqualTo(2);assertThat(a.authorization()).isFalse();
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);noExecution();
    }
    @Test void publicBronzeBindingRequiresTheActualFetchPredecessorNotCommonRunProfile()throws Exception{
        reserveBronze();alternateLinkedProfiles();var before=history();var files=filesystem();
        assertThatThrownBy(()->bronzeBound("other-public-bronze")).hasStackTraceContaining("CONTENT_BRONZE_LAUNCH_PUBLIC_PREDECESSOR_INVALID");
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        bronzeBound("public-bronze");before=history();
        var facts=NarrowContentBronzeLaunchTestSupport.inspect(jdbc,tx,workers,bronzeOperation);
        assertThat(facts.phases()).containsExactly("LAUNCH_BOUND");assertThat(facts.fetchOperation()).isEqualTo(operation);
        assertThat(facts.launchProfileId()).isEqualTo("public-bronze");assertThat(facts.nativeLaunchCompatible()).isFalse();
        assertThat(historyDigest()).hasSize(64);assertThat(history()).isEqualTo(before);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"missing","fetch-link","storage-policy","version"})
    void publicBronzeBoundHistoryRejectsMissingOrChangedRegisteredComposition(String change)throws Exception{
        reserveBronze();bronzeBound("public-bronze");
        assertCorruptHistoryRejected("content_bronze_launch_profiles",()->{
            if(change.equals("missing")){jdbc.update("DELETE FROM ddip_management.content_bronze_launch_profiles WHERE launch_profile_id='public-bronze'");return;}
            var n=(ObjectNode)SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.content_bronze_launch_profiles WHERE launch_profile_id='public-bronze'",byte[].class));
            if(change.equals("fetch-link"))n.put("fetchLaunchProfileSha256","0".repeat(64));
            else if(change.equals("version"))n.put("strictBronzeLaunchVersion",1);else n.put("storageEndpointPolicy","PUBLIC_HTTP");
            replaceConfiguration("content_bronze_launch_profiles","launch_profile_id","public-bronze",n);
        },this::historyDigest);
    }
    void syntheticBronzeCandidateSql(){
        bronzeBound("public-bronze");
        for(String phase:FETCH_PHASES.subList(1,6))
            jdbc.update("INSERT INTO ddip_management.content_bronze_launch_phases(operation_id,phase,child_instance_id,child_pid,child_started_at,bootstrap_sha256,candidate_sha256,exit_code,storage_state,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",
                    bronzeOperation,phase,phase.equals("CHILD_RECORDED")?UUID.randomUUID():null,phase.equals("CHILD_RECORDED")?SYNTHETIC_PID+1:null,
                    phase.equals("CHILD_RECORDED")?java.sql.Timestamp.from(SYNTHETIC_STARTED):null,
                    phase.equals("RELEASE_AUTHORIZED")?"0".repeat(64):null,phase.equals("CANDIDATE_EXITED")?"a".repeat(64):null,
                    phase.equals("CANDIDATE_EXITED")?0:null,phase.equals("CANDIDATE_EXITED")?"EXACT":null,
                    owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
    }
    void verifierBound(String id,String digest,String candidate){
        jdbc.update("INSERT INTO ddip_management.content_bronze_verification_phases(operation_id,phase,verification_profile_id,verification_profile_sha256,candidate_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,'VERIFY_BOUND',?,?,?,?,?,?,?)",
                bronzeOperation,id,digest,candidate,owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
    }
    @ParameterizedTest @ValueSource(strings={"fetch-link","profile-digest","candidate"})
    void publicVerifierBindingRejectsMismatchedSqlClaimsWithoutTreatingClaimsAsNativeSuccess(String change)throws Exception{
        reserveBronze();alternateLinkedProfiles();syntheticBronzeCandidateSql();var before=history();var files=filesystem();
        String id=change.equals("fetch-link")?"other-public-verifier":"public-verifier";
        String digest=change.equals("profile-digest")?"0".repeat(64):profileDigest("content_bronze_verification_profiles","verification_profile_id",id);
        assertThatThrownBy(()->verifierBound(id,digest,change.equals("candidate")?"b".repeat(64):"a".repeat(64))).hasStackTraceContaining("CONTENT_BRONZE_VERIFICATION_BINDING_INVALID");
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        verifierBound("public-verifier",profileDigest("content_bronze_verification_profiles","verification_profile_id","public-verifier"),"a".repeat(64));
        // SQL correspondence alone cannot pass the independently recomputed native packet hash.
        assertThatThrownBy(()->NarrowContentBronzeLaunchTestSupport.inspect(jdbc,tx,workers,bronzeOperation)).hasMessage("RUNTIME_MISMATCH");
        assertThat(count("content_bronze_checkpoints")).isZero();assertThat(count("content_fetch_recovery_handoffs")).isZero();noExecution();
    }
    @ParameterizedTest @ValueSource(booleans={false,true})
    void originalSafeEndRetainsPublicBronzeReservedOrBoundPrefix(boolean bound)throws Exception{
        reserveBronze();if(bound)bronzeBound("public-bronze");recover();var files=filesystem();
        var clock=Clock.fixed(Instant.now(),ZoneOffset.UTC);var terminal=new NarrowContentTerminalTestSupport(jdbc,tx,INSTALL,clock,()->"ABSENT",b->{});
        UUID id=UUID.randomUUID();var target=terminal.inspect(guard,owner.runId(),owner.attemptId(),control,UUID.randomUUID(),id);
        assertThat(terminal.finish(guard,terminal.qualify(guard,target,clock.instant(),""))).isEqualTo("COMMITTED");
        assertThat(terminal.lookup(id)).isEqualTo("CONFIRMED_TERMINAL");assertThat(filesystem()).isEqualTo(files);
        assertThat(count("content_fetch_checkpoints")).isEqualTo(1);assertThat(count("content_bronze_reservations")).isEqualTo(1);
        assertThat(count("content_bronze_launch_phases")).isEqualTo(bound?1:0);assertThat(count("content_bronze_checkpoints")).isZero();
        assertThat(count("content_fetch_recovery_handoffs")).isZero();assertThat(count("content_fetch_recovery_finalizations")).isZero();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(jdbc.queryForObject("SELECT state||':'||outcome_code FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("FAILED:NATIVE_CONTENT_INTERRUPTED");
    }
}
