package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Direct SQL clients cannot bypass immutable profile linkage or ordered owner-fenced phases. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentBronzeLaunchSqlIT extends NarrowContentBronzeLaunchFixtures {
    byte[] profileBytes(){return jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.content_bronze_launch_profiles WHERE launch_profile_id=?",byte[].class,BRONZE_PROFILE);}
    void insertProfile(byte[] bytes){jdbc.update("INSERT INTO ddip_management.content_bronze_launch_profiles(launch_profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)","alternative",bytes,SourceJson.sha(bytes));}
    @ParameterizedTest @ValueSource(strings={"bronzeRuntimeSha256","runProfileSha256","storageBindingSha256","controlBindingId","adapter","unknown","duplicate","float","string","version","strictRuntimeSha256","oversized"})
    void malformedOrUnlinkedLaunchProfilesAreRejected(String change){
        var config=(ObjectNode)SourceJson.parse(profileBytes());byte[] bytes;
        switch(change){
            case "bronzeRuntimeSha256","runProfileSha256","storageBindingSha256"->config.put(change,"0".repeat(64));
            case "controlBindingId"->config.put(change,"foreign");
            case "adapter"->((ObjectNode)config.get("manifest")).put("adapter","platform.ingestion.managed.BoundedManagedBronze");
            case "unknown"->config.put("unknown",true);
            case "version"->config.put("strictBronzeLaunchVersion",2);
            case "strictRuntimeSha256"->config.put("strictRuntimeSha256","invalid");
            default->{ }
        }
        String raw=new String(SourceJson.canonical(config),StandardCharsets.UTF_8);
        if(change.equals("duplicate"))raw=raw.replace("\"strictBronzeLaunchVersion\":1","\"strictBronzeLaunchVersion\":1,\"strictBronzeLaunchVersion\":1");
        if(change.equals("float"))raw=raw.replace("\"strictBronzeLaunchVersion\":1","\"strictBronzeLaunchVersion\":1.0");
        if(change.equals("string"))raw=raw.replace("\"strictBronzeLaunchVersion\":1","\"strictBronzeLaunchVersion\":\"1\"");
        if(change.equals("oversized"))raw=" ".repeat(65536)+raw;
        bytes=raw.getBytes(StandardCharsets.UTF_8);assertThatThrownBy(()->insertProfile(bytes)).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(count("content_bronze_launch_profiles")).isEqualTo(1);assertThat(objectRequests).hasValue(0);noExecution();
    }
    @Test void profileBytesCannotBeUpdatedDeletedOrTruncated(){
        for(String sql:List.of("UPDATE ddip_management.content_bronze_launch_profiles SET configuration_bytes=configuration_bytes","DELETE FROM ddip_management.content_bronze_launch_profiles","TRUNCATE ddip_management.content_bronze_launch_profiles CASCADE"))assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(RuntimeException.class);
        assertThat(count("content_bronze_launch_profiles")).isEqualTo(1);noExecution();
    }
    void reserved(){checkpoint();assertThatThrownBy(()->bronzeLaunch(b->{if(b.equals("RESERVED"))throw new IllegalStateException("STOP");})).hasMessage("STOP");}
    void insertPhase(String phase,String profile,String digest,long coordinator){
        jdbc.update("INSERT INTO ddip_management.content_bronze_launch_phases(operation_id,phase,launch_profile_id,launch_profile_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?)",bronzeOperation,phase,profile,digest,coordinator,owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
    }
    @ParameterizedTest @ValueSource(strings={"FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED"})
    void cannotSkipLaunchBound(String phase){reserved();assertThatThrownBy(()->insertPhase(phase,null,null,owner.worker().coordinatorVersion())).isInstanceOf(org.springframework.dao.DataAccessException.class);assertThat(count("content_bronze_launch_phases")).isZero();bronzeRetained();}
    @ParameterizedTest @ValueSource(strings={"profile","digest","owner","cancel","stop"})
    void launchBoundRechecksIdentityAndCurrentOwner(String change){
        reserved();String profile=change.equals("profile")?"missing":BRONZE_PROFILE;
        String digest=change.equals("digest")?"0".repeat(64):SourceJson.sha(profileBytes());
        long coordinator=owner.worker().coordinatorVersion()+(change.equals("owner")?1:0);
        if(change.equals("cancel"))cancel();if(change.equals("stop"))jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
        assertThatThrownBy(()->insertPhase("LAUNCH_BOUND",profile,digest,coordinator)).isInstanceOf(org.springframework.dao.DataAccessException.class);assertThat(count("content_bronze_launch_phases")).isZero();bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"ISOLATION_REPEATABLE_READ","ISOLATION_SERIALIZABLE"})
    void phaseInsertRejectsSnapshotIsolation(String level){
        reserved();var tx=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));tx.setIsolationLevelName(level);
        assertThatThrownBy(()->tx.execute(s->{insertPhase("LAUNCH_BOUND",BRONZE_PROFILE,SourceJson.sha(profileBytes()),owner.worker().coordinatorVersion());return null;})).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(count("content_bronze_launch_phases")).isZero();bronzeRetained();
    }
    @Test void validSqlPhaseIsHistoricalAndCannotAuthorizeLauncher(){
        reserved();insertPhase("LAUNCH_BOUND",BRONZE_PROFILE,SourceJson.sha(profileBytes()),owner.worker().coordinatorVersion());
        assertThat(bronzeFacts().phases()).containsExactly("LAUNCH_BOUND");assertThat(bronzeFacts().candidateForVerification()).isFalse();
        assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(0);bronzeRetained();
    }

    void sqlThroughChild(){
        insertPhase("LAUNCH_BOUND",BRONZE_PROFILE,SourceJson.sha(profileBytes()),owner.worker().coordinatorVersion());
        insertPhase("FILES_READY",null,null,owner.worker().coordinatorVersion());
        insertPhase("START_AUTHORIZED",null,null,owner.worker().coordinatorVersion());
        jdbc.update("INSERT INTO ddip_management.content_bronze_launch_phases(operation_id,phase,child_instance_id,child_pid,child_started_at,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,'CHILD_RECORDED',?,?,clock_timestamp(),?,?,?,?)",
                bronzeOperation,UUID.randomUUID(),12345L,owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
    }
    void sqlRelease(){
        jdbc.update("INSERT INTO ddip_management.content_bronze_launch_phases(operation_id,phase,bootstrap_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,'RELEASE_AUTHORIZED',?,?,?,?,?)",
                bronzeOperation,"0".repeat(64),owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
    }
    @Test void structurallyValidReleaseHashIsNotTrustedByHistoricalReader(){
        reserved();sqlThroughChild();assertThat(bronzeFacts().phases()).hasSize(4);sqlRelease();
        assertThat(count("content_bronze_launch_phases")).isEqualTo(5);
        assertThatThrownBy(this::bronzeFacts).hasMessage("RUNTIME_MISMATCH");
        assertThatThrownBy(()->bronzeLaunch(b->{})).isInstanceOf(RuntimeException.class);
        assertThat(objectRequests).hasValue(0);bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"EXACT","ABSENT","PAYLOAD_ONLY","METADATA_ONLY","CONFLICT","UNAVAILABLE"})
    void candidateExitCodeMustExactlyMatchStorageState(String state){
        reserved();sqlThroughChild();sqlRelease();int correct=state.equals("EXACT")?0:1;
        String sql="INSERT INTO ddip_management.content_bronze_launch_phases(operation_id,phase,candidate_sha256,exit_code,storage_state,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,'CANDIDATE_EXITED',?,?,?,?,?,?,?)";
        assertThatThrownBy(()->jdbc.update(sql,bronzeOperation,"a".repeat(64),1-correct,state,owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion())).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(count("content_bronze_launch_phases")).isEqualTo(5);
        jdbc.update(sql,bronzeOperation,"a".repeat(64),correct,state,owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
        assertThat(count("content_bronze_launch_phases")).isEqualTo(6);assertThat(objectRequests).hasValue(0);bronzeRetained();
    }
    @Test void heartbeatRefreshedVersionsCanAppendButStaleVersionsCannot(){
        reserved();insertPhase("LAUNCH_BOUND",BRONZE_PROFILE,SourceJson.sha(profileBytes()),owner.worker().coordinatorVersion());
        var previous=owner;owner=workers.heartbeat(guard,owner);
        assertThatThrownBy(()->insertPhase("FILES_READY",null,null,previous.worker().coordinatorVersion())).isInstanceOf(org.springframework.dao.DataAccessException.class);
        insertPhase("FILES_READY",null,null,owner.worker().coordinatorVersion());
        assertThat(bronzeFacts().phases()).containsExactly("LAUNCH_BOUND","FILES_READY");bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"worker","run","attempt"})
    void everyPhaseRequiresExactCurrentVersion(String field){
        reserved();insertPhase("LAUNCH_BOUND",BRONZE_PROFILE,SourceJson.sha(profileBytes()),owner.worker().coordinatorVersion());
        switch(field){
            case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",owner.worker().instanceId());
            case "run"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());
            case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
        }
        assertThatThrownBy(()->insertPhase("FILES_READY",null,null,owner.worker().coordinatorVersion())).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(count("content_bronze_launch_phases")).isEqualTo(1);bronzeRetained();
    }
    @ParameterizedTest @ValueSource(strings={"recovery","terminal"})
    void recoveryModeAndTerminalOriginalAttemptCannotAppendPhases(String state){
        reserved();insertPhase("LAUNCH_BOUND",BRONZE_PROFILE,SourceJson.sha(profileBytes()),owner.worker().coordinatorVersion());
        if(state.equals("recovery"))jdbc.update("UPDATE ddip_management.coordinator SET mode='RECOVERY_ONLY',row_version=row_version+1");
        else jdbc.update("UPDATE ddip_management.attempts SET state='INTERRUPTED',finished_at=clock_timestamp(),outcome_code='INTERRUPTED',row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
        // Supply refreshed versions so failure is due to ownership/state, not stale CAS alone.
        long cv=jdbc.queryForObject("SELECT row_version FROM ddip_management.coordinator",Long.class);
        long av=jdbc.queryForObject("SELECT row_version FROM ddip_management.attempts WHERE attempt_id=?",Long.class,owner.attemptId());
        assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.content_bronze_launch_phases(operation_id,phase,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,'FILES_READY',?,?,?,?)",
                bronzeOperation,cv,owner.worker().workerVersion(),owner.runVersion(),av)).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(count("content_bronze_launch_phases")).isEqualTo(1);assertThat(objectRequests).hasValue(0);bronzeRetained();
    }
    @Test void phaseEvidenceCannotBeUpdatedDeletedOrTruncated(){
        reserved();insertPhase("LAUNCH_BOUND",BRONZE_PROFILE,SourceJson.sha(profileBytes()),owner.worker().coordinatorVersion());
        for(String sql:List.of("UPDATE ddip_management.content_bronze_launch_phases SET phase=phase","DELETE FROM ddip_management.content_bronze_launch_phases","TRUNCATE ddip_management.content_bronze_launch_phases CASCADE"))assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThat(bronzeFacts().phases()).containsExactly("LAUNCH_BOUND");bronzeRetained();
    }
    @Test void protocolDigestHasShapeInSqlButMustMatchExactlyInJava(){
        reserved();var config=(ObjectNode)SourceJson.parse(profileBytes());config.put("strictRuntimeSha256","0".repeat(64));
        byte[] bytes=SourceJson.canonical(config);insertProfile(bytes);
        insertPhase("LAUNCH_BOUND","alternative",SourceJson.sha(bytes),owner.worker().coordinatorVersion());
        assertThat(count("content_bronze_launch_phases")).isEqualTo(1);
        assertThatThrownBy(this::bronzeFacts).hasMessage("RUNTIME_MISMATCH");
        assertThat(objectRequests).hasValue(0);bronzeRetained();
    }
}
