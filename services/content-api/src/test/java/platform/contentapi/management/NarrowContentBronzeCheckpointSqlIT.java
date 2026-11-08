package platform.contentapi.management;

import java.nio.charset.StandardCharsets;
import java.util.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import tools.jackson.databind.node.ObjectNode;
import platform.contentapi.execution.*;
import static platform.contentapi.management.SourceFixtures.*;
import static org.assertj.core.api.Assertions.*;

/** Direct SQL cannot turn candidate claims into a mismatched or unfenced native checkpoint. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentBronzeCheckpointSqlIT extends NarrowContentBronzeLaunchFixtures {
    static final String PROFILE="strict-bronze-verification-test", CANDIDATE="a".repeat(64);
    byte[] verification,receipt,anchor;boolean versionEighteen;
    @Override void migrateFixtureSchema(){
        if(!versionEighteen){super.migrateFixtureSchema();return;}
        org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").target("18").load().migrate();
        new JdbcCoordinatorRepository(jdbc,tx,INSTALL).initialize();
        historicalSubmissionForSchema(18);
    }
    @AfterEach void resetSchemaTarget(){versionEighteen=false;}
    @Test void populatedVersionEighteenUpgradePreservesExactCandidateWithoutCheckpointBackfill()throws Exception{
        versionEighteen=true;closeServer();closeObjectServer();setup();checkpoint();objectError=null;seedPair();
        var launch=bronzeLaunch(b->{});assertThat(launch.candidateForVerification()).isTrue();
        var tables=jdbc.queryForList("SELECT tablename FROM pg_tables WHERE schemaname='ddip_management' AND tablename<>'flyway_schema_history' ORDER BY tablename",String.class);
        var before=new TreeMap<String,List<String>>();for(String table:tables)before.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));
        var files=filesystem();var migrations=jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IS NOT NULL ORDER BY installed_rank");
        var migration=org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        assertThat(migration.migrate().migrationsExecuted).isEqualTo(5);new SchemaLifecycle(migration,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(false);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE content_terminal_policy IS NOT NULL",Long.class)).isZero();
        for(String table:tables){
            String historical=table.equals("attempts")?"(to_jsonb(t)-'content_terminal_policy')":"to_jsonb(t)";
            assertThat(jdbc.queryForList("SELECT "+historical+"::text FROM ddip_management."+table+" t ORDER BY "+historical+"::text",String.class)).as(table).isEqualTo(before.get(table));
        }
        assertThat(filesystem()).isEqualTo(files);assertThat(bronzeFacts()).isEqualTo(launch);
        assertThat(jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IS NOT NULL AND version NOT IN ('19','20','21','22','23') ORDER BY installed_rank")).isEqualTo(migrations);
        for(String table:List.of("content_bronze_verification_profiles","content_bronze_verification_phases","content_bronze_checkpoints"))assertThat(count(table)).isZero();bronzeRetained();
    }
    @BeforeEach void verificationProfile(){
        var n=(ObjectNode)SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.content_bronze_launch_profiles WHERE launch_profile_id=?",byte[].class,BRONZE_PROFILE));
        n.remove("strictBronzeLaunchVersion");n.put("strictBronzeVerificationVersion",1);
        ((ObjectNode)n.get("manifest")).put("adapter","platform.ingestion.managed.StrictBronzeVerifier");
        n.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(n.get("manifest"))));
        n.put("strictRuntimeSha256","b".repeat(64));verification=SourceJson.canonical(n);
        profile(PROFILE,verification);
    }
    void profile(String id,byte[] bytes){jdbc.update("INSERT INTO ddip_management.content_bronze_verification_profiles(verification_profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",id,bytes,SourceJson.sha(bytes));}
    void candidate(String state){
        checkpoint();assertThatThrownBy(()->bronzeLaunch(b->{if(b.equals("RESERVED"))throw new IllegalStateException("STOP");})).hasMessage("STOP");
        for(String phase:List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED")){
            jdbc.update("INSERT INTO ddip_management.content_bronze_launch_phases(operation_id,phase,launch_profile_id,launch_profile_sha256,child_instance_id,child_pid,child_started_at,bootstrap_sha256,candidate_sha256,exit_code,storage_state,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?::timestamptz,?,?,?,?,?,?,?,?)",
                bronzeOperation,phase,phase.equals("LAUNCH_BOUND")?BRONZE_PROFILE:null,phase.equals("LAUNCH_BOUND")?jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.content_bronze_launch_profiles WHERE launch_profile_id=?",String.class,BRONZE_PROFILE):null,
                phase.equals("CHILD_RECORDED")?UUID.randomUUID():null,phase.equals("CHILD_RECORDED")?12345L:null,phase.equals("CHILD_RECORDED")?"2026-11-07T00:00:00Z":null,
                phase.equals("RELEASE_AUTHORIZED")?"0".repeat(64):null,phase.equals("CANDIDATE_EXITED")?CANDIDATE:null,phase.equals("CANDIDATE_EXITED")?(state.equals("EXACT")?0:1):null,phase.equals("CANDIDATE_EXITED")?state:null,
                owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
        }
    }
    void phase(String phase){
        jdbc.update("INSERT INTO ddip_management.content_bronze_verification_phases(operation_id,phase,verification_profile_id,verification_profile_sha256,candidate_sha256,child_instance_id,child_pid,child_started_at,bootstrap_sha256,receipt_bytes,receipt_sha256,verified_bytes,verified_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?::timestamptz,?,?,?,?,?,?,?, ?,?)",
            bronzeOperation,phase,phase.equals("VERIFY_BOUND")?PROFILE:null,phase.equals("VERIFY_BOUND")?SourceJson.sha(verification):null,phase.equals("VERIFY_BOUND")?CANDIDATE:null,
            phase.equals("CHILD_RECORDED")?UUID.randomUUID():null,phase.equals("CHILD_RECORDED")?12346L:null,phase.equals("CHILD_RECORDED")?"2026-11-07T00:00:01Z":null,
            phase.equals("RELEASE_AUTHORIZED")?"c".repeat(64):null,phase.equals("OBSERVED")?receipt:null,phase.equals("OBSERVED")?SourceJson.sha(receipt):null,phase.equals("OBSERVED")?anchor:null,phase.equals("OBSERVED")?SourceJson.sha(anchor):null,
            owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());
    }
    void released(){candidate("EXACT");for(String p:List.of("VERIFY_BOUND","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED"))phase(p);receipt();}
    void receipt(){
        byte[] descriptor=jdbc.queryForObject("SELECT journal_descriptor FROM ddip_management.content_fetch_checkpoints WHERE operation_id=?",byte[].class,operation);
        var d=SourceJson.parse(descriptor);var command=SourceJson.parse(jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_bronze_reservations WHERE operation_id=?",byte[].class,bronzeOperation));
        var n=SourceJson.object().put("receiptVersion",1);
        for(String key:List.of("runId","attemptId","sourceId","revision","capturedConfigSha256","ingestionId","ingestedAt","payloadByteLength","payloadSha256"))n.set(key,d.get(key));
        n.put("journalSha256",SourceJson.sha(descriptor));n.set("storageBindingId",command.get("storage").get("bindingId"));n.set("storageBindingSha256",command.get("storage").get("bindingSha256"));n.set("bucket",command.get("storage").get("configuration").get("bucket"));n.set("expected",d.get("expected"));n.put("payloadContentType","application/octet-stream").put("metadataContentType","application/json");
        receipt=SourceJson.canonical(n);makeAnchor();
    }
    UUID checkpointId(){return jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_bronze_reservations WHERE operation_id=?",UUID.class,bronzeOperation);}
    void makeAnchor(){var n=SourceJson.object().put("bronzeCheckpointVersion",1).put("operationId",bronzeOperation.toString()).put("fetchOperationId",operation.toString()).put("checkpointId",checkpointId().toString()).put("candidateSha256",CANDIDATE).put("verificationProfileSha256",SourceJson.sha(verification)).put("receiptSha256",SourceJson.sha(receipt));n.set("receipt",SourceJson.parse(receipt));anchor=SourceJson.canonical(n);}
    void checkpointRow(UUID id,byte[] stamp){jdbc.update("INSERT INTO ddip_management.content_bronze_checkpoints(operation_id,checkpoint_id,fetch_operation_id,candidate_sha256,receipt_bytes,receipt_sha256,verified_sha256,verified_stamp_bytes,verified_stamp_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)",bronzeOperation,id,operation,CANDIDATE,receipt,SourceJson.sha(receipt),SourceJson.sha(anchor),stamp,SourceJson.sha(stamp),owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion());}
    byte[] stamp(){return SourceJson.canonical(SourceJson.object().put("identity","1:2").put("modified","2026-11-07T00:00:00Z").put("size",anchor.length));}
    @ParameterizedTest @ValueSource(strings={"adapter","version","unknown","duplicate","float","foreign","oversized"})
    void malformedProfilesRejected(String change){
        var n=(ObjectNode)SourceJson.parse(verification);
        switch(change){case "adapter"->((ObjectNode)n.get("manifest")).put("adapter","platform.ingestion.managed.StrictManagedBronze");case "version"->n.put("strictBronzeVerificationVersion",2);case "unknown"->n.put("unknown",true);case "foreign"->n.put("storageBindingSha256","0".repeat(64));default->{}}
        String raw=new String(SourceJson.canonical(n),StandardCharsets.UTF_8);
        if(change.equals("duplicate"))raw=raw.replace("\"strictBronzeVerificationVersion\":1","\"strictBronzeVerificationVersion\":1,\"strictBronzeVerificationVersion\":1");
        if(change.equals("float"))raw=raw.replace("\"strictBronzeVerificationVersion\":1","\"strictBronzeVerificationVersion\":1.0");
        if(change.equals("oversized"))raw=" ".repeat(65536)+raw;
        byte[] bytes=raw.getBytes(StandardCharsets.UTF_8);assertThatThrownBy(()->profile("bad",bytes)).isInstanceOf(RuntimeException.class);
        assertThat(count("content_bronze_verification_profiles")).isEqualTo(1);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"ABSENT","PAYLOAD_ONLY","METADATA_ONLY","CONFLICT","UNAVAILABLE"})
    void nonExactCandidateCannotBeginVerification(String state){candidate(state);assertThatThrownBy(()->phase("VERIFY_BOUND")).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_verification_phases")).isZero();bronzeRetained();}
    @ParameterizedTest @ValueSource(strings={"START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","OBSERVED"})
    void cannotSkipBinding(String skipped){candidate("EXACT");receipt();assertThatThrownBy(()->phase(skipped)).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_verification_phases")).isZero();}
    @ParameterizedTest @ValueSource(strings={"runId","attemptId","journalSha256","storageBindingId","bucket","payloadSha256","metadataContentType","unknown","duplicate","float"})
    void receiptMustMatchOriginalNativeFacts(String field){
        released();var n=(ObjectNode)SourceJson.parse(receipt);n.put(field,"foreign");
        if(field.equals("duplicate")||field.equals("float")){n.remove(field);String raw=new String(SourceJson.canonical(n),StandardCharsets.UTF_8);raw=raw.replace("\"receiptVersion\":1",field.equals("duplicate")?"\"receiptVersion\":1,\"receiptVersion\":1":"\"receiptVersion\":1.0");receipt=raw.getBytes(StandardCharsets.UTF_8);}else receipt=SourceJson.canonical(n);
        if(!field.equals("duplicate")&&!field.equals("float"))makeAnchor();assertThatThrownBy(()->phase("OBSERVED")).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_checkpoints")).isZero();
    }
    @ParameterizedTest @ValueSource(strings={"operationId","fetchOperationId","checkpointId","candidateSha256","verificationProfileSha256","receiptSha256","unknown"})
    void anchorMustMatchExactVerification(String key){released();var n=(ObjectNode)SourceJson.parse(anchor);n.put(key,"foreign");anchor=SourceJson.canonical(n);assertThatThrownBy(()->phase("OBSERVED")).isInstanceOf(RuntimeException.class);}
    @Test void checkpointRequiresObservedAndExactStampAndIdentity(){
        released();assertThatThrownBy(()->checkpointRow(checkpointId(),stamp())).isInstanceOf(RuntimeException.class);
        phase("OBSERVED");assertThatThrownBy(()->checkpointRow(UUID.randomUUID(),stamp())).isInstanceOf(RuntimeException.class);
        byte[] wrong=SourceJson.canonical(SourceJson.object().put("identity","1:2").put("modified","now").put("size",anchor.length+1));
        assertThatThrownBy(()->checkpointRow(checkpointId(),wrong)).isInstanceOf(RuntimeException.class);
        checkpointRow(checkpointId(),stamp());assertThat(count("content_bronze_checkpoints")).isEqualTo(1);bronzeRetained();assertThat(objectRequests).hasValue(0);
        for(String table:List.of("content_bronze_verification_profiles","content_bronze_verification_phases","content_bronze_checkpoints")){
            for(String sql:List.of("UPDATE ddip_management."+table+" SET created_at=created_at","DELETE FROM ddip_management."+table,"TRUNCATE ddip_management."+table+" CASCADE"))assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(RuntimeException.class);
        }
    }
    @Test void heartbeatRefreshAllowedButStaleOwnerRejected(){candidate("EXACT");phase("VERIFY_BOUND");var previous=owner;owner=workers.heartbeat(guard,owner);var fresh=owner;owner=previous;assertThatThrownBy(()->phase("START_AUTHORIZED")).isInstanceOf(RuntimeException.class);owner=fresh;phase("START_AUTHORIZED");assertThat(count("content_bronze_verification_phases")).isEqualTo(2);}
    @ParameterizedTest @ValueSource(strings={"cancel","stop","worker","run","attempt"})
    void checkpointRequiresFreshOriginalOwner(String change){released();phase("OBSERVED");switch(change){case "cancel"->cancel();case "stop"->jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",owner.worker().instanceId());case "run"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());}assertThatThrownBy(()->checkpointRow(checkpointId(),stamp())).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_checkpoints")).isZero();}
    @ParameterizedTest @ValueSource(strings={"ISOLATION_REPEATABLE_READ","ISOLATION_SERIALIZABLE"})
    void snapshotIsolationRejected(String level){candidate("EXACT");var transaction=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));transaction.setIsolationLevelName(level);assertThatThrownBy(()->transaction.execute(s->{phase("VERIFY_BOUND");return null;})).isInstanceOf(RuntimeException.class);assertThat(count("content_bronze_verification_phases")).isZero();}
    @ParameterizedTest @ValueSource(strings={"profile","candidate","digest"})
    void bindingRequiresExactProfileAndCandidate(String change){
        candidate("EXACT");
        assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.content_bronze_verification_phases(operation_id,phase,verification_profile_id,verification_profile_sha256,candidate_sha256,owner_coordinator_version,owner_worker_version,owner_run_version,owner_attempt_version) VALUES (?,'VERIFY_BOUND',?,?,?,?,?,?,?)",bronzeOperation,change.equals("profile")?"foreign":PROFILE,change.equals("digest")?"0".repeat(64):SourceJson.sha(verification),change.equals("candidate")?"0".repeat(64):CANDIDATE,owner.worker().coordinatorVersion(),owner.worker().workerVersion(),owner.runVersion(),owner.attemptVersion())).isInstanceOf(RuntimeException.class);
        assertThat(count("content_bronze_verification_phases")).isZero();
    }
    @ParameterizedTest @ValueSource(strings={"identity","modified","unknown","size","duplicate","float"})
    void checkpointStampRejectsMalformedEvidence(String field){
        released();phase("OBSERVED");var n=(ObjectNode)SourceJson.parse(stamp());
        switch(field){case "identity"->n.put("identity","not-an-inode");case "modified"->n.put("modified","");case "unknown"->n.put("unknown",true);case "size"->n.put("size",anchor.length+1);default->{}}
        String raw=new String(SourceJson.canonical(n),StandardCharsets.UTF_8);
        if(field.equals("duplicate"))raw=raw.replace("\"size\":"+anchor.length,"\"size\":"+anchor.length+",\"size\":"+anchor.length);
        if(field.equals("float"))raw=raw.replace("\"size\":"+anchor.length,"\"size\":"+anchor.length+".0");
        byte[] bytes=raw.getBytes(StandardCharsets.UTF_8);assertThatThrownBy(()->checkpointRow(checkpointId(),bytes)).isInstanceOf(RuntimeException.class);
        assertThat(count("content_bronze_checkpoints")).isZero();
    }

}
