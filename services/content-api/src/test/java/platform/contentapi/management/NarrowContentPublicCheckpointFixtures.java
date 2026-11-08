package platform.contentapi.management;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.BeforeEach;
import org.springframework.jdbc.support.JdbcTransactionManager;
import org.springframework.transaction.support.TransactionTemplate;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Component-only SQL/native-format fixtures. Never public-child or network qualification. */
abstract class NarrowContentPublicCheckpointFixtures extends NarrowContentAdmissionFixtures {
    static final long SYNTHETIC_PID=987654321L;
    static final Instant SYNTHETIC_STARTED=Instant.parse("2026-11-08T00:00:00Z");
    static final List<String> FETCH_PHASES=List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED");
    UUID operation,syntheticChild,control;
    Path candidateDirectory;
    WorkerRepository.Session recovery;
    @Override void profile(String id,boolean content,String source){
        if(content)NarrowContentFetchReservationTestSupport.registerChain(jdbc,id,RunFixtures.profile(true,source),CONTENT,null,false);
        else super.profile(id,false,source);
    }
    @Override void storage(String id,String product,String role){
        if(!role.equals("BRONZE")){super.storage(id,product,role);return;}
        byte[] bytes=SourceJson.canonical(SourceJson.object().put("importBronzeVersion",1).put("endpoint","http://127.0.0.1:9000")
                .put("region","local").put("bucket","bronze").put("credentialsFile","/never/read/component-only-credentials"));
        jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,bytes,SourceJson.sha(bytes),INSTALL);
    }
    @Override void ready(){
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE));
    }
    @BeforeEach void publicCheckpointProfiles(){
        operation=UUID.randomUUID();syntheticChild=UUID.randomUUID();candidateDirectory=null;control=null;recovery=null;
        NarrowContentPublicHistoryTestSupport.registerProfiles(jdbc,CONTENT_PROFILE,binding.id());
    }
    void reserved(){claimed();prepare();NarrowContentPublicHistoryTestSupport.reserve(jdbc,tx,workers,guard,owner,operation);}
    byte[] reservation(){return jdbc.queryForObject("SELECT bootstrap_bytes FROM ddip_management.content_native_operations WHERE operation_id=?",byte[].class,operation);}
    byte[] command(){return jdbc.queryForObject("SELECT command_bytes FROM ddip_management.content_native_operations WHERE operation_id=?",byte[].class,operation);}
    String launchDigest(){return jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.content_fetch_launch_profiles WHERE launch_profile_id='public-fetch'",String.class);}
    byte[] expectedPacket(){
        var r=SourceJson.parse(reservation());
        var p=SourceJson.object().put("bootstrapVersion",2).put("mode","PUBLIC_FETCH_SOURCE_CANDIDATE").put("childPid",Long.toString(SYNTHETIC_PID));
        for(String key:List.of("expected","current","fetch","publicFetch"))p.set(key,r.get(key));
        return SourceJson.canonical(p);
    }
    void phase(String phase){
        jdbc.update("INSERT INTO ddip_management.content_native_launch_phases(operation_id,phase,launch_profile_id,launch_profile_sha256,child_instance_id,child_pid,child_started_at,bootstrap_sha256,candidate_sha256) VALUES (?,?,?,?,?,?,?,?,?)",
                operation,phase,phase.equals("LAUNCH_BOUND")?"public-fetch":null,phase.equals("LAUNCH_BOUND")?launchDigest():null,
                phase.equals("CHILD_RECORDED")?syntheticChild:null,phase.equals("CHILD_RECORDED")?SYNTHETIC_PID:null,
                phase.equals("CHILD_RECORDED")?java.sql.Timestamp.from(SYNTHETIC_STARTED):null,
                phase.equals("RELEASE_AUTHORIZED")?SourceJson.sha(expectedPacket()):null,
                phase.equals("CANDIDATE_EXITED")?NarrowContentPublicCheckpointTestSupport.candidate(candidateDirectory,command()):null);
    }
    void released(){reserved();for(String p:FETCH_PHASES.subList(0,5))phase(p);}
    void candidate()throws Exception{
        released();candidateDirectory=NarrowContentPublicCheckpointTestSupport.syntheticCandidate(jdbc,guard,operation);phase("CANDIDATE_EXITED");
    }
    NarrowContentFetchCompletionTestSupport.Observation checkpoint()throws Exception{
        candidate();return NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});
    }
    NarrowContentFetchLaunchTestSupport.Facts launchFacts(){return NarrowContentFetchLaunchTestSupport.inspect(jdbc,tx,workers,operation);}
    String historyDigest(){return NarrowContentPublicHistoryTestSupport.digest(jdbc,tx,owner.runId(),owner.attemptId());}
    Map<String,List<String>> history(){
        var result=new TreeMap<>(metadata());
        for(String table:List.of("controls","content_native_operations","content_native_launch_phases","content_fetch_completion_intents","content_fetch_checkpoints",
                "content_fetch_recovery_handoffs","content_fetch_recovery_finalizations","content_bronze_reservations","content_bronze_launch_phases",
                "content_bronze_verification_phases","content_bronze_checkpoints","content_original_terminal_receipts",
                "content_fetch_launch_profiles","content_bronze_launch_profiles","content_bronze_verification_profiles","content_pipeline_profiles"))
            result.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));
        return result;
    }
    void recover(){
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,owner.runId());
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);
        control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL,p->{}).request(ControlCommands.parse(owner.runId().toString(),SourceJson.canonical(request),"public-component-reconcile","\""+version+"\"")).get("controlId").stringValue());
        guard.close();guard=InstallationHostGuard.acquire(binding);
        recovery=new WorkerRepository(jdbc,tx,INSTALL).register(guard,CONTENT_PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());
        assertThat(recovery.status()).isEqualTo("RECOVERY_ONLY");
    }
    void assertCorruptHistoryRejected(String table,Runnable corruption,Runnable read){
        var before=history();var changed=new AtomicBoolean();
        var raw=new TransactionTemplate(new JdbcTransactionManager(jdbc.getDataSource()));
        // Deliberately invalid history is visible only in this rolled-back test transaction.
        // All production guards are restored by rollback, and no execution entry point is called.
        assertThatThrownBy(()->raw.execute(status->{
            jdbc.execute("ALTER TABLE ddip_management."+table+" DISABLE TRIGGER ALL");
            corruption.run();changed.set(true);read.run();throw new AssertionError("CORRUPTED_COMPONENT_HISTORY_ACCEPTED");
        })).isInstanceOf(RuntimeException.class);
        assertThat(changed).isTrue();assertThat(history()).isEqualTo(before);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM pg_trigger t JOIN pg_class c ON c.oid=t.tgrelid JOIN pg_namespace n ON n.oid=c.relnamespace WHERE n.nspname='ddip_management' AND c.relname=? AND t.tgenabled='D'",Long.class,table)).isZero();
    }
    void replaceConfiguration(String table,String idColumn,String id,ObjectNode n){
        byte[] bytes=SourceJson.canonical(n);
        jdbc.update("UPDATE ddip_management."+table+" SET configuration_bytes=?,configuration_sha256=? WHERE "+idColumn+"=?",bytes,SourceJson.sha(bytes),id);
    }
}
