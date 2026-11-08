package platform.contentapi.management;

import java.util.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static platform.contentapi.management.SourceFixtures.*;
import static org.assertj.core.api.Assertions.*;

/** Real original FETCH/publisher/verifier chain. Silver only reserves SQL and never runs a child. */
abstract class NarrowContentSilverReservationFixtures extends NarrowContentBronzeLaunchFixtures {
    static final String VERIFY_PROFILE="strict-bronze-verification-test";
    UUID silverOperation;
    @BeforeEach void verificationProfile(){
        silverOperation=UUID.randomUUID();
        var c=(ObjectNode)SourceJson.parse(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.content_bronze_launch_profiles WHERE launch_profile_id=?",byte[].class,BRONZE_PROFILE));
        c.remove("strictBronzeLaunchVersion");c.put("strictBronzeVerificationVersion",1);
        var manifest=(ObjectNode)c.get("manifest");manifest.put("adapter","platform.ingestion.managed.StrictBronzeVerifier");
        c.put("deploymentSha256",SourceJson.sha(SourceJson.canonical(manifest)));
        c.put("strictRuntimeSha256",SourceJson.sha(SourceJson.canonical(SourceJson.object().put("strictBronzeVerifierRuntimeVersion",1).put("adapter","platform.ingestion.managed.StrictBronzeVerifier").put("javaRuntime",Runtime.version().toString()))));
        byte[] bytes=SourceJson.canonical(c);jdbc.update("INSERT INTO ddip_management.content_bronze_verification_profiles(verification_profile_id,configuration_bytes,configuration_sha256) VALUES (?,?,?)",VERIFY_PROFILE,bytes,SourceJson.sha(bytes));
    }
    NarrowContentSilverReservationTestSupport.Facts chain(Consumer<String> completion,Consumer<String> fault){return chain(jdbc,tx,workers,completion,fault);}
    NarrowContentSilverReservationTestSupport.Facts chain(JdbcTemplate j,ManagementTransactions t,WorkerRepository w,Consumer<String> completion,Consumer<String> fault){
        return NarrowContentSilverReservationTestSupport.chain(j,t,w,guard,owner,operation,LAUNCH_PROFILE,bronzeOperation,BRONZE_PROFILE,VERIFY_PROFILE,silverOperation,completion,fault);
    }
    NarrowContentSilverReservationTestSupport.Facts silverFacts(){return NarrowContentSilverReservationTestSupport.inspect(jdbc,tx,silverOperation);}
    void noSilver(){assertThat(count("content_silver_reservations")).isZero();retained();}
    void noReplay()throws Exception{var before=filesystem();int calls=objectRequests.get();assertThatThrownBy(()->chain(b->{},b->{})).isInstanceOf(RuntimeException.class);assertThat(filesystem()).isEqualTo(before);assertThat(objectRequests).hasValue(calls);}
    void original(){prepared();objectError=null;}
    void changeOwner(String change){switch(change){
        case "run"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());
        case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
        case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",owner.worker().instanceId());
        case "coordinator"->jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");
        case "stop"->jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
        case "cancel"->cancel();
        case "deadline"->{
            // Fixture-only clock passage; restore immutable guards before the tested reservation.
            jdbc.execute("ALTER TABLE ddip_management.attempts DISABLE TRIGGER USER");
            try{jdbc.update("UPDATE ddip_management.attempts SET started_at=clock_timestamp()-interval '3 hours' WHERE attempt_id=?",owner.attemptId());}
            finally{jdbc.execute("ALTER TABLE ddip_management.attempts ENABLE TRIGGER USER");}
        }
        default->throw new AssertionError(change);
    }}
}
