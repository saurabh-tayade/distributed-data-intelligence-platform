package platform.contentapi.management;

import java.util.*;
import java.lang.reflect.*;
import java.sql.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import java.nio.file.*;
import platform.contentapi.execution.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;

/** Real PostgreSQL atomicity, replay and native supervisor cleanup checks, without public-network or native effects. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RunningCancelIT extends RunApiFixtures {
    UUID run; ControlRepository controls;
    @Override void storage(String id,String product,String role){
        if(!role.equals("CONTROL")){super.storage(id,product,role);return;}
        try{var b=OwnershipFixtures.host(Files.createTempDirectory(temp,"cancel-"));var named=new InstallationHostGuard.Binding(id,INSTALL,b.root(),b.rootIdentity(),b.guardIdentity(),b.installationIdentity());jdbc.update("INSERT INTO ddip_management.storage_bindings(binding_id,product,role,configuration_bytes,configuration_sha256,installation_id) VALUES (?,?,?,?,?,?)",id,product,role,named.descriptor(),named.digest(),INSTALL);}catch(Exception ex){throw new IllegalStateException(ex);}
    }
    InstallationHostGuard guard()throws Exception{
        Path descriptor=Files.write(Files.createTempFile(temp,"cancel-binding-",".json"),jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.storage_bindings WHERE binding_id='reliability-CONTROL'",byte[].class));
        return InstallationHostGuard.acquire(OwnershipFixtures.readBinding(descriptor));
    }
    WorkerRepository.Session register(WorkerRepository worker,InstallationHostGuard guard){return worker.register(guard,JSON_PROFILE,jdbc.queryForObject("SELECT configuration_sha256 FROM ddip_management.execution_profiles WHERE profile_id=?",String.class,JSON_PROFILE),"a".repeat(64));}
    @BeforeEach void queued(){run=UUID.fromString(submit(reliability(false),"original").get("runId").stringValue());controls=new ControlRepository(jdbc,tx,INSTALL);}
    ControlCommands.Command command(String key,long version){return ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","CANCEL").putNull("attemptId").putNull("quiescenceConfirmed")),key,"\""+version+"\"");}
    long count(String table){return jdbc.queryForObject("SELECT count(*) FROM ddip_management."+table,Long.class);}
    String state(){return jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run);}
    @Test void requestReplaysAndOriginalOwnerCancelsBeforeAnyLaunch()throws Exception{
        try(var guard=guard()){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var owner=worker.claim(guard,register(worker,guard),UUID.randomUUID()).owner();
            var receipt=controls.request(command("cancel",1));assertThat(receipt.get("state").stringValue()).isEqualTo("REQUESTED");assertThat(state()).isEqualTo("CANCEL_REQUESTED");
            assertThat(controls.request(command("cancel",1))).isEqualTo(receipt);
            var finished=RunningCancellationTestSupport.stop(worker,guard,owner,false);assertThat(finished.status()).isEqualTo("READY");assertThat(state()).isEqualTo("CANCELLED");
            assertThat(controls.request(command("cancel",1)).get("state").stringValue()).isEqualTo("APPLIED");assertThat(count("controls")).isEqualTo(1);
            assertThat(jdbc.queryForObject("SELECT active_attempt_id IS NULL FROM ddip_management.coordinator",Boolean.class)).isTrue();
            assertThatThrownBy(()->worker.markRunning(guard,owner)).hasMessage("OWNERSHIP_LOST");
        }
    }
    @Test void uncertainStopNeverReleasesEvenBeforeWriter()throws Exception{
        try(var guard=guard()){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var owner=worker.claim(guard,register(worker,guard),UUID.randomUUID()).owner();controls.request(command("cancel",1));
            var finished=RunningCancellationTestSupport.stop(worker,guard,owner,true);assertThat(finished.status()).isEqualTo("RECOVERY_ONLY");assertThat(state()).isEqualTo("RECOVERY_REQUIRED");
            assertThat(controls.request(command("cancel",1)).get("state").stringValue()).isEqualTo("REQUESTED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());

        }
        try(var replacementGuard=guard()){var replacement=WorkerTestSupport.reliability(jdbc,tx,INSTALL);assertThat(register(replacement,replacementGuard).status()).isNotEqualTo("READY");}
    }
    @Test void authorizedWriterRequiresVerificationEvenAfterCleanStop()throws Exception{
        try(var guard=guard()){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var owner=worker.claim(guard,register(worker,guard),UUID.randomUUID()).owner();
            jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,command_address,command_sha256) VALUES (?,'PUBLISH_RELIABILITY',0,?,'INTENT',?,'cancel-test/command.json',?)",owner.attemptId(),UUID.randomUUID(),"a".repeat(64),"b".repeat(64));
            controls.request(command("cancel",1));assertThat(RunningCancellationTestSupport.stop(worker,guard,owner,false).status()).isEqualTo("RECOVERY_ONLY");assertThat(state()).isEqualTo("RECOVERY_REQUIRED");
            assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations",String.class)).isEqualTo("UNKNOWN");assertThat(controls.request(command("cancel",1)).get("state").stringValue()).isEqualTo("REQUESTED");
        }
    }
    @Test void supervisedChildIsReapedBeforeCancellationApplied()throws Exception{childStop(false);}
    @Test void uncertainDescendantObservationRetainsOccupiedSlot()throws Exception{childStop(true);}
    private void childStop(boolean uncertain)throws Exception{
        try(var guard=guard()){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var owner=worker.claim(guard,register(worker,guard),UUID.randomUUID()).owner();controls.request(command("cancel",1));
            assertThat(RunningCancellationTestSupport.stopWithChild(worker,guard,owner,uncertain).status()).isEqualTo(uncertain?"RECOVERY_ONLY":"READY");
            assertThat(state()).isEqualTo(uncertain?"RECOVERY_REQUIRED":"CANCELLED");
        }
    }
    @Test void unrelatedRunVersionMutationCannotBeAdopted()throws Exception{
        try(var guard=guard()){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var owner=worker.claim(guard,register(worker,guard),UUID.randomUUID()).owner();controls.request(command("cancel",1));
            jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",run);
            assertThatThrownBy(()->RunningCancellationTestSupport.stop(worker,guard,owner,false)).hasMessage("OWNERSHIP_LOST");assertThat(state()).isEqualTo("CANCEL_REQUESTED");
        }
    }
    @Test void cancellationAndTerminalFailureSerializeWithoutMixedOutcome()throws Exception{
        try(var guard=guard();var pool=Executors.newFixedThreadPool(2)){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var owner=worker.claim(guard,register(worker,guard),UUID.randomUUID()).owner();var start=new CountDownLatch(1);
            var terminal=pool.submit(()->{start.await();try{worker.failBeforeLaunch(guard,owner,WorkerRepository.PrelaunchFailure.PROCESS_START_FAILED);return "FAILED";}catch(RuntimeException ex){return ex.getMessage();}});
            var cancel=pool.submit(()->{start.await();try{return controls.request(command("cancel",1)).get("state").stringValue();}catch(RuntimeException ex){return ex.getMessage();}});start.countDown();
            String finished=terminal.get(15,TimeUnit.SECONDS),requested=cancel.get(15,TimeUnit.SECONDS);
            if(finished.equals("FAILED")){assertThat(requested).isEqualTo("PRECONDITION_FAILED");assertThat(state()).isEqualTo("FAILED");assertThat(count("controls")).isZero();}
            else{assertThat(finished).isEqualTo("CANCEL_REQUESTED");assertThat(requested).isEqualTo("REQUESTED");assertThat(state()).isEqualTo("CANCEL_REQUESTED");assertThat(count("controls")).isEqualTo(1);}
        }
    }
    @Test void concurrentSameKeyRequestsResolveOneReceipt()throws Exception{
        try(var guard=guard();var pool=Executors.newFixedThreadPool(2)){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);worker.claim(guard,register(worker,guard),UUID.randomUUID());var start=new CountDownLatch(1);
            Callable<JsonNode> request=()->{start.await();return controls.request(command("cancel",1));};var a=pool.submit(request);var b=pool.submit(request);start.countDown();
            assertThat(a.get(15,TimeUnit.SECONDS)).isEqualTo(b.get(15,TimeUnit.SECONDS));assertThat(count("controls")).isEqualTo(1);
        }
    }
    @Test void lostRequestAcknowledgmentReplaysSameDurableIntent()throws Exception{
        try(var guard=guard()){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);worker.claim(guard,register(worker,guard),UUID.randomUUID());
            var wrapped=commitFault("INSERT INTO ddip_management.controls");var api=new ControlRepository(wrapped,transactions(wrapped),INSTALL);
            assertThatThrownBy(()->api.request(command("lost",1))).hasMessage("MANAGEMENT_UNAVAILABLE");
            assertThat(state()).isEqualTo("CANCEL_REQUESTED");assertThat(controls.request(command("lost",1)).get("state").stringValue()).isEqualTo("REQUESTED");assertThat(count("controls")).isEqualTo(1);
        }
    }
    @Test void lostCancellationSettlementAcknowledgmentNeverReturnsReusableOwner()throws Exception{
        try(var guard=guard()){
            var plain=WorkerTestSupport.reliability(jdbc,tx,INSTALL);var owner=plain.claim(guard,register(plain,guard),UUID.randomUUID()).owner();controls.request(command("lost",1));
            var wrapped=commitFault("UPDATE ddip_management.runs SET state=?");var worker=WorkerTestSupport.reliability(wrapped,transactions(wrapped),INSTALL);
            assertThatThrownBy(()->RunningCancellationTestSupport.stop(worker,guard,owner,false)).hasMessage("MANAGEMENT_UNAVAILABLE");
            assertThat(state()).isEqualTo("CANCELLED");assertThat(controls.request(command("lost",1)).get("state").stringValue()).isEqualTo("APPLIED");
            assertThatThrownBy(()->worker.heartbeat(guard,owner)).hasMessage("OWNERSHIP_LOST");
        }
    }
    JdbcTemplate commitFault(String prefix){var once=new AtomicBoolean();var data=new AbstractDataSource(){
        public Connection getConnection()throws SQLException{var real=jdbc.getDataSource().getConnection();var matching=new AtomicBoolean();
            return (Connection)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{Connection.class},(p,m,a)->{try{
                if(m.getName().equals("prepareStatement") && a[0] instanceof String sql && sql.startsWith(prefix))matching.set(true);
                Object result=m.invoke(real,a);if(m.getName().equals("commit") && matching.get() && once.compareAndSet(false,true))throw new SQLException("INJECTED","08006");return result;
            }catch(InvocationTargetException ex){throw ex.getCause();}});
        }
        public Connection getConnection(String user,String password)throws SQLException{return getConnection();}
    };return new JdbcTemplate(data);}
    @Test void admissionRollsBackAllChangesIfAuditFails()throws Exception{
        try(var guard=guard()){
            var worker=WorkerTestSupport.reliability(jdbc,tx,INSTALL);worker.claim(guard,register(worker,guard),UUID.randomUUID());
            jdbc.execute("CREATE FUNCTION ddip_management.fail_running_cancel() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.action='RUN_CANCEL_REQUESTED' THEN RAISE EXCEPTION 'INJECTED'; END IF; RETURN NEW; END $$");
            jdbc.execute("CREATE TRIGGER fail_running_cancel BEFORE INSERT ON ddip_management.audit_events FOR EACH ROW EXECUTE FUNCTION ddip_management.fail_running_cancel()");
            assertThatThrownBy(()->controls.request(command("cancel",1))).isInstanceOf(RuntimeException.class);assertThat(state()).isEqualTo("RUNNING");assertThat(count("controls")).isZero();
        }
    }
}
