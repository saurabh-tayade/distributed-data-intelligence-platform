package platform.contentapi.management;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.support.*;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;

/** Full test-only composition using real native FETCH evidence; process vetoes are synthetic. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchRecoveryCompositionIT extends NarrowContentFetchLaunchFixtures {
    UUID control,successor,handoff,finalization;
    WorkerRepository.Session recovery;
    NarrowContentFetchRecoveryFinalizationIT.MutableClock clock;
    AtomicReference<String> observed;
    NarrowContentFetchRecoveryComposition flow;
    List<NarrowContentFetchRecoveryComposition.Target> reviews;
    AtomicReference<String> fail;
    @BeforeEach void compositionSetup(){
        control=null;successor=UUID.randomUUID();handoff=UUID.randomUUID();finalization=UUID.randomUUID();
        clock=new NarrowContentFetchRecoveryFinalizationIT.MutableClock(Instant.now());observed=new AtomicReference<>("ABSENT");
        reviews=new ArrayList<>();fail=new AtomicReference<>("");flow=composition(jdbc,tx,b->{if(b.equals(fail.get()))throw new IllegalStateException("TEST_COMPOSITION_FAULT");});
    }
    NarrowContentFetchRecoveryComposition composition(JdbcTemplate db,ManagementTransactions transaction,Consumer<String> boundary){
        return new NarrowContentFetchRecoveryComposition(db,transaction,INSTALL,clock,observed::get,boundary);
    }
    NarrowContentFetchRecoveryComposition.Review review(NarrowContentFetchRecoveryComposition.Target target){
        reviews.add(target);return new NarrowContentFetchRecoveryComposition.Review(target.digest(),clock.instant(),true,true,true,Set.of(target.checkpoint()),List.of(),true,true);
    }
    void start(){flow.start(guard,owner.runId(),owner.attemptId(),recovery,control,successor,handoff,finalization,this::review);}
    void checkpoint(){prepared();launch(b->{});NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});}
    void recover(){
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,owner.runId());
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);
        control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL,processes->{}).request(ControlCommands.parse(owner.runId().toString(),SourceJson.canonical(request),"native-terminal","\""+version+"\"")).get("controlId").stringValue());
        guard.close();guard=InstallationHostGuard.acquire(binding);
        recovery=new WorkerRepository(jdbc,tx,INSTALL).register(guard,CONTENT_PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());
        assertThat(recovery.status()).isEqualTo("RECOVERY_ONLY");clock.now=Instant.now();
    }
    Map<String,List<String>> history(){var rows=new TreeMap<>(metadata());for(String table:List.of("controls","content_native_operations","content_native_launch_phases","content_fetch_completion_intents","content_fetch_checkpoints","content_fetch_recovery_handoffs","content_fetch_recovery_finalizations","run_sources","run_inputs","artifacts"))rows.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));return rows;}
    Map<String,List<String>> preserved(){var rows=history();for(String t:List.of("coordinator","worker_instances","runs","attempts","controls","audit_events","content_fetch_recovery_finalizations"))rows.remove(t);return rows;}
    void occupied(){assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(successor);noExecution();}
    void released(){
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(jdbc.queryForObject("SELECT owner_instance_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(jdbc.queryForObject("SELECT mode FROM ddip_management.coordinator",String.class)).isEqualTo("READY");noExecution();
    }

    @Test void completeLifecycleRequiresTwoDifferentReviewsAndPreservesNativeEvidence()throws Exception{
        checkpoint();recover();var evidence=preserved();var files=filesystem();start();occupied();
        assertThat(reviews).hasSize(1);assertThat(flow.finish(guard,this::review)).isEqualTo("COMMITTED");released();
        assertThat(reviews).hasSize(2);assertThat(reviews.get(0).checkpoint()).isEqualTo(reviews.get(1).checkpoint());
        assertThat(reviews.get(0).digest()).isNotEqualTo(reviews.get(1).digest());
        evidence.remove("content_fetch_recovery_handoffs");var after=preserved();after.remove("content_fetch_recovery_handoffs");
        assertThat(after).isEqualTo(evidence);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);
        assertThat(jdbc.queryForObject("SELECT state||':'||outcome_code||':'||retry_eligible FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("FAILED:NATIVE_FETCH_INTERRUPTED:false");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo("INTERRUPTED");
        assertThat(jdbc.queryForObject("SELECT state||':'||outcome_code FROM ddip_management.attempts WHERE attempt_id=?",String.class,successor)).isEqualTo("SUCCEEDED:RECONCILED_FAILED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,control)).isEqualTo("APPLIED");
        assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances WHERE instance_id=?",String.class,recovery.instanceId())).isEqualTo("STOPPED");
        var terminal=history();assertThat(flow.finish(guard,t->{throw new AssertionError("REPLAY_REVIEW");})).isEqualTo("CONFIRMED_TERMINAL");
        assertThat(flow.lookupHandoff(handoff)).isEqualTo("CONFIRMED_HANDOFF");assertThat(flow.lookupTerminal(finalization)).isEqualTo("CONFIRMED_TERMINAL");
        assertThat(history()).isEqualTo(terminal);assertThat(count("attempts")).isEqualTo(2);
    }
    @ParameterizedTest @ValueSource(strings={"CANDIDATE_EXITED","INTENT_COMMITTED","PREFIX_WRITTEN","MARKER_WRITTEN"})
    void incompletePrefixesCannotReachReviewOrTransfer(String prefix)throws Exception{
        prepared();launch(b->{});if(!prefix.equals("CANDIDATE_EXITED"))assertThatThrownBy(()->NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{if(b.equals(prefix))throw new IllegalStateException("TEST_PREFIX");})).isInstanceOf(RuntimeException.class);
        recover();var before=history();var files=filesystem();assertThatThrownBy(this::start).isInstanceOf(RuntimeException.class);
        assertThat(reviews).isEmpty();assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);retained();
    }
    @ParameterizedTest @ValueSource(strings={"ASSESSED","HANDOFF_BEFORE_TRANSFER","HANDOFF_OLD_ATTEMPT","HANDOFF_SUCCESSOR","HANDOFF_COORDINATOR","HANDOFF_RUN","HANDOFF_HANDOFF","HANDOFF_AUDIT"})
    void failureBeforeHandoffCommitLeavesOriginalOwnership(String boundary)throws Exception{
        checkpoint();recover();var before=history();var files=filesystem();fail.set(boundary);
        assertThatThrownBy(this::start).hasMessage("TEST_COMPOSITION_FAULT");assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);retained();
        assertThatThrownBy(()->flow.finish(guard,this::review)).hasMessage("OWNERSHIP_LOST");
        if(!boundary.equals("ASSESSED"))assertThat(flow.resolveHandoff()).isEqualTo("CONFIRMED_ROLLBACK");
        assertThatThrownBy(this::start).hasMessage("INVALID_REQUEST");
    }
    @ParameterizedTest @ValueSource(strings={"TERMINAL_BEFORE_FINISH","TERMINAL_ATTEMPT","TERMINAL_RUN","TERMINAL_CONTROL","TERMINAL_WORKER","TERMINAL_COORDINATOR","TERMINAL_AUDIT","TERMINAL_RECEIPT"})
    void rollbackRetainsOwnerAndRequiresAnotherFreshTerminalReview(String boundary)throws Exception{
        checkpoint();recover();start();var before=history();var files=filesystem();fail.set(boundary);
        assertThatThrownBy(()->flow.finish(guard,this::review)).hasMessage("TEST_COMPOSITION_FAULT");
        assertThat(flow.resolveTerminal()).isEqualTo("CONFIRMED_ROLLBACK");assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);occupied();
        fail.set("");clock.now=clock.now.plusSeconds(61);assertThat(flow.finish(guard,this::review)).isEqualTo("COMMITTED");
        assertThat(reviews).hasSize(3);assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);released();
    }
    @Test void handoffReviewCannotAuthorizeTerminalDisposition(){
        checkpoint();recover();var handoffReview=new AtomicReference<NarrowContentFetchRecoveryComposition.Review>();
        flow.start(guard,owner.runId(),owner.attemptId(),recovery,control,successor,handoff,finalization,t->{var r=review(t);handoffReview.set(r);return r;});
        var before=history();assertThatThrownBy(()->flow.finish(guard,t->handoffReview.get())).isInstanceOf(RuntimeException.class);
        assertThat(history()).isEqualTo(before);occupied();assertThat(flow.finish(guard,this::review)).isEqualTo("COMMITTED");released();
    }
    @ParameterizedTest @ValueSource(strings={"LIVE","UNAVAILABLE","THROWS"})
    void processObservationNeverReplacesReview(String state){
        checkpoint();recover();start();observed.set(state);var before=history();
        assertThatThrownBy(()->flow.finish(guard,this::review)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);occupied();
    }
    @ParameterizedTest @ValueSource(strings={"stale","future","handlers","unmanaged","orphans","descendants","gaps","checkpoint"})
    void independentTerminalAttestationsAndFreshnessAreMandatory(String omitted){
        checkpoint();recover();start();var before=history();
        assertThatThrownBy(()->flow.finish(guard,t->new NarrowContentFetchRecoveryComposition.Review(t.digest(),clock.instant().plusSeconds(omitted.equals("stale")?-61:omitted.equals("future")?1:0),!omitted.equals("handlers"),!omitted.equals("unmanaged"),!omitted.equals("orphans"),omitted.equals("checkpoint")?Set.of():Set.of(t.checkpoint()),List.of(),!omitted.equals("descendants"),!omitted.equals("gaps")))).isInstanceOf(RuntimeException.class);
        assertThat(history()).isEqualTo(before);occupied();
    }
    @ParameterizedTest @ValueSource(strings={"assessment","handoff-review","terminal-review"})
    void fileChangesAcrossCompositionAreNeverAdopted(String where)throws Exception{
        checkpoint();recover();
        if(where.equals("assessment"))Files.writeString(fetchDirectory().resolve("payload.xml"),"changed");
        if(where.equals("terminal-review"))start();
        if(where.equals("assessment"))assertThatThrownBy(this::start).isInstanceOf(RuntimeException.class);
        else {
            java.util.function.Function<NarrowContentFetchRecoveryComposition.Target,NarrowContentFetchRecoveryComposition.Review> mutate=t->{try{Files.writeString(fetchDirectory().resolve("payload.xml"),"changed");}catch(Exception ex){throw new AssertionError(ex);}return review(t);};
            if(where.equals("handoff-review"))assertThatThrownBy(()->flow.start(guard,owner.runId(),owner.attemptId(),recovery,control,successor,handoff,finalization,mutate)).isInstanceOf(RuntimeException.class);
            else assertThatThrownBy(()->flow.finish(guard,mutate)).isInstanceOf(RuntimeException.class);
        }
        assertThat(count("content_fetch_recovery_finalizations")).isZero();assertThat(requests).hasValue(1);
        if(where.equals("terminal-review"))occupied();else retained();
    }
    @ParameterizedTest @ValueSource(strings={"HANDOFF_COMMITTED","TERMINAL_COMMITTED"})
    void lostReturnNeverReplaysMutationOrRecreatesAuthority(String boundary){
        checkpoint();recover();fail.set(boundary);
        if(boundary.startsWith("HANDOFF")){
            assertThatThrownBy(this::start).hasMessage("TEST_COMPOSITION_FAULT");var before=history();
            assertThat(flow.resolveHandoff()).isEqualTo("CONFIRMED_HANDOFF");
            assertThatThrownBy(()->flow.finish(guard,this::review)).hasMessage("OWNERSHIP_LOST");
            assertThat(history()).isEqualTo(before);occupied();
        }else{
            start();assertThatThrownBy(()->flow.finish(guard,this::review)).hasMessage("TEST_COMPOSITION_FAULT");var before=history();
            assertThat(flow.resolveTerminal()).isEqualTo("CONFIRMED_TERMINAL");assertThat(flow.finish(guard,t->{throw new AssertionError("NO_RETRY");})).isEqualTo("CONFIRMED_TERMINAL");assertThat(history()).isEqualTo(before);released();
        }
    }
    @ParameterizedTest @ValueSource(strings={"HANDOFF_AUDIT","TERMINAL_AUDIT"})
    void actualCommitAcknowledgmentLossCannotTurnInspectionIntoAuthority(String boundary){
        checkpoint();recover();var armed=new AtomicBoolean();var lost=loseCommitAcknowledgment(armed);
        var lostTx=new ManagementTransactions(new TransactionTemplate(new DataSourceTransactionManager(lost.getDataSource())));
        flow=composition(lost,lostTx,b->{if(b.equals(boundary))armed.set(true);});
        if(boundary.startsWith("HANDOFF")){
            assertThatThrownBy(this::start).isInstanceOf(RuntimeException.class);assertThat(flow.resolveHandoff()).isEqualTo("CONFIRMED_HANDOFF");
            assertThatThrownBy(()->flow.finish(guard,this::review)).hasMessage("OWNERSHIP_LOST");occupied();
        }else{start();assertThatThrownBy(()->flow.finish(guard,this::review)).isInstanceOf(RuntimeException.class);assertThat(flow.resolveTerminal()).isEqualTo("CONFIRMED_TERMINAL");released();}
        assertThat(armed).isFalse();assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);assertThat(requests).hasValue(1);
    }
    @Test void newHarnessAndReacquiredGuardCannotResumeRetainedOwner(){
        checkpoint();recover();start();guard.close();guard=InstallationHostGuard.acquire(binding);var before=history();
        var fresh=composition(jdbc,tx,b->{throw new AssertionError("NO_WRITE");});
        assertThat(fresh.lookupHandoff(handoff)).isEqualTo("CONFIRMED_HANDOFF");
        assertThatThrownBy(()->fresh.finish(guard,this::review)).hasMessage("OWNERSHIP_LOST");
        assertThatThrownBy(()->flow.finish(guard,this::review)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);occupied();
    }
    @Test void laterQueueWorkLeavesBothHistoricalReceiptsInspectable(){
        checkpoint();recover();start();flow.finish(guard,this::review);UUID next=queue("after-composition");guard.close();ready();
        var claimed=NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),next).owner();assertThat(claimed).isNotNull();
        var before=history();assertThat(flow.lookupHandoff(handoff)).isEqualTo("CONFIRMED_HANDOFF");assertThat(flow.lookupTerminal(finalization)).isEqualTo("CONFIRMED_TERMINAL");assertThat(history()).isEqualTo(before);assertThat(requests).hasValue(1);
    }
    @ParameterizedTest @ValueSource(strings={"HANDOFF_COMMITTED","OWNER_RETAINED","TERMINAL_BEFORE_FINISH","TERMINAL_RECEIPT","TERMINAL_COMMITTED"})
    void actualJvmDeathRetainsOccupiedOwnershipUntilTerminalCommit(String boundary)throws Exception{
        checkpoint();recover();guard.close();guard=null;
        // Stop the fixture's unused recovery registration before the disposable JVM registers itself.
        jdbc.update("UPDATE ddip_management.worker_instances SET status='STOPPED',row_version=row_version+1 WHERE instance_id=?",recovery.instanceId());
        Path descriptor=temp.resolve("composition-binding.json");Files.write(descriptor,binding.descriptor());
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                "platform.contentapi.execution.NarrowContentFetchCompositionTestProcess",url,user,password,descriptor.toString(),CONTENT_PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest(),
                owner.runId().toString(),owner.attemptId().toString(),control.toString(),successor.toString(),handoff.toString(),finalization.toString(),boundary)
                .redirectError(ProcessBuilder.Redirect.INHERIT).start();
        try{assertThat(child.waitFor(30,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isEqualTo(73);
            assertThat(child.inputReader().lines().toList()).contains("COMPOSITION_CRASH "+boundary);
        }finally{if(child.isAlive()){child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);}}
        guard=InstallationHostGuard.acquire(binding);var before=history();var files=filesystem();
        var restarted=composition(jdbc,tx,b->{throw new AssertionError("RESTART_MUTATION");});
        assertThat(restarted.lookupHandoff(handoff)).isEqualTo("CONFIRMED_HANDOFF");
        assertThatThrownBy(()->restarted.finish(guard,this::review)).hasMessage("OWNERSHIP_LOST");
        if(boundary.equals("TERMINAL_COMMITTED")){assertThat(restarted.lookupTerminal(finalization)).isEqualTo("CONFIRMED_TERMINAL");released();}
        else{assertThat(restarted.lookupTerminal(finalization)).isEqualTo("UNRESOLVED");assertThat(count("content_fetch_recovery_finalizations")).isZero();occupied();}
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);
    }

}
