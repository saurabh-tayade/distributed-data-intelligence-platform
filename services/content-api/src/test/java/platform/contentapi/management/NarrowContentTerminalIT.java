package platform.contentapi.management;

import java.time.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;

/** Original READY and real FETCH prefixes; no synthetic checkpoint or recovered execution owner. */
class NarrowContentTerminalIT extends NarrowContentFetchLaunchFixtures {
    UUID control,terminal,id;MutableClock clock;AtomicReference<String> observed;NarrowContentTerminalTestSupport repo;
    @BeforeEach void terminalSetup(){terminal=UUID.randomUUID();id=UUID.randomUUID();clock=new MutableClock();observed=new AtomicReference<>("ABSENT");repo=repository(b->{});}
    NarrowContentTerminalTestSupport repository(Consumer<String> fault){return new NarrowContentTerminalTestSupport(jdbc,tx,INSTALL,clock,observed::get,fault);}
    void operator(){
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,owner.runId());
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);
        control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL,p->{}).request(ControlCommands.parse(owner.runId().toString(),SourceJson.canonical(request),"content-terminal","\""+version+"\"")).get("controlId").stringValue());
        guard.close();guard=InstallationHostGuard.acquire(binding);
        var recovery=new WorkerRepository(jdbc,tx,INSTALL).register(guard,CONTENT_PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());assertThat(recovery.status()).isEqualTo("RECOVERY_ONLY");clock.now=Instant.now();
    }
    NarrowContentTerminalTestSupport.Target target(){return repo.inspect(guard,owner.runId(),owner.attemptId(),control,terminal,id);}
    NarrowContentTerminalTestSupport.Qualified qualify(){return repo.qualify(guard,target(),clock.instant(),"");}
    @Test void sealedReadyPrefixCanEndWithoutLaunchingFetch()throws Exception{
        prepared();operator();var files=filesystem();var q=qualify();assertThat(repo.finish(guard,q)).isEqualTo("COMMITTED");
        assertThat(jdbc.queryForObject("SELECT state||':'||outcome_code FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("FAILED:NATIVE_CONTENT_INTERRUPTED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo("INTERRUPTED");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE safe_code='NATIVE_CONTENT_INTERRUPTED'",Long.class)).isEqualTo(6);
        assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(0);assertThat(repo.lookup(id)).isEqualTo("CONFIRMED_TERMINAL");
        assertThatThrownBy(()->repo.finish(guard,q)).hasMessage("FRESH_QUALIFICATION_REQUIRED");
    }
    @ParameterizedTest @ValueSource(strings={"ORIGINAL","ATTEMPT","RUN","CONTROL","WORKER","COORDINATOR","AUDIT","RECEIPT"})
    void everyMutationFailureRollsBackAndConsumesCapability(String boundary){
        prepared();operator();var q=qualify();var before=metadata();var failing=repository(b->{if(b.equals(boundary))throw new IllegalStateException("FAULT");});
        assertThat(failing.finish(guard,q)).isEqualTo("CONFIRMED_ROLLBACK");assertThat(metadata()).isEqualTo(before);assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_ROLLBACK");
        assertThatThrownBy(()->repo.finish(guard,q)).hasMessage("FRESH_QUALIFICATION_REQUIRED");assertThat(requests).hasValue(0);
    }
    @ParameterizedTest @ValueSource(strings={"namespace","handlers","unmanaged","orphans","descendants","gaps","gateways","remote","checkpoints","digest","nonce"})
    void everyFreshExactAttestationIsRequired(String omitted){prepared();operator();var t=target();var before=metadata();assertThatThrownBy(()->repo.qualify(guard,t,clock.instant(),omitted)).isInstanceOf(RuntimeException.class);assertThat(metadata()).isEqualTo(before);}
    @ParameterizedTest @ValueSource(strings={"LIVE","UNAVAILABLE"})
    void negativeProcessVeto(String state){prepared();operator();var q=qualify();observed.set(state);assertThatThrownBy(()->repo.finish(guard,q)).hasMessage("PROCESS_UNRESOLVED");assertThat(count("content_original_terminal_receipts")).isZero();}
    @ParameterizedTest @ValueSource(strings={"expired","lostGuard","lostAck"})
    void postcommitFaultsResolveAsCommittedNeverRollback(String fault){prepared();operator();var q=qualify();var failing=repository(b->{if(b.equals("COMMITTED")){if(fault.equals("expired"))clock.now=clock.now.plusSeconds(61);else if(fault.equals("lostGuard"))guard.close();else throw new IllegalStateException("ACK_LOST");}});assertThat(failing.finish(guard,q)).isEqualTo("CONFIRMED_TERMINAL");assertThat(repo.lookup(id)).isEqualTo("CONFIRMED_TERMINAL");}
    @Test void checkpointFailurePreservesActualFetchWithoutMoreRequests()throws Exception{prepared();launch(b->{});NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});operator();var files=filesystem();assertThat(repo.finish(guard,qualify())).isEqualTo("COMMITTED");assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);assertThat(count("content_fetch_checkpoints")).isEqualTo(1);}
    @ParameterizedTest @ValueSource(strings={"RESERVED","LAUNCH_BOUND","DIRECTORY_CREATED","COMMAND_WRITTEN","FILES_READY","CANDIDATE_EXITED"})
    void realFetchPrefixIsRetainedWithoutContinuation(String boundary)throws Exception{
        prepared();assertThatThrownBy(()->launch(b->{if(b.equals(boundary))throw new IllegalStateException("PREFIX_STOP");})).isInstanceOf(RuntimeException.class);operator();var files=filesystem();int before=requests.get();assertThat(repo.finish(guard,qualify())).isEqualTo("COMMITTED");assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(before);assertThat(count("content_fetch_checkpoints")).isZero();assertThat(repo.lookup(id)).isEqualTo("CONFIRMED_TERMINAL");
    }
    void commandPrefix()throws Exception{
        prepared();assertThatThrownBy(()->launch(b->{if(b.equals("COMMAND_WRITTEN"))throw new IllegalStateException("PREFIX_STOP");})).isInstanceOf(RuntimeException.class);
        java.nio.file.Files.move(fetchDirectory().resolve("command.json"),fetchDirectory().resolve("command.json.preparing"));java.nio.file.Files.writeString(fetchDirectory().resolve("command.json.preparing"),"{");operator();
    }
    @Test void changedButStillValidPartialBytesInvalidateExactReviewedInventory()throws Exception{
        commandPrefix();var q=qualify();java.nio.file.Files.writeString(fetchDirectory().resolve("command.json.preparing"),"{ ");var before=metadata();assertThatThrownBy(()->repo.finish(guard,q)).hasMessage("EVIDENCE_CHANGED");assertThat(metadata()).isEqualTo(before);assertThat(count("content_original_terminal_receipts")).isZero();
    }
    @Test void interruptedDualHardlinksRemainIneligibleAndUntouched()throws Exception{
        commandPrefix();java.nio.file.Files.createLink(temp.resolve("partial-hardlink"),fetchDirectory().resolve("command.json.preparing"));var before=metadata();assertThatThrownBy(this::qualify).isInstanceOf(RuntimeException.class);assertThat(metadata()).isEqualTo(before);assertThat(java.nio.file.Files.exists(temp.resolve("partial-hardlink"))).isTrue();
    }
    @Test void receiptEnrollsBeforeLifecycleAndCannotCommitByFlushingConstraintsEarly(){
        prepared();operator();var q=qualify();var before=metadata();var visited=new java.util.ArrayList<String>();
        var early=repository(b->{visited.add(b);if(b.equals("RECEIPT")){
            assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo("CLAIMED");
            assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
            jdbc.execute("SET CONSTRAINTS ALL IMMEDIATE");
        }});
        assertThat(early.finish(guard,q)).isEqualTo("CONFIRMED_ROLLBACK");assertThat(visited).contains("RECEIPT").doesNotContain("ORIGINAL");assertThat(metadata()).isEqualTo(before);assertThat(count("content_original_terminal_receipts")).isZero();
    }
    @Test void actualCommitAcknowledgmentLossResolvesWithoutRepeatingMutation(){
        prepared();operator();var q=qualify();var armed=new java.util.concurrent.atomic.AtomicBoolean();var lost=loseCommitAcknowledgment(armed);
        var failing=new NarrowContentTerminalTestSupport(lost,transactions(lost),INSTALL,clock,observed::get,b->{if(b.equals("RECEIPT"))armed.set(true);});
        assertThat(failing.finish(guard,q)).isEqualTo("CONFIRMED_TERMINAL");assertThat(armed).isFalse();assertThat(count("content_original_terminal_receipts")).isEqualTo(1);assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_TERMINAL");
    }
    @Test void freshnessRecheckedAfterCoordinatorLockWait()throws Exception{
        prepared();operator();var q=qualify();var entering=new java.util.concurrent.CountDownLatch(1);var locked=new java.util.concurrent.CountDownLatch(1);var before=metadata();
        var waiting=repository(b->{if(b.equals("BEFORE_TRANSACTION")){entering.countDown();try{if(!locked.await(10,java.util.concurrent.TimeUnit.SECONDS))throw new AssertionError("LOCK_TIMEOUT");}catch(InterruptedException e){throw new AssertionError(e);}}});
        try(var pool=java.util.concurrent.Executors.newVirtualThreadPerTaskExecutor()){
            var result=pool.submit(()->waiting.finish(guard,q));assertThat(entering.await(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();
            try(var c=jdbc.getDataSource().getConnection()){c.setAutoCommit(false);try(var statement=c.createStatement()){
                int blocker;try(var rows=statement.executeQuery("SELECT pg_backend_pid()")){rows.next();blocker=rows.getInt(1);}try(var rows=statement.executeQuery("SELECT singleton_id FROM ddip_management.coordinator FOR UPDATE")){assertThat(rows.next()).isTrue();}locked.countDown();
                boolean blocked=false;long until=System.nanoTime()+java.util.concurrent.TimeUnit.SECONDS.toNanos(10);while(System.nanoTime()<until){blocked=jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND ?=ANY(pg_blocking_pids(pid)))",Boolean.class,blocker);if(blocked)break;Thread.sleep(10);}assertThat(blocked).isTrue();clock.now=clock.now.plusSeconds(61);
            }finally{locked.countDown();c.rollback();}}
            assertThat(result.get(15,java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("CONFIRMED_ROLLBACK");
        }finally{locked.countDown();}assertThat(metadata()).isEqualTo(before);
    }
    @Test void receiptForDifferentNonceCannotResolveAnotherQualification(){prepared();operator();var first=qualify();var second=qualify();assertThat(repo.finish(guard,first)).isEqualTo("COMMITTED");assertThat(repo.resolve(second)).isEqualTo("UNRESOLVED");assertThat(repo.lookup(id)).isEqualTo("CONFIRMED_TERMINAL");}
    @Test void freshInspectionCannotReuseOldReviewEvenOnUnchangedIdsAndSql(){prepared();operator();var first=target();var second=target();assertThat(first.digest()).isNotEqualTo(second.digest());assertThatThrownBy(()->repo.qualifyWithOldReview(guard,second,first,clock.instant())).hasMessage("QUIESCENCE_REQUIRED");}
    @Test void laterQueueOwnershipDoesNotInvalidateReceipt(){prepared();operator();assertThat(repo.finish(guard,qualify())).isEqualTo("COMMITTED");UUID next=queue("after-content-terminal");guard.close();ready();var nextOwner=NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),next).owner();assertThat(nextOwner).isNotNull();assertThat(repo.lookup(id)).isEqualTo("CONFIRMED_TERMINAL");}
    static final class MutableClock extends Clock {Instant now=Instant.now();public java.time.ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(java.time.ZoneId zone){return this;}public Instant instant(){return now;}}
}
