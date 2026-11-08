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

/** Inactive terminal recovery: real native evidence and PostgreSQL, explicit synthetic process observations. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchRecoveryFinalizationIT extends NarrowContentFetchLaunchFixtures {
    UUID control,successor,handoff,finalization;
    WorkerRepository.Session recovery;
    NarrowContentFetchRecoveryHandoffTestSupport.Owner terminalOwner;
    MutableClock clock;
    AtomicReference<String> observed;
    NarrowContentFetchRecoveryHandoffTestSupport handoffs;
    NarrowContentFetchRecoveryFinalizationTestSupport repo;
    boolean versionFifteen;
    @BeforeEach void terminalSetup(){
        control=null;successor=UUID.randomUUID();handoff=UUID.randomUUID();finalization=UUID.randomUUID();
        recovery=null;terminalOwner=null;clock=new MutableClock(Instant.now());observed=new AtomicReference<>("ABSENT");
        handoffs=new NarrowContentFetchRecoveryHandoffTestSupport(jdbc,tx,INSTALL,clock,observed::get,b->{});repo=repository(b->{});
    }
    @Override void migrateFixtureSchema(){
        if(!versionFifteen){super.migrateFixtureSchema();return;}
        org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").target("15").load().migrate();
        new JdbcCoordinatorRepository(jdbc,tx,INSTALL).initialize();
    }
    @AfterEach void resetSchemaTarget(){versionFifteen=false;}
    NarrowContentFetchRecoveryFinalizationTestSupport repository(Consumer<String> boundary){return new NarrowContentFetchRecoveryFinalizationTestSupport(jdbc,tx,INSTALL,clock,observed::get,boundary);}
    void checkpoint(){prepared();launch(b->{});NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});}
    void recover(){
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,owner.runId());
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);
        control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL,processes->{}).request(ControlCommands.parse(owner.runId().toString(),SourceJson.canonical(request),"native-terminal","\""+version+"\"")).get("controlId").stringValue());
        guard.close();guard=InstallationHostGuard.acquire(binding);
        recovery=new WorkerRepository(jdbc,tx,INSTALL).register(guard,CONTENT_PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());
        assertThat(recovery.status()).isEqualTo("RECOVERY_ONLY");clock.now=Instant.now();
    }
    void transfer(){transfer(List.of());}
    void transfer(List<NarrowContentFetchRecoveryHandoffTestSupport.ProcessIdentity> descendants){
        var t=handoffs.inspect(guard,recovery,control,successor,handoff);
        terminalOwner=handoffs.transferOwner(guard,handoffs.qualify(guard,t,clock.instant(),"",descendants));
    }
    void pending(){checkpoint();recover();transfer();}
    NarrowContentFetchRecoveryFinalizationTestSupport.Target target(){return repo.inspect(guard,terminalOwner,finalization);}
    NarrowContentFetchRecoveryFinalizationTestSupport.Qualified qualify(){return repo.qualify(guard,target(),clock.instant(),"");}
    Map<String,List<String>> history(){var rows=new TreeMap<>(metadata());for(String table:List.of("controls","content_native_operations","content_native_launch_phases","content_fetch_completion_intents","content_fetch_checkpoints","content_fetch_recovery_handoffs","content_fetch_recovery_finalizations","run_sources","run_inputs","artifacts"))rows.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));return rows;}
    Map<String,List<String>> preserved(){var rows=history();for(String t:List.of("coordinator","worker_instances","runs","attempts","controls","audit_events","content_fetch_recovery_finalizations"))rows.remove(t);return rows;}
    void occupied(){assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(successor);noExecution();}
    void released(){
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(jdbc.queryForObject("SELECT owner_instance_id FROM ddip_management.coordinator",UUID.class)).isNull();
        assertThat(jdbc.queryForObject("SELECT mode FROM ddip_management.coordinator",String.class)).isEqualTo("READY");noExecution();
    }
    @Test void intactCheckpointEndsRunPreservesEvidenceAndReleasesOnlySlot()throws Exception{
        pending();Files.delete(installedFetchJar);var before=preserved();var files=filesystem();
        String old=jdbc.queryForObject("SELECT to_jsonb(a)::text FROM ddip_management.attempts a WHERE attempt_id=?",String.class,owner.attemptId());
        var q=qualify();assertThat(repo.finish(guard,q)).isEqualTo("COMMITTED");released();
        assertThat(preserved()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);
        assertThat(jdbc.queryForObject("SELECT to_jsonb(a)::text FROM ddip_management.attempts a WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo(old);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo("INTERRUPTED");
        assertThat(jdbc.queryForObject("SELECT state||':'||outcome_code FROM ddip_management.attempts WHERE attempt_id=?",String.class,successor)).isEqualTo("SUCCEEDED:RECONCILED_FAILED");
        assertThat(jdbc.queryForObject("SELECT state||':'||outcome_code||':'||retry_eligible FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("FAILED:NATIVE_FETCH_INTERRUPTED:false");
        assertThat(jdbc.queryForObject("SELECT state||':'||reason FROM ddip_management.controls WHERE control_id=?",String.class,control)).isEqualTo("APPLIED:NATIVE_FETCH_INTERRUPTED");
        assertThat(jdbc.queryForObject("SELECT status FROM ddip_management.worker_instances WHERE instance_id=?",String.class,recovery.instanceId())).isEqualTo("STOPPED");
        assertThat(count("content_fetch_recovery_finalizations")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE safe_code='NATIVE_FETCH_INTERRUPTED'",Long.class)).isEqualTo(5);
        var after=history();assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_TERMINAL");assertThat(repo.finish(guard,q)).isEqualTo("CONFIRMED_TERMINAL");
        assertThat(repository(b->{throw new AssertionError("READ_ONLY");}).lookup(finalization)).isEqualTo("CONFIRMED_TERMINAL");assertThat(handoffs.lookup(handoff)).isEqualTo("CONFIRMED_HANDOFF");assertThat(history()).isEqualTo(after);
        assertThat(NarrowContentFetchRecoveryFinalizationTestSupport.publicSeams()).doesNotContain("authorize","bootstrap","dispatch","resume","transfer");
    }
    @Test void laterQueueClaimDoesNotInvalidateHistoricalReceipts(){
        pending();repo.finish(guard,qualify());UUID next=queue("after-terminal");guard.close();ready();
        var nextOwner=NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),next).owner();assertThat(nextOwner).isNotNull();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(nextOwner.attemptId());
        assertThat(repo.lookup(finalization)).isEqualTo("CONFIRMED_TERMINAL");assertThat(handoffs.lookup(handoff)).isEqualTo("CONFIRMED_HANDOFF");assertThat(requests).hasValue(1);
    }
    @ParameterizedTest @ValueSource(strings={"LIVE","UNAVAILABLE","THROWS"})
    void processUncertaintyVetoesQualificationAndFinalization(String state)throws Exception{
        pending();var q=qualify();observed.set(state);var before=history();var files=filesystem();
        assertThatThrownBy(this::qualify).isInstanceOf(RuntimeException.class);assertThatThrownBy(()->repo.finish(guard,q)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);occupied();
    }
    @ParameterizedTest @ValueSource(strings={"handlers","unmanaged","orphans","descendants","gaps","checkpoint","digest","stale","future"})
    void everyExplicitReviewAndFreshnessBindingIsRequired(String missing)throws Exception{
        pending();var t=target();var before=history();var files=filesystem();Instant at=clock.instant().plusSeconds(missing.equals("stale")?-61:missing.equals("future")?1:0);
        assertThatThrownBy(()->repo.qualify(guard,t,at,missing)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);occupied();
    }
    @ParameterizedTest @ValueSource(strings={"BEFORE_FINISH","ATTEMPT","RUN","CONTROL","WORKER","COORDINATOR","AUDIT","RECEIPT"})
    void eachMutationBoundaryRollsBackAndConsumesQualification(String boundary)throws Exception{
        pending();var q=qualify();var before=history();var files=filesystem();var hit=new AtomicBoolean();var failing=repository(b->{if(b.equals(boundary)){hit.set(true);throw new IllegalStateException("TEST_FAULT");}});
        assertThatThrownBy(()->failing.finish(guard,q)).isInstanceOf(RuntimeException.class);assertThat(hit).isTrue();assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_ROLLBACK");
        assertThatThrownBy(()->repo.finish(guard,q)).hasMessage("FRESH_QUALIFICATION_REQUIRED");assertThat(history()).isEqualTo(before);occupied();
    }
    @Test void confirmedRollbackNeedsFreshQualificationButNotAnotherTakeover(){
        pending();var old=qualify();var failing=repository(b->{if(b.equals("RUN"))throw new IllegalStateException("TEST_ROLLBACK");});
        assertThatThrownBy(()->failing.finish(guard,old)).isInstanceOf(RuntimeException.class);assertThat(repo.resolve(old)).isEqualTo("CONFIRMED_ROLLBACK");
        assertThatThrownBy(()->repo.finish(guard,old)).hasMessage("FRESH_QUALIFICATION_REQUIRED");
        assertThat(repo.finish(guard,qualify())).isEqualTo("COMMITTED");assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);assertThat(count("attempts")).isEqualTo(2);released();
    }
    @Test void postCommitFaultAndFreshLookupNeverRemintAuthority(){
        pending();var q=qualify();var failing=repository(b->{if(b.equals("COMMITTED"))throw new IllegalStateException("TEST_LOST_RESPONSE");});
        assertThatThrownBy(()->failing.finish(guard,q)).isInstanceOf(RuntimeException.class);var after=history();assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_TERMINAL");assertThat(repository(b->{}).lookup(finalization)).isEqualTo("CONFIRMED_TERMINAL");assertThat(repo.finish(guard,q)).isEqualTo("CONFIRMED_TERMINAL");assertThat(history()).isEqualTo(after);released();
    }
    @Test void actualCommitAcknowledgmentLossResolvesReadOnlyWithoutRetryingMutations(){
        pending();var q=qualify();var armed=new AtomicBoolean(false);JdbcTemplate lost=loseCommitAcknowledgment(armed);var lostTx=new ManagementTransactions(new TransactionTemplate(new DataSourceTransactionManager(lost.getDataSource())));
        var failing=new NarrowContentFetchRecoveryFinalizationTestSupport(lost,lostTx,INSTALL,clock,observed::get,b->{if(b.equals("AUDIT"))armed.set(true);});
        assertThatThrownBy(()->failing.finish(guard,q)).isInstanceOf(RuntimeException.class);assertThat(armed).isFalse();var after=history();assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_TERMINAL");assertThat(repository(b->{}).lookup(finalization)).isEqualTo("CONFIRMED_TERMINAL");assertThat(repo.finish(guard,q)).isEqualTo("CONFIRMED_TERMINAL");assertThat(history()).isEqualTo(after);assertThat(count("content_fetch_recovery_finalizations")).isEqualTo(1);assertThat(count("attempts")).isEqualTo(2);released();
    }
    @ParameterizedTest @ValueSource(strings={"missing","payload","marker","lock","ledger"})
    void changedFilesAfterQualificationNeverFinalizeOrRepair(String change)throws Exception{
        pending();var q=qualify();switch(change){case "missing"->Files.move(attemptRoot(),temp.resolve("detached"));case "payload"->Files.writeString(fetchDirectory().resolve("payload.xml"),"tampered");case "marker"->Files.delete(fetchDirectory().resolve("complete.json"));case "lock"->{Path lock=attemptRoot().resolve(NarrowContentAdmissionTestSupport.LOCK);Files.move(lock,temp.resolve("old-lock"));Files.createFile(lock);}case "ledger"->Files.writeString(attemptRoot().resolve(NarrowContentAdmissionTestSupport.MANIFEST),"{}");}
        var before=history();var files=filesystem();assertThatThrownBy(()->repo.finish(guard,q)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);occupied();assertThat(requests).hasValue(1);
    }
    @Test void qualificationExpiryDuringProcessIoFailsBeforeFinalization(){
        pending();var q=qualify();var before=history();var checked=new AtomicBoolean();
        var expiring=new NarrowContentFetchRecoveryFinalizationTestSupport(jdbc,tx,INSTALL,clock,()->{
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            if(checked.compareAndSet(false,true))clock.now=clock.now.plusSeconds(61);
            return "ABSENT";
        },b->{});
        assertThatThrownBy(()->expiring.finish(guard,q)).isInstanceOf(RuntimeException.class);assertThat(checked).isTrue();assertThat(history()).isEqualTo(before);occupied();
    }
    @ParameterizedTest @ValueSource(strings={"BEFORE_FINISH","AUDIT"})
    void guardLossAtSqlBoundaryCannotCommitTerminalState(String boundary){
        pending();var q=qualify();var before=history();var hit=new AtomicBoolean();
        var closing=repository(b->{if(b.equals(boundary)){hit.set(true);guard.close();}});
        assertThatThrownBy(()->closing.finish(guard,q)).isInstanceOf(RuntimeException.class);assertThat(hit).isTrue();assertThat(history()).isEqualTo(before);occupied();
    }
    @Test void reviewExpiresWhileWaitingForCoordinatorLockWithoutMutation()throws Exception{
        pending();var q=qualify();var before=history();var atBoundary=new CountDownLatch(1);var lockHeld=new CountDownLatch(1);
        var waiting=repository(b->{if(b.equals("BEFORE_FINISH")){atBoundary.countDown();try{if(!lockHeld.await(10,TimeUnit.SECONDS))throw new AssertionError("LOCK_NOT_HELD");}catch(InterruptedException ex){Thread.currentThread().interrupt();throw new AssertionError(ex);}}});
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()){
            var result=pool.submit(()->{try{waiting.finish(guard,q);return "UNEXPECTED_COMMIT";}catch(RuntimeException ex){return ex.getMessage();}});
            try(var connection=jdbc.getDataSource().getConnection()){
                assertThat(atBoundary.await(10,TimeUnit.SECONDS)).isTrue();connection.setAutoCommit(false);
                try(var statement=connection.createStatement()){
                    statement.setQueryTimeout(5);int blocker;
                    try(var rows=statement.executeQuery("SELECT pg_backend_pid()")){assertThat(rows.next()).isTrue();blocker=rows.getInt(1);}
                    try(var rows=statement.executeQuery("SELECT singleton_id FROM ddip_management.coordinator FOR UPDATE")){assertThat(rows.next()).isTrue();}
                    lockHeld.countDown();boolean blocked=false;long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
                    while(System.nanoTime()<deadline){
                        blocked=jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND ? = ANY(pg_blocking_pids(pid)))",Boolean.class,blocker);
                        if(blocked)break;Thread.sleep(10);
                    }
                    assertThat(blocked).as("finalization must actually wait on the coordinator lock").isTrue();clock.now=clock.now.plusSeconds(61);
                }finally{lockHeld.countDown();connection.rollback();}
            }finally{lockHeld.countDown();}
            assertThat(result.get(15,TimeUnit.SECONDS)).isEqualTo("FRESH_QUALIFICATION_REQUIRED");
        }
        assertThat(history()).isEqualTo(before);assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_ROLLBACK");occupied();
    }
    @ParameterizedTest @ValueSource(strings={"stale","guard","run-version","coordinator-version","worker-version"})
    void changedFenceOrExpiredQualificationCannotFinalize(String change){
        pending();var q=qualify();switch(change){case "stale"->clock.now=clock.now.plusSeconds(61);case "guard"->guard.close();case "run-version"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());case "coordinator-version"->jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");case "worker-version"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",recovery.instanceId());}
        var before=history();assertThatThrownBy(()->repo.finish(guard,q)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);occupied();
    }
    @Test void historicalReceiptAndRestartCannotRecreateTerminalAuthority()throws Exception{
        pending();var before=history();assertThatThrownBy(()->repo.inspect(guard,null,finalization)).isInstanceOf(RuntimeException.class);
        assertThat(repo.lookup(finalization)).isEqualTo("UNRESOLVED");assertThat(handoffs.lookup(handoff)).isEqualTo("CONFIRMED_HANDOFF");
        guard.close();guard=InstallationHostGuard.acquire(binding);
        assertThatThrownBy(this::qualify).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);occupied();
    }
    @ParameterizedTest @ValueSource(strings={"CANDIDATE_EXITED","INTENT_COMMITTED","PREFIX_WRITTEN","MARKER_WRITTEN"})
    void incompleteEvidenceNeverAcquiresTerminalOwner(String prefix)throws Exception{
        prepared();launch(b->{});if(!prefix.equals("CANDIDATE_EXITED"))assertThatThrownBy(()->NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{if(b.equals(prefix))throw new IllegalStateException("TEST_PREFIX");})).isInstanceOf(RuntimeException.class);
        recover();var before=history();var files=filesystem();assertThatThrownBy(this::transfer).isInstanceOf(RuntimeException.class);assertThatThrownBy(this::target).isInstanceOf(RuntimeException.class);
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(count("content_fetch_recovery_finalizations")).isZero();assertThat(count("content_fetch_checkpoints")).isZero();assertThat(requests).hasValue(1);
    }
    @Test void descendantsFromHandoffCannotBeForgottenAndAreRechecked(){
        checkpoint();recover();var descendant=new NarrowContentFetchRecoveryHandoffTestSupport.ProcessIdentity(ProcessHandle.current().pid(),Instant.EPOCH);transfer(List.of(descendant));
        var t=target();var before=history();assertThatThrownBy(()->repo.qualify(guard,t,clock.instant(),"")).hasMessage("QUIESCENCE_REQUIRED");
        var state=new AtomicReference<>("ABSENT");var calls=new AtomicInteger();
        var specific=new NarrowContentFetchRecoveryFinalizationTestSupport(jdbc,tx,INSTALL,clock,p->{assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();if(p.equals(descendant)){calls.incrementAndGet();return state.get();}return "ABSENT";},b->{});
        var q=specific.qualify(guard,t,clock.instant(),"",List.of(descendant));assertThat(calls.get()).isPositive();state.set("LIVE");
        assertThatThrownBy(()->specific.finish(guard,q)).hasMessage("PROCESS_UNRESOLVED");assertThat(history()).isEqualTo(before);occupied();
    }
    @Test void concurrentQualifiedFinishesCreateOneReceipt()throws Exception{
        pending();var first=qualify();var other=repo.inspect(guard,terminalOwner,UUID.randomUUID());var second=repo.qualify(guard,other,clock.instant(),"");var start=new CountDownLatch(1);
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()){
            var tasks=List.of(first,second).stream().map(q->pool.submit(()->{start.await();try{return repo.finish(guard,q);}catch(RuntimeException expected){return "REJECTED";}})).toList();start.countDown();int committed=0;for(var task:tasks)if(task.get(20,TimeUnit.SECONDS).equals("COMMITTED"))committed++;assertThat(committed).isEqualTo(1);
        }
        assertThat(count("content_fetch_recovery_finalizations")).isEqualTo(1);released();assertThat(requests).hasValue(1);
    }
    @ParameterizedTest @ValueSource(strings={"CANCEL","RECONCILE","REJECTED"})
    void changedControlSetVetoesStaleQualification(String type){
        pending();var q=qualify();insertControl(jdbc,type);var before=history();
        assertThatThrownBy(()->repo.finish(guard,q)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);occupied();
    }
    void insertControl(JdbcTemplate db,String type){
        byte[] raw=SourceJson.canonical(SourceJson.object());boolean reconcile=type.equals("RECONCILE"),rejected=type.equals("REJECTED");
        db.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,target_attempt_id,quiescence_confirmed,state,reason,applied_at,requester) VALUES (?,?,?,?,?,?,0,?,?,?,?,?,'local-operator')",UUID.randomUUID(),owner.runId(),reconcile?"RECONCILE":"CANCEL",UUID.randomUUID().toString(),raw,SourceJson.sha(raw),reconcile?successor:null,reconcile?true:null,rejected?"REJECTED":"REQUESTED",rejected?"TEST":null,rejected?java.sql.Timestamp.from(Instant.now()):null);
    }
    @Test void controlCommittingDuringTerminalTransactionCannotSneakPastFinalFence()throws Exception{
        pending();var q=qualify();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var first=new AtomicBoolean();var blocker=new AtomicInteger();
        var waiting=repository(b->{if(b.equals("AUDIT") && first.compareAndSet(false,true)){blocker.set(jdbc.queryForObject("SELECT pg_backend_pid()",Integer.class));entered.countDown();try{if(!release.await(15,TimeUnit.SECONDS))throw new AssertionError("RACE_TIMEOUT");}catch(InterruptedException ex){throw new AssertionError(ex);}}});
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()){
            var finish=pool.submit(()->waiting.finish(guard,q));assertThat(entered.await(10,TimeUnit.SECONDS)).isTrue();
            var writer=pool.submit(()->{try{insertControl(jdbc,"CANCEL");return "UNEXPECTED_COMMIT";}catch(RuntimeException expected){return "REJECTED";}});
            try{boolean blocked=false;long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while(System.nanoTime()<deadline){blocked=jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND ?=ANY(pg_blocking_pids(pid)))",Boolean.class,blocker.get());if(blocked)break;Thread.sleep(10);}assertThat(blocked).as("competing control actually waits on the finalization fence").isTrue();}finally{release.countDown();}
            assertThat(finish.get(15,TimeUnit.SECONDS)).isEqualTo("COMMITTED");assertThat(writer.get(15,TimeUnit.SECONDS)).isEqualTo("REJECTED");
        }finally{release.countDown();}
        assertThat(count("controls")).isEqualTo(1);assertThat(repo.lookup(finalization)).isEqualTo("CONFIRMED_TERMINAL");released();
    }
    @Test void staleRepeatableReadCannotInsertControlAfterTerminalRelease()throws Exception{
        pending();var q=qualify();
        try(var stale=jdbc.getDataSource().getConnection()){
            stale.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);stale.setAutoCommit(false);
            try(var read=stale.prepareStatement("SELECT state FROM ddip_management.runs WHERE run_id=?")){read.setObject(1,owner.runId());try(var rows=read.executeQuery()){assertThat(rows.next()).isTrue();assertThat(rows.getString(1)).isEqualTo("RECOVERY_REQUIRED");}}
            repo.finish(guard,q);var before=history();byte[] raw=SourceJson.canonical(SourceJson.object());
            try{assertThatThrownBy(()->{
                try(var insert=stale.prepareStatement("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','stale-terminal',?,?,0,'REQUESTED','local-operator')")){
                    insert.setQueryTimeout(5);insert.setObject(1,UUID.randomUUID());insert.setObject(2,owner.runId());insert.setBytes(3,raw);insert.setString(4,SourceJson.sha(raw));insert.executeUpdate();
                }stale.commit();
            }).isInstanceOfSatisfying(java.sql.SQLException.class,ex->assertThat(ex.getSQLState()).isIn("40001","23514"));}finally{stale.rollback();}
            assertThat(history()).isEqualTo(before);released();
        }
    }
    @ParameterizedTest @ValueSource(strings={"release","attempt","run","control"})
    void directSqlTerminalShortcutsWithoutReceiptAreRejected(String bypass){
        pending();var before=history();assertThatThrownBy(()->tx.required(()->{
            switch(bypass){case "release"->jdbc.update("UPDATE ddip_management.coordinator SET mode='READY',active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,row_version=row_version+1");
                case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET state='SUCCEEDED',outcome_code='RECONCILED_FAILED',finished_at=now(),row_version=row_version+1 WHERE attempt_id=?",successor);
                case "run"->jdbc.update("UPDATE ddip_management.runs SET state='FAILED',outcome_code='NATIVE_FETCH_INTERRUPTED',finished_at=now(),row_version=row_version+1 WHERE run_id=?",owner.runId());
                case "control"->{jdbc.update("UPDATE ddip_management.controls SET state='APPLIED',reason='NATIVE_FETCH_INTERRUPTED',applied_at=now(),row_version=row_version+1 WHERE control_id=?",control);jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,row_version=row_version+1");}}
            return null;
        })).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);occupied();
    }
    @ParameterizedTest @ValueSource(strings={"retry","queued","attempt","control","stage","receipt-update","receipt-delete","truncate","worker"})
    void finalizedHistoryCannotBeRevivedOrMutated(String bypass){
        pending();repo.finish(guard,qualify());var before=history();assertThatThrownBy(()->tx.required(()->{
            switch(bypass){case "retry"->jdbc.update("UPDATE ddip_management.runs SET retry_eligible=true,row_version=row_version+1 WHERE run_id=?",owner.runId());
                case "queued"->jdbc.update("UPDATE ddip_management.runs SET state='QUEUED',finished_at=NULL,row_version=row_version+1 WHERE run_id=?",owner.runId());
                case "attempt"->jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code) VALUES (?,?,3,'EXECUTE',?,1,'FAILED',now(),'TEST')",UUID.randomUUID(),owner.runId(),recovery.instanceId());
                case "control"->insertControl(jdbc,"REJECTED");
                case "stage"->jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?)",successor,UUID.randomUUID(),"a".repeat(64));
                case "receipt-update"->jdbc.update("UPDATE ddip_management.content_fetch_recovery_finalizations SET finalization_id=?",UUID.randomUUID());
                case "receipt-delete"->jdbc.update("DELETE FROM ddip_management.content_fetch_recovery_finalizations");
                case "truncate"->jdbc.execute("TRUNCATE ddip_management.content_fetch_recovery_finalizations CASCADE");
                case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET status='RECOVERY_ONLY',row_version=row_version+1 WHERE instance_id=?",recovery.instanceId());}
            return null;
        })).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(repo.lookup(finalization)).isEqualTo("CONFIRMED_TERMINAL");released();
    }
    @Test void freshJvmLookupNeedsNoFilesGuardOwnerOrProcessObservation()throws Exception{
        pending();repo.finish(guard,qualify());Files.move(attemptRoot(),temp.resolve("terminal-unavailable-"+UUID.randomUUID()));guard.close();guard=null;
        var before=history();var files=filesystem();
        var command=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),"platform.contentapi.execution.NarrowContentFetchTerminalLookupTestProcess",url,user,password,INSTALL.toString(),finalization.toString());
        command.redirectError(ProcessBuilder.Redirect.INHERIT);var child=command.start();try{assertThat(child.waitFor(20,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isZero();assertThat(child.inputReader().lines().toList()).contains("NATIVE_FETCH_TERMINAL_LOOKUP CONFIRMED_TERMINAL");}finally{if(child.isAlive())child.destroyForcibly();}
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);released();
    }
    @Test void ambientTransactionsCannotAuthorizeFileOrProcessIo(){
        pending();var q=qualify();var before=history();assertThatThrownBy(()->tx.required(this::target)).isInstanceOf(RuntimeException.class);assertThatThrownBy(()->tx.required(()->repo.finish(guard,q))).isInstanceOf(RuntimeException.class);assertThatThrownBy(()->tx.required(()->repo.resolve(q))).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);occupied();
    }
    @ParameterizedTest @ValueSource(ints={128,129})
    void historicalControlBoundIsPreserved(int controls){
        pending();for(int i=1;i<controls;i++)insertControl(jdbc,"REJECTED");var before=history();
        if(controls==128){assertThat(repo.finish(guard,qualify())).isEqualTo("COMMITTED");assertThat(repo.lookup(finalization)).isEqualTo("CONFIRMED_TERMINAL");released();}
        else{assertThatThrownBy(this::qualify).hasMessage("CAPACITY_EXCEEDED");assertThat(history()).isEqualTo(before);occupied();}
    }
    @ParameterizedTest @ValueSource(strings={"original","recovery"})
    void terminalOwnershipCannotBeReacquiredThroughSql(String which){
        pending();repo.finish(guard,qualify());var before=history();boolean original=which.equals("original");
        assertThatThrownBy(()->jdbc.update("UPDATE ddip_management.coordinator SET mode='RECOVERY_ONLY',active_attempt_id=?,owner_instance_id=?,epoch=?,heartbeat_at=now(),row_version=row_version+1",original?owner.attemptId():successor,original?owner.worker().instanceId():recovery.instanceId(),owner.worker().epoch()+(original?0:1))).isInstanceOf(RuntimeException.class);
        assertThat(history()).isEqualTo(before);assertThat(repo.lookup(finalization)).isEqualTo("CONFIRMED_TERMINAL");released();
    }
    @Test void unavailableDatabaseReturnsUnresolvedWithoutProcessOrFileChecks(){
        pending();repo.finish(guard,qualify());var before=history();
        var missing=new org.springframework.jdbc.datasource.AbstractDataSource(){
            @Override public java.sql.Connection getConnection()throws java.sql.SQLException{throw new java.sql.SQLException("TEST_DATABASE_UNAVAILABLE");}
            @Override public java.sql.Connection getConnection(String user,String password)throws java.sql.SQLException{throw new java.sql.SQLException("TEST_DATABASE_UNAVAILABLE");}
        };
        var unavailable=new JdbcTemplate(missing);var unavailableTx=new ManagementTransactions(new TransactionTemplate(new DataSourceTransactionManager(missing)));
        var readOnly=new NarrowContentFetchRecoveryFinalizationTestSupport(unavailable,unavailableTx,INSTALL,clock,()->{throw new AssertionError("LOOKUP_PROCESS_IO");},b->{throw new AssertionError("LOOKUP_MUTATION");});
        assertThat(readOnly.lookup(finalization)).isEqualTo("UNRESOLVED");assertThat(history()).isEqualTo(before);released();
    }
    @Test void populatedVersionFifteenUpgradePreservesPendingOwnerWithoutTerminalBackfill()throws Exception{
        versionFifteen=true;closeServer();setup();
        // Seed the historical V15 handoff semantics without weakening production's V17
        // fail-closed reader. Only these two new absence queries are emulated, and only
        // after proving that this fixture is still V15 and the successor table is absent.
        var originalJdbc=jdbc;
        jdbc=new JdbcTemplate(originalJdbc.getDataSource()){
            @Override public <T>T queryForObject(String sql,Class<T> type,Object... args){
                boolean count=sql.equals("SELECT count(*) FROM ddip_management.content_bronze_reservations WHERE attempt_id=?");
                boolean exists=sql.equals("SELECT EXISTS(SELECT 1 FROM ddip_management.content_bronze_reservations b JOIN ddip_management.attempts a USING(attempt_id) WHERE a.run_id=?)");
                if((count || exists) && versionFifteen
                        && Boolean.TRUE.equals(originalJdbc.queryForObject("SELECT to_regclass('ddip_management.content_bronze_reservations') IS NULL",Boolean.class))){
                    assertThat(originalJdbc.queryForObject("SELECT max(version::integer) FROM ddip_management.flyway_schema_history WHERE success AND version IS NOT NULL",Integer.class)).isEqualTo(15);
                    assertThat(args).hasSize(1);
                    return type.cast(count?Long.valueOf(0):Boolean.FALSE);
                }
                return super.queryForObject(sql,type,args);
            }
        };
        terminalSetup();pending();
        var before=metadata();var files=filesystem();var row=jdbc.queryForObject("SELECT to_jsonb(h)::text FROM ddip_management.content_fetch_recovery_handoffs h",String.class);
        var migrations=jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IS NOT NULL ORDER BY installed_rank");
        var migration=org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();assertThat(migration.migrate().migrationsExecuted).isEqualTo(5);
        new SchemaLifecycle(migration,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(false);
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(jdbc.queryForObject("SELECT to_jsonb(h)::text FROM ddip_management.content_fetch_recovery_handoffs h",String.class)).isEqualTo(row);
        assertThat(jdbc.queryForList("SELECT version,checksum,installed_on FROM ddip_management.flyway_schema_history WHERE version IS NOT NULL AND version NOT IN ('16','17','18','19','20') ORDER BY installed_rank")).isEqualTo(migrations);
        assertThat(count("content_fetch_recovery_finalizations")).isZero();occupied();assertThat(handoffs.lookup(handoff)).isEqualTo("CONFIRMED_HANDOFF");
        assertThat(repo.finish(guard,qualify())).isEqualTo("COMMITTED");released();
    }
    @Override void storage(String id,String product,String role){
        if(role.equals("BRONZE")){NarrowContentFetchRecoveryHandoffIT.bronzeStorage(this,id,product,role);return;}
        super.storage(id,product,role);
    }
    @Test void terminalDispositionRejectsPreviouslyValidRawBronzeReservation()throws Exception{
        checkpoint();var bronze=NarrowContentFetchRecoveryHandoffIT.pendingBronze(this);recover();transfer();repo.finish(guard,qualify());var before=history();var files=filesystem();
        assertThatThrownBy(()->tx.required(()->{NarrowContentFetchRecoveryHandoffIT.insertBronze(jdbc,bronze);return null;})).isInstanceOf(RuntimeException.class);
        NarrowContentFetchRecoveryHandoffIT.assertDeferredBronzeExclusion(jdbc,bronze);
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(count("content_bronze_reservations")).isZero();released();
    }
    @Test void rawBronzeInsertionDuringTerminalTransactionRollsBackWholeDisposition()throws Exception{
        checkpoint();var bronze=NarrowContentFetchRecoveryHandoffIT.pendingBronze(this);recover();transfer();var q=qualify();var before=history();var files=filesystem();var attempted=new AtomicBoolean();
        var failing=repository(b->{if(b.equals("RECEIPT")){attempted.set(true);NarrowContentFetchRecoveryHandoffIT.insertBronze(jdbc,bronze);}});
        assertThatThrownBy(()->failing.finish(guard,q)).isInstanceOf(RuntimeException.class);assertThat(attempted).isTrue();
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(count("content_bronze_reservations")).isZero();assertThat(count("content_fetch_recovery_finalizations")).isZero();occupied();
    }
    static final class MutableClock extends Clock {volatile Instant now;MutableClock(Instant now){this.now=now;}public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return now;}}
}
