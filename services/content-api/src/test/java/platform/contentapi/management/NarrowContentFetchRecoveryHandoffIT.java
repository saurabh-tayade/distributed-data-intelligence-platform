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

/** Metadata-only handoff: real PostgreSQL and FETCH evidence, synthetic negative writer observations. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchRecoveryHandoffIT extends NarrowContentFetchLaunchFixtures {
    UUID control,successor,handoff;
    WorkerRepository.Session recovery;
    MutableClock clock;
    AtomicReference<String> observed;
    NarrowContentFetchRecoveryHandoffTestSupport repo;
    @BeforeEach void recoverySetup(){control=null;successor=UUID.randomUUID();handoff=UUID.randomUUID();recovery=null;clock=new MutableClock(Instant.now());observed=new AtomicReference<>("ABSENT");repo=repository(b->{});}
    NarrowContentFetchRecoveryHandoffTestSupport repository(Consumer<String> boundary){return new NarrowContentFetchRecoveryHandoffTestSupport(jdbc,tx,INSTALL,clock,observed::get,boundary);}
    void checkpoint(){prepared();launch(b->{});NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});}
    void recover(){
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,owner.runId());
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);
        control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL,processes->{}).request(ControlCommands.parse(owner.runId().toString(),SourceJson.canonical(request),"native-handoff","\""+version+"\"")).get("controlId").stringValue());
        guard.close();guard=InstallationHostGuard.acquire(binding);
        recovery=new WorkerRepository(jdbc,tx,INSTALL).register(guard,CONTENT_PROFILE,owner.worker().profileDigest(),owner.worker().deploymentDigest());
        assertThat(recovery.status()).isEqualTo("RECOVERY_ONLY");clock.now=Instant.now();
    }
    NarrowContentFetchRecoveryHandoffTestSupport.Target target(){return repo.inspect(guard,recovery,control,successor,handoff);}
    NarrowContentFetchRecoveryHandoffTestSupport.Qualified qualify(){return repo.qualify(guard,target(),clock.instant(),"");}
    Map<String,List<String>> history(){var rows=new TreeMap<>(metadata());for(String table:List.of("controls","content_native_operations","content_native_launch_phases","content_fetch_completion_intents","content_fetch_checkpoints","content_fetch_recovery_handoffs","run_sources","run_inputs","artifacts"))rows.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));return rows;}
    Map<String,List<String>> preserved(){var rows=history();for(String t:List.of("coordinator","runs","attempts","audit_events","content_fetch_recovery_handoffs"))rows.remove(t);return rows;}
    void occupied(UUID attempt){assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(attempt);noExecution();}
    @Test void exactCheckpointTransfersOnlyMetadataAndCannotReleaseOrDispatch()throws Exception{
        checkpoint();recover();Files.delete(installedFetchJar);var before=preserved();var files=filesystem();long epoch=jdbc.queryForObject("SELECT epoch FROM ddip_management.coordinator",Long.class);var q=qualify();
        assertThat(repo.transfer(guard,q)).isEqualTo(new NarrowContentFetchRecoveryHandoffTestSupport.Result("COMMITTED",true));
        occupied(successor);assertThat(preserved()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);
        var c=jdbc.queryForMap("SELECT * FROM ddip_management.coordinator");assertThat(c.get("mode")).isEqualTo("RECOVERY_ONLY");assertThat(c.get("epoch")).isEqualTo(epoch+1);assertThat(c.get("owner_instance_id")).isEqualTo(recovery.instanceId());
        var a=jdbc.queryForMap("SELECT * FROM ddip_management.attempts WHERE attempt_id=?",successor);assertThat(a.get("kind")).isEqualTo("RECONCILE");assertThat(a.get("state")).isEqualTo("CLAIMED");assertThat(a.get("content_recovery_policy")).isEqualTo("ddip-content-fetch-checkpoint-recovery-v1");assertThat(a.get("reused_checkpoint_id")).isNull();
        assertThat(successor).isNotEqualTo(control).isNotEqualTo(owner.attemptId());assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo("INTERRUPTED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("RECOVERY_REQUIRED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,control)).isEqualTo("REQUESTED");
        var after=history();assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_HANDOFF");assertThat(repo.transfer(guard,q).ownerPresent()).isFalse();assertThat(repository(b->{throw new AssertionError("READ_ONLY");}).lookup(handoff)).isEqualTo("CONFIRMED_HANDOFF");assertThat(history()).isEqualTo(after);
        assertThat(NarrowContentFetchRecoveryHandoffTestSupport.publicSeams()).doesNotContain("bootstrap","authorize","release","dispatch","resume","finalizeRecovery");
        assertThatThrownBy(()->workers.claim(guard,recovery,UUID.randomUUID())).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(after);
    }
    @ParameterizedTest @ValueSource(strings={"LIVE","UNAVAILABLE","THROWS"})
    void processUncertaintyVetoesQualificationAndTransfer(String state)throws Exception{
        checkpoint();recover();var q=qualify();observed.set(state);var before=history();var files=filesystem();
        assertThatThrownBy(this::qualify).isInstanceOf(RuntimeException.class);assertThatThrownBy(()->repo.transfer(guard,q)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);occupied(owner.attemptId());
    }
    @ParameterizedTest @ValueSource(strings={"handlers","unmanaged","orphans","descendants","gaps","checkpoint","digest","stale","future"})
    void everyExplicitReviewAndFreshnessBindingIsRequired(String missing)throws Exception{
        checkpoint();recover();var t=target();var before=history();var files=filesystem();Instant at=clock.instant().plusSeconds(missing.equals("stale")?-61:missing.equals("future")?1:0);
        assertThatThrownBy(()->repo.qualify(guard,t,at,missing)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);occupied(owner.attemptId());
    }
    @ParameterizedTest @ValueSource(strings={"BEFORE_TRANSFER","OLD_ATTEMPT","SUCCESSOR","COORDINATOR","RUN","HANDOFF","AUDIT"})
    void eachMutationBoundaryRollsBackAndConsumesQualification(String boundary)throws Exception{
        checkpoint();recover();var q=qualify();var before=history();var files=filesystem();var hit=new AtomicBoolean();var failing=repository(b->{if(b.equals(boundary)){hit.set(true);throw new IllegalStateException("TEST_FAULT");}});
        assertThatThrownBy(()->failing.transfer(guard,q)).isInstanceOf(RuntimeException.class);assertThat(hit).isTrue();assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_ROLLBACK");
        assertThatThrownBy(()->repo.transfer(guard,q)).hasMessage("FRESH_QUALIFICATION_REQUIRED");assertThat(history()).isEqualTo(before);occupied(owner.attemptId());
    }
    @Test void postCommitFaultAndFreshLookupNeverRemintAuthority(){
        checkpoint();recover();var q=qualify();var failing=repository(b->{if(b.equals("COMMITTED"))throw new IllegalStateException("TEST_LOST_RESPONSE");});
        assertThatThrownBy(()->failing.transfer(guard,q)).isInstanceOf(RuntimeException.class);var after=history();assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_HANDOFF");assertThat(repository(b->{}).lookup(handoff)).isEqualTo("CONFIRMED_HANDOFF");assertThat(repo.transfer(guard,q).ownerPresent()).isFalse();assertThat(history()).isEqualTo(after);occupied(successor);
    }
    @Test void actualCommitAcknowledgmentLossResolvesReadOnlyWithoutRetryingMutations(){
        checkpoint();recover();var q=qualify();var armed=new AtomicBoolean(false);JdbcTemplate lost=loseCommitAcknowledgment(armed);var lostTx=new ManagementTransactions(new TransactionTemplate(new DataSourceTransactionManager(lost.getDataSource())));
        var failing=new NarrowContentFetchRecoveryHandoffTestSupport(lost,lostTx,INSTALL,clock,observed::get,b->{if(b.equals("AUDIT"))armed.set(true);});
        assertThatThrownBy(()->failing.transfer(guard,q)).isInstanceOf(RuntimeException.class);assertThat(armed).isFalse();var after=history();assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_HANDOFF");assertThat(repository(b->{}).lookup(handoff)).isEqualTo("CONFIRMED_HANDOFF");assertThat(repo.transfer(guard,q).ownerPresent()).isFalse();assertThat(history()).isEqualTo(after);assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);assertThat(count("attempts")).isEqualTo(2);occupied(successor);
    }
    @ParameterizedTest @ValueSource(strings={"missing","payload","marker","lock","ledger"})
    void changedFilesAfterQualificationNeverTransferOrRepair(String change)throws Exception{
        checkpoint();recover();var q=qualify();switch(change){case "missing"->Files.move(attemptRoot(),temp.resolve("detached"));case "payload"->Files.writeString(fetchDirectory().resolve("payload.xml"),"tampered");case "marker"->Files.delete(fetchDirectory().resolve("complete.json"));case "lock"->{Path lock=attemptRoot().resolve(NarrowContentAdmissionTestSupport.LOCK);Files.move(lock,temp.resolve("old-lock"));Files.createFile(lock);}case "ledger"->Files.writeString(attemptRoot().resolve(NarrowContentAdmissionTestSupport.MANIFEST),"{}");}
        var before=history();var files=filesystem();assertThatThrownBy(()->repo.transfer(guard,q)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);occupied(owner.attemptId());assertThat(requests).hasValue(1);
    }
    @ParameterizedTest @ValueSource(strings={"stale","guard","run-version","coordinator-version","worker-version"})
    void changedFenceOrExpiredQualificationLeavesSlotOccupied(String change){
        checkpoint();recover();var q=qualify();switch(change){case "stale"->clock.now=clock.now.plusSeconds(61);case "guard"->guard.close();case "run-version"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());case "coordinator-version"->jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");case "worker-version"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",recovery.instanceId());}
        var before=history();assertThatThrownBy(()->repo.transfer(guard,q)).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);occupied(owner.attemptId());
    }
    @ParameterizedTest @ValueSource(strings={"CANDIDATE_EXITED","INTENT_COMMITTED","PREFIX_WRITTEN","MARKER_WRITTEN"})
    void incompleteCandidateOrMarkerCannotBecomeCheckpointRecovery(String prefix)throws Exception{
        prepared();launch(b->{});if(!prefix.equals("CANDIDATE_EXITED"))assertThatThrownBy(()->NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{if(b.equals(prefix))throw new IllegalStateException("TEST_PREFIX");})).isInstanceOf(RuntimeException.class);
        recover();var before=history();var files=filesystem();assertThatThrownBy(this::qualify).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);occupied(owner.attemptId());assertThat(count("content_fetch_checkpoints")).isZero();
    }
    @Test void parallelQualificationsHaveExactlyOneMetadataSuccessor()throws Exception{
        checkpoint();recover();var first=qualify();UUID secondId=UUID.randomUUID(),secondHandoff=UUID.randomUUID();var t=repo.inspect(guard,recovery,control,secondId,secondHandoff);var second=repo.qualify(guard,t,clock.instant(),"");var start=new CountDownLatch(1);
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()){
            var tasks=List.of(first,second).stream().map(q->pool.submit(()->{start.await();try{return repo.transfer(guard,q).ownerPresent();}catch(RuntimeException expected){return false;}})).toList();start.countDown();int owners=0;for(var task:tasks)if(task.get(20,TimeUnit.SECONDS))owners++;assertThat(owners).isEqualTo(1);
        }
        assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);assertThat(count("attempts")).isEqualTo(2);assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isIn(successor,secondId);noExecution();
    }
    @Test void committedHandoffRowIsImmutableAndUnknownLookupIsUnresolved(){
        checkpoint();recover();repo.transfer(guard,qualify());var before=history();assertThatThrownBy(()->jdbc.update("UPDATE ddip_management.content_fetch_recovery_handoffs SET handoff_id=? WHERE handoff_id=?",UUID.randomUUID(),handoff)).isInstanceOf(RuntimeException.class);assertThatThrownBy(()->jdbc.update("DELETE FROM ddip_management.content_fetch_recovery_handoffs WHERE handoff_id=?",handoff)).isInstanceOf(RuntimeException.class);assertThat(repo.lookup(UUID.randomUUID())).isEqualTo("UNRESOLVED");assertThat(history()).isEqualTo(before);occupied(successor);
    }
    @Test void freshJvmLookupHasNoGuardOrQualifiedHandleAndDoesNotTouchMissingEvidence()throws Exception{
        checkpoint();recover();repo.transfer(guard,qualify());
        Files.move(attemptRoot(),temp.resolve("unavailable-after-handoff"));guard.close();guard=null;
        var before=history();var files=filesystem();
        var command=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",
                System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                "platform.contentapi.execution.NarrowContentFetchRecoveryLookupTestProcess",url,user,password,INSTALL.toString(),handoff.toString());
        command.redirectError(ProcessBuilder.Redirect.INHERIT);var child=command.start();
        try{
            assertThat(child.waitFor(20,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isZero();
            assertThat(child.inputReader().lines().toList()).contains("NATIVE_FETCH_LOOKUP CONFIRMED_HANDOFF");
        }finally{if(child.isAlive())child.destroyForcibly();}
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);occupied(successor);assertThat(requests).hasValue(1);
    }
    @Test void ambientTransactionsAreRejectedBeforeEvidenceIo(){
        checkpoint();recover();var q=qualify();var before=history();assertThatThrownBy(()->tx.required(this::target)).isInstanceOf(RuntimeException.class);assertThatThrownBy(()->tx.required(()->repo.transfer(guard,q))).isInstanceOf(RuntimeException.class);assertThatThrownBy(()->tx.required(()->repo.resolve(q))).isInstanceOf(RuntimeException.class);assertThat(history()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"release","success","running","clear-policy","legacy-stage","truncate"})
    void directSqlCannotTurnHandoffIntoExecutionOrRelease(String bypass){
        checkpoint();recover();repo.transfer(guard,qualify());var before=history();
        assertThatThrownBy(()->tx.required(()->{
            switch(bypass){
                case "release"->jdbc.update("UPDATE ddip_management.coordinator SET active_attempt_id=NULL,owner_instance_id=NULL,heartbeat_at=NULL,row_version=row_version+1");
                case "success"->jdbc.update("UPDATE ddip_management.runs SET state='SUCCEEDED',finished_at=now(),row_version=row_version+1 WHERE run_id=?",owner.runId());
                case "running"->jdbc.update("UPDATE ddip_management.attempts SET state='RUNNING',row_version=row_version+1 WHERE attempt_id=?",successor);
                case "clear-policy"->jdbc.update("UPDATE ddip_management.attempts SET content_recovery_policy=NULL,row_version=row_version+1 WHERE attempt_id=?",successor);
                case "legacy-stage"->jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?)",successor,UUID.randomUUID(),"a".repeat(64));
                case "truncate"->jdbc.execute("TRUNCATE ddip_management.content_fetch_recovery_handoffs CASCADE");
            }
            return null;
        })).isInstanceOf(RuntimeException.class);
        assertThat(history()).isEqualTo(before);occupied(successor);
    }
    @Test void qualificationExpiryDuringProcessIoFailsBeforeTransfer(){
        checkpoint();recover();var q=qualify();var before=history();var checked=new AtomicBoolean();
        var expiring=new NarrowContentFetchRecoveryHandoffTestSupport(jdbc,tx,INSTALL,clock,()->{
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            if(checked.compareAndSet(false,true))clock.now=clock.now.plusSeconds(61);
            return "ABSENT";
        },b->{});
        assertThatThrownBy(()->expiring.transfer(guard,q)).isInstanceOf(RuntimeException.class);assertThat(checked).isTrue();assertThat(history()).isEqualTo(before);occupied(owner.attemptId());
    }
    @ParameterizedTest @ValueSource(strings={"BEFORE_TRANSFER","AUDIT"})
    void guardLossAtSqlBoundaryCannotCommitHandoff(String boundary){
        checkpoint();recover();var q=qualify();var before=history();var hit=new AtomicBoolean();
        var closing=repository(b->{if(b.equals(boundary)){hit.set(true);guard.close();}});
        assertThatThrownBy(()->closing.transfer(guard,q)).isInstanceOf(RuntimeException.class);assertThat(hit).isTrue();assertThat(history()).isEqualTo(before);occupied(owner.attemptId());
    }
    @Test void metadataDriftDoesNotRefreshHistoricalReceipt(){
        checkpoint();recover();var q=qualify();repo.transfer(guard,q);
        jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");var before=history();
        assertThat(repo.resolve(q)).isEqualTo("UNRESOLVED");assertThat(repo.lookup(handoff)).isEqualTo("UNRESOLVED");assertThatThrownBy(()->repo.transfer(guard,q)).hasMessage("HANDOFF_UNRESOLVED");assertThat(history()).isEqualTo(before);occupied(successor);
    }
    @Test void foreignInstallationCannotConfirmReceipt(){
        checkpoint();recover();repo.transfer(guard,qualify());var before=history();
        var foreign=new NarrowContentFetchRecoveryHandoffTestSupport(jdbc,tx,UUID.randomUUID(),clock,observed::get,b->{});
        assertThat(foreign.lookup(handoff)).isEqualTo("UNRESOLVED");assertThat(history()).isEqualTo(before);occupied(successor);
    }
    @ParameterizedTest @ValueSource(strings={"LIVE","UNAVAILABLE"})
    void explicitlyReviewedDescendantStillVetoesQualificationAndTransfer(String state){
        checkpoint();recover();var t=target();
        var descendant=new NarrowContentFetchRecoveryHandoffTestSupport.ProcessIdentity(ProcessHandle.current().pid(),Instant.EPOCH);
        var descendantState=new AtomicReference<>("ABSENT");var observedDescendant=new AtomicInteger();
        var specific=new NarrowContentFetchRecoveryHandoffTestSupport(jdbc,tx,INSTALL,clock,identity->{
            assertThat(TransactionSynchronizationManager.isActualTransactionActive()).isFalse();
            if(identity.equals(descendant)){observedDescendant.incrementAndGet();return descendantState.get();}
            return "ABSENT";
        },b->{});
        var q=specific.qualify(guard,t,clock.instant(),"",List.of(descendant));assertThat(observedDescendant.get()).isGreaterThan(0);
        descendantState.set(state);observedDescendant.set(0);var before=history();
        assertThatThrownBy(()->specific.qualify(guard,t,clock.instant(),"",List.of(descendant))).hasMessage("PROCESS_UNRESOLVED");
        assertThatThrownBy(()->specific.transfer(guard,q)).hasMessage("PROCESS_UNRESOLVED");
        assertThat(observedDescendant.get()).isGreaterThanOrEqualTo(2);assertThat(history()).isEqualTo(before);occupied(owner.attemptId());
    }
    @Test void reviewExpiresWhileWaitingForCoordinatorLockWithoutMutation()throws Exception{
        checkpoint();recover();var q=qualify();var before=history();var atBoundary=new CountDownLatch(1);var lockHeld=new CountDownLatch(1);
        var waiting=repository(b->{if(b.equals("BEFORE_TRANSFER")){atBoundary.countDown();try{if(!lockHeld.await(10,TimeUnit.SECONDS))throw new AssertionError("LOCK_NOT_HELD");}catch(InterruptedException ex){Thread.currentThread().interrupt();throw new AssertionError(ex);}}});
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()){
            var result=pool.submit(()->{try{waiting.transfer(guard,q);return "UNEXPECTED_COMMIT";}catch(RuntimeException ex){return ex.getMessage();}});
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
                    assertThat(blocked).as("transfer must actually wait on the coordinator lock").isTrue();clock.now=clock.now.plusSeconds(61);
                }finally{lockHeld.countDown();connection.rollback();}
            }finally{lockHeld.countDown();}
            assertThat(result.get(15,TimeUnit.SECONDS)).isEqualTo("FRESH_QUALIFICATION_REQUIRED");
        }
        assertThat(history()).isEqualTo(before);assertThat(repo.resolve(q)).isEqualTo("CONFIRMED_ROLLBACK");occupied(owner.attemptId());
    }
    record Previous(UUID run,UUID attempt) {}
    Previous queuedWithUnmarkedTerminalAttempt(){
        UUID run=queue("prior-terminal"),prior=UUID.randomUUID();ready();
        jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at,outcome_code) VALUES (?,?,1,'EXECUTE',?,1,'FAILED',clock_timestamp(),'INPUT_INVALID')",prior,run,session.instanceId());
        return new Previous(run,prior);
    }
    void checkpointAfter(Previous prior){
        owner=NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),prior.run()).owner();assertThat(owner).isNotNull();
        prepare();launch(b->{});NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});
    }
    @Test void terminalUnmarkedAttemptRemainsUnchangedAcrossHandoff(){
        var prior=queuedWithUnmarkedTerminalAttempt();var row=jdbc.queryForObject("SELECT to_jsonb(a)::text FROM ddip_management.attempts a WHERE attempt_id=?",String.class,prior.attempt());
        checkpointAfter(prior);recover();repo.transfer(guard,qualify());
        assertThat(jdbc.queryForObject("SELECT to_jsonb(a)::text FROM ddip_management.attempts a WHERE attempt_id=?",String.class,prior.attempt())).isEqualTo(row);
        assertThat(jdbc.queryForObject("SELECT attempt_no FROM ddip_management.attempts WHERE attempt_id=?",Integer.class,successor)).isEqualTo(3);
        assertThat(repo.lookup(handoff)).isEqualTo("CONFIRMED_HANDOFF");occupied(successor);
    }
    @Test void staleRepeatableReadCannotAddLegacyStageAfterRunEnrollmentAndHandoff()throws Exception{
        var prior=queuedWithUnmarkedTerminalAttempt();
        try(var stale=jdbc.getDataSource().getConnection()){
            stale.setTransactionIsolation(java.sql.Connection.TRANSACTION_REPEATABLE_READ);stale.setAutoCommit(false);
            try(var read=stale.prepareStatement("SELECT state FROM ddip_management.runs WHERE run_id=?")){
                read.setObject(1,prior.run());try(var rows=read.executeQuery()){assertThat(rows.next()).isTrue();assertThat(rows.getString(1)).isEqualTo("QUEUED");}
            }
            checkpointAfter(prior);recover();repo.transfer(guard,qualify());var before=history();
            try{
                assertThatThrownBy(()->{
                    try(var insert=stale.prepareStatement("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?)")){
                        insert.setQueryTimeout(5);insert.setObject(1,prior.attempt());insert.setObject(2,UUID.randomUUID());insert.setString(3,"a".repeat(64));insert.executeUpdate();
                    }
                    stale.commit();
                }).isInstanceOfSatisfying(java.sql.SQLException.class,ex->assertThat(ex.getSQLState()).isIn("40001","23514"));
            }finally{stale.rollback();}
            assertThat(history()).isEqualTo(before);occupied(successor);assertThat(repo.lookup(handoff)).isEqualTo("CONFIRMED_HANDOFF");
        }
    }
    @Test void unmarkedLegacyStageDoesNotAcquireGlobalCoordinatorLock()throws Exception{
        var prior=queuedWithUnmarkedTerminalAttempt();
        try(var pool=Executors.newVirtualThreadPerTaskExecutor()){
            try(var holder=jdbc.getDataSource().getConnection()){
                holder.setAutoCommit(false);
                try(var lock=holder.createStatement()){
                    lock.setQueryTimeout(5);try(var rows=lock.executeQuery("SELECT singleton_id FROM ddip_management.coordinator FOR UPDATE")){assertThat(rows.next()).isTrue();}
                    var insert=pool.submit(()->{
                        try(var writer=jdbc.getDataSource().getConnection();var statement=writer.prepareStatement("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?)")){
                            writer.setTransactionIsolation(java.sql.Connection.TRANSACTION_READ_COMMITTED);writer.setAutoCommit(true);statement.setQueryTimeout(5);
                            statement.setObject(1,prior.attempt());statement.setObject(2,UUID.randomUUID());statement.setString(3,"a".repeat(64));return statement.executeUpdate();
                        }
                    });
                    assertThat(insert.get(8,TimeUnit.SECONDS)).isEqualTo(1);
                }finally{holder.rollback();}
            }
        }
        assertThat(count("stage_invocations")).isEqualTo(1);assertThat(count("content_fetch_recovery_handoffs")).isZero();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
    }
    static final class MutableClock extends Clock {volatile Instant now;MutableClock(Instant now){this.now=now;}public ZoneId getZone(){return ZoneOffset.UTC;}public Clock withZone(ZoneId zone){return this;}public Instant instant(){return now;}}
}
