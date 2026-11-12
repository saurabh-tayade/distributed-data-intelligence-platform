package platform.contentapi.management;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.nio.file.attribute.PosixFilePermissions;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.flywaydb.core.Flyway;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.AbstractDataSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;
import static platform.contentapi.execution.NarrowContentAdmissionTestSupport.*;
import static platform.contentapi.management.SourceFixtures.*;

/** Real PostgreSQL admission/receipt protocol only. No native runtime, subprocess job or publication. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentAdmissionIT extends NarrowContentAdmissionFixtures {
    @Test void claimAtomicallyEnrollsFrozenIdentitiesWithoutTouchingAttemptFilesystem() throws Exception {
        UUID run=queue("claim");ready();var before=filesystem();
        owner=claim(workers,guard,session,UUID.randomUUID(),run).owner();
        assertThat(owner.attemptState()).isEqualTo("CLAIMED");
        assertThat(jdbc.queryForObject("SELECT content_admission_policy FROM ddip_management.attempts",String.class)).isEqualTo(POLICY);
        var row=jdbc.queryForMap("SELECT admission_bytes,admission_sha256 FROM ddip_management.content_attempt_admissions");
        byte[] bytes=(byte[])row.get("admission_bytes");var admission=SourceJson.parse(bytes);
        assertThat(SourceJson.canonical(admission)).isEqualTo(bytes);assertThat(row.get("admission_sha256")).isEqualTo(SourceJson.sha(bytes));
        assertThat(admission.get("attemptId").stringValue()).isEqualTo(owner.attemptId().toString());
        assertThat(admission.get("runId").stringValue()).isEqualTo(run.toString());
        assertThat(admission.get("workerInstanceId").stringValue()).isEqualTo(session.instanceId().toString());
        assertThat(admission.get("selectionSha256").stringValue()).isEqualTo(jdbc.queryForObject("SELECT selection_sha256 FROM ddip_management.runs",String.class));
        assertThat(admission.get("profileSha256").stringValue()).isEqualTo(digest(CONTENT_PROFILE));
        assertThat(admission.get("runtimeSha256").stringValue()).isEqualTo(digest(RUNTIME));
        assertThat(admission.get("runtimeId").stringValue()).isEqualTo(RUNTIME);
        assertThat(admission.get("controlBindingSha256").stringValue()).isEqualTo(binding.digest());
        assertThat(admission.get("controlBinding")).isEqualTo(SourceJson.parse(binding.descriptor()));
        assertThat(phases()).isEmpty();assertThat(filesystem()).isEqualTo(before);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"attempts","runs","coordinator","audit_events","content_attempt_admissions"})
    void everyClaimMutationRollsBackEnrollmentAndOwnership(String table) throws Exception {
        UUID run=queue("rollback");ready();var db=metadata();var fs=filesystem();
        jdbc.execute("CREATE FUNCTION ddip_management.narrow_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");
        String operation=Set.of("runs","coordinator").contains(table)?"UPDATE":"INSERT";
        jdbc.execute("CREATE TRIGGER narrow_fault BEFORE "+operation+" ON ddip_management."+table+" FOR EACH ROW EXECUTE FUNCTION ddip_management.narrow_fault()");
        assertThatThrownBy(()->claim(workers,guard,session,UUID.randomUUID(),run)).hasMessage("STATE_CONFLICT").hasNoCause();
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void markedAttemptCannotCommitWithoutAtomicEnrollment() throws Exception {
        UUID run=queue("orphan");ready();var db=metadata();var fs=filesystem();
        assertThatThrownBy(()->tx.required(()->jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,content_admission_policy) VALUES (?,?,1,'EXECUTE',?,1,'CLAIMED',?)",UUID.randomUUID(),run,session.instanceId(),POLICY)))
                .hasMessage("STATE_CONFLICT");
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);
    }
    @Test void badAdmissionDigestRollsBackTheWholeClaim() throws Exception {
        UUID run=queue("bad-hash");ready();var before=metadata();var fs=filesystem();
        jdbc.execute("CREATE FUNCTION ddip_management.narrow_bad_hash() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN NEW.admission_sha256=repeat('f',64); RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER zz_narrow_bad_hash BEFORE INSERT ON ddip_management.content_attempt_admissions FOR EACH ROW EXECUTE FUNCTION ddip_management.narrow_bad_hash()");
        assertThatThrownBy(()->claim(workers,guard,session,UUID.randomUUID(),run)).hasMessage("STATE_CONFLICT");
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(fs);
    }
    @Test void fixedPreparationCommitsPreparingBeforeAnyFilesystemThenReadyWithoutDispatch() throws Exception {
        claimed();var boundaries=new ArrayList<String>();var untouched=filesystem();
        var result=NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,owner,boundary->{
            boundaries.add(boundary);
            if(boundary.equals("PREPARING_COMMITTED")) {
                assertThat(phases()).containsExactly("PREPARING");assertThat(attemptRoot()).doesNotExist();
                try {assertThat(filesystem()).isEqualTo(untouched);}catch(Exception ex){throw new AssertionError(ex);}
            }
            if(boundary.equals("FILES_PREPARED") || boundary.equals("BEFORE_READY")) {
                assertThat(phases()).containsExactly("PREPARING");assertThat(attemptRoot().resolve(MANIFEST)).isRegularFile();
            }
            noExecution();
        });
        assertThat(boundaries).containsExactly("PREPARING_COMMITTED","FILES_PREPARED","BEFORE_READY","READY_COMMITTED");
        assertThat(phases()).containsExactly("PREPARING","READY");
        assertThat(result.policy()).isEqualTo(POLICY);assertThat(result.plan()).isEqualTo("ORIGINAL");assertThat(result.source()).isEqualTo("demo-feed");
        assertThat(result.selectionSha256()).isEqualTo(jdbc.queryForObject("SELECT selection_sha256 FROM ddip_management.runs",String.class));
        assertThat(result.profileSha256()).isEqualTo(digest(CONTENT_PROFILE));assertThat(result.runtimeSha256()).isEqualTo(digest(RUNTIME));
        assertThat(result.ledgerSha256()).isEqualTo(SourceJson.sha(Files.readAllBytes(attemptRoot().resolve(MANIFEST))));
        assertThat(workers.lookup(owner.attemptId()).orElseThrow().state()).isEqualTo("CLAIMED");
        assertThatThrownBy(()->workers.markRunning(guard,owner)).isInstanceOf(RuntimeException.class);
        noExecution();
    }
    @Test void runningAndStageIntentCannotBypassPreparingOrReady() throws Exception {
        claimed();var before=metadata();var fs=filesystem();
        assertThatThrownBy(()->workers.markRunning(guard,owner)).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(()->jdbc.update("UPDATE ddip_management.attempts SET state='RUNNING',row_version=row_version+1 WHERE attempt_id=?",owner.attemptId())).hasMessageContaining("CONTENT_ADMISSION_NOT_READY");
        assertThatThrownBy(()->jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?)",owner.attemptId(),UUID.randomUUID(),"a".repeat(64))).hasMessageContaining("CONTENT_ADMISSION_NOT_READY");
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void invisibleUncommittedMarkedAttemptCannotSlipPastTheStageReadinessGuard() throws Exception {
        UUID run=queue("invisible-attempt");ready();UUID attempt=UUID.randomUUID();var db=metadata();var fs=filesystem();
        try(var transaction=jdbc.getDataSource().getConnection()) {
            transaction.setAutoCommit(false);
            try {
                try(var insert=transaction.prepareStatement("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,content_admission_policy) VALUES (?,?,1,'EXECUTE',?,1,'CLAIMED',?)")) {
                    insert.setObject(1,attempt);insert.setObject(2,run);insert.setObject(3,session.instanceId());insert.setString(4,POLICY);insert.executeUpdate();
                }
                // This transaction holds coordinator/attempt locks and has not committed its marker.
                // A second connection must reject immediately rather than classify it as legacy,
                // then wait at its FK and insert a stage after an unready claim becomes visible.
                try(var pool=Executors.newSingleThreadExecutor()) {
                    var stage=pool.submit(()->{
                        var other=new JdbcTemplate(jdbc.getDataSource());other.setQueryTimeout(3);
                        try {
                            other.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?)",attempt,UUID.randomUUID(),"a".repeat(64));
                            return (RuntimeException)null;
                        } catch(RuntimeException ex) {return ex;}
                    });
                    var rejected=stage.get(5,TimeUnit.SECONDS);assertThat(rejected).hasMessageContaining("CONTENT_ADMISSION_ATTEMPT_REQUIRED");
                    Throwable cause=rejected;while(cause.getCause()!=null)cause=cause.getCause();
                    assertThat(cause).isInstanceOf(SQLException.class);assertThat(((SQLException)cause).getSQLState()).isEqualTo("23503");
                }
            } finally {transaction.rollback();}
        }
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void phaseSkipAndIncorrectReceiptDigestAreRejectedWithoutEffects() {
        claimed();var before=metadata();
        assertThatThrownBy(()->milestone("READY",placeholderReady(),null)).hasMessageContaining("CONTENT_ADMISSION_PHASE_INVALID");
        assertThatThrownBy(()->milestone("PREPARING",preparingReceipt(),"f".repeat(64))).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
        assertThatThrownBy(()->milestone("PREPARING",preparingReceipt().put("admissionSha256","b".repeat(64)),null)).hasMessageContaining("CONTENT_ADMISSION_PHASE_INVALID");
        assertThat(metadata()).isEqualTo(before);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"PREPARING","READY"})
    void failedMilestoneCommitLeavesOnlyPreviouslyAcknowledgedEvidence(String phase) throws Exception {
        claimed();
        jdbc.execute("CREATE FUNCTION ddip_management.narrow_phase_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.phase='"+phase+"' THEN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END IF; RETURN NEW; END $$");
        jdbc.execute("CREATE TRIGGER narrow_phase_fault BEFORE INSERT ON ddip_management.content_admission_milestones FOR EACH ROW EXECUTE FUNCTION ddip_management.narrow_phase_fault()");
        assertThatThrownBy(this::prepare).hasMessage("STATE_CONFLICT").hasNoCause();
        if(phase.equals("PREPARING")) {assertThat(phases()).isEmpty();assertThat(attemptRoot()).doesNotExist();}
        else {assertThat(phases()).containsExactly("PREPARING");assertThat(attemptRoot().resolve(MANIFEST)).isRegularFile();}
        var db=metadata();var fs=filesystem();
        assertThatThrownBy(this::inspect).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"PREPARING_COMMITTED","FILES_PREPARED","BEFORE_READY"})
    void interruptedPreparingIsNeitherRetriedNorAdoptedOrCleaned(String boundary) throws Exception {
        claimed();stopAt(boundary);assertThat(phases()).containsExactly("PREPARING");var db=metadata();var fs=filesystem();
        assertThatThrownBy(this::prepare).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(this::inspect).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(()->workers.failBeforeLaunch(guard,owner,WorkerRepository.PrelaunchFailure.PROCESS_START_FAILED)).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void repeatedReadyInspectionPreservesEverySqlRowAndFilesystemEntry() throws Exception {
        claimed();var result=prepare();var db=metadata();var fs=filesystem();
        assertThat(inspect()).isEqualTo(result);assertThat(inspect()).isEqualTo(result);
        assertThatThrownBy(this::prepare).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"content_attempt_admissions","content_admission_milestones"})
    void durableAdmissionAndMilestonesRejectUpdateDeleteAndTruncate(String table) {
        claimed();prepare();var before=metadata();
        String column=table.equals("content_attempt_admissions")?"admission_sha256":"receipt_sha256";
        assertThatThrownBy(()->jdbc.update("UPDATE ddip_management."+table+" SET "+column+"="+column)).hasMessageContaining("IMMUTABLE_EVIDENCE");
        assertThatThrownBy(()->jdbc.update("DELETE FROM ddip_management."+table)).hasMessageContaining("IMMUTABLE_EVIDENCE");
        assertThatThrownBy(()->jdbc.execute("TRUNCATE ddip_management."+table+" CASCADE")).isInstanceOf(org.springframework.dao.DataAccessException.class);
        assertThatThrownBy(()->jdbc.update("UPDATE ddip_management.attempts SET content_admission_policy=NULL,row_version=row_version+1 WHERE attempt_id=?",owner.attemptId())).hasMessageContaining("IMMUTABLE_IDENTITY");
        assertThat(metadata()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"manifest","lock","both","root","alternate-source"})
    void readyCannotRepairMissingOrCoherentlySubstitutedFilesystemEvidence(String change) throws Exception {
        claimed();prepare();Path root=attemptRoot();
        switch(change) {
            case "manifest" -> Files.delete(root.resolve(MANIFEST));
            case "lock" -> Files.delete(root.resolve(LOCK));
            case "both" -> {Files.delete(root.resolve(MANIFEST));Files.delete(root.resolve(LOCK));}
            case "root" -> {try(var tree=Files.walk(root)){for(Path path:tree.sorted(Comparator.reverseOrder()).toList())Files.delete(path);}}
            case "alternate-source" -> alternateOriginal(root,owner.runId(),owner.attemptId(),"alternate-feed");
            default -> throw new AssertionError(change);
        }
        var db=metadata();var fs=filesystem();assertThatThrownBy(this::inspect).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void replacementLockWithIdenticalBytesCannotReplaceDurableIdentity() throws Exception {
        claimed();prepare();Path lock=attemptRoot().resolve(LOCK);Path retained=Files.move(lock,temp.resolve("retained-lock-"+UUID.randomUUID()));
        Files.copy(retained,lock,StandardCopyOption.COPY_ATTRIBUTES);var before=filesystem();var db=metadata();
        assertThatThrownBy(this::inspect).isInstanceOf(RuntimeException.class);
        assertThat(filesystem()).isEqualTo(before);assertThat(metadata()).isEqualTo(db);
    }
    @Test void coherentButDifferentReadyDigestIsNotAReaderExpectation() throws Exception {
        claimed();stopAt("FILES_PREPARED");
        var root=InstallationHostGuard.identity(attemptRoot());var lock=InstallationHostGuard.identity(attemptRoot().resolve(LOCK));
        var receipt=preparingReceipt().put("rootDevice",Long.toString(root.device())).put("rootInode",Long.toString(root.inode()))
                .put("lockDevice",Long.toString(lock.device())).put("lockInode",Long.toString(lock.inode())).put("ledgerSha256","f".repeat(64));
        milestone("READY",receipt,null);var db=metadata();var fs=filesystem();
        assertThatThrownBy(this::inspect).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);
    }
    @Test void staleOwnerCannotBeginPreparation() throws Exception {
        claimed();workers.heartbeat(guard,owner);var before=metadata();var fs=filesystem();
        assertThatThrownBy(this::prepare).hasMessage("OWNERSHIP_LOST");
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(fs);assertThat(phases()).isEmpty();
    }
    @Test void committedCancellationPreventsPreparingWithoutFilesystemEffects() throws Exception {
        claimed();cancel();var before=metadata();var fs=filesystem();
        assertThatThrownBy(this::prepare).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(fs);assertThat(phases()).isEmpty();
    }
    @ParameterizedTest @ValueSource(strings={"cancel","heartbeat","stop"})
    void changedAuthorityBeforeReadyRetainsPreparingWithoutReadiness(String change) throws Exception {
        claimed();
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.prepare(jdbc,tx,workers,guard,owner,boundary->{
            if(boundary.equals("BEFORE_READY"))switch(change) {
                case "cancel" -> cancel();
                case "heartbeat" -> workers.heartbeat(guard,owner);
                case "stop" -> jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
                default -> throw new AssertionError(change);
            }
        })).isInstanceOf(RuntimeException.class);
        assertThat(phases()).containsExactly("PREPARING");assertThat(attemptRoot().resolve(MANIFEST)).isRegularFile();
        var db=metadata();var fs=filesystem();assertThatThrownBy(this::inspect).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void twoConcurrentPreparersHaveExactlyOneFilesystemWriter() throws Exception {
        claimed();var barrier=new CyclicBarrier(2);var writers=new AtomicInteger();
        try(var pool=Executors.newFixedThreadPool(2)) {
            var futures=new ArrayList<Future<Boolean>>();
            for(int i=0;i<2;i++)futures.add(pool.submit(()->{
                barrier.await(5,TimeUnit.SECONDS);
                try {
                    NarrowContentAdmissionTestSupport.prepare(jdbc,tx,WorkerTestSupport.enabled(jdbc,tx,INSTALL),guard,owner,b->{if(b.equals("PREPARING_COMMITTED"))writers.incrementAndGet();});
                    return true;
                } catch(RuntimeException ex) {return false;}
            }));
            assertThat(List.of(futures.get(0).get(15,TimeUnit.SECONDS),futures.get(1).get(15,TimeUnit.SECONDS))).containsExactlyInAnyOrder(true,false);
        }
        assertThat(writers).hasValue(1);assertThat(phases()).containsExactly("PREPARING","READY");assertThat(count("content_attempt_admissions")).isEqualTo(1);inspect();noExecution();
    }
    @Test void lostReadyAcknowledgmentPermitsFactsOnlyLookupAndNeverRefreshesOwner() throws Exception {
        claimed();var armed=new AtomicBoolean();var wrapped=loseCommitAcknowledgment(armed);var wrappedTx=transactions(wrapped);
        var uncertain=WorkerTestSupport.enabled(wrapped,wrappedTx,INSTALL);
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.prepare(wrapped,wrappedTx,uncertain,guard,owner,b->{if(b.equals("BEFORE_READY"))armed.set(true);}))
                .hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();
        assertThat(phases()).containsExactly("PREPARING","READY");var db=metadata();var fs=filesystem();
        assertThat(inspect().attempt()).isEqualTo(owner.attemptId());
        assertThatThrownBy(()->uncertain.heartbeat(guard,owner)).hasMessage("OWNERSHIP_LOST");
        assertThatThrownBy(()->uncertain.markRunning(guard,owner)).hasMessage("OWNERSHIP_LOST");
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.prepare(wrapped,wrappedTx,uncertain,guard,owner)).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void lostPreparingAcknowledgmentNeverTouchesFilesystemOrRetriesTheIntent() throws Exception {
        claimed();var armed=new AtomicBoolean(true);var wrapped=loseCommitAcknowledgment(armed);var wrappedTx=transactions(wrapped);
        var uncertain=WorkerTestSupport.enabled(wrapped,wrappedTx,INSTALL);var fs=filesystem();
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.prepare(wrapped,wrappedTx,uncertain,guard,owner))
                .hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();
        assertThat(phases()).containsExactly("PREPARING");assertThat(attemptRoot()).doesNotExist();var db=metadata();
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.prepare(wrapped,wrappedTx,uncertain,guard,owner)).hasMessage("OWNERSHIP_LOST");
        assertThatThrownBy(this::inspect).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void lostClaimAcknowledgmentRetainsEnrollmentButDoesNotReturnOrAdoptAnOwner() throws Exception {
        UUID run=queue("lost-claim");ready();UUID attempt=UUID.randomUUID();var fs=filesystem();
        var wrapped=loseCommitAcknowledgment(new AtomicBoolean(true));var uncertain=WorkerTestSupport.enabled(wrapped,transactions(wrapped),INSTALL);
        assertThatThrownBy(()->claim(uncertain,guard,session,attempt,run)).hasMessage("MANAGEMENT_UNAVAILABLE").hasNoCause();
        assertThat(workers.lookup(attempt).orElseThrow().runId()).isEqualTo(run);
        assertThat(count("content_attempt_admissions")).isEqualTo(1);assertThat(phases()).isEmpty();var db=metadata();
        assertThatThrownBy(()->claim(uncertain,guard,session,attempt,run)).hasMessage("OWNERSHIP_LOST");
        assertThatThrownBy(()->NarrowContentAdmissionTestSupport.inspect(jdbc,tx,guard,run,attempt)).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void readyFactsRemainHistoricalAfterAuthorityChanges() throws Exception {
        claimed();var expected=prepare();workers.heartbeat(guard,owner);cancel();
        jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
        var db=metadata();var fs=filesystem();assertThat(inspect()).isEqualTo(expected);
        assertThatThrownBy(this::prepare).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void admissionAndInspectionRejectAnOuterTransaction() throws Exception {
        claimed();var db=metadata();var fs=filesystem();
        assertThatThrownBy(()->tx.required(this::prepare)).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThatThrownBy(()->tx.required(this::inspect)).hasMessage("WORKER_CONFIGURATION_INVALID");
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);
    }
    @Test void narrowClaimRejectsRetainedSilverBeforeEnrollmentOrFilesystemEffects() throws Exception {
        UUID run=UUID.fromString(submit(content(),"retained").get("runId").stringValue());ready();var db=metadata();var fs=filesystem();
        assertThatThrownBy(()->claim(workers,guard,session,UUID.randomUUID(),run)).isInstanceOf(RuntimeException.class);
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @ParameterizedTest @ValueSource(ints={18,19})
    void narrowAdmissionReservesTheTwentiethAttemptForReconciliation(int existing) throws Exception {
        UUID run=queue("capacity");ready();historicalAttempts(run,existing);var before=metadata();var fs=filesystem();
        if(existing==18) {
            var next=claim(workers,guard,session,UUID.randomUUID(),run);assertThat(next.owner()).isNotNull();
            assertThat(jdbc.queryForObject("SELECT attempt_no FROM ddip_management.attempts WHERE attempt_id=?",Integer.class,next.owner().attemptId())).isEqualTo(19);
        } else {
            assertThatThrownBy(()->claim(workers,guard,session,UUID.randomUUID(),run)).isInstanceOf(RuntimeException.class);
            assertThat(metadata()).isEqualTo(before);
        }
        assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void sparseHistoricalAttemptNineteenStillExhaustsNarrowContinuationCapacity() throws Exception {
        UUID run=queue("sparse-capacity");ready();
        jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at) VALUES (?,?,19,'EXECUTE',?,1,'FAILED',clock_timestamp())",UUID.randomUUID(),run,session.instanceId());
        assertThat(count("attempts")).isEqualTo(1);var db=metadata();var fs=filesystem();
        assertThatThrownBy(()->claim(workers,guard,session,UUID.randomUUID(),run)).hasMessage("WORKER_NOT_READY");
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    void historicalAttempts(UUID run,int count) {
        for(int i=1;i<=count;i++)jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at) VALUES (?,?,?,'EXECUTE',?,1,'FAILED',clock_timestamp())",UUID.randomUUID(),run,i,session.instanceId());
    }
    @Test void legacyClaimStillAllowsAttemptTwentyWithoutEnrollment() {
        UUID run=queue("legacy");ready();historicalAttempts(run,19);owner=workers.claim(guard,session,UUID.randomUUID()).owner();
        assertThat(workers.lookup(owner.attemptId()).orElseThrow().attemptNo()).isEqualTo(20);
        assertThat(count("content_attempt_admissions")).isZero();assertThat(count("content_admission_milestones")).isZero();
        assertThat(jdbc.queryForObject("SELECT content_admission_policy FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isNull();
        assertThat(workers.markRunning(guard,owner).attemptState()).isEqualTo("RUNNING");
    }
    @Test void defaultProductionCapabilityStillCannotClaimNarrowContent() throws Exception {
        UUID run=queue("inactive");ready();var production=new WorkerRepository(jdbc,tx,INSTALL);var before=metadata();var fs=filesystem();
        assertThat(production.executionAvailable()).isFalse();assertThat(production.claim(guard,session,UUID.randomUUID()).status()).isEqualTo("NO_EXECUTOR");
        assertThat(claim(production,guard,session,UUID.randomUUID(),run).status()).isEqualTo("NO_EXECUTOR");
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }

    Process child(String mode,UUID run,UUID attempt) throws Exception {
        Path descriptor=Files.createTempFile(temp,"admission-child-",".json");Files.write(descriptor,binding.descriptor());
        Files.setPosixFilePermissions(descriptor,PosixFilePermissions.fromString("rw-------"));
        String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",cp,
                NarrowContentAdmissionTestChild.class.getName(),descriptor.toString(),mode,CONTENT_PROFILE,run.toString(),attempt.toString())
                .redirectError(ProcessBuilder.Redirect.DISCARD);
        builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);
        return builder.start();
    }
    String childLine(Process process) throws Exception {
        var line=CompletableFuture.supplyAsync(()->{
            try {var reader=new java.io.BufferedReader(new java.io.InputStreamReader(process.getInputStream()));String text;
                while((text=reader.readLine())!=null)if(text.startsWith("NARROW_"))return text;return "EOF";
            }catch(Exception ex){throw new CompletionException(ex);}
        });
        try {return line.get(20,TimeUnit.SECONDS);}catch(Exception ex){kill(process);throw ex;}
    }
    void kill(Process process) throws Exception {
        if(process.isAlive())process.destroyForcibly();assertThat(process.waitFor(10,TimeUnit.SECONDS)).isTrue();
    }
    String inspectInFreshJvm(UUID run,UUID attempt) throws Exception {
        var before=metadata();var fs=filesystem();var process=child("inspect",run,attempt);
        try {
            assertThat(process.pid()).isNotEqualTo(ProcessHandle.current().pid());String result=childLine(process);
            assertThat(process.waitFor(10,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).isZero();
            assertThat(result).contains(" "+process.pid()+" ");return result;
        } finally {kill(process);assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(fs);}
    }
    @Test void actualFreshJvmAndPostgresRestartReconstructOnlyDurableReadyFacts() throws Exception {
        claimed();var expected=prepare();guard.close();guard=null;
        var before=metadata();var fs=filesystem();var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);
        String signal=System.getProperty("ddip.test.narrow-admission-restart-signal");
        if(signal!=null) {
            Files.writeString(Path.of(signal),"READY");long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
            while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<deadline)Thread.sleep(50);
            assertThat(Files.exists(Path.of(signal+".done"))).isTrue();
        } else {
            // The CI harness supplies its own disposable service container. Never choose a host database.
            String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).matches("[0-9a-f]{12,64}");
            var restart=new ProcessBuilder("docker","restart",container).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try {assertThat(restart.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(restart.exitValue()).isZero();}
            finally {if(restart.isAlive()){restart.destroyForcibly();restart.waitFor(5,TimeUnit.SECONDS);}}
        }
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
        while(true) {
            try {
                var after=jdbc.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);assertThat(after).isAfter(started);
                System.out.println("NARROW_ADMISSION_POSTGRES_RESTART before="+started+" after="+after+" attempt="+owner.attemptId());break;
            } catch(org.springframework.dao.DataAccessException ex) {if(System.nanoTime()>deadline)throw ex;Thread.sleep(100);}
        }
        assertThat(inspectInFreshJvm(owner.runId(),owner.attemptId())).startsWith("NARROW_READY ").endsWith(" "+expected.ledgerSha256());
        assertThat(metadata()).isEqualTo(before);assertThat(filesystem()).isEqualTo(fs);
        ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");
        assertThatThrownBy(()->claim(workers,guard,session,UUID.randomUUID(),owner.runId())).hasMessage("WORKER_NOT_READY");noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"PREPARING_COMMITTED","FILES_PREPARED","READY_COMMITTED"})
    void actualWriterJvmDeathNeverAdoptsOrRepairsAnInterruptedAttempt(String boundary) throws Exception {
        UUID run=queue("killed-writer"),attempt=UUID.randomUUID();var process=child(boundary,run,attempt);
        try {assertThat(childLine(process)).isEqualTo("NARROW_BOUNDARY "+process.pid()+" "+boundary);}finally{kill(process);}
        assertThat(count("attempts")).isEqualTo(1);assertThat(count("content_attempt_admissions")).isEqualTo(1);
        if(boundary.equals("READY_COMMITTED")) {
            assertThat(phases()).containsExactly("PREPARING","READY");assertThat(inspectInFreshJvm(run,attempt)).startsWith("NARROW_READY ");
        } else {
            assertThat(phases()).containsExactly("PREPARING");assertThat(inspectInFreshJvm(run,attempt)).startsWith("NARROW_REJECTED ");
        }
        ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");
        assertThatThrownBy(()->claim(workers,guard,session,UUID.randomUUID(),run)).hasMessage("WORKER_NOT_READY");
        assertThat(workers.lookup(attempt).orElseThrow().state()).isEqualTo("CLAIMED");noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"CLAIMED","PREPARING","READY"})
    void legacyRecoveryNeverTreatsNarrowMissingRootAsAnEmptySafeAttempt(String phase) throws Exception {
        claimed();if(phase.equals("PREPARING"))stopAt("PREPARING_COMMITTED");else if(phase.equals("READY")) {
            prepare();try(var tree=Files.walk(attemptRoot())){for(Path path:tree.sorted(Comparator.reverseOrder()).toList())Files.delete(path);}
        }
        assertThat(attemptRoot()).doesNotExist();UUID control=UUID.randomUUID();
        byte[] request=SourceJson.canonical(SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true));
        jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,target_attempt_id,quiescence_confirmed,state,requester) VALUES (?,?,'RECONCILE','narrow-reconcile',?,?,?,?,true,'REQUESTED','local-operator')",
                control,owner.runId(),request,SourceJson.sha(request),owner.runVersion(),owner.attemptId());
        guard.close();guard=null;ready();assertThat(session.status()).isEqualTo("RECOVERY_ONLY");var db=metadata();var fs=filesystem();
        assertThat(canQualifyLegacyRecovery(jdbc,tx,INSTALL,control)).isFalse();
        assertThatThrownBy(()->inspectLegacyRecovery(jdbc,tx,guard,session,control)).hasMessage("EVIDENCE_UNAVAILABLE");
        assertThatThrownBy(()->tx.required(()->{ContentResumePlan.gates(jdbc,INSTALL,jdbc.queryForMap("SELECT * FROM ddip_management.runs WHERE run_id=?",owner.runId()));return null;}))
                .hasMessage("RESUME_CHECKPOINT_UNSUPPORTED");
        assertThat(metadata()).isEqualTo(db);assertThat(filesystem()).isEqualTo(fs);noExecution();
    }
    @Test void versionNineUpgradePreservesAllLegacyAttemptHistoryWithoutBackfillingAdmission() {
        jdbc.execute("DROP SCHEMA ddip_management CASCADE");
        var old=Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").target("9").load();old.migrate();
        new JdbcCoordinatorRepository(jdbc,tx,INSTALL).initialize();
        byte[] bytes=SourceJson.canonical(SourceJson.object());String hash=SourceJson.sha(bytes);UUID worker=UUID.randomUUID(),run=UUID.randomUUID();
        jdbc.update("INSERT INTO ddip_management.execution_profiles(profile_id,configuration_bytes,configuration_sha256) VALUES ('legacy-upgrade',?,?)",bytes,hash);
        jdbc.update("INSERT INTO ddip_management.worker_instances(instance_id,installation_id,execution_profile_id,profile_sha256,host_binding_sha256,pid,process_started_at,heartbeat_at,status) VALUES (?,?,'legacy-upgrade',?,?,1,clock_timestamp(),clock_timestamp(),'STOPPED')",worker,INSTALL,hash,hash);
        jdbc.update("INSERT INTO ddip_management.runs(run_id,product_id,pipeline,pipeline_version,principal_scope,idempotency_key,normalized_request,request_sha256,captured_selection,selection_sha256,execution_profile_id,state) VALUES (?,'content-intelligence','CONTENT_RSS_V1',1,'local-operator','upgrade',?,?,?,?,'legacy-upgrade','QUEUED')",run,bytes,hash,bytes,hash);
        for(int i=1;i<=20;i++)jdbc.update("INSERT INTO ddip_management.attempts(attempt_id,run_id,attempt_no,kind,instance_id,owner_epoch,state,finished_at) VALUES (?,?,?,'EXECUTE',?,1,'FAILED',clock_timestamp())",UUID.randomUUID(),run,i,worker);
        var history=jdbc.queryForList("SELECT to_jsonb(a)::text FROM ddip_management.attempts a ORDER BY attempt_no",String.class);
        var all=Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load();
        var lifecycle=new SchemaLifecycle(all,jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL));
        assertThatThrownBy(()->lifecycle.verify(false)).hasMessage("MANAGEMENT_INCOMPATIBLE");lifecycle.verify(true);lifecycle.verify(false);
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts WHERE content_admission_policy IS NOT NULL OR content_recovery_policy IS NOT NULL OR content_terminal_policy IS NOT NULL",Long.class)).isZero();
        assertThat(jdbc.queryForList("SELECT (to_jsonb(a)-'content_admission_policy'-'content_recovery_policy'-'content_terminal_policy')::text FROM ddip_management.attempts a ORDER BY attempt_no",String.class)).isEqualTo(history);
        assertThat(count("content_attempt_admissions")).isZero();assertThat(count("content_admission_milestones")).isZero();
        assertThat(all.info().pending()).isEmpty();assertThat(all.info().current().getVersion().getVersion()).isEqualTo("23");
    }
}
