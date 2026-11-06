package platform.contentapi.management;

import java.nio.file.*;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;

/** Read-only assessment observes durable prefixes without minting execution authority. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchRecoveryAssessmentIT extends NarrowContentFetchLaunchFixtures {
    Map<String,List<String>> history(){var rows=new TreeMap<>(metadata());for(String table:List.of("content_native_operations","content_native_launch_phases","content_fetch_completion_intents","content_fetch_checkpoints"))rows.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));return rows;}
    NarrowContentFetchRecoveryAssessmentTestSupport.Assessment assess(){return NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId());}
    void checkpoint(){prepared();launch(b->{});NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});}
    void inert(NarrowContentFetchRecoveryAssessmentTestSupport.Assessment a){
        assertThat(a.authorization()).isFalse();assertThat(a.quiescenceEstablished()).isFalse();assertThat(a.nativeLaunchCompatible()).isFalse();
        assertThat(a.reviewTargets()).contains("REVIEW_ALL_LAUNCH_GAPS_AND_ORPHANS","STOP_EXECUTION_HANDLERS","REVIEW_UNMANAGED_WRITERS","REVIEW_DESCENDANTS","SEPARATE_RECOVERY_AUTHORITY_REQUIRED");
        assertThat(a.toString()).doesNotContain(temp.toString(),"http://","payload.xml","descriptor.json","<feed");
        assertThat(a.recordedProcesses()).hasSizeLessThanOrEqualTo(2);assertThat(a.reviewTargets()).hasSizeLessThanOrEqualTo(8);
        assertThat(a.durable().run()).isEqualTo(owner.runId());assertThat(a.durable().attempt()).isEqualTo(owner.attemptId());
        noExecution();
    }
    void stable(String prefix,String integrity)throws Exception{
        var before=history();var files=filesystem();int http=requests.get();var children=ProcessHandle.current().children().map(ProcessHandle::pid).sorted().toList();
        var a=assess();inert(a);assertThat(a.durable().prefix()).isEqualTo(prefix);assertThat(a.integrity()).isEqualTo(integrity);
        assertThat(assess().durable()).isEqualTo(a.durable());assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(http);
        assertThat(ProcessHandle.current().children().map(ProcessHandle::pid).sorted().toList()).isEqualTo(children);
    }
    @ParameterizedTest @ValueSource(strings={"ENROLLED","PREPARING","READY"})
    void earlyAdmissionPrefixesDoNotInspectOrCreateFiles(String prefix)throws Exception{
        claimed();if(prefix.equals("PREPARING"))stopAt("PREPARING_COMMITTED");else if(prefix.equals("READY"))prepare();
        stable(prefix,"NOT_INSPECTED_NO_CHECKPOINT");assertThat(requests).hasValue(0);assertThat(assess().durable().operation()).isNull();
    }
    @ParameterizedTest @ValueSource(strings={"RESERVED","LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED"})
    void interruptedLaunchPrefixesRemainConsumed(String prefix)throws Exception{
        prepared();assertThatThrownBy(()->launch(b->{if(b.equals(prefix))throw new IllegalStateException("TEST_BOUNDARY");})).isInstanceOf(RuntimeException.class);
        stable(prefix,"NOT_INSPECTED_NO_CHECKPOINT");assertThat(assess().durable().operation()).isEqualTo(operation);
    }
    @Test void completeIntactCheckpointNeedsNoInstalledRuntimeAndReturnsOnlyDetachedFacts()throws Exception{
        checkpoint();Files.delete(installedFetchJar);stable("CHECKPOINT","VERIFIED_AT_INSPECTION");
        var a=assess();assertThat(a.durable().candidateSha256()).matches("[a-f0-9]{64}");assertThat(a.durable().completionSha256()).matches("[a-f0-9]{64}");
        assertThatThrownBy(()->a.durable().launchPhases().clear()).isInstanceOf(UnsupportedOperationException.class);
        var sql=NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,null,owner.runId(),owner.attemptId());inert(sql);assertThat(sql.durable()).isEqualTo(a.durable());assertThat(sql.integrity()).isEqualTo("NOT_REQUESTED");
        assertThat(sql.recordedProcesses()).extracting(NarrowContentFetchRecoveryAssessmentTestSupport.ProcessFact::state).containsOnly("UNAVAILABLE");
    }
    @ParameterizedTest @ValueSource(strings={"ABSENT","REUSED","LIVE","UNAVAILABLE","THROWS"})
    void currentProcessObservationsNeverProveQuiescence(String state)throws Exception{
        checkpoint();var before=history();var files=filesystem();
        var a=NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId(),state,b->{});inert(a);
        assertThat(a.recordedProcesses()).hasSize(2);assertThat(a.recordedProcesses()).extracting(NarrowContentFetchRecoveryAssessmentTestSupport.ProcessFact::state).containsOnly(state.equals("THROWS")?"UNAVAILABLE":state);
        assertThat(a.recordedProcesses()).extracting(NarrowContentFetchRecoveryAssessmentTestSupport.ProcessFact::role).containsExactlyInAnyOrder("WORKER","CHILD");
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);
    }
    void replace(Path file)throws Exception{var bytes=Files.readAllBytes(file);var modified=Files.getLastModifiedTime(file);Files.move(file,temp.resolve("original-"+UUID.randomUUID()));Files.createFile(file,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")));Files.write(file,bytes);Files.setLastModifiedTime(file,modified);}
    @ParameterizedTest @ValueSource(strings={"missing-tree","missing-marker","tampered-payload","copied-payload","copied-marker","copied-lock","tampered-ledger","replaced-directory","symlink","hardlink"})
    void changedFilesNeverEraseDurableCheckpoint(String change)throws Exception{
        checkpoint();var durable=assess().durable();Path payload=fetchDirectory().resolve("payload.xml");
        switch(change){
            case "missing-tree"->Files.move(attemptRoot(),temp.resolve("detached"));
            case "missing-marker"->Files.delete(fetchDirectory().resolve("complete.json"));
            case "tampered-payload"->Files.writeString(payload,"changed");
            case "copied-payload"->replace(payload);
            case "copied-marker"->replace(fetchDirectory().resolve("complete.json"));
            case "copied-lock"->replace(attemptRoot().resolve(NarrowContentAdmissionTestSupport.LOCK));
            case "tampered-ledger"->Files.writeString(attemptRoot().resolve(NarrowContentAdmissionTestSupport.MANIFEST),"{}");
            case "replaced-directory"->{Path dir=fetchDirectory(),old=temp.resolve("old-fetch");Files.move(dir,old);Files.createDirectory(dir,PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwx------")));try(var paths=Files.list(old)){for(Path p:paths.toList())Files.move(p,dir.resolve(p.getFileName()));}}
            case "symlink"->{Path old=temp.resolve("symlink-target");Files.move(payload,old);Files.createSymbolicLink(payload,old);}
            case "hardlink"->Files.createLink(temp.resolve("payload-link"),payload);
        }
        var before=history();int http=requests.get();var result=assess();inert(result);assertThat(result.durable()).isEqualTo(durable);assertThat(result.integrity()).isEqualTo("UNAVAILABLE_OR_CHANGED");
        assertThat(history()).isEqualTo(before);assertThat(requests).hasValue(http);
        // The no-follow fixture cannot snapshot symlinks, but all other hostile trees remain byte-identical.
        if(!change.equals("symlink")){var files=filesystem();assess();assertThat(filesystem()).isEqualTo(files);}
    }
    @Test void identicalLedgerReplacementBeforeInspectionRetainsContentOnlyBinding()throws Exception{
        checkpoint();replace(attemptRoot().resolve(NarrowContentAdmissionTestSupport.MANIFEST));stable("CHECKPOINT","VERIFIED_AT_INSPECTION");
    }
    @Test void foreignGuardCannotVerifyCheckpoint()throws Exception{
        checkpoint();var before=history();var durable=assess().durable();
        try(var foreign=InstallationHostGuard.acquire(OwnershipFixtures.host(Files.createTempDirectory(temp,"foreign-host")))){
            var a=NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,foreign,owner.runId(),owner.attemptId());inert(a);assertThat(a.durable()).isEqualTo(durable);assertThat(a.integrity()).isEqualTo("UNAVAILABLE_OR_CHANGED");
            assertThat(a.recordedProcesses()).extracting(NarrowContentFetchRecoveryAssessmentTestSupport.ProcessFact::state).containsOnly("UNAVAILABLE");
        }
        assertThat(history()).isEqualTo(before);assertThat(requests).hasValue(1);
    }
    @ParameterizedTest @ValueSource(strings={"INTENT_COMMITTED","PREFIX_WRITTEN","MARKER_WRITTEN"})
    void intentWithoutCheckpointNeverTouchesHostileTree(String boundary)throws Exception{
        prepared();launch(b->{});assertThatThrownBy(()->NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{if(b.equals(boundary))throw new IllegalStateException("TEST_BOUNDARY");})).isInstanceOf(RuntimeException.class);
        Files.move(attemptRoot(),temp.resolve("removed-intent-tree-"+UUID.randomUUID()));Files.createSymbolicLink(attemptRoot(),temp.resolve("missing-target"));
        var before=history();var a=NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId(),"ABSENT",b->{throw new AssertionError("UNEXPECTED_FILE_ACCESS");});
        inert(a);assertThat(a.durable().prefix()).isEqualTo("COMPLETION_INTENT");assertThat(a.integrity()).isEqualTo("NOT_INSPECTED_NO_CHECKPOINT");assertThat(a.reviewTargets()).contains("INCOMPLETE_COMPLETION");assertThat(history()).isEqualTo(before);assertThat(Files.isSymbolicLink(attemptRoot())).isTrue();assertThat(requests).hasValue(1);
    }
    @ParameterizedTest @ValueSource(strings={"READY","CHECKPOINT"})
    void mixedLegacyStageHistoryFailsClosedWithoutOmittingItsProcess(String prefix)throws Exception{
        if(prefix.equals("CHECKPOINT"))checkpoint();else prepared();
        // This direct SQL shape is schema-valid, though supported execution APIs already veto it.
        var process=guard.process();jdbc.update("INSERT INTO ddip_management.stage_invocations(attempt_id,stage,item_ordinal,checkpoint_id,state,input_evidence_sha256,child_pid,child_started_at,child_instance_id) VALUES (?,'CHECK_INPUTS',0,?,'INTENT',?,?,?,?)",
                owner.attemptId(),UUID.randomUUID(),"a".repeat(64),process.pid(),java.sql.Timestamp.from(process.startedAt()),process.instanceId());
        var before=history();var files=filesystem();int http=requests.get();
        assertThatThrownBy(this::assess).hasMessage("WORKER_NOT_READY");
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(http);
    }
    @ParameterizedTest @ValueSource(strings={"LOCAL","PROCESS"})
    void occupiedBudgetLockNeverBlocksOrEstablishesQuiescence(String holder)throws Exception{
        checkpoint();var durable=assess().durable();var before=history();var files=filesystem();
        Path lockPath=attemptRoot().resolve(NarrowContentAdmissionTestSupport.LOCK);
        java.nio.channels.FileChannel channel=null;java.nio.channels.FileLock lock=null;Process child=null;
        try{
            if(holder.equals("LOCAL")){channel=java.nio.channels.FileChannel.open(lockPath,StandardOpenOption.WRITE);lock=channel.lock();}
            else{
                child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),NarrowContentAssessmentLockChild.class.getName(),lockPath.toString()).redirectError(ProcessBuilder.Redirect.INHERIT).start();
                var reader=child.inputReader();assertThat(java.util.concurrent.CompletableFuture.supplyAsync(()->{try{return reader.readLine();}catch(java.io.IOException ex){throw new IllegalStateException(ex);}}).get(10,java.util.concurrent.TimeUnit.SECONDS)).isEqualTo("ASSESSMENT_LOCK_HELD");
            }
            org.junit.jupiter.api.Assertions.assertTimeoutPreemptively(java.time.Duration.ofSeconds(3),()->{
                var result=assess();inert(result);assertThat(result.durable()).isEqualTo(durable);assertThat(result.integrity()).isEqualTo("VERIFIED_AT_INSPECTION");
            });
            var probe=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),NarrowContentAssessmentLockChild.class.getName(),lockPath.toString(),"probe").redirectError(ProcessBuilder.Redirect.INHERIT).start();
            try{assertThat(probe.waitFor(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();assertThat(probe.exitValue()).isZero();assertThat(probe.inputReader().readLine()).isEqualTo("ASSESSMENT_LOCK_RETAINED");}
            finally{if(probe.isAlive())probe.destroyForcibly();}
            assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);
        }finally{
            if(lock!=null)lock.close();if(channel!=null)channel.close();
            if(child!=null){child.destroyForcibly();assertThat(child.waitFor(10,java.util.concurrent.TimeUnit.SECONDS)).isTrue();}
        }
    }
    @ParameterizedTest @ValueSource(strings={"BEFORE_REOPEN","JOURNAL_REOPENED","JOURNAL_REOPENED_LEDGER"})
    void sameInvocationReplacementCannotBecomeIntegrityEvidence(String boundary)throws Exception{
        checkpoint();var before=history();var durable=assess().durable();var changed=new AtomicBoolean();
        var a=NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,owner.runId(),owner.attemptId(),"ABSENT",b->{assertThat(org.springframework.transaction.support.TransactionSynchronizationManager.isActualTransactionActive()).isFalse();if(b.equals(boundary) || boundary.equals("JOURNAL_REOPENED_LEDGER") && b.equals("JOURNAL_REOPENED"))try{replace(boundary.endsWith("_LEDGER")?attemptRoot().resolve(NarrowContentAdmissionTestSupport.MANIFEST):fetchDirectory().resolve("payload.xml"));changed.set(true);}catch(Exception ex){throw new IllegalStateException(ex);}});
        assertThat(changed).isTrue();inert(a);assertThat(a.durable()).isEqualTo(durable);assertThat(a.integrity()).isEqualTo("UNAVAILABLE_OR_CHANGED");assertThat(history()).isEqualTo(before);assertThat(requests).hasValue(1);
    }
}
