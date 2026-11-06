package platform.contentapi.management;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;

/** Actual strict child, loopback HTTP, durable PostgreSQL phases and fail-closed crash boundaries. */
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class NarrowContentFetchLaunchIT extends NarrowContentFetchLaunchFixtures {
    static final List<String> PHASES=List.of("LAUNCH_BOUND","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","RELEASE_AUTHORIZED","CANDIDATE_EXITED");
    @Test void oneRequestProducesOnlyCandidateAndPreservesHistoricalRuntime()throws Exception{
        prepared();byte[] old=jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id='fetch'",byte[].class);
        var boundaries=new ArrayList<String>();var result=launch(boundaries::add);
        assertThat(requests).hasValue(1);assertThat(result.candidateForVerification()).isTrue();assertThat(result.completed()).isFalse();assertThat(result.nativeLaunchCompatible()).isFalse();
        assertThat(result.phases()).containsExactlyElementsOf(PHASES);assertThat(result.child()).isNotNull();assertThat(result.child().pid()).isPositive();assertThat(result.child().startedAt()).isNotNull();
        assertThat(result.candidateSha256()).matches("[a-f0-9]{64}");assertThat(facts()).isEqualTo(result);
        assertThat(boundaries).containsSubsequence("RESERVED","LAUNCH_BOUND","DIRECTORY_CREATED","COMMAND_WRITTEN","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","BEFORE_RELEASE","RELEASE_AUTHORIZED","BEFORE_DELIVERY","CANDIDATE_EXITED");
        assertThat(Files.readAllBytes(fetchDirectory().resolve("payload.xml"))).isEqualTo(payload);assertThat(Files.exists(fetchDirectory().resolve("complete.json"))).isFalse();assertThat(Files.exists(fetchDirectory().resolve("result.json.preparing"))).isFalse();
        assertThat(jdbc.queryForObject("SELECT configuration_bytes FROM ddip_management.execution_profiles WHERE profile_id='fetch'",byte[].class)).isEqualTo(old);
        assertThat(SourceJson.parse(old).get("rssFetchRuntimeVersion").intValue()).isEqualTo(2);assertThat(SourceJson.parse(old).get("manifest").get("adapter").stringValue()).isEqualTo("platform.ingestion.managed.BoundedManagedRssFetch");
        assertThatThrownBy(()->result.phases().add("FORGED")).isInstanceOf(UnsupportedOperationException.class);retained();
    }
    @Test void immutablePhasesAndRestartInspectionCannotRefetchOrComplete()throws Exception{
        prepared();var history=launch(b->{});var files=filesystem();
        for(String sql:List.of("UPDATE ddip_management.content_native_launch_phases SET phase=phase","DELETE FROM ddip_management.content_native_launch_phases","TRUNCATE ddip_management.content_native_launch_phases CASCADE"))assertThatThrownBy(()->jdbc.execute(sql)).isInstanceOf(RuntimeException.class);
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThat(facts()).isEqualTo(history);
        assertThatThrownBy(()->launch(b->{})).isInstanceOf(RuntimeException.class);operation=UUID.randomUUID();assertThatThrownBy(()->launch(b->{})).isInstanceOf(RuntimeException.class);
        operation=history.operation();assertThat(facts()).isEqualTo(history);assertThat(requests).hasValue(1);assertThat(filesystem()).isEqualTo(files);retained();
    }
    @ParameterizedTest @ValueSource(strings={"DIRECTORY_CREATED","COMMAND_WRITTEN","FILES_READY","START_AUTHORIZED","CHILD_RECORDED","BEFORE_RELEASE","RELEASE_AUTHORIZED","BEFORE_DELIVERY"})
    void coordinatorChangeAtEveryExternalBoundaryPreventsRequest(String boundary){
        prepared();assertThatThrownBy(()->launch(b->{if(b.equals(boundary))jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");})).isInstanceOf(RuntimeException.class);
        assertThat(requests).hasValue(0);assertThat(facts().candidateForVerification()).isFalse();retained();assertChildStopped(facts().child());
    }
    @ParameterizedTest @ValueSource(strings={"run","attempt","worker","stop","cancel","guard"})
    void finalWriterThreadRechecksExactOwnerAndControls(String changed){
        prepared();long caller=Thread.currentThread().threadId();var writer=new AtomicLong();
        assertThatThrownBy(()->launch(b->{if(b.equals("BEFORE_DELIVERY")){
            writer.set(Thread.currentThread().threadId());
            switch(changed){
                case "run"->jdbc.update("UPDATE ddip_management.runs SET row_version=row_version+1 WHERE run_id=?",owner.runId());
                case "attempt"->jdbc.update("UPDATE ddip_management.attempts SET row_version=row_version+1 WHERE attempt_id=?",owner.attemptId());
                case "worker"->jdbc.update("UPDATE ddip_management.worker_instances SET row_version=row_version+1 WHERE instance_id=?",owner.worker().instanceId());
                case "stop"->jdbc.update("UPDATE ddip_management.coordinator SET mode='STOPPING',row_version=row_version+1");
                case "cancel"->cancel();
                case "guard"->guard.close();
            }
        }})).isInstanceOf(RuntimeException.class);
        assertThat(writer).doesNotHaveValue(0).doesNotHaveValue(caller);assertThat(requests).hasValue(0);retained();assertChildStopped(facts().child());
    }
    @Test void twoLaunchersHaveExactlyOneReservationAndRequest()throws Exception{
        prepared();var barrier=new CyclicBarrier(2);var completed=new AtomicInteger();var refused=new AtomicInteger();
        try(var pool=Executors.newFixedThreadPool(2)){
            var futures=new ArrayList<Future<?>>();for(int i=0;i<2;i++)futures.add(pool.submit(()->{
                try{barrier.await(5,TimeUnit.SECONDS);}catch(Exception ex){throw new CompletionException(ex);}
                var repository=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
                try{var result=NarrowContentFetchLaunchTestSupport.launch(jdbc,tx,repository,guard,owner,UUID.randomUUID(),LAUNCH_PROFILE,b->{});assertThat(result.candidateForVerification()).isTrue();completed.incrementAndGet();}
                catch(ManagementFailure|WorkerFailure|PreflightFailure ex){refused.incrementAndGet();}
            }));for(var f:futures)f.get(30,TimeUnit.SECONDS);
        }
        assertThat(completed).hasValue(1);assertThat(refused).hasValue(1);assertThat(requests).hasValue(1);retained();
    }
    @ParameterizedTest @ValueSource(strings={"BEFORE_RELEASE_PHASE","BEFORE_CANDIDATE_PHASE"})
    void lostPhaseAcknowledgmentKeepsCommittedFactsAndNeverRetries(String boundary)throws Exception{
        prepared();var armed=new AtomicBoolean();var uncertainJdbc=loseCommitAcknowledgment(armed);var uncertainTx=transactions(uncertainJdbc);var uncertain=WorkerTestSupport.enabled(uncertainJdbc,uncertainTx,INSTALL);
        assertThatThrownBy(()->NarrowContentFetchLaunchTestSupport.launch(uncertainJdbc,uncertainTx,uncertain,guard,owner,operation,LAUNCH_PROFILE,b->{if(b.equals(boundary))armed.set(true);})).hasMessage("MANAGEMENT_UNAVAILABLE");
        var history=facts();assertThat(history.phases()).contains(boundary.equals("BEFORE_RELEASE_PHASE")?"RELEASE_AUTHORIZED":"CANDIDATE_EXITED");
        assertThat(requests).hasValue(boundary.equals("BEFORE_RELEASE_PHASE")?0:1);assertThatThrownBy(()->uncertain.heartbeat(guard,owner)).hasMessage("OWNERSHIP_LOST");
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->launch(b->{})).isInstanceOf(RuntimeException.class);assertThat(facts()).isEqualTo(history);retained();assertChildStopped(history.child());
    }
    @Test void heartbeatContinuesWhileChildOwnsEmptyResultPrefix()throws Exception{
        prepared();responseGate=new CountDownLatch(1);var activeHeartbeats=new AtomicInteger();
        var result=launch(b->{if(b.equals("HEARTBEAT") && requests.get()==1){
            assertThat(Files.exists(fetchDirectory().resolve("result.json.preparing"))).isTrue();
            if(activeHeartbeats.incrementAndGet()>=2)responseGate.countDown();
        }});
        assertThat(activeHeartbeats.get()).isGreaterThanOrEqualTo(2);assertThat(requests).hasValue(1);assertThat(result.candidateForVerification()).isTrue();retained();
    }
    @Test void cancelledOwnerDuringHttpIsSupervisedWithoutCompletionOrRefetch(){
        prepared();responseGate=new CountDownLatch(1);var cancelled=new AtomicBoolean();
        assertThatThrownBy(()->launch(b->{if(b.equals("HEARTBEAT") && requests.get()==1 && cancelled.compareAndSet(false,true))cancel();})).isInstanceOf(RuntimeException.class);
        assertThat(cancelled).isTrue();assertThat(requests).hasValue(1);assertChildStopped(facts().child());assertThat(facts().candidateForVerification()).isFalse();
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->launch(b->{})).isInstanceOf(RuntimeException.class);assertThat(requests).hasValue(1);retained();
    }
    @Test void historyInspectionNeedsNeitherOwnerNorAttemptDirectory()throws Exception{
        prepared();var result=launch(b->{});guard.close();guard=null;Files.move(attemptRoot(),attemptRoot().resolveSibling("removed-attempt"));
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThat(facts()).isEqualTo(result);assertThat(requests).hasValue(1);noExecution();
    }
    @ParameterizedTest @ValueSource(strings={"START_AUTHORIZED","BEFORE_DELIVERY"})
    void replacedInstalledJarPreventsRequestAndNeverReusesReservation(String boundary){
        prepared();assertThatThrownBy(()->launch(b->{if(b.equals(boundary)){
            try{Files.writeString(installedFetchJar,"changed installed runtime");}catch(Exception ex){throw new IllegalStateException(ex);}
        }})).isInstanceOf(RuntimeException.class);
        assertThat(requests).hasValue(0);assertThat(facts().candidateForVerification()).isFalse();assertChildStopped(facts().child());retained();
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->launch(b->{})).isInstanceOf(RuntimeException.class);assertThat(requests).hasValue(0);
    }
    @ParameterizedTest @ValueSource(strings={"result.json.preparing","complete.json","foreign-stage"})
    void unexpectedEvidenceBeforeDeliveryPreventsRequest(String evidence){
        prepared();assertThatThrownBy(()->launch(b->{if(b.equals("BEFORE_DELIVERY")){
            try{
                Path path=evidence.equals("foreign-stage")?attemptRoot().resolve("check-inputs"):fetchDirectory().resolve(evidence);
                if(evidence.equals("foreign-stage"))Files.createDirectory(path,java.nio.file.attribute.PosixFilePermissions.asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rwx------")));
                else Files.createFile(path,java.nio.file.attribute.PosixFilePermissions.asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
            }catch(Exception ex){throw new IllegalStateException(ex);}
        }})).isInstanceOf(RuntimeException.class);
        assertThat(requests).hasValue(0);assertThat(facts().candidateForVerification()).isFalse();assertChildStopped(facts().child());retained();
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->launch(b->{})).isInstanceOf(RuntimeException.class);assertThat(requests).hasValue(0);
    }
    @ParameterizedTest @ValueSource(strings={"result.json.preparing","complete.json","result-identity"})
    void invalidCandidateCannotBeRecordedOrRefetched(String evidence){
        prepared();assertThatThrownBy(()->launch(b->{if(b.equals("BEFORE_CANDIDATE")){
            try{
                Path path=fetchDirectory().resolve(evidence.equals("result-identity")?"result.json":evidence);
                if(evidence.equals("result-identity"))Files.writeString(path,"{}");
                else Files.createFile(path,java.nio.file.attribute.PosixFilePermissions.asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rw-------")));
            }catch(Exception ex){throw new IllegalStateException(ex);}
        }})).isInstanceOf(RuntimeException.class);
        assertThat(requests).hasValue(1);assertThat(facts().candidateForVerification()).isFalse();assertChildStopped(facts().child());retained();
        workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->launch(b->{})).isInstanceOf(RuntimeException.class);assertThat(requests).hasValue(1);
    }
    static void assertChildStopped(InstallationHostGuard.ProcessIdentity child){
        if(child==null)return;
        var process=ProcessHandle.of(child.pid());assertThat(process.filter(p->p.info().startInstant().map(start->start.truncatedTo(java.time.temporal.ChronoUnit.MICROS).equals(child.startedAt())).orElse(true)).map(ProcessHandle::isAlive).orElse(false)).isFalse();
    }
}
