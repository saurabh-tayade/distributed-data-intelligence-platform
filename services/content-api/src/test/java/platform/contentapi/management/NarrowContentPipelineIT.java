package platform.contentapi.management;

import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;

/** Actual original-owner chain. Ordinary dispatch remains disabled. No fabricated stages/deliveries. */
class NarrowContentPipelineIT extends NarrowContentPipelineFixtures {
    NarrowContentPipelineTestSupport.Completed execute(java.util.function.Consumer<NarrowContentPipelineTestSupport.Event> fault){
        return NarrowContentPipelineTestSupport.execute(jdbc,tx,workers,guard,owner,PIPELINE_PROFILE,fault);
    }
    byte[] boundaryPayload;
    @org.junit.jupiter.api.BeforeEach void resetBoundaryPayload(){boundaryPayload=null;}
    @Override byte[] responsePayload(){return boundaryPayload==null?super.responsePayload():boundaryPayload;}
    private byte[] payloadAt(int bytes){
        var xml=new StringBuilder("<feed xmlns=\"http://www.w3.org/2005/Atom\"><title>Boundary corpus</title>");
        for(int i=0;i<64;i++)xml.append("<entry><id>urn:envelope:").append(i)
                .append("</id><title>Escaped &amp; &quot; ").append("é 🚀 ".repeat(32))
                .append("</title><link href=\"https://example.test/article/").append(i)
                .append("\"/><updated>2026-10-30T00:00:00Z</updated><summary>Local fixture</summary></entry>");
        xml.append("<!--");String suffix="--></feed>";
        int pad=bytes-xml.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8).length-suffix.length();
        assertThat(pad).isPositive();xml.append("x".repeat(pad)).append(suffix);
        var raw=xml.toString().getBytes(java.nio.charset.StandardCharsets.UTF_8);assertThat(raw).hasSize(bytes);return raw;
    }
    @Test void exactFetchPayloadLimitTraversesCurrentChainWithMeasuredCompleteEnvelopes()throws Exception{
        boundaryPayload=payloadAt(5*1024*1024);original();long start=System.nanoTime();
        var complete=execute(e->{});
        assertThat(count("content_pipeline_checkpoints")).isEqualTo(15);
        assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("SUCCEEDED");
        var before=filesystem();
        var report=NarrowContentEnvelopeTestSupport.measure(jdbc,attemptRoot(),owner.runId(),owner.attemptId(),"demo-feed");
        report.put("corpus","exact-fetch-limit-64-escaped-articles").put("elapsedMillis",(System.nanoTime()-start)/1000000);
        var history=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY));
        assertThat(history.get(owner.runId().toString()).get("counts").get("articles").stringValue()).isEqualTo("64");
        assertThat(history.get(owner.runId().toString()).get("result").get("artifactId").stringValue()).isEqualTo(complete.artifact().toString());
        assertThat(filesystem()).isEqualTo(before);
        assertThat(NarrowContentPipelineTestSupport.ordinaryContentDispatchAvailable()).isFalse();
        System.out.println("CONTENT_ENVELOPE_MEASUREMENT "+new String(SourceJson.canonical(report),java.nio.charset.StandardCharsets.UTF_8));
    }
    @Test void firstByteBeyondFetchLimitCannotReachBronzeOrSuccess()throws Exception{
        boundaryPayload=payloadAt(5*1024*1024+1);original();
        assertThatThrownBy(()->execute(e->{})).isInstanceOf(RuntimeException.class);
        assertThat(requests).hasValue(1);assertThat(objectRequests).hasValue(0);assertThat(objectWrites).hasValue(0);
        UUID fetchSlot=jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.content_native_operations WHERE attempt_id=?",UUID.class,owner.attemptId());
        var fetch=attemptRoot().resolve("fetch-source/demo-feed/"+fetchSlot);
        var result=SourceJson.parse(Files.readAllBytes(fetch.resolve("result.json")));
        assertThat(result.get("status").stringValue()).isEqualTo("FAILED");
        assertThat(result.get("code").stringValue()).isEqualTo("FETCH_PAYLOAD_TOO_LARGE");
        assertThat(fetch.resolve("complete.json")).doesNotExist();assertThat(fetch.resolve("payload.xml")).doesNotExist();
        for(String table:List.of("content_fetch_checkpoints","content_bronze_reservations","content_pipeline_operations","content_pipeline_completions"))assertThat(count(table)).as(table).isZero();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
        assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,owner.runId())).isNull();
        var before=filesystem();int calls=requests.get();
        assertThatThrownBy(()->execute(e->{})).isInstanceOf(RuntimeException.class);
        assertThat(requests).hasValue(calls);assertThat(filesystem()).isEqualTo(before);
        assertThat(NarrowContentPipelineTestSupport.ordinaryContentDispatchAvailable()).isFalse();
        var report=NarrowContentEnvelopeTestSupport.measure(jdbc,attemptRoot(),owner.runId(),owner.attemptId(),"demo-feed");
        report.put("corpus","first-rejected-fetch-byte");
        System.out.println("CONTENT_ENVELOPE_MEASUREMENT "+new String(SourceJson.canonical(report),java.nio.charset.StandardCharsets.UTF_8));
    }
    @Test void originalFetchThroughContentUsesAllSeventeenOperationsAndRealFinalProof()throws Exception{
        original();assertThat(NarrowContentPipelineTestSupport.ordinaryContentDispatchAvailable()).isFalse();var spare=NarrowContentPipelineTestSupport.spareDirectories(jdbc,tx,guard,owner);var events=new ArrayList<NarrowContentPipelineTestSupport.Event>();
        var completed=execute(e->{events.add(e);if(!e.boundary().equals("HEARTBEAT"))System.out.println("STRICT_PIPELINE_EVENT "+e);});
        assertThat(completed.run()).isEqualTo(owner.runId());assertThat(completed.attempt()).isEqualTo(owner.attemptId());
        assertThat(count("content_native_operations")).isEqualTo(1);assertThat(count("content_fetch_checkpoints")).isEqualTo(1);assertThat(count("content_bronze_checkpoints")).isEqualTo(1);
        assertThat(count("content_pipeline_operations")).isEqualTo(15);assertThat(count("content_pipeline_checkpoints")).isEqualTo(15);
        assertThat(jdbc.queryForList("SELECT step_index FROM ddip_management.content_pipeline_operations ORDER BY step_index",Integer.class)).containsExactly(2,3,4,5,6,7,8,9,10,11,12,13,14,15,16);
        assertThat(count("rss_deliveries")).isZero();assertThat(count("stage_invocations")).isZero();
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,owner.runId())).isEqualTo(completed.artifact());
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        byte[] proof=jdbc.queryForObject("SELECT proof_bytes FROM ddip_management.content_pipeline_completions WHERE attempt_id=?",byte[].class,owner.attemptId());
        assertThat(SourceJson.sha(proof)).isEqualTo(completed.proofSha256());
        assertThat(SourceJson.parse(proof).get("operations")).hasSize(15);
        var artifact=SourceJson.parse(jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?",byte[].class,completed.artifact()));
        assertThat(artifact.get("kind").stringValue()).isEqualTo("CONTENT");
        assertThat(artifact.get("verification").get("status").stringValue()).isEqualTo("VERIFIED");
        for(var path:spare)assertThat(path).doesNotExist();assertThat(NarrowContentPipelineTestSupport.ordinaryContentDispatchAvailable()).isFalse();
        assertThat(events.getLast().boundary()).isEqualTo("SUCCESS_COMMITTED");assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);
        NarrowContentPipelineSqlIT.assertCompletedGraphAndCounts(jdbc,owner.runId(),owner.attemptId(),completed.artifact());
        var before=metadata();var nativeRows=jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_pipeline_checkpoints t ORDER BY operation_id",String.class);
        var history=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY));var detail=history.get(owner.runId().toString());
        assertThat(detail.get("result").get("artifactId").stringValue()).isEqualTo(completed.artifact().toString());
        assertThat(detail.get("result").get("verificationStatus").stringValue()).isEqualTo("VERIFIED_AT_EXECUTION");
        assertThat(detail.get("counts").get("articles").stringValue()).isEqualTo("1");
        var attempt=history.attempts(owner.runId().toString(),10,null).data().get(0);
        assertThat(attempt.get("stageCount").stringValue()).isEqualTo("0");assertThat(attempt.get("nativeOperationCount").stringValue()).isEqualTo("17");
        assertThatThrownBy(()->history.nativeOperations(owner.runId().toString(),UUID.randomUUID().toString(),3,null)).hasMessage("NOT_FOUND");
        assertThatThrownBy(()->history.nativeOperations(UUID.randomUUID().toString(),owner.attemptId().toString(),3,null)).hasMessage("NOT_FOUND");
        String nativeCursor=history.nativeOperations(owner.runId().toString(),owner.attemptId().toString(),3,null).nextCursor();
        assertThatThrownBy(()->history.stages(owner.runId().toString(),owner.attemptId().toString(),3,nativeCursor)).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(()->history.attempts(owner.runId().toString(),3,nativeCursor)).hasMessage("INVALID_ARGUMENT");
        assertThatThrownBy(()->history.nativeOperations(owner.runId().toString(),owner.attemptId().toString(),3,nativeCursor+"tampered")).hasMessage("INVALID_ARGUMENT");
        var expiredHistory=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY),java.time.Clock.offset(java.time.Clock.systemUTC(),java.time.Duration.ofDays(2)));
        assertThatThrownBy(()->expiredHistory.nativeOperations(owner.runId().toString(),owner.attemptId().toString(),3,nativeCursor)).hasMessage("INVALID_ARGUMENT");
        var steps=new ArrayList<Integer>();String cursor=null;
        do{
            var page=history.nativeOperations(owner.runId().toString(),owner.attemptId().toString(),3,cursor);
            for(var fact:page.data()){
                steps.add(fact.get("stepIndex").intValue());assertThat(fact.get("continuationAuthorized").booleanValue()).isFalse();
                assertThat(fact.get("proofSha256").stringValue()).matches("[a-f0-9]{64}");assertThat(fact.get("checkpointedAt").isNull()).isFalse();
                assertThat(new String(SourceJson.canonical(fact),java.nio.charset.StandardCharsets.UTF_8)).doesNotContain("proof_bytes","/tmp/","credentialsFile","childPid","command.json");
            }
            cursor=page.nextCursor();
        }while(cursor!=null);
        assertThat(steps).containsExactly(0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16);
        var response=http(org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get(RUNS+"/"+owner.runId()+"/attempts/"+owner.attemptId()+"/native-operations?limit=100"));
        assertThat(response.getResponse().getStatus()).isEqualTo(200);assertThat(body(response).get("data")).hasSize(17);
        assertThat(metadata()).isEqualTo(before);assertThat(jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management.content_pipeline_checkpoints t ORDER BY operation_id",String.class)).isEqualTo(nativeRows);
        int reads=objectRequests.get();assertThatThrownBy(()->execute(e->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(reads);
    }
    @Test void cancellationAfterBronzeBeforeFirstNativeChildNeverContinues()throws Exception{
        original();assertThatThrownBy(()->execute(e->{if(e.operation().equals("BRONZE_BEFORE_SILVER") && e.boundary().equals("RESERVED"))cancel();})).isInstanceOf(RuntimeException.class);
        assertThat(count("content_bronze_checkpoints")).isEqualTo(1);assertThat(count("content_pipeline_operations")).isEqualTo(1);
        assertThat(count("content_pipeline_checkpoints")).isZero();assertThat(count("content_pipeline_completions")).isZero();assertThat(count("rss_deliveries")).isZero();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
        int reads=objectRequests.get();assertThatThrownBy(()->execute(e->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(reads);
    }
    @Test void lostReleaseAcknowledgmentConsumesSlotWithoutContinuation(){
        original();var armed=new AtomicBoolean();var wrapped=loseCommitAcknowledgment(armed);
        var j=new JdbcTemplate(wrapped.getDataSource()){@Override public int update(String sql,Object... args){int changed=super.update(sql,args);if(sql.startsWith("INSERT INTO ddip_management.content_pipeline_phases(") && "RELEASE_AUTHORIZED".equals(args[1]))armed.set(true);return changed;}};
        var t=transactions(j);var w=WorkerTestSupport.enabled(j,t,INSTALL);
        assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(j,t,w,guard,owner,PIPELINE_PROFILE,e->{})).hasMessage("MANAGEMENT_UNAVAILABLE");
        assertThat(jdbc.queryForList("SELECT phase FROM ddip_management.content_pipeline_phases",String.class)).contains("RELEASE_AUTHORIZED");
        assertThat(count("content_pipeline_operations")).isEqualTo(1);assertThat(count("content_pipeline_checkpoints")).isZero();assertThat(count("content_pipeline_completions")).isZero();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
        int reads=objectRequests.get();workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->execute(e->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(reads);
    }
    @Test void interruptedBeforeSuccessRetainsAllNativeProofsWithoutPublishingSuccess(){
        original();assertThatThrownBy(()->execute(e->{if(e.boundary().equals("BEFORE_SUCCESS"))throw new IllegalStateException("STOP_BEFORE_SUCCESS");})).hasMessage("STOP_BEFORE_SUCCESS");
        assertThat(count("content_fetch_checkpoints")).isEqualTo(1);assertThat(count("content_bronze_checkpoints")).isEqualTo(1);
        assertThat(count("content_pipeline_operations")).isEqualTo(15);assertThat(count("content_pipeline_checkpoints")).isEqualTo(15);
        assertThat(count("content_pipeline_completions")).isZero();
        assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,owner.runId())).isNull();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
        assertThat(count("rss_deliveries")).isZero();assertThat(count("stage_invocations")).isZero();
        int reads=objectRequests.get();workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);assertThatThrownBy(()->execute(e->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(reads);
    }
    @ParameterizedTest @ValueSource(strings={"command.json","result.json","evidence.json","operation-directory"})
    void replacementAfterCandidateValidationCannotBecomeCheckpoint(String name){
        original();var replaced=new AtomicBoolean();assertThatThrownBy(()->execute(e->{if(e.operation().equals("BRONZE_BEFORE_SILVER") && e.boundary().equals("BEFORE_CANDIDATE")){
            replaced.set(true);try(var children=Files.list(attemptRoot().resolve("inspect-bronze"))){
                Path directory=children.findFirst().orElseThrow();
                if(name.equals("operation-directory")){
                    Path old=directory.resolveSibling("old-operation");Files.move(directory,old);
                    Files.createDirectory(directory,java.nio.file.attribute.PosixFilePermissions.asFileAttribute(java.nio.file.attribute.PosixFilePermissions.fromString("rwx------")));
                    try(var files=Files.list(old)){for(var file:files.toList())Files.move(file,directory.resolve(file.getFileName()));}
                    Files.delete(old);
                }else{
                    Path target=directory.resolve(name);byte[] bytes=Files.readAllBytes(target);
                    Path replacement=target.resolveSibling("replacement.tmp");Files.write(replacement,bytes);
                    Files.setPosixFilePermissions(replacement,java.nio.file.attribute.PosixFilePermissions.fromString("rw-------"));
                    Files.move(replacement,target,StandardCopyOption.REPLACE_EXISTING);
                }
            }catch(java.io.IOException ex){throw new IllegalStateException(ex);}
        }})).isInstanceOf(RuntimeException.class);
        assertThat(replaced).isTrue();assertThat(count("content_pipeline_checkpoints")).isZero();
        var facts=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY)).nativeOperations(owner.runId().toString(),owner.attemptId().toString(),100,null).data();
        assertThat(facts).hasSize(3);assertThat(facts.get(0).get("state").stringValue()).isEqualTo("CHECKPOINTED");
        assertThat(facts.get(1).get("state").stringValue()).isEqualTo("CHECKPOINTED");assertThat(facts.get(2).get("proofSha256").isNull()).isTrue();
        assertThat(facts.get(2).get("checkpointedAt").isNull()).isTrue();assertThat(facts.get(2).get("continuationAuthorized").booleanValue()).isFalse();
        assertThat(jdbc.queryForList("SELECT phase FROM ddip_management.content_pipeline_phases",String.class)).doesNotContain("CANDIDATE_EXITED");
        assertThat(count("content_pipeline_completions")).isZero();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());
        int reads=objectRequests.get();assertThatThrownBy(()->execute(e->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(reads);
    }
    @Test void emptyFeedAndLostFinalCommitAcknowledgmentLeaveDurableVerifiedSuccess(){
        emptyFeed=true;original();var armed=new AtomicBoolean();var inserted=new AtomicBoolean();var wrapped=loseCommitAcknowledgment(armed);
        var t=transactions(wrapped);var w=WorkerTestSupport.enabled(wrapped,t,INSTALL);
        assertThatThrownBy(()->NarrowContentPipelineTestSupport.execute(wrapped,t,w,guard,owner,PIPELINE_PROFILE,e->{
            if(e.boundary().equals("SUCCESS_INSERTED")){inserted.set(true);armed.set(true);}
        })).hasMessage("MANAGEMENT_UNAVAILABLE");
        assertThat(inserted).isTrue();assertThat(count("content_fetch_checkpoints")).isEqualTo(1);assertThat(count("content_bronze_checkpoints")).isEqualTo(1);
        assertThat(count("content_pipeline_checkpoints")).isEqualTo(15);assertThat(count("content_pipeline_completions")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,owner.runId())).isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isEqualTo("SUCCEEDED");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        UUID artifact=jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,owner.runId());assertThat(artifact).isNotNull();
        NarrowContentPipelineSqlIT.assertCompletedGraphAndCounts(jdbc,owner.runId(),owner.attemptId(),artifact);
        var detail=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY)).get(owner.runId().toString());
        assertThat(detail.get("result").get("artifactId").stringValue()).isEqualTo(artifact.toString());
        assertThat(detail.get("result").get("verificationStatus").stringValue()).isEqualTo("VERIFIED_AT_EXECUTION");
        assertThat(detail.get("counts").get("articles").stringValue()).isEqualTo("0");assertThat(detail.get("counts").get("rows").stringValue()).isEqualTo("0");
        var nativeArtifact=SourceJson.parse(jdbc.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?",byte[].class,artifact));
        var verification=nativeArtifact.get("verification");var coverage=verification.get("coverage");
        assertThat(coverage.get("reference_source_count").stringValue()).isEqualTo("2");
        assertThat(coverage.get("unused_reference_source_count").stringValue()).isEqualTo("2");
        assertThat(coverage.get("matched_source_count").stringValue()).isEqualTo("0");assertThat(coverage.get("catalog_source_count").stringValue()).isEqualTo("0");
        var referenceReports=new ArrayList<tools.jackson.databind.JsonNode>();for(var report:verification.get("smoke"))if(report.get("name").stringValue().equals("reference_coverage"))referenceReports.add(report);
        assertThat(referenceReports).hasSize(1);assertThat(referenceReports.getFirst().get("rows").intValue()).isEqualTo(1);assertThat(referenceReports.getFirst().get("sha256").stringValue()).matches("[a-f0-9]{64}");
        assertThat(verification).isEqualTo(SourceJson.parse(jdbc.queryForObject("SELECT c.evidence_bytes FROM ddip_management.content_pipeline_checkpoints c JOIN ddip_management.content_pipeline_operations o USING(operation_id) WHERE o.attempt_id=? AND o.step_index=16",byte[].class,owner.attemptId())));
        var silver=SourceJson.parse(jdbc.queryForObject("SELECT c.selection_bytes FROM ddip_management.content_pipeline_checkpoints c JOIN ddip_management.content_pipeline_operations o USING(operation_id) WHERE o.attempt_id=? AND o.step_index=4",byte[].class,owner.attemptId()));
        assertThat(silver.get("manifest").get("inputs").get(0).get("counts").get("accepted").intValue()).isZero();
        assertThat(silver.get("manifest").get("inputs").get(0).get("counts").get("rejected").intValue()).isEqualTo(1);

        assertThat(count("rss_deliveries")).isZero();assertThat(count("stage_invocations")).isZero();assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);
        int reads=objectRequests.get();var before=metadata();workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        assertThatThrownBy(()->execute(e->{})).isInstanceOf(RuntimeException.class);assertThat(objectRequests).hasValue(reads);assertThat(metadata()).isEqualTo(before);
        var history=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY));
        String nativeCursor=history.nativeOperations(owner.runId().toString(),owner.attemptId().toString(),3,null).nextCursor();
        UUID foreign=queue("foreign-cursor-scope");guard.close();ready();var foreignOwner=workers.claim(guard,session,UUID.randomUUID()).owner();
        assertThat(foreignOwner).isNotNull();assertThat(foreignOwner.runId()).isEqualTo(foreign);
        assertThat(history.nativeOperations(foreign.toString(),foreignOwner.attemptId().toString(),3,null).data()).isEmpty();
        assertThatThrownBy(()->history.nativeOperations(foreign.toString(),foreignOwner.attemptId().toString(),3,nativeCursor)).hasMessage("INVALID_ARGUMENT");
        assertThat(objectRequests).hasValue(reads);assertThat(requests).hasValue(1);
    }
    @Test void ordinaryClaimWithoutNativeExecutionHasNoNativeFacts(){
        UUID run=queue("legacy-history"),other=queue("other-history");ready();owner=workers.claim(guard,session,UUID.randomUUID()).owner();
        assertThat(owner).isNotNull();assertThat(owner.runId()).isEqualTo(run);
        assertThat(jdbc.queryForObject("SELECT content_admission_policy FROM ddip_management.attempts WHERE attempt_id=?",String.class,owner.attemptId())).isNull();
        var before=metadata();var history=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY));
        assertThat(history.nativeOperations(run.toString(),owner.attemptId().toString(),100,null).data()).isEmpty();
        assertThatThrownBy(()->history.nativeOperations(other.toString(),owner.attemptId().toString(),100,null)).hasMessage("NOT_FOUND");
        assertThatThrownBy(()->history.nativeOperations(run.toString(),UUID.randomUUID().toString(),100,null)).hasMessage("NOT_FOUND");
        assertThat(history.attempts(run.toString(),100,null).data().get(0).has("nativeOperationCount")).isFalse();
        assertThat(metadata()).isEqualTo(before);assertThat(requests).hasValue(0);assertThat(objectRequests).hasValue(0);
    }
    @Test void occupiedProbeSlotRejectsPipelineBeforeFetch(){
        original();UUID probe=UUID.randomUUID();
        assertThatThrownBy(()->NarrowContentOperationTestSupport.probe(jdbc,tx,workers,guard,owner,probe,b->{if(b.equals("RESERVED"))throw new IllegalStateException("STOP_AFTER_RESERVATION");})).isInstanceOf(RuntimeException.class);
        assertThat(count("content_operations")).isEqualTo(1);
        assertThatThrownBy(()->execute(e->{})).isInstanceOf(RuntimeException.class);
        assertThat(requests).hasValue(0);assertThat(objectRequests).hasValue(0);assertThat(count("content_native_operations")).isZero();assertThat(count("content_pipeline_operations")).isZero();
    }
}
