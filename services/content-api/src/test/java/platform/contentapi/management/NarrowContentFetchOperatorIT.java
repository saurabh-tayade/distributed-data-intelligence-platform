package platform.contentapi.management;

import java.nio.file.Path;
import java.sql.Timestamp;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.execution.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Packaged operator, real FETCH evidence and local process observation. Old ownership metadata is
 * an isolated fixture containing a captured disposable process identity, not a packaged-worker crash. */
class NarrowContentFetchOperatorIT extends NarrowContentFetchLaunchFixtures {
    final List<Process> processes=new ArrayList<>();
    UUID control;
    Process originalProcess;
    @AfterEach void stopProcesses() throws Exception {
        for(var p:processes){p.destroyForcibly();assertThat(p.waitFor(10,TimeUnit.SECONDS)).isTrue();}
        processes.clear();
    }
    Process disposable() throws Exception {
        var p=new ProcessBuilder("sleep","120").start();processes.add(p);return p;
    }
    @Override void ready() {
        guard=InstallationHostGuard.acquire(binding);
        try{originalProcess=disposable();}catch(java.io.IOException ex){throw new java.io.UncheckedIOException(ex);}catch(Exception ex){throw new IllegalStateException(ex);}
        var started=Timestamp.from(originalProcess.info().startInstant().orElseThrow());
        // Fixture-only identity injection at the ORIGINAL INSERT, before any immutable history exists.
        // The guard/capability remain test fixtures; recovery always uses the production LOCAL observer.
        var insertions=new java.util.concurrent.atomic.AtomicInteger();
        var registration=new org.springframework.jdbc.core.JdbcTemplate(jdbc.getDataSource()) {
            @Override public int update(String sql,Object... args) {
                if(sql.startsWith("INSERT INTO ddip_management.worker_instances(")){
                    insertions.incrementAndGet();return super.update(sql,NativeFetchOperatorFixtureIdentity.atInsert(sql,args,originalProcess.pid(),started));
                }
                return super.update(sql,args);
            }
        };
        workers=WorkerTestSupport.enabled(registration,tx,INSTALL);
        session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE));
        assertThat(insertions).hasValue(1);
    }
    void admitted() throws Exception {admitted(false);}
    void admitted(boolean keepOwnerAlive) throws Exception {
        prepared();launch(b->{});NarrowContentFetchCompletionTestSupport.complete(jdbc,tx,workers,guard,owner,operation,b->{});
        // Capture at insertion, then prove it exited. Never rewrite immutable PID/start columns.
        if(!keepOwnerAlive){originalProcess.destroy();assertThat(originalProcess.waitFor(10,TimeUnit.SECONDS)).isTrue();}
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,owner.runId());
        var request=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);
        // The live-owner negative case needs an admitted fixture intent to reach the adapter veto.
        // Only intent admission uses its existing test seam; no recovery observation is substituted.
        var controls=keepOwnerAlive?new ControlRepository(jdbc,tx,INSTALL,identities->{}):new ControlRepository(jdbc,tx,INSTALL);
        control=UUID.fromString(controls.request(ControlCommands.parse(owner.runId().toString(),SourceJson.canonical(request),"native-operator","\""+version+"\"")).get("controlId").stringValue());
        guard.close();guard=null;
    }
    Child start(String action,JsonNode plan) throws Exception {return start(action,plan,url,true);}
    Child start(String action,JsonNode plan,String databaseUrl,boolean namespaceAssertion) throws Exception {
        var args=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-jar",Path.of("target/content-api-0.1.0-SNAPSHOT.jar").toAbsolutePath().toString(),
                "--ddip.role=operator-recovery","--ddip.recovery.action="+action));
        if(plan==null)args.addAll(List.of("--ddip.recovery.control-id="+control,
                "--ddip.worker.execution-profile-id="+CONTENT_PROFILE,"--ddip.worker.control-binding-id="+binding.id(),"--ddip.worker.deployment-sha256="+owner.worker().deploymentDigest()));
        else args.addAll(List.of("--ddip.recovery.handoff-id="+plan.get("handoffId").stringValue(),"--ddip.recovery.finalization-id="+plan.get("finalizationId").stringValue()));
        if(namespaceAssertion)args.add("--ddip.recovery.same-host-process-namespace=true");
        var builder=new ProcessBuilder(args).redirectErrorStream(true);builder.environment().putAll(Map.of("DDIP_DB_URL",databaseUrl,"DDIP_DB_USER",user,"DDIP_DB_PASSWORD",password,"DDIP_INSTALLATION_ID",INSTALL.toString()));
        var process=builder.start();processes.add(process);return new Child(process);
    }
    ObjectNode answer(JsonNode prompt) {
        var reply=SourceJson.object().put("approval","AUTHORIZE_"+prompt.get("phase").stringValue()).put("reviewMethod","LOCAL_PROCESS_TREE_AND_WRITERS_REVIEWED");
        for(String key:List.of("challenge","inspectionSha256","targetSha256","checkpointId","recordedProcesses"))reply.set(key,prompt.get(key));
        for(String key:List.of("handlersStopped","unmanagedWritersStopped","orphanReviewComplete","descendantsReviewed","launchGapsReviewed","sameHostProcessNamespace"))reply.put(key,true);
        reply.set("descendants",prompt.get("previouslyReviewedDescendants"));return reply;
    }
    Map<String,List<String>> history() {
        var result=new TreeMap<>(metadata());
        for(String table:List.of("controls","content_native_operations","content_native_launch_phases","content_fetch_completion_intents","content_fetch_checkpoints","content_fetch_recovery_handoffs","content_fetch_recovery_finalizations","run_sources","run_inputs","artifacts"))
            result.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));
        return result;
    }
    void unresolved() {
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNotNull();
        assertThat(count("content_fetch_recovery_finalizations")).isZero();
        assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,owner.runId())).isNull();
    }
    @Test void twoLocalReviewsEndFailedAndRestartInspectionIsReadOnly() throws Exception {
        admitted();var files=filesystem();var child=start("native-fetch-end",null);var plan=child.await("NATIVE_FETCH_PLAN");
        var first=child.await("CONFIRMATION_REQUIRED");assertThat(first.get("phase").stringValue()).isEqualTo("NATIVE_FETCH_HANDOFF");child.send(answer(first));
        var second=child.await("CONFIRMATION_REQUIRED");assertThat(second.get("phase").stringValue()).isEqualTo("NATIVE_FETCH_TERMINAL");
        assertThat(second.get("challenge")).isNotEqualTo(first.get("challenge"));unresolved();child.send(answer(second));
        var done=child.await("COMPLETED");child.exited(0);assertThat(done.get("runState").stringValue()).isEqualTo("FAILED");assertThat(done.get("code").stringValue()).isEqualTo("NATIVE_FETCH_INTERRUPTED");assertThat(done.get("retryEligible").booleanValue()).isFalse();
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);assertThat(count("content_fetch_recovery_finalizations")).isEqualTo(1);
        assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);
        var before=history();guard=InstallationHostGuard.acquire(binding);var inspect=start("native-fetch-inspect",plan);var found=inspect.await("NATIVE_FETCH_INSPECTED");inspect.exited(0);
        assertThat(found.toString()).contains("CONFIRMED_HANDOFF","CONFIRMED_TERMINAL");assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
        var mismatched=(ObjectNode)plan.deepCopy();mismatched.put("handoffId",UUID.randomUUID().toString());
        var mismatch=start("native-fetch-inspect",mismatched);assertThat(mismatch.await("NATIVE_FETCH_INSPECTED").get("terminalStatus").stringValue()).isEqualTo("UNRESOLVED");mismatch.exited(2);
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
    }
    @ParameterizedTest @ValueSource(strings={"missing","false","mismatch","replay","live-descendant","dropped-descendant"})
    void invalidSecondReviewLeavesSuccessorOccupied(String mode) throws Exception {
        admitted();var files=filesystem();var child=start("native-fetch-end",null);var plan=child.await("NATIVE_FETCH_PLAN");var first=child.await("CONFIRMATION_REQUIRED");var initial=answer(first);
        if(mode.equals("dropped-descendant")){var p=disposable();var at=p.info().startInstant().orElseThrow();p.destroy();assertThat(p.waitFor(10,TimeUnit.SECONDS)).isTrue();initial.withArray("descendants").addObject().put("pid",Long.toString(p.pid())).put("startedAt",at.toString());}
        child.send(initial);
        var second=child.await("CONFIRMATION_REQUIRED");var reply=answer(second);
        switch(mode){
            case "missing"->child.process.getOutputStream().close();
            case "false"->{reply.put("launchGapsReviewed",false);child.send(reply);}
            case "mismatch"->{reply.put("targetSha256","0".repeat(64));child.send(reply);}
            case "replay"->child.send(answer(first));
            case "dropped-descendant"->{assertThat(second.get("previouslyReviewedDescendants").size()).isEqualTo(1);reply.putArray("descendants");child.send(reply);}
            default->{var p=disposable();reply.withArray("descendants").addObject().put("pid",Long.toString(p.pid())).put("startedAt",p.info().startInstant().orElseThrow().toString());child.send(reply);}
        }
        child.await("RECOVERY_REQUIRED");child.exited(2);unresolved();assertThat(count("attempts")).isEqualTo(2);assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);assertThat(filesystem()).isEqualTo(files);
        var before=history();var inspect=start("native-fetch-inspect",plan);assertThat(inspect.await("NATIVE_FETCH_INSPECTED").toString()).contains("CONFIRMED_HANDOFF");inspect.exited(2);assertThat(history()).isEqualTo(before);
    }
    @Test void liveOldOwnerCannotBeOverriddenByApproval() throws Exception {
        admitted(true);assertThat(originalProcess.isAlive()).isTrue();var files=filesystem();var child=start("native-fetch-end",null);
        var event=child.await("RECOVERY_REQUIRED","CONFIRMATION_REQUIRED");
        assertThat(event.get("status").stringValue()).isEqualTo("RECOVERY_REQUIRED");assertThat(event.get("code").stringValue()).isEqualTo("PROCESS_UNRESOLVED");
        child.exited(2);unresolved();assertThat(count("content_fetch_recovery_handoffs")).isZero();assertThat(count("attempts")).isEqualTo(1);assertThat(filesystem()).isEqualTo(files);
    }
    @Test void processDeathAfterHandoffAllowsInspectionButNeverReconstructsOwner() throws Exception {
        admitted();var child=start("native-fetch-end",null);var plan=child.await("NATIVE_FETCH_PLAN");child.send(answer(child.await("CONFIRMATION_REQUIRED")));child.await("CONFIRMATION_REQUIRED");
        child.process.destroyForcibly();assertThat(child.process.waitFor(10,TimeUnit.SECONDS)).isTrue();unresolved();var before=history();
        var inspect=start("native-fetch-inspect",plan);assertThat(inspect.await("NATIVE_FETCH_INSPECTED").toString()).contains("CONFIRMED_HANDOFF");inspect.exited(2);assertThat(history()).isEqualTo(before);
        var retry=start("native-fetch-end",null);retry.await("RECOVERY_REQUIRED");retry.exited(2);unresolved();assertThat(count("attempts")).isEqualTo(2);assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);
    }
    @ParameterizedTest @ValueSource(strings={"false","nonce","unknown","oversized"})
    void invalidFirstReviewNeverTransfers(String mode)throws Exception {
        admitted();var files=filesystem();var child=start("native-fetch-end",null);var prompt=child.await("CONFIRMATION_REQUIRED");var reply=answer(prompt);
        switch(mode){
            case "false"->reply.put("handlersStopped",false);
            case "nonce"->reply.put("challenge",UUID.randomUUID().toString());
            case "unknown"->reply.put("unexpected",true);
            default->reply.put("challenge","x".repeat(17*1024));
        }
        // Oversized writes may observe the child's intentional pipe close after its input limit.
        try{child.send(reply);}catch(java.io.IOException ex){if(!mode.equals("oversized"))throw ex;}
        child.await("RECOVERY_REQUIRED");child.exited(2);unresolved();
        assertThat(count("content_fetch_recovery_handoffs")).isZero();assertThat(count("attempts")).isEqualTo(1);
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(owner.attemptId());assertThat(filesystem()).isEqualTo(files);
    }
    @Test void missingNamespaceAssertionFailsBeforeRegistration()throws Exception {
        admitted();var before=history();var files=filesystem();var child=start("native-fetch-end",null,url,false);
        assertThat(child.await("RECOVERY_REQUIRED").get("code").stringValue()).isEqualTo("INVALID_REQUEST");child.exited(2);
        assertThat(history()).isEqualTo(before);assertThat(filesystem()).isEqualTo(files);
    }
    @Test void expiredSecondReviewCannotReleaseSuccessor()throws Exception {
        admitted();var files=filesystem();var child=start("native-fetch-end",null);child.send(answer(child.await("CONFIRMATION_REQUIRED")));
        var second=child.await("CONFIRMATION_REQUIRED");assertThat(second.get("phase").stringValue()).isEqualTo("NATIVE_FETCH_TERMINAL");
        // Exercise the real adapter input deadline, without replacing its clock or injecting authority.
        Thread.sleep(61_000);
        assertThat(child.await("RECOVERY_REQUIRED").get("code").stringValue()).isEqualTo("FRESH_QUALIFICATION_REQUIRED");child.exited(2);
        unresolved();assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);assertThat(count("attempts")).isEqualTo(2);assertThat(filesystem()).isEqualTo(files);
    }
    @ParameterizedTest @ValueSource(strings={"NATIVE_FETCH_HANDOFF","NATIVE_FETCH_TERMINAL_RELEASE"})
    void lostActualCommitAcknowledgmentNeverRecreatesAuthority(String boundary)throws Exception {
        admitted();var files=filesystem();JsonNode plan;
        try(var proxy=new RecoveryAcknowledgmentTestSupport(url,boundary)){
            var child=start("native-fetch-end",null,proxy.url(),true);plan=child.await("NATIVE_FETCH_PLAN");child.send(answer(child.await("CONFIRMATION_REQUIRED")));
            if(boundary.equals("NATIVE_FETCH_HANDOFF")){
                var next=child.await("NATIVE_FETCH_OUTCOME","CONFIRMATION_REQUIRED","RECOVERY_REQUIRED");
                assertThat(next.get("status").stringValue()).isEqualTo("NATIVE_FETCH_OUTCOME");assertThat(next.get("handoffStatus").stringValue()).isEqualTo("CONFIRMED_HANDOFF");
                child.await("RECOVERY_REQUIRED");child.exited(2);unresolved();
                assertThat(child.output).noneMatch(line->line.contains("NATIVE_FETCH_TERMINAL") || line.contains("\"status\":\"COMPLETED\""));
            }else{
                child.send(answer(child.await("CONFIRMATION_REQUIRED")));var done=child.await("COMPLETED");child.exited(0);
                assertThat(done.get("runState").stringValue()).isEqualTo("FAILED");assertThat(done.get("code").stringValue()).isEqualTo("NATIVE_FETCH_INTERRUPTED");
                assertThat(count("content_fetch_recovery_finalizations")).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
            }
            assertThat(proxy.dropped()).withFailMessage(proxy.transactionSummary().toString()).isTrue();
        }
        assertThat(count("content_fetch_recovery_handoffs")).isEqualTo(1);assertThat(count("attempts")).isEqualTo(2);assertThat(filesystem()).isEqualTo(files);assertThat(requests).hasValue(1);
        var before=history();var inspect=start("native-fetch-inspect",plan);var found=inspect.await("NATIVE_FETCH_INSPECTED");assertThat(found.get("handoffStatus").stringValue()).isEqualTo("CONFIRMED_HANDOFF");
        inspect.exited(boundary.equals("NATIVE_FETCH_HANDOFF")?2:0);assertThat(history()).isEqualTo(before);
    }
    static final class Child {
        final Process process;final BlockingQueue<String> lines=new LinkedBlockingQueue<>();final List<String> output=new CopyOnWriteArrayList<>();
        Child(Process process){this.process=process;Thread.ofPlatform().daemon().start(()->{try(var reader=process.inputReader()){String line;while((line=reader.readLine())!=null){output.add(line);lines.add(line);}}catch(java.io.IOException ignored){}});}
        JsonNode await(String... statuses)throws Exception {
            var wanted=Set.of(statuses);long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
            while(System.nanoTime()<deadline){var line=lines.poll(100,TimeUnit.MILLISECONDS);if(line==null){if(!process.isAlive() && lines.isEmpty())break;continue;}
                assertThat(line).startsWith("{").doesNotContain("jdbc:","Exception","command.json","retained.json");var event=SourceJson.parse(line.getBytes(java.nio.charset.StandardCharsets.UTF_8),1024*1024);
                if(wanted.contains(event.get("status").stringValue()))return event;
                if(Set.of("COMPLETED","RECOVERY_REQUIRED").contains(event.get("status").stringValue()))break;
            }
            throw new AssertionError("Expected "+wanted+"; output="+output);
        }
        void send(JsonNode reply)throws Exception {process.getOutputStream().write(SourceJson.canonical(reply));process.getOutputStream().write('\n');process.getOutputStream().flush();}
        void exited(int expected)throws Exception {assertThat(process.waitFor(15,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).withFailMessage(output.toString()).isEqualTo(expected);}
    }
}
