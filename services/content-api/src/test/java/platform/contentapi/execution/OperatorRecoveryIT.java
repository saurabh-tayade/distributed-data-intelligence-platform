package platform.contentapi.execution;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.*;
import org.junit.jupiter.api.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;
import static org.assertj.core.api.Assertions.*;

/** Actual java -jar entry point, real PostgreSQL/native readers, explicit challenge replies. */
class OperatorRecoveryIT extends RecoveryVerificationFixtures {
    final List<Child> children=new ArrayList<>();
    boolean queuedSetup;
    @Override void finishSetup(JsonNode request,JsonNode frozen)throws Exception {
        if(!queuedSetup){super.finishSetup(request,frozen);return;}
        owner=null;run=UUID.fromString(new RunRepository(jdbc,tx,INSTALL,PreflightCatalogFixture.from(SourceJson.parse(Files.readAllBytes(work.resolve("source-catalog.json"))))).submit(RunCommands.parse(SourceJson.canonical(request),"actual-worker-interruption"),UUID.randomUUID().toString()).get("runId").stringValue());
    }
    @AfterEach void children(){children.forEach(Child::close);children.clear();}
    UUID request(UUID attempt,String key) {
        long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
        var body=SourceJson.object().put("action","RECONCILE").put("attemptId",attempt.toString()).put("quiescenceConfirmed",true);
        return UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(ControlCommands.parse(run.toString(),SourceJson.canonical(body),key,"\""+version+"\"")).get("controlId").stringValue());
    }
    void admit(){control=request(owner.attemptId(),"operational-recovery");guard.close();guard=null;}
    Child start(String action)throws Exception {
        return start(action,url);
    }
    Child start(String action,String databaseUrl)throws Exception {
        var command=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-jar",repo.resolve("services/content-api/target/content-api-0.1.0-SNAPSHOT.jar").toString(),"--ddip.role=operator-recovery","--ddip.recovery.action="+action,"--ddip.recovery.control-id="+control,"--ddip.worker.execution-profile-id="+PROFILE,"--ddip.worker.control-binding-id="+binding.id(),"--ddip.worker.runtime-profile-id="+RUNTIME,"--ddip.worker.deployment-sha256="+fixture.get("command").get("runtime").get("configuration").get("deploymentSha256").stringValue()));
        var builder=new ProcessBuilder(command).redirectErrorStream(true);builder.environment().putAll(Map.of("DDIP_DB_URL",databaseUrl,"DDIP_DB_USER",user,"DDIP_DB_PASSWORD",password,"DDIP_INSTALLATION_ID",INSTALL.toString()));
        var child=new Child(builder.start());children.add(child);return child;
    }
    ObjectNode answer(JsonNode prompt) {
        var n=SourceJson.object().put("approval","AUTHORIZE_"+prompt.get("phase").stringValue()).put("challenge",prompt.get("challenge").stringValue()).put("inspectionSha256",prompt.get("inspectionSha256").stringValue()).put("handlersStopped",true).put("unmanagedWritersStopped",true).put("orphanReviewComplete",true).put("reviewMethod","LOCAL_PROCESS_TREE_AND_WRITERS_REVIEWED");
        n.set("recordedProcesses",prompt.get("evidence").get("recordedProcesses"));n.set("reviewedCheckpoints",prompt.get("evidence").get("unresolvedCheckpoints"));n.set("lineageCheckpoints",prompt.get("evidence").get("lineageCheckpoints"));n.set("descendants",prompt.get("evidence").get("previouslyReviewedDescendants"));return n;
    }
    JsonNode recover()throws Exception {var child=start("recover");var initial=child.await("CONFIRMATION_REQUIRED");child.send(answer(initial));child.await("OBSERVED");var last=child.await("CONFIRMATION_REQUIRED");child.send(answer(last));var result=child.await("COMPLETED");child.exited(0);return result;}
    void unresolved(UUID occupied){assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(occupied);assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run)).isNull();assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RECOVERY_FINALIZED'",Long.class)).isZero();}
    @Test void inspectionIsReadOnlyAndDoesNotAuthorize()throws Exception {
        admit();long workers=jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class),audits=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class);var before=nativeSnapshot();
        var child=start("inspect");var summary=child.await("INSPECTED");child.exited(0);assertThat(summary.get("authorization").booleanValue()).isFalse();assertThat(summary.get("canQualify").booleanValue()).isTrue();assertThat(summary.get("unresolvedCheckpoints").size()).isEqualTo(1);unresolved(owner.attemptId());assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class)).isEqualTo(workers);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class)).isEqualTo(audits);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"JSONL","PARQUET"}) void completePublicationAndRepeatedInvocation(String fmt)throws Exception {
        if(fmt.equals("PARQUET"))parquet();admit();var before=nativeSnapshot();var result=recover();assertThat(result.get("runState").stringValue()).isEqualTo("SUCCEEDED");assertThat(result.get("code").stringValue()).isEqualTo("RECOVERED_PUBLICATION");assertThat(state()).isEqualTo("SUCCEEDED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        long audits=jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class),workers=jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class);var repeat=start("recover");assertThat(repeat.await("COMPLETED")).isEqualTo(result);repeat.exited(0);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events",Long.class)).isEqualTo(audits);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class)).isEqualTo(workers);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(2);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='RELIABILITY_V2'",Long.class)).isEqualTo(1);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"absent","incomplete","invalid","changedDependency"}) void knownFailuresReleaseWithoutNativeMutation(String mode)throws Exception {
        String code=switch(mode){case "absent"->{deleteTree(publication());yield "PUBLICATION_ABSENT";}case "incomplete"->{Files.delete(publication().resolve("complete.json"));yield "INCOMPLETE_OUTPUT";}case "invalid"->{Files.writeString(publication().resolve("complete.json"),"invalid");yield "PUBLICATION_INVALID";}default->{Files.writeString(dependencyMarker()," ",StandardOpenOption.APPEND);yield "DEPENDENCY_CHANGED";}};
        admit();var before=nativeSnapshot();var result=recover();assertThat(result.get("runState").stringValue()).isEqualTo("FAILED");assertThat(result.get("code").stringValue()).isEqualTo(code);assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();assertThat(nativeSnapshot()).isEqualTo(before);
    }
    Path dependencyMarker(){var input=originalPack.get("bundle").get("capturedSelection").get("inputs").get(0);String role=input.get("role").stringValue().equals("RELIABILITY_BATCH")?"batch":"reference";return Path.of(fixture.get("bundle").get("bindings").get(role).get("root").stringValue()).resolve(input.get("relativeAddress").stringValue()).resolve("complete.json");}
    @ParameterizedTest @ValueSource(strings={"missing","nonce","process","checkpoint","flag","descendant","evidence","ownership"}) void missingOrInconsistentApprovalCannotTransfer(String mode)throws Exception {
        admit();var before=nativeSnapshot();var child=start("recover");var prompt=child.await("CONFIRMATION_REQUIRED");var reply=answer(prompt);
        switch(mode){case "missing"->child.process.getOutputStream().close();case "nonce"->{reply.put("challenge",UUID.randomUUID().toString());child.send(reply);}case "process"->{reply.putArray("recordedProcesses");child.send(reply);}case "checkpoint"->{reply.putArray("reviewedCheckpoints");child.send(reply);}case "flag"->{reply.put("orphanReviewComplete",false);child.send(reply);}case "descendant"->{reply.withArray("descendants").addObject().put("pid",Long.toString(ProcessHandle.current().pid())).put("startedAt",ProcessHandle.current().info().startInstant().orElseThrow().toString());child.send(reply);}case "ownership"->{jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");child.send(reply);}default->{Files.writeString(binding.root().resolve("runs/"+run+"/attempts/"+owner.attemptId()+"/publish-reliability/"+jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.stage_invocations WHERE stage='PUBLISH_RELIABILITY'",UUID.class)+"/command.json")," ",StandardOpenOption.APPEND);child.send(reply);}}
        child.await("RECOVERY_REQUIRED");child.exited(2);unresolved(owner.attemptId());assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(1);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void guardContentionCannotRegisterCompetingRecovery()throws Exception {
        admit();var first=start("recover");first.await("CONFIRMATION_REQUIRED");long workers=jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class);var second=start("recover");assertThat(second.await("RECOVERY_REQUIRED").get("code").stringValue()).isEqualTo("HOST_GUARD_OCCUPIED");second.exited(2);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class)).isEqualTo(workers);unresolved(owner.attemptId());
    }
    @ParameterizedTest @ValueSource(strings={"RECOVERY_HANDOFF","RECOVERY_FINALIZED"}) void lostAcknowledgmentAtActualProcessBoundaryResolvesOnlyExactFacts(String action)throws Exception {
        admit();var before=nativeSnapshot();try(var proxy=new RecoveryAcknowledgmentProxy(url,action)){var child=start("recover",proxy.url());child.send(answer(child.await("CONFIRMATION_REQUIRED")));child.await("OBSERVED");child.send(answer(child.await("CONFIRMATION_REQUIRED")));assertThat(child.await("COMPLETED").get("runState").stringValue()).isEqualTo("SUCCEEDED");child.exited(0);assertThat(proxy.dropped).isTrue();}
        assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='RECOVERY_FINALIZED'",Long.class)).isEqualTo(1);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(2);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void staleConfirmationCannotTransfer()throws Exception {
        admit();var before=nativeSnapshot();var child=start("recover");var prompt=child.await("CONFIRMATION_REQUIRED");long remaining=61000-Duration.between(Instant.parse(prompt.get("inspectedAt").stringValue()),Instant.now()).toMillis();if(remaining>0)Thread.sleep(remaining);child.await("RECOVERY_REQUIRED");child.exited(2);unresolved(owner.attemptId());assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"unavailableDependency","cancel","changedOutput","ownership"}) void uncertainOrChangedEvidenceRetainsOccupiedRecovery(String mode)throws Exception {
        admit();var before=nativeSnapshot();Path blocked=null;Set<java.nio.file.attribute.PosixFilePermission> permissions=null;
        if(mode.equals("unavailableDependency")){blocked=dependencyMarker().getParent();permissions=Files.getPosixFilePermissions(blocked);Files.setPosixFilePermissions(blocked,Set.of());}
        try {
            var child=start("recover");child.send(answer(child.await("CONFIRMATION_REQUIRED")));
            if(mode.equals("unavailableDependency")){assertThat(child.await("OBSERVED").get("canQualifyFinalization").booleanValue()).isFalse();}
            else {child.await("OBSERVED");var prompt=child.await("CONFIRMATION_REQUIRED");
                if(mode.equals("cancel")){byte[] raw=SourceJson.canonical(SourceJson.object().put("action","CANCEL"));jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','pending-cancel',?,?,2,'REQUESTED','local-operator')",UUID.randomUUID(),run,raw,SourceJson.sha(raw));}
                if(mode.equals("changedOutput")){Files.writeString(publication().resolve("complete.json")," ",StandardOpenOption.APPEND);before=nativeSnapshot();}
                if(mode.equals("ownership"))jdbc.update("UPDATE ddip_management.coordinator SET row_version=row_version+1");child.send(answer(prompt));}
            child.await("RECOVERY_REQUIRED");child.exited(2);unresolved(control);assertThat(state()).isEqualTo("RECOVERY_REQUIRED");
        }finally{if(blocked!=null)Files.setPosixFilePermissions(blocked,permissions);}
        assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void activeOldWriterIsVetoedByPackagedInspection()throws Exception {
        long savedPid=oldPid;Instant savedStart=oldStart;guard.close();guard=null;
        try {
            oldPid=ProcessHandle.current().pid();oldStart=ProcessHandle.current().info().startInstant().orElseThrow();jdbc.execute("DROP SCHEMA ddip_management CASCADE");new SchemaLifecycle(org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load(),jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);provision("JSONL");
            // Fixture creates conflicting admitted evidence; the production admission veto is covered separately.
            var body=SourceJson.object().put("action","RECONCILE").put("attemptId",owner.attemptId().toString()).put("quiescenceConfirmed",true);var parsed=ControlCommands.parse(run.toString(),SourceJson.canonical(body),"conflicting-process-fixture","\"1\"");control=UUID.randomUUID();jdbc.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,target_attempt_id,quiescence_confirmed,state,requester) VALUES (?, ?, 'RECONCILE', 'conflicting-process-fixture', ?, ?, 1, ?, true, 'REQUESTED', 'local-operator')",control,run,parsed.normalized(),SourceJson.sha(parsed.normalized()),owner.attemptId());guard.close();guard=null;
            var child=start("inspect");var result=child.await("INSPECTED");child.exited(0);assertThat(result.get("canQualify").booleanValue()).isFalse();var recovery=start("recover");recovery.await("RECOVERY_REQUIRED");recovery.exited(2);unresolved(owner.attemptId());
        }finally{oldPid=savedPid;oldStart=savedStart;}
    }
    @Test void databaseOutageDuringFinalConfirmationNeverReleases()throws Exception {
        admit();var before=nativeSnapshot();var child=start("recover");child.send(answer(child.await("CONFIRMATION_REQUIRED")));child.await("OBSERVED");var prompt=child.await("CONFIRMATION_REQUIRED");try{outage(true);child.send(answer(prompt));child.await("RECOVERY_REQUIRED");child.exited(2);}finally{outage(false);}unresolved(control);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void unavailableDatabaseAtPackagedStartupHasOnlySafeDiagnostic()throws Exception {
        admit();var before=nativeSnapshot();var child=start("recover","jdbc:postgresql://127.0.0.1:1/"+database);assertThat(child.await("RECOVERY_REQUIRED").get("code").stringValue()).isEqualTo("STARTUP_UNAVAILABLE");child.exited(2);unresolved(owner.attemptId());assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void databaseRestartDuringInspectionNeedsFreshGuardedChecks()throws Exception {
        admit();var before=nativeSnapshot();var child=start("recover");var prompt=child.await("CONFIRMATION_REQUIRED");var started=jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class);String signal=System.getProperty("ddip.test.operator-restart-signal");
        if(signal!=null){Files.writeString(Path.of(signal),"READY");long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<end)Thread.sleep(20);assertThat(Files.exists(Path.of(signal+".done"))).isTrue();assertThat(jdbc.queryForObject("SELECT pg_postmaster_start_time()",java.time.OffsetDateTime.class)).isAfter(started);}
        child.send(answer(prompt));var outcome=child.await(Set.of("OBSERVED","RECOVERY_REQUIRED"));
        if(outcome.get("status").stringValue().equals("RECOVERY_REQUIRED")) {
            // A pooled pre-restart connection may fail its next read. This is a valid fail-closed exit,
            // not permission for application retry/adoption. The operator explicitly invokes anew.
            assertThat(outcome.get("code").stringValue()).isEqualTo("MANAGEMENT_UNAVAILABLE");child.exited(2);unresolved(owner.attemptId());assertThat(recover().get("runState").stringValue()).isEqualTo("SUCCEEDED");
        }else {child.send(answer(child.await("CONFIRMATION_REQUIRED")));child.await("COMPLETED");child.exited(0);}
        assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void freshOrdinaryWorkerExecutesUnrelatedRunOnlyAfterSafeRelease()throws Exception {
        jdbc.update("UPDATE ddip_management.coordinator SET heartbeat_at=clock_timestamp(),row_version=row_version+1");byte[] raw=jdbc.queryForObject("SELECT normalized_request FROM ddip_management.runs WHERE run_id=?",byte[].class,run);
        UUID unrelated=UUID.fromString(new RunRepository(jdbc,tx,INSTALL,PreflightCatalogFixture.from(SourceJson.parse(Files.readAllBytes(work.resolve("source-catalog.json"))))).submit(RunCommands.parse(raw,"unrelated-after-recovery"),UUID.randomUUID().toString()).get("runId").stringValue());
        admit();var before=nativeSnapshot();var recovery=start("recover");recovery.send(answer(recovery.await("CONFIRMATION_REQUIRED")));recovery.await("OBSERVED");var finalPrompt=recovery.await("CONFIRMATION_REQUIRED");unresolved(control);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,unrelated)).isEqualTo("QUEUED");
        recovery.send(answer(finalPrompt));recovery.await("COMPLETED");recovery.exited(0);assertThat(nativeSnapshot()).isEqualTo(before);
        var arguments=new ArrayList<>(List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-jar",repo.resolve("services/content-api/target/content-api-0.1.0-SNAPSHOT.jar").toString(),"--ddip.role=worker","--logging.level.root=OFF","--spring.main.banner-mode=off","--ddip.worker.execution-profile-id="+PROFILE,"--ddip.worker.control-binding-id="+binding.id(),"--ddip.worker.runtime-profile-id="+RUNTIME,"--ddip.worker.deployment-sha256="+owner.worker().deploymentDigest(),"--ddip.worker.python="+python,"--ddip.worker.java-home="+System.getProperty("java.home"),"--ddip.worker.processing-root="+fixture.get("command").get("runtime").get("configuration").get("processingRoot").stringValue()));
        var builder=new ProcessBuilder(arguments).redirectErrorStream(true);builder.environment().putAll(Map.of("DDIP_DB_URL",url,"DDIP_DB_USER",user,"DDIP_DB_PASSWORD",password,"DDIP_INSTALLATION_ID",INSTALL.toString()));var worker=new Child(builder.start());children.add(worker);
        long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(90);while(!"SUCCEEDED".equals(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,unrelated)) && worker.process.isAlive() && System.nanoTime()<end)Thread.sleep(50);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,unrelated)).withFailMessage(worker.output.toString()).isEqualTo("SUCCEEDED");assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
        worker.process.destroy();assertThat(worker.process.waitFor(45,TimeUnit.SECONDS)).isTrue();long workers=jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class);var historical=start("recover");historical.await("COMPLETED");historical.exited(0);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.worker_instances",Long.class)).isEqualTo(workers);
    }
    @ParameterizedTest @ValueSource(strings={"JSONL","PARQUET"}) void actualPackagedWorkerCrashAfterPublicationRecoversWithoutWritingNativeData(String format)throws Exception {
        guard.close();guard=null;queuedSetup=true;
        try {
            jdbc.execute("DROP SCHEMA ddip_management CASCADE");new SchemaLifecycle(org.flywaydb.core.Flyway.configure().dataSource(jdbc.getDataSource()).defaultSchema("ddip_management").schemas("ddip_management").load(),jdbc,new JdbcCoordinatorRepository(jdbc,tx,INSTALL)).verify(true);provision(format);
        }finally{queuedSetup=false;}
        jdbc.execute("CREATE FUNCTION ddip_management.operational_verify_barrier() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.stage='VERIFY_RESULT' THEN PERFORM pg_advisory_xact_lock(1298399); END IF; RETURN NEW; END $$");jdbc.execute("CREATE TRIGGER operational_verify_barrier BEFORE INSERT ON ddip_management.stage_invocations FOR EACH ROW EXECUTE FUNCTION ddip_management.operational_verify_barrier()");
        var identities=new HashMap<Long,RecoveryQuiescence.Identity>();UUID interrupted;
        try(var barrier=jdbc.getDataSource().getConnection();var statement=barrier.createStatement()) {
            statement.execute("SELECT pg_advisory_lock(1298399)");int holder;try(var rs=statement.executeQuery("SELECT pg_backend_pid()")){rs.next();holder=rs.getInt(1);}
            var runtime=fixture.get("command").get("runtime").get("configuration");
            var args=List.of(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-jar",repo.resolve("services/content-api/target/content-api-0.1.0-SNAPSHOT.jar").toString(),"--ddip.role=worker","--logging.level.root=OFF","--spring.main.banner-mode=off","--ddip.worker.execution-profile-id="+PROFILE,"--ddip.worker.control-binding-id="+binding.id(),"--ddip.worker.runtime-profile-id="+RUNTIME,"--ddip.worker.deployment-sha256="+runtime.get("deploymentSha256").stringValue(),"--ddip.worker.python="+python,"--ddip.worker.java-home="+System.getProperty("java.home"),"--ddip.worker.processing-root="+runtime.get("processingRoot").stringValue());
            var builder=new ProcessBuilder(args).redirectErrorStream(true);builder.environment().putAll(Map.of("DDIP_DB_URL",url,"DDIP_DB_USER",user,"DDIP_DB_PASSWORD",password,"DDIP_INSTALLATION_ID",INSTALL.toString()));var worker=new Child(builder.start());children.add(worker);
            boolean waiting=false;long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(180);
            while(worker.process.isAlive() && System.nanoTime()<end) {
                worker.process.descendants().forEach(p->p.info().startInstant().ifPresent(at->identities.put(p.pid(),new RecoveryQuiescence.Identity(p.pid(),at))));
                waiting=Boolean.TRUE.equals(jdbc.queryForObject("SELECT EXISTS(SELECT 1 FROM pg_locks l JOIN pg_stat_activity a ON a.pid=l.pid WHERE a.datname=current_database() AND a.wait_event_type='Lock' AND l.locktype='advisory' AND NOT l.granted AND l.classid=0 AND l.objid=1298399 AND l.objsubid=1 AND ?=ANY(pg_blocking_pids(l.pid)))",Boolean.class,holder));if(waiting)break;Thread.sleep(20);
            }
            assertThat(waiting).withFailMessage(worker.output.toString()).isTrue();interrupted=jdbc.queryForObject("SELECT current_attempt_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run);worker.close();statement.execute("SELECT pg_advisory_unlock(1298399)");
        }
        jdbc.execute("DROP TRIGGER operational_verify_barrier ON ddip_management.stage_invocations");jdbc.execute("DROP FUNCTION ddip_management.operational_verify_barrier()");
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='PUBLISH_RELIABILITY'",String.class,interrupted)).isEqualTo("VERIFIED");
        for(var identity:identities.values())assertThat(RecoveryQuiescence.LOCAL.inspect(identity)).isIn(RecoveryQuiescence.State.ABSENT,RecoveryQuiescence.State.REUSED);
        var before=nativeSnapshot();control=request(interrupted,"actual-packaged-worker-crash");var recovery=start("recover");
        for(String phase:List.of("HANDOFF","FINALIZATION")) {
            var prompt=recovery.await("CONFIRMATION_REQUIRED");assertThat(prompt.get("phase").stringValue()).isEqualTo(phase);var reply=answer(prompt);var descendants=reply.putArray("descendants");identities.values().stream().sorted(Comparator.comparingLong(RecoveryQuiescence.Identity::pid)).forEach(p->descendants.addObject().put("pid",Long.toString(p.pid())).put("startedAt",p.startedAt().toString()));
            for(var hint:prompt.get("evidence").get("descendantPidHints"))assertThat(identities).containsKey(Long.parseLong(hint.stringValue()));recovery.send(reply);if(phase.equals("HANDOFF"))recovery.await("OBSERVED");
        }
        assertThat(recovery.await("COMPLETED").get("runState").stringValue()).isEqualTo("SUCCEEDED");recovery.exited(0);assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,interrupted)).isEqualTo("INTERRUPTED");assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void crashAfterHandoffCannotBeAdoptedByRepeatedCommand()throws Exception {
        admit();var before=nativeSnapshot();var first=start("recover");first.send(answer(first.await("CONFIRMATION_REQUIRED")));first.await("OBSERVED");first.await("CONFIRMATION_REQUIRED");first.close();unresolved(control);assertThat(state()).isEqualTo("RECOVERY_REQUIRED");
        var repeat=start("recover");assertThat(repeat.await("RECOVERY_REQUIRED").get("code").stringValue()).isEqualTo("FRESH_QUALIFICATION_REQUIRED");repeat.exited(2);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(2);control=request(control,"fresh-control-after-crash");assertThat(recover().get("runState").stringValue()).isEqualTo("SUCCEEDED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(3);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @Test void crashBeforeHandoffAllowsFreshInspectionOfSameControl()throws Exception {
        admit();var before=nativeSnapshot();var first=start("recover");first.await("CONFIRMATION_REQUIRED");first.close();unresolved(owner.attemptId());assertThat(recover().get("runState").stringValue()).isEqualTo("SUCCEEDED");assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(2);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    @ParameterizedTest @ValueSource(strings={"packet","originalEvidence","lineageApproval"}) void recoveryChainCannotBypassOriginalProvenanceOrReview(String mode)throws Exception {
        admit();var first=start("recover");first.send(answer(first.await("CONFIRMATION_REQUIRED")));first.await("OBSERVED");first.await("CONFIRMATION_REQUIRED");first.close();UUID occupied=control;control=request(control,"chain-needs-evidence");var before=nativeSnapshot();
        if(mode.equals("packet")){var d=SourceJson.parse(jdbc.queryForObject("SELECT details_bytes FROM ddip_management.audit_events WHERE control_id=? AND action='RECOVERY_HANDOFF'",byte[].class,occupied));Files.writeString(binding.root().resolve(d.get("qualificationAddress").stringValue())," ",StandardOpenOption.APPEND);}
        if(mode.equals("originalEvidence")){UUID checkpoint=jdbc.queryForObject("SELECT checkpoint_id FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='CHECK_INPUTS'",UUID.class,owner.attemptId());Files.delete(binding.root().resolve("runs/"+run+"/attempts/"+owner.attemptId()+"/check-inputs/"+checkpoint+"/verified.json"));}
        var next=start("recover");if(mode.equals("lineageApproval")){var prompt=next.await("CONFIRMATION_REQUIRED");assertThat(prompt.get("evidence").get("lineageCheckpoints").size()).isPositive();var reply=answer(prompt);reply.putArray("lineageCheckpoints");next.send(reply);}next.await("RECOVERY_REQUIRED");next.exited(2);unresolved(occupied);assertThat(jdbc.queryForObject("SELECT count(*) FROM ddip_management.attempts",Long.class)).isEqualTo(2);assertThat(nativeSnapshot()).isEqualTo(before);
    }
    static final class Child implements AutoCloseable {
        final Process process;final BlockingQueue<String> lines=new LinkedBlockingQueue<>();final List<String> output=new CopyOnWriteArrayList<>();
        Child(Process process){this.process=process;Thread.ofPlatform().daemon().start(()->{try(var reader=process.inputReader()){String line;while((line=reader.readLine())!=null){output.add(line);lines.add(line);}}catch(IOException ignored){}});}
        JsonNode await(String status)throws Exception {return await(Set.of(status));}
        JsonNode await(Set<String> statuses)throws Exception {long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(40);while(System.nanoTime()<end){String line=lines.poll(100,TimeUnit.MILLISECONDS);if(line==null){if(!process.isAlive() && lines.isEmpty())break;continue;}assertThat(line).startsWith("{").doesNotContain("jdbc:","PRIVATE", "Exception", "command.json", "retained.json");var n=SourceJson.parse(line.getBytes(StandardCharsets.UTF_8),1024*1024);if(statuses.contains(n.get("status").stringValue()))return n;}throw new AssertionError("Missing "+statuses+" in "+output);}
        void send(JsonNode n)throws Exception {process.getOutputStream().write(SourceJson.canonical(n));process.getOutputStream().write('\n');process.getOutputStream().flush();}
        void exited(int code)throws Exception {assertThat(process.waitFor(15,TimeUnit.SECONDS)).isTrue();assertThat(process.exitValue()).withFailMessage(output.toString()).isEqualTo(code);}
        public void close(){process.destroyForcibly();try{process.waitFor(10,TimeUnit.SECONDS);}catch(InterruptedException ex){Thread.currentThread().interrupt();}}
    }
}
