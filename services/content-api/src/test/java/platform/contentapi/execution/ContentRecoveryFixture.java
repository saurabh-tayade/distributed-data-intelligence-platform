package platform.contentapi.execution;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.*;
import org.junit.jupiter.api.io.TempDir;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** One real D1-D5a fixture; deterministic failure injection reuses its immutable final publication. */
abstract class ContentRecoveryFixture {
    boolean boundedEvidence;
    @TempDir(cleanup=org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path temp;
    String phase="start";
    void recover(String mode)throws Exception{
        String expected=mode.equals("FINAL")?"SUCCEEDED":(mode.equals("PARTIAL_CONTENT") || mode.equals("CONTENT_INTENT"))?"FAILED":"PAUSED";
        long start=System.nanoTime();String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",cp,"platform.contentapi.management.ContentRecoveryTestProcess",temp.toString(),mode,boundedEvidence?"BOUNDED":"LEGACY").redirectError(temp.resolve("worker.log").toFile()).redirectOutput(temp.resolve("worker.out").toFile()).start();
        JdbcTemplate j=null;InstallationHostGuard guard=null;String database=null;UUID run=null;
        try{
            Map<Long,Instant> observedDescendants=new HashMap<>();Path info=temp.resolve("owner.json");long end=System.nanoTime()+TimeUnit.MINUTES.toNanos(8);while(!Files.exists(info) && child.isAlive() && System.nanoTime()<end){var live=child.descendants().limit(33).toList();assertThat(live.size()).isLessThanOrEqualTo(32);for(var process:live)process.info().startInstant().ifPresent(at->observedDescendants.putIfAbsent(process.pid(),at));assertThat(observedDescendants.size()).isLessThanOrEqualTo(128);Thread.sleep(100);}
            assertThat(Files.exists(info)).withFailMessage("Worker failed: %s",Files.readString(temp.resolve("worker.log"))).isTrue();var n=SourceJson.parse(Files.readAllBytes(info));database=n.get("database").stringValue();run=UUID.fromString(n.get("runId").stringValue());
            j=new JdbcTemplate(new DriverManagerDataSource(n.get("url").stringValue(),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));var tx=ReliabilityExecutionFixtures.transactions(j);var binding=OwnershipFixtures.readBinding(writeBinding(n.get("binding")));
            UUID original=j.queryForObject("SELECT current_attempt_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run);long version=j.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
            var command=ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RECONCILE").put("attemptId",original.toString()).put("quiescenceConfirmed",true)),"content-recovery","\""+version+"\"");
            var controls=new ControlRepository(j,tx,binding.installationId());assertThatThrownBy(()->controls.request(command)).hasMessage("STATE_CONFLICT");assertThatThrownBy(()->InstallationHostGuard.acquire(binding)).hasMessage("HOST_GUARD_OCCUPIED");child.getOutputStream().close();assertThat(child.waitFor(20,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isZero();UUID control=UUID.fromString(controls.request(command).get("controlId").stringValue());assertThat(controls.request(command).get("controlId").stringValue()).isEqualTo(control.toString());
            var operatorSummary=new RecoveryOperatorInspection(j,tx,binding.installationId()).inspect(control);assertThat(operatorSummary.get("canQualify").booleanValue()).isTrue();
            phase="register";guard=InstallationHostGuard.acquire(binding);var frozen=SourceJson.parse(j.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run),PreflightFiles.LARGE);var profile=frozen.get("executionProfile");
            var workers=WorkerTestSupport.enabled(j,tx,binding.installationId());var session=workers.register(guard,n.get("profile").stringValue(),profile.get("configurationSha256").stringValue(),profile.get("configuration").get("runtime").get("deploymentSha256").stringValue());
            var h=new RecoveryHandoff(j,tx,binding.installationId());var target=h.inspect(guard,session,control);phase="review-evidence";
            var evidence=h.reviewEvidence(guard,target,n.get("runtime").stringValue());var descendants=descendants(binding.root(),evidence,observedDescendants);
            var review=new RecoveryQuiescence.Review(target.digest(),Instant.now(),true,true,true,target.unresolvedCheckpoints(),descendants);
            // An independently unobservable original identity vetoes release despite operator attestation.
            var ambiguous=new RecoveryHandoff(j,tx,binding.installationId(),Clock.systemUTC(),p->RecoveryQuiescence.State.UNAVAILABLE,s->{});var heldGuard=guard;var initialReview=review;
            assertThatThrownBy(()->ambiguous.qualify(heldGuard,target,initialReview,n.get("runtime").stringValue())).hasMessage("PROCESS_UNRESOLVED");
            assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(original);
            if(mode.equals("CONTENT_INTENT"))assertThat(j.queryForObject("SELECT state FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='PUBLISH_CONTENT'",String.class,original)).isEqualTo("INTENT");
            phase="qualify-handoff";var q=h.qualify(guard,target,review,n.get("runtime").stringValue());assertThat(h.transferReviewed(guard,q).owner()).isNotNull();assertThat(h.transferReviewed(guard,q).owner()).isNull();
            phase="bootstrap";var bootstrap=h.bootstrap(guard,q.address(),q.sha256(),n.get("runtime").stringValue());var recovery=new ContentRecovery(j,tx,h);
            var before=snapshot(outputRoot(frozen));
            if(mode.equals("FINAL")){String signal=System.getProperty("ddip.test.content-recovery-restart-signal");if(signal!=null){Files.writeString(Path.of(signal),"RESTART\n");end=System.nanoTime()+TimeUnit.SECONDS.toNanos(60);while(!Files.exists(Path.of(signal+".done")) && System.nanoTime()<end)Thread.sleep(50);assertThat(Files.exists(Path.of(signal+".done"))).isTrue();}
            else{String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).as("Exact disposable PostgreSQL container is required").matches("[0-9a-f]{12,64}");var restart=new ProcessBuilder("docker","restart",container).redirectError(ProcessBuilder.Redirect.INHERIT).start();assertThat(restart.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(restart.exitValue()).isZero();long ready=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(true){try{j.queryForObject("SELECT 1",Integer.class);break;}catch(org.springframework.dao.DataAccessException ex){if(System.nanoTime()>ready)throw ex;Thread.sleep(100);}}}
            }
            // Refresh the explicit process review after the infrastructure interruption.
            review=new RecoveryQuiescence.Review(target.digest(),Instant.now(),true,true,true,target.unresolvedCheckpoints(),descendants);
            if(mode.equals("PARTIAL_SILVER")){
                var currentGuard=guard;var currentReview=review;var originalFiles=ContentRecoveryFiles.capture(guard,run,original,j.queryForList("SELECT * FROM ddip_management.stage_invocations WHERE attempt_id=?",original)).inventory();
                assertThatThrownBy(()->recovery.qualify(currentGuard,bootstrap,currentReview)).hasMessage("EVIDENCE_UNAVAILABLE");assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(control);assertThat(j.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("RECOVERY_REQUIRED");
                var resumeCommand=ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RESUME").putNull("attemptId").putNull("quiescenceConfirmed")),"partial-resume","\""+j.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run)+"\"");assertThatThrownBy(()->controls.request(resumeCommand)).hasMessage("COORDINATOR_RECOVERY_REQUIRED");assertThat(ContentRecoveryFiles.capture(guard,run,original,j.queryForList("SELECT * FROM ddip_management.stage_invocations WHERE attempt_id=?",original)).inventory()).isEqualTo(originalFiles);System.out.println("D5B2A_BLOCKED partialSilver=1 selectedSources=2 occupied="+control);return;
            }
            phase="qualify-content";var qualified=recovery.qualify(guard,bootstrap,review);assertThat(qualified.observation.get("state").stringValue()).isEqualTo(expected);
            if(mode.equals("CONTENT_INTENT")){
                assertThat(qualified.observation.get("code").stringValue()).isEqualTo("PUBLICATION_ABSENT");
                var intent=j.queryForMap("SELECT state,intended_namespace,command_address,command_sha256,child_pid FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='PUBLISH_CONTENT'",original);
                assertThat(intent.get("state")).isEqualTo("UNKNOWN");assertThat(intent.get("child_pid")).isNull();
                assertThat(intent.get("command_address")).isEqualTo(intent.get("intended_namespace")+"/command.json");
                assertThat(SourceJson.sha(Files.readAllBytes(binding.root().resolve((String)intent.get("command_address"))))).isEqualTo(intent.get("command_sha256"));
                assertThat(before).containsOnlyKeys("untouched.native");
            }
            if(mode.equals("PARTIAL_CONTENT")){
                assertThat(qualified.observation.get("code").stringValue()).isEqualTo("INCOMPLETE_OUTPUT");
                assertThat(before).hasSize(2);assertThat(before).containsKey("untouched.native");assertThat(before.keySet()).anySatisfy(path->assertThat(path).endsWith("interrupted-part.bin"));
                assertThat(j.queryForObject("SELECT child_pid FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='PUBLISH_CONTENT'",Long.class,original)).isNull();
            }
            var finalReview=new RecoveryQuiescence.Review(target.digest(),Instant.now(),true,true,true,target.unresolvedCheckpoints(),descendants);
            ContentRecovery.Receipt result;
            if(mode.equals("FINAL")){
            // Deterministic terminal faults reuse one real qualified publication, without rerunning Spark.
            for(String table:List.of("artifacts","artifact_dependencies","stage_artifacts","attempts","runs","controls","worker_instances","coordinator","audit_events")){
                j.execute("CREATE OR REPLACE FUNCTION ddip_management.d5b_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");
                j.execute("CREATE TRIGGER d5b_fault BEFORE INSERT OR UPDATE ON ddip_management."+table+" FOR EACH ROW EXECUTE FUNCTION ddip_management.d5b_fault()");
                var proof=qualified;assertThatThrownBy(()->applyTransaction(recovery,h,heldGuard,proof,finalReview)).hasMessage("STATE_CONFLICT");
                assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(control);
                assertThat(j.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='CONTENT'",Integer.class)).isZero();
                assertThat(j.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,control)).isEqualTo("REQUESTED");
                j.execute("DROP TRIGGER d5b_fault ON ddip_management."+table);
            }
            // SQL state-contract probes are rolled back; they do not claim earlier native-stage recovery.
            for(String disposition:List.of("PAUSED","FAILED","CANCELLED")){
                var decision=(tools.jackson.databind.node.ObjectNode)qualified.observation.deepCopy();decision.put("state",disposition).put("code",disposition.equals("PAUSED")?"RECOVERED_CHECKPOINT":disposition.equals("FAILED")?"INCOMPLETE_OUTPUT":"CANCELLED");
                var proof=new ContentRecovery.Qualified(qualified.bootstrap,qualified.pack,decision,qualified.owner,qualified.children,qualified.at,qualified.privateSeal,qualified.observationAddress,qualified.observationFile);var databaseForProbe=j;
                assertThatThrownBy(()->h.fenced(heldGuard,proof.owner,()->{try{rawApply(recovery,heldGuard,proof,finalReview);}catch(Exception ex){throw new AssertionError(ex);}assertThat(databaseForProbe.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,proof.owner.runId())).isEqualTo(disposition);assertThat(databaseForProbe.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();throw new IllegalStateException("ROLLBACK_PROBE");})).hasMessage("ROLLBACK_PROBE");
                assertThat(j.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,proof.owner.runId())).isEqualTo("RECOVERY_REQUIRED");
            }
            // Requested cancellation loses to the independently verified completed publication.
            UUID cancellation=UUID.randomUUID();byte[] cancelBytes=SourceJson.canonical(SourceJson.object().put("action","CANCEL"));
            j.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','late-cancel',?,?,?,'REQUESTED','local-operator')",cancellation,run,cancelBytes,SourceJson.sha(cancelBytes),qualified.owner.runVersion());
            phase="finish";
            try(var proxy=new RecoveryAcknowledgmentProxy(n.get("url").stringValue(),"RECOVERY_FINALIZED")){
                var lost=new JdbcTemplate(new DriverManagerDataSource(proxy.url(),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));var lostTx=ReliabilityExecutionFixtures.transactions(lost);
                var finalizer=new ContentRecovery(lost,lostTx,new RecoveryHandoff(lost,lostTx,binding.installationId()));var proof=qualified;
                assertThatThrownBy(()->finalizer.finish(heldGuard,proof,finalReview)).hasMessage("MANAGEMENT_UNAVAILABLE");assertThat(proxy.dropped.get()).isTrue();
                result=recovery.resolve(guard,control);
            }
            assertThat(j.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,cancellation)).isEqualTo("TOO_LATE");
            }else result=recovery.finish(guard,qualified,finalReview);
            assertThat(result.state()).isEqualTo(expected);assertThat(recovery.finish(guard,qualified,finalReview)).isEqualTo(result);assertThat(snapshot(outputRoot(frozen))).isEqualTo(before);
            if(mode.equals("PARTIAL_CONTENT"))assertThat(result.retryEligible()).isFalse();
            assertThat(j.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,original)).isEqualTo("INTERRUPTED");assertThat(j.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='CONTENT'",Integer.class)).isEqualTo(mode.equals("FINAL")?1:0);assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
            afterRecovery(mode,n,j,tx,binding,guard,control,original,frozen);
            // A genuinely fresh run is admitted through the normal repository after safe release.
            var request=n.get("nextRequest");var catalog=PreflightCatalogFixture.from(SourceJson.parse(Files.readAllBytes(temp.resolve("catalog.json"))));
            var admitted=new RunRepository(j,tx,binding.installationId(),catalog).submit(RunCommands.parse(SourceJson.canonical(request),"after-content-recovery"),UUID.randomUUID().toString());assertThat(admitted.get("runId").stringValue()).isNotEqualTo(run.toString());
            System.out.println("D5B1_RECOVERY checkpoint="+mode+" state="+result.state()+" original="+original+" result="+result.artifact()+" immutableFiles="+before.size()+" nextRun="+admitted.get("runId")+" seconds="+Duration.ofNanos(System.nanoTime()-start).toMillis()/1000.0);
        }catch(Throwable ex){if(j!=null)System.out.println("RECOVERY_DIAGNOSTIC stages="+j.queryForList("SELECT a.kind,s.stage,s.item_ordinal,s.state FROM ddip_management.stage_invocations s JOIN ddip_management.attempts a USING(attempt_id) ORDER BY a.attempt_no,s.item_ordinal,s.stage")+" temp="+temp);throw new AssertionError("Recovery phase="+phase+" code="+ex.getMessage(),ex);}finally{child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);if(guard!=null)guard.close();if(database!=null){var admin=new JdbcTemplate(new DriverManagerDataSource(System.getenv("DDIP_TEST_DB_URL"),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));admin.execute("DROP DATABASE "+database+" WITH (FORCE)");}}
    }
    void afterRecovery(String mode,JsonNode n,JdbcTemplate j,ManagementTransactions tx,InstallationHostGuard.Binding binding,InstallationHostGuard guard,UUID reconcile,UUID original,JsonNode frozen)throws Exception{}
    static Object applyTransaction(ContentRecovery service,RecoveryHandoff handoffs,InstallationHostGuard guard,ContentRecovery.Qualified q,RecoveryQuiescence.Review review){return handoffs.fenced(guard,q.owner,()->rawApply(service,guard,q,review));}
    static Object rawApply(ContentRecovery service,InstallationHostGuard guard,ContentRecovery.Qualified q,RecoveryQuiescence.Review review){
        try{var method=ContentRecovery.class.getDeclaredMethod("apply",InstallationHostGuard.class,JsonNode.class,JsonNode.class,RecoveryHandoff.Owner.class,RecoveryQuiescence.Review.class,String.class);method.setAccessible(true);return method.invoke(service,guard,q.pack,q.observation,q.owner,review,q.observationAddress);}
        catch(java.lang.reflect.InvocationTargetException ex){if(ex.getCause() instanceof RuntimeException cause)throw cause;throw new AssertionError(ex.getCause());}catch(ReflectiveOperationException ex){throw new AssertionError(ex);}
    }
    Path writeBinding(JsonNode b)throws Exception{Path p=temp.resolve("binding.json");Files.write(p,SourceJson.canonical(b));return p;}
    static Path outputRoot(JsonNode f){for(var b:f.get("storageBindings"))if(b.get("role").stringValue().equals("CONTENT"))return Path.of(b.get("configuration").get("root").stringValue());throw new AssertionError();}
    static Map<String,String> snapshot(Path root)throws Exception{var result=new TreeMap<String,String>();try(var walk=Files.walk(root)){for(Path p:walk.filter(Files::isRegularFile).toList())result.put(root.relativize(p).toString(),SourceJson.sha(Files.readAllBytes(p))+":"+Files.getLastModifiedTime(p)+":"+Files.getAttribute(p,"unix:ino"));}return result;}
    static List<RecoveryQuiescence.Identity> descendants(Path root,JsonNode evidence,Map<Long,Instant> observed)throws Exception{var result=new ArrayList<RecoveryQuiescence.Identity>();for(var pid:evidence.get("descendantPidHints")){long value=Long.parseLong(pid.stringValue());if(observed.containsKey(value)){result.add(new RecoveryQuiescence.Identity(value,observed.get(value)));continue;}try(var walk=Files.walk(root)){for(Path p:walk.filter(p->p.getFileName().toString().equals("child.json")).toList()){var n=SourceJson.parse(Files.readAllBytes(p));if(Long.parseLong(n.get("pid").stringValue())==value)result.add(new RecoveryQuiescence.Identity(value,Instant.parse(n.get("startedAt").stringValue())));}}}return result.stream().distinct().toList();}
}
