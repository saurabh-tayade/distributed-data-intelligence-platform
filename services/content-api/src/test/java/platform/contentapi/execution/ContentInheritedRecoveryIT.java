package platform.contentapi.execution;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Production native adapters and C3c recovery, with actual original and resumed worker JVMs. */
class ContentInheritedRecoveryIT extends ContentRecoveryFixture {
    String stop;
    @Test void inheritedGoldFinalResultAndSafetyVetoes()throws Exception{stop="PUBLISH_CONTENT";recover("GOLD");}
    @Test void inheritedContentInputsFinalResult()throws Exception{stop="PUBLISH_CONTENT";recover("CHECK_INPUTS");}
    @Test void inheritedGoldBeforeNewStagesPauses()throws Exception{stop="RESUME";recover("GOLD");}
    @Test void inheritedContentInputsBeforePublicationPauses()throws Exception{stop="RESUME";recover("CHECK_INPUTS");}
    @Test void inheritedBeforeFetchPausesWithoutRetrieval()throws Exception{stop="RESUME";recover("BEFORE_FETCH");}

    @Override void afterRecovery(String mode,JsonNode n,JdbcTemplate j,ManagementTransactions tx,InstallationHostGuard.Binding binding,InstallationHostGuard guard,UUID reconcile,UUID original,JsonNode frozen)throws Exception{
        UUID run=UUID.fromString(n.get("runId").stringValue());String runtime=n.get("runtime").stringValue();long version=j.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
        var controls=new ControlRepository(j,tx,binding.installationId());var request=ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RESUME").putNull("attemptId").putNull("quiescenceConfirmed")),"inherited-resume","\""+version+"\"");
        UUID resume=UUID.fromString(controls.request(request).get("controlId").stringValue());var service=new ContentResume(j,tx,binding.installationId());var plan=service.requeue(guard,resume,runtime);
        assertThat(service.requeue(guard,resume,runtime)).isEqualTo(plan);
        var historical=j.queryForList("SELECT to_jsonb(s)::text FROM ddip_management.stage_invocations s WHERE attempt_id=? ORDER BY stage,item_ordinal",original);
        var deliveries=j.queryForList("SELECT to_jsonb(d)::text FROM ddip_management.rss_deliveries d WHERE run_id=? ORDER BY source_id",run);
        var nativeBefore=new TreeMap<String,Map<String,String>>();for(var b:frozen.get("storageBindings"))if(Set.of("SILVER","GOLD").contains(b.get("role").stringValue()))nativeBefore.put(b.get("role").stringValue(),snapshot(Path.of(b.get("configuration").get("root").stringValue())));
        var privateBefore=ContentRecoveryFiles.capture(guard,run,original,j.queryForList("SELECT * FROM ddip_management.stage_invocations WHERE attempt_id=?",original)).inventory();guard.close();
        String cp=System.getProperty("surefire.test.class.path",System.getProperty("java.class.path"));
        var child=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-cp",cp,ContentInheritedRecoveryProcess.class.getName(),temp.toString(),resume.toString(),stop).redirectOutput(temp.resolve("resumed.out").toFile()).redirectError(temp.resolve("resumed.log").toFile()).start();
        try{
            phase="resumed-process";long end=System.nanoTime()+TimeUnit.MINUTES.toNanos(6);var observed=new HashMap<Long,Instant>();
            while(!Files.exists(temp.resolve("resumed-ready")) && child.isAlive() && System.nanoTime()<end){for(var p:child.descendants().toList())p.info().startInstant().ifPresent(at->observed.putIfAbsent(p.pid(),at));Thread.sleep(100);}
            assertThat(Files.exists(temp.resolve("resumed-ready"))).withFailMessage("Resumed process failed: %s",Files.readString(temp.resolve("resumed.log"))).isTrue();
            UUID interrupted=j.queryForObject("SELECT current_attempt_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run);assertThat(interrupted).isNotIn(original,reconcile);
            var recoveryRequest=ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RECONCILE").put("attemptId",interrupted.toString()).put("quiescenceConfirmed",true)),"inherited-reconcile","\""+j.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run)+"\"");
            assertThatThrownBy(()->controls.request(recoveryRequest)).hasMessage("STATE_CONFLICT");assertThatThrownBy(()->InstallationHostGuard.acquire(binding)).hasMessage("HOST_GUARD_OCCUPIED");
            child.getOutputStream().close();assertThat(child.waitFor(20,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isZero();
            if(mode.equals("GOLD") && stop.equals("PUBLISH_CONTENT")){
                var before=j.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).matches("[0-9a-f]{12,64}");
                var restart=new ProcessBuilder("docker","restart",container).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();assertThat(restart.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(restart.exitValue()).isZero();end=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);
                while(true){try{var after=j.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);assertThat(after).isAfter(before);System.out.println("D5B2B_POSTGRES_RESTART before="+before+" after="+after+" occupied="+interrupted);break;}catch(org.springframework.dao.DataAccessException ex){if(System.nanoTime()>end)throw ex;Thread.sleep(100);}}
                assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(interrupted);
            }
            UUID control=UUID.fromString(controls.request(recoveryRequest).get("controlId").stringValue());assertThat(controls.request(recoveryRequest).get("controlId").stringValue()).isEqualTo(control.toString());
            try(var fresh=InstallationHostGuard.acquire(binding)){
                var p=frozen.get("executionProfile");var workers=WorkerTestSupport.enabled(j,tx,binding.installationId());var session=workers.register(fresh,p.get("profileId").stringValue(),p.get("configurationSha256").stringValue(),p.get("configuration").get("runtime").get("deploymentSha256").stringValue());assertThat(session.status()).isEqualTo("RECOVERY_ONLY");assertThatThrownBy(()->workers.claim(fresh,session,UUID.randomUUID())).hasMessage("WORKER_NOT_READY");
                var h=new RecoveryHandoff(j,tx,binding.installationId());var target=h.inspect(fresh,session,control);var reviewEvidence=h.reviewEvidence(fresh,target,runtime);assertThat(reviewEvidence.get("inheritedCheckpoint").get("planSha256").stringValue()).isEqualTo(SourceJson.sha(SourceJson.canonical(plan)));
                var descendants=new ArrayList<>(descendants(binding.root(),reviewEvidence,observed));for(var d:reviewEvidence.get("previouslyReviewedDescendants")){var id=new RecoveryQuiescence.Identity(Long.parseLong(d.get("pid").stringValue()),Instant.parse(d.get("startedAt").stringValue()));if(!descendants.contains(id))descendants.add(id);}
                var review=new RecoveryQuiescence.Review(target.digest(),Instant.now(),true,true,true,target.unresolvedCheckpoints(),descendants);
                phase="inherited-vetoes";
                var contentRuntime=RssContentRuntime.load(j,tx,fresh,run,frozen,runtime);
                for(var state:List.of(RecoveryQuiescence.State.LIVE,RecoveryQuiescence.State.UNAVAILABLE)){var ancestry=new ContentRecoveryAncestry(j,tx,id->state,Clock.systemUTC());assertThatThrownBy(()->ancestry.inspect(fresh,run,interrupted,contentRuntime,review)).hasMessage("EVIDENCE_UNAVAILABLE");}
                Path retained=binding.root().resolve(plan.get("verificationAddress").stringValue());byte[] saved=Files.readAllBytes(retained);Files.write(retained,new byte[]{1});assertThatThrownBy(()->h.qualify(fresh,target,review,runtime)).hasMessage("EVIDENCE_CHANGED");Files.write(retained,saved);
                Path missing=retained.resolveSibling("held-evidence.json");Files.move(retained,missing);try{assertThatThrownBy(()->h.qualify(fresh,target,review,runtime)).hasMessage("EVIDENCE_UNAVAILABLE");}finally{Files.move(missing,retained);}
                if(!mode.equals("BEFORE_FETCH")){
                    Path commandFile=binding.root().resolve(j.queryForObject("SELECT command_address FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='FETCH_SOURCE' AND item_ordinal=0",String.class,original));byte[] bytes=Files.readAllBytes(commandFile);var modified=Files.getLastModifiedTime(commandFile);
                    Files.write(commandFile,new byte[]{1});try{assertThatThrownBy(()->h.qualify(fresh,target,review,runtime)).hasMessage("EVIDENCE_CHANGED");}finally{Files.write(commandFile,bytes);Files.setLastModifiedTime(commandFile,modified);}
                }
                assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(interrupted);assertThat(j.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='CONTENT'",Integer.class)).isZero();
                UUID checkpoint=j.queryForObject("SELECT reused_checkpoint_id FROM ddip_management.attempts WHERE attempt_id=?",UUID.class,interrupted);
                assertThatThrownBy(()->j.update("UPDATE ddip_management.attempts SET reused_checkpoint_id=NULL WHERE attempt_id=?",interrupted)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);
                assertThat(j.queryForObject("SELECT reused_checkpoint_id FROM ddip_management.attempts WHERE attempt_id=?",UUID.class,interrupted)).isEqualTo(checkpoint);
                phase="inherited-handoff";var qualified=h.qualify(fresh,target,review,runtime);assertThat(h.transferReviewed(fresh,qualified).owner()).isNotNull();var bootstrap=h.bootstrap(fresh,qualified.address(),qualified.sha256(),runtime);var recovery=new ContentRecovery(j,tx,h);
                var contentFiles=snapshot(outputRoot(frozen));var newHistory=j.queryForList("SELECT to_jsonb(s)::text FROM ddip_management.stage_invocations s WHERE attempt_id=? ORDER BY stage,item_ordinal",interrupted);
                phase="inherited-observation";var proof=recovery.qualify(fresh,bootstrap,new RecoveryQuiescence.Review(target.digest(),Instant.now(),true,true,true,target.unresolvedCheckpoints(),descendants));
                String expected=stop.equals("RESUME")?"PAUSED":"SUCCEEDED";assertThat(proof.observation.get("state").stringValue()).isEqualTo(expected);
                phase="inherited-finish";var finalReview=new RecoveryQuiescence.Review(target.digest(),Instant.now(),true,true,true,target.unresolvedCheckpoints(),descendants);
                if(expected.equals("SUCCEEDED"))for(String table:List.of("artifacts","artifact_dependencies","stage_artifacts","attempts","runs","controls","worker_instances","coordinator","audit_events")){
                    j.execute("CREATE OR REPLACE FUNCTION ddip_management.inherited_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");j.execute("CREATE TRIGGER inherited_fault BEFORE INSERT OR UPDATE ON ddip_management."+table+" FOR EACH ROW EXECUTE FUNCTION ddip_management.inherited_fault()");
                    try{assertThatThrownBy(()->applyTransaction(recovery,h,fresh,proof,finalReview)).hasMessage("STATE_CONFLICT");}finally{j.execute("DROP TRIGGER inherited_fault ON ddip_management."+table);}
                    assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(control);assertThat(j.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='CONTENT'",Integer.class)).isZero();assertThat(j.queryForObject("SELECT state FROM ddip_management.controls WHERE control_id=?",String.class,control)).isEqualTo("REQUESTED");assertThat(snapshot(outputRoot(frozen))).isEqualTo(contentFiles);
                }
                ContentRecovery.Receipt result;
                if(mode.equals("GOLD") && expected.equals("SUCCEEDED"))try(var proxy=new RecoveryAcknowledgmentProxy(n.get("url").stringValue(),"RECOVERY_FINALIZED")){
                    var lost=new JdbcTemplate(new org.springframework.jdbc.datasource.DriverManagerDataSource(proxy.url(),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));var lostTx=ReliabilityExecutionFixtures.transactions(lost);var finalizer=new ContentRecovery(lost,lostTx,new RecoveryHandoff(lost,lostTx,binding.installationId()));
                    assertThatThrownBy(()->finalizer.finish(fresh,proof,finalReview)).hasMessage("MANAGEMENT_UNAVAILABLE");assertThat(proxy.dropped.get()).isTrue();result=recovery.resolve(fresh,control);
                }else result=recovery.finish(fresh,proof,finalReview);
                assertThat(result.state()).isEqualTo(expected);assertThat(recovery.finish(fresh,proof,finalReview)).isEqualTo(result);
                assertThat(snapshot(outputRoot(frozen))).isEqualTo(contentFiles);assertThat(j.queryForList("SELECT to_jsonb(s)::text FROM ddip_management.stage_invocations s WHERE attempt_id=? ORDER BY stage,item_ordinal",original)).isEqualTo(historical);assertThat(j.queryForList("SELECT to_jsonb(s)::text FROM ddip_management.stage_invocations s WHERE attempt_id=? ORDER BY stage,item_ordinal",interrupted)).isEqualTo(newHistory);
                assertThat(ContentRecoveryFiles.capture(fresh,run,original,j.queryForList("SELECT * FROM ddip_management.stage_invocations WHERE attempt_id=?",original)).inventory()).isEqualTo(privateBefore);
                assertThat(j.queryForList("SELECT to_jsonb(d)::text FROM ddip_management.rss_deliveries d WHERE run_id=? ORDER BY source_id",run)).isEqualTo(deliveries);
                for(var b:frozen.get("storageBindings"))if(nativeBefore.containsKey(b.get("role").stringValue()))assertThat(snapshot(Path.of(b.get("configuration").get("root").stringValue()))).isEqualTo(nativeBefore.get(b.get("role").stringValue()));
                assertThat(j.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,interrupted)).isEqualTo("INTERRUPTED");assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
                if(expected.equals("PAUSED")){
                    var again=ControlCommands.parse(run.toString(),SourceJson.canonical(SourceJson.object().put("action","RESUME").putNull("attemptId").putNull("quiescenceConfirmed")),"nested-resume","\""+j.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run)+"\"");
                    assertThatThrownBy(()->controls.request(again)).hasMessage("RESUME_CHECKPOINT_UNSUPPORTED");
                }
                assertThat(j.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage IN ('FETCH_SOURCE','STORE_BRONZE','SILVER','PUBLISH_GOLD')",Integer.class,interrupted)).isZero();
                if(result.artifact()!=null){UUID gold=j.queryForObject("SELECT artifact_id FROM ddip_management.stage_artifacts WHERE attempt_id=? AND stage='PUBLISH_GOLD' AND role='RESULT'",UUID.class,original);assertThat(j.queryForObject("SELECT dependency_artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? AND role='GOLD'",UUID.class,result.artifact())).isEqualTo(gold);var artifact=SourceJson.parse(j.queryForObject("SELECT evidence_bytes FROM ddip_management.artifacts WHERE artifact_id=?",byte[].class,result.artifact()),PreflightFiles.LARGE);assertThat(artifact.get("verification").get("completion").get("output_counts")).isEqualTo(SourceJson.object().put("content_catalog_v1",frozen.get("sources").size()).put("content_source_discovery_daily_v1",frozen.get("sources").size()));}
                System.out.println("D5B2B_INHERITED boundary="+mode+" stop="+stop+" original="+original+" resume="+interrupted+" reconciliation="+control+" state="+result.state()+" artifact="+result.artifact()+" immutableContentFiles="+contentFiles.size());
            }
        }finally{child.destroyForcibly();child.waitFor(10,TimeUnit.SECONDS);}
    }
}
