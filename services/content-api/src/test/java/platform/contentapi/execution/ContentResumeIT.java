package platform.contentapi.execution;

import java.nio.file.*;
import java.time.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import platform.contentapi.management.*;
import tools.jackson.databind.JsonNode;
import static org.assertj.core.api.Assertions.*;

/** Real interrupted original JVM, read-only recovery, explicit resume and D5a finalization. */
class ContentResumeIT extends ContentRecoveryFixture {
    @org.junit.jupiter.api.Test void preFetchResume()throws Exception{recover("BEFORE_FETCH");}
    @org.junit.jupiter.api.Test void goldResumeAfterRestart()throws Exception{recover("GOLD");}
    @org.junit.jupiter.api.Test void contentInputsResume()throws Exception{recover("CHECK_INPUTS");}
    @org.junit.jupiter.api.Test void journalResumeRejected()throws Exception{recover("FETCH_SOURCE");}
    @org.junit.jupiter.api.Test void bronzeResumeRejected()throws Exception{recover("BRONZE");}
    @org.junit.jupiter.api.Test void partialSilverSelectionBlocked()throws Exception{recover("PARTIAL_SILVER");}
    @org.junit.jupiter.api.Test void silverResumeRejected()throws Exception{recover("SILVER");}
    @Override void afterRecovery(String mode,JsonNode n,JdbcTemplate j,ManagementTransactions tx,InstallationHostGuard.Binding binding,InstallationHostGuard guard,UUID reconcile,UUID original,JsonNode frozen)throws Exception{
        UUID run=UUID.fromString(n.get("runId").stringValue());long version=j.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
        var body=SourceJson.object().put("action","RESUME").putNull("attemptId").putNull("quiescenceConfirmed");var command=ControlCommands.parse(run.toString(),SourceJson.canonical(body),"explicit-resume","\""+version+"\"");var controls=new ControlRepository(j,tx,binding.installationId());
        if(Set.of("FETCH_SOURCE","BRONZE","SILVER").contains(mode)){
            assertThatThrownBy(()->controls.request(command)).hasMessage("RESUME_CHECKPOINT_UNSUPPORTED");assertThat(j.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("PAUSED");return;
        }
        phase="resume-request";UUID control=UUID.fromString(controls.request(command).get("controlId").stringValue());assertThat(controls.request(command).get("controlId").stringValue()).isEqualTo(control.toString());
        var oldStages=j.queryForList("SELECT to_jsonb(s)::text FROM ddip_management.stage_invocations s WHERE attempt_id=? ORDER BY stage,item_ordinal",original);var oldAttempts=j.queryForList("SELECT * FROM ddip_management.attempts ORDER BY attempt_no");var oldDeliveries=j.queryForList("SELECT to_jsonb(d)::text FROM ddip_management.rss_deliveries d ORDER BY source_id");var oldEdges=j.queryForList("SELECT * FROM ddip_management.artifact_dependencies ORDER BY artifact_id,role,ordinal");var oldFiles=new TreeMap<String,Map<String,String>>();for(var b:frozen.get("storageBindings"))if(Set.of("SILVER","GOLD").contains(b.get("role").stringValue()))oldFiles.put(b.get("role").stringValue(),snapshot(Path.of(b.get("configuration").get("root").stringValue())));
        var resume=new ContentResume(j,tx,binding.installationId());
        for(var state:List.of(RecoveryQuiescence.State.LIVE,RecoveryQuiescence.State.UNAVAILABLE)){
            var unsafe=new ContentResume(j,tx,binding.installationId(),new ContentResumeEvidence(j,tx,p->state));assertThatThrownBy(()->unsafe.requeue(guard,control,n.get("runtime").stringValue())).hasMessage("RESUME_EVIDENCE_UNAVAILABLE");assertThat(j.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("PAUSED");
        }
        assertThatThrownBy(()->resume.requeue(guard,control,"unapproved-runtime")).hasMessage("RESUME_EVIDENCE_UNAVAILABLE");
        if(mode.equals("BEFORE_FETCH"))for(String table:List.of("runs","controls","audit_events")){
            j.execute("CREATE OR REPLACE FUNCTION ddip_management.resume_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");j.execute("CREATE TRIGGER resume_fault BEFORE INSERT OR UPDATE ON ddip_management."+table+" FOR EACH ROW EXECUTE FUNCTION ddip_management.resume_fault()");try{assertThatThrownBy(()->resume.requeue(guard,control,n.get("runtime").stringValue())).hasMessage("STATE_CONFLICT");}finally{j.execute("DROP TRIGGER resume_fault ON ddip_management."+table);}assertThat(resume.applied(control)).isNull();assertThat(j.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("PAUSED");
        }
        phase="resume-requeue";JsonNode plan;
        if(mode.equals("BEFORE_FETCH")){
            try(var proxy=new RecoveryAcknowledgmentProxy(n.get("url").stringValue(),"CONTENT_RESUME_QUEUED")){
                var lost=new JdbcTemplate(new DriverManagerDataSource(proxy.url(),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));var lostTx=ReliabilityExecutionFixtures.transactions(lost);assertThatThrownBy(()->new ContentResume(lost,lostTx,binding.installationId()).requeue(guard,control,n.get("runtime").stringValue())).hasMessage("MANAGEMENT_UNAVAILABLE");assertThat(proxy.dropped.get()).isTrue();
            }plan=resume.applied(control);
        }else plan=resume.requeue(guard,control,n.get("runtime").stringValue());
        assertThat(resume.requeue(guard,control,n.get("runtime").stringValue())).isEqualTo(plan);assertThat(new ContentRecovery(j,tx,new RecoveryHandoff(j,tx,binding.installationId())).resolve(guard,reconcile).state()).isEqualTo("PAUSED");
        assertThat(j.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("QUEUED");assertThat(j.queryForObject("SELECT current_attempt_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run)).isNull();assertThat(j.queryForList("SELECT * FROM ddip_management.attempts ORDER BY attempt_no")).isEqualTo(oldAttempts);
        if(mode.equals("BEFORE_FETCH")){
            Path retained=guard.binding().root().resolve(plan.get("verificationAddress").stringValue()),backup=retained.resolveSibling("held-evidence.json");byte[] raw=Files.readAllBytes(retained);Files.move(retained,backup);try{assertThatThrownBy(()->resume.execute(guard,control,n.get("runtime").stringValue())).hasMessage("RESUME_EVIDENCE_UNAVAILABLE");}finally{Files.move(backup,retained);}
            Files.write(retained,new byte[]{1});try{assertThatThrownBy(()->resume.execute(guard,control,n.get("runtime").stringValue())).hasMessage("RESUME_EVIDENCE_INVALID");}finally{Files.write(retained,raw);}
        }
        if(mode.equals("GOLD"))tx.required(()->{
            for(var source:frozen.get("sources")){
                String id=source.get("sourceId").stringValue();j.update("INSERT INTO ddip_management.source_revisions(product_id,source_id,revision,display_name,config_version,configuration_bytes,configuration_sha256,actor,endpoint_id,endpoint_sha256) SELECT product_id,source_id,revision+1,'New revision after captured run',config_version,configuration_bytes,configuration_sha256,actor,endpoint_id,endpoint_sha256 FROM ddip_management.source_revisions WHERE product_id='content-intelligence' AND source_id=? AND revision=?",id,source.get("revision").intValue());j.update("UPDATE ddip_management.sources SET current_revision=current_revision+1,row_version=row_version+1 WHERE product_id='content-intelligence' AND source_id=?",id);
            }return null;
        });
        // A fresh process guard supplies a fresh worker instance, as separate operator invocations do.
        guard.close();
        if(mode.equals("GOLD")){
            phase="resume-postgres-restart";var before=j.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);String container=System.getenv("DDIP_TEST_POSTGRES_CONTAINER_ID");assertThat(container).matches("[0-9a-f]{12,64}");var restart=new ProcessBuilder("docker","restart",container).redirectOutput(ProcessBuilder.Redirect.DISCARD).redirectError(ProcessBuilder.Redirect.INHERIT).start();assertThat(restart.waitFor(60,TimeUnit.SECONDS)).isTrue();assertThat(restart.exitValue()).isZero();long end=System.nanoTime()+TimeUnit.SECONDS.toNanos(45);while(true){try{var after=j.queryForObject("SELECT pg_postmaster_start_time()",OffsetDateTime.class);assertThat(after).isAfter(before);System.out.println("D5B2A_POSTGRES_RESTART before="+before+" after="+after+" run="+run);break;}catch(org.springframework.dao.DataAccessException ex){if(System.nanoTime()>end)throw ex;Thread.sleep(100);}}
        }
        if(mode.equals("BEFORE_FETCH"))for(String table:List.of("attempts","runs","coordinator","audit_events")){
            j.execute("CREATE OR REPLACE FUNCTION ddip_management.resume_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");j.execute("CREATE TRIGGER resume_fault BEFORE INSERT OR UPDATE ON ddip_management."+table+" FOR EACH ROW EXECUTE FUNCTION ddip_management.resume_fault()");try(var candidate=InstallationHostGuard.acquire(binding)){assertThatThrownBy(()->resume.execute(candidate,control,n.get("runtime").stringValue())).hasMessage("STATE_CONFLICT");}finally{j.execute("DROP TRIGGER resume_fault ON ddip_management."+table);}assertThat(j.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("QUEUED");assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();assertThat(j.queryForList("SELECT * FROM ddip_management.attempts ORDER BY attempt_no")).isEqualTo(oldAttempts);
        }
        var endpoint=java.net.URI.create(n.get("endpoint").stringValue());var fetches=new java.util.concurrent.atomic.AtomicInteger();var server=com.sun.net.httpserver.HttpServer.create(new java.net.InetSocketAddress("127.0.0.1",endpoint.getPort()),0);server.createContext("/feed",e->{fetches.incrementAndGet();byte[] payload=Base64.getDecoder().decode(n.get("feedPayload").stringValue());e.getResponseHeaders().set("Content-Type","application/atom+xml");e.sendResponseHeaders(200,payload.length);e.getResponseBody().write(payload);e.close();});server.start();
        UUID artifact;
        try(var fresh=InstallationHostGuard.acquire(binding)){
            phase="resume-execute";
            if(mode.equals("GOLD"))try(var proxy=new RecoveryAcknowledgmentProxy(n.get("url").stringValue(),"RUN_SUCCEEDED")){
                var lost=new JdbcTemplate(new DriverManagerDataSource(proxy.url(),System.getenv("DDIP_TEST_DB_USER"),System.getenv("DDIP_TEST_DB_PASSWORD")));artifact=new ContentResume(lost,terminalTransactions(lost,run),binding.installationId()).execute(fresh,control,n.get("runtime").stringValue());assertThat(proxy.dropped.get()).isTrue();
            }else artifact=resume.execute(fresh,control,n.get("runtime").stringValue());
            assertThat(resume.execute(fresh,control,n.get("runtime").stringValue())).isEqualTo(artifact);assertThat(new ContentRecovery(j,tx,new RecoveryHandoff(j,tx,binding.installationId())).resolve(fresh,reconcile).state()).isEqualTo("PAUSED");var evidence=new ContentResumeEvidence(j,tx);assertThat(SourceJson.sha(SourceJson.canonical(evidence.privateProof(fresh,plan,RssContentRuntime.load(j,tx,fresh,run,frozen,n.get("runtime").stringValue()))))).isEqualTo(plan.get("privateSha256").stringValue());
        }finally{server.stop(0);}
        assertThat(fetches.get()).isEqualTo(mode.equals("BEFORE_FETCH")?1:0);
        phase="resume-assertions";UUID current=j.queryForObject("SELECT current_attempt_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run);assertThat(current).isNotIn(original,reconcile);assertThat(j.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=? AND state='SUCCEEDED'",UUID.class,run)).isEqualTo(artifact);assertThat(j.queryForObject("SELECT state FROM ddip_management.attempts WHERE attempt_id=?",String.class,current)).isEqualTo("SUCCEEDED");
        assertThat(j.queryForList("SELECT to_jsonb(s)::text FROM ddip_management.stage_invocations s WHERE attempt_id=? ORDER BY stage,item_ordinal",original)).isEqualTo(oldStages);assertThat(j.queryForList("SELECT * FROM ddip_management.attempts WHERE attempt_id<>? ORDER BY attempt_no",current)).isEqualTo(oldAttempts);
        if(!mode.equals("BEFORE_FETCH")){
            assertThat(j.queryForList("SELECT to_jsonb(d)::text FROM ddip_management.rss_deliveries d ORDER BY source_id")).isEqualTo(oldDeliveries);for(var b:frozen.get("storageBindings"))if(oldFiles.containsKey(b.get("role").stringValue()))assertThat(snapshot(Path.of(b.get("configuration").get("root").stringValue()))).isEqualTo(oldFiles.get(b.get("role").stringValue()));
            assertThat(j.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage IN ('FETCH_SOURCE','STORE_BRONZE','SILVER','PREPARE_GOLD','PUBLISH_GOLD')",Integer.class,current)).isZero();assertThat(j.queryForObject("SELECT reused_checkpoint_id FROM ddip_management.attempts WHERE attempt_id=?",UUID.class,current)).isNotNull();
        }
        if(mode.equals("CHECK_INPUTS"))assertThat(j.queryForObject("SELECT count(*) FROM ddip_management.stage_invocations WHERE attempt_id=? AND stage='CHECK_INPUTS'",Integer.class,current)).isZero();
        assertThat(j.queryForList("SELECT * FROM ddip_management.artifact_dependencies WHERE artifact_id<>? ORDER BY artifact_id,role,ordinal",artifact)).containsAll(oldEdges);assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();assertThat(j.queryForObject("SELECT count(*) FROM ddip_management.audit_events WHERE action='CONTENT_RESUME_QUEUED' AND control_id=?",Integer.class,control)).isEqualTo(1);
        UUID dataAttempt=mode.equals("BEFORE_FETCH")?current:original;UUID gold=j.queryForObject("SELECT artifact_id FROM ddip_management.stage_artifacts WHERE attempt_id=? AND stage='PUBLISH_GOLD' AND role='RESULT'",UUID.class,dataAttempt);assertThat(j.queryForObject("SELECT dependency_artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? AND role='GOLD'",UUID.class,artifact)).isEqualTo(gold);assertThat(j.queryForObject("SELECT dependency_artifact_id FROM ddip_management.artifact_dependencies WHERE artifact_id=? AND role='PUBLISHER_REFERENCE'",UUID.class,artifact).toString()).isEqualTo(RssContentInputs.reference(frozen).get("artifactId").stringValue());
        if(mode.equals("GOLD"))for(var source:frozen.get("sources")){assertThat(j.queryForObject("SELECT current_revision FROM ddip_management.sources WHERE product_id='content-intelligence' AND source_id=?",Integer.class,source.get("sourceId").stringValue())).isEqualTo(source.get("revision").intValue()+1);assertThat(j.queryForObject("SELECT revision FROM ddip_management.rss_deliveries WHERE run_id=? AND source_id=?",Integer.class,run,source.get("sourceId").stringValue())).isEqualTo(source.get("revision").intValue());}
        System.out.println("D5B2A_RESUME boundary="+mode+" sources="+frozen.get("sources").size()+" original="+original+" recovery="+reconcile+" newAttempt="+current+" artifact="+artifact+" reusedFiles="+oldFiles.values().stream().mapToInt(Map::size).sum());
        var catalog=PreflightCatalogFixture.captured(SourceJson.parse(Files.readAllBytes(temp.resolve("catalog.json"))),frozen);var nextBody=SourceJson.parse(j.queryForObject("SELECT normalized_request FROM ddip_management.runs WHERE run_id=?",byte[].class,run));for(var source:nextBody.get("fetchSources"))((tools.jackson.databind.node.ObjectNode)source).put("revision",j.queryForObject("SELECT current_revision FROM ddip_management.sources WHERE product_id='content-intelligence' AND source_id=?",Integer.class,source.get("sourceId").stringValue()));var request=RunCommands.parse(SourceJson.canonical(nextBody),"next-content-after-resume");UUID next=UUID.fromString(new RunRepository(j,tx,binding.installationId(),catalog).submit(request,"resume-admission").get("runId").stringValue());
        try(var nextGuard=InstallationHostGuard.acquire(binding)){var workers=WorkerTestSupport.enabled(j,tx,binding.installationId());var profile=frozen.get("executionProfile");var session=workers.register(nextGuard,profile.get("profileId").stringValue(),profile.get("configurationSha256").stringValue(),profile.get("configuration").get("runtime").get("deploymentSha256").stringValue());var claimed=workers.claim(nextGuard,session,UUID.randomUUID());assertThat(claimed.runId()).isEqualTo(next);assertThat(claimed.owner()).isNotNull();System.out.println("D5B2A_NEXT_CLAIM run="+next+" epoch="+claimed.owner().worker().epoch());workers.failBeforeLaunch(nextGuard,claimed.owner(),WorkerRepository.PrelaunchFailure.PROCESS_START_FAILED);}

    }
    /** Reinvoke only the existing metadata finalizer callback after rolled-back fault probes. No native replay. */
    static ManagementTransactions terminalTransactions(JdbcTemplate j,UUID run){
        var probed=new java.util.concurrent.atomic.AtomicBoolean();
        var template=new org.springframework.transaction.support.TransactionTemplate(new org.springframework.jdbc.support.JdbcTransactionManager(j.getDataSource())){
            @Override public <T>T execute(org.springframework.transaction.support.TransactionCallback<T> action){
                try{return super.execute(status->{T result=action.doInTransaction(status);if(!probed.get() && j.queryForObject("SELECT state='SUCCEEDED' FROM ddip_management.runs WHERE run_id=?",Boolean.class,run)){probed.set(true);throw new TerminalRollback();}return result;});}
                catch(TerminalRollback expected){
                    assertHeld();var immutable=contentSnapshot();
                    assertThatThrownBy(()->super.execute(status->{UUID cancel=UUID.randomUUID();byte[] body=SourceJson.canonical(SourceJson.object().put("action","CANCEL"));long version=j.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);j.update("INSERT INTO ddip_management.controls(control_id,run_id,type,idempotency_key,normalized_request,request_sha256,expected_version,state,requester) VALUES (?,?,'CANCEL','pending-resumed-cancel',?,?,?,'REQUESTED','local-operator')",cancel,run,body,SourceJson.sha(body),version);assertThatThrownBy(()->action.doInTransaction(status)).hasMessage("WORKER_NOT_READY");throw new TerminalRollback();})).isInstanceOf(TerminalRollback.class);assertHeld();
                    for(String table:List.of("artifacts","artifact_dependencies","stage_artifacts","stage_invocations","attempts","runs","coordinator","audit_events")){
                        j.execute("CREATE OR REPLACE FUNCTION ddip_management.resume_terminal_fault() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION USING MESSAGE='PRIVATE',ERRCODE='23514'; END $$");j.execute("CREATE TRIGGER resume_terminal_fault BEFORE INSERT OR UPDATE ON ddip_management."+table+" FOR EACH ROW "+(table.equals("audit_events")?"WHEN (NEW.action='RUN_SUCCEEDED') ":"")+"EXECUTE FUNCTION ddip_management.resume_terminal_fault()");
                        try{assertThatThrownBy(()->super.execute(action)).isInstanceOf(org.springframework.dao.DataIntegrityViolationException.class);}finally{j.execute("DROP TRIGGER resume_terminal_fault ON ddip_management."+table);}assertHeld();assertThat(contentSnapshot()).isEqualTo(immutable);
                    }
                    System.out.println("D5B2A_TERMINAL rollbackSites=8 pendingCancel=VETO immutableContentFiles="+immutable.size()+" occupied="+j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class));try{return super.execute(action);}finally{assertThat(contentSnapshot()).isEqualTo(immutable);}
                }
            }
            Map<String,String> contentSnapshot(){try{return snapshot(outputRoot(SourceJson.parse(j.queryForObject("SELECT captured_selection FROM ddip_management.runs WHERE run_id=?",byte[].class,run),PreflightFiles.LARGE)));}catch(Exception ex){throw new AssertionError(ex);}}
            void assertHeld(){assertThat(j.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("RUNNING");assertThat(j.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNotNull();assertThat(j.queryForObject("SELECT count(*) FROM ddip_management.artifacts WHERE kind='CONTENT'",Integer.class)).isZero();}
        };
        template.setTimeout(5);return new ManagementTransactions(template);
    }
    static final class TerminalRollback extends RuntimeException{}

}
