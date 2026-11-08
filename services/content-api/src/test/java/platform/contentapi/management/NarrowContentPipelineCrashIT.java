package platform.contentapi.management;

import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import platform.contentapi.execution.*;
import static org.assertj.core.api.Assertions.*;

/** Abrupt real worker death, not a caught exception in the fixture's JVM. */
class NarrowContentPipelineCrashIT extends NarrowContentPipelineFixtures {
    @ParameterizedTest
    @CsvSource({
        "BRONZE_BEFORE_SILVER,CHILD_RECORDED,0,false",
        "PUBLISH_SILVER,HEARTBEAT,1,false",
        "RECHECK_SILVER,CHECKPOINT_COMMITTED,4,false",
        "RECHECK_GOLD,CHECKPOINT_COMMITTED,10,false",
        "RECHECK_CONTENT,SUCCESS_INSERTED,15,false",
        "RECHECK_CONTENT,SUCCESS_COMMITTED,15,true"
    })
    void abruptWorkerDeathAtNativeEvidenceBoundaries(String operation,String boundary,int checkpoints,boolean success)throws Exception {
        crash(operation,boundary,checkpoints,success);
    }
    @ParameterizedTest
    @CsvSource({"BRONZE_BEFORE_GOLD_PREPARATION,RESERVED,4,false","BRONZE_BEFORE_CONTENT_PREPARATION,RESERVED,10,false"})
    void abruptWorkerDeathAfterArtifactRegistration(String operation,String boundary,int checkpoints,boolean success)throws Exception {
        crash(operation,boundary,checkpoints,success);
    }
    private void crash(String operation,String boundary,int checkpoints,boolean success)throws Exception {
        objectError=null;UUID run=queue("actual-pipeline-crash"),attempt=UUID.randomUUID();
        Path descriptor=temp.resolve("pipeline-crash-binding-"+attempt+".json"),signal=temp.resolve("pipeline-crash-signal-"+attempt),log=temp.resolve("pipeline-crash-"+attempt+".log");
        Files.write(descriptor,binding.descriptor());
        var builder=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx512m","-XX:ActiveProcessorCount=2","-cp",
                System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                "platform.contentapi.execution.NarrowContentPipelineCrashTestProcess",descriptor.toString(),CONTENT_PROFILE,
                run.toString(),attempt.toString(),PIPELINE_PROFILE,operation,boundary,signal.toString())
                .redirectErrorStream(true).redirectOutput(log.toFile());
        builder.environment().put("DDIP_TEST_DB_URL",url);builder.environment().put("DDIP_TEST_DB_USER",user);builder.environment().put("DDIP_TEST_DB_PASSWORD",password);
        var child=builder.start();
        List<ProcessHandle> descendants=List.of();
        try {
            long deadline=System.nanoTime()+Duration.ofMinutes(10).toNanos();
            while(!Files.exists(signal) && child.isAlive() && System.nanoTime()<deadline)Thread.sleep(100);
            assertThat(Files.exists(signal)).withFailMessage("Worker did not reach %s:%s; log: %s",operation,boundary,Files.readString(log)).isTrue();
            assertThat(Files.readString(signal)).isEqualTo(operation+":"+boundary);
            descendants=child.descendants().toList();
            if(boundary.equals("CHILD_RECORDED") || boundary.equals("HEARTBEAT"))assertThat(descendants).isNotEmpty();
            if(boundary.equals("HEARTBEAT"))assertThat(descendants).anyMatch(p->p.info().commandLine().orElse("").contains("org.apache.spark.deploy.SparkSubmit"));
            child.destroyForcibly();assertThat(child.waitFor(30,TimeUnit.SECONDS)).isTrue();assertThat(child.exitValue()).isNotZero();
        } finally {
            if(child.isAlive()){child.destroyForcibly();child.waitFor(30,TimeUnit.SECONDS);}
            // Test-owned exact ProcessHandles only. This cleanup is not recovery or quiescence proof.
            for(var process:descendants.reversed())if(process.isAlive())process.destroyForcibly();
            for(var process:descendants)if(process.isAlive())try{process.onExit().get(30,TimeUnit.SECONDS);}catch(java.util.concurrent.TimeoutException ignored){}
        }
        assertThat(count("content_fetch_checkpoints")).isEqualTo(1);assertThat(count("content_bronze_checkpoints")).isEqualTo(1);
        assertThat(count("content_pipeline_checkpoints")).isEqualTo(checkpoints);
        assertThat(count("content_pipeline_completions")).isEqualTo(success?1:0);
        assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo(success?"SUCCEEDED":"RUNNING");
        assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(success?null:attempt);
        assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run)==null).isEqualTo(!success);
        assertThat(count("stage_invocations")).isZero();assertThat(count("rss_deliveries")).isZero();
        assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);
        int reads=objectRequests.get();var before=history();var retainedFiles=files();
        var history=new RunHistoryRepository(jdbc,tx,new SourceCursor(SourceFixtures.KEY));
        var facts=history.nativeOperations(run.toString(),attempt.toString(),100,null).data();
        assertThat(facts).isNotEmpty();for(var fact:facts)assertThat(fact.get("continuationAuthorized").booleanValue()).isFalse();
        assertThat(history()).isEqualTo(before);assertThat(NarrowContentPipelineTestSupport.ordinaryContentDispatchAvailable()).isFalse();
        guard=InstallationHostGuard.acquire(binding);workers=WorkerTestSupport.enabled(jdbc,tx,INSTALL);
        session=workers.register(guard,CONTENT_PROFILE,digest(CONTENT_PROFILE),NarrowContentFetchReservationTestSupport.deploymentDigest(jdbc,CONTENT_PROFILE));
        if(!success){
            assertThat(session.status()).isEqualTo("RECOVERY_ONLY");
            assertThatThrownBy(()->NarrowContentAdmissionTestSupport.claim(workers,guard,session,UUID.randomUUID(),run)).isInstanceOf(RuntimeException.class);
            assertThatThrownBy(()->NarrowContentFetchRecoveryAssessmentTestSupport.inspect(jdbc,tx,guard,run,attempt)).isInstanceOf(RuntimeException.class);
            assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isEqualTo(attempt);
            if(!boundary.equals("CHILD_RECORDED") && !boundary.equals("HEARTBEAT")) {
                long version=jdbc.queryForObject("SELECT row_version FROM ddip_management.runs WHERE run_id=?",Long.class,run);
                var request=SourceJson.object().put("action","RECONCILE").put("attemptId",attempt.toString()).put("quiescenceConfirmed",true);
                UUID control=UUID.fromString(new ControlRepository(jdbc,tx,INSTALL).request(ControlCommands.parse(run.toString(),SourceJson.canonical(request),"crash-terminal-"+attempt,"\""+version+"\"")).get("controlId").stringValue());
                var terminal=new NarrowContentTerminalTestSupport(jdbc,tx,INSTALL);
                UUID terminalId=UUID.randomUUID();
                var target=terminal.inspect(guard,run,attempt,control,UUID.randomUUID(),terminalId);
                var qualified=terminal.qualify(guard,target,java.time.Instant.now(),"");
                assertThat(terminal.finish(guard,qualified)).isEqualTo("COMMITTED");
                assertThat(terminal.lookup(terminalId)).isEqualTo("CONFIRMED_TERMINAL");
                int absentBindings=ContentTerminalSqlTestSupport.assertFrozenBindings(jdbc,terminalId);
                // These reserved inspections follow registration; no next publication address exists yet.
                if(boundary.equals("RESERVED"))assertThat(absentBindings).isZero();
                else assertThat(absentBindings).isGreaterThan(0);
                var lookup=new ProcessBuilder(Path.of(System.getProperty("java.home"),"bin/java").toString(),"-Xmx128m","-XX:ActiveProcessorCount=2","-cp",
                        System.getProperty("surefire.test.class.path",System.getProperty("java.class.path")),
                        "platform.contentapi.execution.NarrowContentTerminalLookupTestProcess",url,user,password,INSTALL.toString(),terminalId.toString()).redirectErrorStream(true).start();
                assertThat(lookup.waitFor(30,TimeUnit.SECONDS)).isTrue();
                assertThat(lookup.exitValue()).withFailMessage(new String(lookup.getInputStream().readAllBytes(),java.nio.charset.StandardCharsets.UTF_8)).isZero();
                assertThat(jdbc.queryForObject("SELECT state FROM ddip_management.runs WHERE run_id=?",String.class,run)).isEqualTo("FAILED");
                assertThat(jdbc.queryForObject("SELECT result_artifact_id FROM ddip_management.runs WHERE run_id=?",UUID.class,run)).isNull();
                assertThat(jdbc.queryForObject("SELECT active_attempt_id FROM ddip_management.coordinator",UUID.class)).isNull();
                assertThat(count("content_pipeline_checkpoints")).isEqualTo(checkpoints);
                assertThat(count("content_pipeline_completions")).isZero();
            }
        }else {
            assertThat(session.status()).isEqualTo("READY");
            assertThat(history.get(run.toString()).get("result").get("verificationStatus").stringValue()).isEqualTo("VERIFIED_AT_EXECUTION");
        }
        assertThat(objectRequests).hasValue(reads);assertThat(requests).hasValue(1);assertThat(objectWrites).hasValue(2);assertThat(files()).isEqualTo(retainedFiles);
    }
    Map<String,String> files()throws Exception {
        var files=new TreeMap<String,String>();
        for(var entry:roots.entrySet())try(var paths=Files.walk(entry.getValue())) {
            for(var path:paths.filter(p->Files.isRegularFile(p,LinkOption.NOFOLLOW_LINKS)).toList())
                files.put(entry.getKey()+":"+entry.getValue().relativize(path),SourceJson.sha(Files.readAllBytes(path)));
        }
        return files;
    }
    Map<String,List<String>> history(){
        var rows=new TreeMap<>(metadata());
        for(String table:List.of("artifacts","artifact_dependencies","content_pipeline_operations","content_pipeline_phases","content_pipeline_checkpoints","content_pipeline_completions"))
            rows.put(table,jdbc.queryForList("SELECT to_jsonb(t)::text FROM ddip_management."+table+" t ORDER BY to_jsonb(t)::text",String.class));
        return rows;
    }
}
